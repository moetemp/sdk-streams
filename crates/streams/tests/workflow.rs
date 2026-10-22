//! The Workflow path on a real Redis: lang's buffer call at the end of each Activation, then
//! the interceptor hooks as Core calls them around each Workflow Task completion.

mod support;

use prost::Message;
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use temporalio_common::protos::temporal::api::{common::v1::Payload, failure::v1::Failure};
use temporalio_sdk_core::{
    ExtensionError, RunCloseKind, WorkerExtension, WorkflowTaskCompletionInfo, WorkflowTaskEpoch,
    WorkflowTaskFailedInfo, WorkflowTaskInterceptor,
};
use temporalio_streams::{
    ExtensionOptions, RedisStreamExtension,
    epoch::withdraws,
    ids::{client_producer_id, default_stream_id},
    proto::{
        AppendRecord, AppendRecordsRequest, BufferWorkflowCallsRequest, CloseStreamRequest,
        CloseStreamResponse, CreateStreamRequest, DrainClosesRequest, DrainClosesResponse,
        Producer, ProducerEpoch, ProducerKind, ReadRecordsRequest, ReadRecordsResponse,
        RedisStreamConfig, StreamRecord, StreamRecordKind, StreamRef, WorkflowCall,
        workflow_call::Call,
    },
};

static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn unique() -> String {
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{now}-{n}")
}

/// One Workflow run on one server, with its default Stream.
struct Run {
    ext: Arc<RedisStreamExtension>,
    hooks: Arc<dyn WorkflowTaskInterceptor>,
    namespace: String,
    workflow_id: String,
    run_id: String,
    first_run_id: String,
    stream: String,
}

fn runs_on(extensions: Vec<Arc<RedisStreamExtension>>) -> Vec<Run> {
    extensions
        .into_iter()
        .map(|ext| {
            let hooks = ext.workflow_task_interceptor().unwrap();
            let unique = unique();
            let (workflow_id, run_id) = (format!("wf-{unique}"), format!("run-{unique}"));
            Run {
                ext,
                hooks,
                namespace: format!("ns-{unique}"),
                stream: default_stream_id(&workflow_id, &run_id),
                workflow_id,
                first_run_id: run_id.clone(),
                run_id,
            }
        })
        .collect()
}

/// A run on each Redis server that is set, with the config changed by `change`.
async fn runs_with(change: impl Fn(&mut RedisStreamConfig)) -> Vec<Run> {
    runs_on(support::extensions(change, false))
}

/// A run on each Redis server that is set and on the memory Store.
async fn runs() -> Vec<Run> {
    runs_on(support::extensions(|_| {}, true))
}

fn payload(text: &str) -> Option<Payload> {
    Some(Payload {
        data: text.as_bytes().to_vec(),
        ..Default::default()
    })
}

fn time(seconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(seconds)
}

impl Run {
    /// A Workflow's ref, with the owner fields that lang fills.
    fn stream_ref(&self) -> Option<StreamRef> {
        Some(StreamRef {
            id: self.stream.clone(),
            workflow_id: self.workflow_id.clone(),
            first_execution_run_id: self.first_run_id.clone(),
        })
    }

    fn create(&self) -> WorkflowCall {
        WorkflowCall {
            call: Some(Call::Create(CreateStreamRequest {
                stream_id: self.stream.clone(),
                ..Default::default()
            })),
            when_run_closes: false,
        }
    }

    fn publish(&self, texts: &[&str]) -> WorkflowCall {
        WorkflowCall {
            call: Some(Call::Append(AppendRecordsRequest {
                stream: self.stream_ref(),
                records: texts
                    .iter()
                    .map(|text| AppendRecord {
                        topic: "turns".to_string(),
                        body: payload(text),
                    })
                    .collect(),
                ..Default::default()
            })),
            when_run_closes: false,
        }
    }

    fn close(&self, when_run_closes: bool, failure: Option<Failure>) -> WorkflowCall {
        WorkflowCall {
            call: Some(Call::Close(CloseStreamRequest {
                stream: self.stream_ref(),
                failure,
                ..Default::default()
            })),
            when_run_closes,
        }
    }

    /// Lang's call at the end of one Activation.
    fn activation(&self, calls: Vec<WorkflowCall>) -> Result<Vec<u8>, ExtensionError> {
        self.activation_of(&self.run_id, "", calls)
    }

    fn activation_of(
        &self,
        run_id: &str,
        withdraw_run: &str,
        calls: Vec<WorkflowCall>,
    ) -> Result<Vec<u8>, ExtensionError> {
        let request = BufferWorkflowCallsRequest {
            run_id: run_id.to_string(),
            withdraw_run: withdraw_run.to_string(),
            calls,
            chain_continues: false,
        };
        self.ext
            .call("BufferWorkflowCalls", &request.encode_to_vec())
    }

    fn task(&self, started: i64, attempt: u32) -> TaskInfo {
        TaskInfo {
            run_id: self.run_id.clone(),
            started,
            attempt,
            started_time: time(100),
            completed: None,
            failed: vec![],
            closes_run: None,
        }
    }

    fn info(&self, task: &TaskInfo) -> WorkflowTaskCompletionInfo {
        WorkflowTaskCompletionInfo::builder()
            .namespace(self.namespace.clone())
            .workflow_id(self.workflow_id.clone())
            .run_id(task.run_id.clone())
            .first_execution_run_id(self.first_run_id.clone())
            .workflow_type("agent")
            .started_event_id(task.started)
            .attempt(task.attempt)
            .started_time(task.started_time)
            .maybe_previous_completed(task.completed.map(|started| {
                WorkflowTaskEpoch::builder()
                    .started_event_id(started)
                    .attempt(1)
                    .started_time(time(100))
                    .build()
            }))
            .failed_tasks(task.failed.clone())
            .maybe_closes_run(task.closes_run)
            .deadline(std::time::Instant::now() + Duration::from_secs(10))
            .build()
    }

    /// Core's two hooks around one completion.
    async fn complete(&self, task: &TaskInfo, accepted: bool) -> Result<(), String> {
        let info = self.info(task);
        self.hooks
            .before_complete(&info)
            .await
            .map_err(|error| error.message)?;
        self.hooks.after_complete(&info, accepted).await;
        Ok(())
    }

    async fn drain(&self) {
        let request = DrainClosesRequest { timeout: None };
        let bytes = self
            .ext
            .call_async("DrainCloses".to_string(), request.encode_to_vec())
            .await
            .unwrap();
        assert_eq!(
            DrainClosesResponse::decode(bytes.as_slice())
                .unwrap()
                .pending,
            0
        );
    }

    async fn read(&self) -> ReadRecordsResponse {
        let request = ReadRecordsRequest {
            namespace: self.namespace.clone(),
            stream: self.stream_ref(),
            ..Default::default()
        };
        let bytes = self
            .ext
            .call_async("ReadRecords".to_string(), request.encode_to_vec())
            .await
            .unwrap();
        ReadRecordsResponse::decode(bytes.as_slice()).unwrap()
    }

    /// The data a Reader keeps, and whether the read ended with the close.
    async fn visible(&self) -> (Vec<String>, bool) {
        let read = self.read().await;
        let mut kept: Vec<StreamRecord> = vec![];
        let mut closed = false;
        for record in read.records {
            match StreamRecordKind::try_from(record.kind).unwrap() {
                StreamRecordKind::Superseded => {
                    let s = record.supersession.unwrap();
                    kept.retain(|old| !withdraws(&s, old));
                }
                StreamRecordKind::Data => kept.push(record),
                StreamRecordKind::Close => closed = true,
                _ => {}
            }
        }
        let texts = kept
            .into_iter()
            .map(|record| String::from_utf8(record.body.unwrap().data).unwrap())
            .collect();
        (texts, closed)
    }
}

struct TaskInfo {
    run_id: String,
    started: i64,
    attempt: u32,
    started_time: SystemTime,
    completed: Option<i64>,
    failed: Vec<i64>,
    closes_run: Option<RunCloseKind>,
}

impl TaskInfo {
    fn after(mut self, completed: i64) -> Self {
        self.completed = Some(completed);
        self
    }

    fn failed(mut self, failed: &[i64]) -> Self {
        self.failed = failed.to_vec();
        self
    }

    fn closing(mut self, kind: RunCloseKind) -> Self {
        self.closes_run = Some(kind);
        self
    }
}

#[tokio::test]
async fn a_task_flushes_its_activations_once_in_order() {
    for run in runs().await {
        run.activation(vec![run.create(), run.publish(&["a", "b"])])
            .unwrap();
        run.activation(vec![run.publish(&["c"])]).unwrap();
        run.complete(&run.task(3, 1), true).await.unwrap();
        let read = run.read().await;
        let producer = read.records[0].producer.as_ref().unwrap();
        assert_eq!(producer.kind, ProducerKind::Workflow as i32);
        assert_eq!(producer.epoch.unwrap().task_started_event_id, 3);
        assert_eq!(
            run.visible().await,
            (vec!["a".into(), "b".into(), "c".into()], false)
        );
        // The next task's buffer is empty, so its hooks write nothing.
        run.complete(&run.task(6, 1).after(3), true).await.unwrap();
        assert_eq!(run.read().await.records.len(), 3);
    }
}

#[tokio::test]
async fn a_run_that_never_publishes_never_needs_redis() {
    let config = RedisStreamConfig {
        urls: vec!["redis://127.0.0.1:1/0".to_string()],
        ..Default::default()
    };
    let ext = RedisStreamExtension::new(ExtensionOptions::from_config(config).unwrap(), None);
    let hooks = ext.workflow_task_interceptor().unwrap();
    let info = WorkflowTaskCompletionInfo::builder()
        .namespace("ns")
        .workflow_id("wf")
        .run_id("run")
        .first_execution_run_id("run")
        .workflow_type("t")
        .started_event_id(3)
        .attempt(1)
        .deadline(std::time::Instant::now() + Duration::from_secs(1))
        .build();
    hooks.before_complete(&info).await.unwrap();
    hooks.after_complete(&info, true).await;
}

#[tokio::test]
async fn a_failed_task_drops_its_buffer_and_the_next_flush_withdraws_its_records() {
    for run in runs().await {
        run.activation(vec![run.create(), run.publish(&["A"])])
            .unwrap();
        run.complete(&run.task(2, 1), true).await.unwrap();
        // Task [5] flushes B, then fails, for example on a refused completion.
        run.activation(vec![run.publish(&["B"])]).unwrap();
        run.complete(&run.task(5, 1).after(2), false).await.unwrap();
        // A task that fails before its hook leaves a buffer that its retry must not write.
        run.activation(vec![run.publish(&["lost"])]).unwrap();
        let failed = WorkflowTaskFailedInfo::builder()
            .run_id(run.run_id.clone())
            .attempt(1)
            .reported(true)
            .build();
        run.hooks.on_task_failed(&failed);
        run.activation(vec![run.publish(&["C"])]).unwrap();
        run.complete(&run.task(8, 1).after(2).failed(&[5]), true)
            .await
            .unwrap();
        assert_eq!(run.visible().await.0, ["A", "C"]);
    }
}

#[tokio::test]
async fn closes_go_out_only_after_an_accepted_completion() {
    for run in runs().await {
        run.activation(vec![
            run.create(),
            run.publish(&["a"]),
            run.close(false, None),
        ])
        .unwrap();
        run.complete(&run.task(3, 1), false).await.unwrap();
        run.drain().await;
        assert!(
            !run.visible().await.1,
            "a refused completion closed the Stream"
        );
        // The replay after the refusal hands the close over again, and the next live task
        // sends it.
        run.activation(vec![run.create(), run.close(false, None)])
            .unwrap();
        run.complete(&run.task(6, 1).after(3), true).await.unwrap();
        run.drain().await;
        assert_eq!(run.visible().await, (vec!["a".into()], true));
    }
}

#[tokio::test]
async fn an_auto_close_waits_for_the_completion_that_closes_the_run() {
    for run in runs().await {
        let failure = Failure {
            message: "boom".to_string(),
            ..Default::default()
        };
        run.activation(vec![
            run.create(),
            run.publish(&["a"]),
            run.close(true, None),
        ])
        .unwrap();
        run.complete(&run.task(3, 1), true).await.unwrap();
        run.drain().await;
        assert!(!run.visible().await.1);
        run.activation(vec![run.close(true, Some(failure))])
            .unwrap();
        run.complete(&run.task(6, 1).after(3).closing(RunCloseKind::Failed), true)
            .await
            .unwrap();
        run.drain().await;
        let read = run.read().await;
        let close = read.records.last().unwrap();
        assert_eq!(close.kind, StreamRecordKind::Close as i32);
        assert_eq!(close.close_failure.as_ref().unwrap().message, "boom");
        assert!(read.done);
    }
}

#[tokio::test]
async fn a_stream_someone_else_closed_drops_the_records_and_the_task_completes() {
    for run in runs().await {
        run.activation(vec![run.create(), run.publish(&["a"])])
            .unwrap();
        run.complete(&run.task(3, 1), true).await.unwrap();
        let close = CloseStreamRequest {
            namespace: run.namespace.clone(),
            stream: run.stream_ref(),
            producer: Some(Producer {
                kind: ProducerKind::Client as i32,
                id: client_producer_id("me", "p"),
                epoch: Some(ProducerEpoch::default()),
                run_id: String::new(),
            }),
            ..Default::default()
        };
        let bytes = run
            .ext
            .call_async("CloseStream".to_string(), close.encode_to_vec())
            .await
            .unwrap();
        CloseStreamResponse::decode(bytes.as_slice()).unwrap();
        run.activation(vec![run.publish(&["b"])]).unwrap();
        run.complete(&run.task(6, 1).after(3), true).await.unwrap();
        assert_eq!(run.visible().await, (vec!["a".into()], true));
    }
}

#[tokio::test]
async fn a_stale_task_fails_on_the_fence() {
    for run in runs().await {
        run.activation(vec![run.create(), run.publish(&["new"])])
            .unwrap();
        run.complete(&run.task(8, 1), true).await.unwrap();
        run.activation(vec![run.publish(&["stale"])]).unwrap();
        let error = run.complete(&run.task(5, 1), true).await.unwrap_err();
        assert!(error.contains("stream_fenced"), "{error}");
        assert_eq!(run.visible().await.0, ["new"]);
    }
}

#[tokio::test]
async fn a_retry_run_withdraws_the_previous_run() {
    for run in runs().await {
        run.activation(vec![run.create(), run.publish(&["r1"])])
            .unwrap();
        run.complete(&run.task(3, 1), true).await.unwrap();
        let retry = format!("{}-retry", run.run_id);
        run.activation_of(
            &retry,
            &run.run_id,
            vec![run.create(), run.publish(&["r2"])],
        )
        .unwrap();
        let mut task = run.task(3, 1);
        task.run_id = retry;
        run.complete(&task, true).await.unwrap();
        assert_eq!(run.visible().await.0, ["r2"]);
    }
}

#[tokio::test]
async fn a_flush_splits_big_output_and_drops_an_oversize_record() {
    for run in runs().await {
        let big = "x".repeat(1_400_000);
        let huge = "y".repeat(1_700_000);
        run.activation(vec![
            run.create(),
            run.publish(&[&big, &big, &huge, &big, "end"]),
        ])
        .unwrap();
        run.complete(&run.task(3, 1), true).await.unwrap();
        let texts = run.visible().await.0;
        assert_eq!(texts.len(), 4);
        assert_eq!(texts.last().unwrap(), "end");
    }
}

#[tokio::test]
async fn durable_writes_fail_the_task_when_no_replica_has_the_flush() {
    let runs = runs_with(|config| {
        config.durable_writes = 1;
        config.durable_timeout = Some(prost_types::Duration {
            seconds: 0,
            nanos: 100_000_000,
        });
    })
    .await;
    for run in runs {
        run.activation(vec![run.create(), run.publish(&["a"])])
            .unwrap();
        let started = Instant::now();
        let error = run.complete(&run.task(3, 1), true).await.unwrap_err();
        assert!(error.contains("replicas"), "{error}");
        assert!(started.elapsed() >= Duration::from_millis(100));
    }
}

#[test]
fn a_buffer_call_needs_a_run_and_stream_ids() {
    let config = RedisStreamConfig {
        urls: vec!["redis://127.0.0.1:1/0".to_string()],
        ..Default::default()
    };
    let ext = RedisStreamExtension::new(ExtensionOptions::from_config(config).unwrap(), None);
    let request = BufferWorkflowCallsRequest::default();
    let error = ext
        .call("BufferWorkflowCalls", &request.encode_to_vec())
        .unwrap_err();
    assert_eq!(error.code, tonic::Code::InvalidArgument);
    let request = BufferWorkflowCallsRequest {
        run_id: "r".to_string(),
        calls: vec![WorkflowCall {
            call: Some(Call::Append(AppendRecordsRequest::default())),
            when_run_closes: false,
        }],
        ..Default::default()
    };
    assert!(
        ext.call("BufferWorkflowCalls", &request.encode_to_vec())
            .is_err()
    );
    assert!(ext.call("BufferWorkflowCalls", b"\xff\xff").is_err());
}

#[tokio::test]
async fn an_auto_close_stays_out_when_the_run_chain_goes_on() {
    for run in runs().await {
        run.activation(vec![run.create(), run.publish(&["a"])])
            .unwrap();
        run.complete(&run.task(3, 1), true).await.unwrap();
        // The run fails, and its retry policy starts the next run of the Chain.
        let request = BufferWorkflowCallsRequest {
            run_id: run.run_id.clone(),
            calls: vec![run.close(true, None)],
            chain_continues: true,
            ..Default::default()
        };
        run.ext
            .call("BufferWorkflowCalls", &request.encode_to_vec())
            .unwrap();
        run.complete(&run.task(6, 1).after(3).closing(RunCloseKind::Failed), true)
            .await
            .unwrap();
        run.drain().await;
        assert_eq!(run.visible().await, (vec!["a".into()], false));
    }
}

#[tokio::test]
async fn a_task_with_no_started_time_fails_its_flush() {
    for run in runs().await {
        run.activation(vec![run.create(), run.publish(&["a"])])
            .unwrap();
        let mut info = run.info(&run.task(3, 1));
        info.started_time = None;
        let error = run.hooks.before_complete(&info).await.unwrap_err();
        assert!(error.message.contains("started time"), "{}", error.message);
    }
}

#[tokio::test]
async fn calls_after_a_drain_still_answer() {
    for run in runs().await {
        run.activation(vec![
            run.create(),
            run.publish(&["a"]),
            run.close(false, None),
        ])
        .unwrap();
        run.complete(&run.task(3, 1), true).await.unwrap();
        for _ in 0..2 {
            run.drain().await;
            let read = tokio::time::timeout(Duration::from_secs(5), run.read())
                .await
                .expect("a call after the drain answered");
            assert!(read.done);
            // A close after the drain still goes out in the background.
            run.activation(vec![run.create(), run.close(false, None)])
                .unwrap();
            run.complete(&run.task(6, 1).after(3), true).await.unwrap();
        }
        run.drain().await;
        // A drain that the caller drops halfway leaves the extension as it was.
        let drain = DrainClosesRequest { timeout: None }.encode_to_vec();
        let dropped = run.ext.call_async("DrainCloses".to_string(), drain);
        let _ = tokio::time::timeout(Duration::from_millis(1), dropped).await;
        let read = tokio::time::timeout(Duration::from_secs(5), run.read()).await;
        assert!(read.expect("a call after a dropped drain answered").done);
        run.drain().await;
    }
}

#[tokio::test]
async fn a_workflow_create_with_an_auto_close_closes_its_quiet_stream() {
    for run in runs().await {
        let create = WorkflowCall {
            call: Some(Call::Create(CreateStreamRequest {
                stream_id: run.stream.clone(),
                auto_close: Some(prost_types::Duration {
                    seconds: 0,
                    nanos: 300_000_000,
                }),
                ..Default::default()
            })),
            when_run_closes: false,
        };
        run.activation(vec![create, run.publish(&["a"])]).unwrap();
        run.complete(&run.task(3, 1), true).await.unwrap();
        assert_eq!(run.visible().await, (vec!["a".to_string()], false));
        tokio::time::sleep(Duration::from_millis(400)).await;
        // A later flush finds the Stream closed and drops its Records, as for any close.
        run.activation(vec![run.publish(&["late"])]).unwrap();
        run.complete(&run.task(7, 1).after(3), true).await.unwrap();
        assert_eq!(run.visible().await, (vec!["a".to_string()], true));
    }
}
