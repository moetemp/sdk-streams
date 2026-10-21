//! The Workflow path driven by Core's mock Worker, as a bridge runs it. A Local Activity keeps
//! the first Workflow Task open, so the first flush goes out in the heartbeat completion that
//! Core sends with no lang Activation.

mod support;

use prost::Message;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use support::server_url;
use temporalio_common::protos::{
    coresdk::{
        ActivityTaskCompletion, activity_result::ActivityExecutionResult,
        activity_task::activity_task, workflow_activation::workflow_activation_job,
        workflow_commands::ActivityCancellationType,
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::common::v1::Payload,
};
use temporalio_sdk_core::{
    ExtensionError, WorkerExtension, WorkflowTaskCompletionInfo, WorkflowTaskFailedInfo,
    WorkflowTaskInterceptError, WorkflowTaskInterceptor,
    replay::TestHistoryBuilder,
    test_help::{
        MockPollCfg, WorkerExt, WorkerTestHelpers, build_mock_pollers, mock_worker,
        mock_worker_client, schedule_local_activity_cmd,
    },
};
use temporalio_streams::{
    ExtensionOptions, RedisStreamExtension,
    ids::{default_stream_id, local_activity_producer_id},
    proto::{
        AppendRecord, AppendRecordsRequest, AppendRecordsResponse, BufferWorkflowCallsRequest,
        CloseStreamRequest, CreateStreamRequest, DrainClosesRequest, Producer, ProducerEpoch,
        ProducerKind, ReadRecordsRequest, ReadRecordsResponse, RedisStreamConfig, StreamRecordKind,
        StreamRef, WorkflowCall, workflow_call::Call,
    },
};

/// The crate's extension, with each hook call written to a log first.
struct Logged {
    inner: Arc<RedisStreamExtension>,
    hooks: Arc<dyn WorkflowTaskInterceptor>,
    log: Arc<Mutex<Vec<String>>>,
    namespace: Arc<Mutex<String>>,
}

#[async_trait::async_trait]
impl WorkerExtension for Logged {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn workflow_task_interceptor(&self) -> Option<Arc<dyn WorkflowTaskInterceptor>> {
        Some(Arc::new(LoggedHooks {
            inner: self.hooks.clone(),
            log: self.log.clone(),
            namespace: self.namespace.clone(),
        }))
    }

    fn call(&self, method: &str, input: &[u8]) -> Result<Vec<u8>, ExtensionError> {
        self.inner.call(method, input)
    }

    async fn call_async(&self, method: String, input: Vec<u8>) -> Result<Vec<u8>, ExtensionError> {
        self.inner.call_async(method, input).await
    }
}

struct LoggedHooks {
    inner: Arc<dyn WorkflowTaskInterceptor>,
    log: Arc<Mutex<Vec<String>>>,
    namespace: Arc<Mutex<String>>,
}

#[async_trait::async_trait]
impl WorkflowTaskInterceptor for LoggedHooks {
    async fn before_complete(
        &self,
        task: &WorkflowTaskCompletionInfo,
    ) -> Result<(), WorkflowTaskInterceptError> {
        *self.namespace.lock().unwrap() = task.namespace.clone();
        let previous = task
            .previous_completed
            .as_ref()
            .map_or(0, |c| c.started_event_id);
        self.log
            .lock()
            .unwrap()
            .push(format!("before {} after {previous}", task.started_event_id));
        let result = self.inner.before_complete(task).await;
        if let Err(error) = &result {
            self.log
                .lock()
                .unwrap()
                .push(format!("refused {}", error.message));
        }
        result
    }

    async fn after_complete(&self, task: &WorkflowTaskCompletionInfo, accepted: bool) {
        self.log.lock().unwrap().push(format!(
            "after {} {accepted} closes={}",
            task.started_event_id,
            task.closes_run.is_some()
        ));
        self.inner.after_complete(task, accepted).await
    }

    fn on_task_failed(&self, task: &WorkflowTaskFailedInfo) {
        self.log.lock().unwrap().push("failed".to_string());
        self.inner.on_task_failed(task)
    }

    fn on_evicted(&self, run_id: &str) {
        self.log.lock().unwrap().push("evicted".to_string());
        self.inner.on_evicted(run_id)
    }
}

fn payload(text: &str) -> Option<Payload> {
    Some(Payload {
        data: text.as_bytes().to_vec(),
        ..Default::default()
    })
}

fn publish(stream: &str, text: &str) -> WorkflowCall {
    WorkflowCall {
        call: Some(Call::Append(AppendRecordsRequest {
            stream: Some(StreamRef {
                id: stream.to_string(),
                ..Default::default()
            }),
            records: vec![AppendRecord {
                topic: String::new(),
                body: payload(text),
            }],
            ..Default::default()
        })),
        when_run_closes: false,
    }
}

fn buffer(ext: &RedisStreamExtension, run_id: &str, calls: Vec<WorkflowCall>) {
    let request = BufferWorkflowCallsRequest {
        run_id: run_id.to_string(),
        calls,
        ..Default::default()
    };
    ext.call("BufferWorkflowCalls", &request.encode_to_vec())
        .unwrap();
}

async fn call<Req: Message, Resp: Message + Default>(
    ext: &RedisStreamExtension,
    method: &str,
    request: Req,
) -> Resp {
    let bytes = ext
        .call_async(method.to_string(), request.encode_to_vec())
        .await
        .unwrap();
    Resp::decode(bytes.as_slice()).unwrap()
}

#[tokio::test]
async fn the_local_activity_heartbeat_completion_flushes_the_first_task() {
    for (variable, cluster) in [
        ("STREAMS_REDIS_URL", false),
        ("STREAMS_REDIS_CLUSTER_URL", true),
    ] {
        let Some(url) = server_url(variable) else {
            continue;
        };
        let config = RedisStreamConfig {
            urls: vec![url],
            cluster,
            key_prefix: "streams-test".to_string(),
            ..Default::default()
        };
        let inner = RedisStreamExtension::new(ExtensionOptions::from_config(config).unwrap(), None);
        let log = Arc::new(Mutex::new(vec![]));
        let namespace = Arc::new(Mutex::new(String::new()));
        let extension: Arc<dyn WorkerExtension> = Arc::new(Logged {
            hooks: inner.workflow_task_interceptor().unwrap(),
            inner: inner.clone(),
            log: log.clone(),
            namespace: namespace.clone(),
        });

        let mut t = TestHistoryBuilder::default();
        // Core heartbeats the task at 80% of its one-second timeout.
        t.add_wfe_started_with_wft_timeout(Duration::from_secs(1));
        t.add_full_wf_task();
        t.add_workflow_task_scheduled_and_started();
        let mut cfg = MockPollCfg::from_resp_batches("fakeid", t, [1, 2], mock_worker_client());
        // An empty poll stream would start shutdown before the run is evicted.
        cfg.make_poll_stream_interminable = true;
        cfg.completion_mock_fn = Some(Box::new(|_| Ok(Default::default())));
        let mut mock = build_mock_pollers(cfg);
        mock.worker_cfg(|wc| {
            wc.max_cached_workflows = 1;
            wc.extension = Some(extension);
        });
        let worker = mock_worker(mock);

        let first = worker.poll_workflow_activation().await.unwrap();
        let run_id = first.run_id.clone();
        // A fresh Stream id per server, since the mock's Workflow id is always the same.
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let stream = default_stream_id(&format!("fakeid-{unique}"), &run_id);
        let create = WorkflowCall {
            call: Some(Call::Create(CreateStreamRequest {
                stream_id: stream.clone(),
                ..Default::default()
            })),
            when_run_closes: false,
        };
        buffer(&inner, &run_id, vec![create, publish(&stream, "before")]);
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
                run_id.clone(),
                vec![schedule_local_activity_cmd(
                    1,
                    "1",
                    ActivityCancellationType::TryCancel,
                    Duration::from_secs(60),
                )],
            ))
            .await
            .unwrap();

        let la = worker.poll_activity_task().await.unwrap();
        let Some(activity_task::Variant::Start(start)) = &la.variant else {
            panic!("Expected a Local Activity start, got {la:?}");
        };
        assert!(start.is_local);
        let dispatch = start.workflow_task.expect("field 20 on a Local Activity");

        // No Activation comes, but the heartbeat completion runs the hooks and flushes.
        let resolved = {
            let wf_poll = worker.poll_workflow_activation();
            tokio::pin!(wf_poll);
            tokio::select! {
                act = &mut wf_poll => panic!("Unexpected activation {act:?}"),
                _ = tokio::time::sleep(Duration::from_millis(1500)) => {}
            }
            assert_eq!(
                *log.lock().unwrap(),
                ["before 3 after 0", "after 3 true closes=false"]
            );
            let namespace = namespace.lock().unwrap().clone();
            // The Local Activity writes as its call, with the epoch of the task that
            // dispatched it.
            let producer = Producer {
                kind: ProducerKind::LocalActivity as i32,
                id: local_activity_producer_id(&run_id, "call-1"),
                epoch: Some(ProducerEpoch {
                    task_started_event_id: dispatch.started_event_id,
                    task_attempt: dispatch.attempt as i32,
                    task_started_time: dispatch.started_time,
                    attempt: start.attempt as i32,
                    ..Default::default()
                }),
                run_id: run_id.clone(),
            };
            let _: AppendRecordsResponse = call(
                &inner,
                "AppendRecords",
                AppendRecordsRequest {
                    namespace,
                    stream: Some(StreamRef {
                        id: stream.clone(),
                        ..Default::default()
                    }),
                    producer: Some(producer),
                    sequence: 1,
                    records: vec![AppendRecord {
                        topic: String::new(),
                        body: payload("la"),
                    }],
                    ..Default::default()
                },
            )
            .await;
            worker
                .complete_activity_task(ActivityTaskCompletion {
                    task_token: la.task_token.clone(),
                    result: Some(ActivityExecutionResult::ok(vec![1].into())),
                })
                .await
                .unwrap();
            wf_poll.await.unwrap()
        };

        let close = WorkflowCall {
            call: Some(Call::Close(CloseStreamRequest {
                stream: Some(StreamRef {
                    id: stream.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            })),
            when_run_closes: true,
        };
        buffer(
            &inner,
            &resolved.run_id,
            vec![publish(&stream, "after"), close],
        );
        worker.complete_execution(&resolved.run_id).await;
        let eviction = worker.poll_workflow_activation().await.unwrap();
        assert!(matches!(
            eviction.jobs[0].variant,
            Some(workflow_activation_job::Variant::RemoveFromCache(_))
        ));
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::empty(eviction.run_id))
            .await
            .unwrap();
        worker.drain_pollers_and_shutdown().await;
        let _: temporalio_streams::proto::DrainClosesResponse =
            call(&inner, "DrainCloses", DrainClosesRequest { timeout: None }).await;

        assert_eq!(
            *log.lock().unwrap(),
            [
                "before 3 after 0",
                "after 3 true closes=false",
                "before 6 after 3",
                "after 6 true closes=true",
                "evicted",
            ]
        );
        let namespace = namespace.lock().unwrap().clone();
        let read: ReadRecordsResponse = call(
            &inner,
            "ReadRecords",
            ReadRecordsRequest {
                namespace,
                stream: Some(StreamRef {
                    id: stream.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await;
        let seen: Vec<(String, i64)> = read
            .records
            .iter()
            .map(|record| {
                let kind = StreamRecordKind::try_from(record.kind).unwrap();
                let text = match kind {
                    StreamRecordKind::Data => {
                        String::from_utf8(record.body.clone().unwrap().data).unwrap()
                    }
                    other => format!("{other:?}"),
                };
                let started = record
                    .producer
                    .as_ref()
                    .and_then(|p| p.epoch)
                    .map_or(0, |e| e.task_started_event_id);
                (text, started)
            })
            .collect();
        assert_eq!(
            seen,
            [
                ("before".to_string(), 3),
                ("la".to_string(), 3),
                ("after".to_string(), 6),
                ("Close".to_string(), 6),
            ]
        );
        assert!(read.done);
    }
}
