//! A Store that lacks a capability: every call that needs it is refused cleanly, by name, with
//! nothing written. The memory Store stands in for such a Store with one capability masked.

mod support;

use prost::Message;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_sdk_core::{
    ExtensionError, WorkerExtension, WorkflowTaskCompletionInfo, WorkflowTaskFailedInfo,
};
use temporalio_streams::{
    MemoryStreamExtension, StreamExtension, StreamResult,
    ids::{client_producer_id, default_stream_id},
    memory::MemoryStore,
    proto::{
        AppendRecord, AppendRecordsRequest, BufferWorkflowCallsRequest, CloseStreamRequest,
        CreateStreamRequest, DescribeStreamRequest, DescribeStreamResponse, FlushRunBufferRequest,
        Producer, ProducerEpoch, ProducerKind, RedisStreamConfig, StreamErrorDetails, StreamRef,
        StreamStatus, WorkflowCall, WorkflowTaskEpoch, workflow_call::Call,
    },
    redis::{RedisStore, RedisStoreOptions},
    store::{
        Appended, Batch, Capabilities, Closed, Created, Entry, Owner, Store, StreamState,
        Withdrawal,
    },
};
use tokio::time::Instant;
use tonic::Code;

/// A memory Store that lacks the capabilities in `without`, as a Store with fewer would.
struct Masked {
    inner: Arc<MemoryStore>,
    without: Capabilities,
}

#[async_trait::async_trait]
impl Store for Masked {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities().without(self.without)
    }

    fn default_retention(&self) -> Duration {
        self.inner.default_retention()
    }

    async fn create(
        &self,
        namespace: &str,
        stream_id: &str,
        retention: Option<Duration>,
        auto_close: Option<Duration>,
        owner: Owner,
        withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Created> {
        self.inner
            .create(
                namespace, stream_id, retention, auto_close, owner, withdrawal,
            )
            .await
    }

    async fn append(
        &self,
        namespace: &str,
        stream_id: &str,
        batch: &Batch<'_>,
    ) -> StreamResult<Appended> {
        self.inner.append(namespace, stream_id, batch).await
    }

    async fn read(
        &self,
        namespace: &str,
        stream_id: &str,
        from: i64,
        count: usize,
    ) -> StreamResult<(StreamState, Vec<Entry>)> {
        self.inner.read(namespace, stream_id, from, count).await
    }

    async fn wait_after(
        &self,
        namespace: &str,
        stream_id: &str,
        after: i64,
        deadline: Instant,
    ) -> StreamResult<bool> {
        self.inner
            .wait_after(namespace, stream_id, after, deadline)
            .await
    }

    // The masked calls still work underneath, so a call that skips the check would succeed,
    // and the tests would see it.
    async fn close(
        &self,
        namespace: &str,
        stream_id: &str,
        producer: &Producer,
        record: &[u8],
        withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Closed> {
        self.inner
            .close(namespace, stream_id, producer, record, withdrawal)
            .await
    }

    async fn auto_close(&self, namespace: &str, stream_id: &str) -> StreamResult<Option<i64>> {
        self.inner.auto_close(namespace, stream_id).await
    }
}

const NAMESPACE: &str = "ns";

struct Case {
    ext: Arc<StreamExtension>,
    memory: Arc<MemoryStore>,
    run_id: String,
    stream: String,
}

fn case(without: Capabilities) -> Case {
    let memory = Arc::new(MemoryStore::new(
        Duration::from_secs(3600),
        Duration::from_secs(7200),
    ));
    let store = Arc::new(Masked {
        inner: memory.clone(),
        without,
    });
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let run_id = format!("run-{unique}");
    Case {
        ext: MemoryStreamExtension::with_store(store, 0, None),
        memory,
        stream: default_stream_id("wf", &run_id),
        run_id,
    }
}

fn client() -> Producer {
    Producer {
        kind: ProducerKind::Client as i32,
        id: client_producer_id("me", "p"),
        epoch: Some(ProducerEpoch {
            attempt: 1,
            ..Default::default()
        }),
        run_id: String::new(),
    }
}

/// The capability an error names, after it checked the code and the reason.
fn refused_capability(error: &ExtensionError) -> String {
    assert_eq!(error.code, Code::Unimplemented, "{error:?}");
    let details = StreamErrorDetails::decode(error.details.as_slice()).unwrap();
    assert_eq!(details.reason, "stream_unsupported");
    error
        .message
        .rsplit(' ')
        .next()
        .unwrap_or_default()
        .to_string()
}

impl Case {
    fn stream_ref(&self) -> Option<StreamRef> {
        Some(StreamRef {
            id: self.stream.clone(),
            ..Default::default()
        })
    }

    async fn call<Req: Message>(
        &self,
        method: &str,
        request: Req,
    ) -> Result<Vec<u8>, ExtensionError> {
        self.ext
            .call_async(method.to_string(), request.encode_to_vec())
            .await
    }

    async fn create(&self) {
        let request = CreateStreamRequest {
            namespace: NAMESPACE.to_string(),
            stream_id: self.stream.clone(),
            ..Default::default()
        };
        self.call("CreateStream", request).await.unwrap();
    }

    fn log_len(&self) -> usize {
        self.memory.test_log_len(NAMESPACE, &self.stream)
    }

    fn created(&self) -> bool {
        self.memory
            .test_meta(NAMESPACE, &self.stream, "v")
            .is_some()
    }

    fn create_call(&self) -> WorkflowCall {
        WorkflowCall {
            call: Some(Call::Create(CreateStreamRequest {
                stream_id: self.stream.clone(),
                ..Default::default()
            })),
            when_run_closes: false,
        }
    }

    fn publish_call(&self) -> WorkflowCall {
        WorkflowCall {
            call: Some(Call::Append(AppendRecordsRequest {
                stream: self.stream_ref(),
                records: vec![AppendRecord {
                    topic: String::new(),
                    body: Some(Payload {
                        data: b"a".to_vec(),
                        ..Default::default()
                    }),
                }],
                ..Default::default()
            })),
            when_run_closes: false,
        }
    }

    fn close_call(&self) -> WorkflowCall {
        WorkflowCall {
            call: Some(Call::Close(CloseStreamRequest {
                stream: self.stream_ref(),
                ..Default::default()
            })),
            when_run_closes: false,
        }
    }

    /// One Activation's calls, then `before_complete` of the task that holds them.
    async fn flush(&self, withdraw_run: &str, calls: Vec<WorkflowCall>) -> Result<(), String> {
        let request = BufferWorkflowCallsRequest {
            run_id: self.run_id.clone(),
            withdraw_run: withdraw_run.to_string(),
            calls,
            chain_continues: false,
        };
        self.ext
            .call("BufferWorkflowCalls", &request.encode_to_vec())
            .unwrap();
        let info = WorkflowTaskCompletionInfo::builder()
            .namespace(NAMESPACE)
            .workflow_id("wf")
            .run_id(self.run_id.clone())
            .first_execution_run_id(self.run_id.clone())
            .workflow_type("t")
            .started_event_id(3)
            .attempt(1)
            .started_time(UNIX_EPOCH + Duration::from_secs(100))
            .deadline(std::time::Instant::now() + Duration::from_secs(10))
            .build();
        let hooks = self.ext.workflow_task_interceptor().unwrap();
        hooks
            .before_complete(&info)
            .await
            .map_err(|error| error.message)
    }
}

#[tokio::test]
async fn an_append_without_the_fence_or_the_flag_is_refused_with_nothing_written() {
    for (capability, name) in [
        (Capabilities::PRODUCER_FENCE, "producer_fence"),
        (Capabilities::WITHDRAWAL, "withdrawal"),
    ] {
        let case = case(capability);
        case.create().await;
        let request = AppendRecordsRequest {
            namespace: NAMESPACE.to_string(),
            stream: case.stream_ref(),
            producer: Some(client()),
            sequence: 1,
            records: vec![AppendRecord::default()],
            ..Default::default()
        };
        let error = case.call("AppendRecords", request).await.unwrap_err();
        assert_eq!(refused_capability(&error), name);
        let error = case.flush("", vec![case.publish_call()]).await.unwrap_err();
        assert!(error.ends_with(name), "{error}");
        assert_eq!(case.log_len(), 0);
    }
}

#[tokio::test]
async fn a_failed_local_activity_flush_fails_the_task_with_nothing_written() {
    let case = case(Capabilities::WITHDRAWAL);
    let request = BufferWorkflowCallsRequest {
        run_id: case.run_id.clone(),
        calls: vec![case.create_call(), case.publish_call()],
        ..Default::default()
    };
    case.ext
        .call("BufferWorkflowCalls", &request.encode_to_vec())
        .unwrap();
    let flush = FlushRunBufferRequest {
        namespace: NAMESPACE.to_string(),
        run_id: case.run_id.clone(),
        workflow_task: Some(WorkflowTaskEpoch {
            started_event_id: 3,
            attempt: 1,
            started_time: Some(prost_types::Timestamp {
                seconds: 100,
                nanos: 0,
            }),
        }),
        timeout: None,
    };
    let error = case.call("FlushRunBuffer", flush).await.unwrap_err();
    assert_eq!(refused_capability(&error), "withdrawal");
    // The completion of the task fails with the same error, though its buffer is empty.
    let error = case.flush("", vec![]).await.unwrap_err();
    assert!(error.ends_with("withdrawal"), "{error}");
    assert!(!case.created());
}

#[tokio::test]
async fn a_close_without_the_capability_is_refused_and_the_stream_stays_open() {
    let case = case(Capabilities::CLOSE);
    case.create().await;
    let request = CloseStreamRequest {
        namespace: NAMESPACE.to_string(),
        stream: case.stream_ref(),
        producer: Some(client()),
        ..Default::default()
    };
    let error = case.call("CloseStream", request).await.unwrap_err();
    assert_eq!(refused_capability(&error), "close");
    // A Workflow flush with a close fails before it writes its Records.
    let error = case
        .flush("", vec![case.publish_call(), case.close_call()])
        .await
        .unwrap_err();
    assert!(error.ends_with("close"), "{error}");
    assert_eq!(case.log_len(), 0);
    let describe = DescribeStreamRequest {
        namespace: NAMESPACE.to_string(),
        stream: case.stream_ref(),
    };
    let bytes = case.call("DescribeStream", describe).await.unwrap();
    let described = DescribeStreamResponse::decode(bytes.as_slice()).unwrap();
    assert_eq!(described.status, StreamStatus::Open as i32);
}

#[tokio::test]
async fn a_workflow_create_without_the_safety_ttl_is_refused_before_it_writes() {
    let case = case(Capabilities::SAFETY_TTL);
    let error = case
        .flush("", vec![case.create_call(), case.publish_call()])
        .await
        .unwrap_err();
    assert!(error.ends_with("safety_ttl"), "{error}");
    assert!(!case.created());
}

#[tokio::test]
async fn a_create_with_an_auto_close_is_refused_by_a_store_without_it() {
    let case = case(Capabilities::AUTO_CLOSE);
    let auto_close = Some(prost_types::Duration {
        seconds: 60,
        nanos: 0,
    });
    let request = CreateStreamRequest {
        namespace: NAMESPACE.to_string(),
        stream_id: case.stream.clone(),
        auto_close,
        ..Default::default()
    };
    let error = case.call("CreateStream", request).await.unwrap_err();
    assert_eq!(refused_capability(&error), "auto_close");
    assert!(!case.created());
    let create = WorkflowCall {
        call: Some(Call::Create(CreateStreamRequest {
            stream_id: case.stream.clone(),
            auto_close,
            ..Default::default()
        })),
        when_run_closes: false,
    };
    let error = case
        .flush("", vec![create, case.publish_call()])
        .await
        .unwrap_err();
    assert!(error.ends_with("auto_close"), "{error}");
    assert!(!case.created());
    // A create with no auto close needs no such capability.
    case.flush("", vec![case.create_call(), case.publish_call()])
        .await
        .unwrap();
    assert_eq!(case.log_len(), 1);
}

#[tokio::test]
async fn a_retry_run_without_the_run_fence_is_refused_before_it_writes() {
    let case = case(Capabilities::RUN_FENCE);
    let error = case
        .flush(
            "previous-run",
            vec![case.create_call(), case.publish_call()],
        )
        .await
        .unwrap_err();
    assert!(error.ends_with("run_fence"), "{error}");
    assert!(!case.created());
    // A run that is no retry needs no run fence.
    let case = self::case(Capabilities::RUN_FENCE);
    case.flush("", vec![case.create_call(), case.publish_call()])
        .await
        .unwrap();
    assert_eq!(case.log_len(), 1);
}

#[tokio::test]
async fn durable_writes_on_the_memory_store_fail_by_name() {
    let config = RedisStreamConfig {
        durable_writes: 1,
        ..Default::default()
    };
    let mut case = case(Capabilities::default());
    case.ext = MemoryStreamExtension::from_config_bytes(&config.encode_to_vec(), None).unwrap();
    let error = case
        .flush("", vec![case.create_call(), case.publish_call()])
        .await
        .unwrap_err();
    assert!(error.ends_with("replica_wait"), "{error}");
}

#[tokio::test]
async fn every_store_and_wrapper_says_what_it_can_do() {
    let memory = Arc::new(MemoryStore::new(
        Duration::from_secs(3600),
        Duration::from_secs(7200),
    ));
    assert_eq!(
        memory.capabilities(),
        Capabilities::ALL.without(Capabilities::REPLICA_WAIT)
    );
    // A wrapper forwards what its Store can do. Only the mask comes off.
    let wrapper = Masked {
        inner: memory.clone(),
        without: Capabilities::default(),
    };
    assert_eq!(wrapper.capabilities(), memory.capabilities());
    let Some(url) = support::server_url("STREAMS_REDIS_URL") else {
        return;
    };
    let mut options = RedisStoreOptions::new(url);
    options.durable_connection = true;
    let redis = RedisStore::connect(options).await.unwrap();
    assert_eq!(Store::capabilities(&redis), Capabilities::ALL);
    let durable = redis.durable_store().expect("a durable connection");
    assert_eq!(durable.capabilities(), Capabilities::ALL);
}

/// A memory Store whose appends take a while, so a test can act while a flush runs.
struct Slow {
    inner: Arc<MemoryStore>,
}

#[async_trait::async_trait]
impl Store for Slow {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    fn default_retention(&self) -> Duration {
        self.inner.default_retention()
    }

    async fn create(
        &self,
        namespace: &str,
        stream_id: &str,
        retention: Option<Duration>,
        auto_close: Option<Duration>,
        owner: Owner,
        withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Created> {
        self.inner
            .create(
                namespace, stream_id, retention, auto_close, owner, withdrawal,
            )
            .await
    }

    async fn append(
        &self,
        namespace: &str,
        stream_id: &str,
        batch: &Batch<'_>,
    ) -> StreamResult<Appended> {
        tokio::time::sleep(Duration::from_millis(300)).await;
        self.inner.append(namespace, stream_id, batch).await
    }

    async fn read(
        &self,
        namespace: &str,
        stream_id: &str,
        from: i64,
        count: usize,
    ) -> StreamResult<(StreamState, Vec<Entry>)> {
        self.inner.read(namespace, stream_id, from, count).await
    }

    async fn wait_after(
        &self,
        namespace: &str,
        stream_id: &str,
        after: i64,
        deadline: Instant,
    ) -> StreamResult<bool> {
        self.inner
            .wait_after(namespace, stream_id, after, deadline)
            .await
    }
}

#[tokio::test]
async fn a_task_that_fails_while_its_early_flush_runs_writes_each_record_once() {
    let mut case = case(Capabilities::default());
    case.ext = MemoryStreamExtension::with_store(
        Arc::new(Slow {
            inner: case.memory.clone(),
        }),
        0,
        None,
    );
    let request = BufferWorkflowCallsRequest {
        run_id: case.run_id.clone(),
        calls: vec![case.create_call(), case.publish_call()],
        ..Default::default()
    };
    case.ext
        .call("BufferWorkflowCalls", &request.encode_to_vec())
        .unwrap();
    let flush = FlushRunBufferRequest {
        namespace: NAMESPACE.to_string(),
        run_id: case.run_id.clone(),
        workflow_task: Some(WorkflowTaskEpoch {
            started_event_id: 3,
            attempt: 1,
            started_time: Some(prost_types::Timestamp {
                seconds: 100,
                nanos: 0,
            }),
        }),
        timeout: None,
    };
    let ext = case.ext.clone();
    let early = tokio::spawn(async move {
        ext.call_async("FlushRunBuffer".to_string(), flush.encode_to_vec())
            .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The task fails while the early write runs. Its retry buffers the same calls again.
    let hooks = case.ext.workflow_task_interceptor().unwrap();
    let failed = WorkflowTaskFailedInfo::builder()
        .run_id(case.run_id.clone())
        .attempt(1)
        .reported(true)
        .build();
    hooks.on_task_failed(&failed);
    early.await.unwrap().unwrap();
    let info = WorkflowTaskCompletionInfo::builder()
        .namespace(NAMESPACE)
        .workflow_id("wf")
        .run_id(case.run_id.clone())
        .first_execution_run_id(case.run_id.clone())
        .workflow_type("t")
        .started_event_id(3)
        .attempt(2)
        .started_time(UNIX_EPOCH + Duration::from_secs(100))
        .deadline(std::time::Instant::now() + Duration::from_secs(10))
        .build();
    case.ext
        .call("BufferWorkflowCalls", &request.encode_to_vec())
        .unwrap();
    hooks.before_complete(&info).await.unwrap();
    // One copy from the failed attempt, and one from the retry, whose entry also has the
    // `Superseded` item that drops the first copy.
    assert_eq!(case.log_len(), 2);
}
