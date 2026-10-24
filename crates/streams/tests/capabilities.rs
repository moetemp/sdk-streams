//! A Store that lacks a capability: every call that needs it is refused cleanly, by name, with
//! nothing written. The memory Store stands in for such a Store with one capability masked.

mod support;

use prost::Message;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use temporalio_streams::{
    MemoryStreamExtension, StreamError, StreamExtension, StreamResult,
    ids::{client_producer_id, default_stream_id},
    memory::MemoryStore,
    proto::{
        AppendRecord, AppendRecordsRequest, CloseStreamRequest, CreateStreamRequest,
        DescribeStreamRequest, DescribeStreamResponse, Producer, ProducerEpoch, ProducerKind,
        StreamErrorDetails, StreamRef, StreamStatus,
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
        stream: &StreamRef,
        retention: Option<Duration>,
        owner: Owner,
        withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Created> {
        self.inner
            .create(namespace, stream, retention, owner, withdrawal)
            .await
    }

    async fn append(
        &self,
        namespace: &str,
        stream: &StreamRef,
        batch: &Batch<'_>,
    ) -> StreamResult<Appended> {
        self.inner.append(namespace, stream, batch).await
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
        stream: &StreamRef,
        producer: &Producer,
        record: &[u8],
    ) -> StreamResult<Closed> {
        self.inner.close(namespace, stream, producer, record).await
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
        ext: MemoryStreamExtension::with_store(store, None),
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
fn refused_capability(error: &StreamError) -> String {
    assert_eq!(error.code(), Code::Unimplemented, "{error:?}");
    let details = StreamErrorDetails::decode(error.details().as_slice()).unwrap();
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

    async fn call<Req: Message>(&self, method: &str, request: Req) -> Result<Vec<u8>, StreamError> {
        self.ext.call(method, &request.encode_to_vec()).await
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

    /// A create from a Workflow's call outside Workflow code, which names the owning run.
    async fn create_owned(&self, withdraw_run: &str) -> Result<Vec<u8>, StreamError> {
        let request = CreateStreamRequest {
            namespace: NAMESPACE.to_string(),
            stream_id: self.stream.clone(),
            withdraw_run: withdraw_run.to_string(),
            owner: Some(Producer {
                kind: ProducerKind::Workflow as i32,
                id: temporalio_streams::ids::workflow_producer_id(&self.run_id),
                run_id: self.run_id.clone(),
                ..Default::default()
            }),
            ..Default::default()
        };
        self.call("CreateStream", request).await
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
        assert_eq!(case.log_len(), 0);
    }
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
    let error = case.create_owned("").await.unwrap_err();
    assert_eq!(refused_capability(&error), "safety_ttl");
    assert!(!case.created());
}

#[tokio::test]
async fn a_retry_run_without_the_run_fence_is_refused_before_it_writes() {
    let case = case(Capabilities::RUN_FENCE);
    let error = case.create_owned("previous-run").await.unwrap_err();
    assert_eq!(refused_capability(&error), "run_fence");
    assert!(!case.created());
    // A run that is no retry needs no run fence.
    case.create_owned("").await.unwrap();
    assert!(case.created());
}

#[tokio::test]
async fn every_store_and_wrapper_says_what_it_can_do() {
    let memory = Arc::new(MemoryStore::new(
        Duration::from_secs(3600),
        Duration::from_secs(7200),
    ));
    assert_eq!(memory.capabilities(), Capabilities::ALL);
    // A wrapper forwards what its Store can do. Only the mask comes off.
    let wrapper = Masked {
        inner: memory.clone(),
        without: Capabilities::default(),
    };
    assert_eq!(wrapper.capabilities(), memory.capabilities());
    let Some(url) = support::server_url("STREAMS_REDIS_URL") else {
        return;
    };
    let redis = RedisStore::connect(RedisStoreOptions::new(url))
        .await
        .unwrap();
    assert_eq!(Store::capabilities(&redis), Capabilities::ALL);
}
