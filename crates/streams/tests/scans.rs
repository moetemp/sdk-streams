//! How a read scans the log, seen through a memory Store that counts the log reads.

use prost::Message;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    MemoryStreamExtension, StreamExtension, StreamResult,
    ids::client_producer_id,
    memory::MemoryStore,
    proto::{
        AppendRecord, AppendRecordsRequest, AppendRecordsResponse, CreateStreamRequest,
        CreateStreamResponse, Producer, ProducerEpoch, ProducerKind, ReadRecordsRequest,
        ReadRecordsResponse, StreamRef,
    },
    store::{Appended, Batch, Capabilities, Created, Entry, Owner, Store, StreamState, Withdrawal},
};
use tokio::time::Instant;

const NAMESPACE: &str = "ns";
const STREAM: &str = "s";

/// The memory Store, with a count of the reads that ask for log entries.
struct Counted {
    inner: MemoryStore,
    log_reads: AtomicUsize,
}

#[async_trait::async_trait]
impl Store for Counted {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
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
        if count > 0 {
            self.log_reads.fetch_add(1, Ordering::Relaxed);
        }
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

fn counted() -> (Arc<Counted>, Arc<StreamExtension>) {
    let store = Arc::new(Counted {
        inner: MemoryStore::new(Duration::from_secs(3600), Duration::from_secs(7200)),
        log_reads: AtomicUsize::new(0),
    });
    let ext = MemoryStreamExtension::with_store(store.clone(), None);
    (store, ext)
}

async fn call<Req: Message, Resp: Message + Default>(
    ext: &StreamExtension,
    method: &str,
    request: Req,
) -> Resp {
    let bytes = ext.call(method, &request.encode_to_vec()).await.unwrap();
    Resp::decode(bytes.as_slice()).unwrap()
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

fn stream() -> Option<StreamRef> {
    Some(StreamRef {
        id: STREAM.to_string(),
        ..Default::default()
    })
}

async fn append(ext: &StreamExtension, sequence: i64, topic: &str, count: usize) {
    let records = (0..count)
        .map(|i| AppendRecord {
            topic: topic.to_string(),
            body: Some(Payload {
                data: i.to_string().into_bytes(),
                ..Default::default()
            }),
        })
        .collect();
    let _: AppendRecordsResponse = call(
        ext,
        "AppendRecords",
        AppendRecordsRequest {
            namespace: NAMESPACE.to_string(),
            stream: stream(),
            producer: Some(client()),
            sequence,
            records,
            ..Default::default()
        },
    )
    .await;
}

/// A filtered page reads the log in chunks of its own, so `max_records: 1` doesn't make one
/// Store call per entry.
#[tokio::test]
async fn a_filtered_page_reads_the_log_in_chunks_whatever_its_max() {
    let (store, ext) = counted();
    let _: CreateStreamResponse = call(
        &ext,
        "CreateStream",
        CreateStreamRequest {
            namespace: NAMESPACE.to_string(),
            stream_id: STREAM.to_string(),
            ..Default::default()
        },
    )
    .await;
    for sequence in 1..=5 {
        append(&ext, sequence, "a", 1000).await;
    }
    append(&ext, 6, "b", 1).await;
    store.log_reads.store(0, Ordering::Relaxed);
    let read: ReadRecordsResponse = call(
        &ext,
        "ReadRecords",
        ReadRecordsRequest {
            namespace: NAMESPACE.to_string(),
            stream: stream(),
            topics: vec!["b".to_string()],
            max_records: 1,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(read.records.len(), 1);
    let reads = store.log_reads.load(Ordering::Relaxed);
    assert!(reads <= 10, "{reads} log reads");
}
