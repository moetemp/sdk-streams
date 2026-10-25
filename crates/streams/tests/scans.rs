//! How a read scans the log, seen through a memory Store that counts the log reads and can
//! trim under a read.

use prost::Message;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    MemoryStreamRuntime, StreamError, StreamResult, StreamRuntime,
    ids::client_producer_id,
    memory::MemoryStore,
    proto::{
        AppendRecord, AppendRecordsRequest, AppendRecordsResponse, CreateStreamRequest,
        CreateStreamResponse, Producer, ProducerEpoch, ReadRecordsRequest, ReadRecordsResponse,
        StreamErrorDetails, StreamProducerKind, StreamRef,
    },
    store::{Appended, Batch, Capabilities, Created, Entry, Owner, Store, StreamState, Withdrawal},
};
use tokio::time::Instant;

const NAMESPACE: &str = "ns";
const STREAM: &str = "s";

/// The memory Store, with a count of the reads that ask for log entries. Once armed, its next
/// such read finds its first entry trimmed, as a trim just before that read leaves it.
struct Counted {
    inner: MemoryStore,
    log_reads: AtomicUsize,
    trim_next: AtomicBool,
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
        let (mut state, mut entries) = self.inner.read(namespace, stream_id, from, count).await?;
        if count > 0 && !entries.is_empty() && self.trim_next.swap(false, Ordering::Relaxed) {
            state.trimmed = entries.remove(0).offset;
        }
        Ok((state, entries))
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

fn counted() -> (Arc<Counted>, Arc<StreamRuntime>) {
    let store = Arc::new(Counted {
        inner: MemoryStore::new(Duration::from_secs(3600), Duration::from_secs(7200)),
        log_reads: AtomicUsize::new(0),
        trim_next: AtomicBool::new(false),
    });
    let ext = MemoryStreamRuntime::with_store(store.clone(), None);
    (store, ext)
}

async fn call<Req: Message, Resp: Message + Default>(
    ext: &StreamRuntime,
    method: &str,
    request: Req,
) -> Resp {
    try_call(ext, method, request).await.unwrap()
}

async fn try_call<Req: Message, Resp: Message + Default>(
    ext: &StreamRuntime,
    method: &str,
    request: Req,
) -> Result<Resp, StreamError> {
    let bytes = ext.call(method, &request.encode_to_vec()).await?;
    Ok(Resp::decode(bytes.as_slice()).unwrap())
}

async fn create(ext: &StreamRuntime) {
    let _: CreateStreamResponse = call(
        ext,
        "CreateStream",
        CreateStreamRequest {
            namespace: NAMESPACE.to_string(),
            stream_id: STREAM.to_string(),
            ..Default::default()
        },
    )
    .await;
}

fn client() -> Producer {
    Producer {
        kind: StreamProducerKind::Client as i32,
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

async fn append(
    ext: &StreamRuntime,
    sequence: i64,
    topic: &str,
    count: usize,
) -> AppendRecordsResponse {
    let records = (0..count)
        .map(|i| AppendRecord {
            topic: topic.to_string(),
            body: Some(Payload {
                data: i.to_string().into_bytes(),
                ..Default::default()
            }),
        })
        .collect();
    call(
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
    .await
}

/// A filtered page reads the log in chunks of its own, so `max_records: 1` doesn't make one
/// Store call per entry.
#[tokio::test]
async fn a_filtered_page_reads_the_log_in_chunks_whatever_its_max() {
    let (store, ext) = counted();
    create(&ext).await;
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

/// A trim between a read's state and its log read fails a read from an offset as expired, so that
/// the Reader always sees the gap. A read from the start skips the trimmed Records.
#[tokio::test]
async fn a_trim_under_a_read_from_an_offset_fails_it_as_expired() {
    let (store, ext) = counted();
    create(&ext).await;
    let first = append(&ext, 1, "a", 3).await;
    let read = |from_offset| ReadRecordsRequest {
        namespace: NAMESPACE.to_string(),
        stream: stream(),
        from_offset,
        ..Default::default()
    };
    store.trim_next.store(true, Ordering::Relaxed);
    let error = try_call::<_, ReadRecordsResponse>(&ext, "ReadRecords", read(first.first_offset))
        .await
        .unwrap_err();
    let details = StreamErrorDetails::decode(error.details().as_slice()).unwrap();
    assert_eq!(details.reason, "stream_expired");
    store.trim_next.store(true, Ordering::Relaxed);
    let from_start: ReadRecordsResponse = call(&ext, "ReadRecords", read(0)).await;
    assert_eq!(from_start.records.len(), 2);
}
