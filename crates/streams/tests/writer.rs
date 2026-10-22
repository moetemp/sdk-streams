//! The buffered writers of Activities and clients, through the byte calls, on a real Redis.

mod support;

use prost::Message;
use prost_types::Timestamp;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_sdk_core::{ExtensionError, WorkerExtension};
use temporalio_streams::{
    MemoryStreamExtension, RedisStreamExtension, StreamError, StreamResult,
    epoch::withdraws,
    ids::{activity_producer_id, client_producer_id},
    memory::MemoryStore,
    proto::{
        AppendRecord, CloseStreamRequest, CloseWriterRequest, CloseWriterResponse,
        CreateStreamRequest, CreateStreamResponse, OpenWriterRequest, OpenWriterResponse, Producer,
        ProducerEpoch, ProducerKind, ReadRecordsRequest, ReadRecordsResponse, StreamErrorDetails,
        StreamRecord, StreamRecordKind, StreamRef, WriteRecordsRequest, WriteRecordsResponse,
    },
    store::{Appended, Batch, Capabilities, Created, Entry, Owner, Store, StreamState, Withdrawal},
};
use tonic::Code;

static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct Case {
    ext: Arc<RedisStreamExtension>,
    namespace: String,
    stream: String,
}

async fn cases() -> Vec<Case> {
    let mut cases = vec![];
    for ext in support::extensions(|_| {}, true) {
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let case = Case {
            ext,
            namespace: format!("ns-{now}-{n}"),
            stream: format!("s-{now}-{n}"),
        };
        let created: CreateStreamResponse = case
            .call(
                "CreateStream",
                CreateStreamRequest {
                    namespace: case.namespace.clone(),
                    stream_id: case.stream.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(created.created);
        cases.push(case);
    }
    cases
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

fn activity(attempt: i32) -> Producer {
    Producer {
        kind: ProducerKind::Activity as i32,
        id: activity_producer_id("r1", "a", "s"),
        epoch: Some(ProducerEpoch {
            attempt_scheduled_time: Some(Timestamp {
                seconds: 10 * attempt as i64,
                nanos: 0,
            }),
            attempt,
            ..Default::default()
        }),
        run_id: "r1".to_string(),
    }
}

fn records(texts: &[&str]) -> Vec<AppendRecord> {
    texts
        .iter()
        .map(|text| AppendRecord {
            topic: String::new(),
            body: Some(Payload {
                data: text.as_bytes().to_vec(),
                ..Default::default()
            }),
        })
        .collect()
}

fn reason(error: &ExtensionError) -> String {
    StreamErrorDetails::decode(error.details.as_slice())
        .unwrap()
        .reason
}

fn millis(ms: u64) -> Option<prost_types::Duration> {
    Some(prost_types::Duration {
        seconds: (ms / 1000) as i64,
        nanos: ((ms % 1000) * 1_000_000) as i32,
    })
}

impl Case {
    async fn call<Req: Message, Resp: Message + Default>(
        &self,
        method: &str,
        request: Req,
    ) -> Result<Resp, ExtensionError> {
        let bytes = self
            .ext
            .call_async(method.to_string(), request.encode_to_vec())
            .await?;
        Ok(Resp::decode(bytes.as_slice()).unwrap())
    }

    async fn open(&self, producer: Producer, change: impl Fn(&mut OpenWriterRequest)) -> u64 {
        let mut request = OpenWriterRequest {
            namespace: self.namespace.clone(),
            stream: Some(StreamRef {
                id: self.stream.clone(),
                ..Default::default()
            }),
            producer: Some(producer),
            ..Default::default()
        };
        change(&mut request);
        let opened: OpenWriterResponse = self.call("OpenWriter", request).await.unwrap();
        opened.writer_id
    }

    /// An awaited publish: the Records are in the Store when it returns.
    async fn write(&self, writer_id: u64, texts: &[&str]) -> Result<(i64, i64), ExtensionError> {
        let written: WriteRecordsResponse = self
            .call(
                "WriteRecords",
                WriteRecordsRequest {
                    writer_id,
                    records: records(texts),
                },
            )
            .await?;
        Ok((written.first_offset, written.last_offset))
    }

    async fn read(&self, wait_ms: u64) -> ReadRecordsResponse {
        self.call(
            "ReadRecords",
            ReadRecordsRequest {
                namespace: self.namespace.clone(),
                stream: Some(StreamRef {
                    id: self.stream.clone(),
                    ..Default::default()
                }),
                wait: millis(wait_ms),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    }

    async fn visible(&self) -> Vec<String> {
        let mut kept: Vec<StreamRecord> = vec![];
        for record in self.read(0).await.records {
            match StreamRecordKind::try_from(record.kind).unwrap() {
                StreamRecordKind::Superseded => {
                    let s = record.supersession.unwrap();
                    kept.retain(|old| !withdraws(&s, old));
                }
                StreamRecordKind::Data => kept.push(record),
                _ => {}
            }
        }
        kept.into_iter()
            .map(|record| String::from_utf8(record.body.unwrap().data).unwrap())
            .collect()
    }
}

#[tokio::test]
async fn a_write_is_in_the_store_when_it_answers_its_offsets() {
    for case in cases().await {
        let writer = case.open(client(), |_| {}).await;
        let (first, last) = case.write(writer, &["a", "b"]).await.unwrap();
        assert!(0 < first && first < last);
        let read = case.read(0).await;
        let offsets: Vec<i64> = read.records.iter().map(|r| r.offset).collect();
        assert_eq!(offsets, [first, last]);
        let (one, same) = case.write(writer, &["c"]).await.unwrap();
        assert_eq!(one, same);
        assert!(one > last);
        assert_eq!(case.write(writer, &[]).await.unwrap(), (0, 0));
        assert_eq!(case.visible().await, ["a", "b", "c"]);
        let closed: CloseWriterResponse = case
            .call("CloseWriter", CloseWriterRequest { writer_id: writer })
            .await
            .unwrap();
        assert_eq!(closed.last_offset, one);
    }
}

#[tokio::test]
async fn writes_split_at_the_batch_size_and_keep_one_sequence() {
    for case in cases().await {
        let writer = case.open(client(), |r| r.max_batch_size = 2).await;
        let (first, last) = case.write(writer, &["1", "2", "3"]).await.unwrap();
        case.write(writer, &["4", "5"]).await.unwrap();
        let read = case.read(0).await;
        assert_eq!(case.visible().await, ["1", "2", "3", "4", "5"]);
        assert_eq!(read.records[0].offset, first);
        assert_eq!(read.records[2].offset, last);
        // Opening the same Producer epoch again gives the same writer, so it keeps the sequence.
        assert_eq!(case.open(client(), |_| {}).await, writer);
    }
}

#[tokio::test]
async fn a_failed_write_fails_every_later_write_and_the_close() {
    for case in cases().await {
        let writer = case.open(client(), |_| {}).await;
        let close = CloseStreamRequest {
            namespace: case.namespace.clone(),
            stream: Some(StreamRef {
                id: case.stream.clone(),
                ..Default::default()
            }),
            producer: Some(client()),
            ..Default::default()
        };
        let _: temporalio_streams::proto::CloseStreamResponse =
            case.call("CloseStream", close).await.unwrap();
        let error = case.write(writer, &["late"]).await.unwrap_err();
        assert_eq!(
            (error.code, reason(&error).as_str()),
            (Code::FailedPrecondition, "stream_closed")
        );
        assert_eq!(
            reason(&case.write(writer, &["x"]).await.unwrap_err()),
            "stream_closed"
        );
        let error = case
            .call::<_, CloseWriterResponse>("CloseWriter", CloseWriterRequest { writer_id: writer })
            .await
            .unwrap_err();
        assert_eq!(reason(&error), "stream_closed");
    }
}

#[tokio::test]
async fn an_activity_retry_withdraws_the_earlier_attempt_even_with_nothing_to_write() {
    for case in cases().await {
        let first = case.open(activity(1), |_| {}).await;
        case.write(first, &["a1"]).await.unwrap();
        case.open(activity(2), |_| {}).await;
        assert!(case.visible().await.is_empty());
    }
}

#[tokio::test]
async fn a_closed_writer_is_gone() {
    for case in cases().await {
        let writer = case.open(client(), |_| {}).await;
        case.write(writer, &["last"]).await.unwrap();
        let closed: CloseWriterResponse = case
            .call("CloseWriter", CloseWriterRequest { writer_id: writer })
            .await
            .unwrap();
        assert!(closed.last_offset > 0);
        let error = case.write(writer, &["x"]).await.unwrap_err();
        assert_eq!(
            (error.code, reason(&error).as_str()),
            (Code::NotFound, "stream_unknown_writer")
        );
    }
}

#[tokio::test]
async fn a_write_splits_big_records_by_bytes_and_refuses_an_oversize_one() {
    for case in cases().await {
        let writer = case.open(client(), |_| {}).await;
        let big = "x".repeat(1_400_000);
        case.write(writer, &[&big, &big, &big, "end"])
            .await
            .unwrap();
        assert_eq!(case.visible().await.len(), 4);
        let huge = "y".repeat(1_700_000);
        let error = case.write(writer, &[&huge]).await.unwrap_err();
        assert_eq!(
            (error.code, reason(&error).as_str()),
            (Code::InvalidArgument, "stream_too_large")
        );
        // A refusal before the Store leaves the writer usable.
        case.write(writer, &["ok"]).await.unwrap();
    }
}

#[tokio::test]
async fn a_reopen_of_a_closed_writer_goes_on_with_its_sequence() {
    for case in cases().await {
        let writer = case.open(client(), |_| {}).await;
        case.write(writer, &["1"]).await.unwrap();
        let _: CloseWriterResponse = case
            .call("CloseWriter", CloseWriterRequest { writer_id: writer })
            .await
            .unwrap();
        let again = case.open(client(), |_| {}).await;
        assert_ne!(again, writer);
        case.write(again, &["2"]).await.unwrap();
        assert_eq!(case.visible().await, ["1", "2"]);
    }
}

#[tokio::test]
async fn closing_every_writer_drops_them_all() {
    for case in cases().await {
        let a = case.open(client(), |_| {}).await;
        let b = case.open(activity(1), |_| {}).await;
        case.write(a, &["a"]).await.unwrap();
        case.write(b, &["b"]).await.unwrap();
        let closed: temporalio_streams::proto::CloseWritersResponse = case
            .call(
                "CloseWriters",
                temporalio_streams::proto::CloseWritersRequest {},
            )
            .await
            .unwrap();
        assert_eq!(closed.failed, 0);
        assert!(case.write(a, &["x"]).await.is_err());
        assert!(case.write(b, &["x"]).await.is_err());
    }
}

#[tokio::test]
async fn the_removed_buffered_calls_are_unknown() {
    for case in cases().await {
        let error = case.ext.call("Publish", &[]).unwrap_err();
        assert!(error.message.contains("Publish"), "{error:?}");
        let error = case
            .ext
            .call_async("FlushWriter".to_string(), vec![])
            .await
            .unwrap_err();
        assert!(error.message.contains("FlushWriter"), "{error:?}");
    }
}

#[tokio::test]
async fn a_write_of_a_retry_attempt_withdraws_the_earlier_attempt() {
    for case in cases().await {
        let first = case.open(activity(1), |_| {}).await;
        case.write(first, &["x"]).await.unwrap();
        let second = case.open(activity(2), |_| {}).await;
        case.write(second, &["y"]).await.unwrap();
        assert_eq!(case.visible().await, ["y"]);
        // The earlier attempt is fenced now.
        let error = case.write(first, &["late"]).await.unwrap_err();
        assert_eq!(reason(&error), "stream_fenced");
        let error = case.write(first, &["after"]).await.unwrap_err();
        assert_eq!(reason(&error), "stream_fenced");
    }
}

/// A memory Store whose first append goes in but whose answer is lost, as on a dropped
/// connection.
struct LosesFirstAnswer {
    inner: Arc<MemoryStore>,
    lost: AtomicBool,
}

#[async_trait::async_trait]
impl Store for LosesFirstAnswer {
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
        auto_close: Option<Duration>,
        owner: Owner<'_>,
        withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Created> {
        self.inner
            .create(namespace, stream, retention, auto_close, owner, withdrawal)
            .await
    }

    async fn append(
        &self,
        namespace: &str,
        stream: &StreamRef,
        batch: &Batch<'_>,
    ) -> StreamResult<Appended> {
        let appended = self.inner.append(namespace, stream, batch).await?;
        if !self.lost.swap(true, Ordering::SeqCst) {
            return Err(StreamError::storage("the answer was lost"));
        }
        Ok(appended)
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
        deadline: tokio::time::Instant,
    ) -> StreamResult<bool> {
        self.inner
            .wait_after(namespace, stream_id, after, deadline)
            .await
    }
}

#[tokio::test]
async fn a_write_whose_answer_is_lost_goes_in_once() {
    let memory = Arc::new(MemoryStore::new(
        Duration::from_secs(3600),
        Duration::from_secs(7200),
    ));
    let store = Arc::new(LosesFirstAnswer {
        inner: memory,
        lost: AtomicBool::new(false),
    });
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let case = Case {
        ext: MemoryStreamExtension::with_store(store, 0, None),
        namespace: format!("ns-lost-{n}"),
        stream: format!("s-lost-{n}"),
    };
    let _: CreateStreamResponse = case
        .call(
            "CreateStream",
            CreateStreamRequest {
                namespace: case.namespace.clone(),
                stream_id: case.stream.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let writer = case.open(client(), |_| {}).await;
    let (first, last) = case.write(writer, &["a", "b"]).await.unwrap();
    let read = case.read(0).await;
    assert_eq!(case.visible().await, ["a", "b"]);
    assert_eq!(
        (read.records[0].offset, read.records[1].offset),
        (first, last)
    );
    case.write(writer, &["c"]).await.unwrap();
    assert_eq!(case.visible().await, ["a", "b", "c"]);
}

#[tokio::test]
async fn a_write_that_waits_behind_a_close_is_refused() {
    for case in cases().await {
        let writer = case.open(client(), |_| {}).await;
        case.write(writer, &["a"]).await.unwrap();
        let close = case.call::<_, CloseWriterResponse>(
            "CloseWriter",
            CloseWriterRequest { writer_id: writer },
        );
        let write = async {
            tokio::task::yield_now().await;
            case.write(writer, &["late"]).await
        };
        let (closed, written) = tokio::join!(close, write);
        closed.unwrap();
        assert_eq!(reason(&written.unwrap_err()), "stream_unknown_writer");
        assert_eq!(case.visible().await, ["a"]);
    }
}
