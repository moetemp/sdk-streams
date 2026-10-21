//! The buffered writers of Activities and clients, through the byte calls, on a real Redis.

mod support;

use prost::Message;
use prost_types::Timestamp;
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use support::server_url;
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_sdk_core::{ExtensionError, WorkerExtension};
use temporalio_streams::{
    ExtensionOptions, RedisStreamExtension,
    epoch::withdraws,
    ids::{activity_producer_id, client_producer_id},
    proto::{
        AppendRecord, CloseStreamRequest, CloseWriterRequest, CloseWriterResponse,
        CreateStreamRequest, CreateStreamResponse, FlushWriterRequest, FlushWriterResponse,
        OpenWriterRequest, OpenWriterResponse, Producer, ProducerEpoch, ProducerKind,
        PublishRequest, ReadRecordsRequest, ReadRecordsResponse, RedisStreamConfig,
        StreamErrorDetails, StreamRecord, StreamRecordKind, StreamRef,
    },
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
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let case = Case {
            ext: RedisStreamExtension::new(ExtensionOptions::from_config(config).unwrap(), None),
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
            }),
            producer: Some(producer),
            ..Default::default()
        };
        change(&mut request);
        let opened: OpenWriterResponse = self.call("OpenWriter", request).await.unwrap();
        opened.writer_id
    }

    fn publish(
        &self,
        writer_id: u64,
        texts: &[&str],
        force_flush: bool,
    ) -> Result<(), ExtensionError> {
        let request = PublishRequest {
            writer_id,
            records: records(texts),
            force_flush,
        };
        self.ext
            .call("Publish", &request.encode_to_vec())
            .map(|_| ())
    }

    async fn flush(&self, writer_id: u64) -> Result<i64, ExtensionError> {
        let flushed: FlushWriterResponse = self
            .call("FlushWriter", FlushWriterRequest { writer_id })
            .await?;
        Ok(flushed.last_offset)
    }

    async fn read(&self, wait_ms: u64) -> ReadRecordsResponse {
        self.call(
            "ReadRecords",
            ReadRecordsRequest {
                namespace: self.namespace.clone(),
                stream: Some(StreamRef {
                    id: self.stream.clone(),
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
async fn a_publish_goes_out_with_the_next_batch_interval() {
    for case in cases().await {
        let writer = case
            .open(client(), |r| r.batch_interval = millis(100))
            .await;
        case.publish(writer, &["a", "b"], false).unwrap();
        assert!(case.read(0).await.records.is_empty());
        let read = case.read(3000).await;
        assert_eq!(read.records.len(), 2);
    }
}

#[tokio::test]
async fn a_forced_flush_goes_out_at_once() {
    for case in cases().await {
        let writer = case
            .open(client(), |r| r.batch_interval = millis(60_000))
            .await;
        let started = Instant::now();
        case.publish(writer, &["a"], true).unwrap();
        assert_eq!(case.read(3000).await.records.len(), 1);
        assert!(started.elapsed() < Duration::from_secs(2));
    }
}

#[tokio::test]
async fn a_flush_waits_for_the_store_and_batches_keep_their_order() {
    for case in cases().await {
        let writer = case
            .open(client(), |r| {
                r.batch_interval = millis(60_000);
                r.max_batch_size = 2;
            })
            .await;
        case.publish(writer, &["1", "2", "3"], false).unwrap();
        case.publish(writer, &["4", "5"], false).unwrap();
        let last = case.flush(writer).await.unwrap();
        let read = case.read(0).await;
        assert_eq!(read.records.last().unwrap().offset, last);
        assert_eq!(case.visible().await, ["1", "2", "3", "4", "5"]);
        // Opening the same Producer epoch again gives the same writer, so it keeps the sequence.
        assert_eq!(case.open(client(), |_| {}).await, writer);
    }
}

#[tokio::test]
async fn a_full_buffer_refuses_a_publish() {
    for case in cases().await {
        let writer = case
            .open(client(), |r| {
                r.batch_interval = millis(60_000);
                r.max_buffered_records = 2;
            })
            .await;
        case.publish(writer, &["1", "2"], false).unwrap();
        let error = case.publish(writer, &["3"], false).unwrap_err();
        assert_eq!(
            (error.code, reason(&error).as_str()),
            (Code::ResourceExhausted, "stream_buffer_full")
        );
        case.flush(writer).await.unwrap();
        case.publish(writer, &["3"], false).unwrap();
    }
}

#[tokio::test]
async fn a_failed_flush_fails_the_flush_and_every_later_publish() {
    for case in cases().await {
        let writer = case
            .open(client(), |r| r.batch_interval = millis(60_000))
            .await;
        let close = CloseStreamRequest {
            namespace: case.namespace.clone(),
            stream: Some(StreamRef {
                id: case.stream.clone(),
            }),
            producer: Some(client()),
            ..Default::default()
        };
        let _: temporalio_streams::proto::CloseStreamResponse =
            case.call("CloseStream", close).await.unwrap();
        case.publish(writer, &["late"], false).unwrap();
        let error = case.flush(writer).await.unwrap_err();
        assert_eq!(
            (error.code, reason(&error).as_str()),
            (Code::FailedPrecondition, "stream_closed")
        );
        assert_eq!(
            reason(&case.publish(writer, &["x"], false).unwrap_err()),
            "stream_closed"
        );
    }
}

#[tokio::test]
async fn an_activity_retry_withdraws_the_earlier_attempt_even_with_nothing_to_publish() {
    for case in cases().await {
        let first = case.open(activity(1), |_| {}).await;
        case.publish(first, &["a1"], false).unwrap();
        case.flush(first).await.unwrap();
        let second = case.open(activity(2), |_| {}).await;
        assert!(case.visible().await.is_empty());
        case.publish(second, &["a2"], false).unwrap();
        case.flush(second).await.unwrap();
        assert_eq!(case.visible().await, ["a2"]);
        // A late flush of the earlier attempt is fenced.
        case.publish(first, &["late"], false).unwrap();
        let error = case.flush(first).await.unwrap_err();
        assert_eq!(reason(&error), "stream_fenced");
    }
}

#[tokio::test]
async fn a_closed_writer_is_gone_after_its_last_flush() {
    for case in cases().await {
        let writer = case
            .open(client(), |r| r.batch_interval = millis(60_000))
            .await;
        case.publish(writer, &["last"], false).unwrap();
        let closed: CloseWriterResponse = case
            .call("CloseWriter", CloseWriterRequest { writer_id: writer })
            .await
            .unwrap();
        assert!(closed.last_offset > 0);
        assert_eq!(case.visible().await, ["last"]);
        let error = case.publish(writer, &["x"], false).unwrap_err();
        assert_eq!(
            (error.code, reason(&error).as_str()),
            (Code::NotFound, "stream_unknown_writer")
        );
    }
}

#[tokio::test]
async fn a_flush_splits_big_records_by_bytes_and_refuses_an_oversize_one() {
    for case in cases().await {
        let writer = case
            .open(client(), |r| r.batch_interval = millis(60_000))
            .await;
        let big = "x".repeat(1_400_000);
        case.publish(writer, &[&big, &big, &big, "end"], false)
            .unwrap();
        case.flush(writer).await.unwrap();
        assert_eq!(case.visible().await.len(), 4);
        let huge = "y".repeat(1_700_000);
        let error = case.publish(writer, &[&huge], false).unwrap_err();
        assert_eq!(
            (error.code, reason(&error).as_str()),
            (Code::InvalidArgument, "stream_too_large")
        );
    }
}

#[tokio::test]
async fn a_reopen_of_a_closed_writer_goes_on_with_its_sequence() {
    for case in cases().await {
        let writer = case
            .open(client(), |r| r.batch_interval = millis(60_000))
            .await;
        case.publish(writer, &["1"], false).unwrap();
        let _: CloseWriterResponse = case
            .call("CloseWriter", CloseWriterRequest { writer_id: writer })
            .await
            .unwrap();
        let again = case
            .open(client(), |r| r.batch_interval = millis(60_000))
            .await;
        assert_ne!(again, writer);
        case.publish(again, &["2"], false).unwrap();
        case.flush(again).await.unwrap();
        assert_eq!(case.visible().await, ["1", "2"]);
    }
}

#[tokio::test]
async fn closing_every_writer_flushes_them_all() {
    for case in cases().await {
        let a = case
            .open(client(), |r| r.batch_interval = millis(60_000))
            .await;
        let b = case
            .open(activity(1), |r| r.batch_interval = millis(60_000))
            .await;
        case.publish(a, &["a"], false).unwrap();
        case.publish(b, &["b"], false).unwrap();
        let closed: temporalio_streams::proto::CloseWritersResponse = case
            .call(
                "CloseWriters",
                temporalio_streams::proto::CloseWritersRequest {},
            )
            .await
            .unwrap();
        assert_eq!(closed.failed, 0);
        let mut texts = case.visible().await;
        texts.sort();
        assert_eq!(texts, ["a", "b"]);
        assert!(case.publish(a, &["x"], false).is_err());
    }
}
