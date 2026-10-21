//! The five operations through the extension's byte call, as lang sends them, on a real Redis.

mod support;

use prost::Message;
use prost_types::Timestamp;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use support::server_url;
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_sdk_core::{ExtensionError, WorkerExtension};
use temporalio_streams::{
    ExtensionOptions, RedisStreamExtension,
    ids::{activity_producer_id, client_producer_id},
    proto::{
        AppendRecord, AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest,
        CloseStreamResponse, CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest,
        DescribeStreamResponse, Producer, ProducerEpoch, ProducerKind, ReadRecordsRequest,
        ReadRecordsResponse, ReadStart, RedisStreamConfig, StreamErrorDetails, StreamRecordKind,
        StreamRef, StreamStatus,
    },
};
use tonic::Code;

struct Case {
    ext: Arc<RedisStreamExtension>,
    namespace: String,
    stream: String,
}

static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn unique() -> String {
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{now}-{n}")
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
        let options = ExtensionOptions::from_config(config).unwrap();
        let unique = unique();
        cases.push(Case {
            ext: RedisStreamExtension::new(options, None),
            namespace: format!("ns-{unique}"),
            stream: format!("s-{unique}"),
        });
    }
    cases
}

async fn call<Req: Message, Resp: Message + Default>(
    ext: &RedisStreamExtension,
    method: &str,
    request: Req,
) -> Result<Resp, ExtensionError> {
    let bytes = ext
        .call_async(method.to_string(), request.encode_to_vec())
        .await?;
    Ok(Resp::decode(bytes.as_slice()).unwrap())
}

fn reason(error: &ExtensionError) -> String {
    StreamErrorDetails::decode(error.details.as_slice())
        .unwrap()
        .reason
}

fn client(attempt: i32) -> Producer {
    Producer {
        kind: ProducerKind::Client as i32,
        id: client_producer_id("me", "p"),
        epoch: Some(ProducerEpoch {
            attempt,
            ..Default::default()
        }),
        run_id: String::new(),
    }
}

fn activity(attempt: i32, scheduled: i64) -> Producer {
    Producer {
        kind: ProducerKind::Activity as i32,
        id: activity_producer_id("r1", "a", "s"),
        epoch: Some(ProducerEpoch {
            attempt_scheduled_time: Some(Timestamp {
                seconds: scheduled,
                nanos: 0,
            }),
            attempt,
            ..Default::default()
        }),
        run_id: "r1".to_string(),
    }
}

fn record(topic: &str, text: &str) -> AppendRecord {
    AppendRecord {
        topic: topic.to_string(),
        body: Some(Payload {
            data: text.as_bytes().to_vec(),
            ..Default::default()
        }),
    }
}

fn millis(ms: u64) -> Option<prost_types::Duration> {
    let duration = Duration::from_millis(ms);
    Some(prost_types::Duration {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    })
}

impl Case {
    fn stream(&self) -> Option<StreamRef> {
        Some(StreamRef {
            id: self.stream.clone(),
        })
    }

    async fn create(
        &self,
        retention: Option<prost_types::Duration>,
    ) -> Result<CreateStreamResponse, ExtensionError> {
        call(
            &self.ext,
            "CreateStream",
            CreateStreamRequest {
                namespace: self.namespace.clone(),
                stream_id: self.stream.clone(),
                retention,
                withdraw_run: String::new(),
            },
        )
        .await
    }

    async fn append(
        &self,
        producer: Producer,
        sequence: i64,
        records: Vec<AppendRecord>,
    ) -> Result<AppendRecordsResponse, ExtensionError> {
        call(
            &self.ext,
            "AppendRecords",
            AppendRecordsRequest {
                namespace: self.namespace.clone(),
                stream: self.stream(),
                producer: Some(producer),
                sequence,
                records,
                ..Default::default()
            },
        )
        .await
    }

    async fn read(
        &self,
        request: ReadRecordsRequest,
    ) -> Result<ReadRecordsResponse, ExtensionError> {
        call(
            &self.ext,
            "ReadRecords",
            ReadRecordsRequest {
                namespace: self.namespace.clone(),
                stream: self.stream(),
                ..request
            },
        )
        .await
    }

    async fn describe(&self) -> Result<DescribeStreamResponse, ExtensionError> {
        call(
            &self.ext,
            "DescribeStream",
            DescribeStreamRequest {
                namespace: self.namespace.clone(),
                stream: self.stream(),
            },
        )
        .await
    }

    async fn close(&self, producer: Producer) -> Result<CloseStreamResponse, ExtensionError> {
        call(
            &self.ext,
            "CloseStream",
            CloseStreamRequest {
                namespace: self.namespace.clone(),
                stream: self.stream(),
                producer: Some(producer),
                result: Some(Payload {
                    data: b"done".to_vec(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
    }
}

fn texts(response: &ReadRecordsResponse) -> Vec<String> {
    response
        .records
        .iter()
        .map(
            |record| match StreamRecordKind::try_from(record.kind).unwrap() {
                StreamRecordKind::Data => {
                    String::from_utf8(record.body.clone().unwrap().data).unwrap()
                }
                StreamRecordKind::Superseded => "<superseded>".to_string(),
                StreamRecordKind::Close => "<close>".to_string(),
                StreamRecordKind::Idle => "<idle>".to_string(),
                StreamRecordKind::Unspecified => "?".to_string(),
            },
        )
        .collect()
}

#[tokio::test]
async fn a_create_is_idempotent_for_one_retention() {
    for case in cases().await {
        let created = case.create(None).await.unwrap();
        assert!(created.created);
        assert_eq!(created.status, StreamStatus::Open as i32);
        assert!(!case.create(None).await.unwrap().created);
        let error = case.create(millis(1000)).await.unwrap_err();
        assert_eq!(error.code, Code::AlreadyExists);
        assert_eq!(reason(&error), "stream_already_exists");
    }
}

#[tokio::test]
async fn a_producer_must_match_its_kind_and_not_be_a_workflow() {
    for case in cases().await {
        case.create(None).await.unwrap();
        let wrong = Producer {
            kind: ProducerKind::Activity as i32,
            ..client(1)
        };
        let error = case
            .append(wrong, 1, vec![record("", "x")])
            .await
            .unwrap_err();
        assert_eq!(
            (error.code, reason(&error).as_str()),
            (Code::InvalidArgument, "stream_invalid")
        );
        let workflow = Producer {
            kind: ProducerKind::Workflow as i32,
            id: temporalio_streams::ids::workflow_producer_id("r1"),
            run_id: "r1".to_string(),
            epoch: None,
        };
        let error = case.append(workflow, 1, vec![]).await.unwrap_err();
        assert_eq!(error.code, Code::InvalidArgument);
    }
}

#[tokio::test]
async fn a_read_is_inclusive_from_its_offset_and_get_offset_reads_only_new_records() {
    for case in cases().await {
        case.create(None).await.unwrap();
        let first = case
            .append(client(1), 1, vec![record("a", "1"), record("b", "2")])
            .await
            .unwrap();
        let all = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&all), ["1", "2"]);
        assert_eq!(all.records[0].offset, first.first_offset);
        assert_eq!(all.next_offset, first.last_offset + 1);
        let from = case
            .read(ReadRecordsRequest {
                from_offset: first.last_offset,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(texts(&from), ["2"]);
        let next = case.describe().await.unwrap().last_offset + 1;
        let none = case
            .read(ReadRecordsRequest {
                from_offset: next,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(none.records.is_empty());
        assert_eq!(none.next_offset, next);
        let end = case
            .read(ReadRecordsRequest {
                start: ReadStart::End as i32,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(end.next_offset, next);
        let error = case
            .read(ReadRecordsRequest {
                from_offset: next + 1,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(
            (error.code, reason(&error).as_str()),
            (Code::InvalidArgument, "stream_offset")
        );
    }
}

#[tokio::test]
async fn a_topic_filter_keeps_superseded_items() {
    for case in cases().await {
        case.create(None).await.unwrap();
        case.append(activity(1, 10), 1, vec![record("a", "old")])
            .await
            .unwrap();
        case.append(
            activity(2, 20),
            1,
            vec![record("b", "b1"), record("a", "a1")],
        )
        .await
        .unwrap();
        let all = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&all), ["old", "<superseded>", "b1", "a1"]);
        let superseded = &all.records[1];
        assert_eq!(superseded.offset, all.records[2].offset);
        let supersession = superseded.supersession.as_ref().unwrap();
        assert_eq!(supersession.producer_id, activity(2, 20).id);
        let only_a = case
            .read(ReadRecordsRequest {
                topics: vec!["a".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(texts(&only_a), ["old", "<superseded>", "a1"]);
    }
}

#[tokio::test]
async fn a_read_waits_for_the_create_only_as_long_as_create_wait() {
    for case in cases().await {
        let error = case.read(ReadRecordsRequest::default()).await.unwrap_err();
        assert_eq!(
            (error.code, reason(&error).as_str()),
            (Code::NotFound, "stream_not_found")
        );
        let started = Instant::now();
        let error = case
            .read(ReadRecordsRequest {
                create_wait: millis(200),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(error.code, Code::NotFound);
        assert!(started.elapsed() >= Duration::from_millis(200));
        let ext = case.ext.clone();
        let (namespace, stream) = (case.namespace.clone(), case.stream.clone());
        let creator = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let request = CreateStreamRequest {
                namespace,
                stream_id: stream,
                ..Default::default()
            };
            call::<_, CreateStreamResponse>(&ext, "CreateStream", request).await
        });
        let read = case
            .read(ReadRecordsRequest {
                create_wait: millis(3000),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(read.records.is_empty());
        creator.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn a_quiet_read_ends_with_an_idle_record_and_a_long_poll_wakes_on_append() {
    for case in cases().await {
        case.create(None).await.unwrap();
        let first = case
            .append(client(1), 1, vec![record("", "1")])
            .await
            .unwrap();
        let from = first.last_offset + 1;
        let started = Instant::now();
        let idle = case
            .read(ReadRecordsRequest {
                from_offset: from,
                wait: millis(5000),
                idle_timeout: millis(200),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(texts(&idle), ["<idle>"]);
        assert_eq!(idle.records[0].offset, from);
        assert!(!idle.done);
        let ext = case.ext.clone();
        let (namespace, stream) = (case.namespace.clone(), case.stream.clone());
        let writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let request = AppendRecordsRequest {
                namespace,
                stream: Some(StreamRef { id: stream }),
                producer: Some(client(1)),
                sequence: 2,
                records: vec![record("", "2")],
                ..Default::default()
            };
            call::<_, AppendRecordsResponse>(&ext, "AppendRecords", request).await
        });
        let woke = case
            .read(ReadRecordsRequest {
                from_offset: from,
                wait: millis(5000),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(texts(&woke), ["2"]);
        writer.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn a_close_ends_every_read_with_its_record() {
    for case in cases().await {
        case.create(None).await.unwrap();
        case.append(client(1), 1, vec![record("", "1")])
            .await
            .unwrap();
        case.close(client(1)).await.unwrap();
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&read), ["1", "<close>"]);
        assert!(read.done);
        assert_eq!(read.records[1].close_result.as_ref().unwrap().data, b"done");
        let again = case
            .read(ReadRecordsRequest {
                from_offset: read.next_offset,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(texts(&again), ["<close>"]);
        assert!(again.done);
        assert_eq!(
            case.describe().await.unwrap().status,
            StreamStatus::Closed as i32
        );
        let error = case
            .append(client(1), 2, vec![record("", "2")])
            .await
            .unwrap_err();
        assert_eq!(
            (error.code, reason(&error).as_str()),
            (Code::FailedPrecondition, "stream_closed")
        );
        let created = case.create(None).await.unwrap();
        assert_eq!(created.status, StreamStatus::Closed as i32);
    }
}

#[tokio::test]
async fn a_read_from_a_trimmed_offset_fails_as_expired() {
    for case in cases().await {
        case.create(millis(200)).await.unwrap();
        // Each write keeps the client Stream one more Retention, so only the old Record goes.
        let old = case
            .append(client(1), 1, vec![record("", "old")])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
        case.append(client(1), 2, vec![record("", "mid")])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
        case.append(client(1), 3, vec![record("", "new")])
            .await
            .unwrap();
        let error = case
            .read(ReadRecordsRequest {
                from_offset: old.first_offset,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(
            (error.code, reason(&error).as_str()),
            (Code::NotFound, "stream_expired")
        );
        let from_start = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert!(!texts(&from_start).contains(&"old".to_string()));
    }
}

#[tokio::test]
async fn an_unknown_method_is_unimplemented() {
    for case in cases().await {
        let error = case
            .ext
            .call_async("Nope".to_string(), vec![])
            .await
            .unwrap_err();
        assert_eq!(error.code, Code::Unimplemented);
        assert_eq!(
            case.ext.call("Nope", &[]).unwrap_err().code,
            Code::Unimplemented
        );
    }
}

#[tokio::test]
async fn a_read_of_an_expired_open_stream_fails_instead_of_waiting() {
    for case in cases().await {
        case.create(millis(300)).await.unwrap();
        case.append(client(1), 1, vec![record("", "1")])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        for from_offset in [0, 1] {
            let started = Instant::now();
            let error = case
                .read(ReadRecordsRequest {
                    from_offset,
                    wait: millis(5000),
                    ..Default::default()
                })
                .await
                .unwrap_err();
            assert_eq!(reason(&error), "stream_expired");
            assert!(started.elapsed() < Duration::from_secs(2));
        }
        let error = case.append(client(1), 0, vec![]).await.unwrap_err();
        assert_eq!(reason(&error), "stream_invalid");
    }
}
