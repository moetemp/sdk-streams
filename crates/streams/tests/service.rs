//! The Stream service operations through the runtime's byte call, as lang sends them, on each
//! backend.

mod support;

use support::Needs;

use prost::Message;
use std::{sync::Arc, time::Duration};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    RedisStreamRuntime, StreamError,
    ids::client_producer_id,
    proto::{
        AppendRecord, AppendRecordsRequest, AppendRecordsResponse, CreateStreamRequest,
        CreateStreamResponse, Producer, ProducerEpoch, StreamErrorDetails, StreamProducerKind,
        StreamRef, StreamStatus,
    },
};
use tonic::Code;

struct Case {
    ext: Arc<RedisStreamRuntime>,
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

/// A fresh Stream on each backend that gives `needs`.
async fn cases_for(needs: Needs) -> Vec<Case> {
    support::runtimes(needs)
        .await
        .into_iter()
        .map(|runtime| {
            let unique = unique();
            Case {
                ext: runtime.ext,
                namespace: runtime.namespace.unwrap_or(format!("ns-{unique}")),
                stream: format!("s-{unique}"),
            }
        })
        .collect()
}

async fn call<Req: Message, Resp: Message + Default>(
    ext: &RedisStreamRuntime,
    method: &str,
    request: Req,
) -> Result<Resp, StreamError> {
    let bytes = ext.call(method, &request.encode_to_vec()).await?;
    Ok(Resp::decode(bytes.as_slice()).unwrap())
}

fn reason(error: &StreamError) -> String {
    StreamErrorDetails::decode(error.details().as_slice())
        .unwrap()
        .reason
}

fn client(attempt: i32) -> Producer {
    Producer {
        kind: StreamProducerKind::Client as i32,
        id: client_producer_id("me", "p"),
        epoch: Some(ProducerEpoch {
            attempt,
            ..Default::default()
        }),
        run_id: String::new(),
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
            ..Default::default()
        })
    }

    async fn create(
        &self,
        retention: Option<prost_types::Duration>,
    ) -> Result<CreateStreamResponse, StreamError> {
        call(
            &self.ext,
            "CreateStream",
            CreateStreamRequest {
                namespace: self.namespace.clone(),
                stream_id: self.stream.clone(),
                retention,
                ..Default::default()
            },
        )
        .await
    }

    async fn append(
        &self,
        producer: Producer,
        sequence: i64,
        records: Vec<AppendRecord>,
    ) -> Result<AppendRecordsResponse, StreamError> {
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
}

/// A create is idempotent for one Retention, and another Retention fails with
/// `stream_already_exists`.
#[tokio::test]
async fn a_create_is_idempotent_for_one_retention() {
    for case in cases_for(Needs::CALLS).await {
        let created = case.create(None).await.unwrap();
        assert!(created.created);
        assert_eq!(created.status, StreamStatus::Open as i32);
        assert!(!case.create(None).await.unwrap().created);
        let error = case.create(millis(1000)).await.unwrap_err();
        assert_eq!(error.code(), Code::AlreadyExists);
        assert_eq!(reason(&error), "stream_already_exists");
    }
}

/// A Producer's id must match its kind, and a Workflow appends only through a Local Activity.
#[tokio::test]
async fn a_producer_must_match_its_kind_and_not_be_a_workflow() {
    for case in cases_for(Needs::CALLS).await {
        case.create(None).await.unwrap();
        let wrong = Producer {
            kind: StreamProducerKind::Activity as i32,
            ..client(1)
        };
        let error = case
            .append(wrong, 1, vec![record("", "x")])
            .await
            .unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::InvalidArgument, "stream_invalid")
        );
        let workflow = Producer {
            kind: StreamProducerKind::Workflow as i32,
            id: temporalio_streams::ids::workflow_producer_id("r1"),
            run_id: "r1".to_string(),
            epoch: None,
        };
        let error = case.append(workflow, 1, vec![]).await.unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
    }
}

/// An unknown method fails with UNIMPLEMENTED.
#[tokio::test]
async fn an_unknown_method_is_unimplemented() {
    for case in cases_for(Needs::CALLS).await {
        let error = case.ext.call("Nope", &[]).await.unwrap_err();
        assert_eq!(error.code(), Code::Unimplemented);
    }
}
