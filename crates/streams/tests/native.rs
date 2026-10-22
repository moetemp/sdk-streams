//! The native Store against a server with the Stream service, through the extension's byte
//! calls. Set `STREAMS_NATIVE_URL`, for example `http://localhost:7533`, and
//! `STREAMS_NATIVE_NAMESPACE` (default `default`).

mod support;

use prost::Message;
use prost_types::Timestamp;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use temporalio_client::{Connection, ConnectionOptions, Url};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_sdk_core::{
    ExtensionError, WorkerExtension, WorkflowTaskCompletionInfo, WorkflowTaskInterceptor,
};
use temporalio_streams::{
    NativeStreamExtension, StreamExtension,
    epoch::withdraws,
    ids::{activity_producer_id, client_producer_id},
    proto::{
        AppendRecord, AppendRecordsRequest, AppendRecordsResponse, BufferWorkflowCallsRequest,
        CloseStreamRequest, CloseStreamResponse, CreateStreamRequest, CreateStreamResponse,
        DescribeStreamRequest, DescribeStreamResponse, OpenWriterRequest, OpenWriterResponse,
        Producer, ProducerEpoch, ProducerKind, ReadRecordsRequest, ReadRecordsResponse,
        StreamErrorDetails, StreamRecord, StreamRecordKind, StreamRef, StreamStatus, WorkflowCall,
        WriteRecordsRequest, WriteRecordsResponse, workflow_call::Call,
    },
};

struct Case {
    ext: Arc<StreamExtension>,
    namespace: String,
    stream: String,
}

async fn case() -> Option<Case> {
    let url = support::server_url("STREAMS_NATIVE_URL")?;
    let options = ConnectionOptions::new(Url::parse(&url).unwrap())
        .identity("streams-native-test")
        .build();
    let connection = Connection::connect(options).await.unwrap();
    let ext = NativeStreamExtension::create("native", None);
    ext.attach(connection);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    Some(Case {
        ext,
        namespace: std::env::var("STREAMS_NATIVE_NAMESPACE").unwrap_or("default".to_string()),
        stream: format!("native-test-{now}"),
    })
}

fn record(text: &str) -> AppendRecord {
    AppendRecord {
        topic: "t".to_string(),
        body: Some(Payload {
            data: text.as_bytes().to_vec(),
            ..Default::default()
        }),
    }
}

fn client() -> Producer {
    Producer {
        kind: ProducerKind::Client as i32,
        id: client_producer_id("me", "native"),
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

fn reason(error: &ExtensionError) -> String {
    StreamErrorDetails::decode(error.details.as_slice())
        .unwrap()
        .reason
}

impl Case {
    fn stream_ref(&self) -> Option<StreamRef> {
        Some(StreamRef {
            id: self.stream.clone(),
            ..Default::default()
        })
    }

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

    async fn append(
        &self,
        sequence: i64,
        texts: &[&str],
    ) -> Result<AppendRecordsResponse, ExtensionError> {
        let request = AppendRecordsRequest {
            namespace: self.namespace.clone(),
            stream: self.stream_ref(),
            producer: Some(client()),
            sequence,
            records: texts.iter().map(|text| record(text)).collect(),
            ..Default::default()
        };
        self.call("AppendRecords", request).await
    }

    async fn write(&self, producer: Producer, texts: &[&str]) -> WriteRecordsResponse {
        let opened: OpenWriterResponse = self
            .call(
                "OpenWriter",
                OpenWriterRequest {
                    namespace: self.namespace.clone(),
                    stream: self.stream_ref(),
                    producer: Some(producer),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let request = WriteRecordsRequest {
            writer_id: opened.writer_id,
            records: texts.iter().map(|text| record(text)).collect(),
        };
        self.call("WriteRecords", request).await.unwrap()
    }

    async fn read(&self) -> ReadRecordsResponse {
        let request = ReadRecordsRequest {
            namespace: self.namespace.clone(),
            stream: self.stream_ref(),
            ..Default::default()
        };
        self.call("ReadRecords", request).await.unwrap()
    }

    /// The data a Reader keeps after each `Superseded` item, and whether the read closed.
    async fn visible(&self) -> (Vec<String>, bool) {
        let mut kept: Vec<StreamRecord> = vec![];
        let mut closed = false;
        for record in self.read().await.records {
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

#[tokio::test]
async fn a_stream_on_the_server_dedupes_withdraws_reads_and_closes() {
    let Some(case) = case().await else {
        return;
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
    assert_eq!(created.stream.unwrap().backend, "native");

    // A resend of a batch answers its first offsets and writes nothing.
    let first = case.append(1, &["a"]).await.unwrap();
    let again = case.append(1, &["a"]).await.unwrap();
    assert_eq!(
        (first.first_offset, first.last_offset),
        (again.first_offset, again.last_offset)
    );
    case.append(2, &["b"]).await.unwrap();
    let error = case.append(1, &["old"]).await.unwrap_err();
    assert_eq!(reason(&error), "stream_sequence");

    // The second Activity attempt withdraws the first one's Records.
    case.write(activity(1), &["x"]).await;
    let written = case.write(activity(2), &["y"]).await;
    assert!(written.last_offset > first.last_offset);
    assert_eq!(
        case.visible().await,
        (vec!["a".into(), "b".into(), "y".into()], false)
    );

    let described: DescribeStreamResponse = case
        .call(
            "DescribeStream",
            DescribeStreamRequest {
                namespace: case.namespace.clone(),
                stream: case.stream_ref(),
            },
        )
        .await
        .unwrap();
    assert_eq!(described.status, StreamStatus::Open as i32);

    let close = CloseStreamRequest {
        namespace: case.namespace.clone(),
        stream: case.stream_ref(),
        producer: Some(client()),
        ..Default::default()
    };
    let _: CloseStreamResponse = case.call("CloseStream", close).await.unwrap();
    assert!(case.visible().await.1);
    let error = case.append(3, &["late"]).await.unwrap_err();
    assert_eq!(reason(&error), "stream_closed");
}

#[tokio::test]
async fn a_workflow_flush_writes_to_the_server_with_its_owner() {
    let Some(case) = case().await else {
        return;
    };
    let run_id = format!("run-{}", case.stream);
    let calls = vec![
        WorkflowCall {
            call: Some(Call::Create(CreateStreamRequest {
                stream_id: case.stream.clone(),
                ..Default::default()
            })),
            when_run_closes: false,
        },
        WorkflowCall {
            call: Some(Call::Append(AppendRecordsRequest {
                stream: case.stream_ref(),
                records: vec![record("w")],
                ..Default::default()
            })),
            when_run_closes: false,
        },
    ];
    let request = BufferWorkflowCallsRequest {
        run_id: run_id.clone(),
        calls,
        ..Default::default()
    };
    case.ext
        .call("BufferWorkflowCalls", &request.encode_to_vec())
        .unwrap();
    let info = WorkflowTaskCompletionInfo::builder()
        .namespace(case.namespace.clone())
        .workflow_id("wf")
        .run_id(run_id.clone())
        .first_execution_run_id(run_id)
        .workflow_type("t")
        .started_event_id(3)
        .attempt(1)
        .started_time(UNIX_EPOCH + Duration::from_secs(100))
        .deadline(std::time::Instant::now() + Duration::from_secs(10))
        .build();
    let hooks: Arc<dyn WorkflowTaskInterceptor> = case.ext.workflow_task_interceptor().unwrap();
    hooks.before_complete(&info).await.unwrap();
    assert_eq!(case.visible().await, (vec!["w".into()], false));
}
