//! The native Store against a server with the Stream service, through the extension's byte
//! calls. Set `STREAMS_NATIVE_URL`, for example `http://localhost:7533`, and
//! `STREAMS_NATIVE_NAMESPACE` (default `default`).

mod support;

use prost::Message;
use prost_types::Timestamp;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use temporalio_client::{Connection, ConnectionOptions, Url};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    NativeStreamExtension, StreamError, StreamExtension,
    epoch::withdraws,
    ids::{activity_producer_id, client_producer_id},
    proto::{
        AppendRecord, AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest,
        CloseStreamResponse, CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest,
        DescribeStreamResponse, OpenWriterRequest, OpenWriterResponse, Producer, ProducerEpoch,
        ProducerKind, ReadRecordsRequest, ReadRecordsResponse, StreamErrorDetails, StreamRecord,
        StreamRecordKind, StreamRef, StreamStatus, WriteRecordsRequest, WriteRecordsResponse,
    },
};

static NEXT: AtomicU64 = AtomicU64::new(0);

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
    let ext = NativeStreamExtension::create("native", connection, None);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    Some(Case {
        ext,
        namespace: std::env::var("STREAMS_NATIVE_NAMESPACE").unwrap_or("default".to_string()),
        // Two tests can read the same clock time, so a counter tells them apart.
        stream: format!("native-test-{now}-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
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

/// A Local Activity call of a made-up run, at `attempt`.
fn local_activity(attempt: i32) -> Producer {
    Producer {
        kind: ProducerKind::LocalActivity as i32,
        id: temporalio_streams::ids::local_activity_producer_id("r1", "call-1"),
        epoch: Some(ProducerEpoch {
            task_started_event_id: 3,
            task_attempt: 1,
            attempt,
            ..Default::default()
        }),
        run_id: "r1".to_string(),
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

fn reason(error: &StreamError) -> String {
    StreamErrorDetails::decode(error.details().as_slice())
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
    ) -> Result<Resp, StreamError> {
        let bytes = self.ext.call(method, &request.encode_to_vec()).await?;
        Ok(Resp::decode(bytes.as_slice()).unwrap())
    }

    async fn append(
        &self,
        sequence: i64,
        texts: &[&str],
    ) -> Result<AppendRecordsResponse, StreamError> {
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

    // A resend of a batch returns its first offsets and writes nothing.
    let first = case.append(1, &["a"]).await.unwrap();
    let again = case.append(1, &["a"]).await.unwrap();
    assert_eq!(
        (first.first_offset, first.last_offset),
        (again.first_offset, again.last_offset)
    );
    case.append(2, &["b"]).await.unwrap();
    let error = case.append(1, &["old"]).await.unwrap_err();
    assert_eq!(reason(&error), "stream_sequence");

    // An Activity writes only with its task token, so a made-up one is refused.
    let refused = case
        .call::<_, OpenWriterResponse>(
            "OpenWriter",
            OpenWriterRequest {
                namespace: case.namespace.clone(),
                stream: case.stream_ref(),
                producer: Some(activity(2)),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(reason(&refused), "stream_not_owner");
    // A Local Activity's next attempt withdraws the earlier one's Records. On a standalone Stream
    // the server accepts the run as sent.
    case.write(local_activity(1), &["x"]).await;
    let written = case.write(local_activity(2), &["y"]).await;
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

impl Case {
    /// Starts a Workflow on a Task Queue that no Worker polls, and returns its run id.
    async fn start_workflow(&self, workflow_id: &str) -> String {
        use temporalio_common::protos::temporal::api::{
            common::v1::WorkflowType,
            taskqueue::v1::TaskQueue,
            workflowservice::v1::{StartWorkflowExecutionRequest, StartWorkflowExecutionResponse},
        };
        let url = support::server_url("STREAMS_NATIVE_URL").unwrap();
        let options = ConnectionOptions::new(Url::parse(&url).unwrap())
            .identity("streams-native-test")
            .build();
        let connection = Connection::connect(options).await.unwrap();
        let request = StartWorkflowExecutionRequest {
            namespace: self.namespace.clone(),
            workflow_id: workflow_id.to_string(),
            workflow_type: Some(WorkflowType {
                name: "streams-native-test".to_string(),
            }),
            task_queue: Some(TaskQueue {
                name: "streams-native-test-no-worker".to_string(),
                ..Default::default()
            }),
            request_id: workflow_id.to_string(),
            ..Default::default()
        };
        let mut call = tonic::Request::new(bytes::Bytes::from(request.encode_to_vec()));
        call.metadata_mut()
            .insert("temporal-namespace", self.namespace.parse().unwrap());
        let answer = connection
            .raw_unary(
                "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
                call,
            )
            .await
            .unwrap();
        StartWorkflowExecutionResponse::decode(answer.into_inner())
            .unwrap()
            .run_id
    }
}

#[tokio::test]
async fn a_create_with_no_retention_takes_the_config_default() {
    let Some(mut case) = case().await else {
        return;
    };
    let config = temporalio_streams::proto::NativeStreamConfig {
        retention: Some(prost_types::Duration {
            seconds: 3 * 60 * 60,
            nanos: 0,
        }),
        ..Default::default()
    };
    let url = support::server_url("STREAMS_NATIVE_URL").unwrap();
    let options = ConnectionOptions::new(Url::parse(&url).unwrap())
        .identity("streams-native-test")
        .build();
    let connection = Connection::connect(options).await.unwrap();
    case.ext = NativeStreamExtension::from_config_bytes(&config.encode_to_vec(), connection, None)
        .unwrap();
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
    assert_eq!(described.retention.unwrap().seconds, 3 * 60 * 60);
}

/// A create with another Retention fails with `stream_already_exists`, and the message names
/// the stored Retention.
#[tokio::test]
async fn a_create_conflict_names_the_stored_retention() {
    let Some(case) = case().await else {
        return;
    };
    let create = |hours: i64| CreateStreamRequest {
        namespace: case.namespace.clone(),
        stream_id: case.stream.clone(),
        retention: Some(prost_types::Duration {
            seconds: hours * 60 * 60,
            nanos: 0,
        }),
        ..Default::default()
    };
    let _: CreateStreamResponse = case.call("CreateStream", create(1)).await.unwrap();
    let error = case
        .call::<_, CreateStreamResponse>("CreateStream", create(2))
        .await
        .unwrap_err();
    assert_eq!(reason(&error), "stream_already_exists");
    assert!(error.message.contains("1h0m0s"), "{}", error.message);
}

#[tokio::test]
async fn a_workflow_stream_is_found_by_its_owner_ref_on_every_call() {
    let Some(case) = case().await else {
        return;
    };
    // The server finds a Workflow's Stream in the Workflow, so the test starts one that no
    // Worker polls.
    let workflow_id = format!("wf-{}", case.stream);
    let run_id = case.start_workflow(&workflow_id).await;
    let owned = StreamRef {
        id: case.stream.clone(),
        workflow_id: workflow_id.clone(),
        first_execution_run_id: run_id.clone(),
        ..Default::default()
    };
    // The Workflow's create and publish run as Local Activities on the service calls.
    let owner = Producer {
        kind: ProducerKind::Workflow as i32,
        id: temporalio_streams::ids::workflow_producer_id(&run_id),
        run_id: run_id.clone(),
        ..Default::default()
    };
    let _: CreateStreamResponse = case
        .call(
            "CreateStream",
            CreateStreamRequest {
                namespace: case.namespace.clone(),
                stream_id: case.stream.clone(),
                owner: Some(owner),
                r#ref: Some(owned.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let local = Producer {
        kind: ProducerKind::LocalActivity as i32,
        id: temporalio_streams::ids::local_activity_producer_id(&run_id, "publish-1"),
        epoch: Some(ProducerEpoch {
            attempt: 1,
            ..Default::default()
        }),
        run_id: run_id.clone(),
    };
    let _: AppendRecordsResponse = case
        .call(
            "AppendRecords",
            AppendRecordsRequest {
                namespace: case.namespace.clone(),
                stream: Some(owned.clone()),
                producer: Some(local),
                sequence: 1,
                records: vec![record("w")],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    // A client writes with the same ref.
    let opened: OpenWriterResponse = case
        .call(
            "OpenWriter",
            OpenWriterRequest {
                namespace: case.namespace.clone(),
                stream: Some(owned.clone()),
                producer: Some(client()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let _: WriteRecordsResponse = case
        .call(
            "WriteRecords",
            WriteRecordsRequest {
                writer_id: opened.writer_id,
                records: vec![record("a")],
            },
        )
        .await
        .unwrap();
    let read: ReadRecordsResponse = case
        .call(
            "ReadRecords",
            ReadRecordsRequest {
                namespace: case.namespace.clone(),
                stream: Some(owned),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let texts: Vec<String> = read
        .records
        .iter()
        .filter_map(|r| r.body.as_ref())
        .map(|body| String::from_utf8(body.data.clone()).unwrap())
        .collect();
    assert_eq!(texts, ["w", "a"]);
}

#[tokio::test]
async fn a_direct_read_long_polls_until_an_append() {
    let Some(case) = case().await else {
        return;
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
    let read = |wait_ms: u64| ReadRecordsRequest {
        namespace: case.namespace.clone(),
        stream: case.stream_ref(),
        wait: Some(prost_types::Duration {
            seconds: (wait_ms / 1000) as i64,
            nanos: ((wait_ms % 1000) * 1_000_000) as i32,
        }),
        ..Default::default()
    };
    let started = std::time::Instant::now();
    let quiet: ReadRecordsResponse = case.call("ReadRecords", read(1000)).await.unwrap();
    assert!(quiet.records.is_empty());
    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "{:?}",
        started.elapsed()
    );
    let (poll, _) = tokio::join!(
        async {
            let started = std::time::Instant::now();
            let woke: ReadRecordsResponse = case.call("ReadRecords", read(10_000)).await.unwrap();
            (woke, started.elapsed())
        },
        async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            case.append(1, &["late"]).await.unwrap();
        }
    );
    let (woke, waited) = poll;
    assert_eq!(woke.records.len(), 1);
    assert!(waited < Duration::from_secs(5), "{waited:?}");
}

#[tokio::test]
async fn a_workflow_read_names_its_run_and_reads_as_a_read_does() {
    let Some(case) = case().await else {
        return;
    };
    let workflow_id = format!("reader-{}", case.stream);
    let run_id = case.start_workflow(&workflow_id).await;
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
    case.append(1, &["a"]).await.unwrap();
    let request = temporalio_streams::proto::WorkflowReadRecordsRequest {
        read: Some(ReadRecordsRequest {
            namespace: case.namespace.clone(),
            stream: case.stream_ref(),
            ..Default::default()
        }),
        workflow_id,
        run_id,
    };
    let read: ReadRecordsResponse = case.call("WorkflowReadRecords", request).await.unwrap();
    assert_eq!(read.records.len(), 1);
}
