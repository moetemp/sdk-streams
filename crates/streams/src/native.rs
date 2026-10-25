//! The native Store: the server's `temporal.api.streamservice.v1.StreamService`, called on the
//! connection of the client that the bridge builds the runtime for. The server keeps the same
//! rules as the Lua scripts, so the crate's writers and its checks work on it unchanged. The
//! server answers whole reads and describes, so the service forwards those.

use crate::{
    MAX_WAIT, Reason, StreamError, StreamResult,
    proto::{
        AppendRecord, AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest,
        CloseStreamResponse, CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest,
        DescribeStreamResponse, Producer, ReadRecordsRequest, ReadRecordsResponse, StreamRecord,
        StreamRef, StreamStatus,
    },
    store::{
        Appended, Batch, Capabilities, Closed, CreateOutcome, Created, Entry, Owner, ReaderRun,
        RemoteReads, Store, StreamState, Withdrawal,
    },
};
use bytes::Bytes;
use prost::Message;
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use temporalio_client::Connection;
use temporalio_common::protos::temporal::api::{
    common::v1::{Link, Payload, link},
    failure::v1::{Failure, failure::FailureInfo},
};
use tokio::time::Instant;

/// The backend name of the server's Stream service.
pub const NATIVE_BACKEND: &str = "native";

// `raw_unary` takes a static method path.
const CREATE: &str = "/temporal.api.streamservice.v1.StreamService/CreateStream";
const APPEND: &str = "/temporal.api.streamservice.v1.StreamService/AppendRecords";
const CLOSE: &str = "/temporal.api.streamservice.v1.StreamService/CloseStream";

/// The Retention the server gives a Stream with none, `stream.defaultRetention`.
const SERVER_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// The deadline of a create, an append and a close, the Redis Store's `response_timeout`
/// default. A Local Activity's timeout cancels lang's await but not the call.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// A Store on the server's Stream service.
pub struct ServiceStore {
    /// The client's connection. Its clones share their headers, so an API key rotation on the
    /// client reaches every call.
    connection: Connection,
    /// The Retention of a create that gives none, or the server's default.
    retention: Option<Duration>,
}

impl std::fmt::Debug for ServiceStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceStore")
            .field("retention", &self.retention)
            .finish_non_exhaustive()
    }
}

/// `google.rpc.Status`, to read the details of a failed call.
#[derive(Clone, PartialEq, prost::Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

impl ServiceStore {
    /// A Store on a client's connection. A create with no Retention takes `retention`, as a
    /// Redis Store's does, or the server's default.
    pub fn new(connection: Connection, retention: Option<Duration>) -> Self {
        Self {
            connection,
            retention,
        }
    }

    async fn call<Req: Message, Resp: Message + Default>(
        &self,
        method: &'static str,
        namespace: &str,
        request: Req,
        timeout: Option<Duration>,
    ) -> StreamResult<Resp> {
        let mut call = tonic::Request::new(Bytes::from(request.encode_to_vec()));
        if let Ok(value) = namespace.parse() {
            call.metadata_mut().insert("temporal-namespace", value);
        }
        if let Some(timeout) = timeout {
            call.set_timeout(timeout);
        }
        let answer = self
            .connection
            .raw_unary(method, call)
            .await
            .map_err(from_status)?;
        Resp::decode(answer.into_inner())
            .map_err(|error| StreamError::storage(format!("The server's answer is bad: {error}")))
    }
}

/// The crate error of a failed server call, by the reason in its `StreamErrorDetails`, then by
/// the `<reason>: ` prefix of its message, then by its code.
fn from_status(status: tonic::Status) -> StreamError {
    let detail = RpcStatus::decode(status.details())
        .ok()
        .into_iter()
        .flat_map(|rpc| rpc.details)
        .find(|any| any.type_url.ends_with("StreamErrorDetails"))
        .and_then(|any| crate::proto::StreamErrorDetails::decode(any.value.as_slice()).ok())
        .and_then(|details| Reason::from_token(&details.reason));
    let prefix = status
        .message()
        .split_once(": ")
        .and_then(|(token, _)| Reason::from_token(token));
    // A FAILED_PRECONDITION with no Stream reason is the server's own, such as a namespace that
    // isn't active here during a failover, so it stays a Store failure that a retry can pass.
    let reason = detail.or(prefix).unwrap_or(match status.code() {
        tonic::Code::NotFound => Reason::NotFound,
        tonic::Code::AlreadyExists => Reason::AlreadyExists,
        tonic::Code::InvalidArgument => Reason::Invalid,
        tonic::Code::Unimplemented => Reason::Unsupported,
        // A limit such as the reader cap. Not a Store failure, so the message stays the reason.
        tonic::Code::ResourceExhausted => Reason::Exhausted,
        _ => Reason::Storage,
    });
    let message = status.message();
    let message = message
        .strip_prefix(&format!("{}: ", reason.token()))
        .unwrap_or(message);
    StreamError::new(reason, message)
}

fn proto_duration(value: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: value.as_secs() as i64,
        nanos: value.subsec_nanos() as i32,
    }
}

fn decode_record(bytes: &[u8]) -> StreamResult<StreamRecord> {
    StreamRecord::decode(bytes)
        .map_err(|error| StreamError::invalid(format!("A Record is bad: {error}")))
}

impl ServiceStore {
    async fn create_request(
        &self,
        request: CreateStreamRequest,
        retention: Option<Duration>,
    ) -> StreamResult<Created> {
        let namespace = request.namespace.clone();
        let answer: StreamResult<CreateStreamResponse> = self
            .call(CREATE, &namespace, request, Some(WRITE_TIMEOUT))
            .await;
        let retention = retention.unwrap_or(SERVER_RETENTION);
        let outcome = match answer {
            Ok(answer) => match StreamStatus::try_from(answer.status).unwrap_or_default() {
                StreamStatus::Closed => CreateOutcome::Closed,
                StreamStatus::Expired => CreateOutcome::Expired,
                _ if answer.created => CreateOutcome::Created,
                _ => CreateOutcome::Exists,
            },
            // The server's conflict names the stored Retention, which a create answer lacks.
            Err(error) => return Err(error),
        };
        Ok(Created {
            outcome,
            retention,
            flagged: false,
        })
    }
}

#[async_trait::async_trait]
impl Store for ServiceStore {
    // The server keeps every rule.
    fn capabilities(&self) -> Capabilities {
        Capabilities::ALL
    }

    fn default_retention(&self) -> Duration {
        self.retention.unwrap_or(SERVER_RETENTION)
    }

    async fn create(
        &self,
        namespace: &str,
        stream: &StreamRef,
        retention: Option<Duration>,
        owner: Owner<'_>,
        withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Created> {
        let (run, activity, task_token) = match owner {
            Owner::Workflow(run) => (run, None, &[][..]),
            Owner::Client => (None, None, &[][..]),
            Owner::Activity {
                producer,
                task_token,
            } => (None, Some(producer), task_token),
        };
        let retention = retention.or(self.retention);
        let request = CreateStreamRequest {
            namespace: namespace.to_string(),
            stream_id: stream.id.clone(),
            retention: retention.map(proto_duration),
            withdraw_run: withdrawal.withdraw_run.to_string(),
            owner: run.map(|run| run.producer).or(activity).cloned(),
            close_with_run: run.is_some_and(|run| run.close_with_run),
            r#ref: Some(stream.clone()),
            task_token: task_token.to_vec(),
        };
        self.create_request(request, retention).await
    }

    async fn append(
        &self,
        namespace: &str,
        stream: &StreamRef,
        batch: &Batch<'_>,
    ) -> StreamResult<Appended> {
        let records = batch
            .records
            .iter()
            .map(|bytes| {
                let record = decode_record(bytes)?;
                Ok(AppendRecord {
                    topic: record.topic,
                    body: record.body,
                })
            })
            .collect::<StreamResult<Vec<_>>>()?;
        let withdrawal = &batch.withdrawal;
        let request = AppendRecordsRequest {
            namespace: namespace.to_string(),
            stream: Some(stream.clone()),
            producer: Some(batch.producer.clone()),
            sequence: batch.sequence,
            records,
            withdraw_run: withdrawal.withdraw_run.to_string(),
            // A started event id of 0 tells the server that no task has completed yet.
            previous_completed_task: withdrawal
                .tasks
                .as_ref()
                .map(|tasks| tasks.completed.copied().unwrap_or_default()),
            failed_tasks: withdrawal
                .tasks
                .as_ref()
                .map(|tasks| tasks.failed_tasks.to_vec())
                .unwrap_or_default(),
            task_token: batch.task_token.to_vec(),
            ..Default::default()
        };
        let answer: AppendRecordsResponse = self
            .call(APPEND, namespace, request, Some(WRITE_TIMEOUT))
            .await?;
        let offsets = (answer.last_offset > 0).then_some((answer.first_offset, answer.last_offset));
        // The server doesn't say whether the batch was a resend or withdrew, which only the
        // metrics read.
        Ok(Appended {
            offsets,
            resend: false,
            flagged: false,
        })
    }

    async fn close(
        &self,
        namespace: &str,
        stream: &StreamRef,
        producer: &Producer,
        task_token: &[u8],
        record: &[u8],
    ) -> StreamResult<Closed> {
        let record = decode_record(record)?;
        let request = CloseStreamRequest {
            namespace: namespace.to_string(),
            stream: Some(stream.clone()),
            producer: Some(producer.clone()),
            result: record.close_result,
            failure: record.close_failure,
            status: record.close_status,
            task_token: task_token.to_vec(),
            ..Default::default()
        };
        let _: CloseStreamResponse = self
            .call(CLOSE, namespace, request, Some(WRITE_TIMEOUT))
            .await?;
        Ok(Closed {
            offset: 0,
            already: false,
        })
    }

    async fn read(
        &self,
        _namespace: &str,
        _stream_id: &str,
        _from: i64,
        _count: usize,
    ) -> StreamResult<(StreamState, Vec<Entry>)> {
        Err(StreamError::new(
            Reason::Unsupported,
            "The server answers whole reads only",
        ))
    }

    async fn wait_after(
        &self,
        _namespace: &str,
        _stream_id: &str,
        _after: i64,
        _deadline: Instant,
    ) -> StreamResult<bool> {
        Err(StreamError::new(
            Reason::Unsupported,
            "The server answers whole reads only",
        ))
    }

    fn remote_reads(&self) -> Option<&dyn RemoteReads> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl RemoteReads for ServiceStore {
    // Reads take the direct System Nexus call, which persists nothing, so every caller reads
    // through the one path that Workflow code uses too.
    async fn read_records(
        &self,
        request: ReadRecordsRequest,
        reader: Option<&ReaderRun>,
    ) -> StreamResult<ReadRecordsResponse> {
        // The server keeps the lowest offset that an open reading run read, and Retention keeps
        // the Records from there.
        let links = reader
            .map(|reader| Link {
                variant: Some(link::Variant::WorkflowEvent(link::WorkflowEvent {
                    namespace: request.namespace.clone(),
                    workflow_id: reader.workflow_id.clone(),
                    run_id: reader.run_id.clone(),
                    reference: None,
                })),
            })
            .into_iter()
            .collect();
        // The server answers about 1 s before the call's deadline, so the deadline leaves room
        // past the read's own wait.
        let cap = if request.wake_only {
            MAX_WAKE_WAIT
        } else {
            MAX_WAIT
        };
        let wait = [request.wait, request.create_wait]
            .iter()
            .flatten()
            .map(|d| Duration::new(d.seconds.max(0) as u64, d.nanos.max(0) as u32))
            .max()
            .unwrap_or_default()
            .min(cap);
        self.nexus(
            "ReadRecords",
            request.namespace.clone(),
            ReadRecordsRequest {
                namespace: String::new(),
                ..request
            },
            Some(wait + READ_MARGIN),
            links,
        )
        .await
    }

    async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        self.nexus(
            "DescribeStream",
            request.namespace.clone(),
            DescribeStreamRequest {
                namespace: String::new(),
                ..request
            },
            None,
            vec![],
        )
        .await
    }
}

/// `temporal.api.workflowservice.v1.StartNexusOperationRequest`, the direct Nexus call. The
/// crate builds only the fields it sends.
#[derive(Clone, PartialEq, prost::Message)]
struct StartNexusOperationRequest {
    #[prost(string, tag = "1")]
    namespace: String,
    #[prost(string, tag = "2")]
    identity: String,
    #[prost(string, tag = "3")]
    request_id: String,
    #[prost(string, tag = "4")]
    endpoint: String,
    #[prost(string, tag = "5")]
    service: String,
    #[prost(string, tag = "6")]
    operation: String,
    #[prost(message, optional, tag = "7")]
    input: Option<Payload>,
    #[prost(message, repeated, tag = "8")]
    links: Vec<Link>,
}

/// `temporal.api.workflowservice.v1.StartNexusOperationResponse`, without its links.
#[derive(Clone, PartialEq, prost::Message)]
struct StartNexusOperationResponse {
    #[prost(message, optional, tag = "1")]
    sync_success: Option<SyncSuccess>,
    #[prost(message, optional, tag = "2")]
    async_success: Option<AsyncSuccess>,
    #[prost(message, optional, tag = "3")]
    operation_failure: Option<OperationFailure>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct SyncSuccess {
    #[prost(message, optional, tag = "1")]
    payload: Option<Payload>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct AsyncSuccess {
    #[prost(string, tag = "1")]
    operation_token: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct OperationFailure {
    #[prost(message, optional, tag = "1")]
    failure: Option<Failure>,
}

/// The time a read's deadline leaves past its wait.
const READ_MARGIN: Duration = Duration::from_secs(5);

/// The longest wait of a `wake_only` read, the server's `stream.maxWakeWait` default. A parked
/// wake adds nothing to History, so it may wait for days, unlike a read with Records.
pub const MAX_WAKE_WAIT: Duration = Duration::from_secs(7 * 24 * 60 * 60);

const START_NEXUS: &str = "/temporal.api.workflowservice.v1.WorkflowService/StartNexusOperation";
const SYSTEM_ENDPOINT: &str = "__temporal_system";
const SERVICE_PACKAGE: &str = "temporal.api.streamservice.v1";
const SERVICE_NAME: &str = "temporal.api.streamservice.v1.StreamService";

static NEXT_REQUEST: AtomicU64 = AtomicU64::new(0);

impl ServiceStore {
    /// Runs a Stream service operation as a direct System Nexus call and decodes its result.
    async fn nexus<Req: Message, Resp: Message + Default>(
        &self,
        operation: &str,
        namespace: String,
        request: Req,
        timeout: Option<Duration>,
        links: Vec<Link>,
    ) -> StreamResult<Resp> {
        let message_type = format!("{SERVICE_PACKAGE}.{operation}Request");
        let identity = self.connection.identity().to_string();
        let call = StartNexusOperationRequest {
            namespace: namespace.clone(),
            identity,
            request_id: request_id(),
            endpoint: SYSTEM_ENDPOINT.to_string(),
            service: SERVICE_NAME.to_string(),
            operation: operation.to_string(),
            input: Some(Payload {
                metadata: [
                    ("encoding".to_string(), b"binary/protobuf".to_vec()),
                    ("messageType".to_string(), message_type.into_bytes()),
                ]
                .into(),
                data: request.encode_to_vec(),
                ..Default::default()
            }),
            links,
        };
        let answer: StartNexusOperationResponse =
            self.call(START_NEXUS, &namespace, call, timeout).await?;
        if let Some(success) = answer.sync_success {
            let data = success
                .payload
                .map(|payload| payload.data)
                .unwrap_or_default();
            return Resp::decode(data.as_slice()).map_err(|error| {
                StreamError::storage(format!("The server's answer is bad: {error}"))
            });
        }
        if let Some(failure) = answer.operation_failure.and_then(|f| f.failure) {
            return Err(from_failure(failure));
        }
        Err(StreamError::storage(format!(
            "The {operation} call started an operation instead of answering"
        )))
    }
}

/// A unique request id for one call.
fn request_id() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let n = NEXT_REQUEST.fetch_add(1, Ordering::Relaxed);
    format!("streams-{}-{now}-{n}", std::process::id())
}

/// The crate error of a failed operation: the reason is the application failure's type, else
/// its first `StreamErrorDetails`, else the `<reason>: ` prefix of the message.
fn from_failure(failure: Failure) -> StreamError {
    let application = match &failure.failure_info {
        Some(FailureInfo::ApplicationFailureInfo(info)) => Some(info),
        _ => None,
    };
    let by_type = application.and_then(|info| Reason::from_token(&info.r#type));
    let by_details = application
        .and_then(|info| info.details.as_ref())
        .and_then(|details| details.payloads.first())
        .and_then(|payload| crate::proto::StreamErrorDetails::decode(payload.data.as_slice()).ok())
        .and_then(|details| Reason::from_token(&details.reason));
    let message = failure.message;
    let by_prefix = message
        .split_once(": ")
        .and_then(|(token, _)| Reason::from_token(token));
    let reason = by_type
        .or(by_details)
        .or(by_prefix)
        .unwrap_or(Reason::Storage);
    let message = message
        .strip_prefix(&format!("{}: ", reason.token()))
        .unwrap_or(&message)
        .to_string();
    StreamError::new(reason, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_server_error_maps_by_its_details_then_its_prefix() {
        let details = crate::proto::StreamErrorDetails {
            reason: "stream_fenced".to_string(),
        };
        let rpc = RpcStatus {
            code: 9,
            message: "stream_fenced: lower epoch".to_string(),
            details: vec![prost_types::Any {
                type_url: "type.googleapis.com/temporal.api.streamservice.v1.StreamErrorDetails"
                    .to_string(),
                value: details.encode_to_vec(),
            }],
        };
        let status = tonic::Status::with_details(
            tonic::Code::FailedPrecondition,
            "stream_fenced: lower epoch",
            Bytes::from(rpc.encode_to_vec()),
        );
        let error = from_status(status);
        assert_eq!(error.reason, Reason::Fenced);
        assert_eq!(error.message, "lower epoch");
        let error = from_status(tonic::Status::failed_precondition("stream_sequence: below"));
        assert_eq!(error.reason, Reason::Sequence);
        let error = from_status(tonic::Status::unavailable("down"));
        assert_eq!(error.reason, Reason::Storage);
        let error = from_status(tonic::Status::failed_precondition(
            "Namespace is not active",
        ));
        assert_eq!(error.reason, Reason::Storage);
        let error = from_status(tonic::Status::resource_exhausted(
            "The Stream has the most Workflow readers",
        ));
        assert_eq!(error.reason, Reason::Exhausted);
        assert_eq!(error.message, "The Stream has the most Workflow readers");
    }

    #[test]
    fn a_failed_operation_maps_by_its_failure_type() {
        use temporalio_common::protos::temporal::api::{
            common::v1::Payloads, failure::v1::ApplicationFailureInfo,
        };
        let failure = |kind: &str, details: Option<&str>, message: &str| Failure {
            message: message.to_string(),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo {
                    r#type: kind.to_string(),
                    details: details.map(|reason| Payloads {
                        payloads: vec![Payload {
                            data: crate::proto::StreamErrorDetails {
                                reason: reason.to_string(),
                            }
                            .encode_to_vec(),
                            ..Default::default()
                        }],
                    }),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let error = from_failure(failure("stream_not_found", None, "stream_not_found: none"));
        assert_eq!(
            (error.reason, error.message.as_str()),
            (Reason::NotFound, "none")
        );
        let error = from_failure(failure("Other", Some("stream_expired"), "gone"));
        assert_eq!(error.reason, Reason::Expired);
        let error = from_failure(failure("Other", None, "stream_offset: past the end"));
        assert_eq!(error.reason, Reason::Offset);
        assert_eq!(
            from_failure(failure("x", None, "?")).reason,
            Reason::Storage
        );
    }
}
