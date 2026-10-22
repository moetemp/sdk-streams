//! The native Store: the server's `temporal.api.streamservice.v1.StreamService`, called on the
//! Worker's client connection. The server keeps the same rules as the Lua scripts, so the crate's
//! Workflow buffer, its writers and its checks work on it unchanged. The server answers whole
//! reads and describes, so the service forwards those.

use crate::{
    Reason, StreamError, StreamResult,
    proto::{
        AppendRecord, AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest,
        CloseStreamResponse, CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest,
        DescribeStreamResponse, Producer, ReadRecordsRequest, ReadRecordsResponse, StreamRecord,
        StreamRef, StreamStatus,
    },
    store::{
        Appended, Batch, Capabilities, Closed, CreateOutcome, Created, Entry, Owner, RemoteReads,
        Store, StreamState, Withdrawal,
    },
};
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use prost::Message;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use temporalio_client::Connection;
use temporalio_common::protos::temporal::api::{
    common::v1::Payload,
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

/// A Store on the server's Stream service.
#[derive(Default)]
pub struct ServiceStore {
    connection: RwLock<Option<Connection>>,
    /// The full ref of each Stream that a call named with owner fields. The server finds a
    /// Workflow's Stream only by them, and the Store calls get the id alone.
    refs: Mutex<HashMap<(String, String), StreamRef>>,
    /// The Retention of a create that gives none, or the server's default.
    retention: Option<Duration>,
}

impl std::fmt::Debug for ServiceStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceStore")
            .field("attached", &self.connection.read().is_some())
            .finish()
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
    /// A Store with no connection yet. [ServiceStore::attach] gives it one.
    pub fn new() -> Self {
        Self::default()
    }

    /// A Store whose creates with no Retention take `retention`, as a Redis Store's do.
    pub fn with_retention(retention: Option<Duration>) -> Self {
        Self {
            retention,
            ..Self::default()
        }
    }

    /// Uses `connection` for every later call, for example the Worker's after a rotation.
    pub fn attach(&self, connection: Connection) {
        *self.connection.write() = Some(connection);
    }

    async fn call<Req: Message, Resp: Message + Default>(
        &self,
        method: &'static str,
        namespace: &str,
        request: Req,
    ) -> StreamResult<Resp> {
        let Some(connection) = self.connection.read().clone() else {
            return Err(StreamError::storage(
                "The native Stream backend has no connection. The Worker or the client attaches it",
            ));
        };
        let mut call = tonic::Request::new(Bytes::from(request.encode_to_vec()));
        if let Ok(value) = namespace.parse() {
            call.metadata_mut().insert("temporal-namespace", value);
        }
        let answer = connection
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
    let reason = detail.or(prefix).unwrap_or(match status.code() {
        tonic::Code::NotFound => Reason::NotFound,
        tonic::Code::AlreadyExists => Reason::AlreadyExists,
        tonic::Code::FailedPrecondition => Reason::Closed,
        tonic::Code::InvalidArgument => Reason::Invalid,
        tonic::Code::Unimplemented => Reason::Unsupported,
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

/// The most refs a Store keeps. Past it, it forgets them all, and later calls note them again.
const MAX_REFS: usize = 100_000;

impl ServiceStore {
    /// The full ref of a Stream, or one with the id alone.
    fn stream_ref(&self, namespace: &str, id: &str) -> Option<StreamRef> {
        let known = self
            .refs
            .lock()
            .get(&(namespace.to_string(), id.to_string()))
            .cloned();
        Some(known.unwrap_or_else(|| StreamRef {
            id: id.to_string(),
            ..Default::default()
        }))
    }

    async fn create_request(
        &self,
        request: CreateStreamRequest,
        retention: Option<Duration>,
    ) -> StreamResult<Created> {
        let auto_close = request
            .auto_close
            .as_ref()
            .map(|d| Duration::new(d.seconds.max(0) as u64, d.nanos.max(0) as u32));
        let namespace = request.namespace.clone();
        let answer: StreamResult<CreateStreamResponse> =
            self.call(CREATE, &namespace, request).await;
        let retention = retention.unwrap_or(SERVER_RETENTION);
        let outcome = match answer {
            Ok(answer) => match StreamStatus::try_from(answer.status).unwrap_or_default() {
                StreamStatus::Closed => CreateOutcome::Closed,
                StreamStatus::Expired => CreateOutcome::Expired,
                _ if answer.created => CreateOutcome::Created,
                _ => CreateOutcome::Exists,
            },
            Err(error) if error.reason == Reason::AlreadyExists => CreateOutcome::Conflict,
            Err(error) => return Err(error),
        };
        Ok(Created {
            outcome,
            retention,
            flagged: false,
            auto_close,
        })
    }
}

#[async_trait::async_trait]
impl Store for ServiceStore {
    // The server keeps every rule. A write is durable when the server acknowledges it, so
    // there is no replica wait.
    fn capabilities(&self) -> Capabilities {
        Capabilities::ALL.without(Capabilities::REPLICA_WAIT)
    }

    fn default_retention(&self) -> Duration {
        self.retention.unwrap_or(SERVER_RETENTION)
    }

    async fn create(
        &self,
        namespace: &str,
        stream_id: &str,
        retention: Option<Duration>,
        auto_close: Option<Duration>,
        owner: Owner<'_>,
        withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Created> {
        let run = match owner {
            Owner::Workflow(run) => run,
            Owner::Client => None,
        };
        let retention = retention.or(self.retention);
        let request = CreateStreamRequest {
            namespace: namespace.to_string(),
            stream_id: stream_id.to_string(),
            retention: retention.map(proto_duration),
            withdraw_run: withdrawal.withdraw_run.to_string(),
            auto_close: auto_close.map(proto_duration),
            owner: run.map(|run| run.producer.clone()),
            close_with_run: run.is_some_and(|run| run.close_with_run),
            r#ref: run.and_then(|run| {
                run.stream
                    .cloned()
                    .or_else(|| self.stream_ref(namespace, stream_id))
            }),
        };
        self.create_request(request, retention).await
    }

    async fn append(
        &self,
        namespace: &str,
        stream_id: &str,
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
            stream: self.stream_ref(namespace, stream_id),
            producer: Some(batch.producer.clone()),
            sequence: batch.sequence,
            records,
            previous_completed_task: withdrawal.previous_completed.copied(),
            failed_tasks: withdrawal.failed_tasks.to_vec(),
            withdraw_run: withdrawal.withdraw_run.to_string(),
            defer_withdrawal: withdrawal.defer,
        };
        let answer: AppendRecordsResponse = self.call(APPEND, namespace, request).await?;
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
        stream_id: &str,
        producer: &Producer,
        record: &[u8],
        withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Closed> {
        let record = decode_record(record)?;
        let request = CloseStreamRequest {
            namespace: namespace.to_string(),
            stream: self.stream_ref(namespace, stream_id),
            producer: Some(producer.clone()),
            result: record.close_result,
            failure: record.close_failure,
            previous_completed_task: withdrawal.previous_completed.copied(),
            failed_tasks: withdrawal.failed_tasks.to_vec(),
            withdraw_run: withdrawal.withdraw_run.to_string(),
        };
        let _: CloseStreamResponse = self.call(CLOSE, namespace, request).await?;
        Ok(Closed {
            offset: 0,
            already: false,
            flagged: false,
        })
    }

    // The server applies the auto close itself.
    async fn auto_close(&self, _namespace: &str, _stream_id: &str) -> StreamResult<Option<i64>> {
        Ok(None)
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

    fn remember(&self, namespace: &str, stream: &StreamRef) {
        if stream.workflow_id.is_empty() {
            return;
        }
        let mut refs = self.refs.lock();
        if refs.len() >= MAX_REFS {
            refs.clear();
        }
        refs.insert((namespace.to_string(), stream.id.clone()), stream.clone());
    }
}

#[async_trait::async_trait]
impl RemoteReads for ServiceStore {
    // Reads take the direct System Nexus call, which persists nothing, so every caller reads
    // through the one path that Workflow code uses too.
    async fn read_records(&self, request: ReadRecordsRequest) -> StreamResult<ReadRecordsResponse> {
        self.nexus(
            "ReadRecords",
            request.namespace.clone(),
            ReadRecordsRequest {
                namespace: String::new(),
                ..request
            },
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
    ) -> StreamResult<Resp> {
        let message_type = format!("{SERVICE_PACKAGE}.{operation}Request");
        let identity = self
            .connection
            .read()
            .as_ref()
            .map(|connection| connection.identity().to_string())
            .unwrap_or_default();
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
        };
        let answer: StartNexusOperationResponse = self.call(START_NEXUS, &namespace, call).await?;
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
