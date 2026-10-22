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
use parking_lot::RwLock;
use prost::Message;
use std::time::Duration;
use temporalio_client::Connection;
use tokio::time::Instant;

/// The backend name of the server's Stream service.
pub const NATIVE_BACKEND: &str = "native";

// `raw_unary` takes a static method path.
const CREATE: &str = "/temporal.api.streamservice.v1.StreamService/CreateStream";
const APPEND: &str = "/temporal.api.streamservice.v1.StreamService/AppendRecords";
const CLOSE: &str = "/temporal.api.streamservice.v1.StreamService/CloseStream";
const READ: &str = "/temporal.api.streamservice.v1.StreamService/ReadRecords";
const DESCRIBE: &str = "/temporal.api.streamservice.v1.StreamService/DescribeStream";

/// The Retention the server gives a Stream with none, `stream.defaultRetention`.
const SERVER_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// A Store on the server's Stream service.
#[derive(Default)]
pub struct ServiceStore {
    connection: RwLock<Option<Connection>>,
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

fn stream_ref(id: &str) -> Option<StreamRef> {
    Some(StreamRef {
        id: id.to_string(),
        ..Default::default()
    })
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
            r#ref: run.and_then(|run| run.stream.cloned().or_else(|| stream_ref(stream_id))),
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
            stream: stream_ref(stream_id),
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
            stream: stream_ref(stream_id),
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
}

#[async_trait::async_trait]
impl RemoteReads for ServiceStore {
    async fn read_records(&self, request: ReadRecordsRequest) -> StreamResult<ReadRecordsResponse> {
        let namespace = request.namespace.clone();
        self.call(READ, &namespace, request).await
    }

    async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        let namespace = request.namespace.clone();
        self.call(DESCRIBE, &namespace, request).await
    }
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
}
