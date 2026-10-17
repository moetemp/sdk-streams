//! The callers of a stream-returning Nexus operation.
//!
//! The operation's start attaches the caller's callback to the stream's notifier on the server as
//! an idle one, and answers with a token that names the stream. An idle callback hears nothing of
//! the stream's progress until its reader, caught up, asks to be woken after a counter. It still
//! gets the completion when the stream closes. Its cancel detaches the callback the start
//! attached. The token format is shared with every SDK, which decodes it on the Workflow side, so
//! it must stay byte for byte what `crates/streams/nexus/operation_token_vectors.json` holds.

use crate::{
    CallerAttachment, NotifierClient, NotifyError, OWNER_RECHECK, StreamError, StreamNotification,
    StreamResult, Streams,
    handle::{Remembered, chain_of},
    proto::{
        AttachCallerRequest, AttachCallerResponse, ChainId, DetachCallerRequest,
        DetachCallerResponse, StreamAddress, StreamOwnerKind,
    },
};
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::{io, sync::Arc, time::SystemTime};

/// Attaches a start's caller to the stream's notifier and mints the operation token.
/// Answers a failure as the Nexus calls do, so lang's handler raises it with Core's one table.
pub(crate) async fn attach_caller(
    streams: &Streams,
    client: Option<&Arc<dyn NotifierClient>>,
    chains: &Remembered<String>,
    request: AttachCallerRequest,
) -> AttachCallerResponse {
    match attach(streams, client, chains, request).await {
        Ok(operation_token) => AttachCallerResponse {
            operation_token,
            failure: None,
        },
        Err(error) => AttachCallerResponse {
            operation_token: String::new(),
            failure: Some(super::failure::of_stream(&error)),
        },
    }
}

/// Detaches the caller a token's start attached. Answers a failure as the Nexus calls do.
pub(crate) async fn detach_caller(
    client: Option<&Arc<dyn NotifierClient>>,
    request: DetachCallerRequest,
) -> DetachCallerResponse {
    DetachCallerResponse {
        failure: detach(client, request)
            .await
            .err()
            .map(|error| super::failure::of_stream(&error)),
    }
}

async fn attach(
    streams: &Streams,
    client: Option<&Arc<dyn NotifierClient>>,
    chains: &Remembered<String>,
    request: AttachCallerRequest,
) -> StreamResult<String> {
    if request.callback_url.is_empty() {
        return Err(bad_input(
            "a stream-returning operation needs a caller callback; start it asynchronously",
        ));
    }
    if request.request_id.is_empty() {
        return Err(bad_input("an attach needs the start's request id"));
    }
    let client = client.ok_or_else(|| {
        StreamError::unsupported("attaching a caller needs a client connected to the server")
    })?;
    let stream = checked(request.stream.as_ref())?;
    // A retried start goes to the chain its first try attached to, so it mints the same token and
    // the server keeps one attach. Otherwise fresh for the latest run, since the Workflow id may
    // have moved on to a new chain since a read warmed the cache, and an attach to the ended
    // chain would never hear the new stream.
    let key = format!(
        "{}\0{}\0{}\0{}",
        stream.namespace, stream.workflow_id, stream.topic, request.request_id
    );
    let chain = match chains.get(&key) {
        Some(attached) => chain_of(stream, &attached.first_run_id),
        None => streams.chain_now(stream).await?,
    };
    let first_run_id = chain.first_run_id.clone();
    let wake_after_counter = request.wake_after_counter;
    let attach_request_id = request.request_id.clone();
    let operation_token = OperationToken {
        stream: stream.clone(),
        attach_request_id: request.request_id.clone(),
        chain_first_run_id: chain.first_run_id.clone(),
    }
    .encode();
    // The server hands the token and start time back with progress and the completion, so a
    // completion that beats the start's response still names the operation.
    client
        .attach_caller(CallerAttachment {
            chain: chain.clone(),
            topic: stream.topic.clone(),
            request_id: request.request_id,
            callback_url: request.callback_url,
            callback_headers: request.callback_headers,
            operation_token: operation_token.clone(),
            start_time: SystemTime::now(),
            // A reader reads first, and a busy one never waits, so it costs no deliveries.
            idle: true,
        })
        .await
        .map_err(caller_error)?;
    chains.put(key, &first_run_id, OWNER_RECHECK);
    if let Some(after_counter) = wake_after_counter {
        client
            .wait_callback(
                chain.clone(),
                stream.topic.clone(),
                attach_request_id,
                after_counter,
            )
            .await
            .map_err(caller_error)?;
        notify_newest(
            streams,
            client.as_ref(),
            chain,
            &stream.topic,
            after_counter,
        )
        .await?;
    }
    Ok(operation_token)
}

/// Notifies the stream's newest record when it is above `after_counter`. A standalone start
/// answers its caller before the handler runs, so a record can land before the attach. The
/// server drops a notification for a stream with no notifier, and the new notifier would never
/// pass the wait. Now that the notifier exists, the newest record wakes the wait at once.
async fn notify_newest(
    streams: &Streams,
    client: &dyn NotifierClient,
    chain: ChainId,
    topic: &str,
    after_counter: i64,
) -> StreamResult<()> {
    let latest = streams
        .store()
        .latest(crate::proto::StoreLatestRequest {
            chain: Some(chain.clone()),
            topic: topic.to_string(),
        })
        .await?;
    if latest.position.is_empty() {
        return Ok(());
    }
    let Some((position, counter)) =
        crate::notify::notification_at(streams.store(), &chain, topic, &latest.position)
    else {
        return Ok(());
    };
    if counter <= after_counter {
        return Ok(());
    }
    client
        .notify(StreamNotification {
            chain,
            topic: topic.to_string(),
            position,
            counter,
            close: None,
        })
        .await
        .map_err(caller_error)
}

async fn detach(
    client: Option<&Arc<dyn NotifierClient>>,
    request: DetachCallerRequest,
) -> StreamResult<()> {
    if request.namespace.is_empty() {
        return Err(bad_input("a detach needs its namespace"));
    }
    let token = OperationToken::decode(&request.namespace, &request.operation_token)?;
    let client = client.ok_or_else(|| {
        StreamError::unsupported("detaching a caller needs a client connected to the server")
    })?;
    // The server's detach changes nothing for an attach that's gone, so a retried cancel lands as
    // the first did.
    client
        .detach(
            token.chain(),
            token.stream.topic.clone(),
            token.attach_request_id,
        )
        .await
        .map_err(caller_error)?;
    Ok(())
}

/// The stream an attach names, checked as the Nexus calls check theirs, so a mistake in it
/// crosses as the caller's input mistake.
fn checked(stream: Option<&StreamAddress>) -> StreamResult<&StreamAddress> {
    let stream = stream.ok_or_else(|| bad_input("an attach needs its stream"))?;
    let (owner_id, other_id, kind) = match StreamOwnerKind::try_from(stream.owner_kind) {
        Ok(StreamOwnerKind::Workflow) => (&stream.workflow_id, &stream.activity_id, "Workflow"),
        Ok(StreamOwnerKind::Activity) => (&stream.activity_id, &stream.workflow_id, "Activity"),
        Ok(StreamOwnerKind::WorkflowActivity) => {
            if stream.namespace.is_empty()
                || stream.workflow_id.is_empty()
                || stream.run_id.is_empty()
                || stream.activity_id.is_empty()
            {
                return Err(bad_input(
                    "a stream that a Workflow Activity owns needs its namespace, its Workflow \
                     id, the run that scheduled it and its Activity id",
                ));
            }
            temporalio_common::streams::check_topic(&stream.topic)
                .map_err(|reason| bad_input(&super::failure::without_values(&reason)))?;
            return Ok(stream);
        }
        _ => {
            return Err(StreamError::unsupported(format!(
                "this release opens streams of Workflows, standalone Activities and Workflow \
                 Activities only, not owner kind {}",
                stream.owner_kind
            )));
        }
    };
    if stream.namespace.is_empty() || owner_id.is_empty() {
        return Err(bad_input(&format!(
            "a stream needs its namespace and its owner's {kind} id"
        )));
    }
    if !other_id.is_empty() {
        return Err(bad_input(&format!(
            "a stream that a {kind} owns names only its {kind} id"
        )));
    }
    // The topic is the caller's own value, so the message names the rule and not the topic.
    temporalio_common::streams::check_topic(&stream.topic)
        .map_err(|reason| bad_input(&super::failure::without_values(&reason)))?;
    Ok(stream)
}

fn bad_input(message: &str) -> StreamError {
    StreamError::invalid_argument(message)
}

pub(super) fn caller_error(error: NotifyError) -> StreamError {
    match error {
        NotifyError::Refused(message) => StreamError::refused(message),
        NotifyError::Unsupported(message) => StreamError::unsupported(message),
        NotifyError::Failed(message) => StreamError::storage(message),
    }
}

/// The token format version. A later release that carries more tells an old token apart by it.
pub const OPERATION_TOKEN_VERSION: u32 = 2;

/// What a stream-returning operation's token carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationToken {
    /// The stream, as the operation's `open_stream` named it.
    pub stream: StreamAddress,
    /// The start's request id, which the attach was keyed by.
    pub attach_request_id: String,
    /// The first run of the chain the start attached to, so a cancel detaches from that chain
    /// even after the Workflow id moved on to a new one. For an Activity owner, the Activity run.
    pub chain_first_run_id: String,
}

// Fields in name order, since SDKs encode with sorted keys and the bytes must match. The model is
// closed in the contract, so a token with another member is not one of ours.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenBody {
    attach: String,
    chain: String,
    #[serde(rename = "ref")]
    stream: TokenRef,
    v: u32,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenRef {
    // A Workflow's ref has none, so its tokens keep the bytes they had before the Activity kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    activity_id: Option<String>,
    // A ref to the newest chain has none, as the generated SDK writers leave an unset member out.
    // SDK releases before Core wrote null, which reads the same.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    chain_run_id: Option<String>,
    kind: String,
    topic: String,
    workflow_id: String,
}

/// The `kind` of a contract `StreamRef`, by Core's owner kind.
const KINDS: [(&str, StreamOwnerKind); 3] = [
    ("workflow", StreamOwnerKind::Workflow),
    ("activity", StreamOwnerKind::Activity),
    ("workflow_activity", StreamOwnerKind::WorkflowActivity),
];

/// The owner kind a contract `kind` names, or `None` for a kind no release opens.
pub(crate) fn owner_kind(kind: &str) -> Option<StreamOwnerKind> {
    KINDS
        .iter()
        .find(|(name, _)| *name == kind)
        .map(|(_, owner_kind)| *owner_kind)
}

/// The contract `kind` of an owner kind. Any kind but the Activity one is a Workflow, as an
/// unset kind is in a `ChainId`.
pub(crate) fn kind_name(owner_kind: i32) -> &'static str {
    KINDS
        .iter()
        .find(|(_, kind)| *kind as i32 == owner_kind)
        .map_or(KINDS[0].0, |(name, _)| name)
}

impl OperationToken {
    /// The token's text: base64url JSON with sorted keys and no padding.
    pub fn encode(&self) -> String {
        let body = TokenBody {
            attach: self.attach_request_id.clone(),
            chain: self.chain_first_run_id.clone(),
            stream: TokenRef {
                activity_id: (self.stream.owner_kind != StreamOwnerKind::Workflow as i32
                    && self.stream.owner_kind != StreamOwnerKind::Unspecified as i32)
                    .then(|| self.stream.activity_id.clone()),
                chain_run_id: (!self.stream.run_id.is_empty()).then(|| self.stream.run_id.clone()),
                kind: kind_name(self.stream.owner_kind).to_string(),
                topic: self.stream.topic.clone(),
                workflow_id: self.stream.workflow_id.clone(),
            },
            v: OPERATION_TOKEN_VERSION,
        };
        let mut json = Vec::new();
        let mut serializer = serde_json::Serializer::with_formatter(&mut json, AsciiOnly);
        body.serialize(&mut serializer)
            .expect("a token body is plain strings");
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
    }

    /// Reads a token in `namespace`. Fails as an invalid argument when it isn't one a stream
    /// operation issued.
    pub fn decode(namespace: &str, token: &str) -> StreamResult<Self> {
        let refuse = |why: &str| bad_input(&format!("not a stream operation token: {why}"));
        let json = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(token.trim_end_matches('='))
            .map_err(|_| refuse("not base64url"))?;
        let version: serde_json::Value =
            serde_json::from_slice(&json).map_err(|_| refuse("not JSON"))?;
        if version.get("v").and_then(serde_json::Value::as_u64)
            != Some(u64::from(OPERATION_TOKEN_VERSION))
        {
            return Err(refuse("unknown token version"));
        }
        let body: TokenBody =
            serde_json::from_value(version).map_err(|error| refuse(&error.to_string()))?;
        let owner_kind = match owner_kind(&body.stream.kind) {
            Some(StreamOwnerKind::Activity) => {
                if body
                    .stream
                    .activity_id
                    .as_deref()
                    .unwrap_or_default()
                    .is_empty()
                    || !body.stream.workflow_id.is_empty()
                {
                    return Err(refuse(
                        "an Activity's stream names its Activity id and no Workflow id",
                    ));
                }
                StreamOwnerKind::Activity
            }
            Some(StreamOwnerKind::WorkflowActivity) => {
                let empty = |id: &Option<String>| id.as_deref().unwrap_or_default().is_empty();
                if empty(&body.stream.activity_id)
                    || empty(&body.stream.chain_run_id)
                    || body.stream.workflow_id.is_empty()
                {
                    return Err(refuse(
                        "a Workflow Activity's stream names its Workflow id, the run that \
                         scheduled it and its Activity id",
                    ));
                }
                StreamOwnerKind::WorkflowActivity
            }
            Some(_) if body.stream.activity_id.is_none() => StreamOwnerKind::Workflow,
            Some(_) => return Err(refuse("a Workflow's stream names no Activity id")),
            None => {
                return Err(refuse(
                    "the stream's owner is no Workflow, standalone Activity or Workflow Activity",
                ));
            }
        };
        Ok(Self {
            stream: StreamAddress {
                namespace: namespace.to_string(),
                owner_kind: owner_kind as i32,
                workflow_id: body.stream.workflow_id,
                run_id: body.stream.chain_run_id.unwrap_or_default(),
                topic: body.stream.topic,
                activity_id: body.stream.activity_id.unwrap_or_default(),
            },
            attach_request_id: body.attach,
            chain_first_run_id: body.chain,
        })
    }

    /// The chain the start attached to.
    pub fn chain(&self) -> ChainId {
        crate::handle::chain_of(&self.stream, &self.chain_first_run_id)
    }
}

/// Writes JSON with every character past ASCII, and DEL, as a `\u` escape, as Python's
/// `json.dumps` does by default.
struct AsciiOnly;

impl serde_json::ser::Formatter for AsciiOnly {
    fn write_string_fragment<W>(&mut self, writer: &mut W, fragment: &str) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        for character in fragment.chars() {
            if character.is_ascii() && character != '\u{7f}' {
                writer.write_all(&[character as u8])?;
            } else {
                let mut units = [0u16; 2];
                for unit in character.encode_utf16(&mut units) {
                    write!(writer, "\\u{unit:04x}")?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MemoryStore, OwnerClient, OwnerDescription, OwnerError, StreamNotification, StreamService,
        StreamsOptions, proto::NexusFailure,
    };
    use prost::Message;
    use std::{collections::HashMap, sync::Mutex, time::UNIX_EPOCH};
    use temporalio_common::protos::temporal::api::enums::v1::WorkflowExecutionStatus;

    /// Answers each Workflow id's chain as `chains` holds it, `chain-1` by default, and records
    /// the runs it was asked about.
    #[derive(Default)]
    struct Owners {
        chains: Mutex<HashMap<String, String>>,
        described: Mutex<Vec<String>>,
        missing: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl OwnerClient for Owners {
        async fn describe(
            &self,
            _: &str,
            owner: crate::OwnerId<'_>,
            run_id: &str,
        ) -> Result<OwnerDescription, OwnerError> {
            let (crate::OwnerId::Workflow(workflow_id)
            | crate::OwnerId::Activity(workflow_id)
            | crate::OwnerId::WorkflowActivity { workflow_id, .. }) = owner;
            self.described.lock().unwrap().push(run_id.to_string());
            if self.missing.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(OwnerError::NotFound(workflow_id.to_string()));
            }
            let first_run_id = self
                .chains
                .lock()
                .unwrap()
                .get(workflow_id)
                .cloned()
                .unwrap_or_else(|| "chain-1".to_string());
            Ok(OwnerDescription {
                run_id: if run_id.is_empty() {
                    first_run_id.clone()
                } else {
                    run_id.to_string()
                },
                first_run_id,
                status: crate::OwnerStatus::of_workflow(WorkflowExecutionStatus::Running),
            })
        }
    }

    /// Records the attaches and detaches, and answers each with `answer`.
    #[derive(Default)]
    struct Notifier {
        attached: Mutex<Vec<CallerAttachment>>,
        detached: Mutex<Vec<(ChainId, String, String)>>,
        notified: Mutex<Vec<StreamNotification>>,
        notify_failures: std::sync::atomic::AtomicUsize,
        answer: Mutex<Option<NotifyError>>,
        waits: Mutex<Vec<(ChainId, String, String, i64)>>,
    }

    #[async_trait::async_trait]
    impl NotifierClient for Notifier {
        async fn wait_callback(
            &self,
            chain: ChainId,
            topic: String,
            request_id: String,
            after_counter: i64,
        ) -> Result<(), NotifyError> {
            self.waits
                .lock()
                .unwrap()
                .push((chain, topic, request_id, after_counter));
            Ok(())
        }

        async fn notify(&self, notification: StreamNotification) -> Result<(), NotifyError> {
            use std::sync::atomic::Ordering;
            if self
                .notify_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(NotifyError::Failed("the server is down".to_string()));
            }
            self.notified.lock().unwrap().push(notification);
            Ok(())
        }

        async fn attach_caller(&self, attachment: CallerAttachment) -> Result<(), NotifyError> {
            self.attached.lock().unwrap().push(attachment);
            self.answer.lock().unwrap().clone().map_or(Ok(()), Err)
        }

        async fn detach(
            &self,
            chain: ChainId,
            topic: String,
            request_id: String,
        ) -> Result<(), NotifyError> {
            self.detached
                .lock()
                .unwrap()
                .push((chain, topic, request_id));
            self.answer.lock().unwrap().clone().map_or(Ok(()), Err)
        }
    }

    struct Setup {
        owners: Arc<Owners>,
        notifier: Arc<Notifier>,
        service: StreamService,
    }

    fn setup() -> Setup {
        let owners = Arc::new(Owners::default());
        let notifier = Arc::new(Notifier::default());
        let service = StreamService::new(
            Arc::new(MemoryStore::new()),
            owners.clone(),
            // The default recheck, so a start that read the owner cache would see a stale chain.
            StreamsOptions::default(),
        )
        .with_callers(notifier.clone());
        Setup {
            owners,
            notifier,
            service,
        }
    }

    fn stream(run_id: &str) -> StreamAddress {
        StreamAddress {
            namespace: "default".to_string(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: "owner-1".to_string(),
            run_id: run_id.to_string(),
            topic: "tokens".to_string(),
            ..Default::default()
        }
    }

    fn attach_request(callback_url: &str) -> AttachCallerRequest {
        AttachCallerRequest {
            stream: Some(stream("")),
            request_id: "attach-1".to_string(),
            callback_url: callback_url.to_string(),
            callback_headers: [("nexus-callback-token".to_string(), "abc".to_string())].into(),
            wake_after_counter: None,
        }
    }

    /// A failure as the start's Nexus handler raises it: its type, whether a retry may help, and
    /// the class its message leads with.
    #[derive(Debug, PartialEq)]
    struct Raised {
        kind: String,
        retryable: bool,
        class: String,
    }

    fn raised(failure: NexusFailure) -> Raised {
        Raised {
            kind: failure.handler_error_type,
            retryable: failure.retryable,
            class: failure.message.split(':').next().unwrap().to_string(),
        }
    }

    fn expect(kind: &str, retryable: bool, class: &str) -> Raised {
        Raised {
            kind: kind.to_string(),
            retryable,
            class: class.to_string(),
        }
    }

    async fn attach(
        service: &StreamService,
        request: AttachCallerRequest,
    ) -> Result<String, Raised> {
        let response = AttachCallerResponse::decode(
            service
                .call("AttachCaller", &request.encode_to_vec())
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        match response.failure {
            Some(failure) => Err(raised(failure)),
            None => Ok(response.operation_token),
        }
    }

    async fn detach(service: &StreamService, token: &str) -> Result<(), Raised> {
        let response = DetachCallerResponse::decode(
            service
                .call(
                    "DetachCaller",
                    &DetachCallerRequest {
                        namespace: "default".to_string(),
                        operation_token: token.to_string(),
                    }
                    .encode_to_vec(),
                )
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        response
            .failure
            .map_or(Ok(()), |failure| Err(raised(failure)))
    }

    fn append(values: &[&str]) -> Vec<u8> {
        use crate::proto::{AppendRecord, AppendRequest, NamedProducer, append_request};
        use temporalio_common::protos::temporal::{
            api::common::v1::Payload, sdk::streams::v1::StreamRecordKind,
        };
        AppendRequest {
            stream: Some(stream("")),
            producer: Some(append_request::Producer::Named(NamedProducer {
                producer_id: "p".to_string(),
                attempt: 1,
            })),
            sequence: 1,
            digest: vec![1; 32],
            records: values
                .iter()
                .map(|value| AppendRecord {
                    kind: StreamRecordKind::Data as i32,
                    body: Some(Payload {
                        data: value.as_bytes().to_vec(),
                        ..Default::default()
                    }),
                    content_hash: vec![value.as_bytes()[0]; 32],
                    ..Default::default()
                })
                .collect(),
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn a_follower_start_asks_for_its_first_wake_and_a_reader_start_does_not() {
        let setup = setup();
        attach(&setup.service, attach_request("temporal://system"))
            .await
            .unwrap();
        assert!(setup.notifier.waits.lock().unwrap().is_empty());
        let follower = AttachCallerRequest {
            request_id: "attach-2".to_string(),
            wake_after_counter: Some(0),
            ..attach_request("temporal://system")
        };
        attach(&setup.service, follower).await.unwrap();
        let waits = setup.notifier.waits.lock().unwrap().clone();
        let [(chain, topic, request_id, after)] = waits.as_slice() else {
            panic!("one wait, not {waits:?}");
        };
        assert_eq!(
            chain,
            &setup.notifier.attached.lock().unwrap()[1].chain,
            "the wait names the chain the attach landed on"
        );
        assert_eq!(
            (topic.as_str(), request_id.as_str(), *after),
            ("tokens", "attach-2", 0)
        );
    }

    fn follower_request(after_counter: i64) -> AttachCallerRequest {
        AttachCallerRequest {
            wake_after_counter: Some(after_counter),
            ..attach_request("temporal://system")
        }
    }

    #[tokio::test]
    async fn a_follower_start_notifies_the_records_that_landed_before_its_attach() {
        let setup = setup();
        setup
            .service
            .call("Append", &append(&["a", "b"]))
            .await
            .unwrap();
        attach(&setup.service, follower_request(0)).await.unwrap();
        let notified = setup.notifier.notified.lock().unwrap().clone();
        let [notification] = notified.as_slice() else {
            panic!("one notification after the wait, not {notified:?}");
        };
        assert_eq!(
            notification.chain,
            setup.notifier.attached.lock().unwrap()[0].chain
        );
        assert_eq!(notification.topic, "tokens");
        // The memory store's second record is at offset 1, so its counter is 2.
        assert_eq!(notification.counter, 2);
        assert!(notification.close.is_none());
        assert_eq!(setup.notifier.waits.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_follower_start_past_the_newest_record_or_on_an_empty_stream_notifies_nothing() {
        let setup = setup();
        attach(&setup.service, follower_request(0)).await.unwrap();
        setup.service.call("Append", &append(&["a"])).await.unwrap();
        let later = AttachCallerRequest {
            request_id: "attach-2".to_string(),
            ..follower_request(1)
        };
        attach(&setup.service, later).await.unwrap();
        assert!(setup.notifier.notified.lock().unwrap().is_empty());
        assert_eq!(setup.notifier.waits.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_failed_notification_of_the_newest_record_fails_the_start_for_a_retry() {
        let setup = setup();
        setup.service.call("Append", &append(&["a"])).await.unwrap();
        setup
            .notifier
            .notify_failures
            .store(1, std::sync::atomic::Ordering::SeqCst);
        let raised = attach(&setup.service, follower_request(0))
            .await
            .unwrap_err();
        assert!(raised.retryable, "{raised:?}");
    }

    #[tokio::test]
    async fn an_activity_stream_start_attaches_waits_and_detaches_on_the_activity_run() {
        let setup = setup();
        let activity = StreamAddress {
            owner_kind: StreamOwnerKind::Activity as i32,
            workflow_id: String::new(),
            activity_id: "tool-1".to_string(),
            ..stream("")
        };
        let start = AttachCallerRequest {
            stream: Some(activity.clone()),
            wake_after_counter: Some(0),
            ..attach_request("temporal://system")
        };
        let token = attach(&setup.service, start).await.unwrap();
        let run = ChainId {
            namespace: "default".to_string(),
            first_run_id: "chain-1".to_string(),
            owner_kind: StreamOwnerKind::Activity as i32,
            activity_id: "tool-1".to_string(),
            ..Default::default()
        };
        assert_eq!(setup.notifier.attached.lock().unwrap()[0].chain, run);
        assert_eq!(setup.notifier.waits.lock().unwrap()[0].0, run);
        let decoded = OperationToken::decode("default", &token).unwrap();
        assert_eq!(decoded.stream, activity);
        assert_eq!(decoded.chain(), run);
        detach(&setup.service, &token).await.unwrap();
        assert_eq!(setup.notifier.detached.lock().unwrap()[0].0, run);
        // An Activity's address names no Workflow id.
        let refused = AttachCallerRequest {
            stream: Some(StreamAddress {
                workflow_id: "owner-1".to_string(),
                ..activity
            }),
            ..attach_request("temporal://system")
        };
        assert_eq!(
            attach(&setup.service, refused).await.unwrap_err().kind,
            "BAD_REQUEST"
        );
    }

    #[tokio::test]
    async fn a_workflow_activity_stream_start_attaches_on_its_scheduling_run() {
        let setup = setup();
        let tool = StreamAddress {
            owner_kind: StreamOwnerKind::WorkflowActivity as i32,
            run_id: "run-7".to_string(),
            activity_id: "1".to_string(),
            ..stream("")
        };
        let start = AttachCallerRequest {
            stream: Some(tool.clone()),
            wake_after_counter: Some(0),
            ..attach_request("temporal://system")
        };
        let token = attach(&setup.service, start).await.unwrap();
        let run = ChainId {
            namespace: "default".to_string(),
            workflow_id: tool.workflow_id.clone(),
            first_run_id: "run-7".to_string(),
            owner_kind: StreamOwnerKind::WorkflowActivity as i32,
            activity_id: "1".to_string(),
        };
        assert_eq!(setup.notifier.attached.lock().unwrap()[0].chain, run);
        let decoded = OperationToken::decode("default", &token).unwrap();
        assert_eq!(decoded.stream, tool);
        assert_eq!(decoded.chain(), run);
        detach(&setup.service, &token).await.unwrap();
        assert_eq!(setup.notifier.detached.lock().unwrap()[0].0, run);
        // A Workflow Activity's address names its run.
        let refused = AttachCallerRequest {
            stream: Some(StreamAddress {
                run_id: String::new(),
                ..tool
            }),
            ..attach_request("temporal://system")
        };
        assert_eq!(
            attach(&setup.service, refused).await.unwrap_err().kind,
            "BAD_REQUEST"
        );
    }

    #[tokio::test]
    async fn a_start_attaches_an_idle_callback_and_announces_nothing() {
        let setup = setup();
        setup.service.call("Append", &append(&["a"])).await.unwrap();
        attach(&setup.service, attach_request("temporal://system"))
            .await
            .unwrap();
        let attached = setup.notifier.attached.lock().unwrap().clone();
        assert!(attached.iter().all(|a| a.idle), "{attached:?}");
        assert!(setup.notifier.notified.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_start_attaches_the_callers_callback_and_hands_back_the_ref() {
        let setup = setup();
        let token = attach(&setup.service, attach_request("temporal://system"))
            .await
            .unwrap();
        assert_eq!(
            OperationToken::decode("default", &token).unwrap().stream,
            stream("")
        );
        let attached = setup.notifier.attached.lock().unwrap().clone();
        let [attachment] = attached.as_slice() else {
            panic!("one attach, not {attached:?}");
        };
        assert_eq!(
            attachment.chain,
            ChainId {
                namespace: "default".to_string(),
                workflow_id: "owner-1".to_string(),
                first_run_id: "chain-1".to_string(),
                ..Default::default()
            },
            "the notifier is keyed by the chain's first run"
        );
        assert_eq!(attachment.topic, "tokens");
        assert_eq!(attachment.request_id, "attach-1");
        assert_eq!(attachment.callback_url, "temporal://system");
        assert_eq!(
            attachment.callback_headers,
            [("nexus-callback-token".to_string(), "abc".to_string())].into()
        );
    }

    #[tokio::test]
    async fn the_attach_carries_the_start_token_and_time() {
        let setup = setup();
        let before = SystemTime::now();
        let token = attach(&setup.service, attach_request("temporal://system"))
            .await
            .unwrap();
        let attachment = setup.notifier.attached.lock().unwrap()[0].clone();
        assert_eq!(attachment.operation_token, token);
        assert!(attachment.start_time >= before && attachment.start_time <= SystemTime::now());
        assert!(attachment.start_time > UNIX_EPOCH);
    }

    #[tokio::test]
    async fn a_start_after_the_workflow_id_moved_to_a_new_chain_attaches_to_the_new_one() {
        let setup = setup();
        let first = attach(&setup.service, attach_request("temporal://system"))
            .await
            .unwrap();
        // The first chain ended, and the app started a new one on the same Workflow id.
        setup
            .owners
            .chains
            .lock()
            .unwrap()
            .insert("owner-1".to_string(), "chain-2".to_string());
        let second = attach(
            &setup.service,
            AttachCallerRequest {
                request_id: "attach-2".to_string(),
                ..attach_request("temporal://system")
            },
        )
        .await
        .unwrap();
        assert_eq!(
            OperationToken::decode("default", &first)
                .unwrap()
                .chain_first_run_id,
            "chain-1"
        );
        assert_eq!(
            OperationToken::decode("default", &second)
                .unwrap()
                .chain_first_run_id,
            "chain-2"
        );
        assert_eq!(
            setup.notifier.attached.lock().unwrap()[1]
                .chain
                .first_run_id,
            "chain-2"
        );
    }

    #[tokio::test]
    async fn a_retried_start_gets_the_same_token() {
        let setup = setup();
        let first = attach(&setup.service, attach_request("temporal://system"))
            .await
            .unwrap();
        let again = attach(&setup.service, attach_request("temporal://system"))
            .await
            .unwrap();
        assert_eq!(first, again);
        let attached = setup.notifier.attached.lock().unwrap().clone();
        assert_eq!(
            attached[0],
            CallerAttachment {
                start_time: attached[0].start_time,
                ..attached[1].clone()
            },
            "the server keys the attach by the request id, so the second changes nothing"
        );
    }

    #[tokio::test]
    async fn a_retried_start_after_the_chain_moved_keeps_its_chain_and_token() {
        let setup = setup();
        let first = attach(&setup.service, attach_request("temporal://system"))
            .await
            .unwrap();
        setup
            .owners
            .chains
            .lock()
            .unwrap()
            .insert("owner-1".to_string(), "chain-2".to_string());
        let again = attach(&setup.service, attach_request("temporal://system"))
            .await
            .unwrap();
        assert_eq!(first, again);
        let attached = setup.notifier.attached.lock().unwrap().clone();
        assert!(
            attached
                .iter()
                .all(|attachment| attachment.chain.first_run_id == "chain-1"),
            "{attached:?}"
        );
    }

    #[tokio::test]
    async fn a_pinned_ref_attaches_by_its_chains_first_run() {
        let setup = setup();
        setup
            .owners
            .chains
            .lock()
            .unwrap()
            .insert("owner-1".to_string(), "run-1".to_string());
        let token = attach(
            &setup.service,
            AttachCallerRequest {
                stream: Some(stream("run-2")),
                ..attach_request("temporal://system")
            },
        )
        .await
        .unwrap();
        assert_eq!(
            OperationToken::decode("default", &token).unwrap().stream,
            stream("run-2")
        );
        assert_eq!(*setup.owners.described.lock().unwrap(), ["run-2"]);
        assert_eq!(
            setup.notifier.attached.lock().unwrap()[0]
                .chain
                .first_run_id,
            "run-1"
        );
    }

    #[tokio::test]
    async fn a_start_without_a_callback_is_refused() {
        let setup = setup();
        let failure = attach(&setup.service, attach_request(""))
            .await
            .unwrap_err();
        assert_eq!(failure, expect("BAD_REQUEST", false, "ValueError"));
        assert!(setup.notifier.attached.lock().unwrap().is_empty());
        assert!(setup.owners.described.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_start_whose_stream_names_no_topic_is_an_input_mistake() {
        let setup = setup();
        for topic in ["", "a\u{0}b"] {
            let failure = attach(
                &setup.service,
                AttachCallerRequest {
                    stream: Some(StreamAddress {
                        topic: topic.to_string(),
                        ..stream("")
                    }),
                    ..attach_request("temporal://system")
                },
            )
            .await
            .unwrap_err();
            assert_eq!(
                failure,
                expect("BAD_REQUEST", false, "ValueError"),
                "{topic:?}"
            );
        }
        assert!(setup.owners.described.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_input_mistake_leaves_the_callers_value_out_of_its_message() {
        let setup = setup();
        let response = AttachCallerResponse::decode(
            setup
                .service
                .call(
                    "AttachCaller",
                    &AttachCallerRequest {
                        stream: Some(StreamAddress {
                            topic: "secret-value\u{1}".to_string(),
                            ..stream("")
                        }),
                        ..attach_request("temporal://system")
                    }
                    .encode_to_vec(),
                )
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let failure = response.failure.unwrap();
        assert_eq!(failure.handler_error_type, "BAD_REQUEST");
        assert!(
            !failure.message.contains("secret-value"),
            "{}",
            failure.message
        );
    }

    #[tokio::test]
    async fn a_start_without_a_request_id_is_refused() {
        let setup = setup();
        let failure = attach(
            &setup.service,
            AttachCallerRequest {
                request_id: String::new(),
                ..attach_request("temporal://system")
            },
        )
        .await
        .unwrap_err();
        assert_eq!(failure, expect("BAD_REQUEST", false, "ValueError"));
        assert!(setup.notifier.attached.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_cancel_detaches_the_callback_it_attached() {
        let setup = setup();
        let token = attach(&setup.service, attach_request("temporal://system"))
            .await
            .unwrap();
        detach(&setup.service, &token).await.unwrap();
        let attachment = setup.notifier.attached.lock().unwrap()[0].clone();
        assert_eq!(
            *setup.notifier.detached.lock().unwrap(),
            [(
                attachment.chain,
                "tokens".to_string(),
                "attach-1".to_string()
            )]
        );
    }

    #[tokio::test]
    async fn a_cancel_detaches_from_the_chain_it_attached_to() {
        let setup = setup();
        let token = attach(&setup.service, attach_request("temporal://system"))
            .await
            .unwrap();
        // The Workflow id moved on to a new chain since the start.
        setup
            .owners
            .chains
            .lock()
            .unwrap()
            .insert("owner-1".to_string(), "chain-2".to_string());
        detach(&setup.service, &token).await.unwrap();
        assert_eq!(
            setup.notifier.detached.lock().unwrap()[0].0.first_run_id,
            "chain-1"
        );
        assert_eq!(
            setup.owners.described.lock().unwrap().len(),
            1,
            "a cancel describes nothing"
        );
    }

    #[tokio::test]
    async fn a_cancel_with_a_token_that_names_no_stream_is_refused() {
        let setup = setup();
        for token in vectors().refused {
            let failure = detach(&setup.service, &token).await.unwrap_err();
            assert_eq!(
                failure,
                expect("BAD_REQUEST", false, "ValueError"),
                "{token:?}"
            );
        }
        assert!(setup.notifier.detached.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_servers_answer_crosses_as_the_nexus_calls_cross_it() {
        for (answer, crossed) in [
            (
                NotifyError::Failed("down".to_string()),
                expect("UNAVAILABLE", true, "StreamStorageError"),
            ),
            (
                NotifyError::Refused("no".to_string()),
                expect("INTERNAL", false, "StreamRefusedError"),
            ),
            (
                NotifyError::Unsupported("no notifier".to_string()),
                expect("NOT_IMPLEMENTED", false, "StreamUnsupportedError"),
            ),
        ] {
            let setup = setup();
            *setup.notifier.answer.lock().unwrap() = Some(answer.clone());
            let failure = attach(&setup.service, attach_request("temporal://system"))
                .await
                .unwrap_err();
            assert_eq!(failure, crossed);
            *setup.notifier.answer.lock().unwrap() = Some(answer);
            let token = OperationToken {
                stream: stream(""),
                attach_request_id: "attach-1".to_string(),
                chain_first_run_id: "chain-1".to_string(),
            }
            .encode();
            assert_eq!(detach(&setup.service, &token).await.unwrap_err(), crossed);
        }
    }

    #[tokio::test]
    async fn a_missing_owner_crosses_as_not_found() {
        let setup = setup();
        setup
            .owners
            .missing
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let failure = attach(&setup.service, attach_request("temporal://system"))
            .await
            .unwrap_err();
        assert_eq!(failure, expect("NOT_FOUND", false, "StreamNotFoundError"));
    }

    #[tokio::test]
    async fn a_service_without_a_client_cant_attach() {
        let service = StreamService::new(
            Arc::new(MemoryStore::new()),
            Arc::new(Owners::default()),
            StreamsOptions::default(),
        );
        let failure = attach(&service, attach_request("temporal://system"))
            .await
            .unwrap_err();
        assert_eq!(
            failure,
            expect("NOT_IMPLEMENTED", false, "StreamUnsupportedError")
        );
    }

    #[derive(Deserialize)]
    struct Vectors {
        vectors: Vec<Vector>,
        decode_only: Vec<Vector>,
        refused: Vec<String>,
    }

    #[derive(Deserialize)]
    struct Vector {
        #[serde(rename = "ref")]
        stream: TokenRef,
        attach: String,
        chain: String,
        token: String,
    }

    fn vectors() -> Vectors {
        serde_json::from_str(include_str!("../../nexus/operation_token_vectors.json")).unwrap()
    }

    fn token_of(vector: &Vector) -> OperationToken {
        OperationToken {
            stream: StreamAddress {
                namespace: "ns".to_string(),
                owner_kind: owner_kind(&vector.stream.kind).unwrap() as i32,
                workflow_id: vector.stream.workflow_id.clone(),
                run_id: vector.stream.chain_run_id.clone().unwrap_or_default(),
                topic: vector.stream.topic.clone(),
                activity_id: vector.stream.activity_id.clone().unwrap_or_default(),
            },
            attach_request_id: vector.attach.clone(),
            chain_first_run_id: vector.chain.clone(),
        }
    }

    #[test]
    fn a_token_encodes_as_the_shared_vectors_say() {
        for vector in vectors().vectors {
            assert_eq!(token_of(&vector).encode(), vector.token, "{}", vector.token);
        }
    }

    #[test]
    fn a_token_decodes_as_the_shared_vectors_say() {
        for vector in vectors().vectors {
            assert_eq!(
                OperationToken::decode("ns", &vector.token).unwrap(),
                token_of(&vector)
            );
            let padded = format!(
                "{}{}",
                vector.token,
                "=".repeat((4 - vector.token.len() % 4) % 4)
            );
            assert_eq!(
                OperationToken::decode("ns", &padded).unwrap(),
                token_of(&vector),
                "padding is optional"
            );
        }
    }

    #[test]
    fn an_activity_token_names_the_activity_run_as_its_chain() {
        let vectors = vectors();
        let vector = vectors
            .vectors
            .iter()
            .find(|vector| vector.stream.kind == "activity")
            .unwrap();
        let token = OperationToken::decode("ns", &vector.token).unwrap();
        assert_eq!(token.stream.owner_kind, StreamOwnerKind::Activity as i32);
        assert_eq!(
            token.chain(),
            ChainId {
                namespace: "ns".to_string(),
                first_run_id: vector.chain.clone(),
                owner_kind: StreamOwnerKind::Activity as i32,
                activity_id: vector.stream.activity_id.clone().unwrap(),
                ..Default::default()
            }
        );
        // A Workflow's token keeps the bytes it had, with no `activity_id` member.
        let workflow = vectors
            .vectors
            .iter()
            .find(|vector| vector.stream.kind == "workflow")
            .unwrap();
        assert_eq!(token_of(workflow).chain().owner_kind, 0);
        assert!(
            !String::from_utf8(
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(&workflow.token)
                    .unwrap()
            )
            .unwrap()
            .contains("activity_id")
        );
    }

    #[test]
    fn a_workflow_activity_token_names_its_scheduling_run_as_its_chain() {
        let vectors = vectors();
        let vector = vectors
            .vectors
            .iter()
            .find(|vector| vector.stream.kind == "workflow_activity")
            .unwrap();
        let token = OperationToken::decode("ns", &vector.token).unwrap();
        assert_eq!(
            token.stream.owner_kind,
            StreamOwnerKind::WorkflowActivity as i32
        );
        assert_eq!(
            token.chain(),
            ChainId {
                namespace: "ns".to_string(),
                workflow_id: vector.stream.workflow_id.clone(),
                first_run_id: vector.stream.chain_run_id.clone().unwrap(),
                owner_kind: StreamOwnerKind::WorkflowActivity as i32,
                activity_id: vector.stream.activity_id.clone().unwrap(),
            }
        );
        assert_eq!(token.encode(), vector.token);
    }

    #[test]
    fn a_token_from_before_core_still_decodes() {
        for vector in vectors().decode_only {
            assert_eq!(
                OperationToken::decode("ns", &vector.token).unwrap(),
                token_of(&vector)
            );
        }
    }

    #[test]
    fn a_token_that_names_no_stream_is_refused() {
        for token in vectors().refused {
            let error = OperationToken::decode("ns", &token).unwrap_err();
            assert_eq!(
                error.kind,
                crate::proto::StreamFailureKind::InvalidArgument,
                "{token:?}"
            );
        }
    }
}
