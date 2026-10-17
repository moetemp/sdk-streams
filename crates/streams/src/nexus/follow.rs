//! The `follow` and `follow_range` operations: Core's follower behind the stream service.
//!
//! The owner's Worker has the store, so one call expands every link that store holds (DD-209).
//! Each record's body crosses decoded with its own owner's codec context, so an answer asks lang
//! for codec work once per owner whose records it holds.

use super::{
    Limits, MAX_RECORDS, MAX_WAIT_MS, NexusCalls, Work, address, failed, failure, input_error,
    operation, output, record_wire, shortened, superseded_wire,
    wire::{
        Base64, FollowInput, FollowOutput, FollowRangeInput, FollowReplay, FollowSourceWire,
        FollowedWire, LinkStateWire, StreamRef, crossing_size, json_size,
    },
};
use crate::{
    StreamError,
    proto::{
        CodecResult, FollowRangeRequest, FollowRequest, FollowResponse, NexusCall, NexusStep,
        SourceRange, StreamAddress, StreamOwnerKind, codec_result, followed_record, link_state,
        read_record,
    },
    subscribe::Subscriptions,
};
use prost::Message;
use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};
use temporalio_common::protos::temporal::api::common::v1::Payload;

/// The most bytes a follow's state may take. It holds every open stream's read state, so it is
/// larger than a read's, and the limits keep it below this (DD-206).
const MAX_FOLLOW_STATE_BYTES: usize = 64 << 10;
/// The longest `floor_holder` a follow takes. Each stream holds as `<floor_holder>/<source>`, which
/// must fit the store's 512 bytes.
const MAX_FOLLOW_HOLDER_BYTES: usize = 480;
const MAX_DEPTH: i64 = 16;
const MAX_OPEN: i64 = 32;
/// How many times a follow is read again with fewer records when its answer is over budget.
const MAX_SHRINKS: usize = 10;

/// The records of one owner whose bodies lang's codec decodes in one step.
pub(super) struct Group {
    stream: StreamRef,
    items: Vec<usize>,
    payloads: Vec<Payload>,
}

impl NexusCalls {
    /// Serves the `follow` operation.
    pub async fn follow(&self, call: NexusCall) -> NexusStep {
        let limits = Limits::of(&call);
        let input: FollowInput = match serde_json::from_slice(&call.input) {
            Ok(input) => input,
            Err(error) => return failed(failure::input_mistake(input_error("follow", error))),
        };
        let root = match followed_root(&call.namespace, &input.stream) {
            Ok(root) => root,
            Err(step) => return *step,
        };
        let max_records = input
            .max_records
            .unwrap_or(crate::DEFAULT_MAX_RECORDS as i64);
        let wait_ms = input.wait_ms.unwrap_or(0);
        let checks = [
            ("max_records", max_records, 1, MAX_RECORDS),
            ("wait_ms", wait_ms, 0, MAX_WAIT_MS),
            ("max_depth", input.max_depth.unwrap_or(1), 1, MAX_DEPTH),
            ("max_open", input.max_open.unwrap_or(1), 1, MAX_OPEN),
        ];
        for (name, value, low, high) in checks {
            if !(low..=high).contains(&value) {
                return failed(failure::input_mistake(format!(
                    "{name} is from {low} to {high}, not {value}"
                )));
            }
        }
        let floor_holder = input.floor_holder.clone().unwrap_or_default();
        if let Err(step) = check_holder(&floor_holder) {
            return *step;
        }
        let state = match follow_state(input.state) {
            Ok(state) => state,
            Err(step) => return *step,
        };
        let wake = match &input.wake_token {
            Some(token) => match operation::OperationToken::decode(&call.namespace, token) {
                Ok(token) if names_root(&token.stream, &root) => Some(token),
                Ok(_) => {
                    return failed(failure::input_mistake(
                        "wake_token names another stream's operation",
                    ));
                }
                Err(error) => return failed(failure::of_stream(&error)),
            },
            None => None,
        };
        let deadline = call.deadline.as_ref().and_then(|deadline| {
            SystemTime::UNIX_EPOCH.checked_add(Duration::new(
                u64::try_from(deadline.seconds).unwrap_or(0),
                u32::try_from(deadline.nanos).unwrap_or(0),
            ))
        });
        let wait = shortened(Duration::from_millis(wait_ms as u64), deadline);
        let request = FollowRequest {
            stream: Some(root.clone()),
            after: input.after_token.clone().unwrap_or_default(),
            state,
            wait: wait.try_into().ok(),
            max_records: max_records as u32,
            max_depth: input.max_depth.unwrap_or(0) as u32,
            max_open: input.max_open.unwrap_or(0) as u32,
            floor_holder: floor_holder.clone(),
            subscriber: None,
        };
        let mut answer = match self.follow_within(request.clone(), limits).await {
            Ok(answer) => answer,
            Err(error) => return failed(failure::of_stream(&error)),
        };
        if let Some(token) = &wake
            && answer.records.is_empty()
            && !answer.done
        {
            let linked = crate::links::FollowState::decode_from(&answer.state)
                .is_ok_and(|state| state.has_linked());
            let again = if linked {
                // The caller's operation hears only the root's notifier, and an agent's root is
                // often idle while a tool it links is busy. So the call waits on every open
                // stream itself, for up to half its deadline. When nothing came, the root's
                // wait still goes out, so a later root record wakes the caller.
                match self
                    .follow_longer(request.clone(), deadline, wait, limits)
                    .await
                {
                    Ok(None) => self.wake_on_root(token, &root, request).await,
                    other => other,
                }
            } else {
                self.wake_on_root(token, &root, request).await
            };
            match again {
                Ok(Some(again)) => answer = again,
                Ok(None) => {}
                Err(error) => return failed(failure::of_stream(&error)),
            }
        }
        self.answer_follow(answer, limits, &call.namespace, !floor_holder.is_empty())
    }

    /// Serves the `follow_range` operation.
    pub async fn follow_range(&self, call: NexusCall) -> NexusStep {
        let limits = Limits::of(&call);
        let input: FollowRangeInput = match serde_json::from_slice(&call.input) {
            Ok(input) => input,
            Err(error) => {
                return failed(failure::input_mistake(input_error("follow_range", error)));
            }
        };
        let root = match followed_root(&call.namespace, &input.stream) {
            Ok(root) => root,
            Err(step) => return *step,
        };
        let floor_holder = input.floor_holder.unwrap_or_default();
        if let Err(step) = check_holder(&floor_holder) {
            return *step;
        }
        let state_in = match follow_state(input.state) {
            Ok(state) => state,
            Err(step) => return *step,
        };
        let mut ranges = Vec::with_capacity(input.ranges.len());
        for range in input.ranges {
            match SourceRange::decode(range.0.as_slice()) {
                Ok(range) => ranges.push(range),
                Err(_) => {
                    return failed(failure::input_mistake(
                        "a range is not one an earlier follow answered",
                    ));
                }
            }
        }
        let request = FollowRangeRequest {
            stream: Some(root),
            state_in,
            ranges,
            follow_digest: input.follow_digest.0,
            floor_holder,
            max_depth: input.max_depth.unwrap_or(0).clamp(0, MAX_DEPTH) as u32,
            max_open: input.max_open.unwrap_or(0).clamp(0, MAX_OPEN) as u32,
        };
        match crate::follow::follow_range(&self.streams, request).await {
            Ok(answer) => self.answer_follow(answer, limits, &call.namespace, false),
            Err(error) => failed(failure::of_stream(&error)),
        }
    }

    /// Follows with fewer records until the answer's records fit the answer's byte budget. The
    /// first record crosses whatever its size, as on read. Each try starts from the same state,
    /// so a smaller answer is the same follow, cut sooner.
    async fn follow_within(
        &self,
        mut request: FollowRequest,
        limits: Limits,
    ) -> Result<FollowResponse, StreamError> {
        let subscriptions = Subscriptions::default();
        for _ in 0..MAX_SHRINKS {
            let answer =
                crate::follow::follow(&self.streams, &subscriptions, request.clone()).await?;
            let stored = stored_count(&answer);
            if stored <= 1 || answer_size(&answer) <= limits.answer {
                return Ok(answer);
            }
            request.max_records = (stored / 2).max(1) as u32;
            request.wait = None;
        }
        crate::follow::follow(&self.streams, &subscriptions, request).await
    }

    /// Follows once more, waiting on every open stream for up to `asked`, the caller's wait, and
    /// at most half of what is left of the deadline. Answers that follow when it found something.
    async fn follow_longer(
        &self,
        mut request: FollowRequest,
        deadline: Option<SystemTime>,
        asked: Duration,
        limits: Limits,
    ) -> Result<Option<FollowResponse>, StreamError> {
        let left = deadline.map_or(asked, |deadline| {
            deadline
                .duration_since(SystemTime::now())
                .unwrap_or_default()
                / 2
        });
        let wait = left.min(asked);
        if wait.is_zero() {
            return Ok(None);
        }
        request.wait = wait.try_into().ok();
        let again = self.follow_within(request, limits).await?;
        Ok((!again.records.is_empty() || again.done).then_some(again))
    }

    /// Asks the root's notifier to wake the caller's operation once the root moves, then follows
    /// once more, as read does. Answers the second follow when it found something.
    async fn wake_on_root(
        &self,
        token: &operation::OperationToken,
        root: &StreamAddress,
        mut request: FollowRequest,
    ) -> Result<Option<FollowResponse>, StreamError> {
        let client = self.callers.as_ref().ok_or_else(|| {
            StreamError::unsupported("waking a caller needs a client connected to the server")
        })?;
        let chain = crate::handle::chain_of(root, &token.chain_first_run_id);
        let (counter, _) = self.streams.newest(root, &chain).await?;
        match client
            .wait_callback(
                chain,
                root.topic.clone(),
                token.attach_request_id.clone(),
                counter,
            )
            .await
        {
            Ok(()) => {}
            // The empty answer stands, and the caller follows again on its own.
            Err(crate::NotifyError::Refused(error) | crate::NotifyError::Unsupported(error)) => {
                tracing::warn!(
                    workflow_id = %root.workflow_id,
                    topic = %root.topic,
                    error,
                    "The notifier took no wait for a caught-up Nexus follower"
                );
                return Ok(None);
            }
            Err(error) => return Err(operation::caller_error(error)),
        }
        request.wait = None;
        let again =
            crate::follow::follow(&self.streams, &Subscriptions::default(), request).await?;
        Ok((!again.records.is_empty() || again.done).then_some(again))
    }

    /// Takes the bodies out of `answer` by owner, and asks lang to decode the first owner's.
    fn answer_follow(
        &self,
        mut answer: FollowResponse,
        limits: Limits,
        namespace: &str,
        positions: bool,
    ) -> NexusStep {
        let refs: HashMap<u32, StreamRef> = answer
            .sources
            .iter()
            .filter_map(|source| Some((source.number, stream_ref(source.stream.as_ref()?))))
            .collect();
        let mut groups: Vec<Group> = Vec::new();
        let mut bytes = 0;
        for (index, item) in answer.records.iter_mut().enumerate() {
            let Some(followed_record::Item::Record(record)) = &mut item.item else {
                continue;
            };
            let Some(read_record::Record::Stored(stored)) = &mut record.record else {
                continue;
            };
            let Some(body) = stored.body.take() else {
                continue;
            };
            bytes += body.encoded_len();
            let Some(stream) = refs.get(&item.source) else {
                return failed(failure::of_stream(&StreamError::storage(
                    "a follow answer names a source it does not describe",
                )));
            };
            match groups.iter_mut().find(|group| group.stream == *stream) {
                Some(group) => {
                    group.items.push(index);
                    group.payloads.push(body);
                }
                None => groups.push(Group {
                    stream: stream.clone(),
                    items: vec![index],
                    payloads: vec![body],
                }),
            }
        }
        let work = FollowWork {
            answer,
            limits,
            namespace: namespace.to_string(),
            positions,
            groups,
            next: 0,
            decoded: HashMap::new(),
        };
        self.next_follow_step(work, bytes)
    }

    /// Asks lang to decode the next owner's bodies, or answers once none is left.
    fn next_follow_step(&self, mut work: FollowWork, bytes: usize) -> NexusStep {
        let Some(group) = work.groups.get_mut(work.next) else {
            return finish_follow(work);
        };
        let payloads = std::mem::take(&mut group.payloads);
        let stream = group.stream.clone();
        let namespace = work.namespace.clone();
        self.ask_codec(
            Work::Follow(Box::new(work)),
            bytes,
            false,
            &namespace,
            &stream,
            payloads,
        )
    }

    /// Goes on with a follow after lang decoded one owner's bodies.
    pub(super) fn resume_follow(
        &self,
        mut work: FollowWork,
        results: Vec<CodecResult>,
    ) -> NexusStep {
        let items = std::mem::take(&mut work.groups[work.next].items);
        if results.len() != items.len() {
            return failed(super::wrong_count(results.len(), items.len()));
        }
        for (index, result) in items.iter().zip(results) {
            let body = match result.result {
                Some(codec_result::Result::Payload(body)) => Ok(body),
                Some(codec_result::Result::Error(error)) => Err(error),
                None => Err("the payload codec answered nothing".to_string()),
            };
            work.decoded.insert(*index, body);
        }
        work.next += 1;
        let bytes = work.groups.get(work.next).map_or(0, |group| {
            group.payloads.iter().map(Message::encoded_len).sum()
        });
        self.next_follow_step(work, bytes)
    }
}

/// A follow waiting for lang's codec, one owner at a time.
pub(super) struct FollowWork {
    answer: FollowResponse,
    limits: Limits,
    namespace: String,
    positions: bool,
    groups: Vec<Group>,
    /// The group lang decodes now.
    next: usize,
    decoded: HashMap<usize, Result<Payload, String>>,
}

fn finish_follow(work: FollowWork) -> NexusStep {
    let FollowWork {
        answer,
        limits,
        positions,
        mut decoded,
        ..
    } = work;
    let records = answer
        .records
        .into_iter()
        .enumerate()
        .filter_map(|(index, item)| {
            let source = item.source;
            match item.item? {
                followed_record::Item::Record(record) => {
                    let wire = match &record.record {
                        Some(read_record::Record::Superseded(superseded)) => {
                            superseded_wire(record.cursor.clone(), superseded)
                        }
                        Some(read_record::Record::Stored(stored)) => record_wire(
                            record.cursor.clone(),
                            stored,
                            decoded.remove(&index),
                            record.stale,
                            limits,
                        ),
                        None => return None,
                    };
                    Some(FollowedWire {
                        source,
                        record: Some(wire),
                        link_state: None,
                    })
                }
                followed_record::Item::LinkState(state) => Some(FollowedWire {
                    source,
                    record: None,
                    link_state: Some(LinkStateWire {
                        kind: link_state::Kind::try_from(state.kind)
                            .map_or("unspecified", |kind| match kind {
                                link_state::Kind::Opened => "opened",
                                link_state::Kind::Done => "done",
                                link_state::Kind::Unavailable => "unavailable",
                                link_state::Kind::Skipped => "skipped",
                                link_state::Kind::Unspecified => "unspecified",
                            })
                            .to_string(),
                        link: Base64(
                            state
                                .link
                                .map(|link| link.encode_to_vec())
                                .unwrap_or_default(),
                        ),
                        error: state.error.map(|error| failure::capped(&error.message)),
                        reason: (!state.reason.is_empty()).then_some(state.reason),
                    }),
                }),
            }
        })
        .collect();
    let sources = answer
        .sources
        .iter()
        .filter_map(|source| {
            Some(FollowSourceWire {
                number: source.number,
                parent: source.parent,
                depth: source.depth,
                stream: stream_ref(source.stream.as_ref()?),
                name: source.name.clone(),
            })
        })
        .collect();
    let replay = positions.then(|| FollowReplay {
        ranges: answer
            .ranges
            .iter()
            .map(|range| Base64(range.encode_to_vec()))
            .collect(),
        follow_digest: Base64(answer.follow_digest.clone()),
    });
    output(&FollowOutput {
        records,
        done: answer.done,
        state: Base64(answer.state),
        sources,
        replay,
    })
}

/// The root a follow reads. A Workflow ref's run only picks the chain, so the follow reads the
/// Workflow id's chain as its first read finds it, and the state keeps that chain.
fn followed_root(namespace: &str, stream: &StreamRef) -> Result<StreamAddress, Box<NexusStep>> {
    let mut root = address(namespace, stream).map_err(|failure| Box::new(failed(*failure)))?;
    if root.owner_kind == StreamOwnerKind::Workflow as i32 {
        root.run_id.clear();
    }
    Ok(root)
}

fn check_holder(floor_holder: &str) -> Result<(), Box<NexusStep>> {
    if floor_holder.len() > MAX_FOLLOW_HOLDER_BYTES {
        return Err(Box::new(failed(failure::input_mistake(format!(
            "a follow's floor_holder is at most {MAX_FOLLOW_HOLDER_BYTES} bytes, not {}",
            floor_holder.len()
        )))));
    }
    Ok(())
}

/// The state a follow carries, decoded. One longer than any state a follow answers is refused
/// before it is decoded.
fn follow_state(state: Option<String>) -> Result<Vec<u8>, Box<NexusStep>> {
    match state {
        None => Ok(Vec::new()),
        Some(text) if text.len() > crossing_size(MAX_FOLLOW_STATE_BYTES) => Err(Box::new(failed(
            failure::of_stream(&StreamError::cursor(format!(
                "the follow state takes {} bytes, above the {} of any state a follow answers",
                text.len(),
                crossing_size(MAX_FOLLOW_STATE_BYTES)
            ))),
        ))),
        Some(text) => super::wire::base64_bytes(&text).ok_or_else(|| {
            Box::new(failed(failure::input_mistake(
                "state must be base64-encoded",
            )))
        }),
    }
}

/// Whether an operation token names the follow's root, on the whole owner.
fn names_root(token: &StreamAddress, root: &StreamAddress) -> bool {
    token.owner_kind == root.owner_kind
        && token.workflow_id == root.workflow_id
        && token.activity_id == root.activity_id
        && (root.owner_kind != StreamOwnerKind::WorkflowActivity as i32
            || token.run_id == root.run_id)
        && token.topic == root.topic
}

/// The contract ref of a source's address.
fn stream_ref(stream: &StreamAddress) -> StreamRef {
    StreamRef {
        kind: operation::kind_name(stream.owner_kind).to_string(),
        workflow_id: stream.workflow_id.clone(),
        activity_id: (stream.owner_kind == StreamOwnerKind::Activity as i32
            || stream.owner_kind == StreamOwnerKind::WorkflowActivity as i32)
            .then(|| stream.activity_id.clone()),
        chain_run_id: (!stream.run_id.is_empty()).then(|| stream.run_id.clone()),
        topic: stream.topic.clone(),
    }
}

fn stored_count(answer: &FollowResponse) -> usize {
    answer
        .records
        .iter()
        .filter(|item| {
            matches!(
                &item.item,
                Some(followed_record::Item::Record(record))
                    if matches!(record.record, Some(read_record::Record::Stored(_)))
            )
        })
        .count()
}

/// About the bytes an answer takes as JSON, before the codec: each stored record at four thirds
/// of its bytes, and each other item as it crosses.
fn answer_size(answer: &FollowResponse) -> usize {
    let items: usize = answer
        .records
        .iter()
        .map(|item| match &item.item {
            Some(followed_record::Item::Record(record)) => match &record.record {
                Some(read_record::Record::Stored(stored)) => {
                    64 + record.cursor.len() + crossing_size(stored.encoded_len())
                }
                Some(read_record::Record::Superseded(superseded)) => json_size(&FollowedWire {
                    source: item.source,
                    record: Some(superseded_wire(record.cursor.clone(), superseded)),
                    link_state: None,
                }),
                None => 0,
            },
            Some(followed_record::Item::LinkState(state)) => {
                96 + crossing_size(state.link.as_ref().map_or(0, Message::encoded_len))
                    + state.error.as_ref().map_or(0, |error| error.message.len())
            }
            None => 0,
        })
        .sum();
    items + crossing_size(answer.state.len())
}
