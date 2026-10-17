//! The follow state machine: one ordered view of a stream and the streams it links, with no I/O.
//!
//! A follower reads each open source and hands every page to [FollowState::apply], which says
//! what to deliver and in which order. Order within each source is kept. A linked stream's
//! records come after its OPEN, since the source opens only when the OPEN is applied. A JOIN
//! whose linked stream has not reached the JOIN's point is held, and its source reads nothing
//! more until that stream reaches it or ends (the close gate). No clock decides order, so the same
//! pages in the same order give the same answer on replay.

use crate::{
    StreamError, StreamResult,
    proto::{
        FollowSource, FollowedRecord, LinkState, ReadRecord, StreamAddress, StreamFailure,
        StreamLink, StreamRecordKind, followed_record, link_state, read_record, stream_link,
    },
};
use prost::Message;
use std::collections::BTreeMap;
use temporalio_common::protos::temporal::api::{common::v1::Payload, failure::v1::Failure};

/// How deep links are followed by default. The root is depth zero.
pub(crate) const DEFAULT_MAX_DEPTH: u32 = 4;

/// How many linked streams are open at once by default.
pub(crate) const DEFAULT_MAX_OPEN: u32 = 16;

/// The composite position of a follow: each open source's cursor and read state, and the gates.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct FollowState {
    /// The root first, then each open linked stream in the order it opened.
    #[prost(message, repeated, tag = "1")]
    sources: Vec<SourceState>,
    #[prost(uint32, tag = "2")]
    next_source: u32,
    #[prost(uint32, tag = "3")]
    max_depth: u32,
    #[prost(uint32, tag = "4")]
    max_open: u32,
    /// The root's stream ended. The follow goes on while linked segments are open.
    #[prost(bool, tag = "5")]
    root_done: bool,
    #[prost(message, optional, tag = "6")]
    root_close_result: Option<Payload>,
    #[prost(message, optional, tag = "7")]
    root_close_failure: Option<Failure>,
}

#[derive(Clone, PartialEq, Message)]
struct SourceState {
    #[prost(uint32, tag = "1")]
    number: u32,
    #[prost(uint32, tag = "2")]
    parent: u32,
    #[prost(uint32, tag = "3")]
    depth: u32,
    #[prost(message, optional, tag = "4")]
    stream: Option<StreamAddress>,
    /// The OPEN that opened the source. Unset for the root.
    #[prost(message, optional, tag = "5")]
    link: Option<StreamLink>,
    /// Where the next read starts.
    #[prost(string, tag = "6")]
    cursor: String,
    /// The read state the next read takes.
    #[prost(bytes = "vec", tag = "7")]
    read_state: Vec<u8>,
    /// The cursor of the last stored record delivered.
    #[prost(string, tag = "8")]
    last_cursor: String,
    /// Set once the linking source read the JOIN of this source's segment.
    #[prost(bool, tag = "9")]
    joined: bool,
    /// The JOIN's `until`. Empty, with `joined`, waits for the stream's end.
    #[prost(string, tag = "10")]
    until: String,
    /// A JOIN this source read and holds until the source it joins reaches its point.
    #[prost(message, optional, tag = "11")]
    held: Option<ReadRecord>,
    #[prost(uint32, tag = "12")]
    held_for: u32,
    /// Set by the source's first read.
    #[prost(bool, tag = "13")]
    started: bool,
    /// The source's stream ended while it held a JOIN. It ends once the JOIN is delivered.
    #[prost(bool, tag = "14")]
    done_pending: bool,
    #[prost(message, optional, tag = "15")]
    close_result: Option<Payload>,
    #[prost(message, optional, tag = "16")]
    close_failure: Option<Failure>,
}

/// One read of one source, as the follower got it.
#[derive(Debug, Clone, Default)]
pub(crate) struct Page {
    pub(crate) records: Vec<ReadRecord>,
    pub(crate) cursor: String,
    pub(crate) state: Vec<u8>,
    pub(crate) done: bool,
    pub(crate) close_result: Option<Payload>,
    pub(crate) close_failure: Option<Failure>,
}

/// What applying a page delivered.
#[derive(Debug, Default)]
pub(crate) struct Applied {
    pub(crate) items: Vec<FollowedRecord>,
    /// The stored records of the page that were taken. The rest wait for a later read.
    pub(crate) consumed: usize,
    /// Each source the items name, as it stood when named.
    pub(crate) named: BTreeMap<u32, FollowSource>,
}

impl FollowState {
    /// A follow of `root` from `after`.
    pub(crate) fn new(root: StreamAddress, after: &str, max_depth: u32, max_open: u32) -> Self {
        Self {
            sources: vec![SourceState {
                stream: Some(root),
                cursor: after.to_string(),
                ..Default::default()
            }],
            next_source: 1,
            max_depth: if max_depth == 0 {
                DEFAULT_MAX_DEPTH
            } else {
                max_depth
            },
            max_open: if max_open == 0 {
                DEFAULT_MAX_OPEN
            } else {
                max_open
            },
            ..Default::default()
        }
    }

    pub(crate) fn decode_from(state: &[u8]) -> StreamResult<Self> {
        let state = Self::decode(state).map_err(|error| {
            StreamError::cursor(format!(
                "the follow state is not one an earlier Follow returned: {error}"
            ))
        })?;
        if state.sources.first().is_none_or(|root| root.number != 0) {
            return Err(StreamError::cursor("the follow state has no root"));
        }
        Ok(state)
    }

    pub(crate) fn root(&self) -> &StreamAddress {
        self.sources[0]
            .stream
            .as_ref()
            .expect("the root names its stream")
    }

    /// The sources to read now, in order: open, not holding a JOIN, and for the root not ended.
    pub(crate) fn readable(&self) -> Vec<u32> {
        self.sources
            .iter()
            .filter(|source| source.held.is_none() && !(source.number == 0 && self.root_done))
            .map(|source| source.number)
            .collect()
    }

    /// What the next read of `number` takes: its stream, cursor, read state, and whether it is
    /// the source's first read.
    pub(crate) fn read_input(&self, number: u32) -> Option<(StreamAddress, String, Vec<u8>, bool)> {
        let source = self.source(number)?;
        Some((
            source.stream.clone()?,
            source.cursor.clone(),
            source.read_state.clone(),
            !source.started,
        ))
    }

    /// Whether the root ended and every linked segment ended.
    pub(crate) fn done(&self) -> bool {
        self.root_done && self.sources.len() == 1 && self.sources[0].held.is_none()
    }

    pub(crate) fn root_close(&self) -> (Option<Payload>, Option<Failure>) {
        (
            self.root_close_result.clone(),
            self.root_close_failure.clone(),
        )
    }

    /// The source `number` as an answer names it, while it is open.
    pub(crate) fn describe(&self, number: u32) -> Option<FollowSource> {
        let source = self.source(number)?;
        Some(FollowSource {
            number,
            parent: source.parent,
            depth: source.depth,
            stream: source.stream.clone(),
            name: source
                .link
                .as_ref()
                .map(|link| link.name.clone())
                .unwrap_or_default(),
        })
    }

    fn source(&self, number: u32) -> Option<&SourceState> {
        self.sources.iter().find(|source| source.number == number)
    }

    fn source_mut(&mut self, number: u32) -> Option<&mut SourceState> {
        self.sources
            .iter_mut()
            .find(|source| source.number == number)
    }

    /// Applies one read of `number`. It delivers the page's records in order and stops after a
    /// JOIN it must hold, or after the record where a joined source reaches its `until`. A page
    /// cut there by the caller is taken whole, so replay of the cut page gives the same answer.
    pub(crate) fn apply(&mut self, number: u32, page: Page) -> Applied {
        let mut applied = Applied::default();
        let Some(source) = self.source_mut(number) else {
            return applied;
        };
        source.started = true;
        self.note(number, &mut applied);
        let stored = page
            .records
            .iter()
            .filter(|record| matches!(record.record, Some(read_record::Record::Stored(_))))
            .count();
        let mut stopped = false;
        let mut holds = false;
        for record in page.records {
            let link = match &record.record {
                Some(read_record::Record::Stored(stored)) => {
                    applied.consumed += 1;
                    stored
                        .link
                        .clone()
                        .filter(|_| stored.kind == StreamRecordKind::Link as i32)
                }
                _ => {
                    push_record(&mut applied, number, record);
                    continue;
                }
            };
            let cursor = record.cursor.clone();
            match link {
                Some(link) if link.event == stream_link::Event::Join as i32 => {
                    if self.join(number, &link, record, &mut applied) {
                        stopped = true;
                        holds = true;
                    }
                }
                Some(link) if link.event == stream_link::Event::Open as i32 => {
                    push_record(&mut applied, number, record);
                    self.open(number, link, &mut applied);
                }
                _ => push_record(&mut applied, number, record),
            }
            if let Some(source) = self.source_mut(number) {
                source.last_cursor = cursor;
                if source.joined
                    && !source.until.is_empty()
                    && reached(&source.last_cursor, &source.until)
                {
                    self.end(number, None, None, &mut applied);
                    stopped = true;
                }
            }
            if stopped {
                break;
            }
        }
        let whole = applied.consumed == stored;
        if let Some(source) = self.source_mut(number) {
            source.cursor = page.cursor;
            source.read_state = page.state;
        }
        if page.done && whole && (!stopped || holds) {
            if number == 0 {
                // The root reads nothing more either way, and the follow is done once the JOIN
                // it holds is delivered.
                self.root_done = true;
                self.root_close_result = page.close_result;
                self.root_close_failure = page.close_failure;
            } else if holds {
                let source = self.source_mut(number).expect("a holding source is open");
                source.done_pending = true;
                source.close_result = page.close_result;
                source.close_failure = page.close_failure;
            } else {
                self.end(number, page.close_result, page.close_failure, &mut applied);
            }
        }
        applied
    }

    fn note(&self, number: u32, applied: &mut Applied) {
        if !applied.named.contains_key(&number)
            && let Some(source) = self.describe(number)
        {
            applied.named.insert(number, source);
        }
    }

    /// Marks source `number` unavailable, since a read of it failed for good.
    pub(crate) fn unavailable(&mut self, number: u32, error: StreamFailure) -> Applied {
        let mut applied = Applied::default();
        if number == 0 || self.source(number).is_none() {
            return applied;
        }
        self.note(number, &mut applied);
        if let Some(source) = self.source_mut(number) {
            source.done_pending = false;
        }
        self.close_descendants(number, &mut applied);
        let source = self.remove(number);
        applied.items.push(link_item(
            number,
            LinkState {
                kind: link_state::Kind::Unavailable as i32,
                link: source.link,
                error: Some(error),
                ..Default::default()
            },
        ));
        self.release(source.parent, number, &mut applied);
        applied
    }

    /// Handles a JOIN that source `number` read. Answers whether the source must hold it.
    fn join(
        &mut self,
        number: u32,
        link: &StreamLink,
        record: ReadRecord,
        applied: &mut Applied,
    ) -> bool {
        let child = self
            .sources
            .iter()
            .find(|source| {
                source.number != 0
                    && source.parent == number
                    && !source.joined
                    && source
                        .link
                        .as_ref()
                        .is_some_and(|open| open.name == link.name)
            })
            .map(|source| source.number);
        let Some(child) = child else {
            // No open segment by that name: skipped, unavailable or already ended.
            push_record(applied, number, record);
            return false;
        };
        let source = self.source_mut(child).expect("found above");
        source.joined = true;
        source.until = link.until.clone();
        if !link.until.is_empty() && reached(&source.last_cursor, &link.until) {
            self.end(child, None, None, applied);
            push_record(applied, number, record);
            return false;
        }
        let holder = self.source_mut(number).expect("the source reads");
        holder.held = Some(record);
        holder.held_for = child;
        true
    }

    /// Opens the stream an OPEN that source `number` read names, or says why not.
    fn open(&mut self, number: u32, link: StreamLink, applied: &mut Applied) {
        let holder = self.source(number).expect("the source reads");
        let holder_stream = holder.stream.clone().unwrap_or_default();
        let depth = holder.depth + 1;
        let skip = |reason: &str, link: StreamLink, applied: &mut Applied| {
            applied.items.push(link_item(
                number,
                LinkState {
                    kind: link_state::Kind::Skipped as i32,
                    link: Some(link),
                    reason: reason.to_string(),
                    ..Default::default()
                },
            ));
        };
        let other_namespace =
            !link.namespace.is_empty() && link.namespace != holder_stream.namespace;
        if !link.endpoint.is_empty() || other_namespace {
            return skip("endpoint", link, applied);
        }
        if depth > self.max_depth {
            return skip("depth", link, applied);
        }
        let stream = link_address(&link, &holder_stream.namespace);
        let key = stream_key(&stream);
        let mut ancestor = Some(number);
        while let Some(at) = ancestor {
            let Some(source) = self.source(at) else { break };
            if source.stream.as_ref().map(stream_key) == Some(key.clone()) {
                return skip("cycle", link, applied);
            }
            ancestor = (at != 0).then_some(source.parent);
        }
        if self
            .sources
            .iter()
            .any(|source| source.stream.as_ref().map(stream_key) == Some(key.clone()))
        {
            return skip("open", link, applied);
        }
        if self.sources.len() > self.max_open as usize {
            return skip("limit", link, applied);
        }
        let child = self.next_source;
        self.next_source += 1;
        self.sources.push(SourceState {
            number: child,
            parent: number,
            depth,
            stream: Some(stream),
            cursor: link.after.clone(),
            link: Some(link.clone()),
            ..Default::default()
        });
        self.note(child, applied);
        applied.items.push(link_item(
            child,
            LinkState {
                kind: link_state::Kind::Opened as i32,
                link: Some(link),
                ..Default::default()
            },
        ));
    }

    /// Ends linked source `number`'s segment, and every source it opened.
    fn end(
        &mut self,
        number: u32,
        close_result: Option<Payload>,
        close_failure: Option<Failure>,
        applied: &mut Applied,
    ) {
        self.note(number, applied);
        // The source ends here, so a JOIN its descendants release must not end it again.
        if let Some(source) = self.source_mut(number) {
            source.done_pending = false;
        }
        self.close_descendants(number, applied);
        let source = self.remove(number);
        applied.items.push(link_item(
            number,
            LinkState {
                kind: link_state::Kind::Done as i32,
                link: source.link,
                close_result,
                close_failure,
                ..Default::default()
            },
        ));
        self.release(source.parent, number, applied);
    }

    fn close_descendants(&mut self, number: u32, applied: &mut Applied) {
        let children: Vec<u32> = self
            .sources
            .iter()
            .filter(|source| source.number != 0 && source.parent == number)
            .map(|source| source.number)
            .collect();
        for child in children {
            if self.source(child).is_some() {
                self.end(child, None, None, applied);
            }
        }
    }

    fn remove(&mut self, number: u32) -> SourceState {
        let at = self
            .sources
            .iter()
            .position(|source| source.number == number)
            .expect("the source is open");
        self.sources.remove(at)
    }

    /// Delivers the JOIN `parent` holds for `child`, which ended, and ends `parent` when its own
    /// stream ended meanwhile.
    fn release(&mut self, parent: u32, child: u32, applied: &mut Applied) {
        let Some(holder) = self.source_mut(parent) else {
            return;
        };
        if holder.held.is_none() || holder.held_for != child {
            return;
        }
        let held = holder.held.take().expect("checked");
        holder.held_for = 0;
        let pending = holder
            .done_pending
            .then(|| (holder.close_result.take(), holder.close_failure.take()));
        self.note(parent, applied);
        push_record(applied, parent, held);
        if let Some((close_result, close_failure)) = pending
            && parent != 0
        {
            self.end(parent, close_result, close_failure, applied);
        }
    }
}

fn push_record(applied: &mut Applied, source: u32, record: ReadRecord) {
    applied.items.push(FollowedRecord {
        source,
        item: Some(followed_record::Item::Record(record)),
    });
}

fn link_item(source: u32, state: LinkState) -> FollowedRecord {
    FollowedRecord {
        source,
        item: Some(followed_record::Item::LinkState(state)),
    }
}

/// The address of the stream `link` names. An empty namespace is the holder's.
fn link_address(link: &StreamLink, holder_namespace: &str) -> StreamAddress {
    let stream = link.stream.clone().unwrap_or_default();
    StreamAddress {
        namespace: if link.namespace.is_empty() {
            holder_namespace.to_string()
        } else {
            link.namespace.clone()
        },
        owner_kind: stream.owner_kind,
        workflow_id: stream.workflow_id,
        run_id: stream.run_id,
        topic: stream.topic,
        activity_id: stream.activity_id,
    }
}

fn stream_key(stream: &StreamAddress) -> (String, i32, String, String, String, String) {
    (
        stream.namespace.clone(),
        stream.owner_kind,
        stream.workflow_id.clone(),
        stream.run_id.clone(),
        stream.activity_id.clone(),
        stream.topic.clone(),
    )
}

/// Whether a source whose last record has cursor `last` reached `until`. Cursors of one stream
/// order by their notification counters, so a source that ran past `until` before its JOIN was
/// read counts as there.
fn reached(last: &str, until: &str) -> bool {
    if last.is_empty() {
        return false;
    }
    if last == until {
        return true;
    }
    match (counter(last), counter(until)) {
        (Some(last), Some(until)) => last >= until,
        _ => false,
    }
}

fn counter(cursor: &str) -> Option<i64> {
    let (_, position) = cursor.rsplit_once(':')?;
    crate::progress_counter(position)
}

/// The digest `FollowRange` checks: SHA-256 over each item in order, every field length-prefixed.
/// A record adds what the read digest takes of it. A link state adds its kind, its link, its
/// reason and its error kind. The encoded bodies stay out, as in the read digest.
pub(crate) fn follow_digest(items: &[FollowedRecord]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    let mut field = |bytes: &[u8]| {
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    };
    for item in items {
        field(&item.source.to_be_bytes());
        match &item.item {
            Some(followed_record::Item::Record(record)) => {
                field(b"record");
                field(&crate::reader::records_digest(std::slice::from_ref(record)));
            }
            Some(followed_record::Item::LinkState(state)) => {
                field(b"link");
                field(&state.kind.to_be_bytes());
                for link_field in state.link.iter().flat_map(crate::record::link_fields) {
                    field(&link_field);
                }
                field(state.reason.as_bytes());
                field(
                    &state
                        .error
                        .as_ref()
                        .map_or(0, |error| error.kind)
                        .to_be_bytes(),
                );
            }
            None => field(b"none"),
        }
    }
    digest.finalize().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{StreamFailureKind, StreamOwnerKind, StreamRecord, Supersession};
    use temporalio_common::protos::temporal::api::stream::v1::StreamReference;

    fn root() -> StreamAddress {
        StreamAddress {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: "agent".to_string(),
            topic: "events".to_string(),
            ..Default::default()
        }
    }

    fn cursor(stream: &str, at: u64) -> String {
        format!("memory:{stream}:{at}")
    }

    fn data(stream: &str, at: u64) -> ReadRecord {
        ReadRecord {
            cursor: cursor(stream, at),
            record: Some(read_record::Record::Stored(StreamRecord {
                topic: "events".to_string(),
                kind: StreamRecordKind::Data as i32,
                producer_id: format!("{stream}-{at}"),
                ..Default::default()
            })),
            stale: false,
        }
    }

    fn reference(workflow_id: &str) -> StreamReference {
        StreamReference {
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: workflow_id.to_string(),
            topic: "events".to_string(),
            ..Default::default()
        }
    }

    fn link_record(stream: &str, at: u64, link: StreamLink) -> ReadRecord {
        ReadRecord {
            cursor: cursor(stream, at),
            record: Some(read_record::Record::Stored(StreamRecord {
                topic: "events".to_string(),
                kind: StreamRecordKind::Link as i32,
                link: Some(link),
                ..Default::default()
            })),
            stale: false,
        }
    }

    fn open(stream: &str, at: u64, name: &str, to: &str) -> ReadRecord {
        link_record(
            stream,
            at,
            StreamLink {
                event: stream_link::Event::Open as i32,
                stream: Some(reference(to)),
                name: name.to_string(),
                ..Default::default()
            },
        )
    }

    fn join(stream: &str, at: u64, name: &str, until: &str) -> ReadRecord {
        link_record(
            stream,
            at,
            StreamLink {
                event: stream_link::Event::Join as i32,
                name: name.to_string(),
                until: until.to_string(),
                ..Default::default()
            },
        )
    }

    fn page(records: Vec<ReadRecord>) -> Page {
        Page {
            cursor: records.last().map(|r| r.cursor.clone()).unwrap_or_default(),
            records,
            ..Default::default()
        }
    }

    fn done(mut page: Page, result: &str) -> Page {
        page.done = true;
        page.close_result = Some(Payload {
            data: result.as_bytes().to_vec(),
            ..Default::default()
        });
        page
    }

    /// Each item as text: `<source>:<record cursor>` or `<source>:<KIND>[:reason]`.
    fn shape(items: &[FollowedRecord]) -> Vec<String> {
        items
            .iter()
            .map(|item| match &item.item {
                Some(followed_record::Item::Record(record)) => match &record.record {
                    Some(read_record::Record::Superseded(_)) => {
                        format!("{}:SUPERSEDED", item.source)
                    }
                    _ => format!(
                        "{}:{}",
                        item.source,
                        record.cursor.rsplit(':').next().unwrap()
                    ),
                },
                Some(followed_record::Item::LinkState(state)) => {
                    let kind = link_state::Kind::try_from(state.kind)
                        .unwrap()
                        .as_str_name();
                    let kind = kind.trim_start_matches("KIND_");
                    if state.reason.is_empty() {
                        format!("{}:{kind}", item.source)
                    } else {
                        format!("{}:{kind}:{}", item.source, state.reason)
                    }
                }
                None => "none".to_string(),
            })
            .collect()
    }

    /// Applies each `(source, page)` in turn, through an encode and decode of the state between
    /// each, as a follower that resumes from every position does.
    fn run(state: &mut FollowState, steps: Vec<(u32, Page)>) -> Vec<String> {
        let mut items = vec![];
        for (source, page) in steps {
            *state = FollowState::decode_from(&state.encode_to_vec()).unwrap();
            items.extend(state.apply(source, page).items);
        }
        shape(&items)
    }

    fn new_state() -> FollowState {
        FollowState::new(root(), "", 0, 0)
    }

    #[test]
    fn a_linked_streams_records_come_after_its_open() {
        let mut state = new_state();
        let items = run(
            &mut state,
            vec![
                (
                    0,
                    page(vec![
                        data("r", 1),
                        open("r", 2, "tool", "tool"),
                        data("r", 3),
                    ]),
                ),
                (1, page(vec![data("t", 1), data("t", 2)])),
            ],
        );
        assert_eq!(items, ["0:1", "0:2", "1:OPENED", "0:3", "1:1", "1:2"]);
        assert_eq!(state.readable(), [0, 1]);
    }

    #[test]
    fn a_join_holds_its_stream_until_the_linked_stream_ends() {
        let mut state = new_state();
        let root_page = page(vec![
            open("r", 1, "tool", "tool"),
            join("r", 2, "tool", ""),
            data("r", 3),
        ]);
        // The caller cuts the page after the held JOIN.
        assert_eq!(state.clone().apply(0, root_page.clone()).consumed, 2);
        let mut cut = root_page;
        cut.records.truncate(2);
        cut.cursor = cursor("r", 2);
        let items = run(&mut state, vec![(0, cut), (1, page(vec![data("t", 1)]))]);
        assert_eq!(items, ["0:1", "1:OPENED", "1:1"]);
        assert_eq!(state.readable(), [1], "the root holds the JOIN");
        let items = run(
            &mut state,
            vec![(1, done(page(vec![]), "ok")), (0, page(vec![data("r", 3)]))],
        );
        assert_eq!(items, ["1:DONE", "0:2", "0:3"]);
        assert_eq!(state.readable(), [0]);
    }

    #[test]
    fn a_join_with_until_ends_the_segment_at_that_record() {
        let mut state = new_state();
        let until = cursor("t", 2);
        run(
            &mut state,
            vec![(
                0,
                page(vec![
                    open("r", 1, "turn", "child"),
                    join("r", 2, "turn", &until),
                ]),
            )],
        );
        let child_page = page(vec![data("t", 1), data("t", 2), data("t", 3)]);
        assert_eq!(state.clone().apply(1, child_page.clone()).consumed, 2);
        let mut cut = child_page;
        cut.records.truncate(2);
        cut.cursor = until.clone();
        let items = run(&mut state, vec![(1, cut), (0, page(vec![data("r", 3)]))]);
        assert_eq!(items, ["1:1", "1:2", "1:DONE", "0:2", "0:3"]);
    }

    #[test]
    fn a_join_after_the_linked_stream_passed_until_does_not_wait() {
        let mut state = new_state();
        let items = run(
            &mut state,
            vec![
                (0, page(vec![open("r", 1, "turn", "child")])),
                (1, page(vec![data("t", 1), data("t", 2)])),
                (
                    0,
                    page(vec![join("r", 2, "turn", &cursor("t", 1)), data("r", 3)]),
                ),
            ],
        );
        assert_eq!(
            items,
            ["0:1", "1:OPENED", "1:1", "1:2", "1:DONE", "0:2", "0:3"]
        );
    }

    #[test]
    fn an_unavailable_stream_opens_the_gate_and_the_follow_goes_on() {
        let mut state = new_state();
        let mut items = run(
            &mut state,
            vec![(
                0,
                page(vec![open("r", 1, "tool", "gone"), join("r", 2, "tool", "")]),
            )],
        );
        let applied = state.unavailable(
            1,
            StreamFailure {
                kind: StreamFailureKind::NotFound as i32,
                ..Default::default()
            },
        );
        items.extend(shape(&applied.items));
        assert_eq!(items, ["0:1", "1:OPENED", "1:UNAVAILABLE", "0:2"]);
        assert_eq!(state.readable(), [0]);
    }

    #[test]
    fn links_past_the_limits_or_back_to_an_open_stream_are_skipped() {
        let mut state = FollowState::new(root(), "", 1, 2);
        let mut far = open("r", 5, "far", "x");
        if let Some(read_record::Record::Stored(stored)) = &mut far.record {
            stored.link.as_mut().unwrap().endpoint = "other".to_string();
        }
        let items = run(
            &mut state,
            vec![
                (
                    0,
                    page(vec![
                        open("r", 1, "a", "a"),
                        open("r", 2, "again", "a"),
                        open("r", 3, "b", "b"),
                        open("r", 4, "c", "c"),
                        far,
                    ]),
                ),
                (
                    1,
                    page(vec![
                        open("a", 1, "deep", "d"),
                        open("a", 2, "back", "agent"),
                    ]),
                ),
            ],
        );
        assert_eq!(
            items,
            [
                "0:1",
                "1:OPENED",
                "0:2",
                "0:SKIPPED:open",
                "0:3",
                "2:OPENED",
                "0:4",
                "0:SKIPPED:limit",
                "0:5",
                "0:SKIPPED:endpoint",
                "1:1",
                "1:SKIPPED:depth",
                "1:2",
                "1:SKIPPED:depth",
            ]
        );
        // With room for the depth, a link back to an ancestor is a cycle.
        let mut deep = FollowState::new(root(), "", 4, 16);
        let items = run(
            &mut deep,
            vec![
                (0, page(vec![open("r", 1, "a", "a")])),
                (1, page(vec![open("a", 1, "back", "agent")])),
            ],
        );
        assert_eq!(items, ["0:1", "1:OPENED", "1:1", "1:SKIPPED:cycle"]);
    }

    #[test]
    fn a_superseded_link_stays_open() {
        let mut state = new_state();
        let supersession = ReadRecord {
            cursor: cursor("r", 1),
            record: Some(read_record::Record::Superseded(Supersession::default())),
            stale: false,
        };
        let items = run(
            &mut state,
            vec![(
                0,
                page(vec![
                    open("r", 1, "tool", "tool"),
                    supersession,
                    data("r", 2),
                ]),
            )],
        );
        assert_eq!(items, ["0:1", "1:OPENED", "0:SUPERSEDED", "0:2"]);
        assert_eq!(state.readable(), [0, 1]);
    }

    #[test]
    fn the_follow_is_done_once_the_root_and_every_segment_ended() {
        let mut state = new_state();
        run(
            &mut state,
            vec![(
                0,
                done(
                    page(vec![open("r", 1, "tool", "tool"), join("r", 2, "tool", "")]),
                    "r",
                ),
            )],
        );
        assert!(!state.done(), "the root holds the JOIN of an open segment");
        assert!(state.readable() == [1]);
        let items = run(&mut state, vec![(1, done(page(vec![data("t", 1)]), "t"))]);
        assert_eq!(items, ["1:1", "1:DONE", "0:2"]);
        assert!(state.done());
        assert_eq!(state.root_close().0.unwrap().data, b"r");
    }

    #[test]
    fn a_source_that_ends_while_it_holds_a_join_ends_after_the_join() {
        let mut state = new_state();
        run(&mut state, vec![(0, page(vec![open("r", 1, "a", "a")]))]);
        let items = run(
            &mut state,
            vec![
                (
                    1,
                    done(
                        page(vec![open("a", 1, "b", "b"), join("a", 2, "b", "")]),
                        "a",
                    ),
                ),
                (2, done(page(vec![data("b", 1)]), "b")),
            ],
        );
        assert_eq!(items, ["1:1", "2:OPENED", "2:1", "2:DONE", "1:2", "1:DONE"]);
        assert_eq!(state.readable(), [0]);
    }

    #[test]
    fn ending_a_segment_ends_the_streams_it_opened() {
        let mut state = new_state();
        let items = run(
            &mut state,
            vec![
                (0, page(vec![open("r", 1, "a", "a")])),
                (1, page(vec![open("a", 1, "b", "b")])),
                (1, done(page(vec![]), "a")),
            ],
        );
        assert_eq!(
            items,
            ["0:1", "1:OPENED", "1:1", "2:OPENED", "2:DONE", "1:DONE"]
        );
    }

    #[test]
    fn the_follow_digest_sees_order_and_link_states() {
        let mut state = new_state();
        let items = state
            .apply(0, page(vec![data("r", 1), open("r", 2, "tool", "tool")]))
            .items;
        let base = follow_digest(&items);
        let mut swapped = items.clone();
        swapped.swap(0, 1);
        assert_ne!(follow_digest(&swapped), base);
        assert_ne!(follow_digest(&items[..2]), base);
        assert_eq!(follow_digest(&items), base);
    }
}
