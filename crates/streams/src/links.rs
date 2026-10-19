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
        StreamLink, StreamRecordKind, Supersession, followed_record, link_state, read_record,
        stream_link,
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
    /// Each segment that a `SUPERSEDED` ended, oldest first, so that the same OPEN from a later
    /// attempt resumes it at its cursor.
    #[prost(message, repeated, tag = "8")]
    withdrawn: Vec<SourceState>,
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
    // Don't reuse tag 13. A saved follow state can still carry it.
    /// The source's stream ended while it held a JOIN. It ends once the JOIN is delivered.
    #[prost(bool, tag = "14")]
    done_pending: bool,
    #[prost(message, optional, tag = "15")]
    close_result: Option<Payload>,
    #[prost(message, optional, tag = "16")]
    close_failure: Option<Failure>,
    /// The producer and the attempt that wrote the OPEN. A `SUPERSEDED` of that attempt ends the
    /// segment.
    #[prost(string, tag = "17")]
    producer_id: String,
    #[prost(int64, tag = "18")]
    attempt: i64,
    /// Set on a withdrawn entry that ended because an ancestor's segment was withdrawn. It
    /// resumes with that ancestor, not by an OPEN of its own.
    #[prost(bool, tag = "19")]
    with_ancestor: bool,
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

    /// What the next read of `number` takes: its stream, cursor and read state.
    pub(crate) fn read_input(&self, number: u32) -> Option<(StreamAddress, String, Vec<u8>)> {
        let source = self.source(number)?;
        Some((
            source.stream.clone()?,
            source.cursor.clone(),
            source.read_state.clone(),
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
        if self.source(number).is_none() {
            return applied;
        }
        self.note(number, &mut applied);
        let stored = page
            .records
            .iter()
            .filter(|record| matches!(record.record, Some(read_record::Record::Stored(_))))
            .count();
        let mut stopped = false;
        let mut holds = false;
        for record in page.records {
            let (link, writer) = match &record.record {
                Some(read_record::Record::Stored(stored)) => {
                    applied.consumed += 1;
                    // A stale LINK comes from an older attempt after the read gave a newer one. It
                    // is not the current answer, so it opens and joins nothing.
                    let link = stored
                        .link
                        .clone()
                        .filter(|_| stored.kind == StreamRecordKind::Link as i32 && !record.stale);
                    (link, (stored.producer_id.clone(), stored.attempt))
                }
                Some(read_record::Record::Superseded(superseded)) => {
                    let superseded = superseded.clone();
                    push_record(&mut applied, number, record);
                    self.withdraw(number, &superseded, &mut applied);
                    continue;
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
                    self.open(number, link, writer, &mut applied);
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
        self.forget(|entry| entry.parent == number);
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

    /// Opens the stream of an OPEN that source `number` read, or says why not. `writer` is the
    /// producer and the attempt that wrote the OPEN.
    fn open(
        &mut self,
        number: u32,
        link: StreamLink,
        writer: (String, i64),
        applied: &mut Applied,
    ) {
        if self.resume(number, &link, &writer, applied) {
            return;
        }
        // Another OPEN of the name replaces what the withdrawn attempt opened.
        self.forget(|entry| {
            entry.parent == number
                && !entry.with_ancestor
                && entry
                    .link
                    .as_ref()
                    .is_some_and(|open| open.name == link.name)
        });
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
            producer_id: writer.0,
            attempt: writer.1,
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
        self.forget(|entry| entry.parent == number);
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

    /// Ends each segment that source `number` opened from the attempt `superseded` withdraws.
    /// The match is on that one attempt, since the earlier completed tasks of a Workflow producer
    /// stay valid.
    fn withdraw(&mut self, number: u32, superseded: &Supersession, applied: &mut Applied) {
        let children: Vec<u32> = self
            .sources
            .iter()
            .filter(|source| {
                source.number != 0
                    && source.parent == number
                    && !source.producer_id.is_empty()
                    && source.producer_id == superseded.producer_id
                    && source.attempt == superseded.previous_attempt
            })
            .map(|source| source.number)
            .collect();
        for child in children {
            self.pause(child, false, applied);
        }
        // A dropped entry makes a later OPEN read its segment from `after` again. The sources
        // that ended with an entry go with it, since a resumed source can wait on a JOIN of one.
        while self.withdrawn.len() > self.max_open as usize {
            let Some(oldest) = self.withdrawn.iter().find(|entry| !entry.with_ancestor) else {
                break;
            };
            let oldest = oldest.number;
            self.forget(|entry| entry.number == oldest);
        }
    }

    /// Moves source `number` and every source it opened to the withdrawn list, with `DONE` and
    /// reason `withdrawn`. Each keeps its cursor and read state.
    fn pause(&mut self, number: u32, with_ancestor: bool, applied: &mut Applied) {
        self.note(number, applied);
        // The entry goes before the entries of its descendants, so the list stays oldest first.
        let at = self.withdrawn.len();
        let children: Vec<u32> = self
            .sources
            .iter()
            .filter(|source| source.number != 0 && source.parent == number)
            .map(|source| source.number)
            .collect();
        for child in children {
            self.pause(child, true, applied);
        }
        let mut source = self.remove(number);
        source.with_ancestor = with_ancestor;
        applied.items.push(link_item(
            number,
            LinkState {
                kind: link_state::Kind::Done as i32,
                link: source.link.clone(),
                reason: "withdrawn".to_string(),
                ..Default::default()
            },
        ));
        self.withdrawn.insert(at, source);
    }

    /// Resumes the withdrawn segment that the same OPEN by the same producer opened, with every
    /// source that ended with it. Answers whether it did.
    fn resume(
        &mut self,
        number: u32,
        link: &StreamLink,
        writer: &(String, i64),
        applied: &mut Applied,
    ) -> bool {
        let holder_namespace = self
            .source(number)
            .and_then(|holder| holder.stream.as_ref())
            .map(|stream| stream.namespace.clone())
            .unwrap_or_default();
        let key = stream_key(&link_address(link, &holder_namespace));
        let Some(at) = self.withdrawn.iter().position(|entry| {
            entry.parent == number
                && !entry.with_ancestor
                && !writer.0.is_empty()
                && entry.producer_id == writer.0
                && entry.stream.as_ref().map(stream_key) == Some(key.clone())
                && entry
                    .link
                    .as_ref()
                    .is_some_and(|open| open.name == link.name && open.after == link.after)
        }) else {
            return false;
        };
        // A top entry that can't come back takes the usual path. That gives `SKIPPED` with `open`
        // or `limit`, so nothing reads the segment again from `after`.
        let is_open = |sources: &[SourceState], entry: &SourceState| {
            sources.iter().any(|source| {
                source.stream.as_ref().map(stream_key) == entry.stream.as_ref().map(stream_key)
            })
        };
        if is_open(&self.sources, &self.withdrawn[at])
            || self.sources.len() > self.max_open as usize
        {
            return false;
        }
        let mut entry = self.withdrawn.remove(at);
        entry.attempt = writer.1;
        let mut resumed = vec![entry.number];
        self.sources.push(entry);
        // Each source that ended with the entry comes back, oldest first, while it fits and its
        // Stream is not open again. One that can't stays ended, with every source it opened, and
        // the JOIN its parent waits on for it goes out, so the follow can still end.
        let mut ended: Vec<(u32, u32)> = vec![];
        let mut gone: Vec<u32> = vec![];
        loop {
            // Source numbers grow in open order, so the lowest number is the oldest source.
            let Some(at) = self
                .withdrawn
                .iter()
                .enumerate()
                .filter(|(_, entry)| {
                    entry.with_ancestor
                        && (resumed.contains(&entry.parent) || gone.contains(&entry.parent))
                })
                .min_by_key(|(_, entry)| entry.number)
                .map(|(at, _)| at)
            else {
                break;
            };
            let mut entry = self.withdrawn.remove(at);
            if gone.contains(&entry.parent)
                || is_open(&self.sources, &entry)
                || self.sources.len() > self.max_open as usize
            {
                if !gone.contains(&entry.parent) {
                    ended.push((entry.parent, entry.number));
                }
                gone.push(entry.number);
                continue;
            }
            entry.with_ancestor = false;
            resumed.push(entry.number);
            self.sources.push(entry);
        }
        // An entry that a source which stays ended opened can't resume either.
        self.forget(|entry| gone.contains(&entry.parent));
        for source in resumed {
            self.note(source, applied);
            let link = self.source(source).and_then(|source| source.link.clone());
            applied.items.push(link_item(
                source,
                LinkState {
                    kind: link_state::Kind::Opened as i32,
                    link,
                    reason: "resumed".to_string(),
                    ..Default::default()
                },
            ));
        }
        for (parent, child) in ended {
            self.release(parent, child, applied);
        }
        true
    }

    /// Drops each withdrawn entry that `drop` picks, and every entry that ended with it or that
    /// it opened, since nothing can resume them.
    fn forget(&mut self, drop: impl Fn(&SourceState) -> bool) {
        let mut gone: Vec<u32> = self
            .withdrawn
            .iter()
            .filter(|entry| drop(entry))
            .map(|entry| entry.number)
            .collect();
        while let Some(number) = gone.pop() {
            self.withdrawn.retain(|entry| {
                if entry.number == number {
                    return false;
                }
                if entry.parent == number {
                    gone.push(entry.number);
                    return false;
                }
                true
            });
        }
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
    use crate::protos::temporal::api::stream::v1::StreamReference;

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
        shape(&run_items(state, steps))
    }

    fn run_items(state: &mut FollowState, steps: Vec<(u32, Page)>) -> Vec<FollowedRecord> {
        let mut items = vec![];
        for (source, page) in steps {
            *state = FollowState::decode_from(&state.encode_to_vec()).unwrap();
            items.extend(state.apply(source, page).items);
        }
        items
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

    /// `record` as written by attempt `attempt` of producer `producer`.
    fn by(producer: &str, attempt: i64, mut record: ReadRecord) -> ReadRecord {
        if let Some(read_record::Record::Stored(stored)) = &mut record.record {
            stored.producer_id = producer.to_string();
            stored.attempt = attempt;
        }
        record
    }

    fn superseded(
        stream: &str,
        at: u64,
        producer: &str,
        previous: i64,
        attempt: i64,
    ) -> ReadRecord {
        ReadRecord {
            cursor: cursor(stream, at),
            record: Some(read_record::Record::Superseded(Supersession {
                topic: "events".to_string(),
                producer_id: producer.to_string(),
                previous_attempt: previous,
                attempt,
            })),
            stale: false,
        }
    }

    /// An attempt of a Workflow run's producer, as the reader numbers task attempts.
    fn task(started: i64, attempt: i64) -> i64 {
        started << 16 | attempt
    }

    #[test]
    fn an_activity_retry_that_writes_the_same_open_resumes_the_segment() {
        // A1. The retry writes the OPEN again, and the Workflow joins the child later.
        let mut state = new_state();
        let items = run(
            &mut state,
            vec![
                (
                    0,
                    page(vec![
                        by("act", 1, data("r", 1)),
                        by("act", 1, open("r", 2, "b", "child")),
                    ]),
                ),
                (1, page(vec![data("t", 1), data("t", 2)])),
                (0, page(vec![superseded("r", 2, "act", 1, 2)])),
            ],
        );
        assert_eq!(
            items,
            [
                "0:1",
                "0:2",
                "1:OPENED",
                "1:1",
                "1:2",
                "0:SUPERSEDED",
                "1:DONE:withdrawn"
            ]
        );
        assert_eq!(state.readable(), [0], "the child pauses");
        let items = run(
            &mut state,
            vec![
                (
                    0,
                    page(vec![
                        by("act", 2, data("r", 3)),
                        by("act", 2, open("r", 4, "b", "child")),
                        by("@run", task(9, 1), join("r", 5, "b", &cursor("t", 3))),
                    ]),
                ),
                (1, page(vec![data("t", 3), data("t", 4)])),
                (0, page(vec![data("r", 6)])),
            ],
        );
        assert_eq!(
            items,
            [
                "0:3",
                "0:4",
                "1:OPENED:resumed",
                "1:3",
                "1:DONE",
                "0:5",
                "0:6"
            ],
            "the child reads on from its cursor, and no child record shows twice"
        );
        assert_eq!(state.readable(), [0]);
    }

    #[test]
    fn an_activity_retry_with_no_open_or_another_one_ends_the_segment() {
        // A2. The retry writes no OPEN, then an OPEN of another name opens the same child.
        let mut state = new_state();
        let items = run(
            &mut state,
            vec![
                (0, page(vec![by("act", 1, open("r", 1, "b", "child"))])),
                (1, page(vec![data("t", 1)])),
                (
                    0,
                    page(vec![
                        superseded("r", 1, "act", 1, 2),
                        by("act", 2, data("r", 2)),
                        by("act", 2, open("r", 3, "c", "child")),
                    ]),
                ),
                (2, page(vec![data("t", 1)])),
            ],
        );
        assert_eq!(
            items,
            [
                "0:1",
                "1:OPENED",
                "1:1",
                "0:SUPERSEDED",
                "1:DONE:withdrawn",
                "0:2",
                "0:3",
                "2:OPENED",
                "2:1",
            ],
            "the other OPEN opens with no SKIPPED:open"
        );
        assert_eq!(state.readable(), [0, 2]);
        // The entry of `b` waits, since its producer can still write the same OPEN.
        assert_eq!(state.withdrawn.len(), 1);
        assert_eq!(state.withdrawn[0].link.as_ref().unwrap().name, "b");
    }

    #[test]
    fn another_open_of_the_name_drops_the_withdrawn_segment() {
        let mut state = new_state();
        let items = run(
            &mut state,
            vec![
                (0, page(vec![by("act", 1, open("r", 1, "b", "child"))])),
                (1, page(vec![data("t", 1)])),
                (
                    0,
                    page(vec![
                        superseded("r", 1, "act", 1, 2),
                        by("other", 1, open("r", 2, "b", "child")),
                        by("act", 2, open("r", 3, "b", "child")),
                    ]),
                ),
            ],
        );
        // The first OPEN comes from another producer, so it opens the child from `after`, and
        // the retry's OPEN then finds the child open.
        assert_eq!(
            items,
            [
                "0:1",
                "1:OPENED",
                "1:1",
                "0:SUPERSEDED",
                "1:DONE:withdrawn",
                "0:2",
                "2:OPENED",
                "0:3",
                "0:SKIPPED:open",
            ]
        );
        assert!(state.withdrawn.is_empty());
    }

    #[test]
    fn a_failed_task_that_reruns_the_same_open_resumes_the_segment() {
        // W1. The new task attempt takes the same path and writes the OPEN again.
        let mut state = new_state();
        let items = run(
            &mut state,
            vec![
                (
                    0,
                    page(vec![by("@run", task(5, 1), open("r", 1, "b", "child"))]),
                ),
                (1, page(vec![data("t", 1)])),
                (
                    0,
                    page(vec![
                        superseded("r", 1, "@run", task(5, 1), task(5, 2)),
                        by("@run", task(5, 2), open("r", 2, "b", "child")),
                    ]),
                ),
                (1, page(vec![data("t", 2)])),
            ],
        );
        assert_eq!(
            items,
            [
                "0:1",
                "1:OPENED",
                "1:1",
                "0:SUPERSEDED",
                "1:DONE:withdrawn",
                "0:2",
                "1:OPENED:resumed",
                "1:2",
            ]
        );
        // The resumed segment belongs to the new attempt now.
        let items = run(
            &mut state,
            vec![(
                0,
                page(vec![superseded("r", 2, "@run", task(5, 2), task(5, 3))]),
            )],
        );
        assert_eq!(items, ["0:SUPERSEDED", "1:DONE:withdrawn"]);
    }

    #[test]
    fn a_failed_task_that_takes_another_path_ends_the_segment() {
        // W2.
        let mut state = new_state();
        let items = run(
            &mut state,
            vec![
                (
                    0,
                    page(vec![by("@run", task(5, 1), open("r", 1, "b", "child"))]),
                ),
                (1, page(vec![data("t", 1)])),
                (
                    0,
                    done(
                        page(vec![
                            superseded("r", 1, "@run", task(5, 1), task(5, 2)),
                            by("@run", task(5, 2), data("r", 2)),
                        ]),
                        "r",
                    ),
                ),
                (1, page(vec![data("t", 2)])),
            ],
        );
        assert_eq!(
            items,
            [
                "0:1",
                "1:OPENED",
                "1:1",
                "0:SUPERSEDED",
                "1:DONE:withdrawn",
                "0:2"
            ]
        );
        assert!(
            state.done(),
            "a withdrawn segment does not keep the follow open"
        );
    }

    #[test]
    fn a_withdrawn_local_activity_call_ends_the_segment() {
        // L1. The failed task's call does not run again, and the next task writes other records.
        let call = "@run/la-3";
        let (before, after) = (task(5, 1) << 8 | 1, task(9, 1) << 8);
        let mut state = new_state();
        let items = run(
            &mut state,
            vec![
                (0, page(vec![by(call, before, open("r", 1, "b", "child"))])),
                (1, page(vec![data("t", 1)])),
                (
                    0,
                    page(vec![
                        superseded("r", 1, call, before, after),
                        by("@run", task(9, 1), data("r", 2)),
                    ]),
                ),
            ],
        );
        assert_eq!(
            items,
            [
                "0:1",
                "1:OPENED",
                "1:1",
                "0:SUPERSEDED",
                "1:DONE:withdrawn",
                "0:2"
            ]
        );
        assert_eq!(state.readable(), [0]);
    }

    #[test]
    fn an_earlier_tasks_open_stays_open() {
        // Only the attempt the SUPERSEDED withdraws loses its OPENs.
        let mut state = new_state();
        let items = run(
            &mut state,
            vec![(
                0,
                page(vec![
                    by("@run", task(3, 1), open("r", 1, "a", "first")),
                    by("@run", task(5, 1), open("r", 2, "b", "second")),
                    superseded("r", 2, "@run", task(5, 1), task(5, 2)),
                ]),
            )],
        );
        assert_eq!(
            items,
            [
                "0:1",
                "1:OPENED",
                "0:2",
                "2:OPENED",
                "0:SUPERSEDED",
                "2:DONE:withdrawn"
            ]
        );
        assert_eq!(state.readable(), [0, 1]);
    }

    #[test]
    fn a_withdrawn_join_stays_in_effect() {
        // J1. The JOIN held the root, so the segment ended before the SUPERSEDED.
        let mut state = new_state();
        let items = run(
            &mut state,
            vec![
                (
                    0,
                    page(vec![
                        by("@run", task(5, 1), open("r", 1, "b", "child")),
                        by("@run", task(5, 1), join("r", 2, "b", "")),
                    ]),
                ),
                (1, done(page(vec![data("t", 1)]), "t")),
                (
                    0,
                    page(vec![
                        superseded("r", 2, "@run", task(5, 1), task(5, 2)),
                        by("@run", task(5, 2), data("r", 3)),
                    ]),
                ),
            ],
        );
        assert_eq!(
            items,
            [
                "0:1",
                "1:OPENED",
                "1:1",
                "1:DONE",
                "0:2",
                "0:SUPERSEDED",
                "0:3"
            ]
        );
        assert!(state.withdrawn.is_empty());
    }

    #[test]
    fn a_join_of_a_withdrawn_open_passes_as_a_record() {
        // J2. The JOIN finds no open segment and does not take the withdrawn one back.
        let mut state = new_state();
        let items = run(
            &mut state,
            vec![
                (0, page(vec![by("act", 1, open("r", 1, "b", "child"))])),
                (
                    0,
                    page(vec![
                        superseded("r", 1, "act", 1, 2),
                        by("act", 2, data("r", 2)),
                        by("@run", task(9, 1), join("r", 3, "b", "")),
                        data("r", 4),
                    ]),
                ),
            ],
        );
        assert_eq!(
            items,
            [
                "0:1",
                "1:OPENED",
                "0:SUPERSEDED",
                "1:DONE:withdrawn",
                "0:2",
                "0:3",
                "0:4"
            ]
        );
        assert_eq!(state.readable(), [0], "the JOIN does not stop the root");
    }

    #[test]
    fn a_stale_open_does_not_resume_a_withdrawn_segment() {
        let mut state = new_state();
        let items = run(
            &mut state,
            vec![
                (0, page(vec![by("act", 1, open("r", 1, "b", "child"))])),
                (1, page(vec![data("t", 1)])),
                (
                    0,
                    page(vec![
                        superseded("r", 1, "act", 1, 2),
                        by("act", 2, data("r", 2)),
                        stale(by("act", 1, open("r", 3, "b", "child"))),
                    ]),
                ),
            ],
        );
        assert_eq!(
            items,
            [
                "0:1",
                "1:OPENED",
                "1:1",
                "0:SUPERSEDED",
                "1:DONE:withdrawn",
                "0:2",
                "0:3"
            ]
        );
        assert_eq!(
            state.withdrawn.len(),
            1,
            "the entry waits for the retry's OPEN"
        );
        let items = run(
            &mut state,
            vec![
                (0, page(vec![by("act", 2, open("r", 4, "b", "child"))])),
                (1, page(vec![data("t", 2)])),
            ],
        );
        assert_eq!(items, ["0:4", "1:OPENED:resumed", "1:2"]);
    }

    #[test]
    fn a_follow_resumed_while_the_segment_pauses_resumes_it_at_its_cursor() {
        let mut state = new_state();
        run(
            &mut state,
            vec![
                (0, page(vec![by("act", 1, open("r", 1, "b", "child"))])),
                (1, page(vec![data("t", 1), data("t", 2)])),
                (0, page(vec![superseded("r", 1, "act", 1, 2)])),
            ],
        );
        let saved = state.encode_to_vec();
        let mut state = FollowState::decode_from(&saved).unwrap();
        assert_eq!(state.withdrawn.len(), 1);
        assert_eq!(state.readable(), [0]);
        let items = run(
            &mut state,
            vec![(0, page(vec![by("act", 2, open("r", 2, "b", "child"))]))],
        );
        assert_eq!(items, ["0:2", "1:OPENED:resumed"]);
        let (_, at, _) = state.read_input(1).unwrap();
        assert_eq!(at, cursor("t", 2));
    }

    #[test]
    fn a_withdrawn_segment_pauses_and_resumes_with_the_streams_it_opened() {
        let mut state = new_state();
        let items = run(
            &mut state,
            vec![
                (0, page(vec![by("act", 1, open("r", 1, "b", "child"))])),
                (1, page(vec![open("t", 1, "g", "grandchild")])),
                (2, page(vec![data("g", 1)])),
                (0, page(vec![superseded("r", 1, "act", 1, 2)])),
            ],
        );
        assert_eq!(
            items,
            [
                "0:1",
                "1:OPENED",
                "1:1",
                "2:OPENED",
                "2:1",
                "0:SUPERSEDED",
                "2:DONE:withdrawn",
                "1:DONE:withdrawn",
            ]
        );
        let items = run(
            &mut state,
            vec![
                (0, page(vec![by("act", 2, open("r", 2, "b", "child"))])),
                (2, page(vec![data("g", 2)])),
            ],
        );
        assert_eq!(
            items,
            ["0:2", "1:OPENED:resumed", "2:OPENED:resumed", "2:2"]
        );
        assert_eq!(state.readable(), [0, 1, 2]);
    }

    #[test]
    fn a_dropped_entry_takes_the_sources_that_ended_with_it() {
        // The child waits on a JOIN of the grandchild when it pauses. The list drops the child
        // and the grandchild together, so no resumed source waits on a source that is gone.
        let mut state = FollowState::new(root(), "", 0, 2);
        let items = run(
            &mut state,
            vec![
                (0, page(vec![by("act", 1, open("r", 1, "b", "child"))])),
                (
                    1,
                    page(vec![open("t", 1, "g", "grandchild"), join("t", 2, "g", "")]),
                ),
                (0, page(vec![superseded("r", 1, "act", 1, 2)])),
                (0, page(vec![by("other", 1, open("r", 2, "c", "x"))])),
                (0, page(vec![superseded("r", 2, "other", 1, 2)])),
            ],
        );
        assert_eq!(
            items,
            [
                "0:1",
                "1:OPENED",
                "1:1",
                "2:OPENED",
                "0:SUPERSEDED",
                "2:DONE:withdrawn",
                "1:DONE:withdrawn",
                "0:2",
                "3:OPENED",
                "0:SUPERSEDED",
                "3:DONE:withdrawn",
            ]
        );
        assert_eq!(
            state
                .withdrawn
                .iter()
                .map(|entry| entry.number)
                .collect::<Vec<_>>(),
            [3]
        );
        let items = run(
            &mut state,
            vec![(0, page(vec![by("act", 2, open("r", 3, "b", "child"))]))],
        );
        assert_eq!(items, ["0:3", "4:OPENED"]);
        assert_eq!(state.readable(), [0, 4]);
    }

    #[test]
    fn the_withdrawn_list_keeps_at_most_max_open_entries() {
        let mut state = FollowState::new(root(), "", 0, 1);
        let items = run(
            &mut state,
            vec![
                (0, page(vec![by("act", 1, open("r", 1, "a", "x"))])),
                (0, page(vec![superseded("r", 1, "act", 1, 2)])),
                (0, page(vec![by("other", 1, open("r", 2, "b", "y"))])),
                (0, page(vec![superseded("r", 2, "other", 1, 2)])),
                (0, page(vec![by("act", 2, open("r", 3, "a", "x"))])),
            ],
        );
        // The oldest entry was dropped, so the retry reads its segment from `after` again.
        assert_eq!(
            items,
            [
                "0:1",
                "1:OPENED",
                "0:SUPERSEDED",
                "1:DONE:withdrawn",
                "0:2",
                "2:OPENED",
                "0:SUPERSEDED",
                "2:DONE:withdrawn",
                "0:3",
                "3:OPENED",
            ]
        );
        assert_eq!(state.withdrawn.len(), 1, "only the entry of `y` is left");
        assert_eq!(state.withdrawn[0].number, 2);
    }

    /// The entry numbers of the withdrawn list, in its order.
    fn withdrawn(state: &FollowState) -> Vec<u32> {
        state.withdrawn.iter().map(|entry| entry.number).collect()
    }

    #[test]
    fn a_resumed_source_that_waits_on_a_join_ends_and_the_follow_ends() {
        // The child waits on a JOIN of the grandchild when both pause.
        let mut state = new_state();
        let items = run(
            &mut state,
            vec![
                (0, page(vec![by("act", 1, open("r", 1, "b", "child"))])),
                (
                    1,
                    page(vec![open("t", 1, "g", "grandchild"), join("t", 2, "g", "")]),
                ),
                (2, page(vec![data("g", 1)])),
                (0, page(vec![superseded("r", 1, "act", 1, 2)])),
            ],
        );
        assert_eq!(
            items[items.len() - 3..],
            ["0:SUPERSEDED", "2:DONE:withdrawn", "1:DONE:withdrawn"]
        );
        assert_eq!(withdrawn(&state), [1, 2], "the parent's entry comes first");
        let items = run(
            &mut state,
            vec![
                (
                    0,
                    done(page(vec![by("act", 2, open("r", 2, "b", "child"))]), "end"),
                ),
                (2, done(page(vec![data("g", 2)]), "end")),
                (1, done(page(vec![data("t", 3)]), "end")),
            ],
        );
        assert_eq!(
            items,
            [
                "0:2",
                "1:OPENED:resumed",
                "2:OPENED:resumed",
                "2:2",
                "2:DONE",
                "1:2",
                "1:3",
                "1:DONE",
            ]
        );
        assert!(
            state.done(),
            "the resumed source got its JOIN back and ended"
        );
    }

    /// Asserts that no stored Record shows twice in `items`, under any source number. The
    /// whole cursor names the Stream as well as the position.
    fn each_record_once(items: &[FollowedRecord]) {
        let cursors: Vec<&str> = items
            .iter()
            .filter_map(|item| match &item.item {
                Some(followed_record::Item::Record(record))
                    if matches!(record.record, Some(read_record::Record::Stored(_))) =>
                {
                    Some(record.cursor.as_str())
                }
                _ => None,
            })
            .collect();
        let mut unique = cursors.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(
            unique.len(),
            cursors.len(),
            "a Record shows twice in {cursors:?}"
        );
    }

    /// The root opens the child as attempt 1 of `act`, the child opens the grandchild, each
    /// reads a Record, and a SUPERSEDED pauses both.
    fn paused_child_and_grandchild(
        state: &mut FollowState,
        join_grandchild: bool,
    ) -> Vec<FollowedRecord> {
        let mut child_page = vec![open("t", 1, "g", "grandchild")];
        if join_grandchild {
            child_page.push(join("t", 2, "g", ""));
        } else {
            child_page.push(data("t", 2));
        }
        run_items(
            state,
            vec![
                (0, page(vec![by("act", 1, open("r", 1, "b", "child"))])),
                (1, page(child_page)),
                (2, page(vec![data("g", 1)])),
                (0, page(vec![superseded("r", 1, "act", 1, 2)])),
            ],
        )
    }

    #[test]
    fn a_descendant_whose_stream_is_open_again_stays_ended() {
        let mut state = new_state();
        let mut items = paused_child_and_grandchild(&mut state, false);
        items.extend(run_items(
            &mut state,
            vec![
                (
                    0,
                    page(vec![by("other", 1, open("r", 2, "x", "grandchild"))]),
                ),
                (3, page(vec![data("g", 2)])),
                (0, page(vec![by("act", 2, open("r", 3, "b", "child"))])),
                (1, page(vec![data("t", 3)])),
            ],
        ));
        each_record_once(&items);
        let items = shape(&items);
        assert_eq!(
            items[items.len() - 6..],
            ["0:2", "3:OPENED", "3:2", "0:3", "1:OPENED:resumed", "1:3"],
        );
        assert_eq!(
            state.readable(),
            [0, 3, 1],
            "the grandchild is read once, as source 3"
        );
        assert!(state.withdrawn.is_empty());
    }

    #[test]
    fn a_descendant_that_stays_ended_lets_its_parent_go_on_past_the_join() {
        let mut state = new_state();
        let mut items = paused_child_and_grandchild(&mut state, true);
        items.extend(run_items(
            &mut state,
            vec![
                (
                    0,
                    page(vec![by("other", 1, open("r", 2, "x", "grandchild"))]),
                ),
                (
                    0,
                    done(page(vec![by("act", 2, open("r", 3, "b", "child"))]), "end"),
                ),
                (1, done(page(vec![data("t", 3)]), "end")),
                (3, done(page(vec![data("g", 2)]), "end")),
            ],
        ));
        each_record_once(&items);
        let items = shape(&items);
        assert_eq!(
            items[items.len() - 9..],
            [
                "0:2",
                "3:OPENED",
                "0:3",
                "1:OPENED:resumed",
                "1:2",
                "1:3",
                "1:DONE",
                "3:2",
                "3:DONE",
            ]
        );
        assert!(
            state.done(),
            "the child got its JOIN back, so the follow ends"
        );
    }

    #[test]
    fn a_resume_past_max_open_is_skipped_and_reads_nothing_again() {
        let mut state = FollowState::new(root(), "", 0, 1);
        let items = run_items(
            &mut state,
            vec![
                (0, page(vec![by("act", 1, open("r", 1, "b", "child"))])),
                (1, page(vec![data("t", 1)])),
                (0, page(vec![superseded("r", 1, "act", 1, 2)])),
                (0, page(vec![by("other", 1, open("r", 2, "c", "x"))])),
                (0, page(vec![by("act", 2, open("r", 3, "b", "child"))])),
            ],
        );
        each_record_once(&items);
        let items = shape(&items);
        assert_eq!(
            items,
            [
                "0:1",
                "1:OPENED",
                "1:1",
                "0:SUPERSEDED",
                "1:DONE:withdrawn",
                "0:2",
                "2:OPENED",
                "0:3",
                "0:SKIPPED:limit",
            ]
        );
        assert_eq!(state.readable(), [0, 2]);
        assert!(state.withdrawn.is_empty());
    }

    #[test]
    fn a_descendant_past_max_open_stays_ended() {
        let mut state = FollowState::new(root(), "", 0, 2);
        let mut items = paused_child_and_grandchild(&mut state, false);
        items.extend(run_items(
            &mut state,
            vec![
                (0, page(vec![by("other", 1, open("r", 2, "c", "x"))])),
                (0, page(vec![by("act", 2, open("r", 3, "b", "child"))])),
                (1, page(vec![data("t", 3)])),
            ],
        ));
        each_record_once(&items);
        let items = shape(&items);
        assert_eq!(
            items[items.len() - 5..],
            ["0:2", "3:OPENED", "0:3", "1:OPENED:resumed", "1:3"]
        );
        assert_eq!(state.readable(), [0, 3, 1]);
        assert!(state.withdrawn.is_empty());
    }

    #[test]
    fn an_entry_under_a_descendant_that_stays_ended_goes_with_it() {
        // The leaf was withdrawn by its own producer before the child paused. The mid Stream is
        // open again when the child resumes, so mid stays ended, and so does the leaf's entry.
        let mut state = new_state();
        run(
            &mut state,
            vec![
                (0, page(vec![by("act", 1, open("r", 1, "b", "child"))])),
                (1, page(vec![by("p", 1, open("t", 1, "m", "mid"))])),
                (2, page(vec![by("q", 1, open("m", 1, "l", "leaf"))])),
                (2, page(vec![superseded("m", 1, "q", 1, 2)])),
                (0, page(vec![superseded("r", 1, "act", 1, 2)])),
            ],
        );
        // The leaf's entry is the oldest, since its own SUPERSEDED came first.
        assert_eq!(withdrawn(&state), [3, 1, 2]);
        let items = run(
            &mut state,
            vec![
                (0, page(vec![by("other", 1, open("r", 2, "x", "mid"))])),
                (0, page(vec![by("act", 2, open("r", 3, "b", "child"))])),
            ],
        );
        assert_eq!(items, ["0:2", "4:OPENED", "0:3", "1:OPENED:resumed"]);
        assert!(
            state.withdrawn.is_empty(),
            "no entry is left that nothing can resume"
        );
    }

    #[test]
    fn descendants_come_back_oldest_first_by_source_number() {
        let mut state = FollowState::new(root(), "", 0, 4);
        run(
            &mut state,
            vec![
                (0, page(vec![by("act", 1, open("r", 1, "b", "child"))])),
                (1, page(vec![open("t", 1, "a", "first")])),
                (1, page(vec![open("t", 2, "c", "second")])),
                (2, page(vec![open("f", 1, "d", "third")])),
                (0, page(vec![superseded("r", 1, "act", 1, 2)])),
                (0, page(vec![by("other", 1, open("r", 2, "x", "x"))])),
            ],
        );
        let items = run(
            &mut state,
            vec![(0, page(vec![by("act", 2, open("r", 3, "b", "child"))]))],
        );
        // Source 4 opened last, so it is the one that stays ended at the limit.
        assert_eq!(
            items,
            [
                "0:3",
                "1:OPENED:resumed",
                "2:OPENED:resumed",
                "3:OPENED:resumed"
            ]
        );
        assert_eq!(state.readable(), [0, 5, 1, 2, 3]);
        assert!(state.withdrawn.is_empty());
    }

    #[test]
    fn a_source_under_a_descendant_that_stays_ended_stays_ended_too() {
        // The child, mid and leaf pause together. Mid is open again, so it stays ended, and the
        // leaf, which paused with mid, stays ended with it.
        let mut state = new_state();
        let mut items = run_items(
            &mut state,
            vec![
                (0, page(vec![by("act", 1, open("r", 1, "b", "child"))])),
                (1, page(vec![open("t", 1, "m", "mid")])),
                (2, page(vec![open("m", 1, "l", "leaf")])),
                (3, page(vec![data("l", 1)])),
                (0, page(vec![superseded("r", 1, "act", 1, 2)])),
            ],
        );
        assert_eq!(withdrawn(&state), [1, 2, 3]);
        items.extend(run_items(
            &mut state,
            vec![
                (0, page(vec![by("other", 1, open("r", 2, "x", "mid"))])),
                (0, page(vec![by("act", 2, open("r", 3, "b", "child"))])),
            ],
        ));
        each_record_once(&items);
        let items = shape(&items);
        assert_eq!(
            items[items.len() - 4..],
            ["0:2", "4:OPENED", "0:3", "1:OPENED:resumed"]
        );
        assert_eq!(state.readable(), [0, 4, 1]);
        assert!(state.withdrawn.is_empty());
    }

    #[test]
    fn a_descendant_past_max_open_takes_the_entries_under_it() {
        // The leaf was withdrawn by its own producer before the child paused. Mid doesn't fit
        // when the child resumes, so mid stays ended, and the leaf's entry goes with it.
        let mut state = FollowState::new(root(), "", 0, 3);
        run(
            &mut state,
            vec![
                (0, page(vec![by("act", 1, open("r", 1, "b", "child"))])),
                (1, page(vec![open("t", 1, "m", "mid")])),
                (2, page(vec![by("q", 1, open("m", 1, "l", "leaf"))])),
                (2, page(vec![superseded("m", 1, "q", 1, 2)])),
                (0, page(vec![superseded("r", 1, "act", 1, 2)])),
            ],
        );
        assert_eq!(withdrawn(&state), [3, 1, 2]);
        let items = run(
            &mut state,
            vec![
                (0, page(vec![by("other", 1, open("r", 2, "x", "x"))])),
                (0, page(vec![by("other", 1, open("r", 3, "y", "y"))])),
                (0, page(vec![by("act", 2, open("r", 4, "b", "child"))])),
            ],
        );
        assert_eq!(
            items,
            [
                "0:2",
                "4:OPENED",
                "0:3",
                "5:OPENED",
                "0:4",
                "1:OPENED:resumed"
            ]
        );
        assert_eq!(state.readable(), [0, 4, 5, 1]);
        assert!(
            state.withdrawn.is_empty(),
            "no entry is left that nothing can resume"
        );
    }

    fn stale(mut record: ReadRecord) -> ReadRecord {
        record.stale = true;
        record
    }

    #[test]
    fn a_stale_open_after_the_join_opens_nothing() {
        // Attempt 2 opened turn 1 and the Workflow joined it. Then attempt 1's late OPEN of the
        // same turn came in, and turn 2 opened the same child again.
        let mut state = new_state();
        let items = run(
            &mut state,
            vec![
                (
                    0,
                    page(vec![
                        open("r", 1, "h#1", "child"),
                        join("r", 2, "h#1", &cursor("t", 1)),
                    ]),
                ),
                (1, page(vec![data("t", 1)])),
                (
                    0,
                    page(vec![
                        stale(open("r", 3, "h#1", "child")),
                        open("r", 4, "h#2", "child"),
                        join("r", 5, "h#2", &cursor("t", 2)),
                    ]),
                ),
                (2, page(vec![data("t", 2)])),
            ],
        );
        assert_eq!(
            items,
            [
                "0:1", "1:OPENED", "1:1", "1:DONE", "0:2", "0:3", "0:4", "2:OPENED", "2:2",
                "2:DONE", "0:5",
            ]
        );
        assert_eq!(state.readable(), [0]);
    }

    #[test]
    fn a_stale_join_joins_nothing() {
        let mut state = new_state();
        let items = run(
            &mut state,
            vec![(
                0,
                page(vec![
                    open("r", 1, "tool", "tool"),
                    stale(join("r", 2, "tool", "")),
                    data("r", 3),
                ]),
            )],
        );
        assert_eq!(items, ["0:1", "1:OPENED", "0:2", "0:3"]);
        assert_eq!(state.readable(), [0, 1], "the root holds no JOIN");
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
