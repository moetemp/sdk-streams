//! Reading a stream: the producer attempts a read observes, and what it carries between calls.
//!
//! An Activity that streams half an answer and then fails leaves those records in the stream, and
//! its retry writes different ones. No store can undo the first half, so the reader is told that a
//! new attempt began and the application decides what to do. This runs over records the read
//! already observed, so it costs no round trip and every store reports a retry the same way.

use crate::{
    BEGINNING, END, StreamError, StreamResult, StreamStore, cursor_position, mint_cursor,
    proto::{
        ChainId, ReadRecord, ReadRequest, ReadResponse, StoreLatestRequest, StoreReadRequest,
        StoreReadResponse, StoredRecord, StreamRecord, StreamRecordKind, Supersession, read_record,
    },
};
use prost::Message;
use std::{collections::HashMap, time::Duration};

/// The low bits of a Workflow producer's attempt, which count the reruns of one task. The high
/// bits name the last task that completed before it.
const WORKFLOW_RETRY_BITS: u32 = 16;

/// Whether `record` withdraws what its producer's `previous` attempt wrote. `started` is the
/// WorkflowTaskStarted event id that `previous` carried.
///
/// Any producer's higher attempt is a retry, except a Workflow producer's (`@<run id>`). It moves
/// to a higher attempt with each completed task, so a later task withdraws nothing by itself. A
/// rerun of the same task withdraws. So does a later task whose list of completed tasks shows
/// that another attempt completed `previous`'s task, which happens when the rerun wrote nothing
/// to the topic. When the list doesn't reach back to that task, nothing is withdrawn.
fn supersedes(previous: i64, started: i64, record: &StreamRecord) -> bool {
    if !record.producer_id.starts_with('@') {
        return true;
    }
    let prefix = previous >> WORKFLOW_RETRY_BITS;
    if prefix == record.attempt >> WORKFLOW_RETRY_BITS {
        return true;
    }
    let completed = &record.completed_tasks;
    // The task completed after `prefix` is the one just before it in the newest-first list.
    started > 0
        && !completed.contains(&started)
        && completed
            .iter()
            .position(|&id| id == prefix)
            .is_some_and(|at| at > 0)
}

/// How many producers a read remembers the newest attempt of.
pub const MAX_TRACKED_PRODUCERS: usize = 1_000;

/// The most bytes the state a read carries takes. It crosses to lang on every call, and a
/// Workflow reader may keep it in History.
pub const MAX_STATE_BYTES: usize = 16 << 10;

/// How many records one call returns when the request leaves it to Core.
pub const DEFAULT_MAX_RECORDS: u32 = 100;

/// One stream, resolved to what its store keys it by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadTarget {
    /// The run chain that owns the stream.
    pub chain: ChainId,
    /// The topic within the chain.
    pub topic: String,
    /// The stream hash its cursors carry, from [crate::stream_hash].
    pub stream_hash: String,
}

/// What a read carries from one call to the next. Lang passes it back as opaque bytes.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct ReadState {
    /// What the read knows of each producer, by producer id. One entry holds the attempt and the
    /// last sight, so a long producer id is stored once.
    #[prost(map = "string, message", tag = "1")]
    producers: HashMap<String, Tracked>,
    /// Set by the first call, so later calls don't look up the resume record again.
    #[prost(bool, tag = "2")]
    started: bool,
    /// The first run of the chain the read follows, so later calls don't resolve it again.
    #[prost(string, tag = "3")]
    pub(crate) first_run_id: String,
    /// Set once the read learned its owner ended, so it only drains what is left.
    #[prost(bool, tag = "4")]
    pub(crate) owner_ended: bool,
    /// How long the read waits between owner checks while nothing arrives.
    #[prost(uint64, tag = "5")]
    pub(crate) check_interval_ms: u64,
    /// When the next owner check is due, in milliseconds since the Unix epoch.
    #[prost(uint64, tag = "6")]
    pub(crate) next_check_ms: u64,
    /// The topic's newest position ever when a read that has delivered nothing started. Such a
    /// read has no position, so a record retention dropped after this mark was lost to it.
    #[prost(string, tag = "7")]
    watermark: String,
    #[prost(bool, tag = "8")]
    watermark_known: bool,
    #[prost(uint64, tag = "10")]
    clock: u64,
    /// When the read first found no owner, in milliseconds since the Unix epoch, or 0 once it
    /// found one. The owner start grace counts from it.
    #[prost(uint64, tag = "11")]
    pub(crate) owner_missing_since_ms: u64,
    /// Set once the read reached the first `CLOSE` record, so it ends there.
    #[prost(bool, tag = "12")]
    closed: bool,
}

/// What a read knows of one producer.
#[derive(Clone, PartialEq, Message)]
struct Tracked {
    /// The newest attempt this read delivered.
    #[prost(int64, tag = "1")]
    attempt: i64,
    /// When this read last saw the producer, on the read's own clock, so the least recently
    /// seen one is forgotten first.
    #[prost(uint64, tag = "2")]
    seen: u64,
    /// The WorkflowTaskStarted event id the newest attempt's records carried, for a Workflow
    /// producer.
    #[prost(int64, tag = "3")]
    started: i64,
    /// The highest sequence of the newest attempt this read delivered. A record of that attempt
    /// at or below it is a copy of one already delivered.
    #[prost(int64, tag = "4")]
    sequence: i64,
}

/// What one record tells the read about its producer's attempts.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Observed {
    /// Nothing new: no producer attempt to compare, or the attempt the read already knows.
    Known,
    /// The producer's first attempt this read sees, or a newer one. A newer one carries the
    /// supersession to deliver before the record.
    Newer(Option<Supersession>),
    /// An older attempt wrote after this read delivered a newer one.
    Behind {
        /// The newest attempt the read delivered.
        newest: i64,
    },
    /// A copy of a record of the newest attempt that the read already delivered. A store without
    /// producer state lands one when a producer repeats a batch whose answer it lost.
    Copy,
}

impl ReadState {
    pub(crate) fn decode_from(state: &[u8]) -> StreamResult<Self> {
        Self::decode(state).map_err(|error| {
            StreamError::cursor(format!(
                "the read state is not one an earlier call of this read returned: {error}"
            ))
        })
    }

    /// The producers, least recently seen first.
    fn by_last_seen(&self) -> Vec<String> {
        let mut producers: Vec<(u64, &String)> = self
            .producers
            .iter()
            .map(|(producer, tracked)| (tracked.seen, producer))
            .collect();
        producers.sort_unstable();
        producers
            .into_iter()
            .map(|(_, producer)| producer.clone())
            .collect()
    }

    /// Keeps the state small however many producers a long chain had, since it crosses to lang
    /// on every call.
    fn forget_beyond(&mut self, limit: usize) {
        if self.producers.len() > limit {
            let excess = self.producers.len() - limit;
            for producer in self.by_last_seen().into_iter().take(excess) {
                self.producers.remove(&producer);
            }
        }
    }

    /// Forgets the least recently seen producers until the encoded state takes at most `bytes`.
    /// A forgotten producer's next attempt reads without `SUPERSEDED`, as past the producer cap.
    fn fit_within(&mut self, bytes: usize) {
        let mut size = self.encoded_len();
        if size <= bytes {
            return;
        }
        for producer in self.by_last_seen() {
            // Map entries encode as repeated fields, so one producer's bytes are those of a
            // state that holds only that producer.
            let tracked = self.producers.remove(&producer).unwrap_or_default();
            size -= ReadState {
                producers: HashMap::from([(producer, tracked)]),
                ..ReadState::default()
            }
            .encoded_len();
            if size <= bytes {
                break;
            }
        }
    }

    fn observe(&mut self, record: &StreamRecord) -> Observed {
        // A producer that declares no attempt has no generation to compare.
        if record.producer_id.is_empty() || record.attempt <= 0 {
            return Observed::Known;
        }
        self.clock += 1;
        let tracked = self
            .producers
            .entry(record.producer_id.clone())
            .or_default();
        tracked.seen = self.clock;
        let seen = tracked.attempt;
        if record.attempt < seen {
            return Observed::Behind { newest: seen };
        }
        if record.attempt == seen {
            // A producer sends one batch at a time and repeats it until it has an answer, so a
            // copy always comes after the record it copies.
            if record.sequence > 0 && record.sequence <= tracked.sequence {
                return Observed::Copy;
            }
            tracked.sequence = tracked.sequence.max(record.sequence);
            return Observed::Known;
        }
        let started = tracked.started;
        tracked.attempt = record.attempt;
        tracked.started = record.task_started_event_id;
        tracked.sequence = record.sequence;
        self.forget_beyond(MAX_TRACKED_PRODUCERS);
        let superseded = seen > 0 && supersedes(seen, started, record);
        Observed::Newer(superseded.then(|| Supersession {
            topic: record.topic.clone(),
            producer_id: record.producer_id.clone(),
            previous_attempt: seen,
            attempt: record.attempt,
        }))
    }
}

/// Reads one page of a stream from `store`, as the `Read` call of `StreamService` answers it.
///
/// The read turns stored records into what lang delivers. A producer's newer attempt gets a
/// `SUPERSEDED` record before it, at the cursor of the record delivered before it, so a read that
/// resumes there reports the supersession again. A Workflow producer's attempt for a later task
/// gets one only when the earlier attempt's task never completed. A record from an attempt below
/// the newest the read delivered is still delivered, marked `stale`. The attempts seen ride in
/// `state`, so this holds across calls. A read that starts from a cursor without state looks up
/// the record at the cursor first, since that record was delivered before. Only that record's
/// producer is known then, and another producer's earlier attempts are not. A read remembers at
/// most [MAX_TRACKED_PRODUCERS] producers in at most [MAX_STATE_BYTES], and the next attempt of one
/// it forgot reads without `SUPERSEDED`.
///
/// A record of a producer's newest attempt at or below the highest sequence the read delivered of
/// it is a copy, and the read drops it, on every store. A record with no sequence is never a
/// copy. A read that starts from a cursor without state knows only the cursor record's sequence,
/// so it can deliver a copy whose first copy came before the cursor.
///
/// The first `CLOSE` record of the topic, or of the chain, ends the read on every store. The read
/// answers the records before it, and later calls answer none. A `TOPIC` record is not the topic
/// reader's, so the read skips it. The read writes nothing.
///
/// A record that does not parse fails the call as `RECORD` with its cursor, so the caller resumes
/// past it on purpose. Skipping it would lose data no one hears of. The records before it in the
/// same page are returned first, and the next call fails.
pub async fn read_page(
    store: &dyn StreamStore,
    target: &ReadTarget,
    request: &ReadRequest,
) -> StreamResult<ReadResponse> {
    let mut state = ReadState::decode_from(&request.state)?;
    let (mut response, _) = read_with(store, target, request, &mut state).await?;
    response.state = state.encode_to_vec();
    Ok(response)
}

/// [read_page] with the state already decoded, for a caller that keeps fields of its own in it.
/// The answer's `state` is left empty. Also answers whether the store marked the stream closed,
/// or the read reached a `CLOSE` record.
pub(crate) async fn read_with(
    store: &dyn StreamStore,
    target: &ReadTarget,
    request: &ReadRequest,
    state: &mut ReadState,
) -> StreamResult<(ReadResponse, bool)> {
    if state.closed {
        let response = ReadResponse {
            cursor: request.after.clone(),
            ..Default::default()
        };
        return Ok((response, true));
    }
    let from_end = request.after == END;
    let position = if from_end {
        store
            .latest(StoreLatestRequest {
                chain: Some(target.chain.clone()),
                topic: target.topic.clone(),
            })
            .await?
            .position
    } else {
        cursor_position(&request.after, store.name(), &target.stream_hash)?
            .unwrap_or_default()
            .to_string()
    };
    let start = if position.is_empty() {
        BEGINNING.to_string()
    } else {
        mint_cursor(store.name(), &target.stream_hash, &position)
    };
    if !state.started && !from_end && !position.is_empty() {
        let at_cursor = store
            .record_at(&target.chain, &target.topic, &position)
            .await?;
        // A record the read can't parse was raised to the caller, who chose to resume past it.
        if let Some(record) = at_cursor.and_then(|bytes| StreamRecord::decode(&*bytes).ok()) {
            state.observe(&record);
        }
    }
    state.started = true;
    let capabilities = store.capabilities();
    if position.is_empty() && capabilities.kept_last {
        if !state.watermark_known {
            state.watermark = store.newest_ever(&target.chain, &target.topic).await?;
            state.watermark_known = true;
        } else if let Some(dropped) = store
            .dropped_after(&target.chain, &target.topic, &state.watermark)
            .await?
        {
            return Err(StreamError::expired(format!(
                "records on topic {:?} were dropped by retention while this read was behind \
                 them, before it delivered any; the newest dropped one is {dropped}",
                target.topic
            )));
        }
    } else if position.is_empty() && !state.watermark_known {
        state.watermark = store
            .latest(StoreLatestRequest {
                chain: Some(target.chain.clone()),
                topic: target.topic.clone(),
            })
            .await?
            .position;
        state.watermark_known = true;
    }

    let page_request = StoreReadRequest {
        chain: Some(target.chain.clone()),
        topic: target.topic.clone(),
        after_position: position.clone(),
        wait: request.wait,
        max_records: match request.max_records {
            0 => DEFAULT_MAX_RECORDS,
            max => max,
        },
    };
    let page = if capabilities.long_poll {
        store.read(page_request).await?
    } else {
        poll(store, page_request).await?
    };
    if !capabilities.kept_last {
        let mark = if position.is_empty() {
            &state.watermark
        } else {
            &position
        };
        check_prefix(
            store,
            target,
            mark,
            &page.records,
            capabilities.dense_positions,
        )
        .await?;
    }
    let mut closed = page.closed;
    let mut records = Vec::with_capacity(page.records.len());
    let mut previous = start;
    for stored in page.records {
        let cursor = mint_cursor(store.name(), &target.stream_hash, &stored.position);
        let record = match parse(&stored.record) {
            Ok(record) => record,
            Err(error) if records.is_empty() => {
                return Err(StreamError::record(
                    cursor.clone(),
                    format!("stream record at {cursor} could not be decoded: {error}"),
                ));
            }
            Err(_) => break,
        };
        match StreamRecordKind::try_from(record.kind) {
            // The cursor stays before the close, so a read that resumes there ends at it too.
            Ok(StreamRecordKind::Close)
                if record.topic.is_empty() || record.topic == target.topic =>
            {
                state.closed = true;
                closed = true;
                break;
            }
            Ok(StreamRecordKind::Close | StreamRecordKind::Topic) => {
                previous = cursor;
                continue;
            }
            _ => {}
        }
        let stale = match state.observe(&record) {
            Observed::Copy => {
                previous = cursor;
                continue;
            }
            Observed::Known => false,
            Observed::Newer(superseded) => {
                if let Some(superseded) = superseded {
                    records.push(ReadRecord {
                        cursor: previous.clone(),
                        record: Some(read_record::Record::Superseded(superseded)),
                        stale: false,
                    });
                }
                false
            }
            Observed::Behind { newest } => {
                tracing::warn!("{}", behind_message(&record, &previous, newest));
                true
            }
        };
        records.push(ReadRecord {
            cursor: cursor.clone(),
            record: Some(read_record::Record::Stored(record)),
            stale,
        });
        previous = cursor;
    }
    state.fit_within(MAX_STATE_BYTES);
    Ok((
        ReadResponse {
            records,
            cursor: previous,
            state: Vec::new(),
            done: false,
        },
        closed,
    ))
}

/// The first poll interval of a read on a store with no long poll, and the longest.
const POLL_FIRST: Duration = Duration::from_millis(50);
const POLL_MAX: Duration = Duration::from_secs(1);

/// Reads a page on a store that can't wait for a record, asking again with a backoff until a
/// record arrives, the stream closes or the request's wait passes.
async fn poll(
    store: &dyn StreamStore,
    request: StoreReadRequest,
) -> StreamResult<StoreReadResponse> {
    let wait = request
        .wait
        .and_then(|wait| Duration::try_from(wait).ok())
        .unwrap_or_default();
    let deadline = tokio::time::Instant::now() + wait;
    let mut interval = POLL_FIRST;
    loop {
        let page = store.read(request.clone()).await?;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if !page.records.is_empty() || page.closed || remaining.is_zero() {
            return Ok(page);
        }
        tokio::time::sleep(interval.min(remaining)).await;
        interval = (interval * 2).min(POLL_MAX);
    }
}

/// Fails as expired when retention may have dropped records after `mark` that the read had not
/// delivered, on a store that doesn't keep a topic's last position. Retention drops only the
/// oldest records, so while the record at `mark` is still there, nothing after it is gone. It
/// looks after the read, so a trim that raced the read is seen too. On a store with dense
/// positions, a page that starts right after `mark` is whole without a look. The answer can be a
/// false expired, for a reader whose mark retention dropped with nothing lost after it, but
/// never a silent skip.
async fn check_prefix(
    store: &dyn StreamStore,
    target: &ReadTarget,
    mark: &str,
    page: &[StoredRecord],
    dense: bool,
) -> StreamResult<()> {
    if mark.is_empty() {
        return Ok(());
    }
    let next = |first: &StoredRecord| {
        let (Ok(mark), Ok(first)) = (mark.parse::<u64>(), first.position.parse::<u64>()) else {
            return false;
        };
        mark.checked_add(1) == Some(first)
    };
    if dense && page.first().is_some_and(next) {
        return Ok(());
    }
    if store
        .record_at(&target.chain, &target.topic, mark)
        .await?
        .is_some()
    {
        return Ok(());
    }
    Err(StreamError::expired(format!(
        "retention dropped position {mark} on topic {:?}, so records after it may be lost to this \
         read",
        target.topic
    )))
}

/// A stored record as the read takes it. Fails on bytes that are no record, and on a kind no
/// writer stores, such as the `SUPERSEDED` that only readers synthesize.
///
/// Empty bytes are no record either. Every stored record names its topic, so it is never empty,
/// and a store hands over an entry it holds without a record as empty bytes.
fn parse(bytes: &[u8]) -> Result<StreamRecord, String> {
    if bytes.is_empty() {
        return Err("the entry holds no stream record".to_string());
    }
    let mut record = StreamRecord::decode(bytes).map_err(|error| error.to_string())?;
    match StreamRecordKind::try_from(record.kind) {
        Ok(
            StreamRecordKind::Data
            | StreamRecordKind::Finish
            | StreamRecordKind::Close
            | StreamRecordKind::Topic,
        ) => {}
        Ok(StreamRecordKind::Unspecified) => record.kind = StreamRecordKind::Data as i32,
        Err(_) => return Err(format!("no writer stores a record of kind {}", record.kind)),
    }
    Ok(record)
}

/// Why a record from an older attempt is suspect. A lower attempt after a higher one means an
/// older attempt kept writing after a newer one started, such as an Activity attempt that timed
/// out but kept running. A consumer that reads it as the current answer shows a stale one.
fn behind_message(record: &StreamRecord, previous: &str, newest: i64) -> String {
    format!(
        "stream record on {:?} after {previous:?} is from attempt {} of producer {:?}, behind \
         attempt {newest}, which this reader already delivered: an older attempt wrote after a \
         newer one started",
        record.topic, record.attempt, record.producer_id
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        proto::{
            DeleteOwnerRequest, DeleteOwnerResponse, StoreAppendRequest, StoreAppendResponse,
            StoreLatestResponse, StoreReadResponse, StoredRecord, StreamFailureKind,
        },
        stream_hash,
    };
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use temporalio_common::protos::temporal::api::common::v1::Payload;

    /// One topic of positions `1`, `2` and so on, so the read logic is tested without a store.
    #[derive(Default)]
    struct Log {
        entries: Mutex<Vec<Vec<u8>>>,
        lookups: AtomicUsize,
        /// The last sequence each producer attempt wrote.
        sequences: Mutex<HashMap<(String, i64), i64>>,
        /// How many of the oldest records retention dropped.
        trimmed: AtomicUsize,
        /// Whether the log forgets its last position, so the read decides expiry itself.
        forgets_last: bool,
    }

    impl Log {
        fn push(&self, bytes: Vec<u8>) -> String {
            let mut entries = self.entries.lock().unwrap();
            entries.push(bytes);
            entries.len().to_string()
        }

        /// Writes the attempt's next record.
        fn write(&self, producer: &str, attempt: i64, data: &[u8]) -> String {
            let mut sequences = self.sequences.lock().unwrap();
            let sequence = sequences
                .entry((producer.to_string(), attempt))
                .or_default();
            *sequence += 1;
            let sequence = *sequence;
            drop(sequences);
            self.write_at(producer, attempt, sequence, data)
        }

        fn write_at(&self, producer: &str, attempt: i64, sequence: i64, data: &[u8]) -> String {
            let record = StreamRecord {
                sequence,
                ..data_record(producer, attempt, data)
            };
            self.push(record.encode_to_vec())
        }
    }

    fn data_record(producer: &str, attempt: i64, data: &[u8]) -> StreamRecord {
        StreamRecord {
            topic: "out".to_string(),
            kind: StreamRecordKind::Data as i32,
            producer_id: producer.to_string(),
            attempt,
            sequence: 1,
            body: Some(Payload {
                data: data.to_vec(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[async_trait::async_trait]
    impl StreamStore for Log {
        fn name(&self) -> &str {
            "log"
        }

        fn capabilities(&self) -> crate::Capabilities {
            crate::Capabilities {
                kept_last: !self.forgets_last,
                ..crate::Capabilities::log_only()
            }
        }

        async fn append(&self, _: StoreAppendRequest) -> StreamResult<StoreAppendResponse> {
            unreachable!()
        }

        async fn read(&self, request: StoreReadRequest) -> StreamResult<StoreReadResponse> {
            let after: usize = request.after_position.parse().unwrap_or(0);
            let after = after.max(self.trimmed.load(Ordering::Relaxed));
            let entries = self.entries.lock().unwrap();
            let records = entries
                .iter()
                .enumerate()
                .skip(after)
                .take(request.max_records as usize)
                .map(|(index, bytes)| StoredRecord {
                    position: (index + 1).to_string(),
                    record: bytes.clone(),
                })
                .collect();
            Ok(StoreReadResponse {
                records,
                closed: false,
            })
        }

        async fn latest(&self, _: StoreLatestRequest) -> StreamResult<StoreLatestResponse> {
            let entries = self.entries.lock().unwrap();
            Ok(StoreLatestResponse {
                position: match entries.len() {
                    0 => String::new(),
                    len => len.to_string(),
                },
            })
        }

        async fn record_at(
            &self,
            _: &ChainId,
            _: &str,
            position: &str,
        ) -> StreamResult<Option<Vec<u8>>> {
            self.lookups.fetch_add(1, Ordering::Relaxed);
            let index: usize = position.parse().unwrap();
            if index <= self.trimmed.load(Ordering::Relaxed) {
                return Ok(None);
            }
            Ok(self.entries.lock().unwrap().get(index - 1).cloned())
        }

        async fn close_chain(&self, _: &ChainId) -> StreamResult<()> {
            unreachable!()
        }

        async fn close_topic(&self, _: &ChainId, _: &str) -> StreamResult<()> {
            unreachable!()
        }

        async fn retire_producer(&self, _: &ChainId, _: &str, _: &str, _: i64) -> StreamResult<()> {
            unreachable!()
        }

        async fn delete_owner(&self, _: DeleteOwnerRequest) -> StreamResult<DeleteOwnerResponse> {
            unreachable!()
        }
    }

    fn target() -> ReadTarget {
        ReadTarget {
            chain: ChainId {
                namespace: "ns".to_string(),
                workflow_id: "wf".to_string(),
                first_run_id: "run-1".to_string(),
            },
            topic: "out".to_string(),
            stream_hash: stream_hash("ns", "workflow", "wf", "out"),
        }
    }

    fn cursor(position: &str) -> String {
        mint_cursor("log", &target().stream_hash, position)
    }

    async fn read(log: &Log, after: &str, state: &[u8]) -> StreamResult<ReadResponse> {
        read_page(
            log,
            &target(),
            &ReadRequest {
                after: after.to_string(),
                state: state.to_vec(),
                ..Default::default()
            },
        )
        .await
    }

    /// Each record as `(cursor, kind, attempt, stale)`, with kind 3 for a supersession.
    fn shape(response: &ReadResponse) -> Vec<(String, i32, i64, bool)> {
        response
            .records
            .iter()
            .map(|record| match record.record.as_ref().unwrap() {
                read_record::Record::Stored(stored) => (
                    record.cursor.clone(),
                    stored.kind,
                    stored.attempt,
                    record.stale,
                ),
                read_record::Record::Superseded(superseded) => {
                    (record.cursor.clone(), 3, superseded.attempt, record.stale)
                }
            })
            .collect()
    }

    fn superseded(response: &ReadResponse, index: usize) -> &Supersession {
        match response.records[index].record.as_ref().unwrap() {
            read_record::Record::Superseded(superseded) => superseded,
            other => panic!("not a supersession: {other:?}"),
        }
    }

    const DATA: i32 = StreamRecordKind::Data as i32;
    const SUPERSEDED: i32 = 3;

    /// Observes a record with no sequence, which is never a copy.
    fn observe(state: &mut ReadState, producer: &str, attempt: i64) -> Observed {
        state.observe(&StreamRecord {
            sequence: 0,
            ..data_record(producer, attempt, b"")
        })
    }

    fn observe_at(state: &mut ReadState, producer: &str, attempt: i64, sequence: i64) -> Observed {
        state.observe(&StreamRecord {
            sequence,
            ..data_record(producer, attempt, b"")
        })
    }

    #[test]
    fn a_record_at_or_below_the_highest_sequence_of_its_attempt_is_a_copy() {
        let mut state = ReadState::default();
        assert_eq!(observe_at(&mut state, "p", 1, 1), Observed::Newer(None));
        assert_eq!(observe_at(&mut state, "p", 1, 2), Observed::Known);
        assert_eq!(observe_at(&mut state, "p", 1, 2), Observed::Copy);
        assert_eq!(observe_at(&mut state, "p", 1, 1), Observed::Copy);
        assert_eq!(observe_at(&mut state, "p", 1, 0), Observed::Known);
        // A gap is no copy, and the highest sequence moves past it.
        assert_eq!(observe_at(&mut state, "p", 1, 5), Observed::Known);
        assert_eq!(observe_at(&mut state, "p", 1, 4), Observed::Copy);
        // A new attempt counts from its own records.
        assert!(matches!(
            observe_at(&mut state, "p", 2, 1),
            Observed::Newer(Some(_))
        ));
        assert_eq!(observe_at(&mut state, "p", 2, 1), Observed::Copy);
        // An older attempt's copy is still delivered, as stale.
        assert_eq!(
            observe_at(&mut state, "p", 1, 1),
            Observed::Behind { newest: 2 }
        );
        assert_eq!(observe_at(&mut state, "q", 1, 1), Observed::Newer(None));
    }

    #[tokio::test]
    async fn a_copy_is_dropped_and_the_cursor_moves_past_it() {
        let log = Log::default();
        log.write_at("p", 1, 1, b"1");
        log.write_at("q", 1, 1, b"2");
        // `p` lost the answer to its batch and sent it again.
        log.write_at("p", 1, 1, b"1");
        log.write_at("p", 1, 2, b"3");
        let all = read(&log, BEGINNING, &[]).await.unwrap();
        assert_eq!(values(&all), ["1", "2", "3"]);
        let first = read_page(
            &log,
            &target(),
            &ReadRequest {
                after: BEGINNING.to_string(),
                max_records: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(values(&first), ["1", "2"]);
        assert_eq!(first.cursor, cursor("3"));
        let next = read(&log, &first.cursor, &first.state).await.unwrap();
        assert_eq!(values(&next), ["3"]);
    }

    #[tokio::test]
    async fn a_read_from_a_cursor_knows_the_sequence_of_the_cursor_record() {
        let log = Log::default();
        log.write_at("p", 1, 1, b"1");
        log.write_at("p", 1, 2, b"2");
        log.write_at("p", 1, 2, b"2");
        log.write_at("p", 1, 1, b"1");
        let resumed = read(&log, &cursor("2"), &[]).await.unwrap();
        assert!(resumed.records.is_empty());
        assert_eq!(resumed.cursor, cursor("4"));
        // A cursor before the first copy delivers its batch once.
        let earlier = read(&log, &cursor("1"), &[]).await.unwrap();
        assert_eq!(values(&earlier), ["2"]);
    }

    fn control(kind: StreamRecordKind, topic: &str) -> Vec<u8> {
        StreamRecord {
            topic: topic.to_string(),
            kind: kind as i32,
            producer_id: "closer".to_string(),
            attempt: 1,
            ..Default::default()
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn the_first_close_ends_the_read_and_every_later_call() {
        let log = Log::default();
        log.write("p", 1, b"1");
        log.push(control(StreamRecordKind::Topic, "out"));
        log.write("p", 1, b"2");
        log.push(control(StreamRecordKind::Close, "out"));
        log.write("p", 1, b"3");
        log.push(control(StreamRecordKind::Close, "out"));
        let mut state = ReadState::default();
        let request = ReadRequest {
            after: BEGINNING.to_string(),
            ..Default::default()
        };
        let (page, closed) = read_with(&log, &target(), &request, &mut state)
            .await
            .unwrap();
        assert!(closed);
        assert_eq!(values(&page), ["1", "2"]);
        // The cursor stays before the close, so a read that resumes there ends at it too.
        assert_eq!(page.cursor, cursor("3"));
        let lookups = log.lookups.load(Ordering::Relaxed);
        let next = ReadRequest {
            after: page.cursor.clone(),
            ..Default::default()
        };
        let (again, closed) = read_with(&log, &target(), &next, &mut state).await.unwrap();
        assert!(closed && again.records.is_empty());
        assert_eq!(log.lookups.load(Ordering::Relaxed), lookups);
        let resumed = read(&log, &page.cursor, &[]).await.unwrap();
        assert!(resumed.records.is_empty());
        let state = ReadState::decode_from(&resumed.state).unwrap();
        assert!(state.closed);
    }

    #[tokio::test]
    async fn a_chain_close_ends_the_read_and_another_topics_close_does_not() {
        let log = Log::default();
        log.write("p", 1, b"1");
        log.push(control(StreamRecordKind::Close, "other"));
        log.write("p", 1, b"2");
        log.push(control(StreamRecordKind::Close, ""));
        log.write("p", 1, b"3");
        let read = read(&log, BEGINNING, &[]).await.unwrap();
        assert_eq!(values(&read), ["1", "2"]);
    }

    fn forgetful() -> Log {
        Log {
            forgets_last: true,
            ..Log::default()
        }
    }

    #[tokio::test]
    async fn without_a_kept_last_position_a_lost_cursor_record_reads_as_expired() {
        let log = forgetful();
        for value in ["1", "2", "3", "4"] {
            log.write("p", 1, value.as_bytes());
        }
        let first = read_page(
            &log,
            &target(),
            &ReadRequest {
                max_records: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(first.cursor, cursor("1"));
        // While the record at the cursor is there, nothing after it is gone.
        log.trimmed.store(1, Ordering::Relaxed);
        let error = read(&log, &first.cursor, &first.state).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Expired);
        // A position the store still holds reads on.
        let held = read(&log, &cursor("2"), &[]).await.unwrap();
        assert_eq!(values(&held), ["3", "4"]);
        // A quiet stream whose last delivered record expired reads as expired too, where a store
        // that keeps the last position answers empty.
        log.trimmed.store(4, Ordering::Relaxed);
        let quiet = read(&log, &cursor("4"), &[]).await.unwrap_err();
        assert_eq!(quiet.kind, StreamFailureKind::Expired);
    }

    #[tokio::test]
    async fn without_a_kept_last_position_a_read_from_the_start_keeps_a_mark() {
        let log = forgetful();
        let empty = read(&log, BEGINNING, &[]).await.unwrap();
        assert!(empty.records.is_empty());
        log.write("p", 1, b"1");
        let next = read(&log, &empty.cursor, &empty.state).await.unwrap();
        assert_eq!(values(&next), ["1"]);
    }

    #[tokio::test]
    async fn on_dense_positions_a_page_right_after_a_lost_cursor_is_whole() {
        let store = crate::MemoryStore::with_capabilities(crate::Capabilities {
            kept_last: false,
            ..crate::MemoryStore::new().capabilities()
        });
        let chain = target().chain;
        for sequence in 1..=4 {
            let record = StreamRecord {
                sequence,
                ..data_record("p", 1, sequence.to_string().as_bytes())
            };
            store
                .append(StoreAppendRequest {
                    chain: Some(chain.clone()),
                    topic: "out".to_string(),
                    producer_id: "p".to_string(),
                    attempt: 1,
                    sequence,
                    digest: vec![1],
                    records: vec![record.encode_to_vec()],
                })
                .await
                .unwrap();
        }
        let at = |position: &str| mint_cursor("memory", &target().stream_hash, position);
        let read = async |after: &str| {
            read_page(
                &store,
                &target(),
                &ReadRequest {
                    after: after.to_string(),
                    ..Default::default()
                },
            )
            .await
        };
        store.truncate(&chain, "out", 3);
        assert_eq!(values(&read(&at("0")).await.unwrap()), ["2", "3", "4"]);
        store.truncate(&chain, "out", 2);
        let error = read(&at("0")).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Expired);
    }

    fn values(response: &ReadResponse) -> Vec<String> {
        response
            .records
            .iter()
            .filter_map(|record| match record.record.as_ref().unwrap() {
                read_record::Record::Stored(stored) => {
                    Some(String::from_utf8(stored.body.clone().unwrap().data).unwrap())
                }
                read_record::Record::Superseded(_) => None,
            })
            .collect()
    }

    /// The reader rules every SDK checks itself against.
    #[tokio::test]
    async fn the_reader_rules_match_the_shared_vectors() {
        let vectors: serde_json::Value =
            serde_json::from_str(include_str!("../testdata/reader_rules.json")).unwrap();
        for case in vectors["cases"].as_array().unwrap() {
            let label = case["label"].as_str().unwrap();
            let log = Log::default();
            for (index, record) in case["records"].as_array().unwrap().iter().enumerate() {
                let stored = StreamRecord {
                    topic: record["topic"].as_str().unwrap_or("out").to_string(),
                    kind: record["kind"].as_i64().unwrap_or(1) as i32,
                    producer_id: record["producer"].as_str().unwrap().to_string(),
                    attempt: record["attempt"].as_i64().unwrap(),
                    sequence: record["sequence"].as_i64().unwrap(),
                    body: Some(Payload {
                        data: index.to_string().into_bytes(),
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                log.push(stored.encode_to_vec());
            }
            let after = match case["resume_after"].as_u64() {
                Some(index) => cursor(&(index + 1).to_string()),
                None => BEGINNING.to_string(),
            };
            let mut state = ReadState::default();
            let request = ReadRequest {
                after,
                ..Default::default()
            };
            let (page, closed) = read_with(&log, &target(), &request, &mut state)
                .await
                .unwrap();
            assert_eq!(closed, case["closed"].as_bool().unwrap_or(false), "{label}");
            let delivered: Vec<u64> = page
                .records
                .iter()
                .filter_map(|record| match record.record.as_ref().unwrap() {
                    read_record::Record::Stored(stored) => {
                        let index = String::from_utf8(stored.body.clone().unwrap().data);
                        Some(index.unwrap().parse().unwrap())
                    }
                    read_record::Record::Superseded(_) => None,
                })
                .collect();
            let expected: Vec<u64> = case["delivered"]
                .as_array()
                .unwrap()
                .iter()
                .map(|index| index.as_u64().unwrap())
                .collect();
            assert_eq!(delivered, expected, "{label}");
            let stale: Vec<u64> = page
                .records
                .iter()
                .filter(|record| record.stale)
                .map(|record| match record.record.as_ref().unwrap() {
                    read_record::Record::Stored(stored) => {
                        let index = String::from_utf8(stored.body.clone().unwrap().data);
                        index.unwrap().parse().unwrap()
                    }
                    read_record::Record::Superseded(_) => unreachable!(),
                })
                .collect();
            let expected_stale: Vec<u64> = case["stale"]
                .as_array()
                .map(|indexes| indexes.iter().map(|i| i.as_u64().unwrap()).collect())
                .unwrap_or_default();
            assert_eq!(stale, expected_stale, "{label}");
        }
    }

    #[test]
    fn supersession_is_synthesized_from_observations() {
        let mut state = ReadState::default();
        assert_eq!(observe(&mut state, "model", 1), Observed::Newer(None));
        assert_eq!(observe(&mut state, "model", 1), Observed::Known);
        assert_eq!(
            observe(&mut state, "model", 2),
            Observed::Newer(Some(Supersession {
                topic: "out".to_string(),
                producer_id: "model".to_string(),
                previous_attempt: 1,
                attempt: 2,
            }))
        );
        assert_eq!(observe(&mut state, "model", 2), Observed::Known);
        // Without an id or an attempt there is no generation to compare.
        assert_eq!(observe(&mut state, "", 5), Observed::Known);
        assert_eq!(observe(&mut state, "other", 0), Observed::Known);
        assert_eq!(state.producers.len(), 1);
        assert_eq!(state.producers["model"].attempt, 2);
    }

    #[test]
    fn an_attempt_that_goes_backwards_is_reported() {
        let mut state = ReadState::default();
        observe(&mut state, "model", 2);
        assert_eq!(
            observe(&mut state, "model", 1),
            Observed::Behind { newest: 2 }
        );
        // The newest attempt stays, so a later record of attempt 2 is not new.
        assert_eq!(observe(&mut state, "model", 2), Observed::Known);
        let message = behind_message(&data_record("model", 1, b""), "log:x:3", 2);
        assert!(message.contains("attempt 1"), "{message}");
        assert!(message.contains("behind attempt 2"), "{message}");
        assert!(message.contains("\"model\""), "{message}");
    }

    #[test]
    fn rule_6_1_a_read_forgets_the_producer_it_saw_least_recently() {
        let mut state = ReadState::default();
        for producer in 0..MAX_TRACKED_PRODUCERS {
            observe(&mut state, &format!("p{producer}"), 1);
        }
        // Seen again, so `p0` is now the most recent and `p1` the least.
        observe(&mut state, "p0", 1);
        observe(&mut state, "new", 1);
        assert_eq!(state.producers.len(), MAX_TRACKED_PRODUCERS);
        assert!(state.producers.contains_key("p0"));
        assert!(!state.producers.contains_key("p1"));
        // A forgotten producer's next attempt reads as its first, without `SUPERSEDED`.
        assert_eq!(observe(&mut state, "p1", 2), Observed::Newer(None));
        assert!(matches!(
            observe(&mut state, "p0", 2),
            Observed::Newer(Some(_))
        ));
        // The bound holds across calls, since it rides in the state.
        let state = ReadState::decode_from(&state.encode_to_vec()).unwrap();
        assert_eq!(state.producers.len(), MAX_TRACKED_PRODUCERS);
    }

    /// A producer id as a Workflow's per-call Local Activity writes as.
    fn local_call(number: usize) -> String {
        format!("__temporal_local_{number}@0b0e5bd5-1c4b-4f4e-9d1e-6f3b7a8c9d0e")
    }

    #[test]
    fn a_producer_costs_its_id_once() {
        let mut state = ReadState::default();
        observe(&mut state, &local_call(0), 1_234_567_890_123);
        let one = state.encoded_len();
        // The id, the attempt and the last sight, with a few bytes of framing.
        assert!(one <= local_call(0).len() + 24, "{one}");
    }

    #[test]
    fn a_state_fits_its_budget_by_forgetting_the_least_recently_seen() {
        let mut state = ReadState::default();
        for producer in 0..MAX_TRACKED_PRODUCERS {
            observe(&mut state, &format!("{producer:0200}"), 1);
        }
        let started = std::time::Instant::now();
        state.fit_within(MAX_STATE_BYTES);
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        let size = state.encoded_len();
        assert!(size <= MAX_STATE_BYTES, "{size}");
        // It forgot no more than it had to.
        let one = size / state.producers.len();
        assert!(size + one > MAX_STATE_BYTES, "{size}");
        let oldest_kept = MAX_TRACKED_PRODUCERS - state.producers.len();
        assert!(state.producers.contains_key(&format!("{oldest_kept:0200}")));
        assert!(
            !state
                .producers
                .contains_key(&format!("{:0200}", oldest_kept - 1))
        );
    }

    #[tokio::test]
    async fn a_read_hands_back_a_state_within_its_budget() {
        let log = Log::default();
        let mut state = Vec::new();
        let mut after = BEGINNING.to_string();
        // A Workflow that flushes as a new Local Activity call each time.
        for call in 0..MAX_TRACKED_PRODUCERS {
            log.write(&local_call(call), 1_234_567_890_123, b"x");
            if call % 100 == 99 {
                let page = read(&log, &after, &state).await.unwrap();
                assert!(page.state.len() <= MAX_STATE_BYTES, "{}", page.state.len());
                (after, state) = (page.cursor, page.state);
            }
        }
        let kept = ReadState::decode_from(&state).unwrap();
        assert!(
            kept.producers
                .contains_key(&local_call(MAX_TRACKED_PRODUCERS - 1))
        );
        assert!(!kept.producers.contains_key(&local_call(0)));
    }

    /// The vectors every SDK checks its attempt bits and supersession rule against.
    fn vectors() -> serde_json::Value {
        serde_json::from_str(include_str!("../testdata/workflow_attempts.json")).unwrap()
    }

    #[test]
    fn a_workflow_producer_withdraws_an_attempt_whose_task_never_completed() {
        let vectors = vectors();
        assert_eq!(vectors["retry_bits"], WORKFLOW_RETRY_BITS);
        let written = |written: &serde_json::Value| StreamRecord {
            task_started_event_id: written["task_started_event_id"].as_i64().unwrap(),
            completed_tasks: written["completed_tasks"]
                .as_array()
                .map(|ids| ids.iter().map(|id| id.as_i64().unwrap()).collect())
                .unwrap_or_default(),
            ..data_record("@run", written["attempt"].as_i64().unwrap(), b"")
        };
        for case in vectors["supersession"].as_array().unwrap() {
            let mut state = ReadState::default();
            state.observe(&written(&case["previous"]));
            let observed = state.observe(&written(&case["next"]));
            assert_eq!(
                matches!(observed, Observed::Newer(Some(_))),
                case["superseded"].as_bool().unwrap(),
                "{}",
                case["label"]
            );
        }
        for case in vectors["attempts"].as_array().unwrap() {
            let attempt = case["attempt"].as_i64().unwrap();
            assert_eq!(
                attempt >> WORKFLOW_RETRY_BITS,
                case["prefix"].as_i64().unwrap()
            );
        }
    }

    #[test]
    fn a_workflow_producer_supersedes_only_within_one_task() {
        let task = |prefix: i64, retry: i64| (prefix << 16) | retry;
        let mut state = ReadState::default();
        assert_eq!(
            observe(&mut state, "@run", task(0, 1)),
            Observed::Newer(None)
        );
        // A completed task moves the prefix, so the next task goes on with no supersession.
        assert_eq!(
            observe(&mut state, "@run", task(3, 1)),
            Observed::Newer(None)
        );
        assert_eq!(
            observe(&mut state, "@run", task(3, 2)),
            Observed::Newer(Some(Supersession {
                topic: "out".to_string(),
                producer_id: "@run".to_string(),
                previous_attempt: task(3, 1),
                attempt: task(3, 2),
            }))
        );
        assert_eq!(
            observe(&mut state, "@run", task(9, 1)),
            Observed::Newer(None)
        );
        // An earlier task's record after a later one is still behind.
        assert_eq!(
            observe(&mut state, "@run", task(3, 2)),
            Observed::Behind { newest: task(9, 1) }
        );
        // Any other producer's higher attempt is a retry, whatever its bits hold.
        observe(&mut state, "model", task(0, 1));
        assert!(matches!(
            observe(&mut state, "model", task(3, 1)),
            Observed::Newer(Some(_))
        ));
    }

    #[test]
    fn a_repeat_of_the_current_attempt_is_not_reported() {
        let mut state = ReadState::default();
        observe(&mut state, "model", 1);
        assert_eq!(observe(&mut state, "model", 1), Observed::Known);
    }

    #[tokio::test]
    async fn a_new_attempt_supersedes_the_old_one() {
        let log = Log::default();
        log.write("model", 1, b"1");
        log.write("model", 2, b"2");
        let read = read(&log, BEGINNING, &[]).await.unwrap();
        // The supersession sits at the cursor of the old attempt's last record.
        assert_eq!(
            shape(&read),
            [
                (cursor("1"), DATA, 1, false),
                (cursor("1"), SUPERSEDED, 2, false),
                (cursor("2"), DATA, 2, false),
            ]
        );
        assert_eq!(
            superseded(&read, 1),
            &Supersession {
                topic: "out".to_string(),
                producer_id: "model".to_string(),
                previous_attempt: 1,
                attempt: 2,
            }
        );
        assert_eq!(read.cursor, cursor("2"));
    }

    #[tokio::test]
    async fn rule_5_1_a_read_resumed_at_a_supersession_reports_it_again() {
        let log = Log::default();
        log.write("model", 1, b"1");
        log.write("model", 2, b"2");
        let resumed = read(&log, &cursor("1"), &[]).await.unwrap();
        assert_eq!(
            shape(&resumed),
            [
                (cursor("1"), SUPERSEDED, 2, false),
                (cursor("2"), DATA, 2, false),
            ]
        );
    }

    #[tokio::test]
    async fn rule_5_1_a_read_resumed_before_a_new_attempt_reports_it() {
        let log = Log::default();
        log.write("model", 1, b"1");
        let delivered = log.write("model", 1, b"2");
        // The reader stops here, then the producer's retry writes.
        log.write("model", 2, b"3");
        let resumed = read(&log, &cursor(&delivered), &[]).await.unwrap();
        assert_eq!(
            shape(&resumed),
            [
                (cursor("2"), SUPERSEDED, 2, false),
                (cursor("3"), DATA, 2, false),
            ]
        );
        assert_eq!(superseded(&resumed, 0).previous_attempt, 1);
    }

    #[tokio::test]
    async fn rule_5_1_only_the_producer_at_the_resume_cursor_is_known() {
        let log = Log::default();
        log.write("a", 1, b"1");
        log.write("b", 1, b"2");
        log.write("a", 2, b"3");
        let resumed = read(&log, &cursor("2"), &[]).await.unwrap();
        assert_eq!(shape(&resumed), [(cursor("3"), DATA, 2, false)]);
    }

    #[tokio::test]
    async fn rule_5_1_the_state_carries_attempts_across_calls() {
        let log = Log::default();
        log.write("a", 1, b"1");
        log.write("b", 1, b"2");
        let first = read(&log, BEGINNING, &[]).await.unwrap();
        log.write("a", 2, b"3");
        let next = read(&log, &first.cursor, &first.state).await.unwrap();
        // Producer `a` is not at the cursor, so only the state knows its first attempt.
        assert_eq!(
            shape(&next),
            [
                (cursor("2"), SUPERSEDED, 2, false),
                (cursor("3"), DATA, 2, false),
            ]
        );
        // A read with state never looks up the record at its cursor again.
        assert_eq!(log.lookups.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn a_read_that_resumes_looks_up_its_cursor_once() {
        let log = Log::default();
        log.write("a", 1, b"1");
        let first = read(&log, &cursor("1"), &[]).await.unwrap();
        assert!(first.records.is_empty());
        assert_eq!(first.cursor, cursor("1"));
        read(&log, &first.cursor, &first.state).await.unwrap();
        assert_eq!(log.lookups.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn rule_11_1_a_record_from_an_older_attempt_is_marked_stale() {
        let log = Log::default();
        log.write("model", 2, b"2");
        log.write("model", 1, b"1");
        log.write("model", 2, b"3");
        let read = read(&log, BEGINNING, &[]).await.unwrap();
        // Still delivered, because dropping it would hide what the store holds, but marked, so
        // the consumer can keep it out of the current answer.
        assert_eq!(
            shape(&read),
            [
                (cursor("1"), DATA, 2, false),
                (cursor("2"), DATA, 1, true),
                (cursor("3"), DATA, 2, false),
            ]
        );
    }

    #[tokio::test]
    async fn rule_11_1_a_stale_record_stays_stale_after_a_resume() {
        let log = Log::default();
        log.write("model", 2, b"2");
        let first = read(&log, BEGINNING, &[]).await.unwrap();
        log.write("model", 1, b"1");
        let next = read(&log, &first.cursor, &first.state).await.unwrap();
        assert_eq!(shape(&next), [(cursor("2"), DATA, 1, true)]);
    }

    #[tokio::test]
    async fn rule_5_2_a_record_that_does_not_parse_fails_with_its_cursor() {
        let log = Log::default();
        let bad = log.push(b"\xff not a record".to_vec());
        log.write("p", 1, b"7");
        let error = read(&log, BEGINNING, &[]).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Record);
        assert_eq!(error.cursor, Some(cursor(&bad)));
        assert!(error.message.contains(&cursor(&bad)), "{error}");
        // Resuming past it is the caller's choice, and the next record reads.
        let after = read(&log, &cursor(&bad), &[]).await.unwrap();
        assert_eq!(shape(&after), [(cursor("2"), DATA, 1, false)]);
    }

    #[tokio::test]
    async fn rule_5_2_records_before_a_bad_one_are_delivered_first() {
        let log = Log::default();
        log.write("p", 1, b"1");
        let bad = log.push(b"\xff".to_vec());
        log.write("p", 1, b"3");
        let first = read(&log, BEGINNING, &[]).await.unwrap();
        assert_eq!(shape(&first), [(cursor("1"), DATA, 1, false)]);
        let error = read(&log, &first.cursor, &first.state).await.unwrap_err();
        assert_eq!(error.cursor, Some(cursor(&bad)));
        let after = read(&log, &cursor(&bad), &first.state).await.unwrap();
        assert_eq!(shape(&after), [(cursor("3"), DATA, 1, false)]);
    }

    #[tokio::test]
    async fn a_kind_no_writer_stores_fails_as_a_record_error() {
        let log = Log::default();
        let mut synthesized = data_record("p", 1, b"1");
        synthesized.kind = SUPERSEDED;
        let bad = log.push(synthesized.encode_to_vec());
        let error = read(&log, BEGINNING, &[]).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Record);
        assert_eq!(error.cursor, Some(cursor(&bad)));
        assert!(error.message.contains("kind 3"), "{error}");
    }

    #[tokio::test]
    async fn rule_20_4_an_entry_without_a_record_fails_with_its_cursor() {
        let log = Log::default();
        log.write("p", 1, b"1");
        let empty = log.push(Vec::new());
        log.write("p", 1, b"3");
        let first = read(&log, BEGINNING, &[]).await.unwrap();
        assert_eq!(shape(&first), [(cursor("1"), DATA, 1, false)]);
        let error = read(&log, &first.cursor, &first.state).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Record);
        assert_eq!(error.cursor, Some(cursor(&empty)));
        let after = read(&log, &cursor(&empty), &first.state).await.unwrap();
        assert_eq!(shape(&after), [(cursor("3"), DATA, 1, false)]);
    }

    #[tokio::test]
    async fn an_unspecified_kind_reads_as_data() {
        let log = Log::default();
        let mut unspecified = data_record("p", 1, b"1");
        unspecified.kind = StreamRecordKind::Unspecified as i32;
        log.push(unspecified.encode_to_vec());
        let read = read(&log, BEGINNING, &[]).await.unwrap();
        assert_eq!(shape(&read), [(cursor("1"), DATA, 1, false)]);
    }

    #[tokio::test]
    async fn a_read_from_the_end_resolves_its_cursor() {
        let log = Log::default();
        let empty = read(&log, END, &[]).await.unwrap();
        assert_eq!(empty.cursor, BEGINNING);
        log.write("model", 1, b"1");
        let resolved = read(&log, END, &[]).await.unwrap();
        assert!(resolved.records.is_empty());
        assert_eq!(resolved.cursor, cursor("1"));
        // The end is not a delivered record, so nothing is looked up.
        assert_eq!(log.lookups.load(Ordering::Relaxed), 0);
        log.write("model", 2, b"2");
        let next = read(&log, &resolved.cursor, &resolved.state).await.unwrap();
        assert_eq!(shape(&next), [(cursor("2"), DATA, 2, false)]);
    }

    #[tokio::test]
    async fn a_page_takes_the_default_size_when_the_request_leaves_it() {
        let log = Log::default();
        for n in 0..=DEFAULT_MAX_RECORDS {
            log.write("p", 1, n.to_string().as_bytes());
        }
        let read = read(&log, BEGINNING, &[]).await.unwrap();
        assert_eq!(read.records.len(), DEFAULT_MAX_RECORDS as usize);
    }

    #[tokio::test]
    async fn a_cursor_or_state_from_elsewhere_is_refused() {
        let log = Log::default();
        log.write("p", 1, b"1");
        let other = mint_cursor("log", &stream_hash("ns", "workflow", "wf", "other"), "1");
        let error = read(&log, &other, &[]).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Cursor);
        let error = read(&log, BEGINNING, b"\xff").await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Cursor);
        assert!(error.message.contains("read state"), "{error}");
    }
}
