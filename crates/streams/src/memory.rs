//! The in-process store, for tests.
//!
//! It holds everything in process memory, so nothing survives the process and it is no store for
//! production. It exists so the conformance suite runs the whole store contract without a server,
//! and to show in one file what a store owes. Positions are offsets that are never reused, so a
//! position keeps naming the same record after [MemoryStore::truncate] drops older ones.
//!
//! [MemoryStore::with_capabilities] masks capabilities off, so the fallbacks run in tests.

use crate::{
    ACTIVITY_OWNER_KIND, ARCHIVE_HOLDER, ArchiveMark, Capabilities, ConditionForms,
    MAX_BATCH_BYTES, MAX_RECORD_BYTES, NotificationEntry, NotificationLog, StreamError,
    StreamResult, StreamStore, WORKFLOW_ACTIVITY_OWNER_KIND, WORKFLOW_OWNER_KIND, chain_owner,
    proto::{
        ChainId, DeleteOwnerRequest, DeleteOwnerResponse, StoreAppendRequest, StoreAppendResponse,
        StoreLatestRequest, StoreLatestResponse, StoreReadRequest, StoreReadResponse, StoredClose,
        StoredRecord, StreamOwnerKind,
    },
    record::part,
};
use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::watch, time::Instant};

const NAME: &str = "memory";

/// Every capability the memory store has.
const FULL: Capabilities = Capabilities {
    producer_state: true,
    conditional: ConditionForms {
        tail: false,
        fence: false,
    },
    kept_last: true,
    holds: true,
    change_feed: true,
    cheap_logs: true,
    delete_owner: true,
    topic_listing: true,
    long_poll: true,
    dense_positions: true,
    replay_horizon: None,
    max_record_bytes: MAX_RECORD_BYTES,
    max_batch_bytes: MAX_BATCH_BYTES,
};

/// A chain's namespace, owner kind, owner id and first run.
type ChainKey = (String, &'static str, String, String);

fn chain_key(chain: &ChainId) -> ChainKey {
    let (kind, id) = chain_owner(chain);
    (
        chain.namespace.clone(),
        kind,
        id.to_string(),
        chain.first_run_id.clone(),
    )
}

/// The newest batch one producer attempt wrote.
#[derive(Debug, Clone)]
struct Held {
    sequence: i64,
    count: i64,
    first: u64,
    digest: Vec<u8>,
}

#[derive(Debug, Default)]
struct Topic {
    /// The offset of the first retained record.
    base: u64,
    records: VecDeque<Vec<u8>>,
    /// One entry per producer attempt, so the state stays bounded however long a producer writes.
    held: HashMap<(String, i64), Held>,
    closed: bool,
    /// The first close of the topic, which later closes keep.
    close: Option<StoredClose>,
    /// The newest offset the archive holds.
    archived: Option<u64>,
}

impl Topic {
    fn head(&self) -> u64 {
        self.base + self.records.len() as u64
    }

    fn push(&mut self, record: Vec<u8>) -> u64 {
        self.records.push_back(record);
        self.head() - 1
    }

    fn at(&self, offset: u64) -> Option<&Vec<u8>> {
        offset
            .checked_sub(self.base)
            .and_then(|index| self.records.get(index as usize))
    }
}

#[derive(Debug, Default)]
struct Chain {
    topics: HashMap<String, Topic>,
    closed: bool,
    /// Per topic, the position each positions-only reader holds the records after. `None` holds
    /// the whole topic.
    floors: HashMap<String, HashMap<String, Option<u64>>>,
    /// The topics producers wrote, for a Workflow Activity chain only.
    written: BTreeSet<String>,
}

/// A [StreamStore] in process memory. For tests only.
///
/// Share one instance between every client and Worker of a test, since two instances share
/// nothing.
#[derive(Debug)]
pub struct MemoryStore {
    chains: Mutex<HashMap<ChainKey, Chain>>,
    /// Bumped on every write, so a waiting read checks again.
    written: watch::Sender<u64>,
    log: Option<Arc<MemoryLog>>,
    /// Whether a topic's first append makes the archive's hold, from the topic's start.
    archive_hold: bool,
    capabilities: Capabilities,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self {
            chains: Mutex::default(),
            written: watch::Sender::new(0),
            log: None,
            archive_hold: false,
            capabilities: FULL,
        }
    }
}

/// A notification log in memory: one partition and one doorbell.
#[derive(Debug)]
struct MemoryLog {
    state: Mutex<LogState>,
    /// Bumped on every entry, so a waiting driver looks again.
    rung: watch::Sender<u64>,
}

#[derive(Debug, Default)]
struct LogState {
    next_id: u64,
    items: VecDeque<LogItem>,
    /// The topics with records to archive, with the time of each one's newest write.
    marks: HashMap<(ChainKey, String), ArchiveMark>,
}

#[derive(Debug)]
struct LogItem {
    entry: NotificationEntry,
    /// The consumer that took the entry and when, until an ack drops it.
    taken: Option<(String, Instant)>,
}

impl MemoryLog {
    fn push(&self, chain: &ChainId, topic: &str, position: String) {
        let mut state = self.state.lock().unwrap();
        state.next_id += 1;
        let id = state.next_id.to_string();
        state.items.push_back(LogItem {
            entry: NotificationEntry {
                id,
                chain: chain.clone(),
                topic: topic.to_string(),
                position,
            },
            taken: None,
        });
        drop(state);
        self.rung.send_modify(|rung| *rung += 1);
    }

    fn mark(&self, chain: &ChainId, topic: &str, written_ms: u64) {
        self.state.lock().unwrap().marks.insert(
            (chain_key(chain), topic.to_string()),
            ArchiveMark {
                chain: chain.clone(),
                topic: topic.to_string(),
                written_ms,
                idle_ms: 0,
            },
        );
    }

    fn unmark(&self, chain: &ChainId, topic: &str) {
        self.state
            .lock()
            .unwrap()
            .marks
            .remove(&(chain_key(chain), topic.to_string()));
    }

    fn holds_entries(&self) -> bool {
        !self.state.lock().unwrap().items.is_empty()
    }
}

#[async_trait::async_trait]
impl NotificationLog for MemoryLog {
    fn partitions(&self) -> u32 {
        1
    }

    fn doorbells(&self) -> u32 {
        1
    }

    async fn wait_doorbell(
        &self,
        _doorbell: u32,
        _consumer: &str,
        block: Duration,
    ) -> StreamResult<Vec<u32>> {
        let mut rung = self.rung.subscribe();
        if !self.holds_entries() {
            let _ = tokio::time::timeout(block, rung.changed()).await;
        }
        Ok(if self.holds_entries() {
            vec![0]
        } else {
            vec![]
        })
    }

    async fn take(
        &self,
        _partition: u32,
        consumer: &str,
        claim_idle: Duration,
        max: usize,
    ) -> StreamResult<Vec<NotificationEntry>> {
        let now = Instant::now();
        let mut state = self.state.lock().unwrap();
        let mut taken = vec![];
        for item in state.items.iter_mut() {
            if taken.len() >= max {
                break;
            }
            let free = match &item.taken {
                None => true,
                Some((owner, _)) if owner == consumer => true,
                Some((_, at)) => now.duration_since(*at) >= claim_idle,
            };
            if free {
                item.taken = Some((consumer.to_string(), now));
                taken.push(item.entry.clone());
            }
        }
        Ok(taken)
    }

    async fn ack(&self, _partition: u32, ids: &[String]) -> StreamResult<()> {
        self.state
            .lock()
            .unwrap()
            .items
            .retain(|item| !ids.contains(&item.entry.id));
        Ok(())
    }

    async fn archive_marks(&self, max: usize) -> StreamResult<Vec<ArchiveMark>> {
        let state = self.state.lock().unwrap();
        let mut marks: Vec<ArchiveMark> = state.marks.values().cloned().collect();
        marks.sort_by_key(|mark| mark.written_ms);
        marks.truncate(max);
        let now = unix_ms();
        for mark in &mut marks {
            mark.idle_ms = now.saturating_sub(mark.written_ms);
        }
        Ok(marks)
    }

    async fn busy(&self) -> StreamResult<Vec<u32>> {
        Ok(if self.holds_entries() {
            vec![0]
        } else {
            vec![]
        })
    }
}

impl MemoryStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty store whose appends write a notification log in the same step.
    pub fn with_notification_log() -> Self {
        Self {
            log: Some(Arc::new(MemoryLog {
                state: Mutex::default(),
                rung: watch::Sender::new(0),
            })),
            ..Self::default()
        }
    }

    /// This store, with an archive hold on every topic from its first append, so
    /// [MemoryStore::truncate] drops only what [StreamStore::advance_archive] says an archive
    /// holds.
    pub fn holding_for_archive(mut self) -> Self {
        self.archive_hold = true;
        self
    }

    /// Forgets a topic and its holds, as a store does when the topic's last key expired.
    pub fn forget(&self, chain: &ChainId, topic: &str) {
        if let Some(chain) = self.chains.lock().unwrap().get_mut(&chain_key(chain)) {
            chain.topics.remove(topic);
            chain.floors.remove(topic);
        }
    }

    /// An empty store with only the capabilities both it and `mask` have, and the limits and
    /// the replay horizon of `mask`. Without producer state it keeps no dedupe entries and no
    /// close flags, without a kept last position its reads never fail as expired or not found,
    /// and without a long poll its reads never wait.
    pub fn with_capabilities(mask: Capabilities) -> Self {
        Self {
            capabilities: Capabilities {
                max_record_bytes: mask.max_record_bytes,
                max_batch_bytes: mask.max_batch_bytes,
                replay_horizon: mask.replay_horizon,
                ..FULL.masked(&mask)
            },
            ..Self::default()
        }
    }

    /// Drops all but the newest `keep` records of a topic, standing in for a store's retention.
    /// It keeps the records a positions-only reader holds, from the position it holds on.
    pub fn truncate(&self, chain: &ChainId, topic: &str, keep: usize) {
        let mut chains = self.chains.lock().unwrap();
        let Some(chain) = chains.get_mut(&chain_key(chain)) else {
            return;
        };
        let held = chain
            .floors
            .get(topic)
            .and_then(|holders| holders.values().map(|position| position.unwrap_or(0)).min());
        let Some(topic) = chain.topics.get_mut(topic) else {
            return;
        };
        let mut drop = topic.records.len().saturating_sub(keep);
        if let Some(held) = held {
            drop = drop.min(held.saturating_sub(topic.base) as usize);
        }
        topic.records.drain(..drop);
        topic.base += drop as u64;
    }

    fn holds(&self) -> StreamResult<()> {
        if self.capabilities.holds {
            return Ok(());
        }
        Err(StreamError::unsupported(
            "this store can't hold records for a positions-only reader",
        ))
    }

    fn producer_state(&self) -> StreamResult<()> {
        if self.capabilities.producer_state {
            return Ok(());
        }
        Err(StreamError::unsupported(
            "this store can't keep producer state",
        ))
    }

    fn wrote(&self) {
        self.written.send_modify(|version| *version += 1);
    }

    /// One look at what a read gets now: `None` when it should wait for more.
    fn read_now(&self, request: &StoreReadRequest) -> StreamResult<Option<StoreReadResponse>> {
        let chains = self.chains.lock().unwrap();
        let chain = chains.get(&chain_key(chain_of(request.chain.as_ref())?));
        let topic = chain.and_then(|chain| chain.topics.get(&request.topic));
        let closed = chain.is_some_and(|chain| chain.closed) || topic.is_some_and(|t| t.closed);
        let after = parse_position(&request.after_position)?;
        let kept_last = self.capabilities.kept_last;
        let Some(topic) = topic else {
            if after.is_some() && kept_last {
                return Err(StreamError::not_found(format!(
                    "topic {:?} holds no records and none were dropped, so nothing is known \
                     about what followed position {}",
                    request.topic, request.after_position
                )));
            }
            return Ok(closed.then(|| StoreReadResponse {
                closed,
                ..Default::default()
            }));
        };
        let mut start = after.map_or(topic.base, |after| after + 1);
        if !kept_last {
            start = start.max(topic.base);
        }
        if start > topic.head() {
            return Err(StreamError::cursor(format!(
                "position {} is one topic {:?} never held",
                request.after_position, request.topic
            )));
        }
        if start < topic.base {
            return Err(StreamError::expired(format!(
                "the records from offset {start} on topic {:?} were dropped by retention; the \
                 topic now starts at {}",
                request.topic, topic.base
            )));
        }
        let records: Vec<_> = (start..topic.head())
            .take(request.max_records.max(1) as usize)
            .map(|offset| StoredRecord {
                position: offset.to_string(),
                record: topic.at(offset).cloned().unwrap_or_default(),
            })
            .collect();
        Ok(
            (!records.is_empty() || closed).then_some(StoreReadResponse {
                records,
                closed,
                close: topic.close.clone(),
            }),
        )
    }
}

fn chain_of(chain: Option<&ChainId>) -> StreamResult<&ChainId> {
    chain.ok_or_else(|| StreamError::refused("a store call needs the chain it is about"))
}

fn parse_position(position: &str) -> StreamResult<Option<u64>> {
    if position.is_empty() {
        return Ok(None);
    }
    position.parse().map(Some).map_err(|_| {
        StreamError::cursor(format!(
            "position {position:?} is not a position of the memory store"
        ))
    })
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as u64)
}

fn wait_of(request: &StoreReadRequest) -> Duration {
    request.wait.as_ref().map_or(Duration::ZERO, |wait| {
        Duration::new(
            u64::try_from(wait.seconds).unwrap_or(0),
            u32::try_from(wait.nanos).unwrap_or(0),
        )
    })
}

#[async_trait::async_trait]
impl StreamStore for MemoryStore {
    fn name(&self) -> &str {
        NAME
    }

    fn notification_log(&self) -> Option<Arc<dyn NotificationLog>> {
        self.log.clone().map(|log| log as Arc<dyn NotificationLog>)
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            change_feed: self.capabilities.change_feed && self.log.is_some(),
            ..self.capabilities
        }
    }

    async fn append(&self, request: StoreAppendRequest) -> StreamResult<StoreAppendResponse> {
        if request.records.is_empty() {
            return Err(StreamError::refused("an append needs at least one record"));
        }
        if request.digest.is_empty() {
            return Err(StreamError::refused("an append needs the batch's digest"));
        }
        let id = chain_of(request.chain.as_ref())?;
        let keeps_topics = id.owner_kind == StreamOwnerKind::WorkflowActivity as i32;
        let producer_state = self.capabilities.producer_state;
        let mut chains = self.chains.lock().unwrap();
        let chain = chains.entry(chain_key(id)).or_default();
        let chain_closed = chain.closed;
        let session = (request.producer_id.clone(), request.attempt);
        let existing = chain.topics.get(&request.topic).filter(|_| producer_state);
        if let Some(held) = existing.and_then(|topic| topic.held.get(&session)) {
            if request.sequence == held.sequence {
                if request.digest != held.digest {
                    return Err(StreamError::new(
                        crate::proto::StreamFailureKind::ProducerDivergent,
                        format!(
                            "sequence {} was already written with different content",
                            request.sequence
                        ),
                    ));
                }
                let last = held.first + held.count as u64 - 1;
                return Ok(StoreAppendResponse {
                    first_position: held.first.to_string(),
                    last_position: last.to_string(),
                });
            }
            let next = held.sequence + held.count;
            if request.sequence < next {
                return Err(StreamError::new(
                    crate::proto::StreamFailureKind::ProducerStale,
                    format!(
                        "sequence {} is below the next one expected, {next}",
                        request.sequence
                    ),
                ));
            }
        }
        // Checked before the topic exists, so a refused batch leaves no trace of it.
        if chain_closed || existing.is_some_and(|topic| topic.closed) {
            let closed_by = existing
                .and_then(|topic| topic.close.as_ref())
                .map(|close| close.closed_by.clone())
                .unwrap_or_default();
            return Err(
                StreamError::closed("the Workflow that owns this stream has closed")
                    .with_closed_by(closed_by),
            );
        }
        // On every batch, since the first one can be lost and the close must still find it.
        if keeps_topics {
            chain.written.insert(request.topic.clone());
        }
        if self.archive_hold && !chain.topics.contains_key(&request.topic) {
            chain
                .floors
                .entry(request.topic.clone())
                .or_default()
                .insert(ARCHIVE_HOLDER.to_string(), None);
        }
        let topic = chain.topics.entry(request.topic.clone()).or_default();
        let count = request.records.len() as i64;
        let mut positions = request.records.into_iter().map(|record| topic.push(record));
        let first = positions.next().unwrap_or_default();
        let last = positions.last().unwrap_or(first);
        if producer_state {
            topic.held.insert(
                session,
                Held {
                    sequence: request.sequence,
                    count,
                    first,
                    digest: request.digest,
                },
            );
        }
        // Under the store's lock, so the entry lands in the same step as the records.
        if let (Some(log), Some(chain)) = (&self.log, &request.chain) {
            log.push(chain, &request.topic, last.to_string());
            if self.archive_hold {
                log.mark(chain, &request.topic, unix_ms());
            }
        }
        drop(chains);
        self.wrote();
        Ok(StoreAppendResponse {
            first_position: first.to_string(),
            last_position: last.to_string(),
        })
    }

    async fn read(&self, request: StoreReadRequest) -> StreamResult<StoreReadResponse> {
        let wait = if self.capabilities.long_poll {
            wait_of(&request)
        } else {
            Duration::ZERO
        };
        let deadline = Instant::now() + wait;
        let mut written = self.written.subscribe();
        loop {
            if let Some(response) = self.read_now(&request)? {
                return Ok(response);
            }
            if tokio::time::timeout_at(deadline, written.changed())
                .await
                .is_err()
            {
                return Ok(StoreReadResponse::default());
            }
        }
    }

    async fn latest(&self, request: StoreLatestRequest) -> StreamResult<StoreLatestResponse> {
        let chains = self.chains.lock().unwrap();
        let head = chains
            .get(&chain_key(chain_of(request.chain.as_ref())?))
            .and_then(|chain| chain.topics.get(&request.topic))
            .map_or(0, Topic::head);
        Ok(StoreLatestResponse {
            position: head
                .checked_sub(1)
                .map(|p| p.to_string())
                .unwrap_or_default(),
        })
    }

    async fn record_at(
        &self,
        chain: &ChainId,
        topic: &str,
        position: &str,
    ) -> StreamResult<Option<Vec<u8>>> {
        let Some(offset) = parse_position(position)? else {
            return Ok(None);
        };
        let chains = self.chains.lock().unwrap();
        Ok(chains
            .get(&chain_key(chain))
            .and_then(|chain| chain.topics.get(topic))
            .and_then(|topic| topic.at(offset))
            .cloned())
    }

    async fn newest_ever(&self, chain: &ChainId, topic: &str) -> StreamResult<String> {
        if !self.capabilities.kept_last {
            return Ok(String::new());
        }
        let chains = self.chains.lock().unwrap();
        Ok(chains
            .get(&chain_key(chain))
            .and_then(|chain| chain.topics.get(topic))
            .and_then(|topic| topic.head().checked_sub(1))
            .map(|offset| offset.to_string())
            .unwrap_or_default())
    }

    async fn dropped_after(
        &self,
        chain: &ChainId,
        topic: &str,
        mark: &str,
    ) -> StreamResult<Option<String>> {
        if !self.capabilities.kept_last {
            return Ok(None);
        }
        let mark = parse_position(mark)?;
        let chains = self.chains.lock().unwrap();
        Ok(chains
            .get(&chain_key(chain))
            .and_then(|chain| chain.topics.get(topic))
            .and_then(|topic| topic.base.checked_sub(1))
            .filter(|dropped| mark.is_none_or(|mark| *dropped > mark))
            .map(|offset| offset.to_string()))
    }

    async fn hold_floor(
        &self,
        chain: &ChainId,
        topic: &str,
        holder: &str,
        position: &str,
    ) -> StreamResult<()> {
        self.holds()?;
        let position = parse_position(position)?;
        let mut chains = self.chains.lock().unwrap();
        let holders = chains
            .entry(chain_key(chain))
            .or_default()
            .floors
            .entry(topic.to_string())
            .or_default();
        if holders.len() >= crate::MAX_FLOOR_HOLDERS && !holders.contains_key(holder) {
            return Err(StreamError::refused(too_many_holders(topic)));
        }
        let held = holders.entry(holder.to_string()).or_insert(position);
        *held = match (*held, position) {
            (Some(held), Some(position)) => Some(held.min(position)),
            _ => None,
        };
        Ok(())
    }

    /// The memory store has no clock for retention, so a release ends the hold at once.
    async fn release_floor(&self, chain: &ChainId, topic: &str, holder: &str) -> StreamResult<()> {
        self.holds()?;
        if let Some(holders) = self
            .chains
            .lock()
            .unwrap()
            .get_mut(&chain_key(chain))
            .and_then(|chain| chain.floors.get_mut(topic))
        {
            holders.remove(holder);
        }
        if let (Some(log), true) = (&self.log, holder == ARCHIVE_HOLDER) {
            log.unmark(chain, topic);
        }
        Ok(())
    }

    async fn advance_archive(
        &self,
        chain: &ChainId,
        topic: &str,
        position: &str,
    ) -> StreamResult<()> {
        self.holds()?;
        if !self.archive_hold {
            return Err(StreamError::unsupported("this store keeps no archive hold"));
        }
        let position = parse_position(position)?;
        let mut chains = self.chains.lock().unwrap();
        let stored = chains.entry(chain_key(chain)).or_default();
        stored
            .floors
            .entry(topic.to_string())
            .or_default()
            .insert(ARCHIVE_HOLDER.to_string(), position);
        if let Some(held) = stored.topics.get_mut(topic) {
            held.archived = position;
            if let Some(log) = &self.log
                && position.is_some_and(|position| position + 1 == held.head())
            {
                log.unmark(chain, topic);
            }
        }
        Ok(())
    }

    async fn archived(&self, chain: &ChainId, topic: &str) -> StreamResult<Option<String>> {
        if !self.archive_hold {
            return Ok(None);
        }
        let chains = self.chains.lock().unwrap();
        Ok(chains
            .get(&chain_key(chain))
            .and_then(|chain| chain.topics.get(topic))
            .map(|topic| {
                topic
                    .archived
                    .map(|offset| offset.to_string())
                    .unwrap_or_default()
            }))
    }

    async fn close_chain(&self, chain: &ChainId) -> StreamResult<()> {
        self.producer_state()?;
        self.chains
            .lock()
            .unwrap()
            .entry(chain_key(chain))
            .or_default()
            .closed = true;
        self.wrote();
        Ok(())
    }

    async fn close_topic(&self, chain: &ChainId, topic: &str) -> StreamResult<()> {
        self.close_topic_with(chain, topic, StoredClose::default())
            .await
            .map(drop)
    }

    async fn close_topic_with(
        &self,
        chain: &ChainId,
        topic: &str,
        close: StoredClose,
    ) -> StreamResult<StoredClose> {
        if !self.capabilities.producer_state {
            crate::close::write_close(self, chain, topic, &close).await?;
            return Ok(close);
        }
        let stored = {
            let mut chains = self.chains.lock().unwrap();
            let topic = chains
                .entry(chain_key(chain))
                .or_default()
                .topics
                .entry(topic.to_string())
                .or_default();
            topic.closed = true;
            topic.close.get_or_insert(close).clone()
        };
        if let (Some(log), true) = (&self.log, self.archive_hold) {
            log.mark(chain, topic, 0);
        }
        self.wrote();
        Ok(stored)
    }

    async fn retire_producer(
        &self,
        chain: &ChainId,
        topic: &str,
        producer_id: &str,
        attempt: i64,
    ) -> StreamResult<()> {
        self.producer_state()?;
        let mut chains = self.chains.lock().unwrap();
        if let Some(topic) = chains
            .get_mut(&chain_key(chain))
            .and_then(|chain| chain.topics.get_mut(topic))
        {
            topic.held.remove(&(producer_id.to_string(), attempt));
        }
        Ok(())
    }

    async fn written_topics(&self, chain: &ChainId) -> StreamResult<Vec<String>> {
        let chains = self.chains.lock().unwrap();
        Ok(chains
            .get(&chain_key(chain))
            .map(|chain| chain.written.iter().cloned().collect())
            .unwrap_or_default())
    }

    async fn list_topics(&self, chain: &ChainId) -> StreamResult<Vec<String>> {
        if !self.capabilities.topic_listing {
            return Err(StreamError::unsupported(
                "this store can't list the topics of a chain",
            ));
        }
        let chains = self.chains.lock().unwrap();
        Ok(chains
            .get(&chain_key(chain))
            .map(|chain| {
                chain
                    .topics
                    .iter()
                    .filter(|(_, topic)| topic.head() > 0)
                    .map(|(name, _)| name.clone())
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn delete_owner(&self, request: DeleteOwnerRequest) -> StreamResult<DeleteOwnerResponse> {
        if !self.capabilities.delete_owner {
            return Err(StreamError::unsupported(
                "this store can't delete the streams of an owner",
            ));
        }
        let (owner_kind, owner_id) = match StreamOwnerKind::try_from(request.owner_kind) {
            Ok(StreamOwnerKind::Workflow) => (WORKFLOW_OWNER_KIND, &request.workflow_id),
            Ok(StreamOwnerKind::Activity) => (ACTIVITY_OWNER_KIND, &request.activity_id),
            _ => {
                return Err(StreamError::unsupported(format!(
                    "this release keeps streams of Workflows and standalone Activities only, not \
                     owner kind {}",
                    request.owner_kind
                )));
            }
        };
        // A Workflow Activity's id starts with its Workflow's, so a Workflow's delete takes it too.
        let scheduled = (owner_kind == WORKFLOW_OWNER_KIND).then(|| format!("{}/", part(owner_id)));
        let mut chains = self.chains.lock().unwrap();
        let mut deleted = 0;
        chains.retain(|(namespace, kind, id, _), chain| {
            let owned = *namespace == request.namespace
                && ((*kind == owner_kind && id == owner_id)
                    || (*kind == WORKFLOW_ACTIVITY_OWNER_KIND
                        && scheduled
                            .as_ref()
                            .is_some_and(|prefix| id.starts_with(prefix))));
            if owned {
                deleted += 1 + chain.topics.len() as u64;
            }
            !owned
        });
        Ok(DeleteOwnerResponse { deleted })
    }
}

/// Why a new floor holder is refused.
fn too_many_holders(topic: &str) -> String {
    format!(
        "topic {topic:?} already has {} floor holders, so a positions-only reader can't start \
         on it; let readers finish or Continue-as-New",
        crate::MAX_FLOOR_HOLDERS
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::StreamFailureKind;

    fn chain() -> ChainId {
        ChainId {
            namespace: "ns".to_string(),
            workflow_id: "wf".to_string(),
            first_run_id: "run-1".to_string(),
            ..Default::default()
        }
    }

    fn append(sequence: i64, records: &[&[u8]]) -> StoreAppendRequest {
        StoreAppendRequest {
            chain: Some(chain()),
            topic: "out".to_string(),
            producer_id: "p".to_string(),
            attempt: 1,
            sequence,
            digest: records.concat(),
            records: records.iter().map(|record| record.to_vec()).collect(),
        }
    }

    fn read_after(position: &str) -> StoreReadRequest {
        StoreReadRequest {
            chain: Some(chain()),
            topic: "out".to_string(),
            after_position: position.to_string(),
            max_records: 10,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_position_past_the_head_is_one_the_topic_never_held() {
        let store = MemoryStore::new();
        store.append(append(1, &[b"a"])).await.unwrap();
        let error = store.read(read_after("5")).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Cursor);
        assert!(error.message.contains("never held"), "{error}");
        let error = store.read(read_after("x")).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Cursor);
    }

    #[tokio::test]
    async fn positions_survive_truncation() {
        let store = MemoryStore::new();
        store.append(append(1, &[b"a", b"b", b"c"])).await.unwrap();
        store.truncate(&chain(), "out", 1);
        let read = store.read(read_after("1")).await.unwrap();
        assert_eq!(read.records.len(), 1);
        assert_eq!(read.records[0].position, "2");
        assert_eq!(read.records[0].record, b"c");
        assert_eq!(
            store.record_at(&chain(), "out", "2").await.unwrap(),
            Some(b"c".to_vec())
        );
        assert_eq!(store.record_at(&chain(), "out", "0").await.unwrap(), None);
    }

    fn log_only() -> MemoryStore {
        MemoryStore::with_capabilities(Capabilities::log_only())
    }

    #[test]
    fn a_mask_only_takes_capabilities_away() {
        // The change feed comes with a notification log.
        let no_log = Capabilities {
            change_feed: false,
            ..FULL
        };
        assert_eq!(MemoryStore::new().capabilities(), no_log);
        assert_eq!(MemoryStore::with_notification_log().capabilities(), FULL);
        let masked = log_only().capabilities();
        assert!(!masked.producer_state && !masked.kept_last && !masked.long_poll);
        // Positions stay offsets whatever the mask, and the limits stay the store's.
        assert!(masked.dense_positions);
        assert_eq!(masked.max_record_bytes, MAX_RECORD_BYTES);
        assert_eq!(MemoryStore::with_capabilities(FULL).capabilities(), no_log);
    }

    #[tokio::test]
    async fn without_producer_state_a_repeat_lands_again_and_a_close_is_a_record() {
        let store = log_only();
        let first = store.append(append(1, &[b"a"])).await.unwrap();
        let again = store.append(append(1, &[b"a"])).await.unwrap();
        assert_ne!(first.first_position, again.first_position);
        assert_eq!(store.read(read_after("")).await.unwrap().records.len(), 2);
        for error in [
            store.close_chain(&chain()).await.unwrap_err(),
            store
                .retire_producer(&chain(), "out", "p", 1)
                .await
                .unwrap_err(),
        ] {
            assert_eq!(error.kind, StreamFailureKind::Unsupported);
        }
        store.close_topic(&chain(), "out").await.unwrap();
        let read = store.read(read_after("")).await.unwrap();
        assert_eq!(read.records.len(), 3);
        assert!(!read.closed);
    }

    #[tokio::test]
    async fn without_a_kept_last_position_a_read_starts_at_what_is_left() {
        let store = log_only();
        store.append(append(1, &[b"a", b"b", b"c"])).await.unwrap();
        store.truncate(&chain(), "out", 1);
        let read = store.read(read_after("0")).await.unwrap();
        assert_eq!(read.records[0].position, "2");
        assert_eq!(store.newest_ever(&chain(), "out").await.unwrap(), "");
        assert_eq!(
            store.dropped_after(&chain(), "out", "").await.unwrap(),
            None
        );
        let missing = StoreReadRequest {
            topic: "missing".to_string(),
            ..read_after("4")
        };
        assert!(store.read(missing).await.unwrap().records.is_empty());
    }

    #[tokio::test]
    async fn without_a_long_poll_a_read_answers_at_once() {
        let store = log_only();
        let started = Instant::now();
        let read = store
            .read(StoreReadRequest {
                wait: Some(Duration::from_secs(30).try_into().unwrap()),
                ..read_after("")
            })
            .await
            .unwrap();
        assert!(read.records.is_empty());
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn the_topics_of_a_chain_are_the_ones_that_took_a_record() {
        let store = MemoryStore::new();
        store.append(append(1, &[b"a"])).await.unwrap();
        store.close_topic(&chain(), "quiet").await.unwrap();
        assert_eq!(store.list_topics(&chain()).await.unwrap(), ["out"]);
        let error = log_only().list_topics(&chain()).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Unsupported);
    }

    #[tokio::test]
    async fn another_owner_kind_is_unsupported() {
        let store = MemoryStore::new();
        let error = store
            .delete_owner(DeleteOwnerRequest {
                namespace: "ns".to_string(),
                owner_kind: StreamOwnerKind::Unspecified as i32,
                workflow_id: "wf".to_string(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Unsupported);
    }

    #[tokio::test]
    async fn a_topic_refuses_a_floor_holder_past_the_cap() {
        let store = MemoryStore::new();
        for holder in 0..crate::MAX_FLOOR_HOLDERS {
            store
                .hold_floor(&chain(), "out", &holder.to_string(), "")
                .await
                .unwrap();
        }
        let error = store
            .hold_floor(&chain(), "out", "one-more", "")
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Refused);
        store.hold_floor(&chain(), "out", "0", "").await.unwrap();
    }
}
