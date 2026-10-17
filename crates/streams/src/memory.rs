//! The in-process store, for tests.
//!
//! It holds everything in process memory, so nothing survives the process and it is no store for
//! production. It exists so the conformance suite runs the whole store contract without a server,
//! and to show in one file what a store owes. Positions are offsets that are never reused, so a
//! position keeps naming the same record after [MemoryStore::truncate] drops older ones.
//!
//! [MemoryStore::with_capabilities] masks capabilities off, so Core's fallbacks run in tests.

use crate::{
    Capabilities, ConditionForms, MAX_BATCH_BYTES, MAX_RECORD_BYTES, StreamError, StreamResult,
    StreamStore,
    proto::{
        ChainId, DeleteOwnerRequest, DeleteOwnerResponse, StoreAppendRequest, StoreAppendResponse,
        StoreLatestRequest, StoreLatestResponse, StoreReadRequest, StoreReadResponse, StoredRecord,
        StreamOwnerKind,
    },
};
use std::{
    collections::{HashMap, VecDeque},
    sync::Mutex,
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
    holds: false,
    change_feed: false,
    cheap_logs: true,
    delete_owner: true,
    topic_listing: true,
    long_poll: true,
    dense_positions: true,
    replay_horizon: None,
    max_record_bytes: MAX_RECORD_BYTES,
    max_batch_bytes: MAX_BATCH_BYTES,
};

type ChainKey = (String, String, String);

fn chain_key(chain: &ChainId) -> ChainKey {
    (
        chain.namespace.clone(),
        chain.workflow_id.clone(),
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
    capabilities: Capabilities,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self {
            chains: Mutex::default(),
            written: watch::Sender::new(0),
            capabilities: FULL,
        }
    }
}

impl MemoryStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty store with only the capabilities both it and `mask` have, and the limits of
    /// `mask`. Without producer state it keeps no dedupe entries and no close flags, without a
    /// kept last position its reads never fail as expired or not found, and without a long poll
    /// its reads never wait.
    pub fn with_capabilities(mask: Capabilities) -> Self {
        Self {
            capabilities: Capabilities {
                max_record_bytes: mask.max_record_bytes,
                max_batch_bytes: mask.max_batch_bytes,
                ..FULL.masked(&mask)
            },
            ..Self::default()
        }
    }

    /// Drops all but the newest `keep` records of a topic, standing in for a store's retention.
    pub fn truncate(&self, chain: &ChainId, topic: &str, keep: usize) {
        let mut chains = self.chains.lock().unwrap();
        let Some(topic) = chains
            .get_mut(&chain_key(chain))
            .and_then(|chain| chain.topics.get_mut(topic))
        else {
            return;
        };
        let drop = topic.records.len().saturating_sub(keep);
        topic.records.drain(..drop);
        topic.base += drop as u64;
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
                records: Vec::new(),
                closed,
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
        Ok((!records.is_empty() || closed).then_some(StoreReadResponse { records, closed }))
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

    fn capabilities(&self) -> Capabilities {
        self.capabilities
    }

    async fn append(&self, request: StoreAppendRequest) -> StreamResult<StoreAppendResponse> {
        if request.records.is_empty() {
            return Err(StreamError::refused("an append needs at least one record"));
        }
        if request.digest.is_empty() {
            return Err(StreamError::refused("an append needs the batch's digest"));
        }
        let producer_state = self.capabilities.producer_state;
        let mut chains = self.chains.lock().unwrap();
        let chain = chains
            .entry(chain_key(chain_of(request.chain.as_ref())?))
            .or_default();
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
            return Err(StreamError::closed(
                "the Workflow that owns this stream has closed",
            ));
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
        self.producer_state()?;
        self.chains
            .lock()
            .unwrap()
            .entry(chain_key(chain))
            .or_default()
            .topics
            .entry(topic.to_string())
            .or_default()
            .closed = true;
        self.wrote();
        Ok(())
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
        if request.owner_kind != StreamOwnerKind::Workflow as i32 {
            return Err(StreamError::unsupported(format!(
                "this release keeps streams of Workflows only, not owner kind {}",
                request.owner_kind
            )));
        }
        let mut chains = self.chains.lock().unwrap();
        let mut deleted = 0;
        chains.retain(|(namespace, workflow_id, _), chain| {
            let owned = *namespace == request.namespace && *workflow_id == request.workflow_id;
            if owned {
                deleted += 1 + chain.topics.len() as u64;
            }
            !owned
        });
        Ok(DeleteOwnerResponse { deleted })
    }
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
        assert_eq!(MemoryStore::new().capabilities(), FULL);
        let masked = log_only().capabilities();
        assert!(!masked.producer_state && !masked.kept_last && !masked.long_poll);
        // Positions stay offsets whatever the mask, and the limits stay the store's.
        assert!(masked.dense_positions);
        assert_eq!(masked.max_record_bytes, MAX_RECORD_BYTES);
        let all = Capabilities {
            holds: true,
            change_feed: true,
            ..FULL
        };
        assert_eq!(MemoryStore::with_capabilities(all).capabilities(), FULL);
    }

    #[tokio::test]
    async fn without_producer_state_a_repeat_lands_again_and_closes_are_unsupported() {
        let store = log_only();
        let first = store.append(append(1, &[b"a"])).await.unwrap();
        let again = store.append(append(1, &[b"a"])).await.unwrap();
        assert_ne!(first.first_position, again.first_position);
        assert_eq!(store.read(read_after("")).await.unwrap().records.len(), 2);
        for error in [
            store.close_chain(&chain()).await.unwrap_err(),
            store.close_topic(&chain(), "out").await.unwrap_err(),
            store
                .retire_producer(&chain(), "out", "p", 1)
                .await
                .unwrap_err(),
        ] {
            assert_eq!(error.kind, StreamFailureKind::Unsupported);
        }
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
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Unsupported);
    }
}
