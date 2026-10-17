use crate::{
    StreamError, StreamResult,
    proto::{
        ChainId, DeleteOwnerRequest, DeleteOwnerResponse, StoreAppendRequest, StoreAppendResponse,
        StoreLatestRequest, StoreLatestResponse, StoreReadRequest, StoreReadResponse,
    },
};
use std::time::Duration;

/// The largest record a store takes by default, with its envelope.
pub const MAX_RECORD_BYTES: usize = 1536 * 1024;

/// The most record bytes one append takes by default.
pub const MAX_BATCH_BYTES: usize = 4 << 20;

/// What a store can do beyond the log every store gives. Core reads it once and takes the full
/// path or a fallback for each capability, so a store that gives only the log still serves
/// streams, with the weaker guarantees each fallback names.
///
/// The log every store gives: a batch lands whole and in order, a read after a position returns
/// the next records in log order, a position never names two records, the store answers the
/// record at a position, and retention drops only the oldest records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    /// C1. The append checks the producer attempt's dedupe entry and the close flags in the same
    /// step, and [StreamStore::close_chain], [StreamStore::close_topic] and
    /// [StreamStore::retire_producer] work. Without it, a close is a `CLOSE` record and readers
    /// drop copies.
    pub producer_state: bool,
    /// C2. The forms of conditional append the store has.
    pub conditional: ConditionForms,
    /// C3. The store knows a topic's newest position also after retention dropped every record,
    /// and [StreamStore::newest_ever] and [StreamStore::dropped_after] answer it. Without it,
    /// Core decides expiry from the record at the read's position.
    pub kept_last: bool,
    /// C4. A holder can keep records from retention.
    pub holds: bool,
    /// C5. One consumer can read every append in order.
    pub change_feed: bool,
    /// C6. A log per topic of each chain costs nothing much.
    pub cheap_logs: bool,
    /// C7. [StreamStore::delete_owner] works.
    pub delete_owner: bool,
    /// C8. [StreamStore::list_topics] works. Without it, producers write a `TOPIC` record to
    /// the chain's control log.
    pub topic_listing: bool,
    /// C9. A read can wait for the next record. Without it, Core polls.
    pub long_poll: bool,
    /// The next record's position is always the last one plus one.
    pub dense_positions: bool,
    /// How long the store keeps records with no hold, when a positions-only reader can count on
    /// it.
    pub replay_horizon: Option<Duration>,
    /// The largest record the store takes, with its envelope.
    pub max_record_bytes: usize,
    /// The most record bytes one append takes.
    pub max_batch_bytes: usize,
}

/// The conditional appends a store has (C2).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConditionForms {
    /// The append fails unless the log's next position is the expected one.
    pub tail: bool,
    /// The append fails unless the stream's fence token is the expected one.
    pub fence: bool,
}

impl Capabilities {
    /// A store that gives only the log, so Core takes every fallback.
    pub const fn log_only() -> Self {
        Self {
            producer_state: false,
            conditional: ConditionForms {
                tail: false,
                fence: false,
            },
            kept_last: false,
            holds: false,
            change_feed: false,
            cheap_logs: false,
            delete_owner: false,
            topic_listing: false,
            long_poll: false,
            dense_positions: false,
            replay_horizon: None,
            max_record_bytes: MAX_RECORD_BYTES,
            max_batch_bytes: MAX_BATCH_BYTES,
        }
    }

    /// The capabilities both stores and `mask` have. Limits and the horizon come from `self`.
    pub fn masked(self, mask: &Capabilities) -> Self {
        Self {
            producer_state: self.producer_state && mask.producer_state,
            conditional: ConditionForms {
                tail: self.conditional.tail && mask.conditional.tail,
                fence: self.conditional.fence && mask.conditional.fence,
            },
            kept_last: self.kept_last && mask.kept_last,
            holds: self.holds && mask.holds,
            change_feed: self.change_feed && mask.change_feed,
            cheap_logs: self.cheap_logs && mask.cheap_logs,
            delete_owner: self.delete_owner && mask.delete_owner,
            topic_listing: self.topic_listing && mask.topic_listing,
            long_poll: self.long_poll && mask.long_poll,
            ..self
        }
    }
}

impl From<Capabilities> for crate::proto::StoreCapabilities {
    fn from(capabilities: Capabilities) -> Self {
        Self {
            producer_state: capabilities.producer_state,
            conditional_tail: capabilities.conditional.tail,
            conditional_fence: capabilities.conditional.fence,
            kept_last: capabilities.kept_last,
            holds: capabilities.holds,
            change_feed: capabilities.change_feed,
            cheap_logs: capabilities.cheap_logs,
            delete_owner: capabilities.delete_owner,
            topic_listing: capabilities.topic_listing,
            long_poll: capabilities.long_poll,
            dense_positions: capabilities.dense_positions,
            replay_horizon: capabilities
                .replay_horizon
                .and_then(|horizon| horizon.try_into().ok()),
            max_record_bytes: capabilities.max_record_bytes as u64,
            max_batch_bytes: capabilities.max_batch_bytes as u64,
        }
    }
}

impl From<&crate::proto::StoreCapabilities> for Capabilities {
    /// A mask from its proto. A zero limit keeps the default.
    fn from(mask: &crate::proto::StoreCapabilities) -> Self {
        let limit = |bytes: u64, default: usize| match bytes {
            0 => default,
            bytes => usize::try_from(bytes).unwrap_or(usize::MAX),
        };
        Self {
            producer_state: mask.producer_state,
            conditional: ConditionForms {
                tail: mask.conditional_tail,
                fence: mask.conditional_fence,
            },
            kept_last: mask.kept_last,
            holds: mask.holds,
            change_feed: mask.change_feed,
            cheap_logs: mask.cheap_logs,
            delete_owner: mask.delete_owner,
            topic_listing: mask.topic_listing,
            long_poll: mask.long_poll,
            dense_positions: mask.dense_positions,
            replay_horizon: mask
                .replay_horizon
                .and_then(|horizon| Duration::try_from(horizon).ok()),
            max_record_bytes: limit(mask.max_record_bytes, MAX_RECORD_BYTES),
            max_batch_bytes: limit(mask.max_batch_bytes, MAX_BATCH_BYTES),
        }
    }
}

impl Default for Capabilities {
    fn default() -> Self {
        Self::log_only()
    }
}

fn unsupported<T>(what: &str) -> StreamResult<T> {
    Err(StreamError::unsupported(format!("this store can't {what}")))
}

/// A store that holds streams, keyed by their owner's run chain.
///
/// A store moves serialized `temporal.sdk.streams.v1.StreamRecord`s with encoded bodies and deals
/// in its own positions. Owner checks, cursors and supersession sit above it, so every
/// store behaves the same where the contract says so, and a new store only implements this.
///
/// Every store implements the log calls. A call of a capability the store doesn't declare in
/// [StreamStore::capabilities] keeps its default, which fails as unsupported, and Core never
/// makes it.
///
/// Every call is safe to repeat. A call that fails while the store may have acted reports
/// [crate::StreamError::outcome_unknown] for an append and a storage failure otherwise.
#[async_trait::async_trait]
pub trait StreamStore: Send + Sync {
    /// The short name minted into this store's cursors, such as `redis` or `memory`. A cursor
    /// from another store is refused.
    fn name(&self) -> &str;

    /// What the store can do beyond the log.
    fn capabilities(&self) -> Capabilities {
        Capabilities::log_only()
    }

    /// Appends one batch for one producer attempt, whole and in order.
    ///
    /// With producer state, the store keeps the attempt's newest batch: its first sequence, its
    /// record count, its positions and its digest. A repeat of that batch with the same digest
    /// answers with its positions and writes nothing. The same first sequence with another
    /// digest fails as producer divergent, and a first sequence below the end of the newest
    /// batch as producer stale. A closed chain or topic refuses a new batch as closed, but still
    /// answers a repeat. Without producer state, the store only appends.
    async fn append(&self, request: StoreAppendRequest) -> StreamResult<StoreAppendResponse>;

    /// Reads the records of a topic after a position, waiting up to the request's `wait` for one
    /// when none is there.
    ///
    /// Fails as expired when the position names a record retention dropped, or when retention
    /// dropped records the read had not delivered yet. Fails as not found when nothing is known
    /// about the topic and the position is not empty.
    async fn read(&self, request: StoreReadRequest) -> StreamResult<StoreReadResponse>;

    /// The position of the newest record of a topic, or empty when it holds none.
    async fn latest(&self, request: StoreLatestRequest) -> StreamResult<StoreLatestResponse>;

    /// The serialized record at `position` on a topic, or `None` when the store no longer holds
    /// it. A read that resumes looks at the record it last delivered, to know that record's
    /// producer attempt.
    async fn record_at(
        &self,
        chain: &ChainId,
        topic: &str,
        position: &str,
    ) -> StreamResult<Option<Vec<u8>>>;

    /// The position of the newest record a topic ever took, even one retention dropped since,
    /// or empty when it took none. A read that has delivered nothing keeps it as its mark, since
    /// it has no position the store could check.
    async fn newest_ever(&self, _chain: &ChainId, _topic: &str) -> StreamResult<String> {
        Ok(String::new())
    }

    /// The position of the newest record retention dropped from a topic, when that record came
    /// after `mark`, the topic's [StreamStore::newest_ever] when a read started. Such a record
    /// was lost to the read. A store that never drops records keeps the default.
    async fn dropped_after(
        &self,
        _chain: &ChainId,
        _topic: &str,
        _mark: &str,
    ) -> StreamResult<Option<String>> {
        Ok(None)
    }

    /// Marks a chain closed, so its topics refuse new batches from producers.
    async fn close_chain(&self, chain: &ChainId) -> StreamResult<()> {
        let _ = chain;
        unsupported("keep close flags")
    }

    /// Marks one topic of a chain closed, like [StreamStore::close_chain] for that topic alone.
    async fn close_topic(&self, chain: &ChainId, topic: &str) -> StreamResult<()> {
        let _ = (chain, topic);
        unsupported("keep close flags")
    }

    /// Drops the newest-batch record [StreamStore::append] keeps for one producer attempt on a
    /// topic, and nothing else. A missing attempt or topic is no failure.
    ///
    /// Call it only once the attempt's last append returned. A retry of an earlier batch would
    /// then find no record to compare with and land a second time.
    async fn retire_producer(
        &self,
        chain: &ChainId,
        topic: &str,
        producer_id: &str,
        attempt: i64,
    ) -> StreamResult<()> {
        let _ = (chain, topic, producer_id, attempt);
        unsupported("keep producer state")
    }

    /// The topics of a chain that took a record, in no set order.
    async fn list_topics(&self, chain: &ChainId) -> StreamResult<Vec<String>> {
        let _ = chain;
        unsupported("list the topics of a chain")
    }

    /// Deletes every stream of one owner, across all its chains.
    async fn delete_owner(&self, request: DeleteOwnerRequest) -> StreamResult<DeleteOwnerResponse> {
        let _ = request;
        unsupported("delete the streams of an owner")
    }
}

#[cfg(test)]
mod tests {
    use super::StreamStore;
    use std::sync::Arc;

    #[test]
    fn a_store_can_be_shared_as_a_trait_object() {
        // Core holds the configured store behind one pointer for every Worker and client call.
        fn _shared(_: Arc<dyn StreamStore>) {}
    }
}
