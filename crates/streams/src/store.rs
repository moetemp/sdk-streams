use crate::{
    StreamError, StreamResult,
    proto::{
        ChainId, DeleteOwnerRequest, DeleteOwnerResponse, StoreAppendRequest, StoreAppendResponse,
        StoreLatestRequest, StoreLatestResponse, StoreReadRequest, StoreReadResponse, StoredClose,
    },
};
use std::time::Duration;

/// The largest record a store takes by default, with its envelope.
pub const MAX_RECORD_BYTES: usize = 1536 * 1024;

/// The most record bytes one append takes by default.
pub const MAX_BATCH_BYTES: usize = 4 << 20;

/// What a store can do beyond the log every store gives. The service reads it once and takes the
/// full path or a fallback for each capability, so a store that gives only the log still serves
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
    /// the service decides expiry from the record at the read's position.
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
    /// C9. A read can wait for the next record. Without it, the service polls.
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
    /// A store that gives only the log, so the service takes every fallback.
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

/// The most bytes a close's outcome takes in the store, `closed_by` included. A store keeps it
/// with the stream as it keeps a record, so it gets the bound one record gets.
pub const MAX_CLOSE_BYTES: usize = 1536 * 1024;

/// The floor holder that stands for the archive in a hot store. Readers can't use it, since a
/// reader's release would end the archive's hold.
pub const ARCHIVE_HOLDER: &str = "archive";

/// The most live floor holders one topic keeps. Every append reads them all, and each pins
/// records against retention, so one topic's readers can't fill the store.
pub const MAX_FLOOR_HOLDERS: usize = 1_000;

/// The result a stream closes with when its owner gave none: a `binary/null` payload, which every
/// SDK's converter reads as no value. A payload with no encoding fails a caller that expects a
/// typed result.
pub fn empty_close_result() -> crate::proto::Payload {
    crate::proto::Payload {
        metadata: [("encoding".to_string(), b"binary/null".to_vec())].into(),
        ..Default::default()
    }
}

/// What a closed stream's waiters see: the readers and callbacks its notifier holds.
// One is built per close, so the size of a failure doesn't matter.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum CloseOutcome {
    /// The stream ended with this payload, after lang's codec.
    Result(crate::proto::Payload),
    /// The stream ended with this failure, or as canceled when it has canceled info.
    Failure(temporalio_common::protos::temporal::api::failure::v1::Failure),
}

impl CloseOutcome {
    /// The result, or `None` for a failure.
    pub fn result(&self) -> Option<crate::proto::Payload> {
        match self {
            CloseOutcome::Result(result) => Some(result.clone()),
            CloseOutcome::Failure(_) => None,
        }
    }

    /// What the waiters of a stream closed with `close` see. A close with no outcome gives them
    /// [empty_close_result].
    pub fn of(close: &StoredClose) -> Self {
        match (&close.failure, &close.result) {
            (Some(failure), _) => CloseOutcome::Failure(failure.clone()),
            (None, Some(result)) => CloseOutcome::Result(result.clone()),
            (None, None) => CloseOutcome::Result(empty_close_result()),
        }
    }
}

impl From<CloseOutcome> for StoredClose {
    fn from(outcome: CloseOutcome) -> Self {
        match outcome {
            CloseOutcome::Result(result) => StoredClose {
                result: Some(result),
                ..Default::default()
            },
            CloseOutcome::Failure(failure) => StoredClose {
                failure: Some(failure),
                ..Default::default()
            },
        }
    }
}

/// A store that holds streams, keyed by their owner's run chain.
///
/// A store moves serialized `temporal.sdk.streams.v1.StreamRecord`s with encoded bodies and deals
/// in its own positions. Owner checks, cursors and supersession sit above it, so every
/// store behaves the same where the contract says so, and a new store only implements this.
///
/// Every store implements the log calls. A call of a capability the store doesn't declare in
/// [StreamStore::capabilities] keeps its default, which fails as unsupported, and the service never
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
    /// A store without producer state appends a `CLOSE` record instead, which ends every read
    /// there but refuses no append.
    async fn close_topic(&self, chain: &ChainId, topic: &str) -> StreamResult<()> {
        crate::close::write_close(self, chain, topic, &StoredClose::default()).await
    }

    /// Like [StreamStore::close_topic], with the close's outcome and who closed it. A store that
    /// keeps the outcome keeps the topic's first close only, and a read of the closed topic
    /// answers it. Answers the close the store keeps, so a wrapper tells the stream's notifier the
    /// same close a reader sees. A store that keeps none answers `close`. Without producer state
    /// the `CLOSE` record carries the outcome, and the first record wins for every reader.
    async fn close_topic_with(
        &self,
        chain: &ChainId,
        topic: &str,
        close: StoredClose,
    ) -> StreamResult<StoredClose> {
        if self.capabilities().producer_state {
            self.close_topic(chain, topic).await?;
        } else {
            crate::close::write_close(self, chain, topic, &close).await?;
        }
        Ok(close)
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

    /// The topics that producers wrote on a Workflow Activity's chain, so the scheduling
    /// Workflow's Core can close each one when the Activity ends. A topic enters the set with
    /// every batch, and a topic that no producer wrote is not in it. Other
    /// chains keep no set and answer empty. A store that keeps no set lists the chain's topics.
    async fn written_topics(&self, chain: &ChainId) -> StreamResult<Vec<String>> {
        self.list_topics(chain).await
    }

    /// Deletes every stream of one owner, across all its chains. A Workflow's delete also deletes
    /// the streams of every Activity it scheduled.
    async fn delete_owner(&self, request: DeleteOwnerRequest) -> StreamResult<DeleteOwnerResponse> {
        let _ = request;
        unsupported("delete the streams of an owner")
    }

    /// Holds the records of a topic after `position` against retention for `holder`, a
    /// positions-only reader that must find them again on replay. The store keeps the lowest
    /// position a holder held, and renews the holder's lease. An empty position holds the whole
    /// topic. A store that can't hold refuses, so a positions-only read fails at once and not
    /// at replay. A new holder past [MAX_FLOOR_HOLDERS] live ones on the topic is refused.
    async fn hold_floor(
        &self,
        chain: &ChainId,
        topic: &str,
        holder: &str,
        position: &str,
    ) -> StreamResult<()> {
        let _ = (chain, topic, holder, position);
        Err(crate::StreamError::unsupported(
            "this store can't hold records for a positions-only reader",
        ))
    }

    /// Ends `holder`'s hold after the store's release grace, so a replay right after the reader
    /// ended still finds its records. A holder the store doesn't know is no failure.
    async fn release_floor(&self, chain: &ChainId, topic: &str, holder: &str) -> StreamResult<()> {
        let _ = (chain, topic, holder);
        Err(crate::StreamError::unsupported(
            "this store can't hold records for a positions-only reader",
        ))
    }

    /// Moves the archive's hold on a topic to `position`, once the archive holds every record up
    /// to it. Unlike [StreamStore::hold_floor], this hold moves forward, so retention drops only
    /// what the archive holds. It also renews the archive's lease, and [StreamStore::archived]
    /// answers `position` from then on. A store with no archive hold refuses.
    async fn advance_archive(
        &self,
        chain: &ChainId,
        topic: &str,
        position: &str,
    ) -> StreamResult<()> {
        let _ = (chain, topic, position);
        Err(crate::StreamError::unsupported(
            "this store keeps no archive hold",
        ))
    }

    /// The newest position the archive holds for a topic, as [StreamStore::advance_archive] last
    /// set it, or empty when the archive holds none yet. `None` when the store knows nothing of
    /// the topic, or keeps no archive hold.
    async fn archived(&self, chain: &ChainId, topic: &str) -> StreamResult<Option<String>> {
        let _ = (chain, topic);
        Ok(None)
    }

    /// Closes one topic of a chain in the store, then tells the topic's notifier on the server,
    /// which completes every attached callback with the outcome. The store closes first, so a
    /// reader the completion reaches always finds the stream's end. A store that sends no
    /// notifications only closes the topic. Fails as refused when the server will never take the
    /// close, and with another kind when trying again may help.
    async fn close_stream(
        &self,
        chain: &ChainId,
        topic: &str,
        close: StoredClose,
    ) -> StreamResult<()> {
        self.close_topic_with(chain, topic, close).await.map(drop)
    }

    /// Waits until the notifications this store sent are out. A store that sends none returns at
    /// once.
    async fn flush_notifications(&self) {}

    /// The store's notification log, when each append writes an entry to it in the same step.
    /// Producers then send no notifications and a driver reads the log. A store without one
    /// keeps the default, and each producer notifies after its append.
    fn notification_log(&self) -> Option<std::sync::Arc<dyn crate::NotificationLog>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::StreamStore;
    use std::sync::Arc;

    #[test]
    fn a_store_can_be_shared_as_a_trait_object() {
        // The service holds the configured store behind one pointer for every Worker and client
        // call.
        fn _shared(_: Arc<dyn StreamStore>) {}
    }
}
