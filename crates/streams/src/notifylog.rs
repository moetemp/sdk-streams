//! The notification log: one entry per append, written by the store in the same step as the
//! append, which a driver reads and hands to the server's stream notifiers.
//!
//! With a log, a producer sends nothing to the server, and readers still hear of every append
//! once a driver runs. Entries go to a fixed set of partitions, so drivers can share the work,
//! and a doorbell tells a waiting driver which partition took its first entry. Delivery is at
//! least once. A repeat carries a counter the notifier has seen and delivers nothing.

use crate::{StreamResult, proto::ChainId};
use std::time::Duration;

/// One append, as the log keeps it until a driver hands it to the stream's notifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationEntry {
    /// The entry's id in its partition, which the ack names.
    pub id: String,
    /// The run chain whose stream moved.
    pub chain: ChainId,
    /// The stream's topic.
    pub topic: String,
    /// The store position of the append's last record.
    pub position: String,
}

/// The notification log of a store, for the drivers that read it.
///
/// Every driver reads as one consumer group, so an entry goes to one consumer at a time. An entry
/// stays until a driver acks it. A consumer that stops without an ack leaves its entries pending,
/// and after `claim_idle` another consumer takes them.
#[async_trait::async_trait]
pub trait NotificationLog: Send + Sync {
    /// How many partitions the log has. One stream's entries always land in the same one.
    fn partitions(&self) -> u32;

    /// How many doorbells the log rings. Partition `p` rings doorbell `p % doorbells`.
    fn doorbells(&self) -> u32;

    /// Waits up to `block` for doorbell `doorbell`, as `consumer`, and answers the partitions it
    /// rang. A ring can be lost, so a driver also sweeps with [NotificationLog::busy].
    async fn wait_doorbell(
        &self,
        doorbell: u32,
        consumer: &str,
        block: Duration,
    ) -> StreamResult<Vec<u32>>;

    /// At most `max` entries of `partition` for `consumer`: the entries another consumer took and
    /// left unacked for `claim_idle`, this consumer's own unacked ones, and new ones.
    async fn take(
        &self,
        partition: u32,
        consumer: &str,
        claim_idle: Duration,
        max: usize,
    ) -> StreamResult<Vec<NotificationEntry>>;

    /// Acks and drops entries of `partition` once their notifications landed.
    async fn ack(&self, partition: u32, ids: &[String]) -> StreamResult<()>;

    /// The partitions that hold entries now, acked or not.
    async fn busy(&self) -> StreamResult<Vec<u32>>;

    /// The topics a store with an archive marked as holding records the archive doesn't hold
    /// yet, at most `max` per partition. A log of a store with no archive answers none.
    async fn archive_marks(&self, max: usize) -> StreamResult<Vec<ArchiveMark>> {
        let _ = max;
        Ok(vec![])
    }
}

/// A topic with records to archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveMark {
    /// The run chain that owns the topic.
    pub chain: ChainId,
    /// The topic.
    pub topic: String,
    /// When the topic last took a write, in milliseconds since the Unix epoch. `0` for a closed
    /// topic, which the archive takes first.
    pub written_ms: u64,
    /// How long the topic went without a write, on the store's clock, so a driver whose clock
    /// is off still finds idle topics.
    pub idle_ms: u64,
}
