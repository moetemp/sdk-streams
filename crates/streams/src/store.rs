//! What every Store gives the Stream service: the Redis Store in production and the memory
//! Store in tests. Both keep the same rules, which the Lua scripts state.

use crate::{
    Reason, StreamError, StreamResult,
    proto::{
        DescribeStreamRequest, DescribeStreamResponse, Producer, ReadRecordsRequest,
        ReadRecordsResponse, StreamRef, WorkflowTaskEpoch,
    },
};
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

/// The format version this crate writes. A Stream with a higher one refuses writes.
pub const FORMAT_VERSION: u32 = 1;
/// A Record's most bytes in a Store.
pub const MAX_RECORD_BYTES: usize = 1536 * 1024;
/// An append's most bytes in a Store.
pub const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;
/// How long the meta outlives the log, so a read can tell an expired Stream from a missing one.
pub const TOMBSTONE: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Who created a Stream, which decides its TTL while it is open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Owner {
    /// The Stream stays `safety_ttl` after its last write until it closes.
    Workflow,
    /// The Stream stays one Retention after its last write.
    Client,
}

/// What a create found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateOutcome {
    /// The create wrote the key.
    Created,
    /// The Stream exists with the same Retention.
    Exists,
    /// The Stream exists with another Retention or auto close, which stays.
    Conflict,
    /// The Stream is closed.
    Closed,
    /// The Stream expired, and its Tombstone is still there.
    Expired,
}

/// The answer of a create.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Created {
    /// What the create found.
    pub outcome: CreateOutcome,
    /// The Retention of the Stream, the stored one if it existed.
    pub retention: Duration,
    /// Whether the create withdrew a retry run's previous run.
    pub flagged: bool,
    /// The auto close period of the Stream, the stored one if it existed.
    pub auto_close: Option<Duration>,
}

/// The withdrawal facts of a Workflow batch or close, and the `Superseded` item they make.
#[derive(Debug, Clone, Default)]
pub struct Withdrawal<'a> {
    /// The previous completed Workflow Task of the run.
    pub previous_completed: Option<&'a WorkflowTaskEpoch>,
    /// The started event ids of the run's failed Workflow Tasks.
    pub failed_tasks: &'a [i64],
    /// A retry run's previous run.
    pub withdraw_run: &'a str,
    /// The encoded Supersession, which the log keeps when the batch withdraws something.
    pub supersession: &'a [u8],
    /// A Workflow batch whose task facts come later, with the task's completion. It withdraws
    /// nothing and keeps no facts, since a batch with no previous completed task would
    /// withdraw every earlier task. A retry run's `withdraw_run` still applies.
    pub defer: bool,
}

/// One batch of one Producer.
#[derive(Debug, Clone)]
pub struct Batch<'a> {
    /// The Producer, with its epoch.
    pub producer: &'a Producer,
    /// The batch number in this epoch.
    pub sequence: i64,
    /// The encoded Records.
    pub records: &'a [Vec<u8>],
    /// What the batch withdraws.
    pub withdrawal: Withdrawal<'a>,
}

/// The answer of an append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Appended {
    /// The offsets of the first and the last entry, `None` for an empty batch with no
    /// `Superseded` item.
    pub offsets: Option<(i64, i64)>,
    /// The batch was in the Store, so the offsets are the first answer's.
    pub resend: bool,
    /// The batch withdrew earlier Records.
    pub flagged: bool,
}

/// The answer of a close.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed {
    /// The offset of the close Record.
    pub offset: i64,
    /// Someone closed the Stream before.
    pub already: bool,
    /// The close withdrew earlier Records.
    pub flagged: bool,
}

/// The state of a Stream as one read saw it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamState {
    /// The meta key exists, live or as a Tombstone.
    pub exists: bool,
    /// The format version.
    pub version: u32,
    /// The offset of the close Record, if the Stream is closed.
    pub closed_at: Option<i64>,
    /// The offset of the newest entry written, zero if none.
    pub newest: i64,
    /// The offset of the newest entry that Retention dropped, zero if none.
    pub trimmed: i64,
    /// The Retention.
    pub retention: Duration,
    /// When the Stream stops taking writes, in ms since 1970.
    pub expires_ms: i64,
    /// The server time of the read, in ms since 1970.
    pub now_ms: i64,
    /// The auto close period, if the Stream has one.
    pub auto_close: Option<Duration>,
    /// When the Stream was created or last took a Record, in ms since 1970. The auto close
    /// counts from it.
    pub last_append_ms: i64,
}

impl StreamState {
    /// The Stream is a Tombstone.
    pub fn expired(&self) -> bool {
        self.exists && self.expires_ms <= self.now_ms
    }

    /// When the auto close of an open Stream is due, in ms since 1970.
    pub fn auto_close_at_ms(&self) -> Option<i64> {
        if self.closed_at.is_some() || self.expired() {
            return None;
        }
        let period = i64::try_from(self.auto_close?.as_millis()).unwrap_or(i64::MAX);
        Some(self.last_append_ms.saturating_add(period))
    }

    /// The auto close period passed with no append, so the next write step closes the Stream.
    pub fn auto_close_due(&self) -> bool {
        self.auto_close_at_ms().is_some_and(|at| at <= self.now_ms)
    }

    /// The offset below which Retention dropped or hides the Records, as of the read.
    pub fn retained_from(&self) -> i64 {
        let cut = self.now_ms - self.retention.as_millis() as i64;
        let hidden = if cut > 0 {
            cut << crate::offset::SEQ_BITS
        } else {
            0
        };
        hidden.max(self.trimmed + 1)
    }
}

/// One log entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Its offset.
    pub offset: i64,
    /// The encoded Record, absent on an entry that only withdraws.
    pub record: Option<Vec<u8>>,
    /// The encoded Supersession of a flagged batch.
    pub supersession: Option<Vec<u8>>,
}

/// What a Store does atomically beyond the log. The service checks a capability before a
/// call that needs it, so a Store that lacks one refuses that call cleanly instead of giving
/// weaker guarantees in silence.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Capabilities(u8);

impl Capabilities {
    /// Each append checks the Producer's epoch and its last `sequence` before it writes.
    pub const PRODUCER_FENCE: Self = Self(1);
    /// Each append flags the first batch of a new epoch that withdraws, keeps the run's task
    /// index and refuses a Local Activity of a withdrawn task.
    pub const WITHDRAWAL: Self = Self(1 << 1);
    /// A retry run's first write withdraws the previous run and fences its Producers.
    pub const RUN_FENCE: Self = Self(1 << 2);
    /// A close writes the close Record and every later append is refused.
    pub const CLOSE: Self = Self(1 << 3);
    /// A write can wait until replicas have it, for `durable_writes`.
    pub const REPLICA_WAIT: Self = Self(1 << 4);
    /// An open Workflow-owned Stream keeps the safety TTL, not the Retention.
    pub const SAFETY_TTL: Self = Self(1 << 5);
    /// A Stream with an auto close period closes with no result when no append came for that
    /// long. Each write step applies it before it acts, so no call sees the Stream open late.
    pub const AUTO_CLOSE: Self = Self(1 << 6);
    /// A Workflow batch whose task facts come later withdraws nothing and keeps no facts, so a
    /// run's Records can go in before a Local Activity of the run starts.
    pub const DEFERRED_WITHDRAWAL: Self = Self(1 << 7);
    /// Every capability.
    pub const ALL: Self = Self(u8::MAX);

    const NAMES: [(Self, &'static str); 8] = [
        (Self::PRODUCER_FENCE, "producer_fence"),
        (Self::WITHDRAWAL, "withdrawal"),
        (Self::RUN_FENCE, "run_fence"),
        (Self::CLOSE, "close"),
        (Self::REPLICA_WAIT, "replica_wait"),
        (Self::SAFETY_TTL, "safety_ttl"),
        (Self::AUTO_CLOSE, "auto_close"),
        (Self::DEFERRED_WITHDRAWAL, "deferred_withdrawal"),
    ];

    /// Both sets.
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// This set without the other one.
    pub const fn without(self, other: Self) -> Self {
        Self(self.0 & !other.0)
    }

    /// Whether every capability of `other` is in this set.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// The names of the capabilities in this set.
    pub fn names(self) -> Vec<&'static str> {
        Self::NAMES
            .iter()
            .filter(|(capability, _)| self.contains(*capability))
            .map(|(_, name)| *name)
            .collect()
    }

    /// Refuses a call that needs a capability this set lacks, and names the missing ones.
    pub fn require(self, needed: Self) -> StreamResult<()> {
        let missing = needed.without(self);
        if missing == Self::default() {
            return Ok(());
        }
        Err(unsupported(&missing.names().join(", ")))
    }
}

fn unsupported(capability: &str) -> StreamError {
    StreamError::new(
        Reason::Unsupported,
        format!("The Store lacks the capability {capability}"),
    )
}

/// The calls of a Store. Each one is atomic, as one Lua script is on Redis.
///
/// The required calls are the log contract: a create key, an atomic batch append, an ordered
/// read with the Stream's state, stable offsets, prefix Retention and a wait for the next
/// entry. An append applies the rules of the capabilities the Store declares. The optional
/// calls refuse by default.
#[async_trait::async_trait]
pub trait Store: Send + Sync + 'static {
    /// What the Store does atomically beyond the log. It has no default, so a Store that wraps
    /// another one must say what it forwards.
    fn capabilities(&self) -> Capabilities;

    /// The Retention of a Stream that its create gives none.
    fn default_retention(&self) -> Duration;

    /// Creates a Stream, or finds it. A retry run's create withdraws its previous run. An
    /// `auto_close` needs [Capabilities::AUTO_CLOSE].
    async fn create(
        &self,
        namespace: &str,
        stream_id: &str,
        retention: Option<Duration>,
        auto_close: Option<Duration>,
        owner: Owner,
        withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Created>;

    /// Creates a Stream that a Workflow run owns. A Store that keeps the owner, such as the
    /// server, overrides it. The default is a Workflow-owned [Store::create].
    async fn create_owned(
        &self,
        namespace: &str,
        stream_id: &str,
        retention: Option<Duration>,
        auto_close: Option<Duration>,
        owner: &RunOwner<'_>,
        withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Created> {
        let _ = owner;
        self.create(
            namespace,
            stream_id,
            retention,
            auto_close,
            Owner::Workflow,
            withdrawal,
        )
        .await
    }

    /// Appends one batch whole, or nothing of it.
    async fn append(
        &self,
        namespace: &str,
        stream_id: &str,
        batch: &Batch<'_>,
    ) -> StreamResult<Appended>;

    /// Reads the Stream's state and up to `count` entries from `from`, inclusive. Zero reads
    /// from the start of the log.
    async fn read(
        &self,
        namespace: &str,
        stream_id: &str,
        from: i64,
        count: usize,
    ) -> StreamResult<(StreamState, Vec<Entry>)>;

    /// Waits until the log has an entry after `after`, or `deadline` passes. Answers whether an
    /// entry came.
    async fn wait_after(
        &self,
        namespace: &str,
        stream_id: &str,
        after: i64,
        deadline: Instant,
    ) -> StreamResult<bool>;

    /// Closes a Stream with an encoded close Record. Needs [Capabilities::CLOSE].
    async fn close(
        &self,
        _namespace: &str,
        _stream_id: &str,
        _producer: &Producer,
        _record: &[u8],
        _withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Closed> {
        Err(unsupported("close"))
    }

    /// Closes the Stream if its auto close is due, for a read that found it due. Answers the
    /// offset of the close Record if the Stream is closed. Needs [Capabilities::AUTO_CLOSE].
    async fn auto_close(&self, _namespace: &str, _stream_id: &str) -> StreamResult<Option<i64>> {
        Err(unsupported("auto_close"))
    }

    /// Waits until `replicas` replicas have this process's writes to the Stream. Answers how
    /// many have them. Needs [Capabilities::REPLICA_WAIT].
    async fn wait_replicas(
        &self,
        _namespace: &str,
        _stream_id: &str,
        _replicas: u32,
        _timeout: Duration,
    ) -> StreamResult<i64> {
        Err(unsupported("replica_wait"))
    }

    /// The Store for flushes that wait for replicas, if it has one of its own.
    fn durable_store(&self) -> Option<Arc<dyn Store>> {
        None
    }

    /// The reads of a Store that answers whole reads and describes itself, such as the
    /// server. The service then forwards those calls and uses no [Store::read].
    fn remote_reads(&self) -> Option<&dyn RemoteReads> {
        None
    }
}

/// The Workflow run that owns a Stream, for a Store that keeps it.
#[derive(Debug, Clone, Copy)]
pub struct RunOwner<'a> {
    /// The run's Producer in the task of the create.
    pub producer: &'a Producer,
    /// Close the Stream when the owner's run chain closes.
    pub close_with_run: bool,
    /// The Stream's ref, with the owner fields.
    pub stream: Option<&'a StreamRef>,
}

/// Whole reads and describes, which a remote Store answers itself.
#[async_trait::async_trait]
pub trait RemoteReads: Send + Sync {
    /// A long-polling read.
    async fn read_records(&self, request: ReadRecordsRequest) -> StreamResult<ReadRecordsResponse>;

    /// The state of a Stream.
    async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_capability_is_refused_by_name() {
        let some = Capabilities::PRODUCER_FENCE.union(Capabilities::CLOSE);
        some.require(Capabilities::CLOSE).unwrap();
        let error = some
            .require(Capabilities::CLOSE.union(Capabilities::REPLICA_WAIT))
            .unwrap_err();
        assert_eq!(error.reason, Reason::Unsupported);
        assert!(error.message.ends_with("replica_wait"), "{}", error.message);
        assert_eq!(Capabilities::ALL.names().len(), 8);
        assert_eq!(
            Capabilities::ALL
                .without(Capabilities::REPLICA_WAIT)
                .names()
                .len(),
            7
        );
    }
}
