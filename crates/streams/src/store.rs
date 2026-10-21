//! What every Store gives the Stream service: the Redis Store in production and the memory
//! Store in tests. Both keep the same rules, which the Lua scripts state.

use crate::{
    StreamResult,
    proto::{Producer, WorkflowTaskEpoch},
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
    /// The Stream exists with another Retention, which stays.
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
}

impl StreamState {
    /// The Stream is a Tombstone.
    pub fn expired(&self) -> bool {
        self.exists && self.expires_ms <= self.now_ms
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

/// The calls of a Store. Each one is atomic, as one Lua script is on Redis.
#[async_trait::async_trait]
pub trait Store: Send + Sync + 'static {
    /// The Retention of a Stream that its create gives none.
    fn default_retention(&self) -> Duration;

    /// Creates a Stream, or finds it. A retry run's create withdraws its previous run.
    async fn create(
        &self,
        namespace: &str,
        stream_id: &str,
        retention: Option<Duration>,
        owner: Owner,
        withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Created>;

    /// Appends one batch.
    async fn append(
        &self,
        namespace: &str,
        stream_id: &str,
        batch: &Batch<'_>,
    ) -> StreamResult<Appended>;

    /// Closes a Stream with an encoded close Record.
    async fn close(
        &self,
        namespace: &str,
        stream_id: &str,
        producer: &Producer,
        record: &[u8],
        withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Closed>;

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

    /// Waits until `replicas` replicas have this process's writes to the Stream. Answers how
    /// many have them.
    async fn wait_replicas(
        &self,
        namespace: &str,
        stream_id: &str,
        replicas: u32,
        timeout: Duration,
    ) -> StreamResult<i64>;

    /// The Store for flushes that wait for replicas, if it has one of its own.
    fn durable_store(&self) -> Option<Arc<dyn Store>> {
        None
    }
}
