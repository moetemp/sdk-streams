//! The archive tier: object storage behind a hot store, for streams that must stay readable
//! longer than the hot store keeps them.
//!
//! The archive keeps the hot store's positions, so cursors and replay reads don't change when a
//! read moves from the hot store to the archive. [archive_topic] copies a topic's records into
//! segments and then moves the hot store's archive hold past them, so the hot store's retention
//! drops only what the archive holds. [crate::TieredStore] reads from both.

mod format;
mod local;
#[cfg(feature = "archive")]
mod object;

pub(crate) use format::{
    ArchivedClose, CLOSE_NAME, Segment, holding_start, owner_prefixes, segment_name,
    segments_start, sort_key, topic_prefix,
};
pub use local::LocalFileArchive;
#[cfg(feature = "archive")]
pub use object::ObjectStoreArchive;

use crate::{
    ARCHIVE_HOLDER, StreamResult, StreamStore,
    proto::{ChainId, StoreReadRequest, StoredRecord},
};
use prost::Message;

/// Object storage that holds archived streams, keyed by names that sort in log order.
///
/// Every call is safe to repeat. Names are relative to the archive's root.
#[async_trait::async_trait]
pub trait StreamArchive: Send + Sync {
    /// Writes `bytes` as `name` unless an object of that name exists. Answers whether it wrote.
    /// An existing object stays as it is.
    async fn put_new(&self, name: &str, bytes: Vec<u8>) -> StreamResult<bool>;

    /// The object at `name`, or `None` when there is none.
    async fn get(&self, name: &str) -> StreamResult<Option<Vec<u8>>>;

    /// The first name that starts with `prefix` and sorts after `after`, in byte order.
    async fn first_after(&self, prefix: &str, after: &str) -> StreamResult<Option<String>>;

    /// The last name that starts with `prefix`, in byte order.
    async fn last(&self, prefix: &str) -> StreamResult<Option<String>>;

    /// Deletes every object whose name starts with `prefix`. Answers how many it deleted.
    async fn delete_prefix(&self, prefix: &str) -> StreamResult<u64>;
}

/// When [archive_topic] ends a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentLimits {
    /// The most records one segment holds.
    pub records: u32,
    /// A segment ends at the first record that takes it to this many bytes or more.
    pub bytes: usize,
}

impl Default for SegmentLimits {
    fn default() -> Self {
        Self {
            records: 10_000,
            bytes: 4 << 20,
        }
    }
}

/// What one [archive_topic] call did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArchiveProgress {
    /// How many segments the call wrote.
    pub segments: u32,
    /// The newest position the archive holds now, or empty when it holds none.
    pub archived: String,
    /// Whether the topic is closed and the archive holds it to its end, with its close.
    pub finished: bool,
}

/// How many records one hot store read asks for.
const READ_PAGE: u32 = 1_000;

/// Copies the records of a topic that the archive doesn't hold yet from `hot` into segments, and
/// moves the hot store's archive hold past each one.
///
/// It writes only full segments, unless `flush_tail` is set, or the topic is closed. Once a
/// closed topic is held to its end, it writes `close.pb` and releases the archive's hold.
///
/// Two calls on one topic at once are safe. Both name a segment by its last position and write it
/// only when no object has that name, and records never change once a hot store holds them. A
/// segment that a call finds written holds the same records, so the call only moves the hold.
pub async fn archive_topic(
    hot: &dyn StreamStore,
    archive: &dyn StreamArchive,
    chain: &ChainId,
    topic: &str,
    limits: SegmentLimits,
    flush_tail: bool,
) -> StreamResult<ArchiveProgress> {
    let prefix = topic_prefix(chain, topic);
    let mut progress = ArchiveProgress {
        archived: hot.archived(chain, topic).await?.unwrap_or_default(),
        ..Default::default()
    };
    let mut segment = Segment {
        after_position: progress.archived.clone(),
        records: Vec::new(),
    };
    let mut bytes = 0;
    let (closed, close) = loop {
        let page = match hot
            .read(StoreReadRequest {
                chain: Some(chain.clone()),
                topic: topic.to_string(),
                after_position: segment.last_position_or_after().to_string(),
                wait: None,
                max_records: READ_PAGE.min(limits.records.max(1)),
            })
            .await
        {
            Ok(page) => page,
            Err(error)
                if error.kind == crate::proto::StreamFailureKind::Expired
                    && segment.records.is_empty() =>
            {
                // The hold lapsed and retention dropped records the archive never took. The
                // segment starts after the newest dropped one, and says so, so a read before it
                // fails as expired and a read at it goes on.
                tracing::warn!(topic, %error, "The archive lost records that retention dropped");
                let Some(dropped) = hot.dropped_after(chain, topic, &progress.archived).await?
                else {
                    return Err(error);
                };
                let page = hot
                    .read(StoreReadRequest {
                        chain: Some(chain.clone()),
                        topic: topic.to_string(),
                        after_position: dropped.clone(),
                        max_records: READ_PAGE.min(limits.records.max(1)),
                        ..Default::default()
                    })
                    .await?;
                segment.after_position = dropped;
                page
            }
            Err(error) => return Err(error),
        };
        let caught_up = page.records.is_empty();
        for record in page.records {
            bytes += record.encoded_len();
            segment.records.push(record);
            if segment.records.len() >= limits.records.max(1) as usize || bytes >= limits.bytes {
                write_segment(hot, archive, chain, topic, &prefix, &mut segment).await?;
                progress.segments += 1;
                progress.archived = segment.after_position.clone();
                bytes = 0;
            }
        }
        if caught_up {
            break (page.closed, page.close);
        }
    };
    if !segment.records.is_empty() && (flush_tail || closed) {
        write_segment(hot, archive, chain, topic, &prefix, &mut segment).await?;
        progress.segments += 1;
        progress.archived = segment.after_position.clone();
    }
    if closed && segment.records.is_empty() {
        let archived = ArchivedClose {
            close,
            last_position: progress.archived.clone(),
            newest_ever: hot.newest_ever(chain, topic).await?,
        };
        archive
            .put_new(&format!("{prefix}{CLOSE_NAME}"), archived.encode_to_vec())
            .await?;
        hot.release_floor(chain, topic, ARCHIVE_HOLDER).await?;
        progress.finished = true;
    }
    Ok(progress)
}

impl Segment {
    fn last_position_or_after(&self) -> &str {
        match self.last_position() {
            "" => &self.after_position,
            last => last,
        }
    }
}

/// Writes `segment` once, moves the hold to its end, and starts the next segment after it.
async fn write_segment(
    hot: &dyn StreamStore,
    archive: &dyn StreamArchive,
    chain: &ChainId,
    topic: &str,
    prefix: &str,
    segment: &mut Segment,
) -> StreamResult<()> {
    let last = segment.last_position().to_string();
    archive
        .put_new(&segment_name(prefix, &last)?, segment.encode())
        .await?;
    hot.advance_archive(chain, topic, &last).await?;
    *segment = Segment {
        after_position: last,
        records: Vec::<StoredRecord>::new(),
    };
    Ok(())
}
