#![warn(missing_docs)]

//! Stream stores for Temporal Workflows.
//!
//! A stream is a topic of a Workflow, kept in a store outside Temporal and keyed by the
//! Workflow's run chain. This crate holds what every store and every SDK share: the
//! [StreamStore] trait a store implements, the cursor format, the record helpers that stamp
//! records, and the error every call fails with. Lang reaches a store through Core's
//! `StreamService`, and Core's Worker appends a Workflow's own output through the same trait.

mod close;
mod cursor;
mod error;
mod handle;
mod memory;
mod owner;
mod reader;
mod record;
#[cfg(feature = "redis")]
mod redis;
mod service;
mod store;

#[cfg(feature = "redis")]
pub use self::redis::{RedisStore, RedisStoreOptions};
pub use close::{CHAIN_LOG, append_batch, close_chain, close_topic};
pub use cursor::{BEGINNING, END, cursor_position, mint_cursor, stream_hash};
pub use error::{StreamError, StreamResult};
pub use handle::{OWNER_RECHECK, OWNER_START_GRACE, Streams, StreamsOptions};
pub use memory::MemoryStore;
pub use owner::{OwnerClient, OwnerDescription, OwnerError};
pub use reader::{
    DEFAULT_MAX_RECORDS, MAX_STATE_BYTES, MAX_TRACKED_PRODUCERS, ReadTarget, read_page,
};
pub use record::{
    WORKFLOW_OWNER_KIND, activity_producer_id, check_append_digest, check_record_limit,
    check_store_limits, close_record, stored_append_record, topic_record, workflow_batch_digest,
    workflow_producer_id,
};
pub use service::StreamService;
pub use store::{Capabilities, ConditionForms, MAX_BATCH_BYTES, MAX_RECORD_BYTES, StreamStore};

/// The protos stores and lang exchange.
pub mod proto {
    pub use temporalio_common::protos::{
        coresdk::streams::*,
        temporal::sdk::streams::v1::{StreamRecord, StreamRecordKind},
    };
}
