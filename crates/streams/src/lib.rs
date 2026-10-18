#![warn(missing_docs)]

//! Stream stores for Temporal Workflows.
//!
//! A stream is a topic of a Workflow or a standalone Activity, kept in a store outside Temporal
//! and keyed by the owner's run chain. An Activity run is a chain of one run. This crate holds
//! what every store and every SDK share: the [StreamStore] trait a store implements, the cursor
//! format, the record helpers that stamp records, and the error every call fails with. Lang
//! reaches a store through the crate's [StreamService], which also appends a Workflow's own
//! output through the same trait.

mod close;
mod connection;
mod conventions;
mod cursor;
mod error;
mod follow;
mod handle;
mod links;
mod memory;
mod notify;
mod notifylog;
mod owner;
pub mod protos;
mod reader;
mod record;
#[cfg(feature = "redis")]
mod redis;
mod service;
mod store;
mod subscribe;
mod wake;
mod workflow;

#[cfg(feature = "redis")]
pub use self::redis::{RedisStore, RedisStoreOptions};
pub use close::{CHAIN_LOG, append_batch, close_chain, close_topic};
pub use connection::connect_stream_service;
pub use conventions::{
    CONTENT_HASH_KEY, MAX_LINK_NAME_BYTES, MAX_TOPIC_BYTES, METADATA_ENCODING, check_link,
    check_topic, content_hash_text,
};
pub use cursor::{BEGINNING, END, cursor_position, mint_cursor, stream_hash};
pub use error::{StreamError, StreamResult};
pub use handle::{OWNER_RECHECK, OWNER_START_GRACE, Streams, StreamsOptions};
pub use memory::MemoryStore;
pub use notify::{
    CLOSE_RETRY_CAP, CLOSE_RETRY_FIRST, CLOSE_RETRY_LIMIT, DEFAULT_MAX_NOTIFIERS, FLUSH_LIMIT,
    Notifier, NotifierClient, NotifierPoll, NotifyError, NotifyingStore, StreamNotification,
    WorkflowAttachment, progress_counter,
};
pub use notifylog::{ArchiveMark, NotificationEntry, NotificationLog};
pub use owner::{OwnerClient, OwnerDescription, OwnerError, OwnerId, OwnerStatus, RunClose};
pub use reader::{
    DEFAULT_MAX_RECORDS, MAX_STATE_BYTES, MAX_TRACKED_PRODUCERS, ReadTarget, read_page,
};
pub use record::{
    ACTIVITY_OWNER_KIND, MAX_KEYED_ACTIVITY_ATTEMPT, WORKFLOW_ACTIVITY_OWNER_KIND,
    WORKFLOW_OWNER_KIND, activity_attempt, activity_producer_id, chain_owner, check_append_digest,
    check_record_limit, check_store_limits, close_record, stored_append_record, topic_record,
    workflow_batch_digest, workflow_producer_id,
};
pub use service::StreamService;
pub use store::{
    ARCHIVE_HOLDER, Capabilities, CloseOutcome, ConditionForms, MAX_BATCH_BYTES, MAX_CLOSE_BYTES,
    MAX_FLOOR_HOLDERS, MAX_RECORD_BYTES, StreamStore, empty_close_result,
};
pub use wake::{STREAM_WAKE_TYPE_URL, fold_stream_wake, fold_stream_wakes};
pub use workflow::{
    MAX_APPEND_BYTES, STREAM_COMPLETED_TASKS, STREAM_OWNER_HEADER, STREAM_RETRY_BITS,
    stream_attempt,
};

/// The protos stores and lang exchange.
pub mod proto {
    pub use crate::protos::{
        coresdk::streams::*,
        temporal::sdk::streams::v1::{StreamLink, StreamRecord, StreamRecordKind, stream_link},
    };
    pub use temporalio_common::protos::temporal::api::common::v1::Payload;
}
