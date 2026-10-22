#![warn(missing_docs)]

//! The Stream runtime of Temporal SDKs.
//!
//! The crate serves the Stream service of the Brook API proposal on the customer's Redis. Lang
//! reaches it through the bridge's generic extension calls, with an operation name and proto
//! bytes. A Workflow makes each call from a Local Activity.

pub mod epoch;
mod error;
mod extension;
pub mod ids;
pub mod memory;
mod metrics;
pub mod offset;
pub mod protos;
mod record;
pub mod redis;
#[cfg(test)]
mod server {
    include!("../tests/support/server.rs");
}
mod service;
pub mod store;
mod writer;

pub use error::{Reason, StreamError, StreamResult};
pub use extension::{
    EXTENSION_NAME, ExtensionOptions, MEMORY_EXTENSION_NAME, MemoryStreamExtension,
    RedisStreamExtension, StreamExtension,
};
pub use service::{MAX_RECORDS, MAX_WAIT};

/// The protos lang and the crate exchange.
pub mod proto {
    pub use crate::protos::temporal::{api::streamservice::v1::*, sdk::streams::v1::*};
}
