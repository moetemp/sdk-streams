#![warn(missing_docs)]

//! The Stream runtime of Temporal SDKs.
//!
//! The crate serves the Stream service of the Brook API proposal on the customer's Redis, or on
//! the server's Stream service through [native::ServiceStore]. Lang's bridge builds the runtime
//! per client and calls it with an operation name and proto bytes. Core has no hook for it. A
//! Workflow makes each call from a Local Activity.

pub mod digest;
pub mod epoch;
mod error;
mod extension;
pub mod ids;
pub mod memory;
mod metrics;
pub mod native;
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
    NATIVE_EXTENSION_NAME, NativeStreamExtension, RedisStreamExtension, StreamExtension,
};
pub use service::{MAX_RECORDS, MAX_WAIT};

/// The protos that lang and the crate exchange.
pub mod proto {
    pub use crate::protos::temporal::{api::streamservice::v1::*, sdk::streams::v1::*};
    // Both packages define `StreamErrorDetails`. Extension errors use the one of `sdk.streams.v1`.
    pub use crate::protos::temporal::sdk::streams::v1::StreamErrorDetails;
}
