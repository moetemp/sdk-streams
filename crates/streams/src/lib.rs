#![warn(missing_docs)]

//! The Stream runtime of Temporal SDKs.
//!
//! The crate serves the Stream service on the customer's Redis. Lang's bridge builds the runtime
//! per client and calls it with an operation name and proto bytes. Core has no hook for it. A
//! Workflow makes each call from a Local Activity.

pub mod digest;
pub mod epoch;
mod error;
pub mod ids;
pub mod memory;
mod metrics;
pub mod offset;
pub mod protos;
mod record;
pub mod redis;
mod runtime;
#[cfg(test)]
mod server {
    include!("../tests/support/server.rs");
}
mod service;
pub mod store;

pub use error::{Reason, StreamError, StreamResult};
pub use runtime::{
    MEMORY_RUNTIME_NAME, MemoryStreamRuntime, REDIS_RUNTIME_NAME, RedisStreamRuntime,
    RuntimeOptions, StreamRuntime,
};

/// The protos that lang and the crate exchange.
pub mod proto {
    pub use crate::protos::temporal::{
        api::{enums::v1::*, stream::v1::*, streamservice::v1::*},
        sdk::streams::v1::*,
    };
    // Both packages define `StreamErrorDetails`. Runtime errors use the one of `sdk.streams.v1`.
    pub use crate::protos::temporal::sdk::streams::v1::StreamErrorDetails;
}
