#![warn(missing_docs)]

//! The Stream runtime of Temporal SDKs.
//!
//! The crate serves the Stream service of the Brook API proposal on the customer's Redis. Lang
//! reaches it through the bridge's generic extension calls, with an operation name and proto
//! bytes, and Core calls its Workflow Task interceptor around each Workflow Task completion.

pub mod epoch;
mod error;
pub mod ids;
pub mod offset;
pub mod protos;
pub mod redis;
#[cfg(test)]
mod server {
    include!("../tests/support/server.rs");
}

pub use error::{Reason, StreamError, StreamResult};

/// The protos lang and the crate exchange.
pub mod proto {
    pub use crate::protos::temporal::{api::streamservice::v1::*, sdk::streams::v1::*};
}
