//! The generated protos: the Stream service of the API proposal, and the crate's own calls.

#![allow(missing_docs, unreachable_pub, clippy::all)]

/// The Temporal API packages the crate generates. The rest map to `temporalio-common`.
pub mod temporal {
    pub mod api {
        pub mod streamservice {
            pub mod v1 {
                tonic::include_proto!("temporal.api.streamservice.v1");
            }
        }
    }

    pub mod sdk {
        pub mod streams {
            pub mod v1 {
                tonic::include_proto!("temporal.sdk.streams.v1");
            }
        }
    }
}
