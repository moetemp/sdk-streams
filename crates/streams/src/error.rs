//! The one error every call fails with, and its form on the extension boundary.

use crate::proto::StreamErrorDetails;
use prost::Message;
use temporalio_sdk_core::ExtensionError;
use tonic::Code;

/// The result of every fallible call in the crate.
pub type StreamResult<T> = Result<T, StreamError>;

/// Why a call failed. Each reason has one status code, and lang maps the reason to one facade
/// error class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reason {
    /// Nobody created the Stream, also after `create_wait`.
    NotFound,
    /// The Stream's Retention or safety TTL passed.
    Expired,
    /// The Stream is closed.
    Closed,
    /// The Producer's epoch is lower than the newest one, its Workflow Task was withdrawn, or a
    /// Workflow retry withdrew its run.
    Fenced,
    /// The batch has a lower `sequence` than the last one of its epoch.
    Sequence,
    /// A newer format version wrote the Stream.
    FormatTooNew,
    /// The Stream exists with another Retention.
    AlreadyExists,
    /// The offset is below zero or past the newest offset + 1.
    Offset,
    /// A Record or a batch is over the Store's limit.
    TooLarge,
    /// The request is malformed.
    Invalid,
    /// The Store failed or did not answer.
    Storage,
    /// No writer has the id.
    UnknownWriter,
    /// The Store lacks a capability that the call needs.
    Unsupported,
}

impl Reason {
    /// The token lang reads from the error details.
    pub fn token(self) -> &'static str {
        match self {
            Reason::NotFound => "stream_not_found",
            Reason::Expired => "stream_expired",
            Reason::Closed => "stream_closed",
            Reason::Fenced => "stream_fenced",
            Reason::Sequence => "stream_sequence",
            Reason::FormatTooNew => "stream_format_too_new",
            Reason::AlreadyExists => "stream_already_exists",
            Reason::Offset => "stream_offset",
            Reason::TooLarge => "stream_too_large",
            Reason::Invalid => "stream_invalid",
            Reason::Storage => "stream_storage",
            Reason::UnknownWriter => "stream_unknown_writer",
            Reason::Unsupported => "stream_unsupported",
        }
    }

    /// The status code of the reason, as in the proposal's Appendix A.
    pub fn code(self) -> Code {
        match self {
            Reason::NotFound | Reason::Expired | Reason::UnknownWriter => Code::NotFound,
            Reason::Closed | Reason::Fenced | Reason::Sequence | Reason::FormatTooNew => {
                Code::FailedPrecondition
            }
            Reason::AlreadyExists => Code::AlreadyExists,
            Reason::Offset | Reason::TooLarge | Reason::Invalid => Code::InvalidArgument,
            Reason::Storage => Code::Unavailable,
            Reason::Unsupported => Code::Unimplemented,
        }
    }
}

/// A failed Stream call.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{}: {message}", reason.token())]
pub struct StreamError {
    /// Why the call failed.
    pub reason: Reason,
    /// What happened, for a log or the facade error.
    pub message: String,
}

impl StreamError {
    /// Creates an error.
    pub fn new(reason: Reason, message: impl Into<String>) -> Self {
        Self {
            reason,
            message: message.into(),
        }
    }

    /// A malformed request.
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(Reason::Invalid, message)
    }

    /// A Store failure.
    pub fn storage(message: impl Into<String>) -> Self {
        Self::new(Reason::Storage, message)
    }
}

impl From<StreamError> for ExtensionError {
    fn from(error: StreamError) -> Self {
        let details = StreamErrorDetails {
            reason: error.reason.token().to_string(),
        };
        ExtensionError::new(error.reason.code(), error.message)
            .with_details(details.encode_to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_error_carries_its_code_and_reason_to_lang() {
        let error: ExtensionError =
            StreamError::new(Reason::FormatTooNew, "format 2 is newer than 1").into();
        assert_eq!(error.code, Code::FailedPrecondition);
        assert_eq!(error.message, "format 2 is newer than 1");
        let details = StreamErrorDetails::decode(error.details.as_slice()).unwrap();
        assert_eq!(details.reason, "stream_format_too_new");
    }
}
