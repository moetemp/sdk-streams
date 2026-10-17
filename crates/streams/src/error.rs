use crate::proto::{StreamFailure, StreamFailureKind};

/// The result of a stream call.
pub type StreamResult<T> = Result<T, StreamError>;

/// Why a stream call failed. It crosses to lang as a [StreamFailure], one kind per class, so
/// every SDK raises the same error for the same failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct StreamError {
    /// The class of the failure.
    pub kind: StreamFailureKind,
    /// What happened, for people.
    pub message: String,
    /// The cursor of the record that failed, for [StreamFailureKind::Record].
    pub cursor: Option<String>,
}

impl StreamError {
    /// A failure of `kind`.
    pub fn new(kind: StreamFailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            cursor: None,
        }
    }

    /// The store failed or could not be reached.
    pub fn storage(message: impl Into<String>) -> Self {
        Self::new(StreamFailureKind::Storage, message)
    }

    /// The append may or may not have landed.
    pub fn outcome_unknown(message: impl Into<String>) -> Self {
        Self::new(StreamFailureKind::OutcomeUnknown, message)
    }

    /// The store refused the write and wrote nothing.
    pub fn refused(message: impl Into<String>) -> Self {
        Self::new(StreamFailureKind::Refused, message)
    }

    /// The stream is closed and refuses appends.
    pub fn closed(message: impl Into<String>) -> Self {
        Self::new(StreamFailureKind::Closed, message)
    }

    /// The cursor is not valid for this stream.
    pub fn cursor(message: impl Into<String>) -> Self {
        Self::new(StreamFailureKind::Cursor, message)
    }

    /// The cursor names a record the store no longer retains.
    pub fn expired(message: impl Into<String>) -> Self {
        Self::new(StreamFailureKind::Expired, message)
    }

    /// The owner does not exist, or nothing is known about the stream.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StreamFailureKind::NotFound, message)
    }

    /// The call's input breaks its contract, so the same call fails the same way.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(StreamFailureKind::InvalidArgument, message)
    }

    /// This release or this store does not offer the capability.
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(StreamFailureKind::Unsupported, message)
    }

    /// A stored record at `cursor` could not be read.
    pub fn record(cursor: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: StreamFailureKind::Record,
            message: message.into(),
            cursor: Some(cursor.into()),
        }
    }
}

impl From<StreamError> for StreamFailure {
    fn from(error: StreamError) -> Self {
        StreamFailure {
            kind: error.kind as i32,
            message: error.message,
            cursor: error.cursor.unwrap_or_default(),
        }
    }
}

impl From<StreamFailure> for StreamError {
    fn from(failure: StreamFailure) -> Self {
        Self {
            kind: StreamFailureKind::try_from(failure.kind).unwrap_or(StreamFailureKind::Storage),
            message: failure.message,
            cursor: Some(failure.cursor).filter(|cursor| !cursor.is_empty()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failure_round_trips_through_its_proto() {
        let error = StreamError::record("redis:8162b717:1-0", "bad bytes");
        let failure = StreamFailure::from(error.clone());
        assert_eq!(failure.kind, StreamFailureKind::Record as i32);
        assert_eq!(failure.cursor, "redis:8162b717:1-0");
        assert_eq!(StreamError::from(failure), error);
    }

    #[test]
    fn an_invalid_argument_crosses_as_its_own_kind() {
        let failure = StreamFailure::from(StreamError::invalid_argument("no producer id"));
        assert_eq!(failure.kind, 12);
        assert_eq!(
            StreamError::from(failure).kind,
            StreamFailureKind::InvalidArgument
        );
    }

    #[test]
    fn a_kind_from_a_later_release_reads_as_a_storage_failure() {
        let failure = StreamFailure {
            kind: 99,
            message: "new".to_string(),
            cursor: String::new(),
        };
        let error = StreamError::from(failure);
        assert_eq!(error.kind, StreamFailureKind::Storage);
        assert_eq!(error.cursor, None);
    }
}
