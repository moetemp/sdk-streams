//! What a PostgreSQL failure means for each kind of call.
//!
//! A server's error text goes into the message only, so lang sees one kind per class whatever
//! the server said.

use crate::{StreamError, proto::StreamFailureKind};

/// What a call did, which decides what a failure means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Call {
    /// An append. Lang retries a doubtful one with the same sequence, and the dedupe answers it.
    Append,
    /// Any other call that changes the store.
    Write,
    /// A call that changes nothing.
    Read,
}

/// Whether a call should run again before it fails: the server rolled it back for a conflict
/// that the next run won't meet.
pub(super) fn retryable(error: &tokio_postgres::Error) -> bool {
    matches!(sqlstate(error), Some("40001" | "40P01"))
}

fn sqlstate(error: &tokio_postgres::Error) -> Option<&str> {
    error.code().map(|code| code.code())
}

/// The kind of failure for `call` from the SQLSTATE of an error reply, or `None` when the
/// connection failed after the call was sent.
pub(super) fn kind(call: Call, sqlstate: Option<&str>) -> StreamFailureKind {
    use StreamFailureKind::{OutcomeUnknown, Refused, Storage};
    let Some(code) = sqlstate else {
        return match call {
            Call::Read => Storage,
            Call::Append | Call::Write => OutcomeUnknown,
        };
    };
    // Class 53 is a lack of resources, and `54000` covers a full NOTIFY queue. Both clear up.
    let transient = matches!(
        code,
        "40001" | "40P01" | "57014" | "57P01" | "57P02" | "57P03" | "25006" | "54000"
    ) || code.starts_with("08")
        || code.starts_with("53");
    match (call, transient) {
        (Call::Read, _) => Storage,
        (Call::Append, true) => OutcomeUnknown,
        (Call::Write, true) => Storage,
        // An error reply in autocommit means a rollback, so the call wrote nothing.
        (Call::Append | Call::Write, false) => Refused,
    }
}

/// The failure for an error of a call that reached the server or may have.
pub(super) fn mapped(call: Call, error: &tokio_postgres::Error) -> StreamError {
    StreamError::new(
        kind(call, sqlstate(error)),
        format!("PostgreSQL: {}", detail(error)),
    )
}

/// The failure for a call that got no answer in time.
pub(super) fn timed_out(call: Call) -> StreamError {
    StreamError::new(
        kind(call, None),
        "PostgreSQL did not answer in time; the call may have run",
    )
}

/// The failure for a call that never reached the server.
pub(super) fn unreachable(error: impl std::fmt::Display) -> StreamError {
    StreamError::storage(format!("PostgreSQL could not be reached: {error}"))
}

fn detail(error: &tokio_postgres::Error) -> String {
    match error.as_db_error() {
        Some(db) => format!("{} ({})", db.message(), db.code().code()),
        None => error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use StreamFailureKind::{OutcomeUnknown, Refused, Storage};

    #[test]
    fn each_class_maps_as_the_design_says() {
        for (code, append, write) in [
            (None, OutcomeUnknown, OutcomeUnknown),
            (Some("40001"), OutcomeUnknown, Storage),
            (Some("40P01"), OutcomeUnknown, Storage),
            (Some("57014"), OutcomeUnknown, Storage),
            (Some("08006"), OutcomeUnknown, Storage),
            (Some("08001"), OutcomeUnknown, Storage),
            (Some("57P01"), OutcomeUnknown, Storage),
            (Some("57P03"), OutcomeUnknown, Storage),
            (Some("25006"), OutcomeUnknown, Storage),
            (Some("53300"), OutcomeUnknown, Storage),
            (Some("53100"), OutcomeUnknown, Storage),
            (Some("53200"), OutcomeUnknown, Storage),
            (Some("54000"), OutcomeUnknown, Storage),
            (Some("23505"), Refused, Refused),
            (Some("42883"), Refused, Refused),
            (Some("P0001"), Refused, Refused),
        ] {
            assert_eq!(kind(Call::Append, code), append, "{code:?}");
            assert_eq!(kind(Call::Write, code), write, "{code:?}");
            assert_eq!(kind(Call::Read, code), Storage, "{code:?}");
        }
    }

    #[test]
    fn an_unreachable_server_is_a_storage_failure() {
        assert_eq!(unreachable("refused").kind, Storage);
        assert_eq!(timed_out(Call::Append).kind, OutcomeUnknown);
        assert_eq!(timed_out(Call::Read).kind, Storage);
    }
}
