//! Turning Redis client errors into stream errors, so no Redis error reaches lang.

use crate::{StreamError, proto::StreamFailureKind};
use redis::RedisError;

/// Whether a failed call may have changed the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Call {
    Read,
    Write,
    /// A producer's batch. A lang producer retries one whose outcome is unknown with the same
    /// sequence, which the dedupe answers, so a doubtful failure is reported as unknown.
    Append,
}

/// Replies that clear up on their own: a primary demoted in a failover, a server loading its
/// data, a busy script, a slot in migration, a cluster or primary that is down, a server out of
/// memory. Under `noeviction` an `OOM` ends when retention trims the logs, so it isn't final.
const TRANSIENT: [&str; 7] = [
    "OOM",
    "READONLY",
    "LOADING",
    "BUSY",
    "TRYAGAIN",
    "CLUSTERDOWN",
    "MASTERDOWN",
];

/// The stream error a Redis error stands for.
///
/// A connection or timeout failure during a write leaves its outcome unknown. A reply the server
/// sent means it refused the write. The scripts name their own refusals with a `STREAMS_` code.
pub(crate) fn stream_error(error: &RedisError, call: Call) -> StreamError {
    if error.is_io_error() || error.is_timeout() || error.is_connection_dropped() {
        return match call {
            Call::Write | Call::Append => StreamError::outcome_unknown(format!(
                "the write may or may not have been applied: {error}"
            )),
            Call::Read => StreamError::storage(format!("Redis could not be reached: {error}")),
        };
    }
    let detail = error.detail().unwrap_or_default().to_string();
    match error.code() {
        Some("STREAMS_DIVERGENT") => StreamError::new(StreamFailureKind::ProducerDivergent, detail),
        Some("STREAMS_STALE") => StreamError::new(StreamFailureKind::ProducerStale, detail),
        Some("STREAMS_CLOSED") => StreamError::closed(detail),
        Some(code) if TRANSIENT.contains(&code) => match call {
            Call::Append => StreamError::outcome_unknown(format!(
                "Redis could not take the batch for now, so it may or may not have landed: \
                 {code} {detail}"
            )),
            _ => StreamError::storage(format!(
                "Redis could not serve the call for now: {code} {detail}"
            )),
        },
        // The reply as the server wrote it, so the message names the code the Redis docs use.
        Some(code) if call != Call::Read => {
            StreamError::refused(format!("Redis refused the write: {code} {detail}"))
        }
        Some(code) => StreamError::storage(format!("Redis refused the read: {code} {detail}")),
        None => StreamError::storage(format!("Redis failed: {error}")),
    }
}

/// Maps a Redis result for a call.
pub(crate) trait Mapped<T> {
    fn mapped(self, call: Call) -> Result<T, StreamError>;
}

impl<T> Mapped<T> for Result<T, RedisError> {
    fn mapped(self, call: Call) -> Result<T, StreamError> {
        self.map_err(|error| stream_error(&error, call))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::{ErrorKind, ServerErrorKind};

    fn server(code: &str, detail: &str) -> RedisError {
        // Parsed as the wire carries it, so the code and detail split as a real reply's do.
        redis::parse_redis_value(format!("-{code} {detail}\r\n").as_bytes())
            .unwrap()
            .extract_error()
            .unwrap_err()
    }

    #[test]
    fn script_refusals_keep_their_class() {
        for (code, kind) in [
            ("STREAMS_DIVERGENT", StreamFailureKind::ProducerDivergent),
            ("STREAMS_STALE", StreamFailureKind::ProducerStale),
            ("STREAMS_CLOSED", StreamFailureKind::Closed),
        ] {
            for call in [Call::Read, Call::Write] {
                let error = stream_error(&server(code, "sequence 3 is below"), call);
                assert_eq!(error.kind, kind, "{code}");
                assert_eq!(error.message, "sequence 3 is below");
            }
        }
    }

    #[test]
    fn a_reply_refuses_a_write_and_fails_a_read() {
        let refused = server("NOPERM", "this user has no permissions");
        assert_eq!(refused.kind(), ErrorKind::Server(ServerErrorKind::NoPerm));
        let error = stream_error(&refused, Call::Write);
        assert_eq!(error.kind, StreamFailureKind::Refused);
        assert!(error.message.contains("NOPERM"), "{error}");
        assert_eq!(
            stream_error(&refused, Call::Read).kind,
            StreamFailureKind::Storage
        );
    }

    #[test]
    fn rule_8_1_a_reply_that_clears_up_on_its_own_is_a_storage_failure() {
        // A failover, a load or a busy script ends by itself, so the caller retries, not gives up.
        for code in [
            "OOM",
            "READONLY",
            "LOADING",
            "BUSY",
            "TRYAGAIN",
            "CLUSTERDOWN",
            "MASTERDOWN",
        ] {
            for call in [Call::Read, Call::Write] {
                let error = stream_error(&server(code, "for a moment"), call);
                assert_eq!(error.kind, StreamFailureKind::Storage, "{code} {call:?}");
                assert!(error.message.contains(code), "{error}");
            }
            let append = stream_error(&server(code, "for a moment"), Call::Append);
            assert_eq!(append.kind, StreamFailureKind::OutcomeUnknown, "{code}");
        }
        // A reply that won't change on a retry still refuses the write.
        for code in ["NOPERM", "WRONGTYPE"] {
            for call in [Call::Write, Call::Append] {
                let error = stream_error(&server(code, "no"), call);
                assert_eq!(error.kind, StreamFailureKind::Refused, "{code} {call:?}");
            }
        }
    }

    #[test]
    fn a_lost_connection_leaves_a_write_unknown() {
        let lost = RedisError::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ));
        assert_eq!(
            stream_error(&lost, Call::Write).kind,
            StreamFailureKind::OutcomeUnknown
        );
        assert_eq!(
            stream_error(&lost, Call::Read).kind,
            StreamFailureKind::Storage
        );
    }
}
