//! Turns Redis client errors into Stream errors, so no Redis error reaches lang.

use crate::{Reason, StreamError};
use redis::RedisError;

/// The Stream error a Redis error stands for. The scripts name their own refusals with a
/// `STREAM_` code. Every other failure is a Store failure, which the caller may retry.
pub(crate) fn stream_error(error: &RedisError) -> StreamError {
    let detail = error.detail().unwrap_or_default().to_string();
    let reason = match error.code() {
        Some("STREAM_NOT_FOUND") => Reason::NotFound,
        Some("STREAM_EXPIRED") => Reason::Expired,
        Some("STREAM_CLOSED") => Reason::Closed,
        Some("STREAM_FENCED") => Reason::Fenced,
        Some("STREAM_SEQUENCE") => Reason::Sequence,
        Some("STREAM_FORMAT_TOO_NEW") => Reason::FormatTooNew,
        Some("STREAM_INVALID") => Reason::Invalid,
        // The code as the server wrote it, so the message has the code the Redis docs use.
        Some(code) => {
            return StreamError::storage(format!("Redis refused the call: {code} {detail}"));
        }
        None => return StreamError::storage(format!("Redis failed: {error}")),
    };
    StreamError::new(reason, detail)
}

/// Maps a Redis result.
pub(crate) trait Mapped<T> {
    fn mapped(self) -> Result<T, StreamError>;
}

impl<T> Mapped<T> for Result<T, RedisError> {
    fn mapped(self) -> Result<T, StreamError> {
        self.map_err(|error| stream_error(&error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server(code: &str, detail: &str) -> RedisError {
        // Parsed as the wire carries it, so the code and detail split as a real reply's do.
        redis::parse_redis_value(format!("-{code} {detail}\r\n").as_bytes())
            .unwrap()
            .extract_error()
            .unwrap_err()
    }

    #[test]
    fn a_script_refusal_keeps_its_reason_and_anything_else_is_storage() {
        for (code, reason) in [
            ("STREAM_NOT_FOUND", Reason::NotFound),
            ("STREAM_EXPIRED", Reason::Expired),
            ("STREAM_CLOSED", Reason::Closed),
            ("STREAM_FENCED", Reason::Fenced),
            ("STREAM_SEQUENCE", Reason::Sequence),
            ("STREAM_FORMAT_TOO_NEW", Reason::FormatTooNew),
            ("STREAM_INVALID", Reason::Invalid),
        ] {
            let error = stream_error(&server(code, "why it refused"));
            assert_eq!(
                (error.reason, error.message.as_str()),
                (reason, "why it refused")
            );
        }
        for code in ["NOPERM", "READONLY", "OOM", "LOADING"] {
            let error = stream_error(&server(code, "x"));
            assert_eq!(error.reason, Reason::Storage, "{code}");
            assert!(error.message.contains(code), "{error}");
        }
    }
}
