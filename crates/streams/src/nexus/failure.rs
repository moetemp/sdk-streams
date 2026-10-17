//! How a stream call's failure crosses to a Nexus caller.
//!
//! The mapping and the class names are N0b's, so a caller in any language raises the class the
//! store raised, and callers saw no change when the handler moved into Core.

use crate::{StreamError, proto::NexusFailure, proto::StreamFailureKind};
use temporalio_common::protos::temporal::api::{
    enums::v1::NexusHandlerErrorRetryBehavior,
    failure::v1::{ApplicationFailureInfo, Failure, NexusHandlerFailureInfo, failure::FailureInfo},
};

/// The class a caller raises for a mistake in the call's input, the same in every SDK.
pub(crate) const INPUT_MISTAKE: &str = "ValueError";

/// The most bytes of one error message that cross. A message lands in the caller's History, and
/// a codec's error may carry any amount of detail.
pub(crate) const MAX_MESSAGE_BYTES: usize = 512;

/// The Nexus `HandlerErrorType` names this table uses.
pub(crate) const BAD_REQUEST: &str = "BAD_REQUEST";
pub(crate) const NOT_FOUND: &str = "NOT_FOUND";
pub(crate) const NOT_IMPLEMENTED: &str = "NOT_IMPLEMENTED";
pub(crate) const INTERNAL: &str = "INTERNAL";
pub(crate) const UNAVAILABLE: &str = "UNAVAILABLE";

/// The class name, the Nexus error type and whether a retry may help, per stream failure.
pub(crate) fn classify(kind: StreamFailureKind) -> (&'static str, &'static str, bool) {
    use StreamFailureKind::*;
    match kind {
        NotFound => ("StreamNotFoundError", NOT_FOUND, false),
        InvalidArgument => (INPUT_MISTAKE, BAD_REQUEST, false),
        Cursor => ("StreamCursorError", BAD_REQUEST, false),
        Expired => ("StreamExpiredError", BAD_REQUEST, false),
        // Only a positions-only replay read fails this way, and the same range fails the same way.
        ReplayMismatch => ("StreamReplayMismatchError", BAD_REQUEST, false),
        ReplayRangeLost => ("StreamReplayRangeLostError", BAD_REQUEST, false),
        ProducerDivergent | ProducerStale => ("StreamProducerError", BAD_REQUEST, false),
        Closed => ("StreamClosedError", BAD_REQUEST, false),
        Unsupported => ("StreamUnsupportedError", NOT_IMPLEMENTED, false),
        // Nothing was written, and the same call gets the same answer until the store changes.
        Refused => ("StreamRefusedError", INTERNAL, false),
        // The write may have landed. The store deduplicates a repeat, so a retry is safe.
        OutcomeUnknown => ("StreamOutcomeUnknownError", UNAVAILABLE, true),
        Storage => ("StreamStorageError", UNAVAILABLE, true),
        // A record that can't be parsed stays that way, so a retry would only loop. The read path
        // hands one over as `RecordWire.error`, and this row is for any other path.
        Record => ("StreamRecordError", INTERNAL, false),
        Unspecified => ("StreamError", INTERNAL, true),
    }
}

/// The failure a stream error crosses as.
pub(crate) fn of_stream(error: &StreamError) -> NexusFailure {
    let (class, kind, retryable) = classify(error.kind);
    failure(class, kind, retryable, &error.message)
}

/// The failure a mistake in the call's input crosses as.
pub(crate) fn input_mistake(message: impl Into<String>) -> NexusFailure {
    of_stream(&StreamError::invalid_argument(message))
}

/// A failure of `kind` with the cause class `class`. The message leads with the class, and the
/// failure carries no stack, so the handler's frames stay on this side.
pub(crate) fn failure(class: &str, kind: &str, retryable: bool, message: &str) -> NexusFailure {
    let message = &capped(message);
    let retry_behavior = if retryable {
        NexusHandlerErrorRetryBehavior::Retryable
    } else {
        NexusHandlerErrorRetryBehavior::NonRetryable
    };
    NexusFailure {
        handler_error_type: kind.to_string(),
        retryable,
        message: format!("{class}: {message}"),
        failure: Some(Failure {
            cause: Some(Box::new(Failure {
                message: message.to_string(),
                failure_info: Some(FailureInfo::ApplicationFailureInfo(
                    ApplicationFailureInfo {
                        r#type: class.to_string(),
                        non_retryable: !retryable,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            })),
            failure_info: Some(FailureInfo::NexusHandlerFailureInfo(
                NexusHandlerFailureInfo {
                    r#type: kind.to_string(),
                    retry_behavior: retry_behavior as i32,
                },
            )),
            ..Default::default()
        }),
    }
}

/// `message`, cut to [MAX_MESSAGE_BYTES].
pub(crate) fn capped(message: &str) -> String {
    if message.len() <= MAX_MESSAGE_BYTES {
        return message.to_string();
    }
    let mut end = MAX_MESSAGE_BYTES - 3;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &message[..end])
}

/// `message` with each double-quoted value left out. Such a value is the caller's input, which
/// the failure would carry into History without the codec.
pub(crate) fn without_values(message: &str) -> String {
    let mut out = String::with_capacity(message.len());
    let mut chars = message.chars();
    while let Some(char) = chars.next() {
        if char != '"' {
            out.push(char);
            continue;
        }
        while let Some(char) = chars.next() {
            match char {
                '\\' => {
                    chars.next();
                }
                '"' => break,
                _ => {}
            }
        }
        if out.ends_with(' ') {
            out.pop();
        }
    }
    out
}

/// `value` if it reads as a name, so a caller's data in its place stays out of the message.
pub(crate) fn named(value: &str) -> String {
    let is_name = value.len() <= 32
        && value
            .chars()
            .all(|char| char.is_ascii_alphanumeric() || char == '_' || char == '-');
    if is_name {
        format!("{value:?}")
    } else {
        "the call names".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_stream_condition_crosses_as_its_own_class() {
        use StreamFailureKind::*;
        for (kind, class, nexus, retryable) in [
            (NotFound, "StreamNotFoundError", NOT_FOUND, false),
            (InvalidArgument, "ValueError", BAD_REQUEST, false),
            (Cursor, "StreamCursorError", BAD_REQUEST, false),
            (Expired, "StreamExpiredError", BAD_REQUEST, false),
            (ProducerDivergent, "StreamProducerError", BAD_REQUEST, false),
            (ProducerStale, "StreamProducerError", BAD_REQUEST, false),
            (Closed, "StreamClosedError", BAD_REQUEST, false),
            (
                Unsupported,
                "StreamUnsupportedError",
                NOT_IMPLEMENTED,
                false,
            ),
            (Refused, "StreamRefusedError", INTERNAL, false),
            (
                OutcomeUnknown,
                "StreamOutcomeUnknownError",
                UNAVAILABLE,
                true,
            ),
            (Storage, "StreamStorageError", UNAVAILABLE, true),
            // A record that can't be parsed stays that way, so a retry would loop.
            (Record, "StreamRecordError", INTERNAL, false),
            // A class the table doesn't name crosses as a retryable internal error, as N0b's
            // table answered any other error.
            (Unspecified, "StreamError", INTERNAL, true),
        ] {
            let crossed = of_stream(&StreamError::new(kind, "why"));
            assert_eq!(crossed.handler_error_type, nexus, "{kind:?}");
            assert_eq!(crossed.retryable, retryable, "{kind:?}");
            assert_eq!(crossed.message, format!("{class}: why"));
            let failure = crossed.failure.unwrap();
            assert!(failure.stack_trace.is_empty());
            let Some(FailureInfo::NexusHandlerFailureInfo(info)) = failure.failure_info else {
                panic!("no Nexus handler info");
            };
            assert_eq!(info.r#type, nexus);
            let cause = failure.cause.unwrap();
            let Some(FailureInfo::ApplicationFailureInfo(application)) = cause.failure_info else {
                panic!("no application info");
            };
            assert_eq!(application.r#type, class);
            assert_eq!(application.non_retryable, !retryable);
            assert_eq!(cause.message, "why");
        }
    }

    #[test]
    fn a_message_crosses_without_values_and_cut_short() {
        assert_eq!(
            without_values(r#"invalid type: string "a \"secret\"", expected a sequence"#),
            "invalid type: string, expected a sequence"
        );
        assert_eq!(named("activity"), "\"activity\"");
        assert_eq!(named("card 4111"), "the call names");
        let long = "é".repeat(400);
        let cut = capped(&long);
        assert!(cut.len() <= MAX_MESSAGE_BYTES && cut.ends_with("..."));
        let crossed = failure(INPUT_MISTAKE, BAD_REQUEST, false, &long);
        assert!(crossed.failure.unwrap().cause.unwrap().message.len() <= MAX_MESSAGE_BYTES);
    }

    #[test]
    fn an_input_mistake_is_a_bad_request() {
        let crossed = input_mistake("an append carries payloads or finishes");
        assert_eq!(crossed.handler_error_type, BAD_REQUEST);
        assert!(!crossed.retryable);
        assert!(crossed.message.starts_with("ValueError: "));
    }
}
