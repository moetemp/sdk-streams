//! Stream ids, Producer ids and the Redis key layout. Every SDK must build them the same way, and
//! `testdata/ids.json` has the shared vectors that check it.
//!
//! ```text
//! <prefix>:{<namespace>:<stream id>}:meta    the create key, the fence, dedupe and close state
//! <prefix>:{<namespace>:<stream id>}:log     the log, one entry per Record
//! ```
//!
//! The part in braces is the Redis Cluster hash tag, so that both keys are in one slot for one
//! script. Each part is percent-encoded, so a `:` or a brace in an id can't make two Streams share
//! a key.

use crate::{
    StreamError, StreamResult,
    proto::{Producer, StreamProducerKind},
};

/// The longest Stream id, in bytes after encoding.
pub const MAX_STREAM_ID_BYTES: usize = 4096;

/// The key prefix when the backend sets none.
pub const DEFAULT_KEY_PREFIX: &str = "temporal-streams";

/// Percent-encodes every byte but the unreserved ones, as Python's `quote(text, safe="")` does.
pub fn encode_part(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Decodes the text of [encode_part]. Returns `None` for text that [encode_part] can't make.
pub fn decode_part(text: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(text.len());
    let mut rest = text.as_bytes();
    while let Some((&byte, tail)) = rest.split_first() {
        if byte == b'%' {
            let hex = std::str::from_utf8(tail.get(..2)?).ok()?;
            bytes.push(u8::from_str_radix(hex, 16).ok()?);
            rest = &tail[2..];
        } else {
            bytes.push(byte);
            rest = tail;
        }
    }
    String::from_utf8(bytes).ok()
}

fn join(parts: &[&str]) -> String {
    parts
        .iter()
        .map(|part| encode_part(part))
        .collect::<Vec<_>>()
        .join(":")
}

/// The id of a Workflow's default Stream, `<workflow id>:<first run id>`.
pub fn default_stream_id(workflow_id: &str, first_run_id: &str) -> String {
    join(&[workflow_id, first_run_id])
}

/// The id of a Stream that a Workflow creates with a name, `<workflow id>:<first run id>:<name>`.
pub fn scoped_stream_id(workflow_id: &str, first_run_id: &str, name: &str) -> String {
    join(&[workflow_id, first_run_id, name])
}

/// Refuses an empty or too long Stream id.
pub fn check_stream_id(id: &str) -> StreamResult<()> {
    if id.is_empty() {
        return Err(StreamError::invalid("The Stream id is empty"));
    }
    if id.len() > MAX_STREAM_ID_BYTES {
        return Err(StreamError::invalid(format!(
            "The Stream id has {} bytes, more than {MAX_STREAM_ID_BYTES}",
            id.len()
        )));
    }
    Ok(())
}

/// The Producer id of a Workflow run, `wf:<run id>`.
pub fn workflow_producer_id(run_id: &str) -> String {
    format!("wf:{}", join(&[run_id]))
}

/// The Producer id of a Local Activity call, `la:<run id>:<call key>`.
pub fn local_activity_producer_id(run_id: &str, call_key: &str) -> String {
    format!("la:{}", join(&[run_id, call_key]))
}

/// The Producer id of an Activity, `act:<run id>:<activity id>:<schedule key>`.
pub fn activity_producer_id(run_id: &str, activity_id: &str, schedule_key: &str) -> String {
    format!("act:{}", join(&[run_id, activity_id, schedule_key]))
}

/// The Producer id of a Standalone Activity, `sa:<activity id>:<activity run id>`.
pub fn standalone_activity_producer_id(activity_id: &str, activity_run_id: &str) -> String {
    format!("sa:{}", join(&[activity_id, activity_run_id]))
}

/// The Producer id of a client process, `client:<identity>:<process key>`.
pub fn client_producer_id(identity: &str, process_key: &str) -> String {
    format!("client:{}", join(&[identity, process_key]))
}

/// The Producer id that a client names itself, `client:<name>`. The caller keeps the name across
/// restarts and raises the epoch on each one, so that a new incarnation fences the earlier one.
pub fn named_client_producer_id(name: &str) -> String {
    format!("client:{}", join(&[name]))
}

/// The id prefix of a Producer kind.
pub(crate) fn kind_prefix(kind: StreamProducerKind) -> Option<&'static str> {
    match kind {
        StreamProducerKind::Workflow => Some("wf:"),
        StreamProducerKind::LocalActivity => Some("la:"),
        StreamProducerKind::Activity => Some("act:"),
        StreamProducerKind::StandaloneActivity => Some("sa:"),
        StreamProducerKind::Client => Some("client:"),
        StreamProducerKind::Unspecified => None,
    }
}

/// Checks that a Producer's id has the prefix of its kind, and that the run in the id is its
/// `run_id`. The kind prefix stops a Producer from using the id of another kind.
pub fn check_producer(producer: &Producer) -> StreamResult<()> {
    let kind =
        StreamProducerKind::try_from(producer.kind).unwrap_or(StreamProducerKind::Unspecified);
    let Some(prefix) = kind_prefix(kind) else {
        return Err(StreamError::invalid("The Producer has no kind"));
    };
    let Some(rest) = producer.id.strip_prefix(prefix) else {
        return Err(StreamError::invalid(format!(
            "The Producer id {:?} doesn't start with {prefix:?}, as its kind requires",
            producer.id
        )));
    };
    let parts: Vec<_> = rest.split(':').collect();
    let expected = match kind {
        StreamProducerKind::Workflow => 1,
        StreamProducerKind::LocalActivity | StreamProducerKind::StandaloneActivity => 2,
        // A named client id has one part. A process client id has two.
        StreamProducerKind::Client if parts.len() == 1 => 1,
        StreamProducerKind::Client => 2,
        StreamProducerKind::Activity => 3,
        StreamProducerKind::Unspecified => unreachable!("refused above"),
    };
    if parts.len() != expected || parts.iter().any(|part| decode_part(part).is_none()) {
        return Err(StreamError::invalid(format!(
            "The Producer id {:?} must have {expected} encoded parts after {prefix:?}",
            producer.id
        )));
    }
    let runs = matches!(
        kind,
        StreamProducerKind::Workflow
            | StreamProducerKind::LocalActivity
            | StreamProducerKind::Activity
    );
    if runs && decode_part(parts[0]).as_deref() != Some(producer.run_id.as_str()) {
        return Err(StreamError::invalid(format!(
            "The Producer id {:?} names another run than its run id {:?}",
            producer.id, producer.run_id
        )));
    }
    if kind == StreamProducerKind::Client && !producer.run_id.is_empty() {
        return Err(StreamError::invalid("A client Producer has no run id"));
    }
    Ok(())
}

/// The run id in a Workflow Producer id, or `None` for another id.
pub fn run_of_workflow_producer(id: &str) -> Option<String> {
    decode_part(id.strip_prefix("wf:")?)
}

/// The fields of the `:meta` hash. The field names hold encoded ids only, so a `|` in a field
/// name is always a separator.
pub mod fields {
    use super::encode_part;

    /// The format version of the Stream.
    pub const VERSION: &str = "v";

    /// The newest epoch key of a Producer, the fence.
    pub fn fence(kind: i32, producer_id: &str) -> String {
        format!("e|{kind}|{producer_id}")
    }

    /// The last batch of a Producer's newest epoch, as
    /// `<epoch key>|<sequence>|<first>|<last>|<digest>`. These are the dedupe key, the first and
    /// last entry ids of the batch, and the content digest that a resend must match.
    pub fn dedupe(kind: i32, producer_id: &str) -> String {
        format!("d|{kind}|{producer_id}")
    }

    /// The newest epoch key of a Producer that still has Records in the Stream.
    pub fn holds(kind: i32, producer_id: &str) -> String {
        format!("h|{kind}|{producer_id}")
    }

    /// Set when a Producer of the run wrote a Record to the Stream.
    pub fn run_wrote(run_id: &str) -> String {
        format!("run|{}", encode_part(run_id))
    }

    /// Set when a Workflow retry withdrew the run.
    pub fn withdrawn_run(run_id: &str) -> String {
        format!("wr|{}", encode_part(run_id))
    }
}

/// The keys of one Stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamKeys {
    base: String,
}

impl StreamKeys {
    /// The keys of `stream_id` in `namespace`, under `prefix`.
    pub fn new(prefix: &str, namespace: &str, stream_id: &str) -> Self {
        Self {
            base: format!(
                "{}:{{{}}}",
                encode_part(prefix),
                join(&[namespace, stream_id])
            ),
        }
    }

    /// The hash with the format version, the fence, dedupe and close state.
    pub fn meta(&self) -> String {
        format!("{}:meta", self.base)
    }

    /// The log.
    pub fn log(&self) -> String {
        format!("{}:log", self.base)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn producer(kind: StreamProducerKind, id: &str, run_id: &str) -> Producer {
        Producer {
            kind: kind as i32,
            id: id.to_string(),
            epoch: None,
            run_id: run_id.to_string(),
        }
    }

    #[test]
    fn a_part_round_trips_and_bad_text_does_not_decode() {
        for text in ["plain", "a:b", "é {x}%", ""] {
            assert_eq!(decode_part(&encode_part(text)).as_deref(), Some(text));
        }
        assert_eq!(decode_part("%zz"), None);
        assert_eq!(decode_part("%4"), None);
    }

    #[test]
    fn the_kind_prefix_and_the_run_must_match_the_producer() {
        let run = "run:1";
        let ok = [
            producer(
                StreamProducerKind::Workflow,
                &workflow_producer_id(run),
                run,
            ),
            producer(
                StreamProducerKind::LocalActivity,
                &local_activity_producer_id(run, "k"),
                run,
            ),
            producer(
                StreamProducerKind::Activity,
                &activity_producer_id(run, "a:1", "s"),
                run,
            ),
            producer(
                StreamProducerKind::StandaloneActivity,
                &standalone_activity_producer_id("a", "r"),
                "",
            ),
            producer(
                StreamProducerKind::Client,
                &client_producer_id("me@host", "p"),
                "",
            ),
        ];
        for producer in &ok {
            check_producer(producer).unwrap();
        }
        let bad = [
            producer(StreamProducerKind::Client, &workflow_producer_id(run), run),
            producer(
                StreamProducerKind::Workflow,
                &workflow_producer_id(run),
                "other",
            ),
            producer(StreamProducerKind::Activity, "act:run%3A1:a", run),
            producer(StreamProducerKind::Unspecified, "wf:x", "x"),
            producer(StreamProducerKind::Client, "client:a:b", "run"),
        ];
        for producer in &bad {
            assert_eq!(
                check_producer(producer).unwrap_err().reason,
                crate::Reason::Invalid,
                "{producer:?}"
            );
        }
        assert_eq!(
            run_of_workflow_producer(&workflow_producer_id(run)).as_deref(),
            Some(run)
        );
    }

    #[test]
    fn a_stream_id_is_bounded() {
        check_stream_id("a").unwrap();
        check_stream_id(&"a".repeat(MAX_STREAM_ID_BYTES)).unwrap();
        assert!(check_stream_id("").is_err());
        assert!(check_stream_id(&"a".repeat(MAX_STREAM_ID_BYTES + 1)).is_err());
    }
}
