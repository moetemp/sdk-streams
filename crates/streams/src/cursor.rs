//! Cursor tokens bound to the store and the stream that minted them.
//!
//! A token reads `<store>:<stream hash>:<position>`. The stream hash is a short digest of the
//! stream's identity, so a store refuses a cursor from another stream at the call instead of
//! resuming at an unrelated position that happens to exist. The position is the store's own.

use crate::{StreamError, StreamResult};
use sha2::{Digest, Sha256};

/// Reads from the oldest record the stream still retains.
pub const BEGINNING: &str = "";

/// Reads only what is appended after the read starts. It has no position, so it is resolved when
/// the read starts.
pub const END: &str = "$end";

const HASH_LENGTH: usize = 8;

/// The short digest that binds a cursor to one stream.
///
/// Taken over the namespace, the owner and the topic. A run id is never part of it, because a
/// stream follows its owner's run chain and a cursor stays valid across Continue-as-New. Eight
/// hex characters catch a mistake, not a crafted token, so the hash is no authorization check.
pub fn stream_hash(namespace: &str, owner_kind: &str, owner_id: &str, topic: &str) -> String {
    let mut digest = Sha256::new();
    for part in [namespace, owner_kind, owner_id, topic] {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part.as_bytes());
    }
    digest
        .finalize()
        .iter()
        .take(HASH_LENGTH / 2)
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A cursor that names `position` on the stream whose hash is `stream`.
pub fn mint_cursor(store: &str, stream: &str, position: &str) -> String {
    format!("{store}:{stream}:{position}")
}

/// The position inside `cursor`, or `None` for [BEGINNING].
///
/// Fails with a cursor error when `cursor` is [END], which has no position, when another store or
/// another stream minted it, or when it is not a token at all.
pub fn cursor_position<'a>(
    cursor: &'a str,
    store: &str,
    stream: &str,
) -> StreamResult<Option<&'a str>> {
    if cursor == BEGINNING {
        return Ok(None);
    }
    if cursor == END {
        return Err(StreamError::cursor(
            "END has no position; resolve it at the read",
        ));
    }
    let mut parts = cursor.splitn(3, ':');
    let (Some(minted_by), Some(minted_for), Some(position)) =
        (parts.next(), parts.next(), parts.next())
    else {
        return Err(not_a_cursor(cursor));
    };
    if position.is_empty() {
        return Err(not_a_cursor(cursor));
    }
    if minted_by != store {
        return Err(StreamError::cursor(format!(
            "cursor {cursor:?} was minted by the {minted_by:?} store, not the {store:?} store"
        )));
    }
    if minted_for != stream {
        return Err(StreamError::cursor(format!(
            "cursor {cursor:?} belongs to another stream; a cursor resumes only the stream it \
             was read from"
        )));
    }
    Ok(Some(position))
}

fn not_a_cursor(cursor: &str) -> StreamError {
    StreamError::cursor(format!("cursor {cursor:?} is not a stream cursor"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::StreamFailureKind;

    #[test]
    fn the_stream_hash_matches_the_python_sdk() {
        // Cursors minted before the store moved into Core must keep resuming.
        assert_eq!(stream_hash("ns", "workflow", "wf", "a"), "8162b717");
        assert_eq!(
            stream_hash("default", "workflow", "order-17", "tokens"),
            "e82d0e98"
        );
    }

    #[test]
    fn the_stream_hash_is_short_and_length_delimited() {
        assert_eq!(stream_hash("ns", "workflow", "wf", "a").len(), 8);
        assert_ne!(
            stream_hash("ns", "workflow", "a:b", "c"),
            stream_hash("ns", "workflow", "a", "b:c")
        );
    }

    #[test]
    fn a_cursor_is_bound_to_its_store_and_stream() {
        let one = stream_hash("ns", "workflow", "wf", "a");
        let cursor = mint_cursor("memory", &one, "7");
        assert_eq!(cursor_position(&cursor, "memory", &one), Ok(Some("7")));
        assert_eq!(cursor_position(BEGINNING, "memory", &one), Ok(None));

        let error = cursor_position(&cursor, "redis", &one).unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Cursor);
        assert!(error.message.contains("minted by the \"memory\" store"));
        for other in [
            stream_hash("ns", "workflow", "wf", "b"),
            stream_hash("ns", "workflow", "wf2", "a"),
            stream_hash("ns2", "workflow", "wf", "a"),
            stream_hash("ns", "activity", "wf", "a"),
        ] {
            let error = cursor_position(&cursor, "memory", &other).unwrap_err();
            assert!(error.message.contains("another stream"), "{error}");
        }
        for token in ["garbage", "memory:", &format!("memory:{one}:")] {
            let error = cursor_position(token, "memory", &one).unwrap_err();
            assert_eq!(error.kind, StreamFailureKind::Cursor, "{token}");
        }
        let error = cursor_position(END, "memory", &one).unwrap_err();
        assert!(error.message.contains("END"));
    }

    #[test]
    fn a_position_may_hold_colons() {
        let one = stream_hash("ns", "workflow", "wf", "a");
        let cursor = mint_cursor("native", &one, "3:4");
        assert_eq!(cursor_position(&cursor, "native", &one), Ok(Some("3:4")));
    }
}
