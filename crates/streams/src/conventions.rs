//! What every SDK and store must agree on for stream records: their stored metadata keys, the
//! topics a store keys, and the links a follower reads.

use crate::protos::temporal::api::stream::v1::StreamReference;
use crate::protos::temporal::sdk::streams::v1::{StreamLink, stream_link};

/// The record metadata key a body's plaintext hash is stored under, as a `binary/plain` payload
/// whose data is the hex SHA-256.
pub const CONTENT_HASH_KEY: &str = "temporal.io/content-hash";

/// The encoding of the payloads stored under [CONTENT_HASH_KEY].
pub const METADATA_ENCODING: &[u8] = b"binary/plain";

/// The longest topic, in UTF-8 bytes, every store keys reliably.
pub const MAX_TOPIC_BYTES: usize = 256;

/// Refuses a topic no store keys reliably, with the reason. A control character could be a
/// separator a store uses.
pub fn check_topic(topic: &str) -> Result<(), String> {
    if topic.is_empty() {
        return Err("a stream needs a topic".to_string());
    }
    if topic.len() > MAX_TOPIC_BYTES {
        return Err(format!(
            "a topic is at most {MAX_TOPIC_BYTES} UTF-8 bytes, not {}",
            topic.len()
        ));
    }
    if topic.chars().any(char::is_control) {
        return Err(format!("topic {topic:?} holds a control character"));
    }
    Ok(())
}

/// The longest link name a writer stores.
pub const MAX_LINK_NAME_BYTES: usize = 256;

/// The longest cursor, namespace or endpoint a link holds.
const MAX_LINK_FIELD_BYTES: usize = 1024;

/// Refuses a link no follower can use, with the reason.
pub fn check_link(link: &StreamLink) -> Result<(), String> {
    if link.name.is_empty() || link.name.len() > MAX_LINK_NAME_BYTES {
        return Err(format!(
            "a link needs a name of 1 to {MAX_LINK_NAME_BYTES} bytes, not {}",
            link.name.len()
        ));
    }
    for (field, value) in [
        ("after", &link.after),
        ("until", &link.until),
        ("namespace", &link.namespace),
        ("endpoint", &link.endpoint),
    ] {
        if value.len() > MAX_LINK_FIELD_BYTES {
            return Err(format!(
                "a link's {field} is at most {MAX_LINK_FIELD_BYTES} bytes, not {}",
                value.len()
            ));
        }
    }
    match stream_link::Event::try_from(link.event) {
        Ok(stream_link::Event::Open) => {
            if !link.until.is_empty() {
                return Err("an OPEN link names no until".to_string());
            }
            let stream = link
                .stream
                .as_ref()
                .ok_or_else(|| "an OPEN link needs the stream it links".to_string())?;
            check_reference(stream)
        }
        Ok(stream_link::Event::Join) => {
            if link.stream.is_some() || !link.after.is_empty() {
                return Err("a JOIN link names only the OPEN it joins and its until".to_string());
            }
            Ok(())
        }
        _ => Err(format!("a link is OPEN or JOIN, not event {}", link.event)),
    }
}

/// Refuses a reference to a stream this release doesn't keep, or whose ids don't fit its kind.
fn check_reference(stream: &StreamReference) -> Result<(), String> {
    use crate::protos::temporal::api::enums::v1::StreamOwnerKind as Kind;
    check_topic(&stream.topic)?;
    if stream.owner_kind != Kind::Workflow as i32 {
        return Err(format!(
            "a link names a Workflow's stream, not one of owner kind {}",
            stream.owner_kind
        ));
    }
    if stream.workflow_id.is_empty() || !stream.activity_id.is_empty() {
        return Err(
            "a link to a Workflow's stream needs the Workflow id and no Activity id".to_string(),
        );
    }
    Ok(())
}

/// The hex text a content hash is stored as under [CONTENT_HASH_KEY].
pub fn content_hash_text(content_hash: &[u8]) -> String {
    content_hash.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_stored_hash_is_lower_case_hex() {
        assert_eq!(content_hash_text(&[0x0a, 0xff]), "0aff");
    }
}
