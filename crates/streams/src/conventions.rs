//! What every SDK and store must agree on for stream records: their stored metadata keys and
//! the topics a store keys.

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
