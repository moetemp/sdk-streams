//! The Redis key layout, shared with every SDK that wrote streams before the store moved into
//! Core.
//!
//! Every key of one run chain shares a Redis Cluster hash tag, the chain itself, so one script
//! can touch all of them:
//!
//! ```text
//! <prefix>:{<namespace>:<workflow id>:<first run id>}:t:<topic>        the log
//! <prefix>:{<namespace>:<workflow id>:<first run id>}:t:<topic>:meta   its meta
//! <prefix>:{<namespace>:<workflow id>:<first run id>}:chain            the close flag
//! ```
//!
//! Each part, the prefix included, is percent-encoded, so a `:` or a brace in an id cannot make
//! two streams share a key.

use crate::proto::ChainId;

/// Percent-encodes every byte but the unreserved ones, as Python's `quote(text, safe="")` does.
pub(crate) fn part(text: &str) -> String {
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

/// Reverses [part], or `None` for text that [part] never makes.
pub(crate) fn unpart(text: &str) -> Option<String> {
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

/// The keys of one run chain's streams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChainKeys {
    base: String,
}

impl ChainKeys {
    pub(crate) fn new(prefix: &str, chain: &ChainId) -> Self {
        Self {
            base: format!(
                "{}:{{{}:{}:{}}}",
                part(prefix),
                part(&chain.namespace),
                part(&chain.workflow_id),
                part(&chain.first_run_id)
            ),
        }
    }

    pub(crate) fn log(&self, topic: &str) -> String {
        format!("{}:t:{}", self.base, part(topic))
    }

    pub(crate) fn meta(&self, topic: &str) -> String {
        format!("{}:meta", self.log(topic))
    }

    pub(crate) fn chain(&self) -> String {
        format!("{}:chain", self.base)
    }

    /// The pattern that matches every topic key of the chain, logs and metas.
    pub(crate) fn topics_pattern(&self) -> String {
        format!("{}:t:*", self.base)
    }

    /// The topic whose log `key` is, or `None` for a meta or another key.
    pub(crate) fn topic_of(&self, key: &str) -> Option<String> {
        let part = key.strip_prefix(&self.base)?.strip_prefix(":t:")?;
        // A topic's own `:` is encoded, so a raw one only starts the meta's suffix.
        if part.contains(':') {
            return None;
        }
        unpart(part)
    }
}

/// The meta field that holds one producer attempt's newest batch.
///
/// Length-prefixed, so an id that holds `:` cannot name another attempt. The length counts UTF-8
/// bytes, so every language counts the same.
pub(crate) fn session_field(producer_id: &str, attempt: i64) -> String {
    format!("hw:{}:{producer_id}:{attempt}", producer_id.len())
}

/// The pattern that matches every key of one Workflow id, across all its chains.
pub(crate) fn owner_pattern(prefix: &str, namespace: &str, workflow_id: &str) -> String {
    format!(
        "{}:{{{}:{}:*",
        part(prefix),
        part(namespace),
        part(workflow_id)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn a_topic_reads_back_from_its_log_key_only() {
        let chain = ChainId {
            namespace: "ns".to_string(),
            workflow_id: "w:f".to_string(),
            first_run_id: "run".to_string(),
        };
        let keys = ChainKeys::new("p", &chain);
        for topic in ["out", "a:b", "é {x}%"] {
            assert_eq!(keys.topic_of(&keys.log(topic)).as_deref(), Some(topic));
            assert_eq!(keys.topic_of(&keys.meta(topic)), None);
        }
        assert_eq!(keys.topic_of(&keys.chain()), None);
        assert_eq!(unpart("%zz"), None);
    }

    fn text<'a>(case: &'a Value, field: &str) -> &'a str {
        case[field].as_str().unwrap()
    }

    #[test]
    fn keys_match_the_python_sdk() {
        // Streams written by an SDK before the store moved into Core must stay readable.
        let cases: Vec<Value> =
            serde_json::from_str(include_str!("../../testdata/redis/keys.json")).unwrap();
        assert!(cases.len() >= 4);
        for case in &cases {
            let keys = ChainKeys::new(
                text(case, "prefix"),
                &ChainId {
                    namespace: text(case, "namespace").to_string(),
                    workflow_id: text(case, "workflow_id").to_string(),
                    first_run_id: text(case, "first_run_id").to_string(),
                },
            );
            let topic = text(case, "topic");
            assert_eq!(keys.log(topic), text(case, "log"));
            assert_eq!(keys.meta(topic), text(case, "meta"));
            assert_eq!(keys.chain(), text(case, "chain"));
        }
    }

    #[test]
    fn session_fields_match_the_python_sdk() {
        let cases: Vec<Value> =
            serde_json::from_str(include_str!("../../testdata/redis/session_fields.json")).unwrap();
        for case in &cases {
            assert_eq!(
                session_field(text(case, "producer_id"), case["attempt"].as_i64().unwrap()),
                text(case, "field")
            );
        }
    }

    #[test]
    fn an_owner_pattern_covers_every_chain_of_one_workflow_only() {
        let chain = ChainId {
            namespace: "ns".to_string(),
            workflow_id: "wf".to_string(),
            first_run_id: "run-1".to_string(),
        };
        let pattern = owner_pattern("p", "ns", "wf");
        assert_eq!(pattern, "p:{ns:wf:*");
        let matches = |key: &str| key.starts_with(pattern.trim_end_matches('*'));
        assert!(matches(&ChainKeys::new("p", &chain).chain()));
        // The `:` after the id keeps `wf` from matching the chains of `wf2`.
        let other = ChainId {
            workflow_id: "wf2".to_string(),
            ..chain
        };
        assert!(!matches(&ChainKeys::new("p", &other).chain()));
        // Each part is encoded as the keys encode it, so a glob character matches only itself.
        assert_eq!(owner_pattern("p", "ns", "a*b"), "p:{ns:a%2Ab:*");
    }
}
