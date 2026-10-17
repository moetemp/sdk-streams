//! The Redis key layout, shared with every SDK that wrote streams before the store moved into
//! Core.
//!
//! Every key of one run chain shares a Redis Cluster hash tag, the chain itself, so one script
//! can touch all of them:
//!
//! ```text
//! <prefix>:{<namespace>:<workflow id>:<first run id>}:t:<topic>        the log
//! <prefix>:{<namespace>:<workflow id>:<first run id>}:t:<topic>:meta   its meta
//! <prefix>:{<namespace>:<workflow id>:<first run id>}:t:<topic>:floors the readers that hold it
//! <prefix>:{<namespace>:<workflow id>:<first run id>}:chain            the close flag
//! ```
//!
//! A standalone Activity run is a chain of one run. Its tag has the literal `!a` after the
//! namespace, `{<namespace>:!a:<activity id>:<run id>}`, and the same keys follow it. Each part
//! escapes `!`, so no Workflow id makes that tag.
//!
//! A Workflow Activity is a chain of one too. Its tag is
//! `{<namespace>:!wa:<workflow id>:<scheduling run id>:<activity id>}`, so each tool call lands in
//! a slot of its own. It has one more key, the set of log keys its producers wrote, which the
//! scheduling Workflow's Core reads to close every topic when the Activity ends:
//!
//! ```text
//! <prefix>:{<namespace>:!wa:<workflow id>:<run id>:<activity id>}:topics
//! ```
//!
//! The notification log lives apart from the chains, one stream per Cluster slot and a few
//! doorbells, each under the hash tag of a slot (see `slots.rs`). A chain's appends go to the
//! stream of the chain's own slot, so the append script writes both:
//!
//! ```text
//! <prefix>:{<tag of slot s>}:n      slot s's notification entries
//! <prefix>:{<tag of slot d * 16384 / doorbells>}:bell
//!                                    doorbell d, which slots s with s % doorbells == d ring
//! <prefix>:{<tag of slot s>}:a      slot s's topics with records to archive, with an archive
//! ```
//!
//! Each part, the prefix included, is percent-encoded, so a `:` or a brace in an id cannot make
//! two streams share a key.

use super::{conn::slot, slots};
use crate::proto::{ChainId, StreamOwnerKind};
pub(crate) use crate::record::part;

/// The part after the namespace that marks an Activity chain's tag. [part] escapes `!`, so no
/// Workflow id makes it.
const ACTIVITY_MARK: &str = "!a";

/// The part after the namespace that marks a Workflow Activity chain's tag.
const WORKFLOW_ACTIVITY_MARK: &str = "!wa";

/// The keys of one run chain's streams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChainKeys {
    base: String,
}

impl ChainKeys {
    pub(crate) fn new(prefix: &str, chain: &ChainId) -> Self {
        let run = part(&chain.first_run_id);
        let owner = match StreamOwnerKind::try_from(chain.owner_kind) {
            Ok(StreamOwnerKind::Activity) => {
                format!("{ACTIVITY_MARK}:{}:{run}", part(&chain.activity_id))
            }
            Ok(StreamOwnerKind::WorkflowActivity) => format!(
                "{WORKFLOW_ACTIVITY_MARK}:{}:{run}:{}",
                part(&chain.workflow_id),
                part(&chain.activity_id)
            ),
            _ => format!("{}:{run}", part(&chain.workflow_id)),
        };
        Self {
            base: format!("{}:{{{}:{owner}}}", part(prefix), part(&chain.namespace)),
        }
    }

    /// Whether the chain is a Workflow Activity's, which keeps the set of topics it wrote.
    pub(crate) fn keeps_topics(chain: &ChainId) -> bool {
        chain.owner_kind == StreamOwnerKind::WorkflowActivity as i32
    }

    /// The log keys a Workflow Activity chain's producers wrote.
    pub(crate) fn topics(&self) -> String {
        format!("{}:topics", self.base)
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

    /// The positions-only readers that hold a topic's records, so a replay finds them again.
    pub(crate) fn floors(&self, topic: &str) -> String {
        format!("{}:floors", self.log(topic))
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

    /// The Cluster slot every key of the chain lands in.
    pub(crate) fn slot(&self) -> u16 {
        slot(&self.base)
    }
}

/// Slot `slot`'s notification stream.
pub(crate) fn notifications(prefix: &str, slot: u16) -> String {
    format!("{}:{{{}}}:n", part(prefix), slots::tag(slot))
}

/// Slot `slot`'s set of log keys with records to archive, scored by their newest write.
pub(crate) fn archive_pending(prefix: &str, slot: u16) -> String {
    format!("{}:{{{}}}:a", part(prefix), slots::tag(slot))
}

/// Doorbell `doorbell`'s stream, of `doorbells`. The doorbells spread over the slot range, so
/// a cluster's primaries share them.
pub(crate) fn doorbell(prefix: &str, doorbell: u32, doorbells: u32) -> String {
    let slot = u64::from(doorbell) * slots::SLOTS as u64 / u64::from(doorbells.max(1));
    format!("{}:{{{}}}:bell", part(prefix), slots::tag(slot as u16))
}

/// The chain and topic of a log key that a notification entry names, or `None` when the key is
/// not a log key under `prefix`.
pub(crate) fn chain_of_log(prefix: &str, key: &str) -> Option<(ChainId, String)> {
    let rest = key.strip_prefix(&format!("{}:{{", part(prefix)))?;
    let (tag, topic) = rest.split_once("}:t:")?;
    let parts: Vec<&str> = tag.split(':').collect();
    let chain = match parts[..] {
        [namespace, workflow_id, first_run_id] if workflow_id != ACTIVITY_MARK => ChainId {
            namespace: unpart(namespace)?,
            workflow_id: unpart(workflow_id)?,
            first_run_id: unpart(first_run_id)?,
            ..Default::default()
        },
        [namespace, ACTIVITY_MARK, activity_id, run_id] => ChainId {
            namespace: unpart(namespace)?,
            first_run_id: unpart(run_id)?,
            owner_kind: StreamOwnerKind::Activity as i32,
            activity_id: unpart(activity_id)?,
            ..Default::default()
        },
        [
            namespace,
            WORKFLOW_ACTIVITY_MARK,
            workflow_id,
            run_id,
            activity_id,
        ] => ChainId {
            namespace: unpart(namespace)?,
            workflow_id: unpart(workflow_id)?,
            first_run_id: unpart(run_id)?,
            owner_kind: StreamOwnerKind::WorkflowActivity as i32,
            activity_id: unpart(activity_id)?,
        },
        _ => return None,
    };
    Some((chain, unpart(topic)?))
}

/// Undoes [part].
fn unpart(text: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(text.len());
    let mut input = text.bytes();
    while let Some(byte) = input.next() {
        if byte == b'%' {
            let hex = [input.next()?, input.next()?];
            bytes.push(u8::from_str_radix(std::str::from_utf8(&hex).ok()?, 16).ok()?);
        } else {
            bytes.push(byte);
        }
    }
    String::from_utf8(bytes).ok()
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

/// The pattern that matches every key of the Workflow Activities that one Workflow id scheduled,
/// across all its runs.
pub(crate) fn workflow_activity_pattern(
    prefix: &str,
    namespace: &str,
    workflow_id: &str,
) -> String {
    format!(
        "{}:{{{}:{WORKFLOW_ACTIVITY_MARK}:{}:*",
        part(prefix),
        part(namespace),
        part(workflow_id)
    )
}

/// The pattern that matches every key of one standalone Activity id, across all its runs.
pub(crate) fn activity_owner_pattern(prefix: &str, namespace: &str, activity_id: &str) -> String {
    format!(
        "{}:{{{}:{ACTIVITY_MARK}:{}:*",
        part(prefix),
        part(namespace),
        part(activity_id)
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
            ..Default::default()
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

    fn case_chain(case: &Value) -> ChainId {
        if case["owner_kind"] == "workflow_activity" {
            ChainId {
                namespace: text(case, "namespace").to_string(),
                workflow_id: text(case, "workflow_id").to_string(),
                first_run_id: text(case, "first_run_id").to_string(),
                owner_kind: StreamOwnerKind::WorkflowActivity as i32,
                activity_id: text(case, "activity_id").to_string(),
            }
        } else if case["owner_kind"] == "activity" {
            ChainId {
                namespace: text(case, "namespace").to_string(),
                first_run_id: text(case, "first_run_id").to_string(),
                owner_kind: StreamOwnerKind::Activity as i32,
                activity_id: text(case, "activity_id").to_string(),
                ..Default::default()
            }
        } else {
            ChainId {
                namespace: text(case, "namespace").to_string(),
                workflow_id: text(case, "workflow_id").to_string(),
                first_run_id: text(case, "first_run_id").to_string(),
                ..Default::default()
            }
        }
    }

    #[test]
    fn keys_match_the_python_sdk() {
        // Streams written by an SDK before the store moved into Core must stay readable.
        let cases: Vec<Value> =
            serde_json::from_str(include_str!("../../testdata/redis/keys.json")).unwrap();
        assert!(cases.len() >= 4);
        assert!(cases.iter().any(|case| case["owner_kind"] == "activity"));
        assert!(
            cases
                .iter()
                .any(|case| case["owner_kind"] == "workflow_activity")
        );
        for case in &cases {
            let chain = case_chain(case);
            let keys = ChainKeys::new(text(case, "prefix"), &chain);
            let topic = text(case, "topic");
            assert_eq!(keys.log(topic), text(case, "log"));
            assert_eq!(keys.meta(topic), text(case, "meta"));
            assert_eq!(keys.chain(), text(case, "chain"));
            assert_eq!(
                ChainKeys::keeps_topics(&chain),
                case.get("topics").is_some()
            );
            if let Some(topics) = case.get("topics") {
                assert_eq!(keys.topics(), topics.as_str().unwrap());
            }
        }
    }

    #[test]
    fn a_chains_notification_stream_shares_its_slot() {
        let cases: Vec<Value> =
            serde_json::from_str(include_str!("../../testdata/redis/keys.json")).unwrap();
        for case in &cases {
            let chain = case_chain(case);
            let prefix = text(case, "prefix");
            let keys = ChainKeys::new(prefix, &chain);
            let topic = text(case, "topic");
            let notifications = notifications(prefix, keys.slot());
            assert_eq!(slot(&notifications), slot(&keys.log(topic)));
            assert_eq!(slot(&notifications), slot(&keys.meta(topic)));
            assert_eq!(slot(&notifications), slot(&keys.topics()));
            assert_eq!(
                chain_of_log(prefix, &keys.log(topic)),
                Some((chain, topic.to_string()))
            );
        }
        assert_eq!(chain_of_log("p", "q:{ns:wf:run}:t:out"), None);
        assert_eq!(chain_of_log("p", "p:{ns:wf}:t:out"), None);
        assert_eq!(chain_of_log("p", "p:{ns:!b:act:run}:t:out"), None);
        assert_eq!(chain_of_log("p", "p:{ns:!a:act}:t:out"), None);
        assert_eq!(chain_of_log("p", "p:{ns:!wa:wf:run}:t:out"), None);
        assert_eq!(chain_of_log("p", "p:{ns:!a:wf:run:act}:t:out"), None);
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
            ..Default::default()
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

    #[test]
    fn an_activity_chain_keeps_apart_from_every_workflow() {
        let activity = |id: &str| ChainId {
            namespace: "ns".to_string(),
            first_run_id: "run-1".to_string(),
            owner_kind: StreamOwnerKind::Activity as i32,
            activity_id: id.to_string(),
            ..Default::default()
        };
        let workflow = |id: &str| ChainId {
            namespace: "ns".to_string(),
            workflow_id: id.to_string(),
            first_run_id: "run-1".to_string(),
            ..Default::default()
        };
        let matcher =
            |pattern: String| move |key: &str| key.starts_with(pattern.trim_end_matches('*'));
        for id in ["wf", "!a", "a", "!a:wf"] {
            let keys = ChainKeys::new("p", &activity(id));
            for workflow_id in [id, "!a", "a", "%21a"] {
                assert!(
                    !matcher(owner_pattern("p", "ns", workflow_id))(&keys.chain()),
                    "{id}"
                );
                assert_ne!(keys, ChainKeys::new("p", &workflow(workflow_id)), "{id}");
            }
            let own = matcher(activity_owner_pattern("p", "ns", id));
            assert!(own(&keys.chain()) && own(&keys.log("out")), "{id}");
            assert!(!own(&ChainKeys::new("p", &workflow(id)).chain()), "{id}");
            // The `:` after the id keeps `wf` from matching the runs of `wf2`.
            assert!(
                !own(&ChainKeys::new("p", &activity(&format!("{id}2"))).chain()),
                "{id}"
            );
        }
        assert_eq!(activity_owner_pattern("p", "ns", "a*b"), "p:{ns:!a:a%2Ab:*");
    }

    #[test]
    fn a_workflow_activity_chain_keeps_apart_from_its_workflow_and_other_runs() {
        let wfa = |workflow_id: &str, run_id: &str, activity_id: &str| ChainId {
            namespace: "ns".to_string(),
            workflow_id: workflow_id.to_string(),
            first_run_id: run_id.to_string(),
            owner_kind: StreamOwnerKind::WorkflowActivity as i32,
            activity_id: activity_id.to_string(),
        };
        let starts = |pattern: String, key: String| key.starts_with(pattern.trim_end_matches('*'));
        for id in ["wf", "!wa", "wa", "!wa:wf", "!a"] {
            let keys = ChainKeys::new("p", &wfa(id, "run-1", "1"));
            for workflow_id in [id, "!wa", "wa", "%21wa", "!a"] {
                assert!(
                    !starts(owner_pattern("p", "ns", workflow_id), keys.chain()),
                    "{id}"
                );
                assert!(!starts(
                    activity_owner_pattern("p", "ns", workflow_id),
                    keys.chain()
                ));
            }
            assert!(starts(
                workflow_activity_pattern("p", "ns", id),
                keys.topics()
            ));
            assert!(starts(
                workflow_activity_pattern("p", "ns", id),
                keys.log("out")
            ));
            // The `:` after the id keeps `wf` from matching the Activities of `wf2`.
            let longer = ChainKeys::new("p", &wfa(&format!("{id}2"), "run-1", "1"));
            assert!(!starts(
                workflow_activity_pattern("p", "ns", id),
                longer.chain()
            ));
            // Activity `1` of the next run is another chain.
            assert_ne!(keys, ChainKeys::new("p", &wfa(id, "run-2", "1")));
        }
        assert_eq!(
            workflow_activity_pattern("p", "ns", "a*b"),
            "p:{ns:!wa:a%2Ab:*"
        );
    }

    #[test]
    fn the_doorbells_spread_over_the_slot_range() {
        let slots: Vec<u16> = (0..16)
            .map(|d| super::super::conn::slot(&doorbell("p", d, 16)))
            .collect();
        assert_eq!(slots, (0..16).map(|d| d * 1024).collect::<Vec<u16>>());
    }
}
