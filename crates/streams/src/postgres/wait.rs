//! Waiting reads. A waiting read holds no connection. It waits for a wake from an append or a
//! close through this store, from one batched poll per process for every stream with a waiter,
//! or from a `NOTIFY` with `notify_reads`. A wake only makes the read look again, so a lost or a
//! spurious wake costs latency and never a record.

use super::Chain;
use std::{collections::HashMap, sync::Mutex};
use tokio::sync::watch;

/// A stream: its chain and topic.
pub(super) type StreamKey = (Chain, String);

/// What a waiter saw of its stream: the newest position and whether it is closed.
pub(super) type Seen = (Option<i64>, bool);

#[derive(Debug, Default)]
pub(super) struct Waiters {
    streams: Mutex<HashMap<StreamKey, Waiting>>,
}

#[derive(Debug)]
struct Waiting {
    wake: watch::Sender<u64>,
    /// The state the waiters were woken for last, which the poll compares with.
    seen: Seen,
    /// The topic's row, which a `NOTIFY` names. `None` before the topic has one.
    topic_id: Option<i64>,
}

impl Waiters {
    /// Registers a waiter that saw `seen`. Register before the read looks once more, so a write
    /// between the two wakes it.
    pub(super) fn register(
        &self,
        key: StreamKey,
        seen: Seen,
        topic_id: Option<i64>,
    ) -> watch::Receiver<u64> {
        let mut streams = self.streams.lock().unwrap();
        let waiting = streams.entry(key).or_insert_with(|| Waiting {
            wake: watch::Sender::new(0),
            seen,
            topic_id,
        });
        waiting.topic_id = waiting.topic_id.or(topic_id);
        waiting.wake.subscribe()
    }

    /// Wakes the waiters of one stream.
    pub(super) fn wake(&self, key: &StreamKey) {
        if let Some(waiting) = self.streams.lock().unwrap().get(key) {
            waiting.wake.send_modify(|n| *n += 1);
        }
    }

    /// Wakes the waiters of every topic of a chain.
    pub(super) fn wake_chain(&self, chain: &Chain) {
        for ((of, _), waiting) in self.streams.lock().unwrap().iter() {
            if of == chain {
                waiting.wake.send_modify(|n| *n += 1);
            }
        }
    }

    /// Wakes the waiters of the topic a `NOTIFY` named. Waiters of a topic with no row yet are
    /// left to the poll, since one `NOTIFY` per append would wake them all.
    pub(super) fn wake_topic(&self, topic_id: i64) {
        for waiting in self.streams.lock().unwrap().values() {
            if waiting.topic_id == Some(topic_id) {
                waiting.wake.send_modify(|n| *n += 1);
            }
        }
    }

    /// Notes the row a read found for a stream whose waiters had none.
    pub(super) fn learn(&self, key: &StreamKey, topic_id: Option<i64>) {
        if let Some(waiting) = self.streams.lock().unwrap().get_mut(key) {
            waiting.topic_id = waiting.topic_id.or(topic_id);
        }
    }

    /// Wakes every waiter, for when wakes may have been lost.
    pub(super) fn wake_all(&self) {
        for waiting in self.streams.lock().unwrap().values() {
            waiting.wake.send_modify(|n| *n += 1);
        }
    }

    /// The streams with a waiter. Streams whose waiters all went are dropped here.
    pub(super) fn streams(&self) -> Vec<StreamKey> {
        let mut streams = self.streams.lock().unwrap();
        streams.retain(|_, waiting| waiting.wake.receiver_count() > 0);
        streams.keys().cloned().collect()
    }

    /// Wakes the waiters of `key` when the poll found it other than they saw.
    pub(super) fn polled(&self, key: &StreamKey, now: Seen) {
        if let Some(waiting) = self.streams.lock().unwrap().get_mut(key)
            && waiting.seen != now
        {
            waiting.seen = now;
            waiting.wake.send_modify(|n| *n += 1);
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.streams
            .lock()
            .unwrap()
            .values()
            .map(|waiting| waiting.wake.receiver_count())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(topic: &str) -> StreamKey {
        (
            Chain {
                namespace: "ns".to_string(),
                kind: 1,
                owner: "wf".to_string(),
                run: "r".to_string(),
                activity: String::new(),
            },
            topic.to_string(),
        )
    }

    #[test]
    fn a_notify_wakes_only_the_topic_it_names() {
        let waiters = Waiters::default();
        let with_row = waiters.register(key("a"), (Some(1), false), Some(7));
        let no_row = waiters.register(key("b"), (None, false), None);
        waiters.wake_topic(9);
        assert!(!with_row.has_changed().unwrap());
        assert!(!no_row.has_changed().unwrap());
        waiters.wake_topic(7);
        assert!(with_row.has_changed().unwrap());
        assert!(!no_row.has_changed().unwrap());
        waiters.learn(&key("b"), Some(8));
        waiters.wake_topic(8);
        assert!(no_row.has_changed().unwrap());
    }

    #[test]
    fn a_poll_wakes_only_on_a_change() {
        let waiters = Waiters::default();
        let mut rx = waiters.register(key("out"), (Some(1), false), Some(7));
        waiters.polled(&key("out"), (Some(1), false));
        assert!(!rx.has_changed().unwrap());
        waiters.polled(&key("out"), (Some(2), false));
        assert!(rx.has_changed().unwrap());
        rx.mark_unchanged();
        waiters.polled(&key("out"), (Some(2), true));
        assert!(rx.has_changed().unwrap());
    }

    #[test]
    fn a_chain_wake_reaches_its_topics_only() {
        let waiters = Waiters::default();
        let out = waiters.register(key("out"), (None, false), None);
        let other = waiters.register(key("other"), (None, false), None);
        let mut elsewhere = key("out");
        elsewhere.0.owner = "wf2".to_string();
        let far = waiters.register(elsewhere, (None, false), None);
        waiters.wake_chain(&key("out").0);
        assert!(out.has_changed().unwrap());
        assert!(other.has_changed().unwrap());
        assert!(!far.has_changed().unwrap());
    }

    #[test]
    fn a_dropped_waiter_leaves_no_stream() {
        let waiters = Waiters::default();
        let rx = waiters.register(key("out"), (None, false), None);
        assert_eq!(waiters.streams().len(), 1);
        assert_eq!(waiters.len(), 1);
        drop(rx);
        assert!(waiters.streams().is_empty());
        assert_eq!(waiters.len(), 0);
    }
}
