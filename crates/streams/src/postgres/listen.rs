//! The listener: one connection per store that hears the notification log's doorbells and, with
//! `notify_reads`, the appends that wake waiting reads. `LISTEN` keeps state on the server
//! session, so this connection never goes through a pooler in transaction mode.

use super::{Inner, PARTITIONS, conn::Target, errors::unreachable};
use crate::StreamResult;
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::sync::{Notify, mpsc};
use tokio_postgres::{Client, Notification};

const FIRST_BACKOFF: Duration = Duration::from_millis(100);
const LAST_BACKOFF: Duration = Duration::from_secs(5);

/// The partitions each doorbell rang that no driver took yet.
#[derive(Debug)]
pub(super) struct Rings {
    pending: Mutex<Vec<BTreeSet<u32>>>,
    bells: Vec<Notify>,
}

impl Rings {
    pub(super) fn new(doorbells: u32) -> Self {
        Self {
            pending: Mutex::new(vec![BTreeSet::new(); doorbells as usize]),
            bells: (0..doorbells).map(|_| Notify::new()).collect(),
        }
    }

    fn ring(&self, partition: u32) {
        let doorbell = partition as usize % self.bells.len();
        self.pending.lock().unwrap()[doorbell].insert(partition);
        self.bells[doorbell].notify_waiters();
    }

    /// Rings every partition, for when rings may have been lost.
    fn ring_all(&self) {
        for partition in 0..PARTITIONS {
            self.ring(partition);
        }
    }

    fn take(&self, doorbell: usize) -> Vec<u32> {
        std::mem::take(&mut self.pending.lock().unwrap()[doorbell])
            .into_iter()
            .collect()
    }

    /// Waits up to `block` for doorbell `doorbell`, and answers the partitions it rang.
    pub(super) async fn wait(&self, doorbell: u32, block: Duration) -> Vec<u32> {
        let Some(bell) = self.bells.get(doorbell as usize) else {
            tokio::time::sleep(block).await;
            return vec![];
        };
        let notified = bell.notified();
        tokio::pin!(notified);
        // Enabled before the look, so a ring between the two still wakes the wait.
        notified.as_mut().enable();
        let rung = self.take(doorbell as usize);
        if !rung.is_empty() {
            return rung;
        }
        let _ = tokio::time::timeout(block, notified).await;
        self.take(doorbell as usize)
    }
}

/// What the listener listens on and whom it wakes.
pub(super) struct Listener {
    pub(super) target: Target,
    pub(super) schema: String,
    /// The `LISTEN` statements, one per channel.
    pub(super) listens: String,
    pub(super) rings: Option<Arc<Rings>>,
}

impl Listener {
    pub(super) fn new(
        target: Target,
        schema: &str,
        rings: Option<Arc<Rings>>,
        notify_reads: bool,
    ) -> Self {
        let mut channels: Vec<String> = vec![];
        if let Some(rings) = &rings {
            channels.extend((0..rings.bells.len()).map(|d| format!("{schema}_bell_{d}")));
        }
        if notify_reads {
            channels.extend((0..64).map(|n| format!("{schema}_read_{n}")));
        }
        let listens = channels
            .iter()
            .map(|channel| format!("LISTEN {channel}"))
            .collect::<Vec<_>>()
            .join("; ");
        Self {
            target,
            schema: schema.to_string(),
            listens,
            rings,
        }
    }

    /// Connects and listens. The store's connect calls this once, so a listener that can't
    /// connect fails the store at once.
    pub(super) async fn connect(
        &self,
    ) -> StreamResult<(Client, mpsc::UnboundedReceiver<Notification>)> {
        let (client, notifications) = self.target.listen().await.map_err(unreachable)?;
        client
            .batch_execute(&self.listens)
            .await
            .map_err(unreachable)?;
        Ok((client, notifications))
    }

    fn deliver(&self, inner: &Weak<Inner>, notification: &Notification) {
        let channel = notification.channel();
        let Some(rest) = channel
            .strip_prefix(self.schema.as_str())
            .and_then(|rest| rest.strip_prefix('_'))
        else {
            return;
        };
        if rest.starts_with("bell_") {
            if let (Some(rings), Ok(partition)) = (&self.rings, notification.payload().parse()) {
                rings.ring(partition);
            }
        } else if rest.starts_with("read_")
            && let (Some(inner), Ok(topic_id)) = (inner.upgrade(), notification.payload().parse())
        {
            inner.waiters.wake_topic(topic_id);
        }
    }

    /// Hands on what the connection hears, and connects again with backoff when it ends. After a
    /// reconnect it rings every partition and wakes every waiter, since what was sent while it
    /// was away is gone.
    pub(super) async fn run(
        self,
        inner: Weak<Inner>,
        mut client: Client,
        mut notifications: mpsc::UnboundedReceiver<Notification>,
    ) {
        loop {
            while let Some(notification) = notifications.recv().await {
                self.deliver(&inner, &notification);
            }
            drop(client);
            let mut backoff = FIRST_BACKOFF;
            loop {
                tokio::time::sleep(backoff).await;
                if inner.strong_count() == 0 {
                    return;
                }
                match self.connect().await {
                    Ok(connected) => {
                        (client, notifications) = connected;
                        break;
                    }
                    Err(error) => {
                        tracing::debug!("The PostgreSQL store's listener can't reconnect: {error}");
                        backoff = (backoff * 2).min(LAST_BACKOFF);
                    }
                }
            }
            if let Some(rings) = &self.rings {
                rings.ring_all();
            }
            if let Some(inner) = inner.upgrade() {
                inner.waiters.wake_all();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_ring_waits_for_its_doorbell_and_is_taken_once() {
        let rings = Rings::new(4);
        rings.ring(5);
        rings.ring(9);
        rings.ring(2);
        assert_eq!(rings.wait(1, Duration::ZERO).await, [5, 9]);
        assert!(rings.wait(1, Duration::from_millis(10)).await.is_empty());
        assert_eq!(rings.wait(2, Duration::ZERO).await, [2]);
    }

    #[tokio::test]
    async fn a_ring_wakes_a_waiting_doorbell() {
        let rings = Arc::new(Rings::new(16));
        let waiting = {
            let rings = rings.clone();
            tokio::spawn(async move { rings.wait(3, Duration::from_secs(10)).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        rings.ring(19);
        assert_eq!(waiting.await.unwrap(), [19]);
    }

    #[test]
    fn ring_all_reaches_every_partition_once() {
        let rings = Rings::new(16);
        rings.ring_all();
        let total: usize = (0..16).map(|d| rings.take(d).len()).sum();
        assert_eq!(total, PARTITIONS as usize);
    }
}
