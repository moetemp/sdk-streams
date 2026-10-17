//! The stream driver: reads a store's notification log and tells the server's stream notifiers.
//!
//! A driver blocks on each doorbell, drains each slot a doorbell names, and sweeps every slot
//! that holds entries now and then, since a ring can be lost. It folds a drain's entries to the
//! newest counter of each stream, sends them in one call per namespace, and acks them only once
//! the call landed. A failed call leaves them pending, so this driver or another one sends them
//! again. Delivery is at least once, and a notifier ignores a counter it has seen. A call the
//! server refuses goes again one stream at a time, so one bad entry holds back no other stream.
//!
//! With an archive, a driver also copies the topics that the store marked into the archive, at
//! each sweep. It takes a topic's tail once the topic is closed or idle, and at least once per
//! idle time while it keeps taking writes.

use crate::{
    NotificationEntry, NotificationLog, NotifierClient, NotifyError, SegmentLimits, StreamArchive,
    StreamError, StreamNotification, StreamResult, StreamStore, archive_topic, chain_owner,
    mint_cursor, progress_counter, proto::ChainId, stream_hash,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::task::JoinSet;

/// How a driver reads the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamDriverOptions {
    /// The driver's name in the log's consumer group. Each driver needs its own, and the default
    /// is the host name and the process id.
    pub consumer: String,
    /// How often the driver looks at every slot that holds entries.
    pub sweep_interval: Duration,
    /// How long another driver's unacked entries wait before this one takes them.
    pub claim_idle: Duration,
    /// How long one doorbell read blocks.
    pub doorbell_wait: Duration,
    /// The most entries one take reads, and so the most streams one notify call names.
    pub batch: usize,
}

impl Default for StreamDriverOptions {
    fn default() -> Self {
        Self {
            consumer: format!(
                "{}-{}",
                gethostname::gethostname().to_string_lossy(),
                std::process::id()
            ),
            sweep_interval: Duration::from_secs(30),
            claim_idle: Duration::from_secs(60),
            doorbell_wait: Duration::from_secs(5),
            batch: 100,
        }
    }
}

/// Waits before a slot whose notify call failed is drained again.
const RETRY_WAIT: Duration = Duration::from_secs(1);

/// The most marked topics one sweep reads per partition.
const ARCHIVE_MARKS: usize = 100;

/// What a driver archives into, and from where.
#[derive(Clone)]
pub struct ArchiveSink {
    /// The hot store itself, not a [crate::TieredStore] over it, since a tiered read could
    /// answer from the archive.
    pub hot: Arc<dyn StreamStore>,
    /// The archive.
    pub archive: Arc<dyn StreamArchive>,
    /// When a segment ends.
    pub limits: SegmentLimits,
    /// How long a topic goes without a write before the archive takes its tail. A topic that
    /// keeps taking writes has its tail taken this often too.
    pub segment_idle: Duration,
}

/// How many times one entry's call may fail while the server takes other streams' calls before
/// the entry is dropped.
const MAX_ENTRY_FAILURES: u32 = 10;

/// How many entries' failures a driver counts at most.
const MAX_TRACKED_FAILURES: usize = 10_000;

/// What one drain did with its entries.
#[derive(Default)]
struct Sent {
    /// The entries to ack, since they landed or never will.
    acked: Vec<String>,
    /// Some entries are left for the next drain.
    left_pending: bool,
    /// A namespace answered that the server takes no notifications.
    unsupported: bool,
}

impl Sent {
    fn ack(&mut self, ids: impl IntoIterator<Item = String>) {
        self.acked.extend(ids);
    }
}

/// A driver over one store's notification log.
pub struct StreamDriver {
    store: Arc<dyn StreamStore>,
    log: Arc<dyn NotificationLog>,
    client: Arc<dyn NotifierClient>,
    options: StreamDriverOptions,
    /// The slots a drain runs on now, so the doorbells and the sweep don't drain one twice. Set
    /// when another task wanted to drain the slot meanwhile, so the drain looks once more.
    draining: Mutex<HashMap<u32, bool>>,
    archive: Option<ArchiveSink>,
    /// Whether any call landed, so a server with no notifiers at all is told from one namespace
    /// without them.
    delivered: AtomicBool,
    /// How often each entry's call failed while the server took other calls.
    failures: Mutex<HashMap<String, u32>>,
}

impl StreamDriver {
    /// A driver for `store`, which must keep a notification log, sending through `client`.
    pub fn new(
        store: Arc<dyn StreamStore>,
        client: Arc<dyn NotifierClient>,
        options: StreamDriverOptions,
    ) -> StreamResult<Self> {
        let log = store.notification_log().ok_or_else(|| {
            StreamError::refused(format!(
                "the {} store keeps no notification log, so it needs no driver",
                store.name()
            ))
        })?;
        if options.consumer.is_empty() {
            return Err(StreamError::invalid_argument(
                "a driver needs a consumer name",
            ));
        }
        Ok(Self {
            store,
            log,
            client,
            options,
            draining: Mutex::default(),
            archive: None,
            delivered: AtomicBool::new(false),
            failures: Mutex::default(),
        })
    }

    /// This driver, archiving the topics the store marks into `sink`.
    pub fn with_archive(mut self, sink: ArchiveSink) -> Self {
        self.archive = Some(sink);
        self
    }

    /// Runs until the returned future is dropped. It only ends early when the server can't take
    /// notifications at all: every namespace it sent for answered so, and no call ever landed.
    pub async fn run(self: Arc<Self>) -> StreamResult<()> {
        let mut tasks = JoinSet::new();
        for doorbell in 0..self.log.doorbells() {
            let driver = self.clone();
            tasks.spawn(async move { driver.listen(doorbell).await });
        }
        let driver = self.clone();
        tasks.spawn(async move { driver.sweep().await });
        if let Some(sink) = self.archive.clone() {
            let driver = self.clone();
            tasks.spawn(async move { driver.archive(sink).await });
        }
        // Dropping the set aborts every task, so a caller stops the driver by dropping this.
        match tasks.join_next().await {
            Some(Ok(result)) => result,
            Some(Err(error)) => Err(StreamError::storage(format!(
                "a stream driver task stopped: {error}"
            ))),
            None => Ok(()),
        }
    }

    async fn listen(&self, doorbell: u32) -> StreamResult<()> {
        loop {
            let slots = match self
                .log
                .wait_doorbell(doorbell, &self.options.consumer, self.options.doorbell_wait)
                .await
            {
                Ok(slots) => slots,
                Err(error) => {
                    tracing::warn!(doorbell, %error, "A stream driver could not read a doorbell");
                    tokio::time::sleep(RETRY_WAIT).await;
                    continue;
                }
            };
            for slot in slots {
                self.drain(slot).await?;
            }
        }
    }

    async fn sweep(&self) -> StreamResult<()> {
        loop {
            match self.log.busy().await {
                Ok(slots) => {
                    for slot in slots {
                        self.drain(slot).await?;
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "A stream driver could not sweep the notification log");
                }
            }
            tokio::time::sleep(self.options.sweep_interval).await;
        }
    }

    /// Archives the marked topics at each sweep. A failure on one topic is logged, and the next
    /// sweep tries it again.
    async fn archive(&self, sink: ArchiveSink) -> StreamResult<()> {
        let mut taken: HashMap<(ChainKey, String), Instant> = HashMap::new();
        loop {
            match self.log.archive_marks(ARCHIVE_MARKS).await {
                Ok(marks) => {
                    let idle_ms = sink.segment_idle.as_millis() as u64;
                    let mut seen = HashMap::with_capacity(marks.len());
                    for mark in marks {
                        let key = (ChainKey::of(&mark.chain), mark.topic.clone());
                        let last = taken.get(&key).copied();
                        let due = mark.written_ms == 0
                            || mark.idle_ms >= idle_ms
                            || last.is_none_or(|last| last.elapsed() >= sink.segment_idle);
                        if !due {
                            seen.insert(key, last.unwrap_or_else(Instant::now));
                            continue;
                        }
                        let archived = archive_topic(
                            &*sink.hot,
                            &*sink.archive,
                            &mark.chain,
                            &mark.topic,
                            sink.limits,
                            true,
                        )
                        .await;
                        match archived {
                            Ok(_) => {
                                seen.insert(key, Instant::now());
                            }
                            Err(error) => {
                                tracing::warn!(
                                    workflow_id = %mark.chain.workflow_id,
                                    activity_id = %mark.chain.activity_id,
                                    topic = %mark.topic,
                                    %error,
                                    "A stream driver could not archive a topic"
                                );
                                if let Some(last) = last {
                                    seen.insert(key, last);
                                }
                            }
                        }
                    }
                    // Only marked topics stay, so the map stays as small as the marks.
                    taken = seen;
                }
                Err(error) => {
                    tracing::warn!(%error, "A stream driver could not read the topics to archive");
                }
            }
            tokio::time::sleep(self.options.sweep_interval).await;
        }
    }

    /// Sends everything `slot` holds, one batch at a time, unless another task drains it now.
    /// Fails only when the server can't take notifications at all.
    async fn drain(&self, slot: u32) -> StreamResult<()> {
        {
            let mut draining = self.draining.lock().unwrap();
            if let Some(again) = draining.get_mut(&slot) {
                *again = true;
                return Ok(());
            }
            draining.insert(slot, false);
        }
        loop {
            let drained = self.drain_slot(slot).await;
            let mut draining = self.draining.lock().unwrap();
            if drained.is_ok() && draining.get(&slot) == Some(&true) {
                draining.insert(slot, false);
                continue;
            }
            draining.remove(&slot);
            return drained;
        }
    }

    async fn drain_slot(&self, slot: u32) -> StreamResult<()> {
        loop {
            let entries = match self
                .log
                .take(
                    slot,
                    &self.options.consumer,
                    self.options.claim_idle,
                    self.options.batch,
                )
                .await
            {
                Ok(entries) if entries.is_empty() => return Ok(()),
                Ok(entries) => entries,
                Err(error) => {
                    tracing::warn!(slot, %error, "A stream driver could not read a slot");
                    return Ok(());
                }
            };
            // A server call names one namespace, and one namespace's refusal must not hold back
            // the others in the slot.
            let mut by_namespace: BTreeMap<String, Vec<NotificationEntry>> = BTreeMap::new();
            for entry in entries {
                by_namespace
                    .entry(entry.chain.namespace.clone())
                    .or_default()
                    .push(entry);
            }
            let mut sent = Sent::default();
            for (namespace, entries) in by_namespace {
                self.send_namespace(slot, &namespace, entries, &mut sent)
                    .await;
            }
            if !sent.acked.is_empty() {
                if let Err(error) = self.log.ack(slot, &sent.acked).await {
                    // The entries come again, and the notifier drops the repeat.
                    tracing::warn!(slot, %error, "A stream driver could not ack its entries");
                    return Ok(());
                }
                let mut failures = self.failures.lock().unwrap();
                for id in &sent.acked {
                    failures.remove(id);
                }
            }
            if sent.unsupported && !self.delivered.load(Ordering::Relaxed) {
                return Err(StreamError::unsupported(
                    "the server takes no stream notifications",
                ));
            }
            if sent.left_pending {
                // Left pending, so the next drain sends them again.
                tokio::time::sleep(RETRY_WAIT).await;
                return Ok(());
            }
        }
    }

    /// Sends one namespace's entries in one call. The server answers each stream, and a stream it
    /// refused or failed is settled on its own. When the server refuses the whole call, each
    /// stream goes on its own, so one bad entry holds back no other stream.
    async fn send_namespace(
        &self,
        slot: u32,
        namespace: &str,
        entries: Vec<NotificationEntry>,
        sent: &mut Sent,
    ) {
        let (streams, unsendable): (Vec<_>, Vec<_>) = self
            .fold(entries)
            .into_iter()
            .partition(|(notification, _)| notification.counter > 0);
        sent.ack(unsendable.into_iter().flat_map(|(_, ids)| ids));
        if streams.is_empty() {
            return;
        }
        let notifications = streams.iter().map(|(n, _)| n.clone()).collect();
        match self.client.notify_streams_each(notifications).await {
            Ok(results) => {
                // A server that took some entries is up, so the ones it failed count toward
                // their drop. A server that failed them all may be busy, so they wait.
                let reached = results.iter().any(Result::is_ok);
                if reached {
                    self.delivered.store(true, Ordering::Relaxed);
                }
                for (stream, result) in streams.into_iter().zip(results) {
                    match result {
                        Ok(()) => sent.ack(stream.1),
                        Err(error) => self.settle_one(slot, error, vec![stream], sent, reached),
                    }
                }
            }
            Err(NotifyError::Unsupported(error)) => {
                // A server with no notifiers answers this for every namespace. Only one namespace
                // answering it, the others still get theirs.
                tracing::warn!(
                    slot,
                    namespace,
                    error,
                    "A namespace takes no stream notifications"
                );
                sent.unsupported = true;
                sent.ack(streams.into_iter().flat_map(|(_, ids)| ids));
            }
            Err(error) if streams.len() == 1 => {
                self.settle_one(slot, error, streams, sent, false);
            }
            Err(_) => {
                let mut reached = false;
                let mut failed = vec![];
                for (notification, ids) in streams {
                    match self.client.notify_streams(vec![notification.clone()]).await {
                        Ok(()) => {
                            self.delivered.store(true, Ordering::Relaxed);
                            reached = true;
                            sent.ack(ids);
                        }
                        Err(error) => failed.push((error, notification, ids)),
                    }
                }
                for (error, notification, ids) in failed {
                    self.settle_one(slot, error, vec![(notification, ids)], sent, reached);
                }
            }
        }
    }

    /// What becomes of one stream's entries whose call failed. A refusal won't change on a retry,
    /// so it is logged and acked. A failure is tried again, but only [MAX_ENTRY_FAILURES] times
    /// while the server takes other streams' calls, since then the server is up and this entry
    /// is the one it can't take.
    fn settle_one(
        &self,
        slot: u32,
        error: NotifyError,
        streams: Vec<(StreamNotification, Vec<String>)>,
        sent: &mut Sent,
        server_reached: bool,
    ) {
        for (notification, ids) in streams {
            let workflow_id = notification.chain.workflow_id.as_str();
            let topic = notification.topic.as_str();
            match &error {
                NotifyError::Unsupported(message) => {
                    tracing::warn!(slot, workflow_id, topic, message, "Dropped a notification");
                    sent.unsupported = true;
                    sent.ack(ids);
                }
                NotifyError::Refused(message) => {
                    tracing::warn!(
                        slot,
                        workflow_id,
                        topic,
                        message,
                        "The server refused a stream notification, so it is dropped"
                    );
                    sent.ack(ids);
                }
                NotifyError::Failed(message) => {
                    let failures = if server_reached {
                        self.failed_again(&ids)
                    } else {
                        0
                    };
                    if failures >= MAX_ENTRY_FAILURES {
                        tracing::warn!(
                            slot,
                            workflow_id,
                            topic,
                            message,
                            "A stream notification failed {failures} times while others landed, \
                             so it is dropped"
                        );
                        sent.ack(ids);
                    } else {
                        tracing::warn!(slot, workflow_id, topic, message, "A notify call failed");
                        sent.left_pending = true;
                    }
                }
            }
        }
    }

    /// Counts one more failure for each entry, and answers the highest count.
    fn failed_again(&self, ids: &[String]) -> u32 {
        let mut failures = self.failures.lock().unwrap();
        // Entries another driver acked since are never seen again, so the map is bounded.
        if failures.len() > MAX_TRACKED_FAILURES {
            failures.clear();
        }
        ids.iter()
            .map(|id| {
                let count = failures.entry(id.clone()).or_default();
                *count += 1;
                *count
            })
            .max()
            .unwrap_or(0)
    }

    /// One notification per stream, at the newest counter the entries hold for it, with the ids
    /// of every entry it stands for.
    fn fold(&self, entries: Vec<NotificationEntry>) -> Vec<(StreamNotification, Vec<String>)> {
        let mut newest: BTreeMap<(ChainKey, String), (i64, String, Vec<String>)> = BTreeMap::new();
        for entry in entries {
            let Some(counter) = progress_counter(&entry.position) else {
                tracing::warn!(
                    workflow_id = %entry.chain.workflow_id,
                    activity_id = %entry.chain.activity_id,
                    topic = %entry.topic,
                    position = %entry.position,
                    "No notifier counter comes from this position, so it isn't notified"
                );
                // No call will take it, so it is acked with the stream's other entries, if any.
                newest
                    .entry((ChainKey::of(&entry.chain), entry.topic))
                    .or_insert((0, String::new(), vec![]))
                    .2
                    .push(entry.id);
                continue;
            };
            let key = (ChainKey::of(&entry.chain), entry.topic);
            let held = newest.entry(key).or_insert((0, String::new(), vec![]));
            held.2.push(entry.id);
            if held.0 < counter {
                (held.0, held.1) = (counter, entry.position);
            }
        }
        newest
            .into_iter()
            .map(|((chain, topic), (counter, position, ids))| {
                let chain = chain.into_chain();
                let (kind, id) = chain_owner(&chain);
                let hash = stream_hash(&chain.namespace, kind, &id, &topic);
                let notification = StreamNotification {
                    position: mint_cursor(self.store.name(), &hash, &position),
                    chain,
                    topic,
                    counter,
                    close: None,
                };
                (notification, ids)
            })
            .collect()
    }
}

/// A chain as a map key: its namespace, Workflow id, first run, owner kind and Activity id.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ChainKey(String, String, String, i32, String);

impl ChainKey {
    fn of(chain: &ChainId) -> Self {
        Self(
            chain.namespace.clone(),
            chain.workflow_id.clone(),
            chain.first_run_id.clone(),
            chain.owner_kind,
            chain.activity_id.clone(),
        )
    }

    fn into_chain(self) -> ChainId {
        ChainId {
            namespace: self.0,
            workflow_id: self.1,
            first_run_id: self.2,
            owner_kind: self.3,
            activity_id: self.4,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryStore, proto::StoreAppendRequest};
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Records each notify call, and fails them while `fail` is set.
    #[derive(Default)]
    struct Recorder {
        calls: Mutex<Vec<Vec<StreamNotification>>>,
        fail: AtomicBool,
        unsupported: AtomicBool,
        /// A call that names this Workflow id is refused whole, as the server does.
        refuse_workflow: Mutex<Option<String>>,
        /// A call that names this Workflow id fails whole.
        fail_workflow: Mutex<Option<String>>,
        /// A call for this namespace answers that it takes no notifications.
        unsupported_namespace: Mutex<Option<String>>,
        /// The server answers an entry for this Workflow id as refused, and takes the others.
        refuse_entry: Mutex<Option<String>>,
        /// The server answers an entry for this Workflow id as failed, and takes the others.
        fail_entry: Mutex<Option<String>>,
    }

    #[async_trait::async_trait]
    impl NotifierClient for Recorder {
        async fn notify(&self, _: StreamNotification) -> Result<(), NotifyError> {
            unreachable!("a driver sends batches")
        }

        async fn notify_streams(
            &self,
            notifications: Vec<StreamNotification>,
        ) -> Result<(), NotifyError> {
            if self.unsupported.load(Ordering::SeqCst) {
                return Err(NotifyError::Unsupported("no notifier".to_string()));
            }
            if self.fail.load(Ordering::SeqCst) {
                return Err(NotifyError::Failed("the server is down".to_string()));
            }
            let names = |field: &Mutex<Option<String>>, of: fn(&StreamNotification) -> &str| {
                let field = field.lock().unwrap();
                notifications
                    .iter()
                    .any(|n| field.as_deref() == Some(of(n)))
            };
            if names(&self.unsupported_namespace, |n| &n.chain.namespace) {
                return Err(NotifyError::Unsupported("off here".to_string()));
            }
            if names(&self.refuse_workflow, |n| &n.chain.workflow_id) {
                return Err(NotifyError::Refused("no such owner".to_string()));
            }
            if names(&self.fail_workflow, |n| &n.chain.workflow_id) {
                return Err(NotifyError::Failed("internal".to_string()));
            }
            self.calls.lock().unwrap().push(notifications);
            Ok(())
        }

        async fn notify_streams_each(
            &self,
            notifications: Vec<StreamNotification>,
        ) -> Result<Vec<Result<(), NotifyError>>, NotifyError> {
            let refused = self.refuse_entry.lock().unwrap().clone();
            let failed = self.fail_entry.lock().unwrap().clone();
            let results: Vec<_> = notifications
                .iter()
                .map(|n| match Some(n.chain.workflow_id.as_str()) {
                    id if id == refused.as_deref() => {
                        Err(NotifyError::Refused("no such owner".to_string()))
                    }
                    id if id == failed.as_deref() => Err(NotifyError::Failed("busy".to_string())),
                    _ => Ok(()),
                })
                .collect();
            let landed = notifications
                .into_iter()
                .zip(&results)
                .filter(|(_, result)| result.is_ok())
                .map(|(notification, _)| notification)
                .collect();
            self.notify_streams(landed).await?;
            Ok(results)
        }
    }

    impl Recorder {
        /// Each stream's notifications, as `(workflow id, topic, counter)`, in call order.
        fn sent(&self) -> Vec<(String, String, i64)> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .flatten()
                .map(|n| (n.chain.workflow_id.clone(), n.topic.clone(), n.counter))
                .collect()
        }
    }

    fn chain_in(namespace: &str, workflow_id: &str) -> ChainId {
        ChainId {
            namespace: namespace.to_string(),
            workflow_id: workflow_id.to_string(),
            first_run_id: "run-1".to_string(),
            ..Default::default()
        }
    }

    async fn append(store: &MemoryStore, workflow_id: &str, topic: &str, sequence: i64) {
        append_in(store, "ns", workflow_id, topic, sequence).await;
    }

    async fn append_in(
        store: &MemoryStore,
        namespace: &str,
        workflow_id: &str,
        topic: &str,
        sequence: i64,
    ) {
        store
            .append(StoreAppendRequest {
                chain: Some(chain_in(namespace, workflow_id)),
                topic: topic.to_string(),
                producer_id: "p".to_string(),
                attempt: 1,
                sequence,
                digest: vec![sequence as u8],
                records: vec![b"record".to_vec()],
            })
            .await
            .unwrap();
    }

    fn driver(store: &Arc<MemoryStore>, client: &Arc<Recorder>, consumer: &str) -> StreamDriver {
        driver_claiming(store, client, consumer, Duration::from_secs(60))
    }

    fn driver_claiming(
        store: &Arc<MemoryStore>,
        client: &Arc<Recorder>,
        consumer: &str,
        claim_idle: Duration,
    ) -> StreamDriver {
        StreamDriver::new(
            store.clone(),
            client.clone(),
            StreamDriverOptions {
                consumer: consumer.to_string(),
                claim_idle,
                ..Default::default()
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn a_drain_sends_each_streams_newest_counter_once_then_acks() {
        let store = Arc::new(MemoryStore::with_notification_log());
        let client = Arc::new(Recorder::default());
        for sequence in 1..=3 {
            append(&store, "a", "out", sequence).await;
        }
        append(&store, "b", "out", 1).await;
        let driver = driver(&store, &client, "d1");
        driver.drain(0).await.unwrap();
        // Memory positions are offsets, and the counter is the offset plus one.
        assert_eq!(
            client.sent(),
            [
                ("a".to_string(), "out".to_string(), 3),
                ("b".to_string(), "out".to_string(), 1)
            ]
        );
        assert_eq!(client.calls.lock().unwrap().len(), 1, "one call per batch");
        let position = client.calls.lock().unwrap()[0][0].position.clone();
        assert!(position.starts_with("memory:"), "a cursor");
        assert!(
            store
                .notification_log()
                .unwrap()
                .busy()
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn an_activity_chains_entries_notify_the_activitys_stream() {
        let store = Arc::new(MemoryStore::with_notification_log());
        let client = Arc::new(Recorder::default());
        let activity = ChainId {
            namespace: "ns".to_string(),
            first_run_id: "run-1".to_string(),
            owner_kind: crate::proto::StreamOwnerKind::Activity as i32,
            activity_id: "a".to_string(),
            ..Default::default()
        };
        // A Workflow with the same id and run writes another stream.
        append(&store, "a", "out", 1).await;
        let landed = store
            .append(StoreAppendRequest {
                chain: Some(activity.clone()),
                topic: "out".to_string(),
                producer_id: "p".to_string(),
                attempt: 1,
                sequence: 1,
                digest: vec![1],
                records: vec![b"record".to_vec(), b"record".to_vec()],
            })
            .await
            .unwrap();
        driver(&store, &client, "d1").drain(0).await.unwrap();
        let calls = client.calls.lock().unwrap().clone();
        assert_eq!(calls[0].len(), 2, "two streams, {calls:?}");
        let sent = calls[0].iter().find(|n| n.chain == activity).unwrap();
        assert_eq!(sent.counter, 2);
        assert_eq!(
            sent.position,
            mint_cursor(
                "memory",
                &stream_hash("ns", crate::ACTIVITY_OWNER_KIND, "a", "out"),
                &landed.last_position
            )
        );
    }

    #[tokio::test]
    async fn a_failed_call_leaves_the_entries_for_the_next_drain() {
        let store = Arc::new(MemoryStore::with_notification_log());
        let client = Arc::new(Recorder::default());
        append(&store, "a", "out", 1).await;
        let driver = driver(&store, &client, "d1");
        client.fail.store(true, Ordering::SeqCst);
        driver.drain(0).await.unwrap();
        assert_eq!(store.notification_log().unwrap().busy().await.unwrap(), [0]);
        client.fail.store(false, Ordering::SeqCst);
        driver.drain(0).await.unwrap();
        assert_eq!(client.sent(), [("a".to_string(), "out".to_string(), 1)]);
    }

    #[tokio::test]
    async fn a_stopped_drivers_entries_move_to_another_driver() {
        let store = Arc::new(MemoryStore::with_notification_log());
        let client = Arc::new(Recorder::default());
        append(&store, "a", "out", 1).await;
        client.fail.store(true, Ordering::SeqCst);
        driver(&store, &client, "stopped").drain(0).await.unwrap();
        client.fail.store(false, Ordering::SeqCst);
        driver(&store, &client, "other").drain(0).await.unwrap();
        assert!(
            client.sent().is_empty(),
            "the first driver's hold is still fresh"
        );
        driver_claiming(&store, &client, "other", Duration::from_millis(10))
            .drain(0)
            .await
            .unwrap();
        assert_eq!(client.sent(), [("a".to_string(), "out".to_string(), 1)]);
    }

    #[tokio::test]
    async fn a_running_driver_wakes_on_the_doorbell() {
        let store = Arc::new(MemoryStore::with_notification_log());
        let client = Arc::new(Recorder::default());
        let running = tokio::spawn(Arc::new(driver(&store, &client, "d1")).run());
        append(&store, "a", "out", 1).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while client.sent().is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the doorbell woke the driver");
        running.abort();
    }

    #[tokio::test]
    async fn a_server_without_notifiers_stops_the_driver() {
        let store = Arc::new(MemoryStore::with_notification_log());
        let client = Arc::new(Recorder::default());
        client.unsupported.store(true, Ordering::SeqCst);
        append(&store, "a", "out", 1).await;
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            Arc::new(driver(&store, &client, "d1")).run(),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.kind, crate::proto::StreamFailureKind::Unsupported);
    }

    async fn log_is_empty(store: &MemoryStore) -> bool {
        store
            .notification_log()
            .unwrap()
            .busy()
            .await
            .unwrap()
            .is_empty()
    }

    #[tokio::test]
    async fn one_refused_entry_holds_back_no_other_stream() {
        let store = Arc::new(MemoryStore::with_notification_log());
        let client = Arc::new(Recorder::default());
        *client.refuse_workflow.lock().unwrap() = Some("bad".to_string());
        append(&store, "bad", "out", 1).await;
        append(&store, "good", "out", 1).await;
        driver(&store, &client, "d1").drain(0).await.unwrap();
        assert_eq!(client.sent(), [("good".to_string(), "out".to_string(), 1)]);
        assert!(log_is_empty(&store).await, "the refused entry is acked too");
    }

    #[tokio::test]
    async fn an_entry_the_answer_refuses_is_acked_with_no_second_call() {
        let store = Arc::new(MemoryStore::with_notification_log());
        let client = Arc::new(Recorder::default());
        *client.refuse_entry.lock().unwrap() = Some("bad".to_string());
        append(&store, "bad", "out", 1).await;
        append(&store, "good", "out", 1).await;
        driver(&store, &client, "d1").drain(0).await.unwrap();
        assert_eq!(client.calls.lock().unwrap().len(), 1);
        assert_eq!(client.sent(), [("good".to_string(), "out".to_string(), 1)]);
        assert!(log_is_empty(&store).await, "the refused entry is acked too");
    }

    #[tokio::test(start_paused = true)]
    async fn an_entry_the_answer_fails_stays_while_no_other_lands() {
        let store = Arc::new(MemoryStore::with_notification_log());
        let client = Arc::new(Recorder::default());
        *client.fail_entry.lock().unwrap() = Some("bad".to_string());
        append(&store, "bad", "out", 1).await;
        let driver = driver(&store, &client, "d1");
        for _ in 0..=MAX_ENTRY_FAILURES {
            driver.drain(0).await.unwrap();
        }
        assert!(!log_is_empty(&store).await);
        *client.fail_entry.lock().unwrap() = None;
        driver.drain(0).await.unwrap();
        assert_eq!(client.sent(), [("bad".to_string(), "out".to_string(), 1)]);
        assert!(log_is_empty(&store).await);
    }

    #[tokio::test(start_paused = true)]
    async fn an_entry_the_answer_keeps_failing_while_others_land_is_dropped() {
        let store = Arc::new(MemoryStore::with_notification_log());
        let client = Arc::new(Recorder::default());
        *client.fail_entry.lock().unwrap() = Some("bad".to_string());
        append(&store, "bad", "out", 1).await;
        let driver = driver(&store, &client, "d1");
        for sequence in 1..=MAX_ENTRY_FAILURES as i64 {
            assert!(!log_is_empty(&store).await, "kept after {sequence} drains");
            append(&store, "good", "out", sequence).await;
            driver.drain(0).await.unwrap();
        }
        assert!(log_is_empty(&store).await);
        assert!(client.sent().iter().all(|(id, _, _)| id == "good"));
    }

    #[tokio::test]
    async fn a_namespace_without_notifiers_stops_neither_the_driver_nor_the_others() {
        let store = Arc::new(MemoryStore::with_notification_log());
        let client = Arc::new(Recorder::default());
        *client.unsupported_namespace.lock().unwrap() = Some("off".to_string());
        append_in(&store, "off", "a", "out", 1).await;
        append_in(&store, "on", "b", "out", 1).await;
        driver(&store, &client, "d1").drain(0).await.unwrap();
        assert_eq!(client.sent(), [("b".to_string(), "out".to_string(), 1)]);
        assert!(log_is_empty(&store).await);
    }

    #[tokio::test(start_paused = true)]
    async fn an_entry_that_keeps_failing_while_others_land_is_dropped() {
        let store = Arc::new(MemoryStore::with_notification_log());
        let client = Arc::new(Recorder::default());
        *client.fail_workflow.lock().unwrap() = Some("bad".to_string());
        append(&store, "bad", "out", 1).await;
        let driver = driver(&store, &client, "d1");
        for sequence in 1..=MAX_ENTRY_FAILURES as i64 {
            assert!(!log_is_empty(&store).await, "kept after {sequence} drains");
            append(&store, "good", "out", sequence).await;
            driver.drain(0).await.unwrap();
        }
        assert!(log_is_empty(&store).await);
        assert_eq!(
            client.sent().last(),
            Some(&(
                "good".to_string(),
                "out".to_string(),
                MAX_ENTRY_FAILURES as i64
            ))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_failing_server_drops_nothing() {
        let store = Arc::new(MemoryStore::with_notification_log());
        let client = Arc::new(Recorder::default());
        append(&store, "a", "out", 1).await;
        append(&store, "b", "out", 1).await;
        client.fail.store(true, Ordering::SeqCst);
        let driver = driver(&store, &client, "d1");
        for _ in 0..=MAX_ENTRY_FAILURES {
            driver.drain(0).await.unwrap();
        }
        assert!(!log_is_empty(&store).await);
    }

    #[test]
    fn the_default_consumer_names_the_host_and_the_process() {
        let consumer = StreamDriverOptions::default().consumer;
        assert!(
            consumer.ends_with(&format!("-{}", std::process::id())),
            "{consumer}"
        );
    }

    #[test]
    fn a_store_without_a_log_needs_no_driver() {
        let error = StreamDriver::new(
            Arc::new(MemoryStore::new()),
            Arc::new(Recorder::default()),
            StreamDriverOptions::default(),
        )
        .err()
        .unwrap();
        assert_eq!(error.kind, crate::proto::StreamFailureKind::Refused);
    }
}
