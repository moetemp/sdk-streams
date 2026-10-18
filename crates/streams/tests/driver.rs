//! The stream driver over a Redis notification log, on a standalone server and on a cluster, and
//! over a PostgreSQL one.
//!
//! Set `STREAMS_REDIS_URL`, `STREAMS_REDIS_CLUSTER_URL` and `STREAMS_POSTGRES_URL` to run these.
//! The cluster cases that move slots and fail a primary over need `STREAMS_REDIS_CLUSTER_ADMIN`,
//! since they change the cluster, and replicas for the failover.

#![cfg(any(feature = "redis", feature = "postgres"))]

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_streams::{
    NotifierClient, NotifyError, StreamDriver, StreamDriverOptions, StreamNotification,
    StreamStore, chain_owner, mint_cursor, progress_counter,
    proto::{ChainId, StoreAppendRequest, StreamOwnerKind},
    stream_hash,
};
#[cfg(feature = "redis")]
use temporalio_streams::{RedisStore, RedisStoreOptions};

static IDS: AtomicUsize = AtomicUsize::new(0);

#[cfg(feature = "redis")]
/// The cases that move a slot or fail a primary over hold it alone, the others share it, since a
/// cluster change makes appends fail for a moment.
static CLUSTER: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        IDS.fetch_add(1, Ordering::Relaxed)
    )
}

/// What a notifier got: each stream's highest counter and its position, by owner id and topic,
/// and every call.
#[derive(Default)]
struct Notifiers {
    newest: Mutex<HashMap<(String, String), i64>>,
    positions: Mutex<HashMap<(String, String), String>>,
    calls: AtomicUsize,
    fail: AtomicBool,
}

#[async_trait::async_trait]
impl NotifierClient for Notifiers {
    async fn notify(&self, _: StreamNotification) -> Result<(), NotifyError> {
        unreachable!("a driver sends batches")
    }

    async fn notify_streams(
        &self,
        notifications: Vec<StreamNotification>,
    ) -> Result<(), NotifyError> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(NotifyError::Failed("the server is down".to_string()));
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut newest = self.newest.lock().unwrap();
        let mut positions = self.positions.lock().unwrap();
        for notification in notifications {
            let key = (
                chain_owner(&notification.chain).1.to_string(),
                notification.topic,
            );
            let counter = newest.entry(key.clone()).or_default();
            if notification.counter > *counter {
                *counter = notification.counter;
                positions.insert(key, notification.position);
            }
        }
        Ok(())
    }
}

impl Notifiers {
    /// Waits a bounded time until every stream in `expected` reached its counter.
    async fn reach(&self, expected: &BTreeMap<(String, String), i64>, within: Duration) {
        let reached = tokio::time::timeout(within, async {
            loop {
                {
                    let newest = self.newest.lock().unwrap();
                    if expected
                        .iter()
                        .all(|(stream, counter)| newest.get(stream) >= Some(counter))
                    {
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        if reached.is_err() {
            let newest = self.newest.lock().unwrap();
            let missing: Vec<_> = expected
                .iter()
                .filter(|(stream, counter)| newest.get(*stream) < Some(*counter))
                .collect();
            panic!("{} streams were not notified: {missing:?}", missing.len());
        }
    }
}

#[cfg(feature = "redis")]
async fn store(url: &str, cluster: bool, prefix: &str) -> Arc<RedisStore> {
    let mut options = RedisStoreOptions::new(url);
    options.cluster = cluster;
    options.key_prefix = prefix.to_string();
    options.notification_log = true;
    options.notify_doorbells = 4;
    Arc::new(RedisStore::connect(options).await.unwrap())
}

fn options(consumer: &str) -> StreamDriverOptions {
    StreamDriverOptions {
        consumer: consumer.to_string(),
        sweep_interval: Duration::from_millis(300),
        claim_idle: Duration::from_millis(200),
        doorbell_wait: Duration::from_millis(200),
        batch: 50,
    }
}

/// Appends one batch to `streams` chains, each in its own slot most likely, and answers each
/// stream's newest counter.
async fn write(
    store: &dyn StreamStore,
    streams: usize,
    sequence: i64,
) -> BTreeMap<(String, String), i64> {
    let mut expected = BTreeMap::new();
    for index in 0..streams {
        let workflow_id = format!("wf-{index}");
        let landed = store
            .append(StoreAppendRequest {
                chain: Some(ChainId {
                    namespace: "ns".to_string(),
                    workflow_id: workflow_id.clone(),
                    first_run_id: "run-1".to_string(),
                    ..Default::default()
                }),
                topic: "out".to_string(),
                producer_id: "p".to_string(),
                attempt: 1,
                sequence,
                digest: vec![sequence as u8],
                records: vec![b"record".to_vec()],
            })
            .await
            .unwrap();
        expected.insert(
            (workflow_id, "out".to_string()),
            progress_counter(&landed.last_position).unwrap(),
        );
    }
    expected
}

/// Appends one batch to the runs of `streams` standalone Activities, `act-<index>`, and answers
/// each Activity stream's newest counter and its cursor.
async fn write_activities(
    store: &dyn StreamStore,
    streams: usize,
) -> BTreeMap<(String, String), (i64, String)> {
    let mut expected = BTreeMap::new();
    for index in 0..streams {
        let id = format!("act-{index}");
        let append = |chain: ChainId| StoreAppendRequest {
            chain: Some(chain),
            topic: "out".to_string(),
            producer_id: "p".to_string(),
            attempt: 1,
            sequence: 1,
            digest: vec![1],
            records: vec![b"record".to_vec()],
        };
        let landed = store
            .append(append(ChainId {
                namespace: "ns".to_string(),
                first_run_id: "run-1".to_string(),
                owner_kind: StreamOwnerKind::Activity as i32,
                activity_id: id.clone(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let position = mint_cursor(
            store.name(),
            &stream_hash("ns", "activity", &id, "out"),
            &landed.last_position,
        );
        expected.insert(
            (id, "out".to_string()),
            (progress_counter(&landed.last_position).unwrap(), position),
        );
    }
    expected
}

async fn activity_streams_reach_their_notifiers(store: Arc<dyn StreamStore>) {
    let notifiers = Arc::new(Notifiers::default());
    let driver =
        Arc::new(StreamDriver::new(store.clone(), notifiers.clone(), options("d1")).unwrap());
    let running = tokio::spawn(driver.run());
    let expected = write_activities(&*store, 100).await;
    let counters = expected
        .iter()
        .map(|(stream, (counter, _))| (stream.clone(), *counter))
        .collect();
    notifiers.reach(&counters, Duration::from_secs(20)).await;
    running.abort();
    let positions = notifiers.positions.lock().unwrap().clone();
    for (stream, (_, position)) in &expected {
        assert_eq!(positions.get(stream), Some(position), "{stream:?}");
    }
}

#[cfg(feature = "redis")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn activity_streams_on_a_server_reach_their_notifiers() {
    let Some(url) = url("STREAMS_REDIS_URL") else {
        return;
    };
    let _shared = CLUSTER.read().await;
    activity_streams_reach_their_notifiers(store(&url, false, &unique("driver-activity")).await)
        .await;
}

#[cfg(feature = "redis")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn activity_streams_in_a_cluster_reach_their_notifiers() {
    let Some(url) = url("STREAMS_REDIS_CLUSTER_URL") else {
        return;
    };
    let _shared = CLUSTER.read().await;
    activity_streams_reach_their_notifiers(store(&url, true, &unique("driver-activity")).await)
        .await;
}

fn url(variable: &str) -> Option<String> {
    let url = std::env::var(variable).ok();
    if url.is_none() {
        eprintln!("set {variable} to run the stream driver tests");
    }
    url
}

async fn streams_in_every_slot_reach_their_notifiers(store: Arc<dyn StreamStore>) {
    let notifiers = Arc::new(Notifiers::default());
    let driver =
        Arc::new(StreamDriver::new(store.clone(), notifiers.clone(), options("d1")).unwrap());
    let running = tokio::spawn(driver.run());
    let expected = write(&*store, 300, 1).await;
    notifiers.reach(&expected, Duration::from_secs(20)).await;
    // A second batch moves every counter on.
    let expected = write(&*store, 300, 2).await;
    notifiers.reach(&expected, Duration::from_secs(20)).await;
    running.abort();
    let log = store.notification_log().unwrap();
    assert!(
        log.busy().await.unwrap().is_empty(),
        "every entry was acked"
    );
}

#[cfg(feature = "redis")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_on_a_server_reach_their_notifiers() {
    let Some(url) = url("STREAMS_REDIS_URL") else {
        return;
    };
    let _shared = CLUSTER.read().await;
    streams_in_every_slot_reach_their_notifiers(store(&url, false, &unique("driver-spread")).await)
        .await;
}

#[cfg(feature = "redis")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_in_every_slot_of_a_cluster_reach_their_notifiers() {
    let Some(url) = url("STREAMS_REDIS_CLUSTER_URL") else {
        return;
    };
    let _shared = CLUSTER.read().await;
    streams_in_every_slot_reach_their_notifiers(store(&url, true, &unique("driver-spread")).await)
        .await;
}

#[cfg(feature = "redis")]
/// The four doorbells of a store under `prefix`, which needs no percent-encoding.
fn bell_keys(prefix: &str) -> Vec<String> {
    let tags: Vec<String> =
        serde_json::from_str(include_str!("../testdata/redis/notify_slots.json")).unwrap();
    tags[..4]
        .iter()
        .map(|tag| format!("{prefix}:{{{tag}}}:bell"))
        .collect()
}

#[cfg(feature = "redis")]
async fn delete(url: &str, cluster: bool, keys: &[String]) {
    for key in keys {
        if cluster {
            let mut conn = redis::cluster::ClusterClient::new(vec![url])
                .unwrap()
                .get_async_connection()
                .await
                .unwrap();
            let _: () = redis::cmd("DEL")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap();
        } else {
            let mut conn = redis::Client::open(url)
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                .unwrap();
            let _: () = redis::cmd("DEL")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap();
        }
    }
}

#[cfg(feature = "redis")]
async fn a_lost_ring_is_found_by_the_sweep(url: &str, cluster: bool) {
    let _shared = CLUSTER.read().await;
    let prefix = unique("driver-sweep");
    let store = store(url, cluster, &prefix).await;
    let expected = write(&*store, 20, 1).await;
    // The rings are gone, as when Core stopped between the append and its ring.
    delete(url, cluster, &bell_keys(&prefix)).await;
    let notifiers = Arc::new(Notifiers::default());
    let driver =
        Arc::new(StreamDriver::new(store.clone(), notifiers.clone(), options("d1")).unwrap());
    let running = tokio::spawn(driver.run());
    // One sweep interval and the time one sweep takes.
    notifiers.reach(&expected, Duration::from_secs(5)).await;
    running.abort();
}

#[cfg(feature = "redis")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_ring_on_a_server_is_found_by_the_sweep() {
    let Some(url) = url("STREAMS_REDIS_URL") else {
        return;
    };
    a_lost_ring_is_found_by_the_sweep(&url, false).await;
}

#[cfg(feature = "redis")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_ring_on_a_cluster_is_found_by_the_sweep() {
    let Some(url) = url("STREAMS_REDIS_CLUSTER_URL") else {
        return;
    };
    a_lost_ring_is_found_by_the_sweep(&url, true).await;
}

#[cfg(feature = "redis")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_drivers_entries_move_to_another_driver() {
    let Some(url) = url("STREAMS_REDIS_CLUSTER_URL") else {
        return;
    };
    let _shared = CLUSTER.read().await;
    a_stopped_drivers_entries_move_on(store(&url, true, &unique("driver-claim")).await).await;
}

async fn a_stopped_drivers_entries_move_on(store: Arc<dyn StreamStore>) {
    let failing = Arc::new(Notifiers::default());
    failing.fail.store(true, Ordering::SeqCst);
    let stopped = tokio::spawn(
        Arc::new(StreamDriver::new(store.clone(), failing.clone(), options("stopped")).unwrap())
            .run(),
    );
    let expected = write(&*store, 30, 1).await;
    // The first driver takes the entries and can't send them, then stops.
    tokio::time::sleep(Duration::from_millis(500)).await;
    stopped.abort();
    let notifiers = Arc::new(Notifiers::default());
    let other = tokio::spawn(
        Arc::new(StreamDriver::new(store.clone(), notifiers.clone(), options("other")).unwrap())
            .run(),
    );
    notifiers.reach(&expected, Duration::from_secs(10)).await;
    other.abort();
}

#[cfg(feature = "redis")]
/// The cluster to move slots on and fail over, when the test may change it.
fn admin_url() -> Option<String> {
    let admin = std::env::var("STREAMS_REDIS_CLUSTER_ADMIN").ok()?;
    url("STREAMS_REDIS_CLUSTER_URL").filter(|_| admin == "1")
}

#[cfg(feature = "redis")]
/// One node of the cluster, as `CLUSTER NODES` lists it.
#[derive(Debug, Clone)]
struct Node {
    id: String,
    url: String,
    primary_id: Option<String>,
}

#[cfg(feature = "redis")]
async fn node_list(url: &str) -> Vec<Node> {
    let mut conn = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let text: String = redis::cmd("CLUSTER")
        .arg("NODES")
        .query_async(&mut conn)
        .await
        .unwrap();
    text.lines()
        .map(|line| {
            let parts: Vec<&str> = line.split_whitespace().collect();
            let address = parts[1].split('@').next().unwrap();
            Node {
                id: parts[0].to_string(),
                url: format!("redis://{address}"),
                primary_id: parts[2].contains("slave").then(|| parts[3].to_string()),
            }
        })
        .collect()
}

#[cfg(feature = "redis")]
async fn on(url: &str, cmd: &redis::Cmd) -> redis::Value {
    let mut conn = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    cmd.query_async(&mut conn).await.unwrap()
}

#[cfg(feature = "redis")]
/// A key's Cluster slot, as Redis computes it from the hash tag.
fn keyslot(key: &str) -> u16 {
    let tag = key
        .split_once('{')
        .and_then(|(_, rest)| rest.split_once('}'))
        .map_or(key, |(tag, _)| tag);
    crc16::State::<crc16::XMODEM>::calculate(tag.as_bytes()) % 16384
}

#[cfg(feature = "redis")]
/// The primary that serves `slot`.
async fn owner_of(url: &str, slot: u16) -> Node {
    let mut conn = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let ranges: Vec<(u16, u16, (String, u16))> = {
        let reply: redis::Value = redis::cmd("CLUSTER")
            .arg("SLOTS")
            .query_async(&mut conn)
            .await
            .unwrap();
        let redis::Value::Array(ranges) = reply else {
            panic!("{reply:?}")
        };
        ranges
            .into_iter()
            .map(|range| {
                let redis::Value::Array(range) = range else {
                    panic!()
                };
                let start: u16 = redis::from_redis_value(range[0].clone()).unwrap();
                let end: u16 = redis::from_redis_value(range[1].clone()).unwrap();
                let redis::Value::Array(node) = range[2].clone() else {
                    panic!()
                };
                let host: String = redis::from_redis_value(node[0].clone()).unwrap();
                let port: u16 = redis::from_redis_value(node[1].clone()).unwrap();
                (start, end, (host, port))
            })
            .collect()
    };
    let (_, _, (host, port)) = ranges
        .into_iter()
        .find(|(start, end, _)| (*start..=*end).contains(&slot))
        .unwrap();
    let url_of = format!("redis://{host}:{port}");
    node_list(url)
        .await
        .into_iter()
        .find(|node| node.url == url_of)
        .unwrap()
}

#[cfg(feature = "redis")]
/// Workflow ids whose chains land in `slot`, found by asking the cluster.
fn ids_in_slot(prefix: &str, slot: u16, count: usize) -> Vec<String> {
    let mut found = vec![];
    let mut index = 0;
    while found.len() < count {
        let workflow_id = format!("wf-{index}");
        if keyslot(&format!("{prefix}:{{ns:{workflow_id}:run-1}}")) == slot {
            found.push(workflow_id);
        }
        index += 1;
    }
    found
}

#[cfg(feature = "redis")]
/// Appends to each of `ids` again and again until `stop`, retrying a batch that a slot move
/// refused, and answers each stream's newest counter.
async fn traffic(
    store: Arc<RedisStore>,
    ids: Vec<String>,
    stop: Arc<AtomicBool>,
) -> BTreeMap<(String, String), i64> {
    let mut expected = BTreeMap::new();
    let mut sequence = 1;
    while !stop.load(Ordering::SeqCst) {
        for workflow_id in &ids {
            let request = StoreAppendRequest {
                chain: Some(ChainId {
                    namespace: "ns".to_string(),
                    workflow_id: workflow_id.clone(),
                    first_run_id: "run-1".to_string(),
                    ..Default::default()
                }),
                topic: "out".to_string(),
                producer_id: "p".to_string(),
                attempt: 1,
                sequence,
                digest: vec![(sequence % 251) as u8, (sequence / 251) as u8],
                records: vec![b"record".to_vec()],
            };
            // The same batch again dedupes, so a retry after an unknown outcome is safe.
            let landed = loop {
                match store.append(request.clone()).await {
                    Ok(landed) => break landed,
                    Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
                }
            };
            expected.insert(
                (workflow_id.clone(), "out".to_string()),
                progress_counter(&landed.last_position).unwrap(),
            );
        }
        sequence += 1;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    expected
}

#[cfg(feature = "redis")]
/// Moves `slot` from its primary to another one, key by key, as `redis-cli --cluster reshard`
/// does.
async fn move_slot(url: &str, slot: u16) {
    let from = owner_of(url, slot).await;
    let to = node_list(url)
        .await
        .into_iter()
        .find(|node| node.primary_id.is_none() && node.id != from.id)
        .unwrap();
    on(
        &to.url,
        redis::cmd("CLUSTER")
            .arg("SETSLOT")
            .arg(slot)
            .arg("IMPORTING")
            .arg(&from.id),
    )
    .await;
    on(
        &from.url,
        redis::cmd("CLUSTER")
            .arg("SETSLOT")
            .arg(slot)
            .arg("MIGRATING")
            .arg(&to.id),
    )
    .await;
    let (host, port) = to
        .url
        .trim_start_matches("redis://")
        .rsplit_once(':')
        .unwrap();
    loop {
        let keys: Vec<String> = redis::from_redis_value(
            on(
                &from.url,
                redis::cmd("CLUSTER")
                    .arg("GETKEYSINSLOT")
                    .arg(slot)
                    .arg(100),
            )
            .await,
        )
        .unwrap();
        if keys.is_empty() {
            break;
        }
        on(
            &from.url,
            redis::cmd("MIGRATE")
                .arg(host)
                .arg(port)
                .arg("")
                .arg(0)
                .arg(5000)
                .arg("KEYS")
                .arg(&keys),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for node in node_list(url).await {
        if node.primary_id.is_none() {
            on(
                &node.url,
                redis::cmd("CLUSTER")
                    .arg("SETSLOT")
                    .arg(slot)
                    .arg("NODE")
                    .arg(&to.id),
            )
            .await;
        }
    }
}

#[cfg(feature = "redis")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slot_moved_under_traffic_loses_no_notification() {
    let Some(url) = admin_url() else { return };
    let _alone = CLUSTER.write().await;
    let prefix = unique("driver-move");
    let store = store(&url, true, &prefix).await;
    let slot = keyslot(&format!("{prefix}:{{ns:wf-0:run-1}}"));
    let ids = ids_in_slot(&prefix, slot, 5);
    let notifiers = Arc::new(Notifiers::default());
    let running = tokio::spawn(
        Arc::new(StreamDriver::new(store.clone(), notifiers.clone(), options("d1")).unwrap()).run(),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let writer = tokio::spawn(traffic(store.clone(), ids, stop.clone()));
    tokio::time::sleep(Duration::from_millis(300)).await;
    move_slot(&url, slot).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop.store(true, Ordering::SeqCst);
    let expected = writer.await.unwrap();
    notifiers.reach(&expected, Duration::from_secs(20)).await;
    running.abort();
}

#[cfg(feature = "redis")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_primary_failover_under_traffic_still_notifies_every_stream() {
    let Some(url) = admin_url() else { return };
    let _alone = CLUSTER.write().await;
    let prefix = unique("driver-failover");
    let store = store(&url, true, &prefix).await;
    let notifiers = Arc::new(Notifiers::default());
    let running = tokio::spawn(
        Arc::new(StreamDriver::new(store.clone(), notifiers.clone(), options("d1")).unwrap()).run(),
    );
    let ids: Vec<String> = (0..40).map(|index| format!("wf-{index}")).collect();
    let stop = Arc::new(AtomicBool::new(false));
    let writer = tokio::spawn(traffic(store.clone(), ids, stop.clone()));
    tokio::time::sleep(Duration::from_millis(300)).await;
    // The replica of the primary that serves slot 0 takes over.
    let primary = owner_of(&url, 0).await;
    let replica = node_list(&url)
        .await
        .into_iter()
        .find(|node| node.primary_id.as_deref() == Some(primary.id.as_str()))
        .expect("each primary has a replica");
    // A manual failover gives up after a few seconds when the replica can't catch up in time,
    // so it is asked again.
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            on(&replica.url, redis::cmd("CLUSTER").arg("FAILOVER")).await;
            for _ in 0..60 {
                if owner_of(&url, 0).await.id == replica.id {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    })
    .await
    .expect("the replica took over");
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::SeqCst);
    let expected = writer.await.unwrap();
    notifiers.reach(&expected, Duration::from_secs(30)).await;
    running.abort();
}

#[cfg(feature = "postgres")]
mod on_postgres {
    use super::*;
    use temporalio_streams::{PostgresStore, PostgresStoreOptions};

    /// A schema no other case uses.
    fn schema() -> String {
        unique("driver").replace('-', "_")
    }

    async fn store(url: &str, schema: &str) -> Arc<PostgresStore> {
        let mut options = PostgresStoreOptions::new(url);
        options.schema = schema.to_string();
        options.max_connections = 8;
        options.notification_log = true;
        options.notify_doorbells = 4;
        Arc::new(PostgresStore::connect(options).await.unwrap())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn streams_on_postgres_reach_their_notifiers() {
        let Some(url) = url("STREAMS_POSTGRES_URL") else {
            return;
        };
        streams_in_every_slot_reach_their_notifiers(store(&url, &schema()).await).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn activity_streams_on_postgres_reach_their_notifiers() {
        let Some(url) = url("STREAMS_POSTGRES_URL") else {
            return;
        };
        activity_streams_reach_their_notifiers(store(&url, &schema()).await).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_lost_ring_on_postgres_is_found_by_the_sweep() {
        let Some(url) = url("STREAMS_POSTGRES_URL") else {
            return;
        };
        let schema = schema();
        // The rings go to the writer's listener, as when the driver's process was away.
        let writer = store(&url, &schema).await;
        let expected = write(&*writer, 20, 1).await;
        let reader = store(&url, &schema).await;
        let notifiers = Arc::new(Notifiers::default());
        let driver =
            Arc::new(StreamDriver::new(reader.clone(), notifiers.clone(), options("d1")).unwrap());
        let running = tokio::spawn(driver.run());
        // One sweep interval and the time one sweep takes.
        notifiers.reach(&expected, Duration::from_secs(5)).await;
        running.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_stopped_drivers_entries_on_postgres_move_to_another_driver() {
        let Some(url) = url("STREAMS_POSTGRES_URL") else {
            return;
        };
        a_stopped_drivers_entries_move_on(store(&url, &schema()).await).await;
    }
}
