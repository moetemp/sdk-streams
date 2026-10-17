//! The store contract, run against every store.
//!
//! Each case goes through [StreamStore] and [read_page] only, so a new store proves the contract
//! by passing this file. A store joins with one `conformance!` line and a [Case] that makes its
//! chains and drops its oldest records. Cases that need a Worker or the owner's History live with
//! the Worker and the owner checks.

use prost::Message;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    BEGINNING, CHAIN_LOG, Capabilities, END, MemoryStore, ReadTarget, StreamResult, StreamStore,
    append_batch, close_chain, close_topic, mint_cursor, proto::*, read_page, stored_append_record,
    stream_hash,
};

#[async_trait::async_trait]
trait Case: Send + Sync {
    fn store(&self) -> Arc<dyn StreamStore>;

    /// A chain no other case uses.
    fn chain(&self) -> ChainId;

    /// Drops all but the newest `keep` records of a topic, as retention does.
    async fn drop_oldest(&self, chain: &ChainId, topic: &str, keep: usize);

    /// Lets a quiet topic's records expire whole, as a log does after a retention with no
    /// write. What the store knows about the topic stays.
    async fn expire_log(&self, chain: &ChainId, topic: &str);
}

static CHAINS: AtomicUsize = AtomicUsize::new(0);

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        CHAINS.fetch_add(1, Ordering::Relaxed)
    )
}

struct Memory(Arc<MemoryStore>);

#[async_trait::async_trait]
impl Case for Memory {
    fn store(&self) -> Arc<dyn StreamStore> {
        self.0.clone()
    }

    fn chain(&self) -> ChainId {
        ChainId {
            namespace: "default".to_string(),
            workflow_id: unique("conformance"),
            first_run_id: unique("run"),
        }
    }

    async fn drop_oldest(&self, chain: &ChainId, topic: &str, keep: usize) {
        self.0.truncate(chain, topic, keep);
    }

    async fn expire_log(&self, chain: &ChainId, topic: &str) {
        self.0.truncate(chain, topic, 0);
    }
}

async fn memory() -> Option<Memory> {
    Some(Memory(Arc::new(MemoryStore::new())))
}

/// The memory store with only the capabilities `mask` leaves, so each fallback runs.
async fn masked(mask: impl FnOnce(&mut Capabilities)) -> Option<Memory> {
    let mut capabilities = MemoryStore::new().capabilities();
    mask(&mut capabilities);
    Some(Memory(Arc::new(MemoryStore::with_capabilities(
        capabilities,
    ))))
}

#[cfg(feature = "redis")]
mod on_redis {
    use super::*;
    use redis::{AsyncCommands, Value};
    use temporalio_streams::{RedisStore, RedisStoreOptions};

    /// A connection of the test's own, to do to the keys what retention does.
    #[derive(Clone)]
    pub(crate) enum Raw {
        Single(redis::aio::MultiplexedConnection),
        Cluster(redis::cluster_async::ClusterConnection),
    }

    impl Raw {
        pub(crate) async fn connect(url: &str, cluster: bool) -> Self {
            if cluster {
                let client = redis::cluster::ClusterClient::new(vec![url]).unwrap();
                return Raw::Cluster(client.get_async_connection().await.unwrap());
            }
            let client = redis::Client::open(url).unwrap();
            Raw::Single(client.get_multiplexed_async_connection().await.unwrap())
        }

        pub(crate) async fn query<T: redis::FromRedisValue>(&mut self, cmd: &redis::Cmd) -> T {
            match self {
                Raw::Single(conn) => cmd.query_async(conn).await.unwrap(),
                Raw::Cluster(conn) => cmd.query_async(conn).await.unwrap(),
            }
        }

        pub(crate) async fn hset(&mut self, key: &str, field: &str, value: &str) {
            let _: () = match self {
                Raw::Single(conn) => conn.hset(key, field, value).await.unwrap(),
                Raw::Cluster(conn) => conn.hset(key, field, value).await.unwrap(),
            };
        }
    }

    /// The key layout, written out again so the tests check the store's.
    pub(crate) fn part(text: &str) -> String {
        text.bytes()
            .map(|byte| match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' | b'~' => {
                    (byte as char).to_string()
                }
                _ => format!("%{byte:02X}"),
            })
            .collect()
    }

    pub(crate) fn log_key(prefix: &str, chain: &ChainId, topic: &str) -> String {
        format!(
            "{}:{{{}:{}:{}}}:t:{}",
            part(prefix),
            part(&chain.namespace),
            part(&chain.workflow_id),
            part(&chain.first_run_id),
            part(topic)
        )
    }

    pub(crate) struct Redis {
        pub(crate) store: Arc<RedisStore>,
        pub(crate) raw: Raw,
        pub(crate) prefix: String,
        _user: Option<DocumentedUser>,
    }

    /// The rules of the documented `ACL SETUSER` line, after the user name.
    pub(crate) fn documented_rules() -> Vec<String> {
        let page = include_str!("../docs/redis.md");
        let lines: Vec<_> = page
            .lines()
            .filter_map(|line| line.strip_prefix("ACL SETUSER "))
            .collect();
        assert_eq!(
            lines.len(),
            1,
            "the Redis page should give one ACL SETUSER line"
        );
        lines[0]
            .split_whitespace()
            .skip(1)
            .map(str::to_string)
            .collect()
    }

    const DOCUMENTED_KEYS: &str = "~temporal-streams:{my-ns:*";

    /// A Redis user with exactly the documented rules, for one prefix and namespace. The page's
    /// key pattern and password are swapped for the test's own, and nothing else changes.
    pub(crate) struct DocumentedUser {
        admin_urls: Vec<String>,
        name: String,
        pub(crate) url: String,
    }

    impl DocumentedUser {
        pub(crate) async fn create(
            url: &str,
            prefix: &str,
            namespace: &str,
            extra: &[&str],
        ) -> Self {
            let rules = documented_rules();
            assert!(
                rules.iter().any(|rule| rule == DOCUMENTED_KEYS),
                "{rules:?}"
            );
            assert!(rules.iter().any(|rule| rule == ">secret"), "{rules:?}");
            let name = unique("streams-acl");
            let password = unique("pw");
            let keys = format!("~{}:{{{}:*", part(prefix), part(namespace));
            let mut setuser = redis::cmd("ACL");
            setuser.arg("SETUSER").arg(&name);
            for rule in &rules {
                match rule.as_str() {
                    DOCUMENTED_KEYS => setuser.arg(&keys),
                    ">secret" => setuser.arg(format!(">{password}")),
                    rule => setuser.arg(rule),
                };
            }
            for rule in extra {
                setuser.arg(*rule);
            }
            // An ACL user lives on one node, so a cluster needs it on every node.
            let admins = nodes(url).await;
            for admin in &admins {
                let mut admin = Raw::connect(admin, false).await;
                let _: () = admin.query(&setuser).await;
            }
            let address = url.trim_start_matches("redis://");
            Self {
                admin_urls: admins,
                url: format!("redis://{name}:{password}@{address}"),
                name,
            }
        }
    }

    impl Drop for DocumentedUser {
        fn drop(&mut self) {
            for url in &self.admin_urls {
                if let Ok(mut admin) =
                    redis::Client::open(url.as_str()).and_then(|client| client.get_connection())
                {
                    let _: redis::RedisResult<()> = redis::cmd("ACL")
                        .arg("DELUSER")
                        .arg(&self.name)
                        .query(&mut admin);
                }
            }
        }
    }

    /// Every node of the server at `url`: the server itself, or each node of a cluster.
    async fn nodes(url: &str) -> Vec<String> {
        let mut admin = Raw::connect(url, false).await;
        let info: String = admin.query(redis::cmd("INFO").arg("cluster")).await;
        if !info.contains("cluster_enabled:1") {
            return vec![url.to_string()];
        }
        let nodes: String = admin.query(redis::cmd("CLUSTER").arg("NODES")).await;
        nodes
            .lines()
            .filter_map(|line| line.split_whitespace().nth(1))
            .map(|address| format!("redis://{}/0", address.split('@').next().unwrap()))
            .collect()
    }

    pub(crate) async fn redis_as_documented_user(
        variable: &str,
        cluster: bool,
        extra: &[&str],
    ) -> Option<Redis> {
        let Ok(url) = std::env::var(variable) else {
            eprintln!("set {variable} to run the Redis store cases");
            return None;
        };
        let prefix = unique("conformance-acl");
        let user = DocumentedUser::create(&url, &prefix, "default", extra).await;
        let mut options = RedisStoreOptions::new(user.url.clone());
        options.key_prefix = prefix.clone();
        options.cluster = cluster;
        Some(Redis {
            store: Arc::new(RedisStore::connect(options).await.unwrap()),
            raw: Raw::connect(&url, cluster).await,
            prefix,
            _user: Some(user),
        })
    }

    pub(crate) async fn redis(variable: &str, cluster: bool) -> Option<Redis> {
        redis_in(variable, cluster, false).await
    }

    /// Redis with no Lua, so the store keeps no producer state and no last position.
    pub(crate) async fn plain_redis(variable: &str, cluster: bool) -> Option<Redis> {
        redis_in(variable, cluster, true).await
    }

    async fn redis_in(variable: &str, cluster: bool, plain: bool) -> Option<Redis> {
        let Ok(url) = std::env::var(variable) else {
            eprintln!("set {variable} to run the Redis store cases");
            return None;
        };
        // A prefix per case keeps cases apart in one Redis.
        let prefix = unique("conformance");
        let mut options = RedisStoreOptions::new(url.clone());
        options.cluster = cluster;
        options.key_prefix = prefix.clone();
        options.plain = plain;
        Some(Redis {
            store: Arc::new(RedisStore::connect(options).await.unwrap()),
            raw: Raw::connect(&url, cluster).await,
            prefix,
            _user: None,
        })
    }

    #[async_trait::async_trait]
    impl Case for Redis {
        fn store(&self) -> Arc<dyn StreamStore> {
            self.store.clone()
        }

        fn chain(&self) -> ChainId {
            ChainId {
                namespace: "default".to_string(),
                workflow_id: unique("conformance"),
                first_run_id: unique("run"),
            }
        }

        async fn drop_oldest(&self, chain: &ChainId, topic: &str, keep: usize) {
            // Trims as the append script does, watermark included.
            let mut raw = self.raw.clone();
            let log = log_key(&self.prefix, chain, topic);
            let entries: Vec<(String, Value)> = raw
                .query(redis::cmd("XRANGE").arg(&log).arg("-").arg("+"))
                .await;
            let Some(doomed) = entries.len().checked_sub(keep).filter(|&n| n > 0) else {
                return;
            };
            let _: i64 = raw
                .query(redis::cmd("XTRIM").arg(&log).arg("MAXLEN").arg(keep))
                .await;
            raw.hset(&format!("{log}:meta"), "trimmed", &entries[doomed - 1].0)
                .await;
        }

        async fn expire_log(&self, chain: &ChainId, topic: &str) {
            // The log key expires and its meta stays, as the tombstone.
            let mut raw = self.raw.clone();
            let _: i64 = raw
                .query(redis::cmd("DEL").arg(log_key(&self.prefix, chain, topic)))
                .await;
        }
    }
}

const OUT: &str = "out";
const OTHER: &str = "other";

/// One producer attempt on one topic, numbering its records as a producer in lang does.
struct Producer {
    store: Arc<dyn StreamStore>,
    chain: ChainId,
    topic: String,
    id: String,
    attempt: i64,
    sequence: i64,
}

impl Producer {
    fn new(case: &dyn Case, chain: &ChainId, topic: &str, id: &str, attempt: i64) -> Self {
        Self {
            store: case.store(),
            chain: chain.clone(),
            topic: topic.to_string(),
            id: id.to_string(),
            attempt,
            sequence: 1,
        }
    }

    fn request(&self, records: &[AppendRecord]) -> StoreAppendRequest {
        StoreAppendRequest {
            chain: Some(self.chain.clone()),
            topic: self.topic.clone(),
            producer_id: self.id.clone(),
            attempt: self.attempt,
            sequence: self.sequence,
            digest: digest(records),
            records: records
                .iter()
                .enumerate()
                .map(|(index, record)| {
                    stored_append_record(
                        &self.topic,
                        &self.id,
                        self.attempt,
                        self.sequence + index as i64,
                        record,
                    )
                    .unwrap()
                    .encode_to_vec()
                })
                .collect(),
        }
    }

    async fn send(&mut self, records: &[AppendRecord]) -> StreamResult<StoreAppendResponse> {
        let response = self.store.append(self.request(records)).await?;
        self.sequence += records.len() as i64;
        Ok(response)
    }

    async fn append(&mut self, values: &[&str]) -> StoreAppendResponse {
        let records: Vec<_> = values.iter().map(|value| data(value)).collect();
        self.send(&records).await.unwrap()
    }
}

/// A record as lang hands it over: the body already encoded, the hash taken before the codec.
fn data(value: &str) -> AppendRecord {
    encoded(value, value.as_bytes())
}

fn encoded(value: &str, ciphertext: &[u8]) -> AppendRecord {
    AppendRecord {
        kind: StreamRecordKind::Data as i32,
        body: Some(Payload {
            metadata: [("encoding".to_string(), b"binary/test".to_vec())].into(),
            data: ciphertext.to_vec(),
            ..Default::default()
        }),
        content_hash: hash(value),
    }
}

fn finish() -> AppendRecord {
    AppendRecord {
        kind: StreamRecordKind::Finish as i32,
        ..Default::default()
    }
}

fn hash(value: &str) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    Sha256::digest(value.as_bytes()).to_vec()
}

/// Stands in for lang's batch digest, which a store keeps and compares as opaque bytes.
fn digest(records: &[AppendRecord]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    for record in records {
        digest.update(record.kind.to_be_bytes());
        digest.update((record.content_hash.len() as u64).to_be_bytes());
        digest.update(&record.content_hash);
    }
    digest.finalize().to_vec()
}

fn target(chain: &ChainId, topic: &str) -> ReadTarget {
    ReadTarget {
        chain: chain.clone(),
        topic: topic.to_string(),
        stream_hash: stream_hash(&chain.namespace, "workflow", &chain.workflow_id, topic),
    }
}

fn cursor(case: &dyn Case, chain: &ChainId, topic: &str, position: &str) -> String {
    mint_cursor(
        case.store().name(),
        &target(chain, topic).stream_hash,
        position,
    )
}

async fn read(
    case: &dyn Case,
    chain: &ChainId,
    topic: &str,
    after: &str,
) -> StreamResult<ReadResponse> {
    read_with(case, chain, topic, after, Duration::ZERO, &[]).await
}

async fn read_with(
    case: &dyn Case,
    chain: &ChainId,
    topic: &str,
    after: &str,
    wait: Duration,
    state: &[u8],
) -> StreamResult<ReadResponse> {
    read_page(
        &*case.store(),
        &target(chain, topic),
        &ReadRequest {
            after: after.to_string(),
            wait: Some(wait.try_into().unwrap()),
            state: state.to_vec(),
            ..Default::default()
        },
    )
    .await
}

fn stored(record: &ReadRecord) -> &StreamRecord {
    match record.record.as_ref().unwrap() {
        read_record::Record::Stored(stored) => stored,
        other => panic!("not a stored record: {other:?}"),
    }
}

/// The body of each stored record, and `SUPERSEDED` for a supersession.
fn values(response: &ReadResponse) -> Vec<String> {
    response
        .records
        .iter()
        .map(|record| match record.record.as_ref().unwrap() {
            read_record::Record::Stored(stored) => match &stored.body {
                Some(body) => String::from_utf8(body.data.clone()).unwrap(),
                None => "FINISH".to_string(),
            },
            read_record::Record::Superseded(_) => "SUPERSEDED".to_string(),
        })
        .collect()
}

async fn latest(case: &dyn Case, chain: &ChainId, topic: &str) -> String {
    case.store()
        .latest(StoreLatestRequest {
            chain: Some(chain.clone()),
            topic: topic.to_string(),
        })
        .await
        .unwrap()
        .position
}

fn kind(result: StreamResult<impl std::fmt::Debug>) -> StreamFailureKind {
    result.unwrap_err().kind
}

mod cases {
    use super::*;

    pub(crate) async fn append_read_roundtrip(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.append(&["1"]).await;
        producer.append(&["2"]).await;
        let read = read(case, &chain, OUT, BEGINNING).await.unwrap();
        assert_eq!(values(&read), ["1", "2"]);
        let identity: Vec<_> = read
            .records
            .iter()
            .map(|record| {
                let stored = stored(record);
                (
                    stored.topic.as_str(),
                    stored.kind,
                    stored.producer_id.as_str(),
                    stored.attempt,
                    stored.sequence,
                )
            })
            .collect();
        let data = StreamRecordKind::Data as i32;
        assert_eq!(identity, [(OUT, data, "p", 1, 1), (OUT, data, "p", 1, 2)]);
    }

    pub(crate) async fn a_batch_lands_in_order_and_names_its_last_record(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        let landed = producer.append(&["1", "2", "3"]).await;
        let read = read(case, &chain, OUT, BEGINNING).await.unwrap();
        assert_eq!(values(&read), ["1", "2", "3"]);
        let sequences: Vec<_> = read.records.iter().map(|r| stored(r).sequence).collect();
        assert_eq!(sequences, [1, 2, 3]);
        assert_eq!(
            read.records[0].cursor,
            cursor(case, &chain, OUT, &landed.first_position)
        );
        assert_eq!(
            read.records[2].cursor,
            cursor(case, &chain, OUT, &landed.last_position)
        );
        assert_eq!(latest(case, &chain, OUT).await, landed.last_position);
    }

    pub(crate) async fn an_empty_append_is_refused(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        assert_eq!(kind(producer.send(&[]).await), StreamFailureKind::Refused);
        assert_eq!(latest(case, &chain, OUT).await, "");
    }

    pub(crate) async fn an_append_without_a_digest_is_refused(case: &dyn Case) {
        let chain = case.chain();
        let producer = Producer::new(case, &chain, OUT, "p", 1);
        let mut request = producer.request(&[data("1")]);
        request.digest.clear();
        assert_eq!(
            kind(case.store().append(request).await),
            StreamFailureKind::Refused
        );
    }

    pub(crate) async fn latest_is_empty_on_an_empty_topic(case: &dyn Case) {
        assert_eq!(latest(case, &case.chain(), OUT).await, "");
    }

    pub(crate) async fn a_cursor_resumes_strictly_after_its_record(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        let first = producer.append(&["1"]).await;
        producer.append(&["2", "3"]).await;
        let after = cursor(case, &chain, OUT, &first.last_position);
        let read = read(case, &chain, OUT, &after).await.unwrap();
        assert_eq!(values(&read), ["2", "3"]);
        // A cursor a reader saw resumes the same way as one an append returned.
        let again = super::read(case, &chain, OUT, &read.records[0].cursor)
            .await
            .unwrap();
        assert_eq!(values(&again), ["3"]);
        assert_eq!(read.cursor, read.records[1].cursor);
    }

    pub(crate) async fn latest_positions_a_reader_at_the_end(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.append(&["1"]).await;
        let end = cursor(case, &chain, OUT, &latest(case, &chain, OUT).await);
        producer.append(&["2"]).await;
        assert_eq!(values(&read(case, &chain, OUT, &end).await.unwrap()), ["2"]);
    }

    pub(crate) async fn end_reads_only_what_arrives_after_the_read_starts(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.append(&["1"]).await;
        let mut later = Producer::new(case, &chain, OUT, "p", 1);
        later.sequence = 2;
        let writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            later.append(&["2"]).await;
        });
        let read = read_with(case, &chain, OUT, END, Duration::from_secs(5), &[])
            .await
            .unwrap();
        writer.await.unwrap();
        assert_eq!(values(&read), ["2"]);
        let quiet = read_with(case, &chain, OUT, END, Duration::from_millis(200), &[])
            .await
            .unwrap();
        assert!(quiet.records.is_empty());
        assert_eq!(quiet.cursor, read.cursor);
    }

    pub(crate) async fn a_read_waits_no_longer_than_asked(case: &dyn Case) {
        let chain = case.chain();
        let started = tokio::time::Instant::now();
        let read = read_with(
            case,
            &chain,
            OUT,
            BEGINNING,
            Duration::from_millis(200),
            &[],
        )
        .await
        .unwrap();
        assert!(read.records.is_empty());
        assert_eq!(read.cursor, BEGINNING);
        let waited = started.elapsed();
        assert!(waited >= Duration::from_millis(150), "{waited:?}");
        assert!(waited < Duration::from_secs(3), "{waited:?}");
    }

    pub(crate) async fn dropping_a_waiting_read_releases_it(case: &dyn Case) {
        let chain = case.chain();
        let parked = read_with(case, &chain, OUT, BEGINNING, Duration::from_secs(30), &[]);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), parked)
                .await
                .is_err()
        );
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        assert_eq!(
            values(&read(case, &chain, OUT, BEGINNING).await.unwrap()),
            ["1"]
        );
    }

    pub(crate) async fn beginning_starts_at_the_oldest_record_still_held(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        let old = producer.append(&["1"]).await;
        producer.append(&["2", "3"]).await;
        case.drop_oldest(&chain, OUT, 2).await;
        let read = read(case, &chain, OUT, BEGINNING).await.unwrap();
        assert_eq!(values(&read), ["2", "3"]);
        // The dropped record's cursor still resumes, because nothing after it was dropped.
        let old = cursor(case, &chain, OUT, &old.last_position);
        assert_eq!(
            values(&super::read(case, &chain, OUT, &old).await.unwrap()),
            ["2", "3"]
        );
        // Once a record after it is gone too, the cursor is expired, which a reader can tell
        // apart from a cursor that was never valid here.
        case.drop_oldest(&chain, OUT, 1).await;
        assert_eq!(
            kind(super::read(case, &chain, OUT, &old).await),
            StreamFailureKind::Expired
        );
    }

    pub(crate) async fn a_reader_that_falls_behind_retention_is_told(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.append(&["1", "2", "3"]).await;
        let first = read(case, &chain, OUT, BEGINNING).await.unwrap();
        let after_one = first.records[0].cursor.clone();
        case.drop_oldest(&chain, OUT, 1).await;
        // The reader got record 1, and record 2 is gone, so it must hear so.
        assert_eq!(
            kind(read_with(case, &chain, OUT, &after_one, Duration::ZERO, &first.state).await),
            StreamFailureKind::Expired
        );
    }

    pub(crate) async fn an_empty_read_from_the_beginning_learns_of_a_trim(case: &dyn Case) {
        // The read has delivered nothing, so it has no position, but records written after it
        // started and dropped before its next call are still lost to it.
        let chain = case.chain();
        let first = read(case, &chain, OUT, BEGINNING).await.unwrap();
        assert!(first.records.is_empty());
        assert_eq!(first.cursor, BEGINNING);
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1", "2"])
            .await;
        case.drop_oldest(&chain, OUT, 1).await;
        assert_eq!(
            kind(
                read_with(
                    case,
                    &chain,
                    OUT,
                    &first.cursor,
                    Duration::ZERO,
                    &first.state
                )
                .await
            ),
            StreamFailureKind::Expired
        );
        // A new read starts at the oldest record left, as a read from the beginning does.
        assert_eq!(
            values(&read(case, &chain, OUT, BEGINNING).await.unwrap()),
            ["2"]
        );
    }

    pub(crate) async fn rule_11_2_an_empty_read_goes_on_when_a_quiet_log_comes_back(
        case: &dyn Case,
    ) {
        // A log that expired comes back with its next write, and nothing the read could have
        // seen was lost.
        let chain = case.chain();
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        case.expire_log(&chain, OUT).await;
        let first = read(case, &chain, OUT, BEGINNING).await.unwrap();
        assert!(first.records.is_empty());
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.sequence = 2;
        producer.append(&["2"]).await;
        let next = read_with(
            case,
            &chain,
            OUT,
            &first.cursor,
            Duration::ZERO,
            &first.state,
        )
        .await
        .unwrap();
        assert_eq!(values(&next), ["2"]);
    }

    pub(crate) async fn an_empty_read_from_the_beginning_with_nothing_trimmed_goes_on(
        case: &dyn Case,
    ) {
        let chain = case.chain();
        let first = read(case, &chain, OUT, BEGINNING).await.unwrap();
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        let next = read_with(
            case,
            &chain,
            OUT,
            &first.cursor,
            Duration::ZERO,
            &first.state,
        )
        .await
        .unwrap();
        assert_eq!(values(&next), ["1"]);
    }

    pub(crate) async fn a_retried_append_returns_the_original_positions(case: &dyn Case) {
        let chain = case.chain();
        let first = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1", "2"])
            .await;
        // A producer that lost the answer starts over with the same identity, as a restarted
        // process does.
        let retry = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1", "2"])
            .await;
        assert_eq!(retry, first);
        assert_eq!(latest(case, &chain, OUT).await, first.last_position);
        assert_eq!(
            values(&read(case, &chain, OUT, BEGINNING).await.unwrap()),
            ["1", "2"]
        );
    }

    pub(crate) async fn a_retry_with_other_ciphertext_still_deduplicates(case: &dyn Case) {
        // A codec that encrypts with a fresh nonce makes a retry's bytes differ. The digest is
        // over the plaintext hashes, so the store still knows the batch.
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        let first = producer.send(&[encoded("1", b"nonce-a")]).await.unwrap();
        let mut retry = Producer::new(case, &chain, OUT, "p", 1);
        let again = retry.send(&[encoded("1", b"nonce-b")]).await.unwrap();
        assert_eq!(again, first);
        assert_eq!(
            values(&read(case, &chain, OUT, BEGINNING).await.unwrap()),
            ["nonce-a"]
        );
    }

    pub(crate) async fn a_divergent_retry_is_refused(case: &dyn Case) {
        let chain = case.chain();
        let first = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        let mut retry = Producer::new(case, &chain, OUT, "p", 1);
        assert_eq!(
            kind(retry.send(&[data("different")]).await),
            StreamFailureKind::ProducerDivergent
        );
        assert_eq!(latest(case, &chain, OUT).await, first.last_position);
    }

    pub(crate) async fn a_sequence_below_the_newest_is_refused(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.append(&["1"]).await;
        let newest = producer.append(&["2"]).await;
        // Even the same content is refused: only the newest batch is a retry.
        let mut stale = Producer::new(case, &chain, OUT, "p", 1);
        assert_eq!(
            kind(stale.send(&[data("1")]).await),
            StreamFailureKind::ProducerStale
        );
        assert_eq!(latest(case, &chain, OUT).await, newest.last_position);
    }

    pub(crate) async fn rule_9_1_a_batch_inside_the_held_batch_is_refused(case: &dyn Case) {
        let chain = case.chain();
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1", "2", "3"])
            .await;
        // A second writer of the same attempt, whose sequence sits inside the held batch.
        let mut inside = Producer::new(case, &chain, OUT, "p", 1);
        inside.sequence = 2;
        assert_eq!(
            kind(inside.send(&[data("9")]).await),
            StreamFailureKind::ProducerStale
        );
    }

    pub(crate) async fn rule_64_1_a_gap_in_the_sequence_is_accepted(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.append(&["1"]).await;
        producer.sequence = 10;
        producer.append(&["10"]).await;
        assert_eq!(
            values(&read(case, &chain, OUT, BEGINNING).await.unwrap()),
            ["1", "10"]
        );
    }

    pub(crate) async fn producers_and_attempts_dedupe_apart(case: &dyn Case) {
        let chain = case.chain();
        Producer::new(case, &chain, OUT, "a", 1)
            .append(&["1"])
            .await;
        Producer::new(case, &chain, OUT, "b", 1)
            .append(&["1"])
            .await;
        Producer::new(case, &chain, OUT, "a", 2)
            .append(&["1"])
            .await;
        let read = read(case, &chain, OUT, BEGINNING).await.unwrap();
        let data: Vec<_> = read
            .records
            .iter()
            .filter_map(|record| match record.record.as_ref().unwrap() {
                read_record::Record::Stored(s) => Some((s.producer_id.as_str(), s.attempt)),
                read_record::Record::Superseded(_) => None,
            })
            .collect();
        assert_eq!(data, [("a", 1), ("b", 1), ("a", 2)]);
    }

    pub(crate) async fn a_new_attempt_supersedes_the_old_one(case: &dyn Case) {
        let chain = case.chain();
        Producer::new(case, &chain, OUT, "model", 1)
            .append(&["1"])
            .await;
        Producer::new(case, &chain, OUT, "model", 2)
            .append(&["2"])
            .await;
        let read = read(case, &chain, OUT, BEGINNING).await.unwrap();
        assert_eq!(values(&read), ["1", "SUPERSEDED", "2"]);
        let Some(read_record::Record::Superseded(superseded)) = &read.records[1].record else {
            panic!("no supersession");
        };
        assert_eq!(
            (
                superseded.producer_id.as_str(),
                superseded.previous_attempt,
                superseded.attempt
            ),
            ("model", 1, 2)
        );
        // The supersession sits at the cursor of the old attempt's last record. A read that
        // resumes there knows that attempt, so it reports the new one again.
        assert_eq!(read.records[1].cursor, read.records[0].cursor);
        let resumed = super::read(case, &chain, OUT, &read.records[1].cursor)
            .await
            .unwrap();
        assert_eq!(values(&resumed), ["SUPERSEDED", "2"]);
    }

    pub(crate) async fn a_read_resumed_before_a_new_attempt_reports_it(case: &dyn Case) {
        let chain = case.chain();
        let mut first = Producer::new(case, &chain, OUT, "model", 1);
        first.append(&["1"]).await;
        let delivered = first.append(&["2"]).await;
        // The reader stops here, then the producer's retry writes.
        Producer::new(case, &chain, OUT, "model", 2)
            .append(&["3"])
            .await;
        let after = cursor(case, &chain, OUT, &delivered.last_position);
        let resumed = read(case, &chain, OUT, &after).await.unwrap();
        assert_eq!(values(&resumed), ["SUPERSEDED", "3"]);
    }

    pub(crate) async fn a_record_that_does_not_parse_fails_with_its_cursor(case: &dyn Case) {
        let chain = case.chain();
        let producer = Producer::new(case, &chain, OUT, "p", 1);
        let mut request = producer.request(&[data("bad")]);
        request.records = vec![b"\xff not a record".to_vec()];
        let bad = case.store().append(request).await.unwrap();
        let mut next = Producer::new(case, &chain, OUT, "p", 1);
        next.sequence = 2;
        next.append(&["7"]).await;
        let error = read(case, &chain, OUT, BEGINNING).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Record);
        let bad = cursor(case, &chain, OUT, &bad.last_position);
        assert_eq!(error.cursor.as_deref(), Some(bad.as_str()));
        // Resuming past it is the caller's choice, and the next record reads.
        assert_eq!(values(&read(case, &chain, OUT, &bad).await.unwrap()), ["7"]);
    }

    pub(crate) async fn finish_is_a_record_of_its_own(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.append(&["1"]).await;
        let finished = producer.send(&[finish()]).await.unwrap();
        let read = read(case, &chain, OUT, BEGINNING).await.unwrap();
        assert_eq!(values(&read), ["1", "FINISH"]);
        assert_eq!(
            stored(&read.records[1]).kind,
            StreamRecordKind::Finish as i32
        );
        assert_eq!(stored(&read.records[1]).producer_id, "p");
        assert_eq!(
            read.records[1].cursor,
            cursor(case, &chain, OUT, &finished.last_position)
        );
    }

    pub(crate) async fn topics_and_chains_are_apart(case: &dyn Case) {
        let chain = case.chain();
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        Producer::new(case, &chain, OTHER, "p", 1)
            .append(&["2"])
            .await;
        // A new chain on the same Workflow id gets streams of its own.
        let next = ChainId {
            first_run_id: format!("{}-next", chain.first_run_id),
            ..chain.clone()
        };
        Producer::new(case, &next, OUT, "p", 1).append(&["3"]).await;
        assert_eq!(
            values(&read(case, &chain, OTHER, BEGINNING).await.unwrap()),
            ["2"]
        );
        assert_eq!(
            values(&read(case, &chain, OUT, BEGINNING).await.unwrap()),
            ["1"]
        );
        assert_eq!(
            values(&read(case, &next, OUT, BEGINNING).await.unwrap()),
            ["3"]
        );
    }

    pub(crate) async fn a_cursor_from_another_stream_or_store_is_refused(case: &dyn Case) {
        let one = case.chain();
        let other = case.chain();
        let landed = Producer::new(case, &one, OUT, "p", 1).append(&["1"]).await;
        Producer::new(case, &other, OUT, "p", 1)
            .append(&["1"])
            .await;
        Producer::new(case, &one, OTHER, "p", 1)
            .append(&["1"])
            .await;
        // Another Workflow's stream, and another topic of the same one, both hold a record at
        // that position. Neither may resume from it.
        let cursor = cursor(case, &one, OUT, &landed.last_position);
        for (chain, topic) in [(&other, OUT), (&one, OTHER)] {
            let error = read(case, chain, topic, &cursor).await.unwrap_err();
            assert_eq!(error.kind, StreamFailureKind::Cursor);
            assert!(error.message.contains("another stream"), "{error}");
        }
        for token in ["elsewhere:0000abcd:1", "not a cursor"] {
            assert_eq!(
                kind(read(case, &one, OUT, token).await),
                StreamFailureKind::Cursor
            );
        }
    }

    pub(crate) async fn a_closed_chain_refuses_new_batches_but_answers_a_repeat(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        let landed = producer.append(&["1"]).await;
        case.store().close_chain(&chain).await.unwrap();
        let repeat = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        assert_eq!(repeat, landed);
        assert_eq!(
            kind(producer.send(&[data("2")]).await),
            StreamFailureKind::Closed
        );
        assert_eq!(
            kind(
                Producer::new(case, &chain, OTHER, "q", 1)
                    .send(&[data("1")])
                    .await
            ),
            StreamFailureKind::Closed
        );
        let read = case
            .store()
            .read(StoreReadRequest {
                chain: Some(chain.clone()),
                topic: OUT.to_string(),
                max_records: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(read.closed);
        assert_eq!(read.records.len(), 1);
    }

    pub(crate) async fn a_refused_append_leaves_no_trace_of_its_topic(case: &dyn Case) {
        // Every store must answer a resume on a topic that never held a record the same way.
        let chain = case.chain();
        let landed = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        case.store().close_chain(&chain).await.unwrap();
        assert_eq!(
            kind(
                Producer::new(case, &chain, OTHER, "p", 1)
                    .send(&[data("2")])
                    .await
            ),
            StreamFailureKind::Closed
        );
        let after = cursor(case, &chain, OTHER, &landed.last_position);
        assert_eq!(
            kind(read(case, &chain, OTHER, &after).await),
            StreamFailureKind::NotFound
        );
    }

    pub(crate) async fn a_closed_topic_leaves_the_others_open(case: &dyn Case) {
        let chain = case.chain();
        case.store().close_topic(&chain, OUT).await.unwrap();
        assert_eq!(
            kind(
                Producer::new(case, &chain, OUT, "p", 1)
                    .send(&[data("1")])
                    .await
            ),
            StreamFailureKind::Closed
        );
        Producer::new(case, &chain, OTHER, "p", 1)
            .append(&["1"])
            .await;
    }

    pub(crate) async fn a_closed_topic_answers_a_repeat_and_ends_reads(case: &dyn Case) {
        let chain = case.chain();
        let landed = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        case.store().close_topic(&chain, OUT).await.unwrap();
        let repeat = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        assert_eq!(repeat, landed);
        for topic in [OUT, OTHER] {
            let read = case
                .store()
                .read(StoreReadRequest {
                    chain: Some(chain.clone()),
                    topic: topic.to_string(),
                    max_records: 10,
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(read.closed, topic == OUT, "{topic}");
        }
    }

    pub(crate) async fn retiring_a_producer_drops_only_its_dedupe_state(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        let first = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        let kept = Producer::new(case, &chain, OUT, "q", 1)
            .append(&["2"])
            .await;
        store.retire_producer(&chain, OUT, "p", 1).await.unwrap();
        // A repeat, an attempt that never wrote, a topic and a chain that hold nothing.
        store.retire_producer(&chain, OUT, "p", 1).await.unwrap();
        store.retire_producer(&chain, OUT, "p", 2).await.unwrap();
        store.retire_producer(&chain, OTHER, "p", 1).await.unwrap();
        store
            .retire_producer(&case.chain(), OUT, "p", 1)
            .await
            .unwrap();
        assert_eq!(
            values(&read(case, &chain, OUT, BEGINNING).await.unwrap()),
            ["1", "2"]
        );
        // The other producer still dedupes.
        let repeat = Producer::new(case, &chain, OUT, "q", 1)
            .append(&["2"])
            .await;
        assert_eq!(repeat, kept);
        // The retired attempt no longer does, which is why only an attempt whose last append
        // returned may be retired. Readers still drop the second copy.
        let again = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        assert_ne!(again.first_position, first.first_position);
        assert_eq!(
            values(&read(case, &chain, OUT, BEGINNING).await.unwrap()),
            ["1", "2"]
        );
    }

    pub(crate) async fn deleting_an_owner_drops_every_chain_of_it(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        let next = ChainId {
            first_run_id: format!("{}-next", chain.first_run_id),
            ..chain.clone()
        };
        let bystander = case.chain();
        let landed = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        Producer::new(case, &next, OUT, "p", 1).append(&["1"]).await;
        Producer::new(case, &bystander, OUT, "p", 1)
            .append(&["1"])
            .await;
        let request = DeleteOwnerRequest {
            namespace: chain.namespace.clone(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: chain.workflow_id.clone(),
        };
        assert!(store.delete_owner(request.clone()).await.unwrap().deleted > 0);
        assert_eq!(store.delete_owner(request).await.unwrap().deleted, 0);
        assert_eq!(latest(case, &chain, OUT).await, "");
        // Nothing is known about what followed the cursor any more.
        let after = cursor(case, &chain, OUT, &landed.last_position);
        assert_eq!(
            kind(read(case, &chain, OUT, &after).await),
            StreamFailureKind::NotFound
        );
        assert_eq!(
            values(&read(case, &bystander, OUT, BEGINNING).await.unwrap()),
            ["1"]
        );
    }

    /// What a store declares agrees with the calls it answers.
    pub(crate) async fn the_declared_capabilities_match_the_calls(case: &dyn Case) {
        let store = case.store();
        let declared = store.capabilities();
        let chain = case.chain();
        let unsupported = |result: StreamResult<()>| {
            result.is_err_and(|error| error.kind == StreamFailureKind::Unsupported)
        };
        assert_eq!(
            unsupported(store.close_chain(&chain).await),
            !declared.producer_state
        );
        assert_eq!(
            unsupported(store.close_topic(&chain, OUT).await),
            !declared.producer_state
        );
        assert_eq!(
            unsupported(store.retire_producer(&chain, OUT, "p", 1).await),
            !declared.producer_state
        );
        assert_eq!(
            unsupported(store.list_topics(&chain).await.map(drop)),
            !declared.topic_listing
        );
        let delete = store
            .delete_owner(DeleteOwnerRequest {
                namespace: chain.namespace.clone(),
                owner_kind: StreamOwnerKind::Workflow as i32,
                workflow_id: chain.workflow_id.clone(),
            })
            .await;
        assert_eq!(unsupported(delete.map(drop)), !declared.delete_owner);
        assert!(declared.max_record_bytes > 0 && declared.max_batch_bytes > 0);
    }

    /// Without producer state a repeat after a lost answer lands again, and readers drop it.
    pub(crate) async fn a_repeat_lands_a_copy_that_readers_drop(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        let records = [data("1"), data("2")];
        let request = producer.request(&records);
        let first = producer.send(&records).await.unwrap();
        let again = case.store().append(request).await.unwrap();
        assert_ne!(again.first_position, first.first_position);
        producer.append(&["3"]).await;
        let read = read(case, &chain, OUT, BEGINNING).await.unwrap();
        assert_eq!(values(&read), ["1", "2", "3"]);
    }

    /// Without producer state a divergent batch lands, and readers keep the first one.
    pub(crate) async fn a_divergent_batch_lands_and_readers_keep_the_first(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        let divergent = producer.request(&[data("other")]);
        producer.append(&["1"]).await;
        case.store().append(divergent).await.unwrap();
        let read = read(case, &chain, OUT, BEGINNING).await.unwrap();
        assert_eq!(values(&read), ["1"]);
    }

    /// Without close flags a close is a record, and every read ends at the first one.
    pub(crate) async fn a_close_record_ends_every_read(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        let landed = producer.append(&["1"]).await;
        close_topic(&*store, &chain, OUT, "closer", 1)
            .await
            .unwrap();
        close_topic(&*store, &chain, OUT, "late", 1).await.unwrap();
        // The store can't refuse what comes after the close, and no reader delivers it.
        producer.append(&["2"]).await;
        let first = read(case, &chain, OUT, BEGINNING).await.unwrap();
        assert_eq!(values(&first), ["1"]);
        assert_eq!(
            first.cursor,
            cursor(case, &chain, OUT, &landed.last_position)
        );
        let next = read_with(
            case,
            &chain,
            OUT,
            &first.cursor,
            Duration::ZERO,
            &first.state,
        )
        .await
        .unwrap();
        assert!(next.records.is_empty());
        let resumed = read(case, &chain, OUT, &first.cursor).await.unwrap();
        assert!(resumed.records.is_empty());
    }

    /// Without close flags a chain close writes a close on each topic it finds.
    pub(crate) async fn a_chain_close_reaches_every_topic(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        for topic in [OUT, OTHER] {
            let mut producer = Producer::new(case, &chain, topic, "p", 1);
            let request = producer.request(&[data(topic)]);
            append_batch(&*store, request).await.unwrap();
            producer.sequence += 1;
            producer.append(&["after"]).await;
        }
        close_chain(&*store, &chain, "@run", 1).await.unwrap();
        for topic in [OUT, OTHER] {
            let mut producer = Producer::new(case, &chain, topic, "q", 1);
            producer.append(&["late"]).await;
            let read = read(case, &chain, topic, BEGINNING).await.unwrap();
            assert_eq!(values(&read), [topic, "after"], "{topic}");
        }
        // A topic reader skips the names, so the control log reads as empty either way.
        let control = read(case, &chain, CHAIN_LOG, BEGINNING).await.unwrap();
        assert!(control.records.is_empty());
    }

    /// Without a kept last position a read whose cursor record retention dropped is expired.
    pub(crate) async fn a_lost_cursor_record_reads_as_expired(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        let first = producer.append(&["1"]).await;
        producer.append(&["2", "3"]).await;
        let at = cursor(case, &chain, OUT, &first.last_position);
        assert_eq!(
            values(&read(case, &chain, OUT, &at).await.unwrap()),
            ["2", "3"]
        );
        case.drop_oldest(&chain, OUT, 1).await;
        assert_eq!(
            kind(read(case, &chain, OUT, &at).await),
            StreamFailureKind::Expired
        );
    }
}

macro_rules! conformance {
    ($case:expr; $($name:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                let Some(case) = $case.await else { return };
                super::cases::$name(&case).await;
            }
        )*
    };
}

mod memory {
    conformance!(super::memory();
        append_read_roundtrip,
        a_batch_lands_in_order_and_names_its_last_record,
        an_empty_append_is_refused,
        an_append_without_a_digest_is_refused,
        latest_is_empty_on_an_empty_topic,
        a_cursor_resumes_strictly_after_its_record,
        latest_positions_a_reader_at_the_end,
        end_reads_only_what_arrives_after_the_read_starts,
        a_read_waits_no_longer_than_asked,
        dropping_a_waiting_read_releases_it,
        beginning_starts_at_the_oldest_record_still_held,
        a_reader_that_falls_behind_retention_is_told,
        an_empty_read_from_the_beginning_learns_of_a_trim,
        an_empty_read_from_the_beginning_with_nothing_trimmed_goes_on,
        rule_11_2_an_empty_read_goes_on_when_a_quiet_log_comes_back,
        a_retried_append_returns_the_original_positions,
        a_retry_with_other_ciphertext_still_deduplicates,
        a_divergent_retry_is_refused,
        a_sequence_below_the_newest_is_refused,
        rule_9_1_a_batch_inside_the_held_batch_is_refused,
        rule_64_1_a_gap_in_the_sequence_is_accepted,
        producers_and_attempts_dedupe_apart,
        a_new_attempt_supersedes_the_old_one,
        a_read_resumed_before_a_new_attempt_reports_it,
        a_record_that_does_not_parse_fails_with_its_cursor,
        finish_is_a_record_of_its_own,
        topics_and_chains_are_apart,
        a_cursor_from_another_stream_or_store_is_refused,
        a_closed_chain_refuses_new_batches_but_answers_a_repeat,
        a_closed_topic_leaves_the_others_open,
        a_closed_topic_answers_a_repeat_and_ends_reads,
        a_refused_append_leaves_no_trace_of_its_topic,
        retiring_a_producer_drops_only_its_dedupe_state,
        deleting_an_owner_drops_every_chain_of_it,
        the_declared_capabilities_match_the_calls,
        a_lost_cursor_record_reads_as_expired,
    );
}

/// Every case but owner deletes, which need a rule of their own under the documented ACL.
#[cfg(feature = "redis")]
macro_rules! redis_cases {
    ($case:expr) => {
        conformance!($case;
            append_read_roundtrip,
            a_batch_lands_in_order_and_names_its_last_record,
            an_empty_append_is_refused,
            an_append_without_a_digest_is_refused,
            latest_is_empty_on_an_empty_topic,
            a_cursor_resumes_strictly_after_its_record,
            latest_positions_a_reader_at_the_end,
            end_reads_only_what_arrives_after_the_read_starts,
            a_read_waits_no_longer_than_asked,
            dropping_a_waiting_read_releases_it,
            beginning_starts_at_the_oldest_record_still_held,
            a_reader_that_falls_behind_retention_is_told,
            an_empty_read_from_the_beginning_learns_of_a_trim,
            an_empty_read_from_the_beginning_with_nothing_trimmed_goes_on,
            rule_11_2_an_empty_read_goes_on_when_a_quiet_log_comes_back,
            a_refused_append_leaves_no_trace_of_its_topic,
            a_retried_append_returns_the_original_positions,
            a_retry_with_other_ciphertext_still_deduplicates,
            a_divergent_retry_is_refused,
            a_sequence_below_the_newest_is_refused,
            rule_9_1_a_batch_inside_the_held_batch_is_refused,
            rule_64_1_a_gap_in_the_sequence_is_accepted,
            producers_and_attempts_dedupe_apart,
            a_new_attempt_supersedes_the_old_one,
            a_read_resumed_before_a_new_attempt_reports_it,
            a_record_that_does_not_parse_fails_with_its_cursor,
            finish_is_a_record_of_its_own,
            topics_and_chains_are_apart,
            a_cursor_from_another_stream_or_store_is_refused,
            a_closed_chain_refuses_new_batches_but_answers_a_repeat,
            a_closed_topic_leaves_the_others_open,
            a_closed_topic_answers_a_repeat_and_ends_reads,
            retiring_a_producer_drops_only_its_dedupe_state,
            the_declared_capabilities_match_the_calls,
            a_lost_cursor_record_reads_as_expired,
        );
    };
}

#[cfg(feature = "redis")]
mod standalone_redis {
    redis_cases!(super::on_redis::redis("STREAMS_REDIS_URL", false));
    conformance!(super::on_redis::redis("STREAMS_REDIS_URL", false);
        deleting_an_owner_drops_every_chain_of_it,
    );
}

#[cfg(feature = "redis")]
mod cluster_redis {
    redis_cases!(super::on_redis::redis("STREAMS_REDIS_CLUSTER_URL", true));
    // A Workflow id's chains hash to different slots, so the delete reaches every primary.
    conformance!(super::on_redis::redis("STREAMS_REDIS_CLUSTER_URL", true);
        deleting_an_owner_drops_every_chain_of_it,
    );
}

#[cfg(feature = "redis")]
mod documented_acl_redis {
    redis_cases!(super::on_redis::redis_as_documented_user(
        "STREAMS_REDIS_URL",
        false,
        &[]
    ));
    // The page names the one rule a delete needs beyond the others.
    conformance!(super::on_redis::redis_as_documented_user("STREAMS_REDIS_URL", false, &["+scan"]);
        deleting_an_owner_drops_every_chain_of_it,
    );
}

#[cfg(feature = "redis")]
mod documented_acl_cluster_redis {
    // A cluster client asks the topology first, so the documented rules must allow it.
    redis_cases!(super::on_redis::redis_as_documented_user(
        "STREAMS_REDIS_CLUSTER_URL",
        true,
        &[]
    ));
    conformance!(super::on_redis::redis_as_documented_user(
        "STREAMS_REDIS_CLUSTER_URL",
        true,
        &["+scan"]
    );
        deleting_an_owner_drops_every_chain_of_it,
    );
}

/// The cases every store passes, whatever it declares beyond the log. A cursor whose record
/// retention dropped resumes only with a kept last position or dense positions, so that case
/// runs apart.
macro_rules! required {
    ($case:expr) => {
        conformance!($case;
            append_read_roundtrip,
            a_batch_lands_in_order_and_names_its_last_record,
            an_empty_append_is_refused,
            an_append_without_a_digest_is_refused,
            latest_is_empty_on_an_empty_topic,
            a_cursor_resumes_strictly_after_its_record,
            latest_positions_a_reader_at_the_end,
            end_reads_only_what_arrives_after_the_read_starts,
            a_read_waits_no_longer_than_asked,
            dropping_a_waiting_read_releases_it,
            a_reader_that_falls_behind_retention_is_told,
            an_empty_read_from_the_beginning_with_nothing_trimmed_goes_on,
            rule_64_1_a_gap_in_the_sequence_is_accepted,
            producers_and_attempts_dedupe_apart,
            a_new_attempt_supersedes_the_old_one,
            a_read_resumed_before_a_new_attempt_reports_it,
            a_record_that_does_not_parse_fails_with_its_cursor,
            finish_is_a_record_of_its_own,
            topics_and_chains_are_apart,
            a_cursor_from_another_stream_or_store_is_refused,
            the_declared_capabilities_match_the_calls,
            a_lost_cursor_record_reads_as_expired,
        );
    };
}

/// The cases of a store with no producer state, where closes are records.
macro_rules! without_producer_state {
    ($case:expr) => {
        conformance!($case;
            a_repeat_lands_a_copy_that_readers_drop,
            a_divergent_batch_lands_and_readers_keep_the_first,
            a_close_record_ends_every_read,
            a_chain_close_reaches_every_topic,
        );
    };
}

/// A store with only the log, so every fallback runs.
mod memory_log_only {
    async fn case() -> Option<super::Memory> {
        super::masked(|c| {
            *c = temporalio_streams::Capabilities {
                dense_positions: true,
                ..temporalio_streams::Capabilities::log_only()
            }
        })
        .await
    }
    required!(case());
    // Dense positions tell a page right after a dropped cursor is whole.
    conformance!(case(); beginning_starts_at_the_oldest_record_still_held);
    without_producer_state!(case());
}

mod memory_without_producer_state {
    async fn case() -> Option<super::Memory> {
        super::masked(|c| c.producer_state = false).await
    }
    required!(case());
    // Dense positions tell a page right after a dropped cursor is whole.
    conformance!(case(); beginning_starts_at_the_oldest_record_still_held);
    without_producer_state!(case());
    conformance!(case();
        an_empty_read_from_the_beginning_learns_of_a_trim,
        rule_11_2_an_empty_read_goes_on_when_a_quiet_log_comes_back,
        deleting_an_owner_drops_every_chain_of_it,
    );
}

/// No producer state and no topic listing, so a chain close finds topics in the control log.
mod memory_without_listing {
    async fn case() -> Option<super::Memory> {
        super::masked(|c| {
            c.producer_state = false;
            c.topic_listing = false;
        })
        .await
    }
    required!(case());
    // Dense positions tell a page right after a dropped cursor is whole.
    conformance!(case(); beginning_starts_at_the_oldest_record_still_held);
    without_producer_state!(case());
}

mod memory_without_kept_last {
    async fn case() -> Option<super::Memory> {
        super::masked(|c| c.kept_last = false).await
    }
    required!(case());
    // Dense positions tell a page right after a dropped cursor is whole.
    conformance!(case(); beginning_starts_at_the_oldest_record_still_held);
    conformance!(case();
        a_retried_append_returns_the_original_positions,
        a_retry_with_other_ciphertext_still_deduplicates,
        a_divergent_retry_is_refused,
        a_sequence_below_the_newest_is_refused,
        rule_9_1_a_batch_inside_the_held_batch_is_refused,
        a_closed_chain_refuses_new_batches_but_answers_a_repeat,
        a_closed_topic_leaves_the_others_open,
        a_closed_topic_answers_a_repeat_and_ends_reads,
        retiring_a_producer_drops_only_its_dedupe_state,
    );
}

#[cfg(feature = "redis")]
mod plain_redis {
    required!(super::on_redis::plain_redis("STREAMS_REDIS_URL", false));
    without_producer_state!(super::on_redis::plain_redis("STREAMS_REDIS_URL", false));
}

#[cfg(feature = "redis")]
mod plain_cluster_redis {
    required!(super::on_redis::plain_redis(
        "STREAMS_REDIS_CLUSTER_URL",
        true
    ));
    without_producer_state!(super::on_redis::plain_redis(
        "STREAMS_REDIS_CLUSTER_URL",
        true
    ));
}
