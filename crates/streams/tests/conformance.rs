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
    BEGINNING, CHAIN_LOG, Capabilities, END, LocalFileArchive, MemoryStore, NotificationEntry,
    NotificationLog, Notifier, NotifierClient, NotifyError, NotifyingStore, ReadTarget,
    SegmentLimits, StreamArchive, StreamNotification, StreamResult, StreamStore, TieredStore,
    append_batch, archive_topic, chain_owner, close_chain, close_topic, mint_cursor, proto::*,
    read_page, stored_append_record, stream_hash,
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
            ..Default::default()
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

async fn memory_with_log() -> Option<Memory> {
    Some(Memory(Arc::new(MemoryStore::with_notification_log())))
}

/// Takes every notification and sends nothing.
struct Quiet;

#[async_trait::async_trait]
impl NotifierClient for Quiet {
    async fn notify(&self, _: StreamNotification) -> Result<(), NotifyError> {
        Ok(())
    }
}

/// Another case's store behind `NotifyingStore`, which every process with a client runs, so a
/// method the wrapper doesn't forward fails the contract.
struct Notifying<C> {
    inner: C,
    store: Arc<dyn StreamStore>,
}

async fn notifying<C: Case>(
    inner: impl std::future::Future<Output = Option<C>>,
) -> Option<Notifying<C>> {
    let inner = inner.await?;
    let store = Arc::new(NotifyingStore::new(
        inner.store(),
        Arc::new(Notifier::new(Arc::new(Quiet), 10)),
    ));
    Some(Notifying { inner, store })
}

#[async_trait::async_trait]
impl<C: Case> Case for Notifying<C> {
    fn store(&self) -> Arc<dyn StreamStore> {
        self.store.clone()
    }

    fn chain(&self) -> ChainId {
        self.inner.chain()
    }

    async fn drop_oldest(&self, chain: &ChainId, topic: &str, keep: usize) {
        self.inner.drop_oldest(chain, topic, keep).await;
    }

    async fn expire_log(&self, chain: &ChainId, topic: &str) {
        self.inner.expire_log(chain, topic).await;
    }
}

/// A hot store that can forget a topic whole, as when its last key expired, meta included.
#[async_trait::async_trait]
trait Forgets: Case {
    async fn forget(&self, chain: &ChainId, topic: &str);

    /// Ends the archive's hold on a topic at once, as a lapsed lease does.
    async fn lapse_archive_hold(&self, chain: &ChainId, topic: &str) {
        self.store()
            .release_floor(chain, topic, temporalio_streams::ARCHIVE_HOLDER)
            .await
            .unwrap();
    }
}

#[async_trait::async_trait]
impl Forgets for Memory {
    async fn forget(&self, chain: &ChainId, topic: &str) {
        self.0.forget(chain, topic);
    }
}

/// Another case's store as the hot store of a [TieredStore], with an archive in a directory of
/// its own. With a hot store that keeps no archive hold, nothing is archived, so the contract
/// holds as it does on the hot store alone.
struct Tiered<C> {
    inner: C,
    store: Arc<TieredStore>,
    /// The directory of a local archive, which the case deletes at its end.
    root: Option<std::path::PathBuf>,
}

async fn tiered<C: Case>(inner: impl std::future::Future<Output = Option<C>>) -> Option<Tiered<C>> {
    let inner = inner.await?;
    let root = std::env::temp_dir().join(unique("streams-archive"));
    let store = Arc::new(TieredStore::new(
        inner.store(),
        Arc::new(LocalFileArchive::new(&root)),
    ));
    Some(Tiered {
        inner,
        store,
        root: Some(root),
    })
}

/// [tiered] with the archive under a prefix of its own at `STREAMS_S3_URL`.
#[cfg(feature = "archive")]
async fn tiered_on_s3<C: Case>(
    inner: impl std::future::Future<Output = Option<C>>,
) -> Option<Tiered<C>> {
    let Ok(url) = std::env::var("STREAMS_S3_URL") else {
        eprintln!("set STREAMS_S3_URL to run the S3 archive cases");
        return None;
    };
    let inner = inner.await?;
    let url = format!("{}/{}", url.trim_end_matches('/'), unique("conformance"));
    let archive = temporalio_streams::ObjectStoreArchive::from_s3_url(&url).unwrap();
    let store = Arc::new(TieredStore::new(inner.store(), Arc::new(archive)));
    Some(Tiered {
        inner,
        store,
        root: None,
    })
}

impl<C> Drop for Tiered<C> {
    fn drop(&mut self) {
        if let Some(root) = &self.root {
            let _ = std::fs::remove_dir_all(root);
        }
    }
}

#[async_trait::async_trait]
impl<C: Case> Case for Tiered<C> {
    fn store(&self) -> Arc<dyn StreamStore> {
        self.store.clone()
    }

    fn chain(&self) -> ChainId {
        self.inner.chain()
    }

    async fn drop_oldest(&self, chain: &ChainId, topic: &str, keep: usize) {
        self.inner.drop_oldest(chain, topic, keep).await;
    }

    async fn expire_log(&self, chain: &ChainId, topic: &str) {
        self.inner.expire_log(chain, topic).await;
    }
}

/// A tiered store whose hot store keeps an archive hold, for the archive cases.
#[async_trait::async_trait]
trait ArchiveCase: Case {
    fn hot(&self) -> Arc<dyn StreamStore>;

    fn archive(&self) -> Arc<dyn StreamArchive>;

    /// Forgets a topic in the hot store, so only the archive holds it.
    async fn forget(&self, chain: &ChainId, topic: &str);

    /// Deletes one archive object, as a bucket lifecycle rule does.
    async fn lose(&self, name: &str);

    /// Ends the archive's hold on a topic at once, as a lapsed lease does.
    async fn lapse_archive_hold(&self, chain: &ChainId, topic: &str);

    /// Archives what the hot store holds of a topic, in segments of at most `records` records.
    async fn archive_now(&self, chain: &ChainId, topic: &str, records: u32) {
        let limits = SegmentLimits {
            records,
            ..Default::default()
        };
        archive_topic(&*self.hot(), &*self.archive(), chain, topic, limits, true)
            .await
            .unwrap();
    }
}

#[async_trait::async_trait]
impl<C: Forgets> ArchiveCase for Tiered<C> {
    fn hot(&self) -> Arc<dyn StreamStore> {
        self.inner.store()
    }

    fn archive(&self) -> Arc<dyn StreamArchive> {
        self.store.archive().clone()
    }

    async fn forget(&self, chain: &ChainId, topic: &str) {
        self.inner.forget(chain, topic).await;
    }

    async fn lapse_archive_hold(&self, chain: &ChainId, topic: &str) {
        self.inner.lapse_archive_hold(chain, topic).await;
    }

    async fn lose(&self, name: &str) {
        match &self.root {
            Some(root) => std::fs::remove_file(root.join(name)).unwrap(),
            None => assert_eq!(self.archive().delete_prefix(name).await.unwrap(), 1),
        }
    }
}

#[async_trait::async_trait]
impl<C: Forgets> Forgets for Activity<C> {
    async fn forget(&self, chain: &ChainId, topic: &str) {
        self.0.forget(chain, topic).await;
    }

    async fn lapse_archive_hold(&self, chain: &ChainId, topic: &str) {
        self.0.lapse_archive_hold(chain, topic).await;
    }
}

#[async_trait::async_trait]
impl<C: Forgets> Forgets for WorkflowActivity<C> {
    async fn forget(&self, chain: &ChainId, topic: &str) {
        self.0.forget(chain, topic).await;
    }

    async fn lapse_archive_hold(&self, chain: &ChainId, topic: &str) {
        self.0.lapse_archive_hold(chain, topic).await;
    }
}

async fn memory_holding_for_archive() -> Option<Memory> {
    Some(Memory(Arc::new(MemoryStore::new().holding_for_archive())))
}

/// Another case whose chains are standalone Activity runs, so every case runs on the Activity
/// keys too.
struct Activity<C>(C);

async fn activity<C: Case>(
    inner: impl std::future::Future<Output = Option<C>>,
) -> Option<Activity<C>> {
    Some(Activity(inner.await?))
}

/// The chain of standalone Activity run `run_id` of `activity_id`.
fn activity_chain(namespace: &str, activity_id: &str, run_id: &str) -> ChainId {
    ChainId {
        namespace: namespace.to_string(),
        first_run_id: run_id.to_string(),
        owner_kind: StreamOwnerKind::Activity as i32,
        activity_id: activity_id.to_string(),
        ..Default::default()
    }
}

#[async_trait::async_trait]
impl<C: Case> Case for Activity<C> {
    fn store(&self) -> Arc<dyn StreamStore> {
        self.0.store()
    }

    fn chain(&self) -> ChainId {
        let chain = self.0.chain();
        activity_chain(&chain.namespace, &chain.workflow_id, &chain.first_run_id)
    }

    async fn drop_oldest(&self, chain: &ChainId, topic: &str, keep: usize) {
        self.0.drop_oldest(chain, topic, keep).await;
    }

    async fn expire_log(&self, chain: &ChainId, topic: &str) {
        self.0.expire_log(chain, topic).await;
    }
}

/// Another case whose chains are Workflow Activities, each named by a Workflow, its scheduling
/// run and an Activity id.
struct WorkflowActivity<C>(C);

async fn workflow_activity<C: Case>(
    inner: impl std::future::Future<Output = Option<C>>,
) -> Option<WorkflowActivity<C>> {
    Some(WorkflowActivity(inner.await?))
}

/// The chain of Activity `activity_id` that run `run_id` of `workflow_id` scheduled.
fn workflow_activity_chain(
    namespace: &str,
    workflow_id: &str,
    run_id: &str,
    activity_id: &str,
) -> ChainId {
    ChainId {
        namespace: namespace.to_string(),
        workflow_id: workflow_id.to_string(),
        first_run_id: run_id.to_string(),
        owner_kind: StreamOwnerKind::WorkflowActivity as i32,
        activity_id: activity_id.to_string(),
    }
}

#[async_trait::async_trait]
impl<C: Case> Case for WorkflowActivity<C> {
    fn store(&self) -> Arc<dyn StreamStore> {
        self.0.store()
    }

    fn chain(&self) -> ChainId {
        let chain = self.0.chain();
        // SDK default Activity ids repeat in every run, so each chain reuses `1`.
        workflow_activity_chain(
            &chain.namespace,
            &chain.workflow_id,
            &chain.first_run_id,
            "1",
        )
    }

    async fn drop_oldest(&self, chain: &ChainId, topic: &str, keep: usize) {
        self.0.drop_oldest(chain, topic, keep).await;
    }

    async fn expire_log(&self, chain: &ChainId, topic: &str) {
        self.0.expire_log(chain, topic).await;
    }
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
        let run = part(&chain.first_run_id);
        let owner = match StreamOwnerKind::try_from(chain.owner_kind) {
            Ok(StreamOwnerKind::Activity) => format!("!a:{}:{run}", part(&chain.activity_id)),
            Ok(StreamOwnerKind::WorkflowActivity) => format!(
                "!wa:{}:{run}:{}",
                part(&chain.workflow_id),
                part(&chain.activity_id)
            ),
            _ => format!("{}:{run}", part(&chain.workflow_id)),
        };
        format!(
            "{}:{{{}:{owner}}}:t:{}",
            part(prefix),
            part(&chain.namespace),
            part(topic)
        )
    }

    fn now_ms() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    }

    /// A stream id as its two numbers, so ids compare in log order.
    fn stream_id(id: &str) -> (u64, u64) {
        let (ms, seq) = id.split_once('-').unwrap_or((id, "0"));
        (ms.parse().unwrap(), seq.parse().unwrap())
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

    /// The rules of the MemoryDB access string on the managed Redis page.
    pub(crate) fn memorydb_rules() -> Vec<String> {
        let page = include_str!("../docs/managed-redis.md");
        let lines: Vec<_> = page
            .lines()
            .filter(|line| line.starts_with("on ~"))
            .collect();
        assert_eq!(
            lines.len(),
            1,
            "the managed Redis page should give one access string"
        );
        lines[0].split_whitespace().map(str::to_string).collect()
    }

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
            Self::set(url, name, password, &setuser).await
        }

        /// A user with exactly the MemoryDB access string, for one prefix and namespace, and a
        /// password, which MemoryDB keeps apart from the string.
        pub(crate) async fn create_memorydb(url: &str, prefix: &str, namespace: &str) -> Self {
            let rules = memorydb_rules();
            assert!(
                rules.iter().any(|rule| rule == DOCUMENTED_KEYS),
                "{rules:?}"
            );
            let name = unique("streams-memorydb");
            let password = unique("pw");
            let mut setuser = redis::cmd("ACL");
            setuser
                .arg("SETUSER")
                .arg(&name)
                .arg(format!(">{password}"));
            for rule in &rules {
                if rule == DOCUMENTED_KEYS {
                    setuser.arg(format!("~{}:{{{}:*", part(prefix), part(namespace)));
                } else if let Some(rest) = rule.strip_prefix("~temporal-streams:{*}") {
                    setuser.arg(format!("~{}:{{*}}{rest}", part(prefix)));
                } else {
                    setuser.arg(rule);
                }
            }
            Self::set(url, name, password, &setuser).await
        }

        async fn set(url: &str, name: String, password: String, setuser: &redis::Cmd) -> Self {
            // An ACL user lives on one node, so a cluster needs it on every node.
            let admins = nodes(url).await;
            for admin in &admins {
                let mut admin = Raw::connect(admin, false).await;
                let _: () = admin.query(setuser).await;
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
        redis_as_documented_user_with(variable, cluster, extra, false).await
    }

    /// [redis_as_documented_user] with the notification log on, and the documented rules for
    /// its keys.
    pub(crate) async fn redis_as_documented_user_with_log(
        variable: &str,
        cluster: bool,
    ) -> Option<Redis> {
        redis_as_documented_user_with(variable, cluster, &[], true).await
    }

    async fn redis_as_documented_user_with(
        variable: &str,
        cluster: bool,
        extra: &[&str],
        notification_log: bool,
    ) -> Option<Redis> {
        let Ok(url) = std::env::var(variable) else {
            eprintln!("set {variable} to run the Redis store cases");
            return None;
        };
        let prefix = unique("conformance-acl");
        let log_rules = [
            format!("~{}:{{*}}:n", part(&prefix)),
            format!("%W~{}:{{*}}:bell", part(&prefix)),
        ];
        let mut rules: Vec<&str> = extra.to_vec();
        if notification_log {
            rules.extend(log_rules.iter().map(String::as_str));
        }
        let user = DocumentedUser::create(&url, &prefix, "default", &rules).await;
        let mut options = RedisStoreOptions::new(user.url.clone());
        options.key_prefix = prefix.clone();
        options.cluster = cluster;
        options.notification_log = notification_log;
        Some(Redis {
            store: Arc::new(RedisStore::connect(options).await.unwrap()),
            raw: Raw::connect(&url, cluster).await,
            prefix,
            _user: Some(user),
        })
    }

    /// A store with the notification log and an archive, as a user with the documented rules,
    /// the log's and the archive's. A driver reads the archive marks under the same user.
    pub(crate) async fn redis_as_documented_user_with_archive(
        variable: &str,
        cluster: bool,
    ) -> Option<Redis> {
        let Ok(url) = std::env::var(variable) else {
            eprintln!("set {variable} to run the Redis store cases");
            return None;
        };
        let page = include_str!("../docs/redis.md");
        for rules in [
            "`~temporal-streams:{*}:a +zadd +hexists`",
            "`+zrange +zrem`",
        ] {
            assert!(page.contains(rules), "the Redis page names {rules}");
        }
        let prefix = unique("conformance-acl-archive");
        let rules = [
            format!("~{}:{{*}}:n", part(&prefix)),
            format!("%W~{}:{{*}}:bell", part(&prefix)),
            format!("~{}:{{*}}:a", part(&prefix)),
            "+zadd".to_string(),
            "+hexists".to_string(),
            "+zrange".to_string(),
            "+zrem".to_string(),
            // The page names it as the one rule an owner delete needs beyond the others.
            "+scan".to_string(),
        ];
        let rules: Vec<&str> = rules.iter().map(String::as_str).collect();
        let user = DocumentedUser::create(&url, &prefix, "default", &rules).await;
        let mut options = RedisStoreOptions::new(user.url.clone());
        options.key_prefix = prefix.clone();
        options.cluster = cluster;
        options.notification_log = true;
        options.archive_lease = Some(Duration::from_secs(7 * 24 * 60 * 60));
        Some(Redis {
            store: Arc::new(RedisStore::connect(options).await.unwrap()),
            raw: Raw::connect(&url, cluster).await,
            prefix,
            _user: Some(user),
        })
    }

    /// A store with the notification log on, as a user with exactly the MemoryDB access string.
    pub(crate) async fn redis_as_memorydb_user(variable: &str, cluster: bool) -> Option<Redis> {
        let Ok(url) = std::env::var(variable) else {
            eprintln!("set {variable} to run the Redis store cases");
            return None;
        };
        let prefix = unique("conformance-memorydb");
        let user = DocumentedUser::create_memorydb(&url, &prefix, "default").await;
        let mut options = RedisStoreOptions::new(user.url.clone());
        options.key_prefix = prefix.clone();
        options.cluster = cluster;
        options.notification_log = true;
        Some(Redis {
            store: Arc::new(RedisStore::connect(options).await.unwrap()),
            raw: Raw::connect(&url, cluster).await,
            prefix,
            _user: Some(user),
        })
    }

    pub(crate) async fn redis(variable: &str, cluster: bool) -> Option<Redis> {
        redis_with(variable, cluster, false, false).await
    }

    /// [redis] with a notification log.
    pub(crate) async fn redis_with_log(variable: &str, cluster: bool) -> Option<Redis> {
        redis_with(variable, cluster, true, false).await
    }

    /// [redis_with_log] with an archive hold, for a tiered store.
    pub(crate) async fn redis_with_archive(variable: &str, cluster: bool) -> Option<Redis> {
        let Ok(url) = std::env::var(variable) else {
            eprintln!("set {variable} to run the Redis store cases");
            return None;
        };
        let prefix = unique("conformance-archive");
        let mut options = RedisStoreOptions::new(url.clone());
        options.cluster = cluster;
        options.key_prefix = prefix.clone();
        options.notification_log = true;
        options.archive_lease = Some(Duration::from_secs(7 * 24 * 60 * 60));
        Some(Redis {
            store: Arc::new(RedisStore::connect(options).await.unwrap()),
            raw: Raw::connect(&url, cluster).await,
            prefix,
            _user: None,
        })
    }

    #[async_trait::async_trait]
    impl Forgets for Redis {
        async fn forget(&self, chain: &ChainId, topic: &str) {
            let mut raw = self.raw.clone();
            let log = log_key(&self.prefix, chain, topic);
            for key in [format!("{log}:meta"), format!("{log}:floors"), log] {
                let _: i64 = raw.query(redis::cmd("DEL").arg(key)).await;
            }
        }

        async fn lapse_archive_hold(&self, chain: &ChainId, topic: &str) {
            let floors = format!("{}:floors", log_key(&self.prefix, chain, topic));
            let _: i64 = self
                .raw
                .clone()
                .query(
                    redis::cmd("HDEL")
                        .arg(floors)
                        .arg(temporalio_streams::ARCHIVE_HOLDER),
                )
                .await;
        }
    }

    /// Redis with no Lua, so the store keeps no producer state and no last position.
    pub(crate) async fn plain_redis(variable: &str, cluster: bool) -> Option<Redis> {
        redis_with(variable, cluster, false, true).await
    }

    async fn redis_with(
        variable: &str,
        cluster: bool,
        notification_log: bool,
        plain: bool,
    ) -> Option<Redis> {
        let Ok(url) = std::env::var(variable) else {
            eprintln!("set {variable} to run the Redis store cases");
            return None;
        };
        // A prefix per case keeps cases apart in one Redis, its notification streams too.
        let prefix = unique("conformance");
        let mut options = RedisStoreOptions::new(url.clone());
        options.cluster = cluster;
        options.key_prefix = prefix.clone();
        options.notification_log = notification_log;
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
                ..Default::default()
            }
        }

        async fn drop_oldest(&self, chain: &ChainId, topic: &str, keep: usize) {
            // Trims as the append script does, watermark included, and keeps what a live hold
            // holds, as `keep.lua` does.
            let mut raw = self.raw.clone();
            let log = log_key(&self.prefix, chain, topic);
            let entries: Vec<(String, Value)> = raw
                .query(redis::cmd("XRANGE").arg(&log).arg("-").arg("+"))
                .await;
            let floors: Vec<String> = raw
                .query(redis::cmd("HVALS").arg(format!("{log}:floors")))
                .await;
            let held = floors
                .iter()
                .filter_map(|floor| {
                    let (id, until) = floor.split_once('|')?;
                    (until.parse::<u128>().ok()? > now_ms()).then(|| stream_id(id))
                })
                .min();
            let below_hold = entries
                .iter()
                .take_while(|(id, _)| held.is_none_or(|held| stream_id(id) < held))
                .count();
            let doomed = entries.len().saturating_sub(keep).min(below_hold);
            if doomed == 0 {
                return;
            }
            let _: i64 = raw
                .query(
                    redis::cmd("XTRIM")
                        .arg(&log)
                        .arg("MAXLEN")
                        .arg(entries.len() - doomed),
                )
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

#[cfg(feature = "postgres")]
mod on_postgres {
    use super::*;
    use temporalio_streams::{PostgresStore, PostgresStoreOptions};

    /// A schema name no other case uses.
    pub(crate) fn schema() -> String {
        unique("conformance").replace('-', "_")
    }

    /// A connection of the test's own, to do to the rows what retention does.
    pub(crate) async fn raw(url: &str) -> tokio_postgres::Client {
        let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        client
    }

    pub(crate) fn options(url: &str, schema: &str) -> PostgresStoreOptions {
        let mut options = PostgresStoreOptions::new(url);
        options.schema = schema.to_string();
        options.max_connections = 4;
        options
    }

    pub(crate) struct Postgres {
        pub(crate) store: Arc<PostgresStore>,
        pub(crate) raw: Arc<tokio_postgres::Client>,
        pub(crate) schema: String,
    }

    pub(crate) async fn postgres(variable: &str) -> Option<Postgres> {
        postgres_with(variable, |_| {}).await
    }

    /// [postgres] with a notification log.
    pub(crate) async fn postgres_with_log(variable: &str) -> Option<Postgres> {
        postgres_with(variable, |options| {
            options.notification_log = true;
            options.notify_doorbells = 4;
        })
        .await
    }

    /// [postgres_with_log] with an archive hold, for a tiered store.
    pub(crate) async fn postgres_with_archive(variable: &str) -> Option<Postgres> {
        postgres_with(variable, |options| {
            options.notification_log = true;
            options.notify_doorbells = 4;
            options.archive_lease = Some(Duration::from_secs(60));
        })
        .await
    }

    /// [postgres] with a `NOTIFY` per append for waiting reads.
    pub(crate) async fn postgres_notifying_reads(variable: &str) -> Option<Postgres> {
        postgres_with(variable, |options| options.notify_reads = true).await
    }

    pub(crate) async fn postgres_with(
        variable: &str,
        change: impl FnOnce(&mut PostgresStoreOptions),
    ) -> Option<Postgres> {
        let Ok(url) = std::env::var(variable) else {
            eprintln!("set {variable} to run the PostgreSQL store cases");
            return None;
        };
        let schema = schema();
        let mut options = options(&url, &schema);
        change(&mut options);
        Some(Postgres {
            store: Arc::new(PostgresStore::connect(options).await.unwrap()),
            raw: Arc::new(raw(&url).await),
            schema,
        })
    }

    /// The store through PgBouncer in transaction mode at `STREAMS_POSTGRES_PGBOUNCER_URL`, with
    /// the log, and the listener and the migrations on the direct `STREAMS_POSTGRES_URL`.
    pub(crate) async fn postgres_through_pgbouncer() -> Option<Postgres> {
        let Ok(pooled) = std::env::var("STREAMS_POSTGRES_PGBOUNCER_URL") else {
            eprintln!("set STREAMS_POSTGRES_PGBOUNCER_URL to run the PgBouncer cases");
            return None;
        };
        let direct = std::env::var("STREAMS_POSTGRES_URL").unwrap();
        let schema = schema();
        let mut options = options(&pooled, &schema);
        options.listen_url = Some(direct.clone());
        options.notification_log = true;
        options.notify_doorbells = 4;
        Some(Postgres {
            store: Arc::new(PostgresStore::connect(options).await.unwrap()),
            raw: Arc::new(raw(&direct).await),
            schema,
        })
    }

    /// The role lines of the PostgreSQL page: one `CREATE ROLE` and the `GRANT`s after it.
    pub(crate) fn documented_lines() -> Vec<String> {
        let page = include_str!("../docs/postgres.md");
        let lines: Vec<String> = page
            .lines()
            .filter(|line| line.starts_with("CREATE ROLE ") || line.starts_with("GRANT "))
            .map(str::to_string)
            .collect();
        assert_eq!(
            lines
                .iter()
                .filter(|line| line.starts_with("CREATE ROLE "))
                .count(),
            1,
            "the PostgreSQL page should give one CREATE ROLE line"
        );
        lines
    }

    /// A store as a role with exactly the documented grants, on a schema an owner migrated. The
    /// page's schema, role and password are swapped for the test's own, and nothing else changes.
    pub(crate) async fn postgres_as_documented_role(variable: &str) -> Option<Postgres> {
        postgres_as_documented_role_with(variable, false).await
    }

    /// [postgres_as_documented_role] with a notification log.
    pub(crate) async fn postgres_as_documented_role_with_log(variable: &str) -> Option<Postgres> {
        postgres_as_documented_role_with(variable, true).await
    }

    async fn postgres_as_documented_role_with(variable: &str, log: bool) -> Option<Postgres> {
        let Ok(url) = std::env::var(variable) else {
            eprintln!("set {variable} to run the PostgreSQL store cases");
            return None;
        };
        let schema = schema();
        drop(
            PostgresStore::connect(options(&url, &schema))
                .await
                .unwrap(),
        );
        let raw = raw(&url).await;
        let role = format!("{schema}_app");
        let password = format!("pw_{schema}");
        for line in documented_lines() {
            let line = line
                .replace("temporal_streams", &schema)
                .replace("streams_app", &role)
                .replace("'secret'", &format!("'{password}'"));
            raw.batch_execute(&line).await.unwrap();
        }
        let (_, address) = url.split_once('@').unwrap();
        let role_url = format!("postgres://{role}:{password}@{address}");
        let mut options = options(&role_url, &schema);
        options.skip_migrations = true;
        options.notification_log = log;
        Some(Postgres {
            store: Arc::new(PostgresStore::connect(options).await.unwrap()),
            raw: Arc::new(raw),
            schema,
        })
    }

    impl Postgres {
        fn key(chain: &ChainId) -> (String, i16, String, String, String) {
            let (kind, owner, activity) = match StreamOwnerKind::try_from(chain.owner_kind) {
                Ok(StreamOwnerKind::Activity) => (2, chain.activity_id.clone(), String::new()),
                Ok(StreamOwnerKind::WorkflowActivity) => {
                    (3, chain.workflow_id.clone(), chain.activity_id.clone())
                }
                _ => (1, chain.workflow_id.clone(), String::new()),
            };
            (
                chain.namespace.clone(),
                kind,
                owner,
                chain.first_run_id.clone(),
                activity,
            )
        }

        /// Runs `sql` with the chain key as `$1` to `$5` and the topic as `$6`.
        pub(crate) async fn on_topic(
            &self,
            sql: &str,
            chain: &ChainId,
            topic: &str,
            more: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        ) -> u64 {
            let (namespace, kind, owner, run, activity) = Self::key(chain);
            let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                vec![&namespace, &kind, &owner, &run, &activity, &topic];
            params.extend_from_slice(more);
            let sql = sql.replace("{s}", &self.schema);
            self.raw.execute(&sql, &params).await.unwrap()
        }
    }

    /// The topic row of `$1` to `$6`.
    const TOPIC: &str = "SELECT t.topic_id, t.last_position FROM {s}.chains c \
        JOIN {s}.topics t ON t.chain_id = c.chain_id AND t.topic = $6 \
        WHERE (c.namespace, c.owner_kind, c.owner_id, c.first_run_id, c.activity_id) \
        = ($1, $2, $3, $4, $5)";

    #[async_trait::async_trait]
    impl Case for Postgres {
        fn store(&self) -> Arc<dyn StreamStore> {
            self.store.clone()
        }

        fn chain(&self) -> ChainId {
            ChainId {
                namespace: "default".to_string(),
                workflow_id: unique("conformance"),
                first_run_id: unique("run"),
                ..Default::default()
            }
        }

        async fn drop_oldest(&self, chain: &ChainId, topic: &str, keep: usize) {
            // Deletes as the janitor does, keeping what a live hold holds, and moves `trimmed`.
            let keep = keep as i64;
            self.on_topic(
                &format!(
                    "WITH t AS ({TOPIC}), \
                     cut AS (SELECT t.topic_id, CASE WHEN n.position IS NULL THEN NULL \
                       ELSE least(n.position, (SELECT min(f.position) - 1 FROM {{s}}.floors f \
                         WHERE f.topic_id = t.topic_id AND f.until > now())) END AS position \
                       FROM t LEFT JOIN LATERAL (SELECT r.position FROM {{s}}.records r \
                         WHERE r.topic_id = t.topic_id ORDER BY r.position DESC \
                         OFFSET $7 LIMIT 1) n ON true), \
                     gone AS (DELETE FROM {{s}}.records r USING cut \
                       WHERE r.topic_id = cut.topic_id AND r.position <= cut.position \
                       RETURNING r.position) \
                     UPDATE {{s}}.topics x SET trimmed = greatest(x.trimmed, \
                       (SELECT max(position) FROM gone)) \
                     FROM cut WHERE x.topic_id = cut.topic_id AND EXISTS (SELECT 1 FROM gone)"
                ),
                chain,
                topic,
                &[&keep],
            )
            .await;
        }

        async fn expire_log(&self, chain: &ChainId, topic: &str) {
            // The records go and the topic row stays, as the tombstone.
            self.on_topic(
                &format!(
                    "WITH t AS ({TOPIC}), \
                     gone AS (DELETE FROM {{s}}.records r USING t WHERE r.topic_id = t.topic_id) \
                     UPDATE {{s}}.topics x SET trimmed = x.last_position FROM t \
                     WHERE x.topic_id = t.topic_id"
                ),
                chain,
                topic,
                &[],
            )
            .await;
        }
    }

    #[async_trait::async_trait]
    impl Forgets for Postgres {
        async fn forget(&self, chain: &ChainId, topic: &str) {
            self.on_topic(
                &format!(
                    "WITH t AS ({TOPIC}), \
                     records AS (DELETE FROM {{s}}.records r USING t WHERE r.topic_id = t.topic_id), \
                     producers AS (DELETE FROM {{s}}.producers h USING t \
                       WHERE h.topic_id = t.topic_id) \
                     DELETE FROM {{s}}.topics x USING t WHERE x.topic_id = t.topic_id"
                ),
                chain,
                topic,
                &[],
            )
            .await;
        }

        async fn lapse_archive_hold(&self, chain: &ChainId, topic: &str) {
            self.on_topic(
                &format!(
                    "WITH t AS ({TOPIC}) UPDATE {{s}}.floors f SET until = now() - interval '1 s' \
                     FROM t WHERE f.topic_id = t.topic_id AND f.holder = 'archive'"
                ),
                chain,
                topic,
                &[],
            )
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
        ..Default::default()
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
        stream_hash: {
            let (kind, id) = chain_owner(chain);
            stream_hash(&chain.namespace, kind, &id, topic)
        },
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

    async fn close_of(case: &dyn Case, chain: &ChainId, topic: &str) -> StoreReadResponse {
        case.store()
            .read(StoreReadRequest {
                chain: Some(chain.clone()),
                topic: topic.to_string(),
                max_records: 10,
                ..Default::default()
            })
            .await
            .unwrap()
    }

    fn failure(message: &str) -> temporalio_common::protos::temporal::api::failure::v1::Failure {
        temporalio_common::protos::temporal::api::failure::v1::Failure {
            message: message.to_string(),
            ..Default::default()
        }
    }

    pub(crate) async fn a_topic_keeps_its_first_close(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        let first = StoredClose {
            failure: Some(failure("the agent gave up")),
            closed_by: "run-1".to_string(),
            ..Default::default()
        };
        store
            .close_topic_with(&chain, OUT, first.clone())
            .await
            .unwrap();
        // A later close, with another outcome, leaves the first one.
        store
            .close_topic_with(
                &chain,
                OUT,
                StoredClose {
                    result: Some(Payload {
                        data: b"late".to_vec(),
                        ..Default::default()
                    }),
                    closed_by: "run-2".to_string(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        store.close_topic(&chain, OUT).await.unwrap();
        let read = close_of(case, &chain, OUT).await;
        assert!(read.closed);
        assert_eq!(read.close, Some(first));
        let refused = Producer::new(case, &chain, OUT, "q", 1)
            .send(&[data("2")])
            .await
            .unwrap_err();
        assert_eq!(refused.kind, StreamFailureKind::Closed);
        assert_eq!(refused.closed_by.as_deref(), Some("run-1"));
    }

    pub(crate) async fn a_second_close_answers_the_first(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        let first = StoredClose {
            result: Some(Payload {
                data: b"x".to_vec(),
                ..Default::default()
            }),
            closed_by: "run-1".to_string(),
            ..Default::default()
        };
        let second = StoredClose {
            result: Some(Payload {
                data: b"y".to_vec(),
                ..Default::default()
            }),
            closed_by: "run-2".to_string(),
            ..Default::default()
        };
        let kept = store
            .close_topic_with(&chain, OUT, first.clone())
            .await
            .unwrap();
        assert_eq!(kept, first);
        // A wrapper tells the notifier what this answers, so it must be the first close too.
        let kept = store.close_topic_with(&chain, OUT, second).await.unwrap();
        assert_eq!(kept, first);
        assert_eq!(close_of(case, &chain, OUT).await.close, Some(first));
    }

    pub(crate) async fn a_close_with_no_outcome_is_the_first_close_too(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        store.close_topic(&chain, OUT).await.unwrap();
        store
            .close_topic_with(
                &chain,
                OUT,
                StoredClose {
                    failure: Some(failure("too late")),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            close_of(case, &chain, OUT).await.close,
            Some(StoredClose::default())
        );
        let refused = Producer::new(case, &chain, OUT, "p", 1)
            .send(&[data("1")])
            .await
            .unwrap_err();
        assert_eq!(refused.closed_by, None, "the close named no one");
    }

    pub(crate) async fn a_chain_close_keeps_no_outcome(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        store.close_chain(&chain).await.unwrap();
        let read = close_of(case, &chain, OUT).await;
        assert!(read.closed);
        assert_eq!(read.close, None);
        let refused = Producer::new(case, &chain, OUT, "q", 1)
            .send(&[data("2")])
            .await
            .unwrap_err();
        assert_eq!(refused.kind, StreamFailureKind::Closed);
        assert_eq!(refused.closed_by, None);
    }

    pub(crate) async fn a_floor_holds_and_releases_for_any_reader(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        let landed = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1", "2"])
            .await;
        store
            .hold_floor(&chain, OUT, "a/1", &landed.first_position)
            .await
            .unwrap();
        // A lower hold, a hold from the beginning, another reader and a topic with no records.
        store
            .hold_floor(&chain, OUT, "a/1", &landed.last_position)
            .await
            .unwrap();
        store.hold_floor(&chain, OUT, "b/1", "").await.unwrap();
        store.hold_floor(&chain, OTHER, "a/1", "").await.unwrap();
        // A held topic still takes appends and reads.
        Producer::new(case, &chain, OUT, "q", 1)
            .append(&["3"])
            .await;
        let read = store
            .read(StoreReadRequest {
                chain: Some(chain.clone()),
                topic: OUT.to_string(),
                max_records: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(read.records.len(), 3);
        for holder in ["a/1", "b/1", "never-held"] {
            store.release_floor(&chain, OUT, holder).await.unwrap();
        }
        store
            .release_floor(&case.chain(), OUT, "a/1")
            .await
            .unwrap();
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

    pub(crate) async fn a_link_record_reads_back_with_its_link(case: &dyn Case) {
        use temporalio_common::protos::temporal::api::{
            enums::v1::StreamOwnerKind as Kind, stream::v1::StreamReference,
        };
        let chain = case.chain();
        let open = StreamLink {
            event: stream_link::Event::Open as i32,
            stream: Some(StreamReference {
                owner_kind: Kind::Workflow as i32,
                workflow_id: "child".to_string(),
                topic: OUT.to_string(),
                ..Default::default()
            }),
            name: "sub-3".to_string(),
            after: "a cursor".to_string(),
            ..Default::default()
        };
        let join = StreamLink {
            event: stream_link::Event::Join as i32,
            name: "sub-3".to_string(),
            ..Default::default()
        };
        let link = |link: &StreamLink| AppendRecord {
            kind: StreamRecordKind::Link as i32,
            link: Some(link.clone()),
            ..Default::default()
        };
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer
            .send(&[data("1"), link(&open), data("2"), link(&join)])
            .await
            .unwrap();
        let page = read(case, &chain, OUT, BEGINNING).await.unwrap();
        let kinds: Vec<_> = page.records.iter().map(|r| stored(r).kind).collect();
        let (data_kind, link_kind) = (StreamRecordKind::Data as i32, StreamRecordKind::Link as i32);
        assert_eq!(kinds, [data_kind, link_kind, data_kind, link_kind]);
        assert_eq!(stored(&page.records[1]).link, Some(open));
        assert_eq!(stored(&page.records[3]).link, Some(join));
        assert_eq!(stored(&page.records[2]).link, None);
    }

    pub(crate) async fn a_workflow_activity_keeps_a_topic_whose_first_batch_was_lost(
        case: &dyn Case,
    ) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.sequence = 3;
        producer.append(&["3"]).await;
        let written = case.store().written_topics(&chain).await.unwrap();
        if chain.owner_kind == StreamOwnerKind::WorkflowActivity as i32 {
            assert_eq!(written, [OUT]);
        } else {
            assert!(written.is_empty());
        }
    }

    pub(crate) async fn a_workflow_activity_keeps_the_topics_it_wrote(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        let keeps = chain.owner_kind == StreamOwnerKind::WorkflowActivity as i32;
        assert!(store.written_topics(&chain).await.unwrap().is_empty());
        let mut first = Producer::new(case, &chain, OUT, "p", 1);
        first.append(&["1"]).await;
        first.append(&["2"]).await;
        // A repeat of the newest batch adds nothing.
        first.sequence = 2;
        first.append(&["2"]).await;
        Producer::new(case, &chain, OTHER, "q", 2)
            .append(&["1"])
            .await;
        let written = store.written_topics(&chain).await.unwrap();
        if keeps {
            assert_eq!(written, [OTHER, OUT]);
        } else {
            assert!(written.is_empty());
        }
        // A refused batch adds nothing, and a closed topic stays in the set.
        store.close_topic(&chain, OUT).await.unwrap();
        let refused = Producer::new(case, &chain, OUT, "r", 1)
            .send(&[data("3")])
            .await;
        assert_eq!(kind(refused), StreamFailureKind::Closed);
        assert_eq!(store.written_topics(&chain).await.unwrap(), written);
        // Another chain's set is its own.
        assert!(
            store
                .written_topics(&case.chain())
                .await
                .unwrap()
                .is_empty()
        );
    }

    pub(crate) async fn deleting_a_workflow_drops_its_workflow_activities(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        let sibling = ChainId {
            first_run_id: format!("{}-next", chain.first_run_id),
            ..chain.clone()
        };
        let workflow = ChainId {
            namespace: chain.namespace.clone(),
            workflow_id: chain.workflow_id.clone(),
            first_run_id: chain.first_run_id.clone(),
            ..Default::default()
        };
        // A Workflow whose id only starts with this one keeps its Activities' streams.
        let bystander = ChainId {
            workflow_id: format!("{}2", chain.workflow_id),
            ..chain.clone()
        };
        for written in [&chain, &sibling, &workflow, &bystander] {
            Producer::new(case, written, OUT, "p", 1)
                .append(&["1"])
                .await;
        }
        let request = DeleteOwnerRequest {
            namespace: chain.namespace.clone(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: chain.workflow_id.clone(),
            ..Default::default()
        };
        assert!(store.delete_owner(request.clone()).await.unwrap().deleted > 0);
        assert_eq!(store.delete_owner(request).await.unwrap().deleted, 0);
        for gone in [&chain, &sibling, &workflow] {
            assert_eq!(latest(case, gone, OUT).await, "");
        }
        assert!(store.written_topics(&chain).await.unwrap().is_empty());
        assert_eq!(
            values(&read(case, &bystander, OUT, BEGINNING).await.unwrap()),
            ["1"]
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
        let (kind_name, id) = chain_owner(&chain);
        let activity = kind_name == "activity";
        // An owner of the other kind with the same id keeps its streams.
        let twin = if activity {
            ChainId {
                namespace: chain.namespace.clone(),
                workflow_id: id.to_string(),
                first_run_id: chain.first_run_id.clone(),
                ..Default::default()
            }
        } else {
            activity_chain(&chain.namespace, &id, &chain.first_run_id)
        };
        let landed = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        Producer::new(case, &next, OUT, "p", 1).append(&["1"]).await;
        for kept in [&bystander, &twin] {
            Producer::new(case, kept, OUT, "p", 1).append(&["1"]).await;
        }
        let request = if activity {
            DeleteOwnerRequest {
                namespace: chain.namespace.clone(),
                owner_kind: StreamOwnerKind::Activity as i32,
                activity_id: id.to_string(),
                ..Default::default()
            }
        } else {
            DeleteOwnerRequest {
                namespace: chain.namespace.clone(),
                owner_kind: StreamOwnerKind::Workflow as i32,
                workflow_id: id.to_string(),
                ..Default::default()
            }
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
        for kept in [&bystander, &twin] {
            assert_eq!(
                values(&read(case, kept, OUT, BEGINNING).await.unwrap()),
                ["1"]
            );
        }
    }

    /// The store's log. A case runs only on a store that keeps one.
    fn log_of(case: &dyn Case) -> Arc<dyn NotificationLog> {
        case.store()
            .notification_log()
            .expect("the notification log cases run on stores that keep a log")
    }

    /// Takes the entries of `chain` from every busy partition, as `consumer`.
    async fn entries_of(
        log: &dyn NotificationLog,
        chain: &ChainId,
        consumer: &str,
        claim_idle: Duration,
    ) -> Vec<(u32, NotificationEntry)> {
        let mut found = vec![];
        for partition in log.busy().await.unwrap() {
            for entry in log
                .take(partition, consumer, claim_idle, 1000)
                .await
                .unwrap()
            {
                if &entry.chain == chain {
                    found.push((partition, entry));
                }
            }
        }
        found
    }

    pub(crate) async fn an_append_writes_one_entry_with_its_last_position(case: &dyn Case) {
        let log = log_of(case);
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        let landed = producer.append(&["1", "2"]).await;
        // A repeat of the newest batch writes nothing again.
        producer.sequence = 1;
        assert_eq!(producer.append(&["1", "2"]).await, landed);
        let entries = entries_of(log.as_ref(), &chain, "a", Duration::ZERO).await;
        let [(partition, entry)] = entries.as_slice() else {
            panic!("one append gives one entry: {entries:?}");
        };
        assert!(*partition < log.partitions());
        assert_eq!(
            (entry.topic.as_str(), entry.position.as_str()),
            (OUT, landed.last_position.as_str())
        );
    }

    pub(crate) async fn an_entry_stays_until_its_ack(case: &dyn Case) {
        let log = log_of(case);
        let chain = case.chain();
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        let first = entries_of(log.as_ref(), &chain, "a", Duration::ZERO).await;
        assert_eq!(first.len(), 1);
        // A consumer that took an entry and didn't ack it gets it again.
        let again = entries_of(log.as_ref(), &chain, "a", Duration::ZERO).await;
        assert_eq!(again, first);
        let (partition, entry) = &first[0];
        log.ack(*partition, std::slice::from_ref(&entry.id))
            .await
            .unwrap();
        assert!(
            entries_of(log.as_ref(), &chain, "a", Duration::ZERO)
                .await
                .is_empty()
        );
    }

    pub(crate) async fn another_consumer_claims_an_entry_only_after_claim_idle(case: &dyn Case) {
        let log = log_of(case);
        let chain = case.chain();
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        let hour = Duration::from_secs(3600);
        assert_eq!(entries_of(log.as_ref(), &chain, "a", hour).await.len(), 1);
        assert!(
            entries_of(log.as_ref(), &chain, "b", hour).await.is_empty(),
            "a live consumer keeps what it took"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        let claimed = entries_of(log.as_ref(), &chain, "b", Duration::from_millis(10)).await;
        assert_eq!(claimed.len(), 1, "a stopped consumer's entry moves on");
    }

    pub(crate) async fn a_refused_append_writes_no_entry(case: &dyn Case) {
        let log = log_of(case);
        let store = case.store();
        let chain = case.chain();
        store.close_topic(&chain, OUT).await.unwrap();
        let refused = Producer::new(case, &chain, OUT, "p", 1)
            .send(&[data("1")])
            .await;
        assert!(refused.is_err());
        assert!(
            entries_of(log.as_ref(), &chain, "a", Duration::ZERO)
                .await
                .is_empty()
        );
    }

    pub(crate) async fn the_doorbell_of_a_partition_with_entries_rings(case: &dyn Case) {
        let log = log_of(case);
        let chain = case.chain();
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        let entries = entries_of(log.as_ref(), &chain, "a", Duration::ZERO).await;
        let partition = entries[0].0;
        let rung = log
            .wait_doorbell(partition % log.doorbells(), "a", Duration::from_millis(100))
            .await
            .unwrap();
        assert!(rung.contains(&partition), "{rung:?}");
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
        // Without close flags a topic close is a `CLOSE` record, which every store can append.
        store.close_topic(&chain, OUT).await.unwrap();
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
                owner_kind: if chain.owner_kind == StreamOwnerKind::Activity as i32 {
                    StreamOwnerKind::Activity as i32
                } else {
                    StreamOwnerKind::Workflow as i32
                },
                workflow_id: chain.workflow_id.clone(),
                activity_id: chain.activity_id.clone(),
            })
            .await;
        assert_eq!(unsupported(delete.map(drop)), !declared.delete_owner);
        assert!(declared.max_record_bytes > 0 && declared.max_batch_bytes > 0);
        assert_eq!(
            unsupported(store.hold_floor(&chain, OTHER, "reader", "").await),
            !declared.holds
        );
        assert_eq!(store.notification_log().is_some(), declared.change_feed);
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

/// The archive tier, through a [TieredStore] whose hot store keeps an archive hold.
mod archive_cases {
    use super::*;

    /// Reads a topic page by page from `after` until a page is empty. Answers the values and
    /// their cursors.
    async fn read_all(
        case: &dyn Case,
        chain: &ChainId,
        topic: &str,
        after: &str,
    ) -> (Vec<String>, Vec<String>) {
        let (mut values, mut cursors) = (Vec::new(), Vec::new());
        let mut page = read(case, chain, topic, after).await.unwrap();
        while !page.records.is_empty() {
            values.extend(super::values(&page));
            cursors.extend(page.records.iter().map(|record| record.cursor.clone()));
            page = read_with(
                case,
                chain,
                topic,
                &page.cursor,
                Duration::ZERO,
                &page.state,
            )
            .await
            .unwrap();
        }
        (values, cursors)
    }

    async fn segments(case: &dyn ArchiveCase, chain: &ChainId, topic: &str) -> usize {
        let prefix = format!("v1/{}/", chain.namespace);
        let mut count = 0;
        let mut after = String::new();
        // Each topic's objects share one directory, so walk the names of this topic only.
        let topic_dir = archive_dir(case, chain, topic).await;
        let Some(dir) = topic_dir else { return 0 };
        while let Some(name) = case.archive().first_after(&dir, &after).await.unwrap() {
            assert!(name.starts_with(&prefix), "{name}");
            if name.ends_with(".tsr") {
                count += 1;
            }
            after = name;
        }
        count
    }

    /// The name prefix of a topic's objects, found from its first segment.
    async fn archive_dir(case: &dyn ArchiveCase, chain: &ChainId, topic: &str) -> Option<String> {
        let (kind, _) = chain_owner(chain);
        let owner = match kind {
            "activity" => format!("a/{}/{}", chain.activity_id, chain.first_run_id),
            "workflow_activity" => format!(
                "wa/{}/{}/{}",
                chain.workflow_id, chain.first_run_id, chain.activity_id
            ),
            _ => format!("w/{}/{}", chain.workflow_id, chain.first_run_id),
        };
        let dir = format!("v1/{}/{owner}/{topic}/", chain.namespace);
        case.archive()
            .first_after(&dir, "")
            .await
            .unwrap()
            .map(|_| dir)
    }

    async fn archived(case: &dyn ArchiveCase, chain: &ChainId, topic: &str) -> String {
        case.hot()
            .archived(chain, topic)
            .await
            .unwrap()
            .unwrap_or_default()
    }

    pub(crate) async fn archived_records_read_back_with_their_cursors(case: &dyn ArchiveCase) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.append(&["1", "2", "3"]).await;
        let last = producer.append(&["4", "5"]).await;
        let (values, cursors) = read_all(case, &chain, OUT, BEGINNING).await;
        assert_eq!(values, ["1", "2", "3", "4", "5"]);
        case.archive_now(&chain, OUT, 2).await;
        assert_eq!(segments(case, &chain, OUT).await, 3);
        assert_eq!(archived(case, &chain, OUT).await, last.last_position);
        case.drop_oldest(&chain, OUT, 0).await;
        assert_eq!(
            read_all(case, &chain, OUT, BEGINNING).await,
            (values.clone(), cursors.clone())
        );
        // A cursor in the middle resumes strictly after its record, across segments.
        let (tail, _) = read_all(case, &chain, OUT, &cursors[1]).await;
        assert_eq!(tail, ["3", "4", "5"]);
    }

    pub(crate) async fn one_page_crosses_segments_and_the_hot_store(case: &dyn ArchiveCase) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.append(&["1", "2", "3"]).await;
        case.archive_now(&chain, OUT, 1).await;
        producer.append(&["4", "5"]).await;
        case.drop_oldest(&chain, OUT, 2).await;
        // An exact range, as a replay reads it, comes back in one page.
        let page = read(case, &chain, OUT, BEGINNING).await.unwrap();
        assert_eq!(values(&page), ["1", "2", "3", "4", "5"]);
        let cursor = page.records[0].cursor.clone();
        let rest = read(case, &chain, OUT, &cursor).await.unwrap();
        assert_eq!(values(&rest), ["2", "3", "4", "5"]);
    }

    pub(crate) async fn a_read_crosses_from_the_archive_into_the_hot_store(case: &dyn ArchiveCase) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.append(&["1", "2", "3"]).await;
        case.archive_now(&chain, OUT, 10).await;
        producer.append(&["4", "5"]).await;
        case.drop_oldest(&chain, OUT, 2).await;
        let (values, _) = read_all(case, &chain, OUT, BEGINNING).await;
        assert_eq!(values, ["1", "2", "3", "4", "5"]);
        // A later write reaches a reader that read the archive first.
        let (_, cursors) = read_all(case, &chain, OUT, BEGINNING).await;
        producer.append(&["6"]).await;
        let (tail, _) = read_all(case, &chain, OUT, cursors.last().unwrap()).await;
        assert_eq!(tail, ["6"]);
    }

    pub(crate) async fn the_trim_keeps_what_the_archive_does_not_hold(case: &dyn ArchiveCase) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.append(&["1", "2", "3"]).await;
        case.drop_oldest(&chain, OUT, 0).await;
        assert_eq!(
            values(&read(case, &chain, OUT, BEGINNING).await.unwrap()),
            ["1", "2", "3"]
        );
        let hot = case.hot();
        let from_hot = hot
            .read(StoreReadRequest {
                chain: Some(chain.clone()),
                topic: OUT.to_string(),
                max_records: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(from_hot.records.len(), 3);
        case.archive_now(&chain, OUT, 10).await;
        case.drop_oldest(&chain, OUT, 0).await;
        let (values, _) = read_all(case, &chain, OUT, BEGINNING).await;
        assert_eq!(values, ["1", "2", "3"]);
    }

    pub(crate) async fn an_archived_record_is_found_by_its_position(case: &dyn ArchiveCase) {
        let chain = case.chain();
        let first = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1", "2"])
            .await;
        case.archive_now(&chain, OUT, 10).await;
        case.forget(&chain, OUT).await;
        let record = case
            .store()
            .record_at(&chain, OUT, &first.first_position)
            .await
            .unwrap()
            .unwrap();
        let record = StreamRecord::decode(&*record).unwrap();
        assert_eq!(record.body.unwrap().data, b"1");
        // A read that resumes with no state looks up its cursor's record, and finds it.
        let cursor = cursor(case, &chain, OUT, &first.first_position);
        assert_eq!(
            values(&read(case, &chain, OUT, &cursor).await.unwrap()),
            ["2"]
        );
    }

    pub(crate) async fn archiving_twice_at_once_writes_each_segment_once(case: &dyn ArchiveCase) {
        let chain = case.chain();
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1", "2", "3", "4", "5"])
            .await;
        tokio::join!(
            case.archive_now(&chain, OUT, 2),
            case.archive_now(&chain, OUT, 2)
        );
        case.archive_now(&chain, OUT, 2).await;
        assert_eq!(segments(case, &chain, OUT).await, 3);
        case.drop_oldest(&chain, OUT, 0).await;
        let (values, _) = read_all(case, &chain, OUT, BEGINNING).await;
        assert_eq!(values, ["1", "2", "3", "4", "5"]);
    }

    pub(crate) async fn a_closed_topic_reads_its_close_from_the_archive(case: &dyn ArchiveCase) {
        let chain = case.chain();
        let landed = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1", "2"])
            .await;
        let close = StoredClose {
            closed_by: "run-9".to_string(),
            ..Default::default()
        };
        case.store()
            .close_topic_with(&chain, OUT, close.clone())
            .await
            .unwrap();
        let limits = SegmentLimits::default();
        let progress = archive_topic(&*case.hot(), &*case.archive(), &chain, OUT, limits, false)
            .await
            .unwrap();
        assert!(progress.finished, "{progress:?}");
        assert_eq!(progress.archived, landed.last_position);
        case.forget(&chain, OUT).await;
        let page = case
            .store()
            .read(StoreReadRequest {
                chain: Some(chain.clone()),
                topic: OUT.to_string(),
                max_records: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.records.len(), 2);
        assert!(page.closed);
        assert_eq!(page.close.unwrap().closed_by, "run-9");
        let end = case
            .store()
            .read(StoreReadRequest {
                chain: Some(chain.clone()),
                topic: OUT.to_string(),
                after_position: landed.last_position.clone(),
                max_records: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(end.records.is_empty() && end.closed);
        assert_eq!(end.close.unwrap().closed_by, "run-9");
        assert_eq!(latest(case, &chain, OUT).await, landed.last_position);
        assert_eq!(
            case.store().newest_ever(&chain, OUT).await.unwrap(),
            landed.last_position
        );
    }

    pub(crate) async fn an_open_topic_the_hot_store_forgot_reads_from_the_archive(
        case: &dyn ArchiveCase,
    ) {
        let chain = case.chain();
        let landed = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1", "2"])
            .await;
        case.archive_now(&chain, OUT, 10).await;
        case.forget(&chain, OUT).await;
        let (values, cursors) = read_all(case, &chain, OUT, BEGINNING).await;
        assert_eq!(values, ["1", "2"]);
        let end = read(case, &chain, OUT, &cursors[1]).await.unwrap();
        assert!(end.records.is_empty());
        assert_eq!(latest(case, &chain, OUT).await, landed.last_position);
        assert_eq!(
            case.store().newest_ever(&chain, OUT).await.unwrap(),
            landed.last_position
        );
        // A topic that neither tier holds is not found, as on the hot store alone.
        let missing = cursor(case, &chain, OTHER, &landed.last_position);
        assert_eq!(
            kind(read(case, &chain, OTHER, &missing).await),
            StreamFailureKind::NotFound
        );
    }

    pub(crate) async fn a_read_before_what_the_archive_holds_is_expired(case: &dyn ArchiveCase) {
        let chain = case.chain();
        let first = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1", "2", "3", "4"])
            .await;
        case.archive_now(&chain, OUT, 2).await;
        case.drop_oldest(&chain, OUT, 0).await;
        // A bucket lifecycle rule deleted the oldest segment.
        let dir = archive_dir(case, &chain, OUT).await.unwrap();
        let oldest = case
            .archive()
            .first_after(&dir, &format!("{dir}seg-"))
            .await
            .unwrap()
            .unwrap();
        case.lose(&oldest).await;
        let cursor = cursor(case, &chain, OUT, &first.first_position);
        assert_eq!(
            kind(read(case, &chain, OUT, &cursor).await),
            StreamFailureKind::Expired
        );
        // A read from the beginning starts at the oldest record still held.
        let (values, _) = read_all(case, &chain, OUT, BEGINNING).await;
        assert_eq!(values, ["3", "4"]);
    }

    pub(crate) async fn a_read_at_the_last_record_the_archive_lost_goes_on(case: &dyn ArchiveCase) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        let first = producer.append(&["1", "2"]).await;
        case.archive_now(&chain, OUT, 10).await;
        let lost = producer.append(&["3"]).await;
        producer.append(&["4", "5"]).await;
        // The archive's hold lapses, and retention drops `3` before the archive takes it.
        case.lapse_archive_hold(&chain, OUT).await;
        case.drop_oldest(&chain, OUT, 2).await;
        case.archive_now(&chain, OUT, 10).await;
        // Only the archive answers now.
        case.forget(&chain, OUT).await;
        let at_lost = cursor(case, &chain, OUT, &lost.last_position);
        let (values, _) = read_all(case, &chain, OUT, &at_lost).await;
        assert_eq!(values, ["4", "5"]);
        let before_lost = cursor(case, &chain, OUT, &first.last_position);
        assert_eq!(
            kind(read(case, &chain, OUT, &before_lost).await),
            StreamFailureKind::Expired
        );
    }

    pub(crate) async fn a_driver_finds_the_topics_to_archive(case: &dyn ArchiveCase) {
        let chain = case.chain();
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        let marks = case
            .hot()
            .notification_log()
            .unwrap()
            .archive_marks(1000)
            .await
            .unwrap();
        assert!(
            marks
                .iter()
                .any(|mark| mark.chain == chain && mark.topic == OUT),
            "{marks:?}"
        );
    }

    pub(crate) async fn a_drop_the_archive_holds_loses_nothing(case: &dyn ArchiveCase) {
        let chain = case.chain();
        let first = read(case, &chain, OUT, BEGINNING).await.unwrap();
        assert!(first.records.is_empty());
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1", "2"])
            .await;
        case.archive_now(&chain, OUT, 10).await;
        case.drop_oldest(&chain, OUT, 1).await;
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
        assert_eq!(values(&next), ["1", "2"]);
    }

    pub(crate) async fn deleting_an_owner_deletes_its_archive(case: &dyn ArchiveCase) {
        let chain = case.chain();
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        case.archive_now(&chain, OUT, 10).await;
        assert_eq!(segments(case, &chain, OUT).await, 1);
        let (kind_name, id) = chain_owner(&chain);
        let request = match kind_name {
            "activity" => DeleteOwnerRequest {
                namespace: chain.namespace.clone(),
                owner_kind: StreamOwnerKind::Activity as i32,
                activity_id: id.to_string(),
                ..Default::default()
            },
            _ => DeleteOwnerRequest {
                namespace: chain.namespace.clone(),
                owner_kind: StreamOwnerKind::Workflow as i32,
                workflow_id: chain.workflow_id.clone(),
                ..Default::default()
            },
        };
        let deleted = case.store().delete_owner(request).await.unwrap();
        assert!(deleted.deleted >= 1);
        assert_eq!(segments(case, &chain, OUT).await, 0);
        assert!(read_all(case, &chain, OUT, BEGINNING).await.0.is_empty());
    }
}

macro_rules! archive_conformance {
    ($case:expr; $($name:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                let Some(case) = $case.await else { return };
                super::archive_cases::$name(&case).await;
            }
        )*
    };
}

/// The archive cases. `the_trim_keeps_what_the_archive_does_not_hold` runs on its own, since it
/// needs a test trim that keeps what a hold holds.
macro_rules! archive_cases {
    ($case:expr) => {
        archive_conformance!($case;
            archived_records_read_back_with_their_cursors,
            one_page_crosses_segments_and_the_hot_store,
            a_read_crosses_from_the_archive_into_the_hot_store,
            an_archived_record_is_found_by_its_position,
            archiving_twice_at_once_writes_each_segment_once,
            a_closed_topic_reads_its_close_from_the_archive,
            an_open_topic_the_hot_store_forgot_reads_from_the_archive,
            a_read_before_what_the_archive_holds_is_expired,
            a_read_at_the_last_record_the_archive_lost_goes_on,
            a_drop_the_archive_holds_loses_nothing,
            deleting_an_owner_deletes_its_archive,
        );
    };
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

/// Every case but owner deletes, which need a rule of their own under the documented ACL.
macro_rules! store_cases {
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
            a_topic_keeps_its_first_close,
            a_floor_holds_and_releases_for_any_reader,
            a_second_close_answers_the_first,
            a_close_with_no_outcome_is_the_first_close_too,
            a_chain_close_keeps_no_outcome,
            a_workflow_activity_keeps_the_topics_it_wrote,
            a_workflow_activity_keeps_a_topic_whose_first_batch_was_lost,
            a_link_record_reads_back_with_its_link,
            a_lost_cursor_record_reads_as_expired,
            the_declared_capabilities_match_the_calls,
        );
    };
}

/// The notification log, for a store that keeps one.
macro_rules! log_cases {
    ($case:expr) => {
        conformance!($case;
            an_append_writes_one_entry_with_its_last_position,
            an_entry_stays_until_its_ack,
            another_consumer_claims_an_entry_only_after_claim_idle,
            a_refused_append_writes_no_entry,
            the_doorbell_of_a_partition_with_entries_rings,
        );
    };
}

mod memory {
    store_cases!(super::memory());
    conformance!(super::memory(); deleting_an_owner_drops_every_chain_of_it);
}

mod memory_with_log {
    store_cases!(super::memory_with_log());
    log_cases!(super::memory_with_log());
}

mod activity_memory {
    store_cases!(super::activity(super::memory()));
    log_cases!(super::activity(super::memory_with_log()));
    conformance!(super::activity(super::memory()); deleting_an_owner_drops_every_chain_of_it);
}

mod workflow_activity_memory {
    store_cases!(super::workflow_activity(super::memory()));
    log_cases!(super::workflow_activity(super::memory_with_log()));
    conformance!(super::workflow_activity(super::memory());
        deleting_a_workflow_drops_its_workflow_activities,
    );
}

mod notifying_memory {
    store_cases!(super::notifying(super::memory()));
    log_cases!(super::notifying(super::memory_with_log()));
    conformance!(super::notifying(super::memory());
        deleting_an_owner_drops_every_chain_of_it,
    );
}

#[cfg(feature = "redis")]
mod standalone_redis {
    store_cases!(super::on_redis::redis("STREAMS_REDIS_URL", false));
    conformance!(super::on_redis::redis("STREAMS_REDIS_URL", false);
        deleting_an_owner_drops_every_chain_of_it,
    );
}

#[cfg(feature = "redis")]
mod cluster_redis {
    store_cases!(super::on_redis::redis("STREAMS_REDIS_CLUSTER_URL", true));
    // A Workflow id's chains hash to different slots, so the delete reaches every primary.
    conformance!(super::on_redis::redis("STREAMS_REDIS_CLUSTER_URL", true);
        deleting_an_owner_drops_every_chain_of_it,
    );
}

#[cfg(feature = "redis")]
mod activity_standalone_redis {
    store_cases!(super::activity(super::on_redis::redis(
        "STREAMS_REDIS_URL",
        false
    )));
    log_cases!(super::activity(super::on_redis::redis_with_log(
        "STREAMS_REDIS_URL",
        false
    )));
    conformance!(super::activity(super::on_redis::redis("STREAMS_REDIS_URL", false));
        deleting_an_owner_drops_every_chain_of_it,
    );
}

#[cfg(feature = "redis")]
mod activity_documented_acl_redis {
    // The documented key rule covers an Activity's keys, since they start with the namespace.
    store_cases!(super::activity(super::on_redis::redis_as_documented_user(
        "STREAMS_REDIS_URL",
        false,
        &[]
    )));
    conformance!(super::activity(super::on_redis::redis_as_documented_user(
        "STREAMS_REDIS_URL",
        false,
        &["+scan"]
    ));
        deleting_an_owner_drops_every_chain_of_it,
    );
}

#[cfg(feature = "redis")]
mod activity_cluster_redis {
    store_cases!(super::activity(super::on_redis::redis(
        "STREAMS_REDIS_CLUSTER_URL",
        true
    )));
    log_cases!(super::activity(super::on_redis::redis_with_log(
        "STREAMS_REDIS_CLUSTER_URL",
        true
    )));
    conformance!(super::activity(super::on_redis::redis("STREAMS_REDIS_CLUSTER_URL", true));
        deleting_an_owner_drops_every_chain_of_it,
    );
}

#[cfg(feature = "redis")]
mod workflow_activity_standalone_redis {
    store_cases!(super::workflow_activity(super::on_redis::redis(
        "STREAMS_REDIS_URL",
        false
    )));
    log_cases!(super::workflow_activity(super::on_redis::redis_with_log(
        "STREAMS_REDIS_URL",
        false
    )));
    conformance!(super::workflow_activity(super::on_redis::redis("STREAMS_REDIS_URL", false));
        deleting_a_workflow_drops_its_workflow_activities,
    );
}

#[cfg(feature = "redis")]
mod workflow_activity_documented_acl_redis {
    // The topic set needs `SADD` and `SMEMBERS`, which the page names.
    store_cases!(super::workflow_activity(
        super::on_redis::redis_as_documented_user("STREAMS_REDIS_URL", false, &[])
    ));
    conformance!(super::workflow_activity(super::on_redis::redis_as_documented_user(
        "STREAMS_REDIS_URL",
        false,
        &["+scan"]
    ));
        deleting_a_workflow_drops_its_workflow_activities,
    );
}

#[cfg(feature = "redis")]
mod workflow_activity_documented_acl_cluster_redis_with_log {
    store_cases!(super::workflow_activity(
        super::on_redis::redis_as_documented_user_with_log("STREAMS_REDIS_CLUSTER_URL", true)
    ));
}

#[cfg(feature = "redis")]
mod workflow_activity_cluster_redis {
    store_cases!(super::workflow_activity(super::on_redis::redis(
        "STREAMS_REDIS_CLUSTER_URL",
        true
    )));
    log_cases!(super::workflow_activity(super::on_redis::redis_with_log(
        "STREAMS_REDIS_CLUSTER_URL",
        true
    )));
    // Each Activity's chain hashes to its own slot, so the delete reaches every primary.
    conformance!(super::workflow_activity(super::on_redis::redis(
        "STREAMS_REDIS_CLUSTER_URL",
        true
    ));
        deleting_a_workflow_drops_its_workflow_activities,
    );
}

#[cfg(feature = "redis")]
mod standalone_redis_with_log {
    store_cases!(super::on_redis::redis_with_log("STREAMS_REDIS_URL", false));
    log_cases!(super::on_redis::redis_with_log("STREAMS_REDIS_URL", false));
}

#[cfg(feature = "redis")]
mod cluster_redis_with_log {
    store_cases!(super::on_redis::redis_with_log(
        "STREAMS_REDIS_CLUSTER_URL",
        true
    ));
    log_cases!(super::on_redis::redis_with_log(
        "STREAMS_REDIS_CLUSTER_URL",
        true
    ));
}

#[cfg(feature = "redis")]
mod notifying_redis {
    store_cases!(super::notifying(super::on_redis::redis(
        "STREAMS_REDIS_URL",
        false
    )));
}

#[cfg(feature = "redis")]
mod documented_acl_redis {
    store_cases!(super::on_redis::redis_as_documented_user(
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
mod documented_acl_redis_with_log {
    store_cases!(super::on_redis::redis_as_documented_user_with_log(
        "STREAMS_REDIS_URL",
        false
    ));
}

#[cfg(feature = "redis")]
mod documented_acl_cluster_redis_with_log {
    store_cases!(super::on_redis::redis_as_documented_user_with_log(
        "STREAMS_REDIS_CLUSTER_URL",
        true
    ));
}

#[cfg(feature = "redis")]
mod documented_acl_cluster_redis {
    // A cluster client asks the topology first, so the documented rules must allow it.
    store_cases!(super::on_redis::redis_as_documented_user(
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

#[cfg(feature = "redis")]
mod memorydb_acl_redis {
    store_cases!(super::on_redis::redis_as_memorydb_user(
        "STREAMS_REDIS_URL",
        false
    ));
    conformance!(super::on_redis::redis_as_memorydb_user("STREAMS_REDIS_URL", false);
        deleting_an_owner_drops_every_chain_of_it,
    );
}

#[cfg(feature = "redis")]
mod memorydb_acl_cluster_redis {
    // A MemoryDB cluster is always a Redis Cluster.
    store_cases!(super::on_redis::redis_as_memorydb_user(
        "STREAMS_REDIS_CLUSTER_URL",
        true
    ));
    conformance!(super::on_redis::redis_as_memorydb_user("STREAMS_REDIS_CLUSTER_URL", true);
        deleting_an_owner_drops_every_chain_of_it,
    );
}

/// Apache Kvrocks, a test lane with no support claim. It has no ACL users and no Redis Cluster.
#[cfg(feature = "redis")]
mod kvrocks {
    store_cases!(super::on_redis::redis("STREAMS_KVROCKS_URL", false));
    conformance!(super::on_redis::redis("STREAMS_KVROCKS_URL", false);
        deleting_an_owner_drops_every_chain_of_it,
    );
    // Kvrocks answers an empty `XREADGROUP` for a missing group where Redis fails with
    // `NOGROUP`, so the doorbell's group is never made and its case can't pass.
    conformance!(super::on_redis::redis_with_log("STREAMS_KVROCKS_URL", false);
        an_append_writes_one_entry_with_its_last_position,
        an_entry_stays_until_its_ack,
        another_consumer_claims_an_entry_only_after_claim_idle,
        a_refused_append_writes_no_entry,
    );
}

#[cfg(feature = "redis")]
mod workflow_activity_kvrocks {
    store_cases!(super::workflow_activity(super::on_redis::redis(
        "STREAMS_KVROCKS_URL",
        false
    )));
}

#[cfg(feature = "postgres")]
mod postgres {
    store_cases!(super::on_postgres::postgres("STREAMS_POSTGRES_URL"));
    conformance!(super::on_postgres::postgres("STREAMS_POSTGRES_URL");
        deleting_an_owner_drops_every_chain_of_it,
    );
}

#[cfg(feature = "postgres")]
mod postgres_with_log {
    store_cases!(super::on_postgres::postgres_with_log(
        "STREAMS_POSTGRES_URL"
    ));
    log_cases!(super::on_postgres::postgres_with_log(
        "STREAMS_POSTGRES_URL"
    ));
}

#[cfg(feature = "postgres")]
mod postgres_notifying_reads {
    store_cases!(super::on_postgres::postgres_notifying_reads(
        "STREAMS_POSTGRES_URL"
    ));
}

#[cfg(feature = "postgres")]
mod pgbouncer_postgres {
    // Transaction mode, with the listener and the migrations on a direct connection.
    store_cases!(super::on_postgres::postgres_through_pgbouncer());
    log_cases!(super::on_postgres::postgres_through_pgbouncer());
}

#[cfg(feature = "postgres")]
mod activity_postgres {
    store_cases!(super::activity(super::on_postgres::postgres(
        "STREAMS_POSTGRES_URL"
    )));
    log_cases!(super::activity(super::on_postgres::postgres_with_log(
        "STREAMS_POSTGRES_URL"
    )));
    conformance!(super::activity(super::on_postgres::postgres("STREAMS_POSTGRES_URL"));
        deleting_an_owner_drops_every_chain_of_it,
    );
}

#[cfg(feature = "postgres")]
mod workflow_activity_postgres {
    store_cases!(super::workflow_activity(super::on_postgres::postgres(
        "STREAMS_POSTGRES_URL"
    )));
    log_cases!(super::workflow_activity(
        super::on_postgres::postgres_with_log("STREAMS_POSTGRES_URL")
    ));
    conformance!(super::workflow_activity(super::on_postgres::postgres("STREAMS_POSTGRES_URL"));
        deleting_a_workflow_drops_its_workflow_activities,
    );
}

#[cfg(feature = "postgres")]
mod documented_role_postgres_with_log {
    store_cases!(super::on_postgres::postgres_as_documented_role_with_log(
        "STREAMS_POSTGRES_URL"
    ));
    log_cases!(super::on_postgres::postgres_as_documented_role_with_log(
        "STREAMS_POSTGRES_URL"
    ));
}

#[cfg(feature = "postgres")]
mod notifying_postgres {
    store_cases!(super::notifying(super::on_postgres::postgres(
        "STREAMS_POSTGRES_URL"
    )));
}

#[cfg(feature = "postgres")]
mod documented_role_postgres {
    // A role with only the page's grants, on a schema that a DBA migrated.
    store_cases!(super::on_postgres::postgres_as_documented_role(
        "STREAMS_POSTGRES_URL"
    ));
    conformance!(super::on_postgres::postgres_as_documented_role("STREAMS_POSTGRES_URL");
        deleting_an_owner_drops_every_chain_of_it,
    );
}

mod tiered_memory {
    store_cases!(super::tiered(super::memory()));
    log_cases!(super::tiered(super::memory_with_log()));
    conformance!(super::tiered(super::memory()); deleting_an_owner_drops_every_chain_of_it);
}

mod archive_memory {
    archive_cases!(super::tiered(super::memory_holding_for_archive()));
    archive_conformance!(super::tiered(super::memory_holding_for_archive());
        the_trim_keeps_what_the_archive_does_not_hold,
    );
}

mod archive_activity_memory {
    archive_cases!(super::tiered(super::activity(
        super::memory_holding_for_archive()
    )));
}

mod archive_workflow_activity_memory {
    archive_cases!(super::tiered(super::workflow_activity(
        super::memory_holding_for_archive()
    )));
}

#[cfg(feature = "redis")]
mod archive_standalone_redis {
    archive_cases!(super::tiered(super::on_redis::redis_with_archive(
        "STREAMS_REDIS_URL",
        false
    )));
    archive_conformance!(super::tiered(super::on_redis::redis_with_archive(
        "STREAMS_REDIS_URL",
        false
    ));
        the_trim_keeps_what_the_archive_does_not_hold,
    );
    // The test trim keeps what the archive's hold holds, so the store cases that trim run with no
    // hold, as on memory and PostgreSQL.
    store_cases!(super::tiered(super::on_redis::redis_with_log(
        "STREAMS_REDIS_URL",
        false
    )));
    log_cases!(super::tiered(super::on_redis::redis_with_archive(
        "STREAMS_REDIS_URL",
        false
    )));
}

#[cfg(feature = "redis")]
mod archive_documented_acl_redis {
    archive_cases!(super::tiered(
        super::on_redis::redis_as_documented_user_with_archive("STREAMS_REDIS_URL", false)
    ));
    archive_conformance!(super::tiered(
        super::on_redis::redis_as_documented_user_with_archive("STREAMS_REDIS_URL", false)
    );
        a_driver_finds_the_topics_to_archive,
    );
}

#[cfg(feature = "redis")]
mod archive_cluster_redis {
    archive_cases!(super::tiered(super::on_redis::redis_with_archive(
        "STREAMS_REDIS_CLUSTER_URL",
        true
    )));
    archive_conformance!(super::tiered(super::on_redis::redis_with_archive(
        "STREAMS_REDIS_CLUSTER_URL",
        true
    ));
        the_trim_keeps_what_the_archive_does_not_hold,
    );
    store_cases!(super::tiered(super::on_redis::redis_with_log(
        "STREAMS_REDIS_CLUSTER_URL",
        true
    )));
}

#[cfg(feature = "redis")]
mod archive_workflow_activity_cluster_redis {
    archive_cases!(super::tiered(super::workflow_activity(
        super::on_redis::redis_with_archive("STREAMS_REDIS_CLUSTER_URL", true)
    )));
}

#[cfg(feature = "postgres")]
mod archive_postgres {
    archive_cases!(super::tiered(super::on_postgres::postgres_with_archive(
        "STREAMS_POSTGRES_URL"
    )));
    // The test trim keeps what a hold holds, as the janitor does.
    archive_conformance!(super::tiered(super::on_postgres::postgres_with_archive(
        "STREAMS_POSTGRES_URL"
    ));
        the_trim_keeps_what_the_archive_does_not_hold,
    );
    log_cases!(super::tiered(super::on_postgres::postgres_with_archive(
        "STREAMS_POSTGRES_URL"
    )));
}

#[cfg(feature = "postgres")]
mod tiered_postgres {
    // The test trim keeps what the archive's hold holds, so the store cases that trim run with no
    // hold, as on memory.
    store_cases!(super::tiered(super::on_postgres::postgres_with_log(
        "STREAMS_POSTGRES_URL"
    )));
    log_cases!(super::tiered(super::on_postgres::postgres_with_log(
        "STREAMS_POSTGRES_URL"
    )));
    conformance!(super::tiered(super::on_postgres::postgres("STREAMS_POSTGRES_URL"));
        deleting_an_owner_drops_every_chain_of_it,
    );
}

#[cfg(feature = "postgres")]
mod archive_activity_postgres {
    archive_cases!(super::tiered(super::activity(
        super::on_postgres::postgres_with_archive("STREAMS_POSTGRES_URL")
    )));
}

#[cfg(feature = "postgres")]
mod archive_workflow_activity_postgres {
    archive_cases!(super::tiered(super::workflow_activity(
        super::on_postgres::postgres_with_archive("STREAMS_POSTGRES_URL")
    )));
}

#[cfg(all(feature = "archive", feature = "postgres"))]
mod archive_s3_postgres {
    archive_cases!(super::tiered_on_s3(
        super::on_postgres::postgres_with_archive("STREAMS_POSTGRES_URL")
    ));
    archive_conformance!(super::tiered_on_s3(super::on_postgres::postgres_with_archive(
        "STREAMS_POSTGRES_URL"
    ));
        the_trim_keeps_what_the_archive_does_not_hold,
    );
}

#[cfg(feature = "archive")]
mod archive_s3_memory {
    archive_cases!(super::tiered_on_s3(super::memory_holding_for_archive()));
    archive_conformance!(super::tiered_on_s3(super::memory_holding_for_archive());
        the_trim_keeps_what_the_archive_does_not_hold,
    );
}

#[cfg(all(feature = "archive", feature = "redis"))]
mod archive_s3_cluster_redis {
    archive_cases!(super::tiered_on_s3(super::on_redis::redis_with_archive(
        "STREAMS_REDIS_CLUSTER_URL",
        true
    )));
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
