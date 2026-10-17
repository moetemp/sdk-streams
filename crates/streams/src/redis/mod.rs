#![doc = include_str!("../../docs/redis.md")]

mod conn;
mod errors;
mod keys;
mod log;
mod scripts;
mod slots;

use crate::{
    Capabilities, ConditionForms, MAX_BATCH_BYTES, MAX_RECORD_BYTES, NotificationLog, StreamError,
    StreamResult, StreamStore,
    proto::{
        ChainId, DeleteOwnerRequest, DeleteOwnerResponse, StoreAppendRequest, StoreAppendResponse,
        StoreLatestRequest, StoreLatestResponse, StoreReadRequest, StoreReadResponse, StoredClose,
        StoredRecord, StreamFailureKind, StreamOwnerKind,
    },
};
use conn::{BlockingPool, Shared};
use errors::{Call, Mapped};
use keys::{
    ChainKeys, activity_owner_pattern, owner_pattern, session_field, workflow_activity_pattern,
};
use prost::Message;
use redis::{Cmd, Script, Value, streams::StreamReadReply};
use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::Instant;

const NAME: &str = "redis";
const RECORD_FIELD: &str = "r";
const TOMBSTONE_GRACE: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const CLOSED_FIELD: &str = "closed";
/// The meta field that holds the topic's first close, a serialized `StoredClose`.
const CLOSE_FIELD: &str = "close";
/// The meta field that holds the newest position the archive holds.
const ARCHIVED_FIELD: &str = "archived";
const DELETE_BATCH: usize = 500;

const CAPABILITIES: Capabilities = Capabilities {
    producer_state: true,
    conditional: ConditionForms {
        tail: false,
        fence: false,
    },
    kept_last: true,
    holds: true,
    change_feed: true,
    cheap_logs: true,
    delete_owner: true,
    topic_listing: true,
    long_poll: true,
    dense_positions: false,
    replay_horizon: None,
    max_record_bytes: MAX_RECORD_BYTES,
    max_batch_bytes: MAX_BATCH_BYTES,
};

/// How to reach Redis and how long streams live there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisStoreOptions {
    /// One `redis://` or `rediss://` URL, or the seed nodes of a cluster. Credentials and TLS
    /// come from the URL.
    pub urls: Vec<String>,
    /// Whether `urls` name a Redis Cluster.
    pub cluster: bool,
    /// Prepended to every key, so streams can share a Redis with other data and an ACL can scope
    /// them.
    pub key_prefix: String,
    /// How long a stream keeps a record, and how long after its last write the stream lives.
    pub retention: Duration,
    /// How many reads may wait on one node at once. Each holds a connection of its own.
    pub blocking_reads_per_node: usize,
    /// How long a short call waits for its reply.
    pub response_timeout: Duration,
    /// How long a positions-only reader's hold lasts after its last read. `None` uses the
    /// retention.
    pub floor_lease: Option<Duration>,
    /// How long a hold lasts after its reader released it.
    pub floor_release_grace: Duration,
    /// Each append also writes an entry to its slot's notification stream, which a driver reads.
    pub notification_log: bool,
    /// How many doorbells the notification log rings, each a stream that a driver blocks on.
    pub notify_doorbells: u32,
    /// With an archive, how long the archive's hold on a topic lasts after the archive last
    /// moved it. Each topic's first append makes the hold, so retention drops only what the
    /// archive holds while the archive keeps up. When the archive stops for longer, the hold
    /// ends and retention goes on. Needs `notification_log`, since a driver archives.
    pub archive_lease: Option<Duration>,
    /// Runs with no Lua: no dedupe entries, no close flags and no topic meta. Core then takes
    /// the fallbacks of producer state and the kept last position, as on a store that has
    /// only a log. For tests of those fallbacks on a real store.
    pub plain: bool,
}

impl RedisStoreOptions {
    /// The defaults for one Redis at `url`.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            urls: vec![url.into()],
            cluster: false,
            key_prefix: "temporal-streams".to_string(),
            retention: Duration::from_secs(7 * 24 * 60 * 60),
            blocking_reads_per_node: 256,
            response_timeout: Duration::from_secs(5),
            floor_lease: None,
            floor_release_grace: Duration::from_secs(24 * 60 * 60),
            notification_log: false,
            notify_doorbells: DEFAULT_NOTIFY_DOORBELLS,
            archive_lease: None,
            plain: false,
        }
    }
}

/// How many doorbells a notification log rings when the options don't say.
pub const DEFAULT_NOTIFY_DOORBELLS: u32 = 16;

/// A [StreamStore] on Redis.
pub struct RedisStore {
    shared: Shared,
    blocking: BlockingPool,
    prefix: String,
    retention_ms: u64,
    grace_ms: u64,
    floor_lease_ms: u64,
    floor_release_grace_ms: u64,
    log: Option<Arc<log::RedisLog>>,
    archive_lease_ms: Option<u64>,
    plain: bool,
}

impl std::fmt::Debug for RedisStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisStore")
            .field("prefix", &self.prefix)
            .field("retention_ms", &self.retention_ms)
            .field("plain", &self.plain)
            .finish_non_exhaustive()
    }
}

impl RedisStore {
    /// Connects to Redis.
    ///
    /// Fails as refused when the options can't work, and as a storage failure when Redis can't
    /// be reached.
    pub async fn connect(options: RedisStoreOptions) -> StreamResult<Self> {
        let Some(seed) = options.urls.first() else {
            return Err(StreamError::refused("the Redis store needs a URL"));
        };
        if options.key_prefix.is_empty() {
            return Err(StreamError::refused("the key prefix must not be empty"));
        }
        let retention_ms = u64::try_from(options.retention.as_millis()).unwrap_or(u64::MAX);
        if retention_ms == 0 {
            return Err(StreamError::refused(format!(
                "the retention must be at least a millisecond, not {:?}",
                options.retention
            )));
        }
        if options.plain && (options.notification_log || options.archive_lease.is_some()) {
            return Err(StreamError::refused(
                "the Redis store in plain mode runs no script, so it keeps no notification log \
                 and no archive hold",
            ));
        }
        if options.archive_lease.is_some() && !options.notification_log {
            return Err(StreamError::refused(
                "an archive needs the notification log, since a driver archives",
            ));
        }
        // The scripts trim below `now - retention` as a stream id, which can't be negative.
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_millis() as u64);
        if retention_ms >= now_ms {
            return Err(StreamError::refused(format!(
                "the retention must be shorter than the time since 1970, not {:?}",
                options.retention
            )));
        }
        let shared = Shared::connect(&options.urls, options.cluster, options.response_timeout)
            .await
            .mapped(Call::Read)?;
        check_server(&shared).await?;
        let blocking = BlockingPool::new(
            seed,
            options.blocking_reads_per_node,
            options.cluster,
            options.response_timeout,
        )
        .mapped(Call::Read)?;
        let log = if options.notification_log {
            // Doorbell reads block, so they get connections of their own.
            let doorbell_reads = BlockingPool::new(
                seed,
                options.notify_doorbells.max(1) as usize,
                options.cluster,
                options.response_timeout,
            )
            .mapped(Call::Read)?;
            Some(Arc::new(log::RedisLog::new(
                shared.clone(),
                doorbell_reads,
                options.key_prefix.clone(),
                options.notify_doorbells,
                options.archive_lease.is_some(),
            )))
        } else {
            None
        };
        Ok(Self {
            log,
            shared,
            blocking,
            prefix: options.key_prefix,
            retention_ms,
            grace_ms: TOMBSTONE_GRACE.as_millis() as u64,
            floor_lease_ms: options
                .floor_lease
                .map_or(retention_ms, |lease| millis(lease).max(1)),
            floor_release_grace_ms: millis(options.floor_release_grace),
            archive_lease_ms: options.archive_lease.map(|lease| millis(lease).max(1)),
            plain: options.plain,
        })
    }

    /// Names who closed the topic on a refusal, from the topic's first close. The close is kept
    /// once and never changes, so reading it after the refusal gives the close that refused.
    async fn with_closed_by(
        &self,
        error: StreamError,
        keys: &ChainKeys,
        topic: &str,
    ) -> StreamError {
        let close: Option<Vec<u8>> = redis::cmd("HGET")
            .arg(keys.meta(topic))
            .arg(CLOSE_FIELD)
            .query_async(&mut self.shared.clone())
            .await
            .unwrap_or_default();
        let closed_by = close
            .and_then(|bytes| StoredClose::decode(bytes.as_slice()).ok())
            .map(|close| close.closed_by)
            .unwrap_or_default();
        error.with_closed_by(closed_by)
    }

    /// Takes deleted keys out of their slot's archive marks. A mark of a deleted topic stays
    /// lowest in its slot, and enough of them stop the slot's other topics from being archived.
    async fn drop_archive_marks(&self, keys: &[String]) -> StreamResult<()> {
        if self.archive_lease_ms.is_none() {
            return Ok(());
        }
        let mut by_slot: HashMap<u16, Vec<&String>> = HashMap::new();
        for key in keys {
            by_slot.entry(conn::slot(key)).or_default().push(key);
        }
        for (slot, keys) in by_slot {
            let _: u64 = redis::cmd("ZREM")
                .arg(keys::archive_pending(&self.prefix, slot))
                .arg(keys)
                .query_async(&mut self.shared.clone())
                .await
                .mapped(Call::Write)?;
        }
        Ok(())
    }

    /// Appends the batch with `XADD`s in one transaction, each trimming by age as it lands.
    async fn append_plain(
        &self,
        request: &StoreAppendRequest,
    ) -> StreamResult<StoreAppendResponse> {
        let log = self.keys(request.chain.as_ref())?.log(&request.topic);
        // The client's clock, since no script can read the server's in the same step. A skew
        // of seconds moves the trim by seconds, against a retention of days.
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_millis() as u64);
        let floor = format!("{}-0", now_ms.saturating_sub(self.retention_ms));
        let mut pipe = redis::pipe();
        pipe.atomic();
        for record in &request.records {
            pipe.cmd("XADD")
                .arg(&log)
                .arg("MINID")
                .arg(&floor)
                .arg("*")
                .arg(RECORD_FIELD)
                .arg(record.as_slice());
        }
        pipe.cmd("PEXPIRE")
            .arg(&log)
            .arg(self.retention_ms)
            .ignore();
        let ids: Vec<String> = pipe
            .query_async(&mut self.shared.clone())
            .await
            .mapped(Call::Append)?;
        let (Some(first), Some(last)) = (ids.first(), ids.last()) else {
            return Err(StreamError::outcome_unknown(
                "Redis answered the append with no stream ids",
            ));
        };
        Ok(StoreAppendResponse {
            first_position: first.clone(),
            last_position: last.clone(),
        })
    }

    /// Reads with `XREAD` alone. With no topic meta, the read can't tell lost records apart, and
    /// Core decides expiry from the record at the cursor.
    async fn read_plain(&self, request: &StoreReadRequest) -> StreamResult<StoreReadResponse> {
        let keys = self.keys(request.chain.as_ref())?;
        let topic = request.topic.as_str();
        if !request.after_position.is_empty() {
            entry(&request.after_position)?;
        }
        let after = match request.after_position.as_str() {
            "" => "0-0",
            position => position,
        };
        let wait = wait_of(request);
        let deadline = Instant::now() + wait;
        let xread = Self::xread(&keys, topic, after, request.max_records, None);
        let reply: Value = xread
            .query_async(&mut self.shared.clone())
            .await
            .mapped(Call::Read)?;
        let mut found = records(reply)?;
        if found.is_empty() && !wait.is_zero() {
            let xread = Self::xread(&keys, topic, after, request.max_records, Some(wait));
            if let Some(reply) = self
                .blocking
                .query(&keys.log(topic), &xread, deadline)
                .await
                .mapped(Call::Read)?
            {
                found = records(reply)?;
            }
        }
        Ok(StoreReadResponse {
            records: found,
            ..Default::default()
        })
    }

    fn holds(&self) -> StreamResult<()> {
        if self.plain {
            return Err(StreamError::unsupported(
                "the Redis store in plain mode can't hold records for a positions-only reader",
            ));
        }
        Ok(())
    }

    fn producer_state(&self) -> StreamResult<()> {
        if self.plain {
            return Err(StreamError::unsupported(
                "the Redis store in plain mode keeps no producer state",
            ));
        }
        Ok(())
    }

    fn keys(&self, chain: Option<&ChainId>) -> StreamResult<ChainKeys> {
        let chain = chain
            .ok_or_else(|| StreamError::refused("a store call needs the chain it is about"))?;
        Ok(ChainKeys::new(&self.prefix, chain))
    }

    /// What a read needs to know besides the records, in one script so a cluster answers it
    /// from the slot the chain's keys share. By hash, unless the server answered it lost the
    /// script, since the text costs every read its bytes and a parse.
    fn checks(keys: &ChainKeys, topic: &str, by_hash: bool) -> Cmd {
        let mut eval = if by_hash {
            let mut eval = redis::cmd("EVALSHA");
            eval.arg(TOPIC_STATE_SCRIPT.get_hash());
            eval
        } else {
            let mut eval = redis::cmd("EVAL");
            eval.arg(TOPIC_STATE);
            eval
        };
        eval.arg(4)
            .arg(keys.log(topic))
            .arg(keys.meta(topic))
            .arg(keys.chain())
            .arg(keys.meta(topic))
            .arg(CLOSED_FIELD);
        eval
    }

    fn xread(
        keys: &ChainKeys,
        topic: &str,
        after: &str,
        count: u32,
        block: Option<Duration>,
    ) -> Cmd {
        let mut xread = redis::cmd("XREAD");
        xread.arg("COUNT").arg(count.max(1));
        if let Some(block) = block {
            xread.arg("BLOCK").arg(block.as_millis().max(1) as u64);
        }
        xread.arg("STREAMS").arg(keys.log(topic)).arg(after);
        xread
    }
}

/// Refuses a server older than Redis 7.0, and warns about one that may evict stream keys.
async fn check_server(shared: &Shared) -> StreamResult<()> {
    let mut info = redis::cmd("INFO");
    info.arg("server");
    for (node, reply) in shared.on_each_primary(info).await.mapped(Call::Read)? {
        let text: String = redis::from_redis_value(reply).map_err(|error| {
            StreamError::storage(format!("Redis answered INFO with no text: {error}"))
        })?;
        check_version(&node, &text)?;
    }
    let mut policy = redis::cmd("CONFIG");
    policy.arg("GET").arg("maxmemory-policy");
    match shared.on_each_primary(policy).await {
        Ok(replies) => {
            let policies: Vec<_> = replies
                .into_iter()
                .map(|(node, reply)| (node, eviction_policy(reply)))
                .collect();
            if let Some(warning) = eviction_warning(&policies) {
                tracing::warn!("{warning}");
            }
        }
        // Managed services often refuse CONFIG. The policy is theirs to document then.
        Err(error) => tracing::debug!("Could not read maxmemory-policy: {error}"),
    }
    Ok(())
}

fn check_version(node: &str, info: &str) -> StreamResult<()> {
    let version = info
        .lines()
        .find_map(|line| line.trim().strip_prefix("redis_version:"))
        .unwrap_or("0");
    let major: u32 = version
        .split('.')
        .next()
        .and_then(|major| major.parse().ok())
        .unwrap_or(0);
    if major < 7 {
        let at = if node.is_empty() {
            String::new()
        } else {
            format!(" at {node}")
        };
        return Err(StreamError::unsupported(format!(
            "the Redis store needs Redis 7.0 or later, but the server{at} reports redis_version \
             {version}"
        )));
    }
    Ok(())
}

fn eviction_policy(reply: Value) -> Option<String> {
    let pairs: Vec<(String, String)> = match reply {
        Value::Map(pairs) => pairs
            .into_iter()
            .filter_map(|(name, value)| {
                Some((
                    redis::from_redis_value(name).ok()?,
                    redis::from_redis_value(value).ok()?,
                ))
            })
            .collect(),
        other => {
            let flat: Vec<String> = redis::from_redis_value(other).ok()?;
            flat.chunks(2)
                .filter_map(|pair| Some((pair.first()?.clone(), pair.get(1)?.clone())))
                .collect()
        }
    };
    pairs
        .into_iter()
        .find(|(name, _)| name == "maxmemory-policy")
        .map(|(_, value)| value)
}

fn eviction_warning(policies: &[(String, Option<String>)]) -> Option<String> {
    let (node, policy) = policies.iter().find_map(|(node, policy)| {
        policy
            .as_deref()
            .filter(|policy| !policy.is_empty() && *policy != "noeviction")
            .map(|policy| (node, policy))
    })?;
    let at = if node.is_empty() {
        String::new()
    } else {
        format!(" at {node}")
    };
    Some(format!(
        "Redis{at} has maxmemory-policy {policy:?}. Under memory pressure Redis may evict whole \
         stream keys, losing records and their dedupe state. Use \"noeviction\" for streams."
    ))
}

/// A Redis stream id as its two numbers, so ids compare in log order.
fn entry(id: &str) -> StreamResult<(u64, u64)> {
    let (milliseconds, sequence) = id.split_once('-').unwrap_or((id, "0"));
    match (milliseconds.parse(), sequence.parse()) {
        (Ok(milliseconds), Ok(sequence)) => Ok((milliseconds, sequence)),
        _ => Err(StreamError::cursor(format!(
            "{id:?} is not a Redis stream id"
        ))),
    }
}

/// The records of an `XREAD` reply, in log order. An entry without a record reads as empty bytes,
/// which no reader takes for a record.
fn records(reply: Value) -> StreamResult<Vec<StoredRecord>> {
    let reply: Option<StreamReadReply> = redis::from_redis_value(reply).map_err(|error| {
        StreamError::storage(format!(
            "Redis answered XREAD with no stream entries: {error}"
        ))
    })?;
    Ok(reply
        .into_iter()
        .flat_map(|reply| reply.keys)
        .flat_map(|key| key.ids)
        .map(|id| StoredRecord {
            record: match id.map.get(RECORD_FIELD) {
                Some(Value::BulkString(bytes)) => bytes.clone(),
                _ => Vec::new(),
            },
            position: id.id,
        })
        .collect())
}

/// Reads whether the log is there, what its meta says and both close flags. Lua turns a missing
/// value into `false`, which keeps the reply's places where a `nil` would cut the list short.
const TOPIC_STATE: &str = "\
return {redis.call('EXISTS', KEYS[1]), redis.call('EXISTS', KEYS[2]), \
redis.call('HGET', KEYS[2], 'trimmed'), redis.call('HGET', KEYS[2], 'last'), \
redis.call('HGET', KEYS[3], ARGV[1]), redis.call('HGET', KEYS[4], ARGV[1]), \
redis.call('HGET', KEYS[2], 'close')}";

static TOPIC_STATE_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(TOPIC_STATE));

fn no_script(error: &redis::RedisError) -> bool {
    error.kind() == redis::ErrorKind::Server(redis::ServerErrorKind::NoScript)
}

/// What the store knows about a topic besides its records.
#[derive(Debug, Default)]
struct TopicState {
    log_exists: bool,
    meta_exists: bool,
    /// The newest id retention trimmed from the log.
    trimmed: Option<String>,
    /// The newest id the log ever took. It outlives the log in the meta.
    last: Option<String>,
    chain_closed: bool,
    topic_closed: bool,
    /// The topic's first close, when it was closed as a topic.
    close: Option<StoredClose>,
}

impl redis::FromRedisValue for TopicState {
    fn from_redis_value(value: Value) -> Result<Self, redis::ParsingError> {
        let text = |value: &Value| match value {
            Value::BulkString(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
            _ => None,
        };
        let number = |value: &Value| matches!(value, Value::Int(n) if *n > 0);
        let Value::Array(values) = value else {
            return Err(format!("a topic state is a list, not {value:?}").into());
        };
        let [log, meta, trimmed, last, chain_closed, topic_closed, close] = values.as_slice()
        else {
            return Err(format!("a topic state has seven values, not {}", values.len()).into());
        };
        let close = match close {
            Value::BulkString(bytes) => Some(
                StoredClose::decode(bytes.as_slice())
                    .map_err(|error| format!("a topic's close does not decode: {error}"))?,
            ),
            _ => None,
        };
        Ok(Self {
            log_exists: number(log),
            meta_exists: number(meta),
            trimmed: text(trimmed),
            last: text(last),
            chain_closed: text(chain_closed).is_some(),
            topic_closed: text(topic_closed).is_some(),
            close,
        })
    }
}

impl TopicState {
    /// Why a read after `position` can't go on, if the store lost records after it.
    ///
    /// With the log there, records after the position are lost when retention trimmed past it
    /// and the read did not get them first. With the log gone but its meta left, they are lost
    /// when the log took records after the position. With neither, nothing is known. A read
    /// from the beginning loses nothing, since anything trimmed was behind its start.
    fn lost(
        &self,
        topic: &str,
        position: Option<(&str, (u64, u64))>,
        found: &[StoredRecord],
    ) -> StreamResult<Option<StreamError>> {
        let Some((position, at)) = position else {
            return Ok(None);
        };
        if let Some(last) = &self.last
            && at > entry(last)?
        {
            // The memory store refuses this too. A failover that lost the tail also lands here,
            // and a read that went on would skip the records written after it.
            return Ok(Some(StreamError::cursor(format!(
                "position {position} is one topic {topic:?} never held"
            ))));
        }
        if !self.log_exists && found.is_empty() {
            if !self.meta_exists {
                return Ok(Some(StreamError::not_found(format!(
                    "topic {topic:?} keeps no log and no tombstone, so nothing is known about \
                     what followed {position}"
                ))));
            }
            if let Some(last) = &self.last
                && entry(last)? > at
            {
                return Ok(Some(StreamError::expired(format!(
                    "the log of topic {topic:?} expired with records after {position}"
                ))));
            }
            return Ok(None);
        }
        let Some(trimmed) = &self.trimmed else {
            return Ok(None);
        };
        let watermark = entry(trimmed)?;
        let first = found
            .first()
            .map(|record| entry(&record.position))
            .transpose()?;
        if watermark > at && first.is_none_or(|first| watermark < first) {
            return Ok(Some(StreamError::expired(format!(
                "records after {position} on topic {topic:?} were dropped by retention while \
                 this read was behind them; the newest dropped one is {trimmed}"
            ))));
        }
        Ok(None)
    }
}

fn wait_of(request: &StoreReadRequest) -> Duration {
    request.wait.as_ref().map_or(Duration::ZERO, |wait| {
        Duration::new(
            u64::try_from(wait.seconds).unwrap_or(0),
            u32::try_from(wait.nanos).unwrap_or(0),
        )
    })
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[async_trait::async_trait]
impl StreamStore for RedisStore {
    fn name(&self) -> &str {
        NAME
    }

    fn notification_log(&self) -> Option<Arc<dyn NotificationLog>> {
        self.log.clone().map(|log| log as Arc<dyn NotificationLog>)
    }

    fn capabilities(&self) -> Capabilities {
        if self.plain {
            return Capabilities {
                producer_state: false,
                kept_last: false,
                holds: false,
                change_feed: false,
                ..CAPABILITIES
            };
        }
        Capabilities {
            change_feed: self.log.is_some(),
            ..CAPABILITIES
        }
    }

    async fn append(&self, request: StoreAppendRequest) -> StreamResult<StoreAppendResponse> {
        if request.records.is_empty() {
            return Err(StreamError::refused("an append needs at least one record"));
        }
        if request.digest.is_empty() {
            return Err(StreamError::refused("an append needs the batch's digest"));
        }
        if self.plain {
            return self.append_plain(&request).await;
        }
        let keys = self.keys(request.chain.as_ref())?;
        let mut script = match (&self.log, self.archive_lease_ms) {
            (Some(_), Some(_)) => scripts::APPEND_ARCHIVED_SCRIPT.prepare_invoke(),
            (Some(_), None) => scripts::APPEND_LOGGED_SCRIPT.prepare_invoke(),
            (None, _) => scripts::APPEND_SCRIPT.prepare_invoke(),
        };
        script
            .key(keys.log(&request.topic))
            .key(keys.meta(&request.topic))
            .key(keys.chain())
            .key(keys.floors(&request.topic));
        if self.log.is_some() {
            script.key(keys::notifications(&self.prefix, keys.slot()));
        }
        if self.archive_lease_ms.is_some() {
            script.key(keys::archive_pending(&self.prefix, keys.slot()));
        }
        if request.chain.as_ref().is_some_and(ChainKeys::keeps_topics) {
            script.key(keys.topics());
        }
        script
            .arg(self.retention_ms)
            .arg(self.grace_ms)
            .arg(session_field(&request.producer_id, request.attempt))
            .arg(request.sequence)
            .arg(hex(&request.digest));
        if let Some(lease) = self.archive_lease_ms {
            script.arg(lease);
        }
        for record in &request.records {
            script.arg(record.as_slice());
        }
        let appended: StreamResult<Vec<String>> = script
            .invoke_async(&mut self.shared.clone())
            .await
            .mapped(Call::Append);
        let answer = match appended {
            Err(error) if error.kind == StreamFailureKind::Closed => {
                return Err(self.with_closed_by(error, &keys, &request.topic).await);
            }
            appended => appended?,
        };
        let [first_position, last_position, rest @ ..] = answer.as_slice() else {
            return Err(StreamError::storage(format!(
                "Redis answered the append with {answer:?}"
            )));
        };
        // The entry made a slot's stream non-empty, so a driver may be asleep on its doorbell.
        if let (Some(log), [length]) = (&self.log, rest)
            && length == "1"
        {
            log.ring(keys.slot()).await;
        }
        let (first_position, last_position) = (first_position.clone(), last_position.clone());
        Ok(StoreAppendResponse {
            first_position,
            last_position,
        })
    }

    async fn read(&self, request: StoreReadRequest) -> StreamResult<StoreReadResponse> {
        if self.plain {
            return self.read_plain(&request).await;
        }
        let keys = self.keys(request.chain.as_ref())?;
        let topic = request.topic.as_str();
        let position = match request.after_position.as_str() {
            "" => None,
            position => Some((position, entry(position)?)),
        };
        let after = position.map_or("0-0", |(position, _)| position);
        let wait = wait_of(&request);
        let deadline = Instant::now() + wait;
        let first = async |by_hash| {
            let mut pipeline = redis::pipe();
            pipeline.add_command(Self::xread(&keys, topic, after, request.max_records, None));
            pipeline.add_command(Self::checks(&keys, topic, by_hash));
            pipeline
                .query_async::<(Value, TopicState)>(&mut self.shared.clone())
                .await
        };
        let (reply, mut state) = match first(true).await {
            Err(error) if no_script(&error) => first(false).await,
            answer => answer,
        }
        .mapped(Call::Read)?;
        let mut found = records(reply)?;
        if found.is_empty() && !wait.is_zero() && state.lost(topic, position, &found)?.is_none() {
            let xread = Self::xread(&keys, topic, after, request.max_records, Some(wait));
            let blocked = self
                .blocking
                .query(&keys.log(topic), &xread, deadline)
                .await
                .mapped(Call::Read)?;
            if let Some(reply) = blocked {
                found = records(reply)?;
                // Read again after the wait, so a trim or a close that raced it is seen.
                state = match Self::checks(&keys, topic, true)
                    .query_async(&mut self.shared.clone())
                    .await
                {
                    Err(error) if no_script(&error) => {
                        Self::checks(&keys, topic, false)
                            .query_async(&mut self.shared.clone())
                            .await
                    }
                    answer => answer,
                }
                .mapped(Call::Read)?;
            }
        }
        if let Some(error) = state.lost(topic, position, &found)? {
            return Err(error);
        }
        Ok(StoreReadResponse {
            records: found,
            closed: state.chain_closed || state.topic_closed,
            close: state.close.filter(|_| state.topic_closed),
        })
    }

    async fn latest(&self, request: StoreLatestRequest) -> StreamResult<StoreLatestResponse> {
        let keys = self.keys(request.chain.as_ref())?;
        let newest: Vec<(String, Value)> = redis::cmd("XREVRANGE")
            .arg(keys.log(&request.topic))
            .arg("+")
            .arg("-")
            .arg("COUNT")
            .arg(1)
            .query_async(&mut self.shared.clone())
            .await
            .mapped(Call::Read)?;
        Ok(StoreLatestResponse {
            position: newest
                .into_iter()
                .next()
                .map(|(id, _)| id)
                .unwrap_or_default(),
        })
    }

    async fn record_at(
        &self,
        chain: &ChainId,
        topic: &str,
        position: &str,
    ) -> StreamResult<Option<Vec<u8>>> {
        entry(position)?;
        let keys = ChainKeys::new(&self.prefix, chain);
        let found: Vec<(String, HashMap<String, Value>)> = redis::cmd("XRANGE")
            .arg(keys.log(topic))
            .arg(position)
            .arg(position)
            .query_async(&mut self.shared.clone())
            .await
            .mapped(Call::Read)?;
        Ok(found
            .into_iter()
            .next()
            .map(|(_, fields)| match fields.get(RECORD_FIELD) {
                Some(Value::BulkString(bytes)) => bytes.clone(),
                _ => Vec::new(),
            }))
    }

    async fn newest_ever(&self, chain: &ChainId, topic: &str) -> StreamResult<String> {
        if self.plain {
            return Ok(String::new());
        }
        let last: Option<String> = redis::cmd("HGET")
            .arg(ChainKeys::new(&self.prefix, chain).meta(topic))
            .arg("last")
            .query_async(&mut self.shared.clone())
            .await
            .mapped(Call::Read)?;
        Ok(last.unwrap_or_default())
    }

    async fn dropped_after(
        &self,
        chain: &ChainId,
        topic: &str,
        mark: &str,
    ) -> StreamResult<Option<String>> {
        if self.plain {
            return Ok(None);
        }
        let trimmed: Option<String> = redis::cmd("HGET")
            .arg(ChainKeys::new(&self.prefix, chain).meta(topic))
            .arg("trimmed")
            .query_async(&mut self.shared.clone())
            .await
            .mapped(Call::Read)?;
        let Some(trimmed) = trimmed else {
            return Ok(None);
        };
        // A log that expired comes back with `trimmed` set to its old `last`, which is the mark
        // itself, so a revive counts as no loss.
        if !mark.is_empty() && entry(&trimmed)? <= entry(mark)? {
            return Ok(None);
        }
        Ok(Some(trimmed))
    }

    async fn hold_floor(
        &self,
        chain: &ChainId,
        topic: &str,
        holder: &str,
        position: &str,
    ) -> StreamResult<()> {
        self.holds()?;
        let keys = ChainKeys::new(&self.prefix, chain);
        // Redis never stores `0-0`, so `0-1` holds every record, and the trim's exclusive range
        // below it stays valid. Below it, a crafted `0-0` would fail every later append.
        let position = if position.is_empty() || entry(position)? < (0, 1) {
            "0-1"
        } else {
            position
        };
        scripts::HOLD_SCRIPT
            .key(keys.floors(topic))
            .key(keys.log(topic))
            .key(keys.meta(topic))
            .arg(holder)
            .arg(position)
            .arg(self.floor_lease_ms)
            .arg(self.grace_ms)
            .arg(crate::MAX_FLOOR_HOLDERS)
            .invoke_async::<i64>(&mut self.shared.clone())
            .await
            .mapped(Call::Write)
            .map(drop)
    }

    async fn release_floor(&self, chain: &ChainId, topic: &str, holder: &str) -> StreamResult<()> {
        self.holds()?;
        let keys = ChainKeys::new(&self.prefix, chain);
        let mut script = scripts::RELEASE_SCRIPT.key(keys.floors(topic));
        // The archive releases once it holds a closed topic to its end, so the topic leaves the
        // set to archive in the same step.
        if holder == crate::ARCHIVE_HOLDER && self.archive_lease_ms.is_some() {
            script.key(keys::archive_pending(&self.prefix, keys.slot()));
        }
        script
            .arg(holder)
            .arg(self.floor_release_grace_ms)
            .arg(keys.log(topic))
            .invoke_async::<i64>(&mut self.shared.clone())
            .await
            .mapped(Call::Write)
            .map(drop)
    }

    async fn advance_archive(
        &self,
        chain: &ChainId,
        topic: &str,
        position: &str,
    ) -> StreamResult<()> {
        self.holds()?;
        let Some(lease) = self.archive_lease_ms else {
            return Err(StreamError::unsupported("this store keeps no archive hold"));
        };
        entry(position)?;
        let keys = ChainKeys::new(&self.prefix, chain);
        scripts::ADVANCE_SCRIPT
            .key(keys.floors(topic))
            .key(keys.log(topic))
            .key(keys.meta(topic))
            .key(keys::archive_pending(&self.prefix, keys.slot()))
            .arg(crate::ARCHIVE_HOLDER)
            .arg(position)
            .arg(lease)
            .arg(self.grace_ms)
            .invoke_async::<i64>(&mut self.shared.clone())
            .await
            .mapped(Call::Write)
            .map(drop)
    }

    async fn archived(&self, chain: &ChainId, topic: &str) -> StreamResult<Option<String>> {
        if self.archive_lease_ms.is_none() {
            return Ok(None);
        }
        let (exists, archived): (bool, Option<String>) = redis::pipe()
            .exists(ChainKeys::new(&self.prefix, chain).meta(topic))
            .hget(
                ChainKeys::new(&self.prefix, chain).meta(topic),
                ARCHIVED_FIELD,
            )
            .query_async(&mut self.shared.clone())
            .await
            .mapped(Call::Read)?;
        Ok(exists.then(|| archived.unwrap_or_default()))
    }

    async fn close_chain(&self, chain: &ChainId) -> StreamResult<()> {
        self.producer_state()?;
        let keys = ChainKeys::new(&self.prefix, chain);
        redis::pipe()
            .atomic()
            .hset(keys.chain(), CLOSED_FIELD, "1")
            .pexpire(keys.chain(), (self.retention_ms + self.grace_ms) as i64)
            .query_async::<()>(&mut self.shared.clone())
            .await
            .mapped(Call::Write)
    }

    async fn close_topic(&self, chain: &ChainId, topic: &str) -> StreamResult<()> {
        self.close_topic_with(chain, topic, StoredClose::default())
            .await
            .map(drop)
    }

    async fn close_topic_with(
        &self,
        chain: &ChainId,
        topic: &str,
        close: StoredClose,
    ) -> StreamResult<StoredClose> {
        let keys = ChainKeys::new(&self.prefix, chain);
        if self.plain {
            crate::close::write_close(self, chain, topic, &close).await?;
            return Ok(close);
        }
        // The meta is the topic's tombstone, so the mark and the outcome live as long as the topic
        // is known.
        let mut script = scripts::CLOSE_SCRIPT.key(keys.meta(topic));
        // A closed topic gets nothing new, so the archive takes it first.
        if self.archive_lease_ms.is_some() {
            script.key(keys::archive_pending(&self.prefix, keys.slot()));
        }
        let stored = script
            .arg(close.encode_to_vec())
            .arg(self.retention_ms + self.grace_ms)
            .arg(keys.log(topic))
            .invoke_async::<Vec<u8>>(&mut self.shared.clone())
            .await
            .mapped(Call::Write)?;
        StoredClose::decode(stored.as_slice()).map_err(|error| {
            StreamError::storage(format!("a topic's close does not decode: {error}"))
        })
    }

    async fn retire_producer(
        &self,
        chain: &ChainId,
        topic: &str,
        producer_id: &str,
        attempt: i64,
    ) -> StreamResult<()> {
        self.producer_state()?;
        // One field of one key, so a plain `HDEL` is atomic, and it creates nothing when the meta
        // is gone.
        redis::cmd("HDEL")
            .arg(ChainKeys::new(&self.prefix, chain).meta(topic))
            .arg(session_field(producer_id, attempt))
            .query_async::<()>(&mut self.shared.clone())
            .await
            .mapped(Call::Write)
    }

    async fn written_topics(&self, chain: &ChainId) -> StreamResult<Vec<String>> {
        if !ChainKeys::keeps_topics(chain) {
            return Ok(Vec::new());
        }
        let keys = ChainKeys::new(&self.prefix, chain);
        let logs: Vec<String> = redis::cmd("SMEMBERS")
            .arg(keys.topics())
            .query_async(&mut self.shared.clone())
            .await
            .mapped(Call::Read)?;
        let mut topics = logs
            .iter()
            .map(|log| match keys::chain_of_log(&self.prefix, log) {
                Some((of, topic)) if of == *chain => Ok(topic),
                _ => Err(StreamError::storage(format!(
                    "the topic set of a Workflow Activity holds {log:?}, which is no log key of it"
                ))),
            })
            .collect::<StreamResult<Vec<_>>>()?;
        topics.sort();
        Ok(topics)
    }

    async fn list_topics(&self, chain: &ChainId) -> StreamResult<Vec<String>> {
        let keys = ChainKeys::new(&self.prefix, chain);
        let pattern = keys.topics_pattern();
        let mut topics = Vec::new();
        // The chain's keys share one slot, but which primary holds it is the cluster's business.
        for node in self.shared.primaries().await.mapped(Call::Read)? {
            let mut cursor = "0".to_string();
            loop {
                let (next, found) = self
                    .shared
                    .scan(node.as_ref(), &cursor, &pattern)
                    .await
                    .mapped(Call::Read)?;
                topics.extend(found.iter().filter_map(|key| keys.topic_of(key)));
                if next == "0" {
                    break;
                }
                cursor = next;
            }
        }
        topics.sort_unstable();
        topics.dedup();
        Ok(topics)
    }

    async fn delete_owner(&self, request: DeleteOwnerRequest) -> StreamResult<DeleteOwnerResponse> {
        let patterns = match StreamOwnerKind::try_from(request.owner_kind) {
            Ok(StreamOwnerKind::Workflow) => vec![
                owner_pattern(&self.prefix, &request.namespace, &request.workflow_id),
                workflow_activity_pattern(&self.prefix, &request.namespace, &request.workflow_id),
            ],
            Ok(StreamOwnerKind::Activity) => vec![activity_owner_pattern(
                &self.prefix,
                &request.namespace,
                &request.activity_id,
            )],
            _ => {
                return Err(StreamError::unsupported(format!(
                    "this release keeps streams of Workflows and standalone Activities only, not \
                     owner kind {}",
                    request.owner_kind
                )));
            }
        };
        // An owner id's chains hash to different slots, so every primary is scanned. Some keys
        // may be gone when a later step fails, which is why a failure leaves the outcome unknown.
        let mut deleted = 0;
        let nodes = self.shared.primaries().await.mapped(Call::Write)?;
        for (node, pattern) in nodes
            .iter()
            .flat_map(|node| patterns.iter().map(move |pattern| (node, pattern)))
            .collect::<Vec<_>>()
        {
            let mut cursor = "0".to_string();
            loop {
                let (next, keys) = self
                    .shared
                    .scan(node.as_ref(), &cursor, pattern)
                    .await
                    .mapped(Call::Write)?;
                for batch in keys.chunks(DELETE_BATCH) {
                    let removed: u64 = redis::cmd("UNLINK")
                        .arg(batch)
                        .query_async(&mut self.shared.clone())
                        .await
                        .mapped(Call::Write)?;
                    deleted += removed;
                    self.drop_archive_marks(batch).await?;
                }
                if next == "0" {
                    break;
                }
                cursor = next;
            }
        }
        Ok(DeleteOwnerResponse { deleted })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::StreamFailureKind;

    #[test]
    fn stream_ids_compare_in_log_order() {
        assert!(entry("1-2").unwrap() < entry("1-10").unwrap());
        assert!(entry("9-0").unwrap() < entry("10-0").unwrap());
        assert_eq!(entry("5").unwrap(), (5, 0));
        for bad in ["", "x-1", "1-x", "-1"] {
            assert!(entry(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_server_older_than_redis_7_is_refused() {
        for (info, ok) in [
            ("# Server\r\nredis_version:7.0.0\r\n", true),
            ("redis_version:8.10.1\r\nredis_mode:standalone", true),
            ("redis_version:6.2.14\r\n", false),
            ("no version here", false),
        ] {
            let checked = check_version("", info);
            assert_eq!(checked.is_ok(), ok, "{info:?}");
            if let Err(error) = checked {
                assert_eq!(error.kind, StreamFailureKind::Unsupported);
                assert!(error.message.contains("Redis 7.0"), "{error}");
            }
        }
        let error = check_version("10.0.0.2:7102", "redis_version:6.0.0").unwrap_err();
        assert!(error.message.contains("10.0.0.2:7102"), "{error}");
    }

    #[test]
    fn only_a_policy_that_may_evict_is_warned_about() {
        let primary = |policy: Option<&str>| (String::new(), policy.map(str::to_string));
        assert_eq!(eviction_warning(&[primary(Some("noeviction"))]), None);
        // A server that refused CONFIG, or answered nothing, says nothing either way.
        assert_eq!(eviction_warning(&[primary(None)]), None);
        let warning = eviction_warning(&[
            primary(Some("noeviction")),
            ("10.0.0.3:7103".to_string(), Some("allkeys-lru".to_string())),
        ])
        .unwrap();
        assert!(warning.contains("allkeys-lru"), "{warning}");
        assert!(warning.contains("10.0.0.3:7103"), "{warning}");
    }

    #[test]
    fn the_policy_reads_from_either_reply_shape() {
        let bulk = |text: &str| Value::BulkString(text.as_bytes().to_vec());
        let array = Value::Array(vec![bulk("maxmemory-policy"), bulk("allkeys-lru")]);
        assert_eq!(eviction_policy(array).as_deref(), Some("allkeys-lru"));
        let map = Value::Map(vec![(bulk("maxmemory-policy"), bulk("noeviction"))]);
        assert_eq!(eviction_policy(map).as_deref(), Some("noeviction"));
        assert_eq!(eviction_policy(Value::Nil), None);
    }

    #[test]
    fn the_digest_is_stored_as_lowercase_hex() {
        assert_eq!(hex(&[0xab, 0x01]), "ab01");
    }

    async fn store(per_node: usize) -> Option<RedisStore> {
        let Ok(url) = std::env::var("STREAMS_REDIS_URL") else {
            eprintln!("set STREAMS_REDIS_URL to run the Redis store tests");
            return None;
        };
        let mut options = RedisStoreOptions::new(url);
        options.key_prefix = format!("pool-{}", std::process::id());
        options.blocking_reads_per_node = per_node;
        Some(RedisStore::connect(options).await.unwrap())
    }

    fn chain(workflow_id: &str) -> ChainId {
        ChainId {
            namespace: "ns".to_string(),
            workflow_id: workflow_id.to_string(),
            first_run_id: "run-1".to_string(),
            ..Default::default()
        }
    }

    fn waiting_read(workflow_id: &str, wait: Duration) -> StoreReadRequest {
        StoreReadRequest {
            chain: Some(chain(workflow_id)),
            topic: "out".to_string(),
            wait: Some(wait.try_into().unwrap()),
            max_records: 10,
            ..Default::default()
        }
    }

    fn append(workflow_id: &str, sequence: i64) -> StoreAppendRequest {
        StoreAppendRequest {
            chain: Some(chain(workflow_id)),
            topic: "out".to_string(),
            producer_id: "p".to_string(),
            attempt: 1,
            sequence,
            digest: vec![sequence as u8],
            records: vec![b"\x1a\x03out".to_vec()],
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parked_readers_do_not_slow_appends() {
        let Some(store) = store(256).await else {
            return;
        };
        let store = std::sync::Arc::new(store);
        let run = format!("parked-{}", std::process::id());
        let readers: Vec<_> = (0..100)
            .map(|reader| {
                let store = store.clone();
                let workflow_id = format!("{run}-{reader}");
                tokio::spawn(async move {
                    store
                        .read(waiting_read(&workflow_id, Duration::from_secs(30)))
                        .await
                })
            })
            .collect();
        let parked = Instant::now() + Duration::from_secs(10);
        while store.blocking.in_use() < 100 {
            assert!(
                Instant::now() < parked,
                "{} parked",
                store.blocking.in_use()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut slowest = Duration::ZERO;
        for sequence in 1..=20 {
            let started = Instant::now();
            store.append(append(&run, sequence)).await.unwrap();
            slowest = slowest.max(started.elapsed());
        }
        // On a shared connection the first append would wait behind a parked XREAD for 30 s.
        assert!(
            slowest < Duration::from_secs(1),
            "slowest append {slowest:?}"
        );
        // A cancelled read drops its connection instead of handing it to the next read.
        for reader in &readers {
            reader.abort();
        }
        for reader in readers {
            assert!(reader.await.unwrap_err().is_cancelled());
        }
        assert_eq!(store.blocking.in_use(), 0);
    }

    #[tokio::test]
    async fn in_plain_mode_a_repeat_lands_again_and_a_close_is_a_record() {
        let Ok(url) = std::env::var("STREAMS_REDIS_URL") else {
            return;
        };
        let mut options = RedisStoreOptions::new(url);
        options.key_prefix = format!("plain-{}", std::process::id());
        options.plain = true;
        let store = RedisStore::connect(options).await.unwrap();
        let capabilities = store.capabilities();
        assert!(!capabilities.producer_state && !capabilities.kept_last);
        assert!(capabilities.long_poll && capabilities.topic_listing);
        let run = format!("plain-{}", std::process::id());
        let first = store.append(append(&run, 1)).await.unwrap();
        let again = store.append(append(&run, 1)).await.unwrap();
        assert!(entry(&again.first_position).unwrap() > entry(&first.last_position).unwrap());
        let read = store
            .read(waiting_read(&run, Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(read.records.len(), 2);
        assert!(!read.closed);
        assert_eq!(store.list_topics(&chain(&run)).await.unwrap(), ["out"]);
        let error = store.close_chain(&chain(&run)).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Unsupported);
        // A topic close is a `CLOSE` record, which lands after the two copies.
        store.close_topic(&chain(&run), "out").await.unwrap();
        let read = store
            .read(waiting_read(&run, Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(read.records.len(), 3);
        // A read after a position the log never held answers what follows, as a plain log does.
        let missing = StoreReadRequest {
            after_position: "1-0".to_string(),
            ..waiting_read(&format!("{run}-missing"), Duration::ZERO)
        };
        assert!(store.read(missing).await.unwrap().records.is_empty());
    }

    #[tokio::test]
    async fn a_read_that_finds_the_pool_full_answers_empty_at_its_deadline() {
        let Some(store) = store(1).await else {
            return;
        };
        let store = std::sync::Arc::new(store);
        let run = format!("full-{}", std::process::id());
        let holder = {
            let store = store.clone();
            let run = run.clone();
            tokio::spawn(async move {
                store
                    .read(waiting_read(&format!("{run}-a"), Duration::from_secs(30)))
                    .await
            })
        };
        while store.blocking.in_use() < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let started = Instant::now();
        let read = store
            .read(waiting_read(
                &format!("{run}-b"),
                Duration::from_millis(300),
            ))
            .await
            .unwrap();
        assert!(read.records.is_empty());
        assert!(started.elapsed() < Duration::from_secs(3));
        holder.abort();
    }
}
