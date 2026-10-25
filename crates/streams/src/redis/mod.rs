//! The Redis Store. One Lua script runs each create, append and close, so the fence, dedupe and
//! close checks are atomic with the write. `docs/redis.md` has the layout of the stored data.

mod conn;
mod errors;
mod scripts;

pub use crate::store::{
    Appended, Batch, Closed, CreateOutcome, Created, Entry, FORMAT_VERSION, MAX_BATCH_BYTES,
    MAX_RECORD_BYTES, Owner, StreamState, TOMBSTONE, Withdrawal,
};
use crate::{
    StreamError, StreamResult,
    epoch::{epoch_key, task_key, task_of},
    ids::{StreamKeys, encode_part},
    offset::{entry_id_of_offset, offset_of_entry_id},
    proto::{Producer, ProducerKind, StreamRef},
    store::{Capabilities, Store},
};
use conn::{BlockingPool, Shared};
use errors::Mapped;
use redis::Value;
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

/// The Redis connection options and the Stream TTLs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisStoreOptions {
    /// One `redis://` or `rediss://` URL, or the seed nodes of a cluster. Credentials and TLS
    /// come from the URL.
    pub urls: Vec<String>,
    /// Whether `urls` are the seeds of a Redis Cluster.
    pub cluster: bool,
    /// The prefix of every key, so that Streams can share a Redis with other data.
    pub key_prefix: String,
    /// The Retention of a Stream whose create sets none.
    pub retention: Duration,
    /// How long an open Workflow-owned Stream stays after its last write.
    pub safety_ttl: Duration,
    /// How many blocking reads may wait on one node at once. Each has a connection of its own.
    pub blocking_reads_per_node: usize,
    /// How long a short call waits for its answer.
    pub response_timeout: Duration,
}

impl RedisStoreOptions {
    /// The defaults for one Redis at `url`.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            urls: vec![url.into()],
            cluster: false,
            key_prefix: crate::ids::DEFAULT_KEY_PREFIX.to_string(),
            retention: Duration::from_secs(7 * 24 * 60 * 60),
            safety_ttl: Duration::from_secs(90 * 24 * 60 * 60),
            blocking_reads_per_node: 256,
            response_timeout: Duration::from_secs(5),
        }
    }
}

/// The Redis Store.
#[derive(Clone)]
pub struct RedisStore {
    shared: Shared,
    blocking: Arc<BlockingPool>,
    prefix: String,
    retention: Duration,
    safety_ttl: Duration,
}

impl std::fmt::Debug for RedisStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisStore")
            .field("prefix", &self.prefix)
            .field("retention", &self.retention)
            .field("safety_ttl", &self.safety_ttl)
            .finish_non_exhaustive()
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl RedisStore {
    /// Connects to Redis and checks that it is Redis 7.0 or later.
    pub async fn connect(options: RedisStoreOptions) -> StreamResult<Self> {
        let Some(seed) = options.urls.first() else {
            return Err(StreamError::invalid("The Redis Store needs a URL"));
        };
        if options.key_prefix.is_empty() {
            return Err(StreamError::invalid("The key prefix must not be empty"));
        }
        if options.retention.is_zero() || options.safety_ttl.is_zero() {
            return Err(StreamError::invalid(
                "The Retention and the safety TTL must be above zero",
            ));
        }
        let shared = Shared::connect(&options.urls, options.cluster, options.response_timeout)
            .await
            .mapped()?;
        check_server(&shared).await?;
        let blocking = BlockingPool::new(
            seed,
            options.blocking_reads_per_node,
            options.cluster,
            options.response_timeout,
        )
        .mapped()?;
        Ok(Self {
            shared,
            blocking: Arc::new(blocking),
            prefix: options.key_prefix,
            retention: options.retention,
            safety_ttl: options.safety_ttl,
        })
    }

    /// The Retention of a Stream whose create sets none.
    pub fn default_retention(&self) -> Duration {
        self.retention
    }

    fn keys(&self, namespace: &str, stream_id: &str) -> StreamKeys {
        StreamKeys::new(&self.prefix, namespace, stream_id)
    }

    /// Creates a Stream, or finds it. A retry run's create withdraws its previous run. A
    /// Workflow-owned Stream stores the run chain of its ref, for the owner check of a close.
    pub async fn create(
        &self,
        namespace: &str,
        stream: &StreamRef,
        retention: Option<Duration>,
        owner: Owner<'_>,
        withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Created> {
        let keys = self.keys(namespace, &stream.id);
        let retention = retention.filter(|r| !r.is_zero()).unwrap_or(self.retention);
        let reply: Value = scripts::CREATE_SCRIPT
            .key(keys.meta())
            .key(keys.log())
            .arg(FORMAT_VERSION)
            .arg(millis(retention))
            .arg(match owner {
                Owner::Workflow(_) => "wf",
                Owner::Client => "client",
                Owner::Activity { .. } => return Err(crate::memory::activity_owner()),
            })
            .arg(millis(self.safety_ttl))
            .arg(millis(TOMBSTONE))
            .arg(encode_run(withdrawal.withdraw_run))
            .arg(withdrawal.supersession)
            .arg(encode_run(&stream.workflow_id))
            .invoke_async(&mut self.shared.clone())
            .await
            .mapped()?;
        let [outcome, stored, flag] = strings::<3>(reply, "create")?;
        let outcome = match outcome.as_str() {
            "created" => CreateOutcome::Created,
            "exists" => CreateOutcome::Exists,
            "conflict" => CreateOutcome::Conflict,
            "closed" => CreateOutcome::Closed,
            "expired" => CreateOutcome::Expired,
            other => return Err(bad_reply("create", other)),
        };
        let stored: u64 = stored.parse().map_err(|_| bad_reply("create", &stored))?;
        Ok(Created {
            outcome,
            retention: Duration::from_millis(stored),
            flagged: flag == "1",
        })
    }

    /// Appends one batch.
    pub async fn append(
        &self,
        namespace: &str,
        stream_id: &str,
        batch: &Batch<'_>,
    ) -> StreamResult<Appended> {
        let keys = self.keys(namespace, stream_id);
        let producer = batch.producer;
        let epoch = producer.epoch.unwrap_or_default();
        let withdrawal = &batch.withdrawal;
        let mut invocation = scripts::APPEND_SCRIPT.prepare_invoke();
        invocation
            .key(keys.meta())
            .key(keys.log())
            .arg(FORMAT_VERSION)
            .arg(producer.kind)
            .arg(&producer.id)
            .arg(encode_run(&producer.run_id))
            .arg(epoch_key(&epoch))
            .arg(batch.sequence)
            .arg(encode_run(withdrawal.withdraw_run))
            .arg(withdrawal.supersession)
            .arg(millis(self.safety_ttl))
            .arg(millis(TOMBSTONE))
            .arg(local_activity_task(producer))
            .arg(completed_arg(withdrawal))
            .arg(failed_list(withdrawal))
            .arg(withdrawal.tasks.map_or(&[][..], |tasks| tasks.supersession))
            .arg(batch.digest);
        for record in batch.records {
            invocation.arg(record.as_slice());
        }
        let reply: Value = invocation
            .invoke_async(&mut self.shared.clone())
            .await
            .mapped()?;
        let [outcome, first, last, flag] = strings::<4>(reply, "append")?;
        let offsets = match (first.as_str(), last.as_str()) {
            ("", _) | (_, "") => None,
            (first, last) => Some((offset_of_entry_id(first)?, offset_of_entry_id(last)?)),
        };
        Ok(Appended {
            offsets,
            resend: outcome == "resend",
            flagged: flag == "1",
        })
    }

    /// Closes a Stream with an encoded close Record, if the Producer's kind and the ref's run
    /// chain are the owner's.
    pub async fn close(
        &self,
        namespace: &str,
        stream: &StreamRef,
        producer: &Producer,
        record: &[u8],
    ) -> StreamResult<Closed> {
        let keys = self.keys(namespace, &stream.id);
        let epoch = producer.epoch.unwrap_or_default();
        let reply: Value = scripts::CLOSE_SCRIPT
            .key(keys.meta())
            .key(keys.log())
            .arg(FORMAT_VERSION)
            .arg(producer.kind)
            .arg(&producer.id)
            .arg(encode_run(&producer.run_id))
            .arg(epoch_key(&epoch))
            .arg(millis(TOMBSTONE))
            .arg(record)
            .arg(encode_run(&stream.workflow_id))
            .invoke_async(&mut self.shared.clone())
            .await
            .mapped()?;
        let [outcome, id] = strings::<2>(reply, "close")?;
        Ok(Closed {
            offset: offset_of_entry_id(&id)?,
            already: outcome == "already",
        })
    }

    /// Reads the Stream's state and up to `count` entries from `from`, inclusive. Zero reads
    /// from the start of the log.
    pub async fn read(
        &self,
        namespace: &str,
        stream_id: &str,
        from: i64,
        count: usize,
    ) -> StreamResult<(StreamState, Vec<Entry>)> {
        let keys = self.keys(namespace, stream_id);
        let start = if from > 0 {
            entry_id_of_offset(from)
        } else {
            "-".to_string()
        };
        let reply: Value = scripts::READ_SCRIPT
            .key(keys.meta())
            .key(keys.log())
            .arg(start)
            .arg(count)
            .invoke_async(&mut self.shared.clone())
            .await
            .mapped()?;
        read_reply(reply)
    }

    /// Reads up to `count` entries at or before `before`, newest first, with `XREVRANGE`.
    pub async fn read_back(
        &self,
        namespace: &str,
        stream_id: &str,
        before: i64,
        count: usize,
    ) -> StreamResult<Vec<Entry>> {
        let keys = self.keys(namespace, stream_id);
        let reply: Value = redis::cmd("XREVRANGE")
            .arg(keys.log())
            .arg(entry_id_of_offset(before))
            .arg("-")
            .arg("COUNT")
            .arg(count)
            .query_async(&mut self.shared.clone())
            .await
            .mapped()?;
        let Value::Array(entries) = reply else {
            return Err(bad_reply("read back", reply));
        };
        entries.into_iter().map(entry).collect()
    }

    /// Waits until the log has an entry after `after`, or `deadline` passes. Returns whether an
    /// entry came. The wait uses a dedicated connection.
    pub async fn wait_after(
        &self,
        namespace: &str,
        stream_id: &str,
        after: i64,
        deadline: Instant,
    ) -> StreamResult<bool> {
        let keys = self.keys(namespace, stream_id);
        let wait = deadline.saturating_duration_since(Instant::now());
        if wait.is_zero() {
            return Ok(false);
        }
        let after = if after > 0 {
            entry_id_of_offset(after)
        } else {
            "0-0".to_string()
        };
        let mut xread = redis::cmd("XREAD");
        xread
            .arg("COUNT")
            .arg(1)
            .arg("BLOCK")
            .arg(wait.as_millis().max(1) as u64)
            .arg("STREAMS")
            .arg(keys.log())
            .arg(after);
        let reply = self
            .blocking
            .query(&keys.log(), &xread, deadline)
            .await
            .mapped()?;
        Ok(
            matches!(reply, Some(Value::Array(ref keys)) if !keys.is_empty())
                || matches!(reply, Some(Value::Map(ref keys)) if !keys.is_empty()),
        )
    }
}

#[async_trait::async_trait]
impl Store for RedisStore {
    // Every rule is in the Lua scripts.
    fn capabilities(&self) -> Capabilities {
        Capabilities::ALL
    }

    fn default_retention(&self) -> Duration {
        self.retention
    }

    async fn create(
        &self,
        namespace: &str,
        stream: &StreamRef,
        retention: Option<Duration>,
        owner: Owner<'_>,
        withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Created> {
        RedisStore::create(self, namespace, stream, retention, owner, withdrawal).await
    }

    async fn append(
        &self,
        namespace: &str,
        stream: &StreamRef,
        batch: &Batch<'_>,
    ) -> StreamResult<Appended> {
        RedisStore::append(self, namespace, &stream.id, batch).await
    }

    async fn close(
        &self,
        namespace: &str,
        stream: &StreamRef,
        producer: &Producer,
        _task_token: &[u8],
        record: &[u8],
    ) -> StreamResult<Closed> {
        RedisStore::close(self, namespace, stream, producer, record).await
    }

    async fn read(
        &self,
        namespace: &str,
        stream_id: &str,
        from: i64,
        count: usize,
    ) -> StreamResult<(StreamState, Vec<Entry>)> {
        RedisStore::read(self, namespace, stream_id, from, count).await
    }

    async fn wait_after(
        &self,
        namespace: &str,
        stream_id: &str,
        after: i64,
        deadline: Instant,
    ) -> StreamResult<bool> {
        RedisStore::wait_after(self, namespace, stream_id, after, deadline).await
    }

    async fn read_back(
        &self,
        namespace: &str,
        stream_id: &str,
        before: i64,
        count: usize,
    ) -> StreamResult<Vec<Entry>> {
        RedisStore::read_back(self, namespace, stream_id, before, count).await
    }
}

fn encode_run(run_id: &str) -> String {
    if run_id.is_empty() {
        String::new()
    } else {
        encode_part(run_id)
    }
}

/// The task key of a Local Activity's dispatching Workflow Task, empty for any other kind.
fn local_activity_task(producer: &Producer) -> String {
    if producer.kind != ProducerKind::LocalActivity as i32 {
        return String::new();
    }
    task_key(&task_of(&producer.epoch.unwrap_or_default()))
}

/// The run's last completed task key, `-` when the batch gives no task facts.
fn completed_arg(withdrawal: &Withdrawal<'_>) -> String {
    match &withdrawal.tasks {
        None => "-".to_string(),
        Some(tasks) => tasks.completed.map(task_key).unwrap_or_default(),
    }
}

fn failed_list(withdrawal: &Withdrawal<'_>) -> String {
    let failed = withdrawal.tasks.map_or(&[][..], |tasks| tasks.failed_tasks);
    failed
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(" ")
}

fn bad_reply(call: &str, reply: impl std::fmt::Debug) -> StreamError {
    StreamError::storage(format!("Redis gave the {call} script the answer {reply:?}"))
}

fn text(value: &Value) -> Option<String> {
    match value {
        Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
        Value::SimpleString(text) => Some(text.clone()),
        Value::Int(number) => Some(number.to_string()),
        Value::Nil => Some(String::new()),
        _ => None,
    }
}

fn strings<const N: usize>(reply: Value, call: &str) -> StreamResult<[String; N]> {
    let Value::Array(items) = &reply else {
        return Err(bad_reply(call, &reply));
    };
    let texts: Option<Vec<String>> = items.iter().map(text).collect();
    texts
        .and_then(|texts| texts.try_into().ok())
        .ok_or_else(|| bad_reply(call, &reply))
}

fn read_reply(reply: Value) -> StreamResult<(StreamState, Vec<Entry>)> {
    let Value::Array(mut parts) = reply else {
        return Err(bad_reply("read", reply));
    };
    if parts.len() != 3 {
        return Err(bad_reply("read", parts));
    }
    let entries = parts.pop().expect("three parts");
    let time = parts.pop().expect("three parts");
    let meta = parts.pop().expect("three parts");
    let Value::Array(meta) = meta else {
        return Err(bad_reply("read", meta));
    };
    let meta: Vec<Option<String>> = meta
        .iter()
        .map(|value| text(value).filter(|text| !text.is_empty()))
        .collect();
    let field = |i: usize| meta.get(i).cloned().flatten();
    let offset =
        |i: usize| -> StreamResult<i64> { field(i).map_or(Ok(0), |id| offset_of_entry_id(&id)) };
    let number = |i: usize| {
        field(i)
            .and_then(|text| text.parse::<i64>().ok())
            .unwrap_or(0)
    };
    let [seconds, micros] = match time {
        Value::Array(time) => {
            let time: Vec<i64> = time
                .iter()
                .filter_map(|value| text(value)?.parse().ok())
                .collect();
            <[i64; 2]>::try_from(time).map_err(|time| bad_reply("read", time))?
        }
        other => return Err(bad_reply("read", other)),
    };
    let state = StreamState {
        exists: field(0).is_some(),
        version: number(0) as u32,
        closed_at: if field(1).is_some() {
            Some(offset(2)?)
        } else {
            None
        },
        newest: offset(3)?,
        trimmed: offset(4)?,
        retention: Duration::from_millis(number(5).max(0) as u64),
        expires_ms: number(6),
        now_ms: seconds * 1000 + micros / 1000,
    };
    let Value::Array(entries) = entries else {
        return Err(bad_reply("read", entries));
    };
    let entries = entries
        .into_iter()
        .map(entry)
        .collect::<StreamResult<Vec<_>>>()?;
    Ok((state, entries))
}

fn entry(value: Value) -> StreamResult<Entry> {
    let Value::Array(mut pair) = value else {
        return Err(bad_reply("read", value));
    };
    if pair.len() != 2 {
        return Err(bad_reply("read", pair));
    }
    let fields = pair.pop().expect("two parts");
    let id = text(&pair.pop().expect("two parts")).unwrap_or_default();
    let mut entry = Entry {
        offset: offset_of_entry_id(&id)?,
        record: None,
        supersession: None,
    };
    let Value::Array(fields) = fields else {
        return Err(bad_reply("read", fields));
    };
    for pair in fields.chunks(2) {
        let (Some(Value::BulkString(name)), Some(Value::BulkString(value))) =
            (pair.first(), pair.get(1))
        else {
            continue;
        };
        match name.as_slice() {
            b"r" => entry.record = Some(value.clone()),
            b"s" => entry.supersession = Some(value.clone()),
            _ => {}
        }
    }
    Ok(entry)
}

/// Refuses a server older than Redis 7.0, and warns about one that may evict Stream keys.
async fn check_server(shared: &Shared) -> StreamResult<()> {
    let mut info = redis::cmd("INFO");
    info.arg("server");
    match shared.on_each_primary(info).await {
        Ok(replies) => {
            for (node, reply) in replies {
                let text: String =
                    redis::from_redis_value(reply).map_err(|error| bad_reply("INFO", error))?;
                check_version(&node, &text)?;
            }
        }
        // A user with only the Store's commands may not have INFO. Then a Redis before 7.0 fails at
        // the first read, on the flags of the read script.
        Err(error) if error.code() == Some("NOPERM") => {
            tracing::debug!("Could not read the Redis version: {error}");
        }
        Err(error) => return Err(errors::stream_error(&error)),
    }
    let mut policy = redis::cmd("CONFIG");
    policy.arg("GET").arg("maxmemory-policy");
    match shared.on_each_primary(policy).await {
        Ok(replies) => {
            for (node, reply) in replies {
                if let Some(policy) = eviction_policy(reply).filter(|p| p != "noeviction") {
                    tracing::warn!(
                        node,
                        policy,
                        "Redis may evict Stream keys under memory pressure, which loses Records \
                         and their fence. Use maxmemory-policy noeviction for Streams."
                    );
                    break;
                }
            }
        }
        // Managed services often refuse CONFIG and document their eviction policy instead.
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
        return Err(StreamError::invalid(format!(
            "The Redis Store needs Redis 7.0 or later, but {node} reports {version}"
        )));
    }
    Ok(())
}

fn eviction_policy(reply: Value) -> Option<String> {
    let flat: Vec<String> = match reply {
        Value::Map(pairs) => pairs
            .into_iter()
            .flat_map(|(name, value)| [text(&name), text(&value)])
            .collect::<Option<_>>()?,
        Value::Array(items) => items.iter().map(text).collect::<Option<_>>()?,
        _ => return None,
    };
    flat.chunks(2)
        .find(|pair| pair.first().map(String::as_str) == Some("maxmemory-policy"))
        .and_then(|pair| pair.get(1).cloned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_server_before_redis_7_is_refused() {
        check_version("a", "# Server\r\nredis_version:7.0.15\r\n").unwrap();
        check_version("a", "redis_version:8.10.1").unwrap();
        assert!(check_version("a", "redis_version:6.2.14").is_err());
        assert!(check_version("a", "").is_err());
    }

    #[test]
    fn retention_hides_old_records_on_read() {
        let state = StreamState {
            exists: true,
            trimmed: 5,
            retention: Duration::from_millis(1000),
            now_ms: 3000,
            ..Default::default()
        };
        assert_eq!(state.retained_from(), 2000 << crate::offset::SEQ_BITS);
        let state = StreamState {
            trimmed: 9000 << crate::offset::SEQ_BITS,
            ..state
        };
        assert_eq!(state.retained_from(), (9000 << crate::offset::SEQ_BITS) + 1);
    }
}
