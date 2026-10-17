#![doc = include_str!("../../docs/postgres.md")]

mod conn;
mod errors;
mod janitor;
mod listen;
mod log;
mod migrations;
mod wait;

use crate::{
    Capabilities, ConditionForms, MAX_BATCH_BYTES, MAX_RECORD_BYTES, NotificationLog, StreamError,
    StreamResult, StreamStore,
    proto::{
        ChainId, DeleteOwnerRequest, DeleteOwnerResponse, StoreAppendRequest, StoreAppendResponse,
        StoreLatestRequest, StoreLatestResponse, StoreReadRequest, StoreReadResponse, StoredClose,
        StoredRecord, StreamFailureKind, StreamOwnerKind,
    },
};
use conn::Target;
use deadpool_postgres::Pool;
use errors::{Call, mapped, retryable, timed_out, unreachable};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{task::JoinHandle, time::Instant};
use tokio_postgres::{Row, types::ToSql};
use wait::{Seen, Waiters};

const NAME: &str = "postgres";
const TOMBSTONE_GRACE: Duration = Duration::from_secs(30 * 24 * 60 * 60);
/// How many rows one janitor or purge transaction deletes at most.
const DELETE_BATCH: i32 = 10_000;
/// How many partitions the notification log has, fixed in the schema.
const PARTITIONS: u32 = 64;
/// The oldest PostgreSQL the store runs on, as `server_version_num`.
const OLDEST_SERVER: i32 = 140_000;
/// How many entries each log partition keeps at most, as the Redis log's `MAXLEN ~ 10000`.
const LOG_CAP: i32 = 10_000;
/// The longest schema name. A channel name, `<schema>_read_63`, takes at most 63 bytes.
const MAX_SCHEMA: usize = 55;

/// How many doorbells a notification log rings when the options don't say.
pub const DEFAULT_POSTGRES_DOORBELLS: u32 = 16;

/// How to reach PostgreSQL and how long streams live there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresStoreOptions {
    /// A `postgres://` or `postgresql://` URL. Credentials come from the URL, and TLS follows its
    /// `sslmode` and `sslrootcert` as libpq reads them.
    pub url: String,
    /// The schema that holds the store's tables, so one database can hold several stores and a
    /// `GRANT` can scope a role to one.
    pub schema: String,
    /// How long a stream keeps a record.
    pub retention: Duration,
    /// How many connections the pool keeps. Waiting reads hold none.
    pub max_connections: usize,
    /// How long a call waits, as the session's `statement_timeout`.
    pub response_timeout: Duration,
    /// How long a positions-only reader's hold lasts after its last read. `None` uses the
    /// retention.
    pub floor_lease: Option<Duration>,
    /// How long a hold lasts after its reader released it.
    pub floor_release_grace: Duration,
    /// How often a process looks for new records for its waiting reads.
    pub poll_interval: Duration,
    /// Check the schema version and never change it, for a DBA who applies the migrations.
    pub skip_migrations: bool,
    /// How often the janitor trims records past the retention and deletes old tombstones.
    pub janitor_interval: Duration,
    /// Each append also writes an entry to the notification log in its transaction, which a
    /// driver reads.
    pub notification_log: bool,
    /// How many doorbells the notification log rings, from 1 to 64.
    pub notify_doorbells: u32,
    /// A URL for the connection that listens and the one that applies migrations, when `url`
    /// goes through a pooler in transaction mode. `None` uses `url`.
    pub listen_url: Option<String>,
    /// Each append also sends `NOTIFY`, so waiting reads in other processes wake in milliseconds.
    /// Costs append throughput when many producers write at once.
    pub notify_reads: bool,
    /// With an archive, how long the archive's hold on a topic lasts after the archive last moved
    /// it. Each topic's first record makes the hold, so retention drops only what the archive
    /// holds while the archive keeps up. Needs `notification_log`, since a driver archives.
    pub archive_lease: Option<Duration>,
}

impl PostgresStoreOptions {
    /// The defaults for one database at `url`.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            schema: "temporal_streams".to_string(),
            retention: Duration::from_secs(7 * 24 * 60 * 60),
            max_connections: 16,
            response_timeout: Duration::from_secs(5),
            floor_lease: None,
            floor_release_grace: Duration::from_secs(24 * 60 * 60),
            poll_interval: Duration::from_millis(100),
            skip_migrations: false,
            janitor_interval: Duration::from_secs(60),
            notification_log: false,
            notify_doorbells: DEFAULT_POSTGRES_DOORBELLS,
            listen_url: None,
            notify_reads: false,
            archive_lease: None,
        }
    }
}

/// A [StreamStore] on PostgreSQL.
pub struct PostgresStore {
    inner: Arc<Inner>,
    log: Option<Arc<log::PostgresLog>>,
    tasks: Vec<JoinHandle<()>>,
}

impl std::fmt::Debug for PostgresStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresStore")
            .field("schema", &self.inner.schema)
            .field("retention_ms", &self.inner.retention_ms)
            .finish_non_exhaustive()
    }
}

impl Drop for PostgresStore {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// A chain as the `chains` table keys it. A Workflow Activity keeps its Workflow's id as the
/// owner, so a Workflow's delete finds it by an exact match.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Chain {
    namespace: String,
    kind: i16,
    owner: String,
    run: String,
    activity: String,
}

impl Chain {
    fn of(chain: &ChainId) -> Self {
        let (kind, owner, activity) = match StreamOwnerKind::try_from(chain.owner_kind) {
            Ok(StreamOwnerKind::Activity) => (2, &chain.activity_id, ""),
            Ok(StreamOwnerKind::WorkflowActivity) => {
                (3, &chain.workflow_id, chain.activity_id.as_str())
            }
            _ => (1, &chain.workflow_id, ""),
        };
        Self {
            namespace: chain.namespace.clone(),
            kind,
            owner: owner.clone(),
            run: chain.first_run_id.clone(),
            activity: activity.to_string(),
        }
    }

    /// The notification log partition of every topic of the chain.
    fn partition(&self) -> i16 {
        let mut hash = Sha256::new();
        for part in [
            self.namespace.as_bytes(),
            &self.kind.to_be_bytes(),
            self.owner.as_bytes(),
            self.run.as_bytes(),
            self.activity.as_bytes(),
        ] {
            hash.update((part.len() as u32).to_be_bytes());
            hash.update(part);
        }
        let digest = hash.finalize();
        (u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]) % PARTITIONS) as i16
    }

    fn keeps_topics(&self) -> bool {
        self.kind == 3
    }
}

/// The SQL of each call, with the store's schema in it.
#[derive(Debug)]
struct Sql {
    append: String,
    read: String,
    latest: String,
    record_at: String,
    topic_state: String,
    close_chain: String,
    close_topic: String,
    retire: String,
    written: String,
    topics: String,
    delete_owner: String,
    purge: String,
    hold: String,
    release: String,
    poll: String,
    due: String,
    trim: String,
    idle_chains: String,
    take: String,
    ack: String,
    busy: String,
    cap_log: String,
    advance: String,
    archive_marks: String,
}

/// The chain key, as `$1` to `$5`.
const CHAIN_KEY: &str = "(c.namespace, c.owner_kind, c.owner_id, c.first_run_id, c.activity_id) \
                         = ($1, $2, $3, $4, $5)";

impl Sql {
    fn new(s: &str) -> Self {
        Self {
            append: format!(
                "SELECT status, first_position, last_position, next_sequence, close FROM \
                 {s}.append_v1($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, \
                 $16, $17, $18)"
            ),
            read: format!(
                "SELECT t.topic_id, t.last_position, t.trimmed, t.closed, c.closed, \
                 CASE WHEN t.closed THEN t.close END, r.position, r.record \
                 FROM {s}.chains c \
                 LEFT JOIN {s}.topics t ON t.chain_id = c.chain_id AND t.topic = $6 \
                 LEFT JOIN LATERAL (SELECT k.position, k.record FROM {s}.records k \
                   WHERE k.topic_id = t.topic_id AND k.position > $7 \
                   ORDER BY k.position LIMIT $8) r ON true \
                 WHERE {CHAIN_KEY} ORDER BY r.position"
            ),
            latest: format!(
                "SELECT CASE WHEN t.last_position > coalesce(t.trimmed, t.base) \
                 THEN t.last_position END FROM {s}.chains c \
                 JOIN {s}.topics t ON t.chain_id = c.chain_id AND t.topic = $6 WHERE {CHAIN_KEY}"
            ),
            record_at: format!(
                "SELECT r.record FROM {s}.chains c \
                 JOIN {s}.topics t ON t.chain_id = c.chain_id AND t.topic = $6 \
                 JOIN {s}.records r ON r.topic_id = t.topic_id AND r.position = $7 \
                 WHERE {CHAIN_KEY}"
            ),
            topic_state: format!(
                "SELECT t.last_position, t.trimmed, t.archived FROM {s}.chains c \
                 JOIN {s}.topics t ON t.chain_id = c.chain_id AND t.topic = $6 WHERE {CHAIN_KEY}"
            ),
            close_chain: format!("SELECT {s}.close_chain_v1($1, $2, $3, $4, $5)"),
            close_topic: format!("SELECT {s}.close_topic_v1($1, $2, $3, $4, $5, $6, $7, $8, $9)"),
            retire: format!(
                "DELETE FROM {s}.producers h USING {s}.chains c, {s}.topics t \
                 WHERE {CHAIN_KEY} AND t.chain_id = c.chain_id AND t.topic = $6 \
                 AND h.topic_id = t.topic_id AND h.producer_id = $7 AND h.attempt = $8"
            ),
            topics: format!(
                "SELECT t.topic FROM {s}.chains c JOIN {s}.topics t ON t.chain_id = c.chain_id \
                 WHERE {CHAIN_KEY} AND t.last_position IS NOT NULL ORDER BY t.topic COLLATE \"C\""
            ),
            written: format!(
                "SELECT t.topic FROM {s}.chains c JOIN {s}.topics t ON t.chain_id = c.chain_id \
                 WHERE {CHAIN_KEY} AND t.written ORDER BY t.topic COLLATE \"C\""
            ),
            delete_owner: format!("SELECT {s}.delete_owner_v1($1, $2, $3)"),
            purge: format!("SELECT deleted, done FROM {s}.purge_v1($1)"),
            hold: format!("SELECT {s}.hold_floor_v1($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"),
            release: format!("SELECT {s}.release_floor_v1($1, $2, $3, $4, $5, $6, $7, $8)"),
            poll: format!(
                "SELECT k.i, t.last_position, t.closed OR c.closed \
                 FROM unnest($1::text[], $2::smallint[], $3::text[], $4::text[], $5::text[], \
                   $6::text[]) WITH ORDINALITY AS k(ns, kind, owner, run, activity, topic, i) \
                 JOIN {s}.chains c ON (c.namespace, c.owner_kind, c.owner_id, c.first_run_id, \
                   c.activity_id) = (k.ns, k.kind, k.owner, k.run, k.activity) \
                 LEFT JOIN {s}.topics t ON t.chain_id = c.chain_id AND t.topic = k.topic"
            ),
            due: format!(
                "SELECT topic_id FROM {s}.topics WHERE trim_at <= now() ORDER BY trim_at LIMIT 500"
            ),
            trim: format!("SELECT deleted, done FROM {s}.trim_topic_v1($1, $2, $3, $4)"),
            idle_chains: format!("SELECT {s}.drop_idle_chains_v1($1, $2, $3)"),
            take: format!(
                "WITH picked AS (SELECT n.partition, n.id FROM {s}.notifications n \
                   WHERE n.partition = $1 AND (n.taken_by IS NULL OR n.taken_by = $2 \
                     OR n.taken_at < now() - $3::bigint * interval '1 millisecond') \
                   ORDER BY n.id LIMIT $4 FOR UPDATE SKIP LOCKED), \
                 taken AS (UPDATE {s}.notifications n SET taken_by = $2, taken_at = now() \
                   FROM picked WHERE n.partition = picked.partition AND n.id = picked.id \
                   RETURNING n.id, n.topic_id, n.position) \
                 SELECT taken.id, c.namespace, c.owner_kind, c.owner_id, c.first_run_id, \
                   c.activity_id, t.topic, taken.position \
                 FROM taken LEFT JOIN {s}.topics t ON t.topic_id = taken.topic_id \
                 LEFT JOIN {s}.chains c ON c.chain_id = t.chain_id ORDER BY taken.id"
            ),
            ack: format!("DELETE FROM {s}.notifications WHERE partition = $1 AND id = ANY ($2)"),
            busy: format!(
                "SELECT p FROM generate_series(0, 63) p \
                 WHERE EXISTS (SELECT 1 FROM {s}.notifications n WHERE n.partition = p)"
            ),
            cap_log: format!("SELECT {s}.cap_log_v1($1)"),
            advance: format!("SELECT {s}.advance_archive_v1($1, $2, $3, $4, $5, $6, $7, $8)"),
            archive_marks: format!(
                "SELECT x.namespace, x.owner_kind, x.owner_id, x.first_run_id, x.activity_id, \
                   x.topic, x.written_ms, x.idle_ms FROM ( \
                   SELECT c.namespace, c.owner_kind, c.owner_id, c.first_run_id, c.activity_id, \
                     t.topic, CASE WHEN t.closed THEN 0 \
                       ELSE (extract(epoch FROM t.written_at) * 1000)::bigint END AS written_ms, \
                     (extract(epoch FROM now() - t.written_at) * 1000)::bigint AS idle_ms, \
                     row_number() OVER (PARTITION BY t.partition \
                       ORDER BY t.closed DESC, t.written_at) AS n \
                   FROM {s}.topics t JOIN {s}.chains c ON c.chain_id = t.chain_id \
                   WHERE t.archive_pending) x \
                 WHERE x.n <= $1"
            ),
        }
    }
}

struct Inner {
    pool: Pool,
    schema: String,
    sql: Sql,
    retention_ms: i64,
    grace_ms: i64,
    floor_lease_ms: i64,
    floor_release_grace_ms: i64,
    /// How long a call waits for an answer before the store gives up on it. Longer than the
    /// statement timeout, so the server's own timeout comes first.
    call_timeout: Duration,
    waiters: Waiters,
    /// Whether appends write the notification log.
    log: bool,
    doorbells: i32,
    notify_reads: bool,
    /// The archive's lease in milliseconds, with an archive.
    archive_lease_ms: Option<i64>,
}

impl PostgresStore {
    /// Connects to PostgreSQL, and applies the store's migrations unless the options skip them.
    ///
    /// Fails as refused when the options can't work, as unsupported for a server older than
    /// PostgreSQL 14 or a schema from a newer build, and as a storage failure when the server
    /// can't be reached.
    pub async fn connect(options: PostgresStoreOptions) -> StreamResult<Self> {
        if !valid_schema(&options.schema) {
            return Err(StreamError::refused(format!(
                "the schema must match [a-z_][a-z0-9_]{{0,{}}}, start with no `pg_` and be no \
                 reserved word, not {:?}",
                MAX_SCHEMA - 1,
                options.schema
            )));
        }
        if options.archive_lease.is_some() && !options.notification_log {
            return Err(StreamError::refused(
                "an archive needs the notification log, since a driver archives",
            ));
        }
        if !(1..=PARTITIONS).contains(&options.notify_doorbells) {
            return Err(StreamError::refused(format!(
                "the notification log rings 1 to {PARTITIONS} doorbells, not {}",
                options.notify_doorbells
            )));
        }
        let retention_ms = i64::try_from(options.retention.as_millis()).unwrap_or(i64::MAX);
        if retention_ms == 0 {
            return Err(StreamError::refused(format!(
                "the retention must be at least a millisecond, not {:?}",
                options.retention
            )));
        }
        let target = Target::from_url(&options.url)?;
        // Migrations take a session lock, so they run on the direct connection when there is one.
        let direct_target = match &options.listen_url {
            Some(url) => Target::from_url(url)?,
            None => target.clone(),
        };
        let mut direct = direct_target.connect().await.map_err(unreachable)?;
        check_server(&direct).await?;
        migrations::run(&mut direct, &options.schema, !options.skip_migrations).await?;
        drop(direct);
        let pool = target.pool(options.max_connections, options.response_timeout)?;
        let rings = options
            .notification_log
            .then(|| Arc::new(listen::Rings::new(options.notify_doorbells)));
        let listener = (options.notification_log || options.notify_reads).then(|| {
            listen::Listener::new(
                direct_target,
                &options.schema,
                rings.clone(),
                options.notify_reads,
            )
        });
        let listening = match &listener {
            Some(listener) => Some(listener.connect().await?),
            None => None,
        };
        let inner = Arc::new(Inner {
            pool,
            sql: Sql::new(&options.schema),
            schema: options.schema,
            retention_ms,
            grace_ms: millis(TOMBSTONE_GRACE),
            floor_lease_ms: options
                .floor_lease
                .map_or(retention_ms, |lease| millis(lease).max(1)),
            floor_release_grace_ms: millis(options.floor_release_grace),
            call_timeout: options.response_timeout + Duration::from_secs(2),
            waiters: Waiters::default(),
            log: options.notification_log,
            doorbells: options.notify_doorbells as i32,
            notify_reads: options.notify_reads,
            archive_lease_ms: options.archive_lease.map(|lease| millis(lease).max(1)),
        });
        let mut tasks = vec![
            tokio::spawn(poll(Arc::downgrade(&inner), options.poll_interval)),
            tokio::spawn(janitor::run(
                Arc::downgrade(&inner),
                options.janitor_interval,
            )),
        ];
        if let (Some(listener), Some((client, notifications))) = (listener, listening) {
            tasks.push(tokio::spawn(listener.run(
                Arc::downgrade(&inner),
                client,
                notifications,
            )));
        }
        let log = rings.map(|rings| {
            Arc::new(log::PostgresLog {
                inner: inner.clone(),
                rings,
                doorbells: options.notify_doorbells,
                archive: options.archive_lease.is_some(),
            })
        });
        Ok(Self { inner, log, tasks })
    }

    /// Runs one janitor pass now, as the store's janitor task does every `janitor_interval`.
    /// Answers how many records it deleted. A pass that another process runs now is skipped.
    pub async fn run_janitor(&self) -> StreamResult<u64> {
        self.inner.janitor_pass().await
    }
}

fn valid_schema(schema: &str) -> bool {
    let mut bytes = schema.bytes();
    bytes
        .next()
        .is_some_and(|first| first.is_ascii_lowercase() || first == b'_')
        && schema.len() <= MAX_SCHEMA
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        // PostgreSQL keeps `pg_` names for itself, and a reserved word needs quotes in SQL.
        && !schema.starts_with("pg_")
        && !RESERVED_WORDS.contains(&schema)
}

/// PostgreSQL's reserved key words, which can't name a schema without quotes.
const RESERVED_WORDS: [&str; 78] = [
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "both",
    "case",
    "cast",
    "check",
    "collate",
    "column",
    "constraint",
    "create",
    "current_catalog",
    "current_date",
    "current_role",
    "current_time",
    "current_timestamp",
    "current_user",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "false",
    "fetch",
    "for",
    "foreign",
    "from",
    "grant",
    "group",
    "having",
    "in",
    "initially",
    "intersect",
    "into",
    "lateral",
    "leading",
    "limit",
    "localtime",
    "localtimestamp",
    "not",
    "null",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "placing",
    "primary",
    "references",
    "returning",
    "select",
    "session_user",
    "some",
    "symmetric",
    "system_user",
    "table",
    "then",
    "to",
    "trailing",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "variadic",
    "when",
    "where",
    "window",
    "with",
];

async fn check_server(client: &tokio_postgres::Client) -> StreamResult<()> {
    let version: String = client
        .query_one("SHOW server_version_num", &[])
        .await
        .map_err(unreachable)?
        .get(0);
    check_version(&version)
}

fn check_version(version: &str) -> StreamResult<()> {
    match version.trim().parse::<i32>() {
        Ok(number) if number >= OLDEST_SERVER => Ok(()),
        _ => Err(StreamError::unsupported(format!(
            "the PostgreSQL store needs PostgreSQL 14 or later, but the server reports \
             server_version_num {version}"
        ))),
    }
}

fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn chain_of(chain: Option<&ChainId>) -> StreamResult<Chain> {
    chain
        .map(Chain::of)
        .ok_or_else(|| StreamError::refused("a store call needs the chain it is about"))
}

fn parse_position(position: &str) -> StreamResult<Option<i64>> {
    if position.is_empty() {
        return Ok(None);
    }
    position.parse().map(Some).map_err(|_| {
        StreamError::cursor(format!(
            "position {position:?} is not a position of the PostgreSQL store"
        ))
    })
}

fn decode_close(bytes: &[u8]) -> StreamResult<StoredClose> {
    StoredClose::decode(bytes)
        .map_err(|error| StreamError::storage(format!("a topic's close does not decode: {error}")))
}

fn wait_of(request: &StoreReadRequest) -> Duration {
    request.wait.as_ref().map_or(Duration::ZERO, |wait| {
        Duration::new(
            u64::try_from(wait.seconds).unwrap_or(0),
            u32::try_from(wait.nanos).unwrap_or(0),
        )
    })
}

/// One look at a topic, as the read statement gives it.
struct Look {
    response: StoreReadResponse,
    seen: Seen,
    topic_id: Option<i64>,
}

impl Inner {
    /// Runs one statement in autocommit on a pooled connection. Retries a serialization failure
    /// or a deadlock three times first.
    async fn query(
        &self,
        call: Call,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> StreamResult<Vec<Row>> {
        let mut tries = 0;
        loop {
            let client = self.pool.get().await.map_err(unreachable)?;
            let statement = match client.prepare_cached(sql).await {
                Ok(statement) => statement,
                Err(error) => return Err(mapped(Call::Read, &error)),
            };
            let answer =
                tokio::time::timeout(self.call_timeout, client.query(&statement, params)).await;
            match answer {
                Ok(Ok(rows)) => return Ok(rows),
                Ok(Err(error)) if retryable(&error) && tries < 3 => tries += 1,
                Ok(Err(error)) => {
                    // A connection the error ended never goes back to the pool.
                    let fatal = error
                        .as_db_error()
                        .is_none_or(|db| matches!(db.severity(), "FATAL" | "PANIC"));
                    if fatal || client.is_closed() {
                        drop(deadpool_postgres::Object::take(client));
                    }
                    return Err(mapped(call, &error));
                }
                Err(_) => {
                    // The connection may still run the call, so it never goes back to the pool.
                    drop(deadpool_postgres::Object::take(client));
                    return Err(timed_out(call));
                }
            }
        }
    }

    /// The read statement once, with the rules that turn its rows into an answer.
    async fn look(&self, chain: &Chain, request: &StoreReadRequest) -> StreamResult<Look> {
        let position = parse_position(&request.after_position)?;
        let max = i64::from(request.max_records.max(1));
        let rows = self
            .query(
                Call::Read,
                &self.sql.read,
                &[
                    &chain.namespace,
                    &chain.kind,
                    &chain.owner,
                    &chain.run,
                    &chain.activity,
                    &request.topic,
                    &position.unwrap_or(0),
                    &max,
                ],
            )
            .await?;
        let first = rows.first();
        let topic_id: Option<i64> = first.and_then(|row| row.get(0));
        let last: Option<i64> = first.and_then(|row| row.get(1));
        let trimmed: Option<i64> = first.and_then(|row| row.get(2));
        let topic_closed = first.and_then(|row| row.get::<_, Option<bool>>(3)) == Some(true);
        let chain_closed = first.is_some_and(|row| row.get::<_, bool>(4));
        let close = first
            .and_then(|row| row.get::<_, Option<Vec<u8>>>(5))
            .map(|bytes| decode_close(&bytes))
            .transpose()?;
        let closed = chain_closed || topic_closed;
        let seen = (last, closed);
        let answer = |records| Look {
            response: StoreReadResponse {
                records,
                closed,
                close: close.clone().filter(|_| topic_closed),
            },
            seen,
            topic_id,
        };
        let (Some(_), Some(last)) = (topic_id, last) else {
            // A topic that never held a record. A closed one answers empty and closed, as a
            // Redis topic does whose meta alone exists.
            if position.is_some() && !(topic_id.is_some() && topic_closed) {
                return Err(StreamError::not_found(format!(
                    "topic {:?} holds no records and none were dropped, so nothing is known \
                     about what followed position {}",
                    request.topic, request.after_position
                )));
            }
            return Ok(answer(vec![]));
        };
        let records: Vec<StoredRecord> = rows
            .iter()
            .filter_map(|row| {
                let position: Option<i64> = row.get(6);
                position.map(|position| StoredRecord {
                    position: position.to_string(),
                    record: row.get(7),
                })
            })
            .collect();
        if let Some(position) = position {
            if position > last {
                return Err(StreamError::cursor(format!(
                    "position {position} is one topic {:?} never held",
                    request.topic
                )));
            }
            let first = records.first().map(|record| record.position.parse::<i64>());
            if let Some(trimmed) = trimmed
                && trimmed > position
                && first.is_none_or(|first| first.is_ok_and(|first| trimmed < first))
            {
                return Err(StreamError::expired(format!(
                    "records after {position} on topic {:?} were dropped by retention while \
                     this read was behind them; the newest dropped one is {trimmed}",
                    request.topic
                )));
            }
        }
        Ok(answer(records))
    }

    async fn poll_once(&self, streams: &[wait::StreamKey]) -> StreamResult<()> {
        let text = |field: fn(&wait::StreamKey) -> &str| -> Vec<&str> {
            streams.iter().map(field).collect()
        };
        let kinds: Vec<i16> = streams.iter().map(|(chain, _)| chain.kind).collect();
        let rows = self
            .query(
                Call::Read,
                &self.sql.poll,
                &[
                    &text(|(chain, _)| &chain.namespace),
                    &kinds,
                    &text(|(chain, _)| &chain.owner),
                    &text(|(chain, _)| &chain.run),
                    &text(|(chain, _)| &chain.activity),
                    &text(|(_, topic)| topic),
                ],
            )
            .await?;
        for row in rows {
            let index: i64 = row.get(0);
            let last: Option<i64> = row.get(1);
            let closed: Option<bool> = row.get(2);
            if let Some(key) = usize::try_from(index - 1).ok().and_then(|i| streams.get(i)) {
                self.waiters.polled(key, (last, closed == Some(true)));
            }
        }
        Ok(())
    }

    /// Empties `purge`, one batch per transaction. A failure leaves rows that only `purge` can
    /// reach, which the janitor finishes.
    async fn purge(&self) -> StreamResult<u64> {
        let mut deleted = 0;
        loop {
            let rows = self
                .query(Call::Write, &self.sql.purge, &[&DELETE_BATCH])
                .await?;
            let Some(row) = rows.first() else {
                return Ok(deleted);
            };
            deleted += row.get::<_, i64>(0) as u64;
            if row.get::<_, bool>(1) {
                return Ok(deleted);
            }
        }
    }
}

/// Wakes the waiters of streams that moved, once every `interval`, while any read waits.
async fn poll(inner: Weak<Inner>, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        let Some(inner) = inner.upgrade() else {
            return;
        };
        let streams = inner.waiters.streams();
        if streams.is_empty() {
            continue;
        }
        if let Err(error) = inner.poll_once(&streams).await {
            tracing::debug!("The PostgreSQL store's poll for waiting reads failed: {error}");
        }
    }
}

#[async_trait::async_trait]
impl StreamStore for PostgresStore {
    fn name(&self) -> &str {
        NAME
    }

    fn notification_log(&self) -> Option<Arc<dyn NotificationLog>> {
        self.log.clone().map(|log| log as Arc<dyn NotificationLog>)
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            producer_state: true,
            conditional: ConditionForms::default(),
            kept_last: true,
            holds: true,
            change_feed: self.notification_log().is_some(),
            cheap_logs: true,
            delete_owner: true,
            topic_listing: true,
            long_poll: true,
            dense_positions: true,
            replay_horizon: None,
            max_record_bytes: MAX_RECORD_BYTES,
            max_batch_bytes: MAX_BATCH_BYTES,
        }
    }

    async fn append(&self, request: StoreAppendRequest) -> StreamResult<StoreAppendResponse> {
        if request.records.is_empty() {
            return Err(StreamError::refused("an append needs at least one record"));
        }
        if request.digest.is_empty() {
            return Err(StreamError::refused("an append needs the batch's digest"));
        }
        let inner = &self.inner;
        let chain = chain_of(request.chain.as_ref())?;
        let rows = inner
            .query(
                Call::Append,
                &inner.sql.append,
                &[
                    &chain.namespace,
                    &chain.kind,
                    &chain.owner,
                    &chain.run,
                    &chain.activity,
                    &request.topic,
                    &chain.partition(),
                    &request.producer_id,
                    &request.attempt,
                    &request.sequence,
                    &request.digest,
                    &request.records,
                    &chain.keeps_topics(),
                    &inner.log,
                    &inner.doorbells,
                    &inner.archive_lease_ms,
                    &inner.retention_ms,
                    &inner.notify_reads,
                ],
            )
            .await?;
        let row = rows
            .first()
            .ok_or_else(|| StreamError::storage("PostgreSQL answered the append with no row"))?;
        let status: i16 = row.get(0);
        let first: Option<i64> = row.get(1);
        let last: Option<i64> = row.get(2);
        match (status, first, last) {
            (0 | 1, Some(first), Some(last)) => {
                if status == 0 {
                    inner.waiters.wake(&(chain, request.topic));
                }
                Ok(StoreAppendResponse {
                    first_position: first.to_string(),
                    last_position: last.to_string(),
                })
            }
            (2, ..) => Err(StreamError::new(
                StreamFailureKind::ProducerDivergent,
                format!(
                    "sequence {} was already written with different content",
                    request.sequence
                ),
            )),
            (3, ..) => {
                let next: Option<i64> = row.get(3);
                Err(StreamError::new(
                    StreamFailureKind::ProducerStale,
                    format!(
                        "sequence {} is below the next one expected, {}",
                        request.sequence,
                        next.unwrap_or_default()
                    ),
                ))
            }
            (4, ..) => {
                let close: Option<Vec<u8>> = row.get(4);
                let closed_by = close
                    .and_then(|bytes| StoredClose::decode(bytes.as_slice()).ok())
                    .map(|close| close.closed_by)
                    .unwrap_or_default();
                Err(
                    StreamError::closed("the Workflow that owns this stream has closed")
                        .with_closed_by(closed_by),
                )
            }
            _ => Err(StreamError::storage(format!(
                "PostgreSQL answered the append with status {status}"
            ))),
        }
    }

    async fn read(&self, request: StoreReadRequest) -> StreamResult<StoreReadResponse> {
        let inner = &self.inner;
        let chain = chain_of(request.chain.as_ref())?;
        let wait = wait_of(&request);
        let deadline = Instant::now() + wait;
        let mut look = inner.look(&chain, &request).await?;
        if wait.is_zero() || !look.response.records.is_empty() || look.response.closed {
            return Ok(look.response);
        }
        let mut wake = inner.waiters.register(
            (chain.clone(), request.topic.clone()),
            look.seen,
            look.topic_id,
        );
        loop {
            look = inner.look(&chain, &request).await?;
            if !look.response.records.is_empty() || look.response.closed {
                return Ok(look.response);
            }
            inner
                .waiters
                .learn(&(chain.clone(), request.topic.clone()), look.topic_id);
            if tokio::time::timeout_at(deadline, wake.changed())
                .await
                .is_err()
            {
                return Ok(look.response);
            }
        }
    }

    async fn latest(&self, request: StoreLatestRequest) -> StreamResult<StoreLatestResponse> {
        let inner = &self.inner;
        let chain = chain_of(request.chain.as_ref())?;
        let rows = inner
            .query(
                Call::Read,
                &inner.sql.latest,
                &[
                    &chain.namespace,
                    &chain.kind,
                    &chain.owner,
                    &chain.run,
                    &chain.activity,
                    &request.topic,
                ],
            )
            .await?;
        let position: Option<i64> = rows.first().and_then(|row| row.get(0));
        Ok(StoreLatestResponse {
            position: position.map(|p| p.to_string()).unwrap_or_default(),
        })
    }

    async fn record_at(
        &self,
        chain: &ChainId,
        topic: &str,
        position: &str,
    ) -> StreamResult<Option<Vec<u8>>> {
        let Some(position) = parse_position(position)? else {
            return Ok(None);
        };
        let inner = &self.inner;
        let chain = Chain::of(chain);
        let rows = inner
            .query(
                Call::Read,
                &inner.sql.record_at,
                &[
                    &chain.namespace,
                    &chain.kind,
                    &chain.owner,
                    &chain.run,
                    &chain.activity,
                    &topic,
                    &position,
                ],
            )
            .await?;
        Ok(rows.first().map(|row| row.get(0)))
    }

    async fn newest_ever(&self, chain: &ChainId, topic: &str) -> StreamResult<String> {
        let (last, _, _) = self.inner.topic_state(chain, topic).await?;
        Ok(last.map(|last| last.to_string()).unwrap_or_default())
    }

    async fn dropped_after(
        &self,
        chain: &ChainId,
        topic: &str,
        mark: &str,
    ) -> StreamResult<Option<String>> {
        let mark = parse_position(mark)?;
        let (_, trimmed, _) = self.inner.topic_state(chain, topic).await?;
        // A topic that expired whole keeps `trimmed` at its old `last_position`, the mark itself,
        // so a revive counts as no loss.
        Ok(trimmed
            .filter(|trimmed| mark.is_none_or(|mark| *trimmed > mark))
            .map(|trimmed| trimmed.to_string()))
    }

    async fn hold_floor(
        &self,
        chain: &ChainId,
        topic: &str,
        holder: &str,
        position: &str,
    ) -> StreamResult<()> {
        let position = parse_position(position)?.unwrap_or(0);
        let inner = &self.inner;
        let chain = Chain::of(chain);
        inner
            .query(
                Call::Write,
                &inner.sql.hold,
                &[
                    &chain.namespace,
                    &chain.kind,
                    &chain.owner,
                    &chain.run,
                    &chain.activity,
                    &topic,
                    &chain.partition(),
                    &holder,
                    &position,
                    &inner.floor_lease_ms,
                ],
            )
            .await
            .map(drop)
    }

    async fn release_floor(&self, chain: &ChainId, topic: &str, holder: &str) -> StreamResult<()> {
        let inner = &self.inner;
        let chain = Chain::of(chain);
        inner
            .query(
                Call::Write,
                &inner.sql.release,
                &[
                    &chain.namespace,
                    &chain.kind,
                    &chain.owner,
                    &chain.run,
                    &chain.activity,
                    &topic,
                    &holder,
                    &inner.floor_release_grace_ms,
                ],
            )
            .await
            .map(drop)
    }

    async fn close_chain(&self, chain: &ChainId) -> StreamResult<()> {
        let inner = &self.inner;
        let chain = Chain::of(chain);
        inner
            .query(
                Call::Write,
                &inner.sql.close_chain,
                &[
                    &chain.namespace,
                    &chain.kind,
                    &chain.owner,
                    &chain.run,
                    &chain.activity,
                ],
            )
            .await?;
        inner.waiters.wake_chain(&chain);
        Ok(())
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
        let inner = &self.inner;
        let chain = Chain::of(chain);
        let rows = inner
            .query(
                Call::Write,
                &inner.sql.close_topic,
                &[
                    &chain.namespace,
                    &chain.kind,
                    &chain.owner,
                    &chain.run,
                    &chain.activity,
                    &topic,
                    &chain.partition(),
                    &close.encode_to_vec(),
                    &inner.archive_lease_ms.is_some(),
                ],
            )
            .await?;
        let stored: Option<Vec<u8>> = rows.first().and_then(|row| row.get(0));
        let stored = decode_close(&stored.unwrap_or_default())?;
        inner.waiters.wake(&(chain, topic.to_string()));
        Ok(stored)
    }

    async fn retire_producer(
        &self,
        chain: &ChainId,
        topic: &str,
        producer_id: &str,
        attempt: i64,
    ) -> StreamResult<()> {
        let inner = &self.inner;
        let chain = Chain::of(chain);
        inner
            .query(
                Call::Write,
                &inner.sql.retire,
                &[
                    &chain.namespace,
                    &chain.kind,
                    &chain.owner,
                    &chain.run,
                    &chain.activity,
                    &topic,
                    &producer_id,
                    &attempt,
                ],
            )
            .await
            .map(drop)
    }

    async fn list_topics(&self, chain: &ChainId) -> StreamResult<Vec<String>> {
        let chain = Chain::of(chain);
        let inner = &self.inner;
        let rows = inner
            .query(
                Call::Read,
                &inner.sql.topics,
                &[
                    &chain.namespace,
                    &chain.kind,
                    &chain.owner,
                    &chain.run,
                    &chain.activity,
                ],
            )
            .await?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    async fn written_topics(&self, chain: &ChainId) -> StreamResult<Vec<String>> {
        let chain = Chain::of(chain);
        if !chain.keeps_topics() {
            return Ok(Vec::new());
        }
        let inner = &self.inner;
        let rows = inner
            .query(
                Call::Read,
                &inner.sql.written,
                &[
                    &chain.namespace,
                    &chain.kind,
                    &chain.owner,
                    &chain.run,
                    &chain.activity,
                ],
            )
            .await?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    async fn advance_archive(
        &self,
        chain: &ChainId,
        topic: &str,
        position: &str,
    ) -> StreamResult<()> {
        let inner = &self.inner;
        let Some(lease) = inner.archive_lease_ms else {
            return Err(StreamError::unsupported("this store keeps no archive hold"));
        };
        let position = parse_position(position)?.unwrap_or(0);
        let chain = Chain::of(chain);
        inner
            .query(
                Call::Write,
                &inner.sql.advance,
                &[
                    &chain.namespace,
                    &chain.kind,
                    &chain.owner,
                    &chain.run,
                    &chain.activity,
                    &topic,
                    &position,
                    &lease,
                ],
            )
            .await
            .map(drop)
    }

    async fn archived(&self, chain: &ChainId, topic: &str) -> StreamResult<Option<String>> {
        if self.inner.archive_lease_ms.is_none() {
            return Ok(None);
        }
        Ok(self
            .inner
            .topic_row(chain, topic)
            .await?
            .map(|(_, _, archived)| archived.map(|p| p.to_string()).unwrap_or_default()))
    }

    async fn delete_owner(&self, request: DeleteOwnerRequest) -> StreamResult<DeleteOwnerResponse> {
        let (kind, owner): (i16, &str) = match StreamOwnerKind::try_from(request.owner_kind) {
            Ok(StreamOwnerKind::Workflow) => (1, &request.workflow_id),
            Ok(StreamOwnerKind::Activity) => (2, &request.activity_id),
            _ => {
                return Err(StreamError::unsupported(format!(
                    "this release keeps streams of Workflows and standalone Activities only, not \
                     owner kind {}",
                    request.owner_kind
                )));
            }
        };
        let inner = &self.inner;
        let rows = inner
            .query(
                Call::Write,
                &inner.sql.delete_owner,
                &[&request.namespace, &kind, &owner],
            )
            .await?;
        let deleted: i64 = rows.first().map_or(0, |row| row.get(0));
        if let Err(error) = inner.purge().await {
            tracing::warn!(
                "Deleted the streams of {owner:?}, but their records remain until the janitor \
                 purges them: {error}"
            );
        }
        Ok(DeleteOwnerResponse {
            deleted: deleted as u64,
        })
    }
}

impl Inner {
    /// The newest position ever, the newest dropped one and the newest archived one of a topic.
    async fn topic_state(
        &self,
        chain: &ChainId,
        topic: &str,
    ) -> StreamResult<(Option<i64>, Option<i64>, Option<i64>)> {
        Ok(self
            .topic_row(chain, topic)
            .await?
            .unwrap_or((None, None, None)))
    }

    /// [Inner::topic_state], or `None` when the topic has no row.
    async fn topic_row(
        &self,
        chain: &ChainId,
        topic: &str,
    ) -> StreamResult<Option<(Option<i64>, Option<i64>, Option<i64>)>> {
        let chain = Chain::of(chain);
        let rows = self
            .query(
                Call::Read,
                &self.sql.topic_state,
                &[
                    &chain.namespace,
                    &chain.kind,
                    &chain.owner,
                    &chain.run,
                    &chain.activity,
                    &topic,
                ],
            )
            .await?;
        Ok(rows.first().map(|row| (row.get(0), row.get(1), row.get(2))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_server_older_than_postgres_14_is_refused() {
        for (version, ok) in [
            ("140000", true),
            ("170011", true),
            ("130016", false),
            ("not a number", false),
        ] {
            let checked = check_version(version);
            assert_eq!(checked.is_ok(), ok, "{version}");
            if let Err(error) = checked {
                assert_eq!(error.kind, StreamFailureKind::Unsupported);
                assert!(error.message.contains("PostgreSQL 14"), "{error}");
            }
        }
    }

    #[test]
    fn a_schema_name_is_a_plain_identifier() {
        for (schema, ok) in [
            ("temporal_streams", true),
            ("_s1", true),
            ("streams_2", true),
            ("", false),
            ("1streams", false),
            ("Streams", false),
            ("a;drop", false),
            ("a-b", false),
            ("pg_streams", false),
            ("user", false),
            ("select", false),
            ("users", true),
        ] {
            assert_eq!(valid_schema(schema), ok, "{schema:?}");
        }
        assert!(valid_schema(&"a".repeat(55)));
        assert!(!valid_schema(&"a".repeat(56)));
    }

    #[test]
    fn a_workflow_activity_keeps_its_workflow_as_the_owner() {
        let chain = Chain::of(&ChainId {
            namespace: "ns".to_string(),
            workflow_id: "wf".to_string(),
            first_run_id: "run".to_string(),
            owner_kind: StreamOwnerKind::WorkflowActivity as i32,
            activity_id: "1".to_string(),
        });
        assert_eq!((chain.kind, chain.owner.as_str()), (3, "wf"));
        assert_eq!(chain.activity, "1");
        let activity = Chain::of(&ChainId {
            namespace: "ns".to_string(),
            first_run_id: "run".to_string(),
            owner_kind: StreamOwnerKind::Activity as i32,
            activity_id: "act".to_string(),
            ..Default::default()
        });
        assert_eq!((activity.kind, activity.owner.as_str()), (2, "act"));
        assert_eq!(activity.activity, "");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parked_readers_do_not_slow_appends() {
        let Ok(url) = std::env::var("STREAMS_POSTGRES_URL") else {
            eprintln!("set STREAMS_POSTGRES_URL to run the PostgreSQL store tests");
            return;
        };
        let mut options = PostgresStoreOptions::new(url);
        options.schema = format!("parked_{}", std::process::id());
        options.max_connections = 2;
        let store = Arc::new(PostgresStore::connect(options).await.unwrap());
        let chain = |n: usize| ChainId {
            namespace: "ns".to_string(),
            workflow_id: format!("parked-{n}"),
            first_run_id: "run-1".to_string(),
            ..Default::default()
        };
        let readers: Vec<_> = (0..100)
            .map(|n| {
                let store = store.clone();
                tokio::spawn(async move {
                    store
                        .read(StoreReadRequest {
                            chain: Some(chain(n)),
                            topic: "out".to_string(),
                            wait: Some(Duration::from_secs(30).try_into().unwrap()),
                            max_records: 10,
                            ..Default::default()
                        })
                        .await
                })
            })
            .collect();
        let parked = Instant::now() + Duration::from_secs(10);
        while store.inner.waiters.len() < 100 {
            assert!(
                Instant::now() < parked,
                "{} parked",
                store.inner.waiters.len()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut slowest = Duration::ZERO;
        for sequence in 1..=20 {
            let started = Instant::now();
            store
                .append(StoreAppendRequest {
                    chain: Some(chain(1000)),
                    topic: "out".to_string(),
                    producer_id: "p".to_string(),
                    attempt: 1,
                    sequence,
                    digest: vec![sequence as u8],
                    records: vec![b"\x1a\x03out".to_vec()],
                })
                .await
                .unwrap();
            slowest = slowest.max(started.elapsed());
        }
        // A pool of two connections serves the appends, so no waiting read holds one.
        assert!(
            slowest < Duration::from_secs(1),
            "slowest append {slowest:?}"
        );
        assert!(store.inner.pool.status().size <= 2);
        for reader in &readers {
            reader.abort();
        }
        for reader in readers {
            assert!(reader.await.unwrap_err().is_cancelled());
        }
        assert!(store.inner.waiters.streams().is_empty());
    }

    #[test]
    fn every_topic_of_a_chain_lands_in_one_partition() {
        let chain = |run: &str| {
            Chain::of(&ChainId {
                namespace: "ns".to_string(),
                workflow_id: "wf".to_string(),
                first_run_id: run.to_string(),
                ..Default::default()
            })
        };
        assert_eq!(chain("a").partition(), chain("a").partition());
        let partitions: std::collections::BTreeSet<_> = (0..1000)
            .map(|n| chain(&n.to_string()).partition())
            .collect();
        assert!(partitions.iter().all(|p| (0..64).contains(p)));
        assert_eq!(partitions.len(), 64);
    }
}
