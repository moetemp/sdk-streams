//! What the Redis store owes beyond the store contract: the stored format other SDKs read, and
//! how Redis failures reach the caller.
//!
//! Set `STREAMS_REDIS_URL` to run these.

#![cfg(feature = "redis")]

use prost::Message;
use redis::AsyncCommands;
use serde_json::Value as Json;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_streams::{
    RedisStore, RedisStoreOptions, StreamResult, StreamStore,
    proto::{
        ChainId, StoreAppendRequest, StoreAppendResponse, StoreLatestRequest, StoreReadRequest,
        StreamFailureKind, StreamRecord,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

static IDS: AtomicUsize = AtomicUsize::new(0);

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        IDS.fetch_add(1, Ordering::Relaxed)
    )
}

fn url() -> Option<String> {
    let url = std::env::var("STREAMS_REDIS_URL").ok();
    if url.is_none() {
        eprintln!("set STREAMS_REDIS_URL to run the Redis store tests");
    }
    url
}

struct Setup {
    store: RedisStore,
    raw: redis::aio::MultiplexedConnection,
    prefix: String,
}

async fn setup_at(url: &str, raw_url: &str) -> Setup {
    setup_with(url, raw_url, &unique("redis-store"), |_| {}).await
}

async fn setup_with(
    url: &str,
    raw_url: &str,
    prefix: &str,
    configure: impl FnOnce(&mut RedisStoreOptions),
) -> Setup {
    let prefix = prefix.to_string();
    let mut options = RedisStoreOptions::new(url);
    options.key_prefix = prefix.clone();
    configure(&mut options);
    Setup {
        store: RedisStore::connect(options).await.unwrap(),
        raw: redis::Client::open(raw_url)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap(),
        prefix,
    }
}

async fn setup() -> Option<Setup> {
    let url = url()?;
    Some(setup_at(&url, &url).await)
}

fn chain() -> ChainId {
    ChainId {
        namespace: "ns".to_string(),
        workflow_id: unique("wf"),
        first_run_id: "run-1".to_string(),
    }
}

fn record(topic: &str, producer: &str, sequence: i64) -> Vec<u8> {
    StreamRecord {
        topic: topic.to_string(),
        producer_id: producer.to_string(),
        attempt: 1,
        sequence,
        ..Default::default()
    }
    .encode_to_vec()
}

fn append(
    chain: &ChainId,
    producer: &str,
    sequence: i64,
    count: i64,
    digest: &[u8],
) -> StoreAppendRequest {
    StoreAppendRequest {
        chain: Some(chain.clone()),
        topic: "events".to_string(),
        producer_id: producer.to_string(),
        attempt: 1,
        sequence,
        digest: digest.to_vec(),
        records: (0..count)
            .map(|index| record("events", producer, sequence + index))
            .collect(),
    }
}

fn part(text: &str) -> String {
    text.bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

fn base(prefix: &str, chain: &ChainId) -> String {
    format!(
        "{}:{{{}:{}:{}}}",
        part(prefix),
        part(&chain.namespace),
        part(&chain.workflow_id),
        part(&chain.first_run_id)
    )
}

fn log_key(prefix: &str, chain: &ChainId, topic: &str) -> String {
    format!("{}:t:{}", base(prefix, chain), part(topic))
}

async fn held(raw: &mut redis::aio::MultiplexedConnection, meta: &str) -> HashMap<String, String> {
    let fields: HashMap<String, String> = raw.hgetall(meta).await.unwrap();
    fields
        .into_iter()
        .filter(|(field, _)| field.starts_with("hw:"))
        .collect()
}

#[tokio::test]
async fn a_batch_is_one_script_and_keeps_one_high_water_field() {
    let Some(mut setup) = setup().await else {
        return;
    };
    let chain = chain();
    for batch in 0..5 {
        setup
            .store
            .append(append(&chain, "p", 1 + 2 * batch, 2, &[batch as u8 + 1]))
            .await
            .unwrap();
    }
    let log = log_key(&setup.prefix, &chain, "events");
    let entries: Vec<(String, HashMap<String, Vec<u8>>)> =
        setup.raw.xrange_all(&log).await.unwrap();
    let sequences: Vec<_> = entries
        .iter()
        .map(|(_, fields)| StreamRecord::decode(&*fields["r"]).unwrap().sequence)
        .collect();
    assert_eq!(sequences, (1..=10).collect::<Vec<_>>());
    // Bounded state: one field per producer attempt, however long it writes.
    let held = held(&mut setup.raw, &format!("{log}:meta")).await;
    assert_eq!(held.len(), 1);
    let value = &held["hw:1:p:1"];
    // The newest batch's first sequence, its record count, its ids and the digest in hex.
    let parts: Vec<_> = value.split('|').collect();
    assert_eq!(parts[..2], ["9", "2"]);
    assert_eq!(parts[2], entries[8].0);
    assert_eq!(parts[3], entries[9].0);
    assert_eq!(parts[4], "05");
}

#[tokio::test]
async fn retiring_a_producer_deletes_its_session_field_only() {
    let Some(mut setup) = setup().await else {
        return;
    };
    let chain = chain();
    for producer in ["p", "q"] {
        setup
            .store
            .append(append(&chain, producer, 1, 1, b"\x01"))
            .await
            .unwrap();
    }
    let meta = format!("{}:meta", log_key(&setup.prefix, &chain, "events"));
    setup
        .store
        .retire_producer(&chain, "events", "p", 1)
        .await
        .unwrap();
    let fields: HashMap<String, String> = setup.raw.hgetall(&meta).await.unwrap();
    assert!(!fields.contains_key("hw:1:p:1"), "{fields:?}");
    assert!(fields.contains_key("hw:1:q:1"), "{fields:?}");
    assert_eq!(fields["added"], "2");
    // A topic Redis holds nothing for keeps holding nothing.
    setup
        .store
        .retire_producer(&chain, "unknown", "p", 1)
        .await
        .unwrap();
    let exists: bool = setup
        .raw
        .exists(format!(
            "{}:meta",
            log_key(&setup.prefix, &chain, "unknown")
        ))
        .await
        .unwrap();
    assert!(!exists);
}

#[tokio::test]
async fn appends_answer_as_the_python_sdk_recorded() {
    // The outcomes the Python provider's script gave, run again through the Core store.
    let Some(mut setup) = setup().await else {
        return;
    };
    let vectors: Json =
        serde_json::from_str(include_str!("../testdata/redis/scripts.json")).unwrap();
    let chain = chain();
    let mut landed: HashMap<String, StoreAppendResponse> = HashMap::new();
    for step in vectors["append"].as_array().unwrap() {
        let label = step["label"].as_str().unwrap();
        if label == "new batch after close" {
            setup.store.close_chain(&chain).await.unwrap();
        }
        let digest = hex(step["digest"].as_str().unwrap());
        let result = setup
            .store
            .append(append(
                &chain,
                "p",
                step["sequence"].as_i64().unwrap(),
                step["records"].as_i64().unwrap(),
                &digest,
            ))
            .await;
        match step["outcome"].as_str().unwrap() {
            "ok" => {
                let response = result.unwrap();
                if let Some(same) = step.get("same_ids_as").and_then(Json::as_str) {
                    assert_eq!(response, landed[same], "{label}");
                }
                landed.insert(label.to_string(), response);
            }
            _ => {
                let kind = match step["error_prefix"].as_str().unwrap() {
                    "STREAMS_DIVERGENT" => StreamFailureKind::ProducerDivergent,
                    "STREAMS_STALE" => StreamFailureKind::ProducerStale,
                    "STREAMS_CLOSED" => StreamFailureKind::Closed,
                    other => panic!("no class for {other}"),
                };
                assert_eq!(result.unwrap_err().kind, kind, "{label}");
            }
        }
    }
    assert_eq!(
        landed["retry of newest batch after close"],
        landed["gap after the newest batch"]
    );
    let gap = &landed["gap after the newest batch"];
    let meta = format!("{}:meta", log_key(&setup.prefix, &chain, "events"));
    let expected = format!("10|1|{}|{}|ee", gap.first_position, gap.last_position);
    let shape: Vec<_> = vectors["meta_value_after_next_and_gap"]
        .as_str()
        .unwrap()
        .split('|')
        .collect();
    assert_eq!(expected.split('|').count(), shape.len());
    assert_eq!(held(&mut setup.raw, &meta).await["hw:1:p:1"], expected);
}

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

#[tokio::test]
async fn the_session_field_counts_the_producer_id_in_utf8_bytes() {
    let Some(mut setup) = setup().await else {
        return;
    };
    let chain = chain();
    let mut request = append(&chain, "日本", 1, 1, b"\x01");
    request.attempt = 7;
    setup.store.append(request).await.unwrap();
    let meta = format!("{}:meta", log_key(&setup.prefix, &chain, "events"));
    let held = held(&mut setup.raw, &meta).await;
    assert_eq!(held.keys().collect::<Vec<_>>(), ["hw:6:日本:7"]);
}

#[tokio::test]
async fn ids_with_separators_and_braces_keep_their_own_keys() {
    let Some(url) = url() else {
        return;
    };
    let mut setup = setup_at(&url, &url).await;
    let one = ChainId {
        namespace: "ns".to_string(),
        workflow_id: format!("{}:{{b}}", unique("a")),
        first_run_id: "c".to_string(),
    };
    let (head, tail) = one.workflow_id.split_once(':').unwrap();
    let other = ChainId {
        workflow_id: head.to_string(),
        first_run_id: format!("{tail}:c"),
        ..one.clone()
    };
    setup
        .store
        .append(append(&one, "p", 1, 1, b"\x01"))
        .await
        .unwrap();
    setup
        .store
        .append(append(&other, "p", 1, 2, b"\x02"))
        .await
        .unwrap();
    for (chain, count) in [(&one, 1), (&other, 2)] {
        let log = log_key(&setup.prefix, chain, "events");
        assert!(log.contains("%7Bb%7D") || log.contains("%3A"), "{log}");
        let length: usize = setup.raw.xlen(&log).await.unwrap();
        assert_eq!(length, count, "{log}");
    }
}

#[tokio::test]
async fn a_reply_that_refuses_a_write_arrives_as_refused() {
    let Some(mut setup) = setup().await else {
        return;
    };
    let chain = chain();
    let meta = format!("{}:meta", log_key(&setup.prefix, &chain, "events"));
    let _: () = setup.raw.set(&meta, "not a hash").await.unwrap();
    let error = setup
        .store
        .append(append(&chain, "p", 1, 1, b"\x01"))
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Refused);
    assert!(error.message.contains("WRONGTYPE"), "{error}");
}

/// Passes bytes between the store and Redis, and fails one connection on purpose when asked.
struct Faults {
    port: u16,
    mode: Arc<AtomicU8>,
    kills: tokio::sync::watch::Sender<u64>,
}

const PASS: u8 = 0;
/// Forward the next script call, then drop the connection before Redis answers.
const LOSE_REPLY: u8 = 1;
/// Drop the connection instead of forwarding the next request.
const LOSE_REQUEST: u8 = 2;
/// Forward the next blocking read, then never answer on that connection, and keep it open, as a
/// server that died without a reset does.
const HOLD_BLOCKED: u8 = 3;
/// Hold the next script call back a while before forwarding it.
const DELAY_SCRIPT: u8 = 4;

impl Faults {
    async fn start(url: &str) -> Self {
        let upstream = url
            .trim_start_matches("redis://")
            .split('/')
            .next()
            .unwrap()
            .to_string();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let mode = Arc::new(AtomicU8::new(PASS));
        let (kills, _) = tokio::sync::watch::channel(0);
        let shared = mode.clone();
        let killer = kills.clone();
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let server = TcpStream::connect(&upstream).await.unwrap();
                tokio::spawn(relay(client, server, shared.clone(), killer.subscribe()));
            }
        });
        Self { port, mode, kills }
    }

    fn url(&self) -> String {
        format!("redis://127.0.0.1:{}/0", self.port)
    }

    fn next(&self, mode: u8) {
        self.mode.store(mode, Ordering::SeqCst);
    }

    /// Drops every connection that has carried a blocking read, as a Redis restart does to the
    /// idle ones in a pool.
    fn kill_blocking_connections(&self) {
        self.kills.send_modify(|generation| *generation += 1);
    }
}

async fn relay(
    client: TcpStream,
    server: TcpStream,
    mode: Arc<AtomicU8>,
    mut kills: tokio::sync::watch::Receiver<u64>,
) {
    let (mut client_in, mut client_out) = client.into_split();
    let (mut server_in, mut server_out) = server.into_split();
    let muted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mute = muted.clone();
    let replies = tokio::spawn(async move {
        let mut buffer = vec![0; 64 * 1024];
        while let Ok(read) = server_in.read(&mut buffer).await {
            if read == 0 {
                break;
            }
            if mute.load(Ordering::SeqCst) {
                continue;
            }
            if client_out.write_all(&buffer[..read]).await.is_err() {
                break;
            }
        }
    });
    let mut blocking = false;
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let read = tokio::select! {
            read = client_in.read(&mut buffer) => read,
            _ = kills.changed(), if blocking => break,
        };
        let Ok(read) = read else { break };
        if read == 0 {
            break;
        }
        let chunk = &buffer[..read];
        let has = |needle: &[u8]| chunk.windows(needle.len()).any(|window| window == needle);
        let script = has(b"EVAL");
        blocking |= has(b"BLOCK");
        let now = match mode.load(Ordering::SeqCst) {
            LOSE_REPLY | DELAY_SCRIPT if !script => PASS,
            HOLD_BLOCKED if !has(b"BLOCK") => PASS,
            _ => mode.swap(PASS, Ordering::SeqCst),
        };
        match now {
            LOSE_REQUEST => break,
            LOSE_REPLY => {
                muted.store(true, Ordering::SeqCst);
                let _ = server_out.write_all(chunk).await;
                // Long enough for Redis to run the request, so only its answer is lost.
                tokio::time::sleep(Duration::from_millis(200)).await;
                break;
            }
            HOLD_BLOCKED => {
                muted.store(true, Ordering::SeqCst);
                let _ = server_out.write_all(chunk).await;
            }
            DELAY_SCRIPT => {
                tokio::time::sleep(Duration::from_millis(300)).await;
                if server_out.write_all(chunk).await.is_err() {
                    break;
                }
            }
            _ => {
                if server_out.write_all(chunk).await.is_err() {
                    break;
                }
            }
        }
    }
    replies.abort();
}

/// Repeats a call the way a producer does after a lost connection, until Redis is reachable.
async fn until_reachable<T>(mut call: impl AsyncFnMut() -> StreamResult<T>) -> StreamResult<T> {
    for _ in 0..50 {
        match call().await {
            Err(error)
                if matches!(
                    error.kind,
                    StreamFailureKind::OutcomeUnknown | StreamFailureKind::Storage
                ) =>
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            other => return other,
        }
    }
    call().await
}

#[tokio::test]
async fn a_lost_connection_is_an_unknown_outcome_and_the_retry_dedupes() {
    let Some(url) = url() else {
        return;
    };
    let faults = Faults::start(&url).await;
    let mut setup = setup_at(&faults.url(), &url).await;
    let chain = chain();
    // A first batch loads the script, so the lost request is the append itself.
    setup
        .store
        .append(append(&chain, "warm", 1, 1, b"\x01"))
        .await
        .unwrap();
    faults.next(LOSE_REPLY);
    let error = setup
        .store
        .append(append(&chain, "p", 1, 1, b"\x02"))
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::OutcomeUnknown, "{error}");
    let log = log_key(&setup.prefix, &chain, "events");
    let length: usize = setup.raw.xlen(&log).await.unwrap();
    assert_eq!(length, 2, "the lost append landed");
    // The producer kept its sequence, so the retry matches what landed.
    let retried =
        until_reachable(async || setup.store.append(append(&chain, "p", 1, 1, b"\x02")).await)
            .await
            .unwrap();
    let latest = setup
        .store
        .latest(StoreLatestRequest {
            chain: Some(chain.clone()),
            topic: "events".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(retried.last_position, latest.position);
    let length: usize = setup.raw.xlen(&log).await.unwrap();
    assert_eq!(length, 2);
}

#[tokio::test]
async fn store_errors_on_reads_arrive_as_storage_failures() {
    let Some(url) = url() else {
        return;
    };
    let faults = Faults::start(&url).await;
    let setup = setup_at(&faults.url(), &url).await;
    let chain = chain();
    faults.next(LOSE_REQUEST);
    let error = setup
        .store
        .latest(StoreLatestRequest {
            chain: Some(chain.clone()),
            topic: "events".to_string(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Storage, "{error}");
    // Reconnected, and lost again.
    until_reachable(async || {
        setup
            .store
            .latest(StoreLatestRequest {
                chain: Some(chain.clone()),
                topic: "events".to_string(),
            })
            .await
    })
    .await
    .unwrap();
    faults.next(LOSE_REQUEST);
    let error = setup
        .store
        .read(StoreReadRequest {
            chain: Some(chain.clone()),
            topic: "events".to_string(),
            max_records: 10,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Storage, "{error}");
}

async fn retained_for(retention: Duration) -> Option<Setup> {
    let url = url()?;
    Some(
        setup_with(&url, &url, &unique("retention"), |options| {
            options.retention = retention
        })
        .await,
    )
}

const GRACE_MS: i64 = 30 * 24 * 60 * 60 * 1000;

#[tokio::test]
async fn retention_must_be_positive() {
    let mut options = RedisStoreOptions::new("redis://localhost:1");
    options.retention = Duration::ZERO;
    let error = RedisStore::connect(options).await.unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Refused);
    assert!(error.message.contains("retention"), "{error}");
}

#[tokio::test]
async fn appends_trim_to_retention_and_slide_the_expiry() {
    let Some(mut setup) = retained_for(Duration::from_millis(200)).await else {
        return;
    };
    let chain = chain();
    setup
        .store
        .append(append(&chain, "p", 1, 2, b"\x01"))
        .await
        .unwrap();
    let log = log_key(&setup.prefix, &chain, "events");
    let meta = format!("{log}:meta");
    let ttl: i64 = setup.raw.pttl(&log).await.unwrap();
    assert!(0 < ttl && ttl <= 200, "{ttl}");
    // The meta outlives the log by the tombstone grace.
    let ttl: i64 = setup.raw.pttl(&meta).await.unwrap();
    assert!(ttl > GRACE_MS - 60_000, "{ttl}");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let landed = setup
        .store
        .append(append(&chain, "p", 3, 1, b"\x02"))
        .await
        .unwrap();
    let entries: Vec<(String, HashMap<String, Vec<u8>>)> =
        setup.raw.xrange_all(&log).await.unwrap();
    let sequences: Vec<_> = entries
        .iter()
        .map(|(_, fields)| StreamRecord::decode(&*fields["r"]).unwrap().sequence)
        .collect();
    assert_eq!(sequences, [3]);
    let fields: HashMap<String, String> = setup.raw.hgetall(&meta).await.unwrap();
    assert_eq!(fields["added"], "3");
    assert_eq!(fields["last"], landed.last_position);
    // The trim leaves the newest dropped id, since XTRIM leaves no trace a reader could compare.
    assert!(fields.contains_key("trimmed"));
}

#[tokio::test]
async fn rule_18_1_a_session_older_than_the_retention_leaves_the_meta() {
    // One field per session would otherwise grow the meta for as long as the topic is written,
    // long after the records it guards are gone.
    let Some(mut setup) = retained_for(Duration::from_millis(200)).await else {
        return;
    };
    let chain = chain();
    setup
        .store
        .append(append(&chain, "old", 1, 1, b"\x01"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    setup
        .store
        .append(append(&chain, "new", 1, 1, b"\x02"))
        .await
        .unwrap();
    let meta = format!("{}:meta", log_key(&setup.prefix, &chain, "events"));
    let held = held(&mut setup.raw, &meta).await;
    assert_eq!(held.keys().collect::<Vec<_>>(), ["hw:3:new:1"]);
}

fn waiting_read(chain: &ChainId, wait: Duration) -> StoreReadRequest {
    StoreReadRequest {
        chain: Some(chain.clone()),
        topic: "events".to_string(),
        max_records: 10,
        wait: Some(wait.try_into().unwrap()),
        ..Default::default()
    }
}

#[tokio::test]
async fn rule_8_2_a_blocked_read_on_a_silent_server_ends_by_its_deadline() {
    let Some(url) = url() else {
        return;
    };
    let faults = Faults::start(&url).await;
    let setup = setup_with(&faults.url(), &url, &unique("silent"), |options| {
        options.blocking_reads_per_node = 1;
        options.response_timeout = Duration::from_millis(500);
    })
    .await;
    let chain = chain();
    faults.next(HOLD_BLOCKED);
    let started = tokio::time::Instant::now();
    let read = tokio::time::timeout(
        Duration::from_secs(10),
        setup
            .store
            .read(waiting_read(&chain, Duration::from_millis(300))),
    )
    .await
    .expect("the read must end by its wait plus the response timeout");
    assert_eq!(read.unwrap_err().kind, StreamFailureKind::Storage);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    // The connection was dropped, so its place in the pool is free for the next read.
    let next = setup
        .store
        .read(waiting_read(&chain, Duration::from_millis(100)))
        .await
        .unwrap();
    assert!(next.records.is_empty());
}

#[tokio::test]
async fn rule_8_3_a_dead_idle_connection_is_replaced_in_the_same_read() {
    let Some(url) = url() else {
        return;
    };
    let faults = Faults::start(&url).await;
    let setup = setup_at(&faults.url(), &url).await;
    let chain = chain();
    // The first waiting read leaves its connection idle in the pool.
    setup
        .store
        .read(waiting_read(&chain, Duration::from_millis(100)))
        .await
        .unwrap();
    faults.kill_blocking_connections();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let read = setup
        .store
        .read(waiting_read(&chain, Duration::from_millis(100)))
        .await;
    assert!(read.is_ok(), "{read:?}");
}

/// Temporal with one Workflow whose latest run can end.
struct OneOwner {
    status: std::sync::Mutex<
        temporalio_common::protos::temporal::api::enums::v1::WorkflowExecutionStatus,
    >,
}

#[async_trait::async_trait]
impl temporalio_streams::OwnerClient for OneOwner {
    async fn describe(
        &self,
        _namespace: &str,
        _workflow_id: &str,
        _run_id: &str,
    ) -> Result<temporalio_streams::OwnerDescription, temporalio_streams::OwnerError> {
        Ok(temporalio_streams::OwnerDescription {
            run_id: "run-1".to_string(),
            first_run_id: "run-1".to_string(),
            status: *self.status.lock().unwrap(),
        })
    }
}

#[tokio::test]
async fn a_new_producer_marks_an_ended_chain_closed_until_it_expires() {
    use temporalio_common::protos::temporal::api::enums::v1::WorkflowExecutionStatus;
    use temporalio_streams::{
        Streams, StreamsOptions,
        proto::{
            AppendRecord, AppendRequest, NamedProducer, StreamAddress, StreamOwnerKind,
            append_request::Producer,
        },
    };
    let Some(url) = url() else {
        return;
    };
    let mut setup = setup_at(&url, &url).await;
    let owner = Arc::new(OneOwner {
        status: std::sync::Mutex::new(WorkflowExecutionStatus::Running),
    });
    let mut options = RedisStoreOptions::new(url.clone());
    options.key_prefix = setup.prefix.clone();
    let streams = Streams::new(
        Arc::new(RedisStore::connect(options).await.unwrap()),
        owner.clone(),
        // Producers share their owner's check, so only a check on every batch finds the end now.
        StreamsOptions {
            owner_recheck: Duration::ZERO,
            ..StreamsOptions::default()
        },
    );
    let chain = chain();
    let request = |producer: &str| AppendRequest {
        stream: Some(StreamAddress {
            namespace: chain.namespace.clone(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: chain.workflow_id.clone(),
            run_id: String::new(),
            topic: "events".to_string(),
        }),
        producer: Some(Producer::Named(NamedProducer {
            producer_id: producer.to_string(),
            attempt: 1,
        })),
        sequence: 1,
        records: vec![AppendRecord {
            kind: 2,
            ..Default::default()
        }],
        digest: vec![1; 32],
    };
    streams.append(request("p")).await.unwrap();
    *owner.status.lock().unwrap() = WorkflowExecutionStatus::Terminated;
    let chain_key = format!("{}:chain", base(&setup.prefix, &chain));
    let closed: Option<String> = setup.raw.hget(&chain_key, "closed").await.unwrap();
    assert_eq!(closed, None);
    let error = streams.append(request("q")).await.unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Closed);
    let closed: Option<String> = setup.raw.hget(&chain_key, "closed").await.unwrap();
    assert_eq!(closed.as_deref(), Some("1"));
    let ttl: i64 = setup.raw.pttl(&chain_key).await.unwrap();
    assert!(ttl > 0, "{ttl}");
}

#[tokio::test]
async fn a_topic_closed_before_its_first_write_still_expires() {
    let Some(mut setup) = setup().await else {
        return;
    };
    let chain = chain();
    setup.store.close_topic(&chain, "events").await.unwrap();
    let meta = format!("{}:meta", log_key(&setup.prefix, &chain, "events"));
    let closed: Option<String> = setup.raw.hget(&meta, "closed").await.unwrap();
    assert_eq!(closed.as_deref(), Some("1"));
    let ttl: i64 = setup.raw.pttl(&meta).await.unwrap();
    assert!(ttl > 0, "{ttl}");
    let error = setup
        .store
        .append(append(&chain, "p", 1, 1, b"\x01"))
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Closed);
}

#[tokio::test]
async fn rule_10_1_an_append_that_races_a_topic_close_is_refused_once_the_close_returned() {
    let Some(url) = url() else {
        return;
    };
    let faults = Faults::start(&url).await;
    let prefix = unique("race");
    let producer = setup_with(&faults.url(), &url, &prefix, |_| {}).await;
    let closer = setup_with(&url, &url, &prefix, |_| {}).await;
    let chain = chain();
    // A first batch loads the script, so the held call is the second batch's.
    producer
        .store
        .append(append(&chain, "p", 1, 1, b"\x01"))
        .await
        .unwrap();
    faults.next(DELAY_SCRIPT);
    let racing = {
        let chain = chain.clone();
        tokio::spawn(async move {
            producer
                .store
                .append(append(&chain, "p", 2, 1, b"\x02"))
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    closer.store.close_topic(&chain, "events").await.unwrap();
    let error = racing.await.unwrap().unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Closed, "{error}");
    let mut raw = closer.raw.clone();
    let length: usize = raw.xlen(log_key(&prefix, &chain, "events")).await.unwrap();
    assert_eq!(length, 1);
}

fn read_after(chain: &ChainId, position: &str, max_records: u32) -> StoreReadRequest {
    StoreReadRequest {
        chain: Some(chain.clone()),
        topic: "events".to_string(),
        after_position: position.to_string(),
        max_records,
        ..Default::default()
    }
}

#[tokio::test]
async fn an_expired_log_leaves_a_tombstone_that_tells_expired_from_empty() {
    let Some(mut setup) = retained_for(Duration::from_millis(300)).await else {
        return;
    };
    let chain = chain();
    let first = setup
        .store
        .append(append(&chain, "p", 1, 1, b"\x01"))
        .await
        .unwrap();
    let last = setup
        .store
        .append(append(&chain, "p", 2, 1, b"\x02"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let log = log_key(&setup.prefix, &chain, "events");
    let exists: bool = setup.raw.exists(&log).await.unwrap();
    assert!(!exists);
    let error = setup
        .store
        .read(read_after(&chain, &first.last_position, 10))
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Expired);
    assert!(error.message.contains("expired"), "{error}");
    // Nothing came after the last record, so the stream is empty after it.
    let empty = setup
        .store
        .read(read_after(&chain, &last.last_position, 10))
        .await
        .unwrap();
    assert!(empty.records.is_empty());
    let _: () = setup.raw.del(format!("{log}:meta")).await.unwrap();
    let error = setup
        .store
        .read(read_after(&chain, &last.last_position, 10))
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::NotFound);
    assert!(error.message.contains("no tombstone"), "{error}");
    // A read from the beginning of a missing log just finds no records.
    let empty = setup.store.read(read_after(&chain, "", 10)).await.unwrap();
    assert!(empty.records.is_empty());
}

#[tokio::test]
async fn retention_expires_a_cursor_only_past_the_newest_trimmed_record() {
    let Some(setup) = retained_for(Duration::from_millis(300)).await else {
        return;
    };
    let chain = chain();
    let first = setup
        .store
        .append(append(&chain, "p", 1, 1, b"\x01"))
        .await
        .unwrap();
    let second = setup
        .store
        .append(append(&chain, "p", 2, 1, b"\x02"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    let third = setup
        .store
        .append(append(&chain, "p", 3, 1, b"\x03"))
        .await
        .unwrap();
    let error = setup
        .store
        .read(read_after(&chain, &first.last_position, 10))
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Expired);
    assert!(error.message.contains("dropped by retention"), "{error}");
    // Nothing after the newest trimmed record was lost, so it still resumes.
    let resumed = setup
        .store
        .read(read_after(&chain, &second.last_position, 10))
        .await
        .unwrap();
    let positions: Vec<_> = resumed.records.iter().map(|r| &r.position).collect();
    assert_eq!(positions, [&third.last_position]);
}

#[tokio::test]
async fn a_read_that_falls_behind_retention_fails_instead_of_skipping() {
    let Some(setup) = retained_for(Duration::from_millis(300)).await else {
        return;
    };
    let chain = chain();
    setup
        .store
        .append(append(&chain, "p", 1, 2, b"\x01"))
        .await
        .unwrap();
    // One record per read, so the trim lands between two reads of one batch.
    let first = setup.store.read(read_after(&chain, "", 1)).await.unwrap();
    assert_eq!(first.records.len(), 1);
    tokio::time::sleep(Duration::from_millis(350)).await;
    // This append trims both earlier records, and the second was never delivered.
    let third = setup
        .store
        .append(append(&chain, "p", 3, 1, b"\x02"))
        .await
        .unwrap();
    let error = setup
        .store
        .read(read_after(&chain, &first.records[0].position, 1))
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Expired);
    assert!(
        error.message.contains("while this read was behind"),
        "{error}"
    );
    // A read that starts after the trim begins at the oldest record left.
    let fresh = setup.store.read(read_after(&chain, "", 1)).await.unwrap();
    assert_eq!(fresh.records[0].position, third.last_position);
}

#[tokio::test]
async fn rule_22_1_a_lost_connection_on_the_read_checks_is_a_storage_failure() {
    let Some(url) = url() else {
        return;
    };
    let faults = Faults::start(&url).await;
    let setup = setup_at(&faults.url(), &url).await;
    let chain = chain();
    let landed = setup
        .store
        .append(append(&chain, "p", 1, 1, b"\x01"))
        .await
        .unwrap();
    faults.next(LOSE_REQUEST);
    let error = setup
        .store
        .read(read_after(&chain, &landed.last_position, 10))
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Storage, "{error}");
}

#[tokio::test]
async fn rule_20_4_a_malformed_entry_fails_with_its_cursor() {
    use temporalio_streams::{ReadTarget, mint_cursor, proto::ReadRequest, read_page, stream_hash};
    let Some(mut setup) = setup().await else {
        return;
    };
    let chain = chain();
    setup
        .store
        .append(append(&chain, "p", 1, 1, b"\x01"))
        .await
        .unwrap();
    let log = log_key(&setup.prefix, &chain, "events");
    let entry: String = setup.raw.xadd(&log, "*", &[("not-r", "x")]).await.unwrap();
    setup
        .store
        .append(append(&chain, "p", 2, 1, b"\x02"))
        .await
        .unwrap();
    let target = ReadTarget {
        chain: chain.clone(),
        topic: "events".to_string(),
        stream_hash: stream_hash(&chain.namespace, "workflow", &chain.workflow_id, "events"),
    };
    let first = read_page(&setup.store, &target, &ReadRequest::default())
        .await
        .unwrap();
    assert_eq!(first.records.len(), 1);
    let error = read_page(
        &setup.store,
        &target,
        &ReadRequest {
            after: first.cursor.clone(),
            state: first.state.clone(),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Record);
    let bad = mint_cursor("redis", &target.stream_hash, &entry);
    assert_eq!(error.cursor.as_deref(), Some(bad.as_str()));
    let after = read_page(
        &setup.store,
        &target,
        &ReadRequest {
            after: bad,
            state: first.state,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(after.records.len(), 1);
}

/// Tests that change the server's eviction policy run one at a time.
static POLICY: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// What the store logs while `connect` runs.
async fn logs_of_connect(options: RedisStoreOptions) -> String {
    #[derive(Clone, Default)]
    struct Logs(Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for Logs {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let logs = Logs::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    RedisStore::connect(options).await.unwrap();
    String::from_utf8(logs.0.lock().unwrap().clone()).unwrap()
}

async fn set_policy(url: &str, policy: &str) {
    let mut admin = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory-policy")
        .arg(policy)
        .query_async(&mut admin)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_noeviction_server_is_not_warned_about() {
    let Some(url) = url() else {
        return;
    };
    let _serial = POLICY.lock().await;
    set_policy(&url, "noeviction").await;
    let logs = logs_of_connect(RedisStoreOptions::new(url)).await;
    assert!(!logs.contains("maxmemory-policy"), "{logs}");
}

#[tokio::test]
async fn an_evicting_server_is_warned_about() {
    let Some(url) = url() else {
        return;
    };
    let _serial = POLICY.lock().await;
    set_policy(&url, "allkeys-lru").await;
    let logs = logs_of_connect(RedisStoreOptions::new(url.clone())).await;
    set_policy(&url, "noeviction").await;
    assert!(logs.contains("allkeys-lru"), "{logs}");
    assert_eq!(logs.matches("maxmemory-policy").count(), 1, "{logs}");
}

#[tokio::test]
async fn a_server_that_refuses_config_is_not_warned_about() {
    let Some(url) = url() else {
        return;
    };
    let _serial = POLICY.lock().await;
    set_policy(&url, "allkeys-lru").await;
    // Managed services often refuse CONFIG, so the store goes on without the check.
    let name = unique("no-config");
    let mut admin = redis::Client::open(url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&name)
        .arg("on")
        .arg(">pw")
        .arg("allkeys")
        .arg("+@all")
        .arg("-config")
        .query_async(&mut admin)
        .await
        .unwrap();
    let address = url.trim_start_matches("redis://");
    let logs = logs_of_connect(RedisStoreOptions::new(format!(
        "redis://{name}:pw@{address}"
    )))
    .await;
    let _: () = redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&name)
        .query_async(&mut admin)
        .await
        .unwrap();
    set_policy(&url, "noeviction").await;
    assert!(!logs.contains("maxmemory-policy"), "{logs}");
}

#[tokio::test]
async fn an_evicting_primary_of_a_cluster_is_warned_about() {
    let Ok(url) = std::env::var("STREAMS_REDIS_CLUSTER_URL") else {
        eprintln!("set STREAMS_REDIS_CLUSTER_URL to run the Redis cluster tests");
        return;
    };
    let _serial = POLICY.lock().await;
    // One primary of three, so a check of only the seed would miss it.
    let mut nodes = redis::Client::open(url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let shards: String = redis::cmd("CLUSTER")
        .arg("NODES")
        .query_async(&mut nodes)
        .await
        .unwrap();
    let seed = url
        .trim_start_matches("redis://")
        .split('/')
        .next()
        .unwrap();
    let other = shards
        .lines()
        .filter(|line| line.contains("master"))
        .filter_map(|line| line.split_whitespace().nth(1))
        .map(|address| address.split('@').next().unwrap().to_string())
        .find(|address| !seed.ends_with(address.rsplit(':').next().unwrap()))
        .unwrap();
    let other_url = format!("redis://{other}/0");
    set_policy(&other_url, "allkeys-lru").await;
    let mut options = RedisStoreOptions::new(url);
    options.cluster = true;
    let logs = logs_of_connect(options).await;
    set_policy(&other_url, "noeviction").await;
    assert!(logs.contains("allkeys-lru"), "{logs}");
    assert!(logs.contains(other.rsplit(':').next().unwrap()), "{logs}");
}

#[tokio::test]
async fn the_documented_acl_is_the_python_sdks() {
    // One rule set for every SDK, so an application's ACL keeps working across the move.
    let Ok(checkout) = std::env::var("STREAMS_BROOK_PY") else {
        eprintln!("set STREAMS_BROOK_PY to compare the documented ACL with the Python SDK's");
        return;
    };
    let rules = |page: &str| -> Vec<String> {
        page.lines()
            .find_map(|line| line.strip_prefix("ACL SETUSER "))
            .unwrap()
            .split_whitespace()
            .map(str::to_string)
            .collect()
    };
    let python = std::fs::read_to_string(
        std::path::Path::new(&checkout).join("temporalio/contrib/streams/docs/redis-guarantees.md"),
    )
    .unwrap();
    // A cluster client asks the topology with `CLUSTER SLOTS`, which the Python SDK's page left
    // out. It is the one rule Core adds.
    let mut expected = rules(&python);
    expected.push("+cluster|slots".to_string());
    assert_eq!(rules(include_str!("../docs/redis.md")), expected);
}

fn delete_request(workflow_id: &str) -> temporalio_streams::proto::DeleteOwnerRequest {
    temporalio_streams::proto::DeleteOwnerRequest {
        namespace: "ns".to_string(),
        owner_kind: temporalio_streams::proto::StreamOwnerKind::Workflow as i32,
        workflow_id: workflow_id.to_string(),
    }
}

#[tokio::test]
async fn an_admin_deletes_one_workflows_stream_keys() {
    let Some(mut setup) = setup().await else {
        return;
    };
    let chain = chain();
    let next = ChainId {
        first_run_id: "run-2".to_string(),
        ..chain.clone()
    };
    let bystander = self::chain();
    // A log, its meta and a close flag on one chain, and a log on the next chain of the same
    // Workflow id.
    setup
        .store
        .append(append(&chain, "p", 1, 1, b"\x01"))
        .await
        .unwrap();
    setup.store.close_chain(&chain).await.unwrap();
    setup
        .store
        .append(append(&next, "p", 1, 1, b"\x01"))
        .await
        .unwrap();
    setup
        .store
        .append(append(&bystander, "p", 1, 1, b"\x01"))
        .await
        .unwrap();
    let pattern = format!("{}:{{ns:{}:*", setup.prefix, part(&chain.workflow_id));
    let before: Vec<String> = redis::cmd("KEYS")
        .arg(&pattern)
        .query_async(&mut setup.raw)
        .await
        .unwrap();
    assert_eq!(before.len(), 5, "{before:?}");
    let deleted = setup
        .store
        .delete_owner(delete_request(&chain.workflow_id))
        .await
        .unwrap();
    assert_eq!(deleted.deleted, 5);
    let after: Vec<String> = redis::cmd("KEYS")
        .arg(&pattern)
        .query_async(&mut setup.raw)
        .await
        .unwrap();
    assert!(after.is_empty(), "{after:?}");
    let left: usize = setup
        .raw
        .xlen(log_key(&setup.prefix, &bystander, "events"))
        .await
        .unwrap();
    assert_eq!(left, 1);
    let again = setup
        .store
        .delete_owner(delete_request(&chain.workflow_id))
        .await
        .unwrap();
    assert_eq!(again.deleted, 0);
}

#[tokio::test]
async fn a_store_error_on_delete_arrives_as_a_stream_error() {
    let Some(url) = url() else {
        return;
    };
    let faults = Faults::start(&url).await;
    let setup = setup_at(&faults.url(), &url).await;
    let chain = chain();
    setup
        .store
        .append(append(&chain, "p", 1, 1, b"\x01"))
        .await
        .unwrap();
    faults.next(LOSE_REQUEST);
    // Some keys may already be gone when a later step fails.
    let error = setup
        .store
        .delete_owner(delete_request(&chain.workflow_id))
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::OutcomeUnknown, "{error}");
}

#[tokio::test]
async fn a_delete_without_scan_is_refused_under_the_documented_acl() {
    let Some(url) = url() else {
        return;
    };
    let prefix = unique("acl-delete");
    let rules = |page: &str| -> Vec<String> {
        page.lines()
            .find_map(|line| line.strip_prefix("ACL SETUSER "))
            .unwrap()
            .split_whitespace()
            .skip(1)
            .map(str::to_string)
            .collect()
    };
    let name = unique("streams-acl");
    let mut admin = redis::Client::open(url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let mut setuser = redis::cmd("ACL");
    setuser.arg("SETUSER").arg(&name);
    for rule in rules(include_str!("../docs/redis.md")) {
        match rule.as_str() {
            "~temporal-streams:{my-ns:*" => setuser.arg(format!("~{}:{{ns:*", part(&prefix))),
            ">secret" => setuser.arg(">pw"),
            rule => setuser.arg(rule),
        };
    }
    let _: () = setuser.query_async(&mut admin).await.unwrap();
    let address = url.trim_start_matches("redis://");
    let mut options = RedisStoreOptions::new(format!("redis://{name}:pw@{address}"));
    options.key_prefix = prefix;
    let store = RedisStore::connect(options).await.unwrap();
    let error = store.delete_owner(delete_request("wf")).await.unwrap_err();
    let _: () = redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&name)
        .query_async(&mut admin)
        .await
        .unwrap();
    assert_eq!(error.kind, StreamFailureKind::Refused, "{error}");
    assert!(error.message.contains("NOPERM"), "{error}");
}

#[tokio::test]
async fn a_delete_reaches_every_primary_of_a_cluster() {
    let Ok(url) = std::env::var("STREAMS_REDIS_CLUSTER_URL") else {
        eprintln!("set STREAMS_REDIS_CLUSTER_URL to run the Redis cluster tests");
        return;
    };
    let prefix = unique("cluster-delete");
    let mut options = RedisStoreOptions::new(url.clone());
    options.cluster = true;
    options.key_prefix = prefix.clone();
    let store = RedisStore::connect(options).await.unwrap();
    let workflow_id = unique("wf");
    for run in 0..30 {
        let chain = ChainId {
            namespace: "ns".to_string(),
            workflow_id: workflow_id.clone(),
            first_run_id: format!("run-{run}"),
        };
        store
            .append(append(&chain, "p", 1, 1, b"\x01"))
            .await
            .unwrap();
    }
    let mut seed = redis::Client::open(url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let nodes: String = redis::cmd("CLUSTER")
        .arg("NODES")
        .query_async(&mut seed)
        .await
        .unwrap();
    let primaries: Vec<String> = nodes
        .lines()
        .filter(|line| line.contains("master"))
        .filter_map(|line| line.split_whitespace().nth(1))
        .map(|address| address.split('@').next().unwrap().to_string())
        .collect();
    let pattern = format!("{}:{{ns:{}:*", part(&prefix), part(&workflow_id));
    let mut held = Vec::new();
    for primary in &primaries {
        let mut node = redis::Client::open(format!("redis://{primary}/0"))
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut node)
            .await
            .unwrap();
        held.push((node, keys.len()));
    }
    assert!(
        held.iter().all(|(_, count)| *count > 0),
        "the chains should land on every primary"
    );
    let deleted = store
        .delete_owner(delete_request(&workflow_id))
        .await
        .unwrap();
    assert_eq!(
        deleted.deleted as usize,
        held.iter().map(|(_, count)| count).sum::<usize>()
    );
    for (mut node, _) in held {
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut node)
            .await
            .unwrap();
        assert!(keys.is_empty(), "{keys:?}");
    }
}

#[tokio::test]
async fn a_retention_past_the_epoch_is_refused_at_connect() {
    // The check runs before any connection, so no server is needed.
    let mut options = RedisStoreOptions::new("redis://127.0.0.1:1");
    options.retention = Duration::from_secs(100 * 365 * 24 * 60 * 60);
    let error = RedisStore::connect(options).await.unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Refused);
    assert!(error.message.contains("retention"), "{error}");
}

#[tokio::test]
async fn a_position_past_the_head_is_one_the_topic_never_held() {
    let Some(setup) = setup().await else {
        return;
    };
    let chain = chain();
    let landed = setup
        .store
        .append(append(&chain, "p", 1, 1, b"\x01"))
        .await
        .unwrap();
    let (ms, _) = landed.last_position.split_once('-').unwrap();
    let past = format!("{}-0", ms.parse::<u64>().unwrap() + 1000);
    let error = setup
        .store
        .read(read_after(&chain, &past, 10))
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Cursor, "{error}");
    assert!(error.message.contains("never held"), "{error}");
}

#[tokio::test]
async fn the_read_checks_run_by_hash_and_survive_a_script_flush() {
    let Some(mut setup) = setup().await else {
        return;
    };
    let chain = chain();
    setup
        .store
        .append(append(&chain, "p", 1, 1, b"\x01"))
        .await
        .unwrap();
    let evalsha_calls = async |raw: &mut redis::aio::MultiplexedConnection| {
        let info: String = redis::cmd("INFO")
            .arg("commandstats")
            .query_async(raw)
            .await
            .unwrap();
        info.lines()
            .find_map(|line| line.strip_prefix("cmdstat_evalsha:calls="))
            .and_then(|rest| rest.split(',').next())
            .map_or(0, |calls| calls.parse::<u64>().unwrap())
    };
    let _: () = redis::cmd("SCRIPT")
        .arg("FLUSH")
        .query_async(&mut setup.raw)
        .await
        .unwrap();
    let read = setup.store.read(read_after(&chain, "", 10)).await.unwrap();
    assert_eq!(read.records.len(), 1);
    let before = evalsha_calls(&mut setup.raw).await;
    let read = setup.store.read(read_after(&chain, "", 10)).await.unwrap();
    assert_eq!(read.records.len(), 1);
    assert!(evalsha_calls(&mut setup.raw).await > before);
}
