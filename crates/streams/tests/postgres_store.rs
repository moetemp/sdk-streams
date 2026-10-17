//! What the PostgreSQL store owes beyond the store contract: migrations, positions under
//! concurrent writers, the chain close, the janitor and its failures. Each test runs in a schema
//! of its own on `STREAMS_POSTGRES_URL`.

#![cfg(feature = "postgres")]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_streams::{
    PostgresStore, PostgresStoreOptions, StreamStore,
    proto::{
        ChainId, DeleteOwnerRequest, StoreAppendRequest, StoreReadRequest, StreamFailureKind,
        StreamOwnerKind,
    },
};
use tokio::time::Instant;

static SCHEMAS: AtomicUsize = AtomicUsize::new(0);

fn url() -> Option<String> {
    let url = std::env::var("STREAMS_POSTGRES_URL").ok();
    if url.is_none() {
        eprintln!("set STREAMS_POSTGRES_URL to run the PostgreSQL store tests");
    }
    url
}

fn options(url: &str) -> PostgresStoreOptions {
    let mut options = PostgresStoreOptions::new(url);
    options.schema = format!(
        "store_{}_{}",
        std::process::id(),
        SCHEMAS.fetch_add(1, Ordering::Relaxed)
    );
    options.max_connections = 4;
    options
}

async fn raw(url: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    client
}

fn chain(workflow_id: &str) -> ChainId {
    ChainId {
        namespace: "ns".to_string(),
        workflow_id: workflow_id.to_string(),
        first_run_id: "run-1".to_string(),
        ..Default::default()
    }
}

fn append(
    chain: &ChainId,
    topic: &str,
    producer: &str,
    sequence: i64,
    n: usize,
) -> StoreAppendRequest {
    StoreAppendRequest {
        chain: Some(chain.clone()),
        topic: topic.to_string(),
        producer_id: producer.to_string(),
        attempt: 1,
        sequence,
        digest: format!("{producer}/{sequence}").into_bytes(),
        records: (0..n)
            .map(|i| format!("r{sequence}.{i}").into_bytes())
            .collect(),
    }
}

fn read(chain: &ChainId, topic: &str, after: &str, wait: Duration) -> StoreReadRequest {
    StoreReadRequest {
        chain: Some(chain.clone()),
        topic: topic.to_string(),
        after_position: after.to_string(),
        wait: Some(wait.try_into().unwrap()),
        max_records: 1000,
    }
}

/// The positions and write times of a topic's records, in position order.
async fn rows(raw: &tokio_postgres::Client, schema: &str) -> Vec<(i64, std::time::SystemTime)> {
    raw.query(
        &format!("SELECT position, written_at FROM {schema}.records ORDER BY topic_id, position"),
        &[],
    )
    .await
    .unwrap()
    .iter()
    .map(|row| (row.get(0), row.get(1)))
    .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ten_connects_at_once_apply_each_migration_once() {
    let Some(url) = url() else { return };
    let options = options(&url);
    let connects: Vec<_> = (0..10)
        .map(|_| {
            let options = options.clone();
            tokio::spawn(async move { PostgresStore::connect(options).await.map(drop) })
        })
        .collect();
    for connect in connects {
        connect.await.unwrap().unwrap();
    }
    let raw = raw(&url).await;
    let row = raw
        .query_one(
            &format!(
                "SELECT count(*), count(DISTINCT version) FROM {}.schema_migrations",
                options.schema
            ),
            &[],
        )
        .await
        .unwrap();
    let (all, distinct): (i64, i64) = (row.get(0), row.get(1));
    assert!(all >= 1);
    assert_eq!(all, distinct);
}

#[tokio::test]
async fn a_changed_migration_is_refused() {
    let Some(url) = url() else { return };
    let options = options(&url);
    drop(PostgresStore::connect(options.clone()).await.unwrap());
    raw(&url)
        .await
        .execute(
            &format!(
                "UPDATE {}.schema_migrations SET checksum = '\\x00' WHERE version = 1",
                options.schema
            ),
            &[],
        )
        .await
        .unwrap();
    let error = PostgresStore::connect(options).await.unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Refused);
    assert!(error.message.contains("other content"), "{error}");
}

#[tokio::test]
async fn a_schema_with_no_migrations_is_refused_when_they_are_skipped() {
    let Some(url) = url() else { return };
    let mut options = options(&url);
    options.skip_migrations = true;
    let error = PostgresStore::connect(options).await.unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Unsupported);
    assert!(error.message.contains("lacks migration 1"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn writers_on_one_topic_get_dense_positions_in_time_order() {
    let Some(url) = url() else { return };
    let mut options = options(&url);
    options.max_connections = 16;
    let schema = options.schema.clone();
    let store = Arc::new(PostgresStore::connect(options).await.unwrap());
    let chain = chain("dense");
    let writers: Vec<_> = (0..16)
        .map(|writer| {
            let store = store.clone();
            let chain = chain.clone();
            tokio::spawn(async move {
                for batch in 0..20 {
                    store
                        .append(append(
                            &chain,
                            "out",
                            &format!("p{writer}"),
                            1 + batch * 3,
                            3,
                        ))
                        .await
                        .unwrap();
                }
            })
        })
        .collect();
    for writer in writers {
        writer.await.unwrap();
    }
    let rows = rows(&raw(&url).await, &schema).await;
    assert_eq!(rows.len(), 16 * 20 * 3);
    for pair in rows.windows(2) {
        assert_eq!(
            pair[1].0,
            pair[0].0 + 1,
            "a gap or a repeat at {}",
            pair[0].0
        );
        assert!(pair[1].1 >= pair[0].1, "time went back at {}", pair[1].0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn appends_against_a_chain_close_land_before_it_returns() {
    let Some(url) = url() else { return };
    let mut options = options(&url);
    options.max_connections = 17;
    let schema = options.schema.clone();
    let store = Arc::new(PostgresStore::connect(options).await.unwrap());
    let chain = chain("closing");
    let landed = Arc::new(AtomicUsize::new(0));
    let writers: Vec<_> = (0..16)
        .map(|writer| {
            let (store, chain, landed) = (store.clone(), chain.clone(), landed.clone());
            tokio::spawn(async move {
                // Writers share 40 topics, and some take their first record near the close.
                for batch in 0.. {
                    let topic = format!("t{}", (writer * 7 + batch) % 40);
                    match store
                        .append(append(&chain, &topic, &format!("p{writer}.{batch}"), 1, 2))
                        .await
                    {
                        Ok(_) => landed.fetch_add(2, Ordering::Relaxed),
                        Err(error) => {
                            assert_eq!(error.kind, StreamFailureKind::Closed, "{error}");
                            return;
                        }
                    };
                }
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(500)).await;
    store.close_chain(&chain).await.unwrap();
    let raw = raw(&url).await;
    let closed: std::time::SystemTime = raw
        .query_one("SELECT clock_timestamp()", &[])
        .await
        .unwrap()
        .get(0);
    for writer in writers {
        writer.await.unwrap();
    }
    let rows = rows(&raw, &schema).await;
    assert!(!rows.is_empty());
    assert_eq!(rows.len(), landed.load(Ordering::Relaxed));
    assert!(rows.iter().all(|(_, at)| *at < closed));
}

/// Moves a schema's clock back by `days` for every row the janitor looks at.
async fn age(raw: &tokio_postgres::Client, schema: &str, days: i32) {
    raw.batch_execute(&format!(
        "UPDATE {schema}.records SET written_at = written_at - interval '{days} days'; \
         UPDATE {schema}.producers SET written_at = written_at - interval '{days} days'; \
         UPDATE {schema}.topics SET written_at = written_at - interval '{days} days', \
           trim_at = now(); \
         UPDATE {schema}.chains SET touched_at = touched_at - interval '{days} days'"
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn the_janitor_keeps_holds_and_deletes_ended_holds_and_tombstones() {
    let Some(url) = url() else { return };
    let options = options(&url);
    let schema = options.schema.clone();
    let store = PostgresStore::connect(options).await.unwrap();
    let raw = raw(&url).await;
    let chain = chain("janitor");
    let landed = store
        .append(append(&chain, "out", "p", 1, 5))
        .await
        .unwrap();
    let first: i64 = landed.first_position.parse().unwrap();
    let last: i64 = landed.last_position.parse().unwrap();
    store
        .hold_floor(&chain, "out", "a/1", &(first + 2).to_string())
        .await
        .unwrap();
    age(&raw, &schema, 8).await;
    assert_eq!(store.run_janitor().await.unwrap(), 2);
    let kept: Vec<i64> = rows(&raw, &schema).await.iter().map(|row| row.0).collect();
    assert_eq!(kept, (first + 2..=last).collect::<Vec<_>>());
    assert_eq!(
        store.dropped_after(&chain, "out", "").await.unwrap(),
        Some((first + 1).to_string())
    );
    // An ended hold goes, and so do the records it held.
    raw.batch_execute(&format!(
        "UPDATE {schema}.floors SET until = now() - interval '1 second'; \
         UPDATE {schema}.topics SET trim_at = now()"
    ))
    .await
    .unwrap();
    assert_eq!(store.run_janitor().await.unwrap(), 3);
    let floors: i64 = raw
        .query_one(&format!("SELECT count(*) FROM {schema}.floors"), &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(floors, 0);
    // The tombstone still answers what the topic held.
    assert_eq!(
        store.newest_ever(&chain, "out").await.unwrap(),
        last.to_string()
    );
    let expired = store
        .read(read(&chain, "out", &first.to_string(), Duration::ZERO))
        .await
        .unwrap_err();
    assert_eq!(expired.kind, StreamFailureKind::Expired);
    // After the retention plus 30 days, the tombstone and the idle chain go.
    age(&raw, &schema, 31).await;
    store.run_janitor().await.unwrap();
    assert_eq!(store.newest_ever(&chain, "out").await.unwrap(), "");
    let chains: i64 = raw
        .query_one(&format!("SELECT count(*) FROM {schema}.chains"), &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(chains, 0);
    // A topic made again starts above every position it had before.
    let again = store
        .append(append(&chain, "out", "p", 1, 1))
        .await
        .unwrap();
    assert!(again.first_position.parse::<i64>().unwrap() > last);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn janitors_on_many_connections_leave_no_lock_behind() {
    // Through PgBouncer in transaction mode, a session lock taken on one server connection and
    // released on another stays, and every later pass skips. Set `STREAMS_POSTGRES_DIRECT_URL` to
    // the server behind it to check the locks.
    let Some(url) = url() else { return };
    let direct = std::env::var("STREAMS_POSTGRES_DIRECT_URL").unwrap_or_else(|_| url.clone());
    let options = options(&url);
    let schema = options.schema.clone();
    let mut stores = vec![Arc::new(
        PostgresStore::connect(options.clone()).await.unwrap(),
    )];
    for _ in 0..3 {
        let mut options = options.clone();
        options.skip_migrations = true;
        stores.push(Arc::new(PostgresStore::connect(options).await.unwrap()));
    }
    let raw = raw(&direct).await;
    for round in 0..5 {
        let chain = chain(&format!("janitors-{round}"));
        stores[0]
            .append(append(&chain, "out", "p", 1, 2))
            .await
            .unwrap();
        age(&raw, &schema, 8).await;
        let passes: Vec<_> = stores
            .iter()
            .flat_map(|store| [store.clone(), store.clone()])
            .map(|store| tokio::spawn(async move { store.run_janitor().await.unwrap() }))
            .collect();
        let mut deleted = 0;
        for pass in passes {
            deleted += pass.await.unwrap();
        }
        assert_eq!(deleted, 2, "round {round}");
    }
    let held: i64 = raw
        .query_one(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND objid::int = \
             hashtext($1)",
            &[&format!("{schema}:janitor")],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(held, 0, "a janitor lock stayed behind");
}

#[tokio::test]
async fn an_owner_delete_purges_every_row() {
    let Some(url) = url() else { return };
    let options = options(&url);
    let schema = options.schema.clone();
    let store = PostgresStore::connect(options).await.unwrap();
    let chain = chain("deleted");
    let scheduled = ChainId {
        owner_kind: StreamOwnerKind::WorkflowActivity as i32,
        activity_id: "1".to_string(),
        ..chain.clone()
    };
    store
        .append(append(&chain, "out", "p", 1, 3))
        .await
        .unwrap();
    store
        .append(append(&scheduled, "out", "p", 1, 2))
        .await
        .unwrap();
    store.hold_floor(&chain, "out", "a/1", "").await.unwrap();
    let deleted = store
        .delete_owner(DeleteOwnerRequest {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: "deleted".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(deleted.deleted, 4);
    let raw = raw(&url).await;
    for table in [
        "chains",
        "topics",
        "records",
        "producers",
        "floors",
        "purge",
    ] {
        let count: i64 = raw
            .query_one(&format!("SELECT count(*) FROM {schema}.{table}"), &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 0, "{table}");
    }
}

/// Holds the row lock of a topic in an open transaction of its own, so an append waits.
async fn lock_topic(url: &str, schema: &str) -> tokio_postgres::Client {
    let holder = raw(url).await;
    holder
        .batch_execute(&format!(
            "BEGIN; SELECT 1 FROM {schema}.topics WHERE topic = 'out' FOR UPDATE"
        ))
        .await
        .unwrap();
    holder
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backend_killed_during_an_append_leaves_the_outcome_unknown() {
    let Some(url) = url() else { return };
    let options = options(&url);
    let schema = options.schema.clone();
    let store = Arc::new(PostgresStore::connect(options).await.unwrap());
    let chain = chain("killed");
    store
        .append(append(&chain, "out", "p", 1, 1))
        .await
        .unwrap();
    let holder = lock_topic(&url, &schema).await;
    let appending = {
        let (store, chain) = (store.clone(), chain.clone());
        tokio::spawn(async move { store.append(append(&chain, "out", "p", 2, 1)).await })
    };
    let admin = raw(&url).await;
    let started = Instant::now();
    loop {
        let killed: i64 = admin
            .query_one(
                "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity \
                 WHERE application_name = 'temporal-streams' AND wait_event_type = 'Lock' \
                 AND query LIKE $1",
                &[&format!("%{schema}.append_v1%")],
            )
            .await
            .unwrap()
            .get(0);
        if killed > 0 {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "no append waited"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let error = appending.await.unwrap().unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::OutcomeUnknown, "{error}");
    holder.batch_execute("ROLLBACK").await.unwrap();
    // Lang retries with the same sequence. The first retry lands, and a second one repeats it.
    let retry = store
        .append(append(&chain, "out", "p", 2, 1))
        .await
        .unwrap();
    let repeat = store
        .append(append(&chain, "out", "p", 2, 1))
        .await
        .unwrap();
    assert_eq!(retry, repeat);
}

#[tokio::test]
async fn an_append_past_the_statement_timeout_leaves_the_outcome_unknown() {
    let Some(url) = url() else { return };
    let mut options = options(&url);
    options.response_timeout = Duration::from_millis(300);
    let schema = options.schema.clone();
    let store = PostgresStore::connect(options).await.unwrap();
    let chain = chain("slow");
    store
        .append(append(&chain, "out", "p", 1, 1))
        .await
        .unwrap();
    let holder = lock_topic(&url, &schema).await;
    let error = store
        .append(append(&chain, "out", "p", 2, 1))
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::OutcomeUnknown, "{error}");
    assert!(error.message.contains("57014"), "{error}");
    holder.batch_execute("ROLLBACK").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_waiting_read_wakes_on_another_process_append() {
    let Some(url) = url() else { return };
    let options = options(&url);
    let reader = Arc::new(PostgresStore::connect(options.clone()).await.unwrap());
    let writer = PostgresStore::connect(options).await.unwrap();
    let chain = chain("poll");
    let waiting = {
        let (reader, chain) = (reader.clone(), chain.clone());
        tokio::spawn(async move {
            let started = Instant::now();
            let page = reader
                .read(read(&chain, "out", "", Duration::from_secs(10)))
                .await
                .unwrap();
            (page, started.elapsed())
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    writer
        .append(append(&chain, "out", "p", 1, 1))
        .await
        .unwrap();
    let (page, waited) = waiting.await.unwrap();
    assert_eq!(page.records.len(), 1);
    // The append came at 300 ms, and the poll runs every 100 ms.
    assert!(waited < Duration::from_secs(2), "{waited:?}");
}

#[tokio::test]
async fn tls_with_require_reaches_a_server_with_tls() {
    let Ok(url) = std::env::var("STREAMS_POSTGRES_TLS_URL") else {
        eprintln!("set STREAMS_POSTGRES_TLS_URL to run the PostgreSQL TLS test");
        return;
    };
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut options = options(&url);
    options.url = format!("{url}?sslmode=require");
    let store = PostgresStore::connect(options.clone()).await.unwrap();
    let chain = chain("tls");
    store
        .append(append(&chain, "out", "p", 1, 2))
        .await
        .unwrap();
    let page = store
        .read(read(&chain, "out", "", Duration::ZERO))
        .await
        .unwrap();
    assert_eq!(page.records.len(), 2);
    let raw = raw(&url).await;
    let encrypted: bool = raw
        .query_one(
            "SELECT count(*) FILTER (WHERE s.ssl) > 0 FROM pg_stat_ssl s \
             JOIN pg_stat_activity a USING (pid) WHERE a.application_name = 'temporal-streams'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(encrypted);
    // The server's certificate is self-signed, so a check of its chain fails.
    options.url = format!("{url}?sslmode=verify-full");
    let error = PostgresStore::connect(options).await.unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Storage, "{error}");
}

/// Waits for one record on `chain` from `reader` in a task of its own, and answers how long the
/// wait took.
fn wait_for_one(reader: Arc<PostgresStore>, chain: &ChainId) -> tokio::task::JoinHandle<Duration> {
    let chain = chain.clone();
    tokio::spawn(async move {
        let started = Instant::now();
        let page = reader
            .read(read(&chain, "out", "", Duration::from_secs(10)))
            .await
            .unwrap();
        assert_eq!(page.records.len(), 1);
        started.elapsed()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn notify_reads_wakes_a_reader_in_another_process_at_once() {
    let Some(url) = url() else { return };
    let mut options = options(&url);
    options.notify_reads = true;
    // The poll can't be what wakes the reader.
    options.poll_interval = Duration::from_secs(3600);
    let reader = Arc::new(PostgresStore::connect(options.clone()).await.unwrap());
    let writer = PostgresStore::connect(options).await.unwrap();
    let chain = chain("notified");
    // The topic exists first, so the waiter knows the row a `NOTIFY` names.
    writer
        .append(append(&chain, "other", "p", 1, 1))
        .await
        .unwrap();
    writer
        .append(append(&chain, "out", "q", 1, 1))
        .await
        .unwrap();
    // The reader reads first, so its pool holds a connection before the timed part.
    let after = reader
        .read(read(&chain, "out", "", Duration::ZERO))
        .await
        .unwrap()
        .records[0]
        .position
        .clone();
    let waiting = {
        let (reader, chain, after) = (reader.clone(), chain.clone(), after.clone());
        tokio::spawn(async move {
            let page = reader
                .read(read(&chain, "out", &after, Duration::from_secs(10)))
                .await
                .unwrap();
            (page.records.len(), Instant::now())
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    let sent = Instant::now();
    writer
        .append(append(&chain, "out", "q", 2, 1))
        .await
        .unwrap();
    let (records, woke) = waiting.await.unwrap();
    assert_eq!(records, 1);
    let took = woke - sent;
    assert!(took < Duration::from_millis(50), "{took:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_listener_that_lost_its_connection_wakes_every_waiter() {
    let Some(url) = url() else { return };
    let mut options = options(&url);
    options.notify_reads = true;
    options.poll_interval = Duration::from_secs(3600);
    let schema = options.schema.clone();
    let reader = Arc::new(PostgresStore::connect(options.clone()).await.unwrap());
    let mut quiet = options.clone();
    quiet.notify_reads = false;
    let writer = PostgresStore::connect(quiet).await.unwrap();
    let chain = chain("reconnect");
    let waiting = wait_for_one(reader.clone(), &chain);
    tokio::time::sleep(Duration::from_millis(300)).await;
    // The writer sends no `NOTIFY` and the poll is an hour away, so only the listener's
    // reconnect can wake the waiter.
    let admin = raw(&url).await;
    let killed: i64 = admin
        .query_one(
            "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity \
             WHERE application_name = 'temporal-streams' AND query LIKE $1",
            &[&format!("LISTEN {schema}_%")],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(killed, 1);
    writer
        .append(append(&chain, "out", "p", 1, 1))
        .await
        .unwrap();
    let waited = waiting.await.unwrap();
    assert!(waited < Duration::from_secs(5), "{waited:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_topics_leave_the_janitor_room_for_the_others() {
    let Some(url) = url() else { return };
    let options = options(&url);
    let schema = options.schema.clone();
    let store = PostgresStore::connect(options).await.unwrap();
    let raw = raw(&url).await;
    // More held topics than one pass takes, then one plain topic.
    for topic in 0..600 {
        let chain = chain(&format!("held-{topic}"));
        store
            .append(append(&chain, "out", "p", 1, 1))
            .await
            .unwrap();
        store.hold_floor(&chain, "out", "r/1", "").await.unwrap();
    }
    let plain = chain("plain");
    store
        .append(append(&plain, "out", "p", 1, 1))
        .await
        .unwrap();
    age(&raw, &schema, 8).await;
    store.run_janitor().await.unwrap();
    store.run_janitor().await.unwrap();
    let left: i64 = raw
        .query_one(
            &format!(
                "SELECT count(*) FROM {schema}.records r JOIN {schema}.topics t \
                 ON t.topic_id = r.topic_id JOIN {schema}.chains c ON c.chain_id = t.chain_id \
                 WHERE c.owner_id = 'plain'"
            ),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(left, 0, "the plain topic was trimmed");
}

#[tokio::test]
async fn a_topic_refuses_a_floor_holder_past_the_cap() {
    let Some(url) = url() else { return };
    let store = PostgresStore::connect(options(&url)).await.unwrap();
    let chain = chain("holders");
    for holder in 0..temporalio_streams::MAX_FLOOR_HOLDERS {
        store
            .hold_floor(&chain, "out", &format!("r/{holder}"), "")
            .await
            .unwrap();
    }
    let error = store
        .hold_floor(&chain, "out", "one-more/1", "")
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Refused, "{error}");
    store.hold_floor(&chain, "out", "r/0", "").await.unwrap();
}

#[tokio::test]
async fn two_archive_advances_out_of_order_keep_the_higher() {
    let Some(url) = url() else { return };
    let mut options = options(&url);
    options.notification_log = true;
    options.archive_lease = Some(Duration::from_secs(60));
    let store = PostgresStore::connect(options).await.unwrap();
    let chain = chain("advances");
    let landed = store
        .append(append(&chain, "out", "p", 1, 2))
        .await
        .unwrap();
    store
        .advance_archive(&chain, "out", &landed.last_position)
        .await
        .unwrap();
    store
        .advance_archive(&chain, "out", &landed.first_position)
        .await
        .unwrap();
    assert_eq!(
        store.archived(&chain, "out").await.unwrap(),
        Some(landed.last_position)
    );
}

#[tokio::test]
async fn an_archive_advance_lets_the_janitor_trim_what_it_archived() {
    let Some(url) = url() else { return };
    let mut options = options(&url);
    options.notification_log = true;
    options.archive_lease = Some(Duration::from_secs(3600));
    let schema = options.schema.clone();
    let store = PostgresStore::connect(options).await.unwrap();
    let raw = raw(&url).await;
    let chain = chain("archived");
    let landed = store
        .append(append(&chain, "out", "p", 1, 5))
        .await
        .unwrap();
    let first: i64 = landed.first_position.parse().unwrap();
    let last: i64 = landed.last_position.parse().unwrap();
    let kept = |from: i64| (from..=last).collect::<Vec<_>>();
    store
        .advance_archive(&chain, "out", &(first + 1).to_string())
        .await
        .unwrap();
    age(&raw, &schema, 8).await;
    store.run_janitor().await.unwrap();
    let positions = |rows: Vec<(i64, _)>| rows.into_iter().map(|row| row.0).collect::<Vec<_>>();
    assert_eq!(positions(rows(&raw, &schema).await), kept(first + 1));
    // The hold's lease is an hour away, and the advance alone makes the topic due.
    store
        .advance_archive(&chain, "out", &(last - 1).to_string())
        .await
        .unwrap();
    store.run_janitor().await.unwrap();
    assert_eq!(positions(rows(&raw, &schema).await), kept(last - 1));
}

#[tokio::test]
async fn an_append_rings_while_a_driver_holds_the_partitions_entries() {
    let Some(url) = url() else { return };
    let mut options = options(&url);
    options.notification_log = true;
    let store = PostgresStore::connect(options).await.unwrap();
    let log = store.notification_log().unwrap();
    let chain = chain("rings");
    store
        .append(append(&chain, "out", "p", 1, 1))
        .await
        .unwrap();
    let rung = |doorbell| log.wait_doorbell(doorbell, "d", Duration::from_secs(2));
    let mut partition = None;
    for doorbell in 0..log.doorbells() {
        if let Some(&first) = rung(doorbell).await.unwrap().first() {
            partition = Some((doorbell, first));
        }
    }
    let (doorbell, partition) = partition.expect("the first append rang");
    let taken = log
        .take(partition, "d", Duration::from_secs(60), 10)
        .await
        .unwrap();
    assert_eq!(taken.len(), 1);
    // The driver drains and goes back to its doorbell before this append commits.
    store
        .append(append(&chain, "out", "p", 2, 1))
        .await
        .unwrap();
    let ids: Vec<String> = taken.iter().map(|entry| entry.id.clone()).collect();
    log.ack(partition, &ids).await.unwrap();
    assert_eq!(rung(doorbell).await.unwrap(), [partition]);
}

#[tokio::test]
async fn the_log_cap_keeps_an_entry_a_driver_works_on() {
    let Some(url) = url() else { return };
    let mut options = options(&url);
    options.notification_log = true;
    let schema = options.schema.clone();
    let store = PostgresStore::connect(options).await.unwrap();
    let raw = raw(&url).await;
    let log = store.notification_log().unwrap();
    let chain = chain("cap");
    store
        .append(append(&chain, "out", "p", 1, 1))
        .await
        .unwrap();
    let partition: i16 = raw
        .query_one(
            &format!("SELECT partition FROM {schema}.notifications"),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let taken = log
        .take(partition as u32, "d", Duration::from_secs(60), 10)
        .await
        .unwrap();
    for sequence in 2..=3 {
        store
            .append(append(&chain, "out", "p", sequence, 1))
            .await
            .unwrap();
    }
    raw.execute(&format!("SELECT {schema}.cap_log_v1(1)"), &[])
        .await
        .unwrap();
    let left: Vec<i64> = raw
        .query(
            &format!("SELECT id FROM {schema}.notifications ORDER BY id"),
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(left.len(), 2, "{left:?}");
    assert_eq!(left[0].to_string(), taken[0].id);
}
