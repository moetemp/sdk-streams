//! The stream driver's archive sink: it copies the topics a store marks into an archive, so a
//! `TieredStore` reads them after the store drops them.
//!
//! The memory cases always run. Set `STREAMS_REDIS_URL` for the Redis case,
//! `STREAMS_POSTGRES_URL` for the PostgreSQL case, and `STREAMS_S3_URL` with the `AWS_*` variables
//! for the S3 case.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_streams::{
    ArchiveSink, LocalFileArchive, MemoryStore, NotifierClient, NotifyError, SegmentLimits,
    StreamArchive, StreamDriver, StreamDriverOptions, StreamNotification, StreamStore, TieredStore,
    proto::{ChainId, StoreAppendRequest, StoreReadRequest, StoredClose},
};

static IDS: AtomicUsize = AtomicUsize::new(0);

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        IDS.fetch_add(1, Ordering::Relaxed)
    )
}

/// Takes every notification and sends nothing.
struct Quiet;

#[async_trait::async_trait]
impl NotifierClient for Quiet {
    async fn notify(&self, _: StreamNotification) -> Result<(), NotifyError> {
        Ok(())
    }

    async fn notify_streams(&self, _: Vec<StreamNotification>) -> Result<(), NotifyError> {
        Ok(())
    }
}

fn chain() -> ChainId {
    ChainId {
        namespace: "ns".to_string(),
        workflow_id: unique("wf"),
        first_run_id: "run-1".to_string(),
        ..Default::default()
    }
}

fn append(chain: &ChainId, topic: &str, sequence: i64, values: &[&str]) -> StoreAppendRequest {
    StoreAppendRequest {
        chain: Some(chain.clone()),
        topic: topic.to_string(),
        producer_id: "p".to_string(),
        attempt: 1,
        sequence,
        digest: vec![sequence as u8],
        records: values
            .iter()
            .map(|value| value.as_bytes().to_vec())
            .collect(),
    }
}

/// Runs a driver that archives `hot` into `archive` until the returned task is aborted.
fn sink(
    hot: Arc<dyn StreamStore>,
    archive: Arc<dyn StreamArchive>,
    segment_idle: Duration,
) -> tokio::task::JoinHandle<()> {
    let driver = StreamDriver::new(
        hot.clone(),
        Arc::new(Quiet),
        StreamDriverOptions {
            consumer: unique("archiver"),
            sweep_interval: Duration::from_millis(100),
            ..Default::default()
        },
    )
    .unwrap()
    .with_archive(ArchiveSink {
        hot,
        archive,
        limits: SegmentLimits {
            records: 2,
            ..Default::default()
        },
        segment_idle,
    });
    tokio::spawn(async move {
        let _ = Arc::new(driver).run().await;
    })
}

async fn archived_to(hot: &dyn StreamStore, chain: &ChainId, topic: &str, position: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while hot.archived(chain, topic).await.unwrap().as_deref() != Some(position) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the driver archives the topic");
}

/// Every record of a topic through `store`, page by page, and whether the last page was closed.
async fn read_all(store: &dyn StreamStore, chain: &ChainId, topic: &str) -> (Vec<String>, bool) {
    let mut values = vec![];
    let mut after = String::new();
    loop {
        let page = store
            .read(StoreReadRequest {
                chain: Some(chain.clone()),
                topic: topic.to_string(),
                after_position: after.clone(),
                max_records: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        if page.records.is_empty() {
            return (values, page.closed);
        }
        for record in page.records {
            values.push(String::from_utf8(record.record).unwrap());
            after = record.position;
        }
    }
}

/// Writes a closed topic and an open one, lets the driver archive both, drops them from the hot
/// store with `forget`, and reads them back through a tiered store.
async fn the_driver_archives_closed_and_busy_topics(
    hot: Arc<dyn StreamStore>,
    archive: Arc<dyn StreamArchive>,
    forget: impl AsyncFn(&ChainId, &str),
) {
    let tiered = TieredStore::new(hot.clone(), archive.clone());
    let chain = chain();
    hot.append(append(&chain, "closed", 1, &["1", "2", "3"]))
        .await
        .unwrap();
    let last = hot
        .append(append(&chain, "closed", 4, &["4", "5"]))
        .await
        .unwrap();
    hot.close_topic_with(
        &chain,
        "closed",
        StoredClose {
            closed_by: "run-1".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let open = hot
        .append(append(&chain, "open", 1, &["a", "b", "c"]))
        .await
        .unwrap();
    // The open topic is not idle yet, and its tail is still taken once, at the first sweep.
    let running = sink(hot.clone(), archive, Duration::from_secs(60));
    archived_to(&*hot, &chain, "closed", &last.last_position).await;
    archived_to(&*hot, &chain, "open", &open.last_position).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while !hot
            .notification_log()
            .unwrap()
            .archive_marks(10)
            .await
            .unwrap()
            .is_empty()
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the archived topics leave the marks");
    running.abort();
    forget(&chain, "closed").await;
    forget(&chain, "open").await;
    assert_eq!(
        read_all(&tiered, &chain, "closed").await,
        (["1", "2", "3", "4", "5"].map(str::to_string).to_vec(), true)
    );
    assert_eq!(
        read_all(&tiered, &chain, "open").await,
        (["a", "b", "c"].map(str::to_string).to_vec(), false)
    );
}

#[tokio::test]
async fn the_driver_archives_a_memory_store_into_a_directory() {
    let store = Arc::new(MemoryStore::with_notification_log().holding_for_archive());
    let root = std::env::temp_dir().join(unique("streams-archive-sink"));
    let archive = Arc::new(LocalFileArchive::new(&root));
    let forgets = store.clone();
    the_driver_archives_closed_and_busy_topics(store, archive, async |chain, topic| {
        forgets.forget(chain, topic)
    })
    .await;
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "archive")]
#[tokio::test]
async fn the_driver_archives_a_memory_store_into_s3() {
    let Ok(url) = std::env::var("STREAMS_S3_URL") else {
        eprintln!("set STREAMS_S3_URL to run the S3 archive cases");
        return;
    };
    let store = Arc::new(MemoryStore::with_notification_log().holding_for_archive());
    let archive = Arc::new(
        temporalio_streams::ObjectStoreArchive::from_s3_url(&format!(
            "{}/{}",
            url.trim_end_matches('/'),
            unique("sink")
        ))
        .unwrap(),
    );
    let forgets = store.clone();
    the_driver_archives_closed_and_busy_topics(store, archive, async |chain, topic| {
        forgets.forget(chain, topic)
    })
    .await;
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn the_driver_archives_a_redis_store_into_a_directory() {
    let Ok(url) = std::env::var("STREAMS_REDIS_URL") else {
        eprintln!("set STREAMS_REDIS_URL to run the Redis archive cases");
        return;
    };
    let prefix = unique("archive-sink");
    let mut options = temporalio_streams::RedisStoreOptions::new(url.clone());
    options.key_prefix = prefix.clone();
    options.notification_log = true;
    options.archive_lease = Some(Duration::from_secs(60));
    let store = Arc::new(
        temporalio_streams::RedisStore::connect(options)
            .await
            .unwrap(),
    );
    let root = std::env::temp_dir().join(unique("streams-archive-sink"));
    let archive = Arc::new(LocalFileArchive::new(&root));
    let raw = redis::Client::open(url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    the_driver_archives_closed_and_busy_topics(store, archive, async |chain, topic| {
        let log = format!(
            "{prefix}:{{{}:{}:{}}}:t:{topic}",
            chain.namespace, chain.workflow_id, chain.first_run_id
        );
        let _: i64 = redis::cmd("DEL")
            .arg(&log)
            .arg(format!("{log}:meta"))
            .arg(format!("{log}:floors"))
            .query_async(&mut raw.clone())
            .await
            .unwrap();
    })
    .await;
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn the_driver_archives_a_postgres_store_into_a_directory() {
    let Ok(url) = std::env::var("STREAMS_POSTGRES_URL") else {
        eprintln!("set STREAMS_POSTGRES_URL to run the PostgreSQL archive cases");
        return;
    };
    let schema = unique("archive_sink").replace('-', "_");
    let mut options = temporalio_streams::PostgresStoreOptions::new(url.clone());
    options.schema = schema.clone();
    options.max_connections = 4;
    options.notification_log = true;
    options.archive_lease = Some(Duration::from_secs(60));
    let store = Arc::new(
        temporalio_streams::PostgresStore::connect(options)
            .await
            .unwrap(),
    );
    let root = std::env::temp_dir().join(unique("streams-archive-sink"));
    let archive = Arc::new(LocalFileArchive::new(&root));
    let (raw, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    let raw = Arc::new(raw);
    the_driver_archives_closed_and_busy_topics(store, archive, async |chain, topic| {
        // The topic row goes with its records, as when the janitor deleted a tombstone.
        raw.execute(
            &format!(
                "WITH t AS (SELECT t.topic_id FROM {schema}.chains c \
                   JOIN {schema}.topics t ON t.chain_id = c.chain_id AND t.topic = $3 \
                   WHERE c.namespace = $1 AND c.owner_id = $2), \
                 gone AS (DELETE FROM {schema}.records r USING t WHERE r.topic_id = t.topic_id) \
                 DELETE FROM {schema}.topics x USING t WHERE x.topic_id = t.topic_id"
            ),
            &[&chain.namespace, &chain.workflow_id, &topic],
        )
        .await
        .unwrap();
    })
    .await;
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn a_redis_mark_is_aged_on_the_store_clock() {
    let Ok(url) = std::env::var("STREAMS_REDIS_URL") else {
        eprintln!("set STREAMS_REDIS_URL to run the Redis archive cases");
        return;
    };
    let mut options = temporalio_streams::RedisStoreOptions::new(url);
    options.key_prefix = unique("archive-age");
    options.notification_log = true;
    options.archive_lease = Some(Duration::from_secs(60));
    let store = temporalio_streams::RedisStore::connect(options)
        .await
        .unwrap();
    let chain = chain();
    store
        .append(append(&chain, "out", 1, &["1"]))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let marks = store
        .notification_log()
        .unwrap()
        .archive_marks(1000)
        .await
        .unwrap();
    let mark = marks.iter().find(|mark| mark.chain == chain).unwrap();
    assert!((250..5_000).contains(&mark.idle_ms), "{mark:?}");
}
