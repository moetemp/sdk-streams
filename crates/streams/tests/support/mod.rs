//! What the Redis tests share: one Store per server, a fresh Stream per case, and the Producers
//! of a Workflow run.

#![allow(dead_code)]

mod server;

use prost::Message;
use prost_types::Timestamp;
use redis::AsyncCommands;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use temporalio_streams::{
    ExtensionOptions, MemoryStreamExtension, StreamExtension, StreamResult,
    epoch::withdraws,
    ids::{StreamKeys, local_activity_producer_id, workflow_producer_id},
    memory::MemoryStore,
    proto::{
        Producer, ProducerEpoch, ProducerKind, RedisStreamConfig, StreamRecord, StreamRecordKind,
        StreamRef, Supersession, WorkflowTaskEpoch,
    },
    redis::{RedisStore, RedisStoreOptions},
    store::{Appended, Batch, Closed, Entry, Owner, Store, Withdrawal},
};

pub(crate) use server::server_url;

static NEXT: AtomicU64 = AtomicU64::new(0);

/// One Stream on one server, fresh for each case.
pub(crate) struct Target {
    pub(crate) store: Arc<dyn Store>,
    pub(crate) backend: Backend,
    pub(crate) namespace: String,
    pub(crate) stream: String,
}

/// The Store under a target, for the steps a test takes under the Store's calls.
pub(crate) enum Backend {
    Redis {
        store: Arc<RedisStore>,
        url: String,
        cluster: bool,
    },
    Memory(Arc<MemoryStore>),
}

fn unique() -> String {
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    format!(
        "{}-{n}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// A Store on each server that is set, standalone first, then the cluster, then the memory
/// Store.
pub(crate) async fn stores() -> Vec<Target> {
    stores_with(|options| options).await
}

/// Like [stores], with the options changed.
pub(crate) async fn stores_with(
    change: impl Fn(RedisStoreOptions) -> RedisStoreOptions,
) -> Vec<Target> {
    let mut targets = vec![];
    for (variable, cluster) in [
        ("STREAMS_REDIS_URL", false),
        ("STREAMS_REDIS_CLUSTER_URL", true),
    ] {
        let Some(url) = server_url(variable) else {
            continue;
        };
        let mut options = RedisStoreOptions::new(url.clone());
        options.cluster = cluster;
        options.key_prefix = "streams-test".to_string();
        let store = Arc::new(RedisStore::connect(change(options)).await.unwrap());
        let unique = unique();
        targets.push(Target {
            store: store.clone(),
            backend: Backend::Redis {
                store,
                url,
                cluster,
            },
            namespace: format!("ns-{unique}"),
            stream: format!("wf:{unique}"),
        });
    }
    // The memory Store runs the same cases, so both Stores keep one set of rules.
    let defaults = RedisStoreOptions::new(String::new());
    let memory = Arc::new(MemoryStore::new(defaults.retention, defaults.safety_ttl));
    let unique = unique();
    targets.push(Target {
        store: memory.clone(),
        backend: Backend::Memory(memory),
        namespace: format!("ns-{unique}"),
        stream: format!("wf:{unique}"),
    });
    targets
}

/// The Producers of one Workflow run.
pub(crate) struct Run {
    pub(crate) id: String,
}

fn at(seconds: i64) -> Option<Timestamp> {
    Some(Timestamp { seconds, nanos: 0 })
}

impl Run {
    pub(crate) fn new(id: &str) -> Self {
        Self { id: id.to_string() }
    }

    /// The run's Producer in the Workflow Task `[started]` at `attempt`.
    pub(crate) fn task(&self, started: i64, attempt: i32) -> Producer {
        self.task_at(started, attempt, 100)
    }

    pub(crate) fn task_at(&self, started: i64, attempt: i32, seconds: i64) -> Producer {
        Producer {
            kind: ProducerKind::Workflow as i32,
            id: workflow_producer_id(&self.id),
            epoch: Some(ProducerEpoch {
                task_started_event_id: started,
                task_attempt: attempt,
                task_started_time: at(seconds),
                ..Default::default()
            }),
            run_id: self.id.clone(),
        }
    }

    /// A completed Workflow Task, as the previous completed task.
    pub(crate) fn done(&self, started: i64) -> WorkflowTaskEpoch {
        self.done_at(started, 1)
    }

    pub(crate) fn done_at(&self, started: i64, attempt: i32) -> WorkflowTaskEpoch {
        WorkflowTaskEpoch {
            started_event_id: started,
            attempt,
            started_time: at(100),
        }
    }

    pub(crate) fn local_activity(
        &self,
        call: &str,
        started: i64,
        task_attempt: i32,
        attempt: i32,
    ) -> Producer {
        Producer {
            kind: ProducerKind::LocalActivity as i32,
            id: local_activity_producer_id(&self.id, call),
            epoch: Some(ProducerEpoch {
                task_started_event_id: started,
                task_attempt,
                task_started_time: at(100),
                attempt,
                ..Default::default()
            }),
            run_id: self.id.clone(),
        }
    }
}

pub(crate) fn record(producer: &Producer, text: &str) -> Vec<u8> {
    StreamRecord {
        kind: StreamRecordKind::Data as i32,
        topic: "t".to_string(),
        producer: Some(producer.clone()),
        body: Some(
            temporalio_common::protos::temporal::api::common::v1::Payload {
                data: text.as_bytes().to_vec(),
                ..Default::default()
            },
        ),
        ..Default::default()
    }
    .encode_to_vec()
}

pub(crate) fn supersession(producer: &Producer, withdrawal: &Withdrawal<'_>) -> Vec<u8> {
    Supersession {
        producer_kind: producer.kind,
        producer_id: producer.id.clone(),
        epoch: producer.epoch,
        previous_completed_task: withdrawal.previous_completed.copied(),
        failed_tasks: withdrawal.failed_tasks.to_vec(),
        withdraw_run: withdrawal.withdraw_run.to_string(),
        run_id: producer.run_id.clone(),
    }
    .encode_to_vec()
}

/// The data texts of the entries, in log order.
pub(crate) fn records_of(entries: &[Entry]) -> Vec<String> {
    entries
        .iter()
        .filter_map(|entry| entry.record.as_ref())
        .map(|bytes| StreamRecord::decode(bytes.as_slice()).unwrap())
        .filter(|record| record.kind == StreamRecordKind::Data as i32)
        .map(|record| String::from_utf8(record.body.unwrap().data).unwrap())
        .collect()
}

impl Target {
    pub(crate) fn stream_ref(&self) -> StreamRef {
        StreamRef {
            id: self.stream.clone(),
            ..Default::default()
        }
    }

    pub(crate) fn keys(&self) -> StreamKeys {
        StreamKeys::new("streams-test", &self.namespace, &self.stream)
    }

    pub(crate) async fn create(&self, owner: Owner<'_>) {
        self.store
            .create(
                &self.namespace,
                &self.stream_ref(),
                None,
                None,
                owner,
                &Withdrawal::default(),
            )
            .await
            .unwrap();
    }

    /// A plain connection to the node of the Stream's slot, to change the stored state under
    /// the scripts.
    async fn raw(&self, url: &str, cluster: bool) -> redis::aio::ConnectionManager {
        let url = if cluster {
            let client = redis::cluster::ClusterClient::new(vec![url]).unwrap();
            let mut conn = client.get_async_connection().await.unwrap();
            let slot: u16 = redis::cmd("CLUSTER")
                .arg("KEYSLOT")
                .arg(self.keys().meta())
                .query_async::<i64>(&mut conn)
                .await
                .unwrap() as u16;
            let shards: redis::Value = redis::cmd("CLUSTER")
                .arg("SLOTS")
                .query_async(&mut conn)
                .await
                .unwrap();
            node_of(&shards, slot).expect("a node serves the slot")
        } else {
            url.to_string()
        };
        redis::Client::open(url)
            .unwrap()
            .get_connection_manager()
            .await
            .unwrap()
    }

    /// Sets one meta field under the Store's calls.
    pub(crate) async fn set_meta(&self, field: &str, value: &str) {
        match &self.backend {
            Backend::Redis { url, cluster, .. } => {
                let mut conn = self.raw(url, *cluster).await;
                let _: () = conn.hset(self.keys().meta(), field, value).await.unwrap();
            }
            Backend::Memory(store) => {
                store.test_set_meta(&self.namespace, &self.stream, field, value)
            }
        }
    }

    /// One meta field.
    pub(crate) async fn meta(&self, field: &str) -> Option<String> {
        match &self.backend {
            Backend::Redis { url, cluster, .. } => {
                let mut conn = self.raw(url, *cluster).await;
                conn.hget(self.keys().meta(), field).await.unwrap()
            }
            Backend::Memory(store) => store.test_meta(&self.namespace, &self.stream, field),
        }
    }

    /// The ms left on the log and on the meta, as `PTTL` gives them.
    pub(crate) async fn ttls(&self) -> (i64, i64) {
        match &self.backend {
            Backend::Redis { url, cluster, .. } => {
                let mut conn = self.raw(url, *cluster).await;
                let log: i64 = conn.pttl(self.keys().log()).await.unwrap();
                let meta: i64 = conn.pttl(self.keys().meta()).await.unwrap();
                (log, meta)
            }
            Backend::Memory(store) => store.test_ttls(&self.namespace, &self.stream),
        }
    }

    /// Adds a log entry with an explicit id, as `XADD <id>`.
    pub(crate) async fn add_raw_entry(&self, id: &str) {
        match &self.backend {
            Backend::Redis { url, cluster, .. } => {
                let mut conn = self.raw(url, *cluster).await;
                let _: String = redis::cmd("XADD")
                    .arg(self.keys().log())
                    .arg(id)
                    .arg("x")
                    .arg("y")
                    .query_async(&mut conn)
                    .await
                    .unwrap();
            }
            Backend::Memory(store) => store.test_add_entry(&self.namespace, &self.stream, id),
        }
    }

    /// The number of log entries.
    pub(crate) async fn log_len(&self) -> usize {
        match &self.backend {
            Backend::Redis { url, cluster, .. } => {
                let mut conn = self.raw(url, *cluster).await;
                conn.xlen(self.keys().log()).await.unwrap()
            }
            Backend::Memory(store) => store.test_log_len(&self.namespace, &self.stream),
        }
    }

    /// Every entry of the log.
    pub(crate) async fn entries(&self) -> Vec<Entry> {
        let (_, entries) = self
            .store
            .read(&self.namespace, &self.stream, 0, 10_000)
            .await
            .unwrap();
        entries
    }

    /// The data texts in the log, withdrawn or not.
    pub(crate) async fn texts(&self) -> Vec<String> {
        records_of(&self.entries().await)
    }

    /// The data texts a Reader keeps after it applies each `Superseded` item.
    pub(crate) async fn visible(&self) -> Vec<String> {
        let mut kept: Vec<StreamRecord> = vec![];
        for entry in self.entries().await {
            if let Some(bytes) = &entry.supersession {
                let s = Supersession::decode(bytes.as_slice()).unwrap();
                kept.retain(|old| !withdraws(&s, old));
            }
            if let Some(bytes) = &entry.record {
                let record = StreamRecord::decode(bytes.as_slice()).unwrap();
                if record.kind == StreamRecordKind::Data as i32 {
                    kept.push(record);
                }
            }
        }
        kept.into_iter()
            .map(|record| String::from_utf8(record.body.unwrap().data).unwrap())
            .collect()
    }

    pub(crate) async fn batch(
        &self,
        producer: &Producer,
        sequence: i64,
        texts: &[&str],
    ) -> StreamResult<Appended> {
        self.write(producer, sequence, None, &[], "", texts).await
    }

    /// A Workflow Task's flush to the Stream, as its first batch.
    pub(crate) async fn flush(
        &self,
        task: &Producer,
        completed: Option<WorkflowTaskEpoch>,
        failed: &[i64],
        texts: &[&str],
    ) -> StreamResult<Appended> {
        self.write(task, 1, completed.as_ref(), failed, "", texts)
            .await
    }

    /// A retry run's flush that withdraws `previous`.
    pub(crate) async fn flush_retry(
        &self,
        task: &Producer,
        completed: Option<WorkflowTaskEpoch>,
        previous: &str,
        texts: &[&str],
    ) -> StreamResult<Appended> {
        self.write(task, 1, completed.as_ref(), &[], previous, texts)
            .await
    }

    /// A Workflow batch whose task facts come later, as a Local Activity flush writes it.
    pub(crate) async fn flush_deferred(
        &self,
        task: &Producer,
        sequence: i64,
        texts: &[&str],
    ) -> StreamResult<Appended> {
        self.write_with(task, sequence, None, &[], "", texts, true)
            .await
    }

    /// A later batch of a task, with its facts.
    pub(crate) async fn flush_next(
        &self,
        task: &Producer,
        sequence: i64,
        completed: Option<WorkflowTaskEpoch>,
        failed: &[i64],
        texts: &[&str],
    ) -> StreamResult<Appended> {
        self.write(task, sequence, completed.as_ref(), failed, "", texts)
            .await
    }

    async fn write(
        &self,
        producer: &Producer,
        sequence: i64,
        completed: Option<&WorkflowTaskEpoch>,
        failed: &[i64],
        withdraw_run: &str,
        texts: &[&str],
    ) -> StreamResult<Appended> {
        self.write_with(
            producer,
            sequence,
            completed,
            failed,
            withdraw_run,
            texts,
            false,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_with(
        &self,
        producer: &Producer,
        sequence: i64,
        completed: Option<&WorkflowTaskEpoch>,
        failed: &[i64],
        withdraw_run: &str,
        texts: &[&str],
        defer: bool,
    ) -> StreamResult<Appended> {
        let records: Vec<_> = texts.iter().map(|text| record(producer, text)).collect();
        let mut withdrawal = Withdrawal {
            previous_completed: completed,
            failed_tasks: failed,
            withdraw_run,
            supersession: &[],
            defer,
        };
        let bytes = supersession(producer, &withdrawal);
        withdrawal.supersession = &bytes;
        self.store
            .append(
                &self.namespace,
                &self.stream_ref(),
                &Batch {
                    producer,
                    sequence,
                    records: &records,
                    withdrawal,
                },
            )
            .await
    }

    pub(crate) async fn close(&self, producer: &Producer) -> StreamResult<Closed> {
        self.close_with(producer, &[]).await
    }

    /// A Workflow's close in task `task`, with the failed tasks.
    pub(crate) async fn close_run(&self, task: &Producer, failed: &[i64]) -> StreamResult<Closed> {
        self.close_with(task, failed).await
    }

    async fn close_with(&self, producer: &Producer, failed: &[i64]) -> StreamResult<Closed> {
        let record = StreamRecord {
            kind: StreamRecordKind::Close as i32,
            producer: Some(producer.clone()),
            ..Default::default()
        }
        .encode_to_vec();
        let mut withdrawal = Withdrawal {
            failed_tasks: failed,
            ..Default::default()
        };
        let bytes = supersession(producer, &withdrawal);
        withdrawal.supersession = &bytes;
        self.store
            .close(
                &self.namespace,
                &self.stream_ref(),
                producer,
                &record,
                &withdrawal,
            )
            .await
    }
}

fn node_of(slots: &redis::Value, slot: u16) -> Option<String> {
    let redis::Value::Array(ranges) = slots else {
        return None;
    };
    for range in ranges {
        let redis::Value::Array(parts) = range else {
            continue;
        };
        let number = |value: &redis::Value| match value {
            redis::Value::Int(n) => Some(*n),
            _ => None,
        };
        let (Some(start), Some(end)) = (number(&parts[0]), number(&parts[1])) else {
            continue;
        };
        if (start..=end).contains(&(slot as i64)) {
            let redis::Value::Array(node) = &parts[2] else {
                continue;
            };
            let host = match &node[0] {
                redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok()?,
                _ => return None,
            };
            let port = number(&node[1])?;
            let host = if host.is_empty() {
                "127.0.0.1".to_string()
            } else {
                host
            };
            return Some(format!("redis://{host}:{port}/0"));
        }
    }
    None
}

/// An extension on each Redis server that is set, with the config changed by `change`, and on
/// the memory Store when `memory` is true.
pub(crate) fn extensions(
    change: impl Fn(&mut RedisStreamConfig),
    memory: bool,
) -> Vec<Arc<StreamExtension>> {
    let mut extensions = vec![];
    for (variable, cluster) in [
        ("STREAMS_REDIS_URL", false),
        ("STREAMS_REDIS_CLUSTER_URL", true),
    ] {
        let Some(url) = server_url(variable) else {
            continue;
        };
        let mut config = RedisStreamConfig {
            urls: vec![url],
            cluster,
            key_prefix: "streams-test".to_string(),
            ..Default::default()
        };
        change(&mut config);
        let options = ExtensionOptions::from_config(config).unwrap();
        extensions.push(StreamExtension::new(options, None));
    }
    if memory {
        let defaults = RedisStoreOptions::new(String::new());
        extensions.push(MemoryStreamExtension::create(
            defaults.retention,
            defaults.safety_ttl,
            None,
        ));
    }
    extensions
}
