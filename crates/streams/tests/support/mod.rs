//! What the Redis tests share: one Store per server, a fresh Stream per case, and the Producers
//! of a Workflow run.

#![allow(dead_code)]

mod server;

use prost::Message;
use prost_types::Timestamp;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use temporalio_streams::{
    StreamResult,
    epoch::withdraws,
    ids::{StreamKeys, local_activity_producer_id, workflow_producer_id},
    proto::{
        Producer, ProducerEpoch, ProducerKind, StreamRecord, StreamRecordKind, Supersession,
        WorkflowTaskEpoch,
    },
    redis::{Appended, Batch, Closed, Entry, Owner, RedisStore, RedisStoreOptions, Withdrawal},
};

pub(crate) use server::server_url;

static NEXT: AtomicU64 = AtomicU64::new(0);

/// One Stream on one server, fresh for each case.
pub(crate) struct Target {
    pub(crate) store: Arc<RedisStore>,
    pub(crate) url: String,
    pub(crate) cluster: bool,
    pub(crate) namespace: String,
    pub(crate) stream: String,
}

/// A Store on each server that is set: standalone first, then the cluster.
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
        let store = RedisStore::connect(change(options)).await.unwrap();
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let unique = format!(
            "{}-{n}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        targets.push(Target {
            store: Arc::new(store),
            url,
            cluster,
            namespace: format!("ns-{unique}"),
            stream: format!("wf:{unique}"),
        });
    }
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
    pub(crate) fn keys(&self) -> StreamKeys {
        StreamKeys::new("streams-test", &self.namespace, &self.stream)
    }

    pub(crate) async fn create(&self, owner: Owner) {
        self.store
            .create(
                &self.namespace,
                &self.stream,
                None,
                owner,
                &Withdrawal::default(),
            )
            .await
            .unwrap();
    }

    /// A plain connection, to change the stored state under the scripts.
    pub(crate) async fn raw(&self) -> redis::aio::ConnectionManager {
        // Every key of a Stream is in one slot, so a connection to the slot's node is enough.
        let url = if self.cluster {
            let client = redis::cluster::ClusterClient::new(vec![self.url.as_str()]).unwrap();
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
            self.url.clone()
        };
        redis::Client::open(url)
            .unwrap()
            .get_connection_manager()
            .await
            .unwrap()
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

    async fn write(
        &self,
        producer: &Producer,
        sequence: i64,
        completed: Option<&WorkflowTaskEpoch>,
        failed: &[i64],
        withdraw_run: &str,
        texts: &[&str],
    ) -> StreamResult<Appended> {
        let records: Vec<_> = texts.iter().map(|text| record(producer, text)).collect();
        let mut withdrawal = Withdrawal {
            previous_completed: completed,
            failed_tasks: failed,
            withdraw_run,
            supersession: &[],
        };
        let bytes = supersession(producer, &withdrawal);
        withdrawal.supersession = &bytes;
        self.store
            .append(
                &self.namespace,
                &self.stream,
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
                &self.stream,
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
