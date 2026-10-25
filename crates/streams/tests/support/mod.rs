//! The helpers that the Store tests share. They give one Store per server, a new Stream per case,
//! and the Producers of a Workflow run.

#![allow(dead_code)]

use prost::Message;
use prost_types::Timestamp;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use temporalio_common::protos::temporal::api::{common::v1::Execution, enums::v1::ExecutionType};
use temporalio_streams::{
    StreamResult,
    digest::batch_digest,
    epoch::withdraws,
    ids::local_activity_producer_id,
    memory::MemoryStore,
    proto::{
        AppendRecord, Producer, ProducerEpoch, StreamProducerKind, StreamRecord, StreamRecordKind,
        StreamRef, Supersession,
    },
    store::{Appended, Batch, Closed, Entry, Owner, Store, Withdrawal},
};

static NEXT: AtomicU64 = AtomicU64::new(0);

/// What a case needs beyond the Store's calls on its own Stream. A case runs on every backend that
/// has all that it needs. Each case's comment says its guarantee and its needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Needs(u8);

impl Needs {
    /// Only the Store's calls on the Stream and reads of its Records.
    pub(crate) const CALLS: Self = Self(0);
    /// The Store's own results beyond the API. These are a conflict's stored Retention, `resend`,
    /// `flagged`, and a close's offset and `already`.
    pub(crate) const STORE_ANSWERS: Self = Self(1);
    /// The keys under the calls. These are the meta fields, TTLs and raw log entries, and the
    /// Store's log calls `read`, `read_back` and `wait_after`.
    pub(crate) const RAW_KEYS: Self = Self(1 << 1);
    /// Producers, owners and runs as sent. The server checks them against its executions, so a
    /// case with made-up runs, or an Activity with no task token, can't run there.
    pub(crate) const UNCHECKED: Self = Self(1 << 2);
    /// A Retention of a few hundred ms.
    pub(crate) const SHORT_TIMES: Self = Self(1 << 3);
    /// The runtime built from the crate's own config, such as a backend name.
    pub(crate) const OWN_CONFIG: Self = Self(1 << 4);
    /// The Store keys on the id only, so the owner of a ref doesn't route a call.
    pub(crate) const ID_KEYS: Self = Self(1 << 5);
    /// Every need. Redis and memory have all of them.
    pub(crate) const ALL: Self = Self(u8::MAX);

    /// Both needs.
    pub(crate) const fn and(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Whether this set gives every need of `needs`.
    pub(crate) const fn gives(self, needs: Self) -> bool {
        self.0 & needs.0 == needs.0
    }
}

/// One Stream on one server, fresh for each case.
pub(crate) struct Target {
    pub(crate) store: Arc<dyn Store>,
    pub(crate) backend: Backend,
    pub(crate) namespace: String,
    pub(crate) stream: String,
    /// What the backend gives. A case checks it before a step that needs more than it asked for.
    pub(crate) gives: Needs,
}

/// The Store under a target, for the steps a test takes under the Store's calls.
pub(crate) enum Backend {
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

/// The Stores of every backend that gives `needs`.
pub(crate) async fn stores_for(needs: Needs) -> Vec<Target> {
    let mut targets = vec![];
    // The memory Store runs the same cases, so both Stores keep one set of rules.
    let memory = Arc::new(MemoryStore::new(
        Duration::from_secs(7 * 24 * 60 * 60),
        Duration::from_secs(90 * 24 * 60 * 60),
    ));
    let unique = unique();
    targets.push(Target {
        store: memory.clone(),
        backend: Backend::Memory(memory),
        namespace: format!("ns-{unique}"),
        stream: format!("wf:{unique}"),
        gives: Needs::ALL,
    });
    targets.retain(|target| target.gives.gives(needs));
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

    pub(crate) fn local_activity(
        &self,
        call: &str,
        started: i64,
        task_attempt: i32,
        attempt: i32,
    ) -> Producer {
        Producer {
            kind: StreamProducerKind::LocalActivity as i32,
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

/// The owner of a ref that a Workflow's run chain makes, with the chain's first run id.
pub(crate) fn workflow_execution(workflow_id: &str, first_run_id: &str) -> Option<Execution> {
    Some(Execution {
        r#type: ExecutionType::Workflow as i32,
        business_id: workflow_id.to_string(),
        run_id: first_run_id.to_string(),
    })
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

/// The digest of the batch that [record] makes of each text.
pub(crate) fn digest(texts: &[&str]) -> String {
    let records: Vec<_> = texts
        .iter()
        .map(|text| AppendRecord {
            topic: "t".to_string(),
            body: Some(
                temporalio_common::protos::temporal::api::common::v1::Payload {
                    data: text.as_bytes().to_vec(),
                    ..Default::default()
                },
            ),
        })
        .collect();
    batch_digest(&records)
}

pub(crate) fn supersession(producer: &Producer, withdrawal: &Withdrawal<'_>) -> Vec<u8> {
    Supersession {
        producer_kind: producer.kind,
        producer_id: producer.id.clone(),
        epoch: producer.epoch,
        withdraw_run: withdrawal.withdraw_run.to_string(),
        run_id: producer.run_id.clone(),
        ..Default::default()
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

    pub(crate) async fn create(&self, owner: Owner) {
        self.store
            .create(
                &self.namespace,
                &self.stream_ref(),
                None,
                owner,
                &Withdrawal::default(),
            )
            .await
            .unwrap();
    }

    /// Sets one meta field under the Store's calls.
    pub(crate) async fn set_meta(&self, field: &str, value: &str) {
        match &self.backend {
            Backend::Memory(store) => {
                store.test_set_meta(&self.namespace, &self.stream, field, value)
            }
        }
    }

    /// One meta field.
    pub(crate) async fn meta(&self, field: &str) -> Option<String> {
        match &self.backend {
            Backend::Memory(store) => store.test_meta(&self.namespace, &self.stream, field),
        }
    }

    /// The ms left on the log and on the meta, as `PTTL` gives them.
    pub(crate) async fn ttls(&self) -> (i64, i64) {
        match &self.backend {
            Backend::Memory(store) => store.test_ttls(&self.namespace, &self.stream),
        }
    }

    /// Adds a log entry with an explicit id, as `XADD <id>`.
    pub(crate) async fn add_raw_entry(&self, id: &str) {
        match &self.backend {
            Backend::Memory(store) => store.test_add_entry(&self.namespace, &self.stream, id),
        }
    }

    /// The number of log entries.
    pub(crate) async fn log_len(&self) -> usize {
        match &self.backend {
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
        self.write(producer, sequence, "", texts).await
    }

    async fn write(
        &self,
        producer: &Producer,
        sequence: i64,
        withdraw_run: &str,
        texts: &[&str],
    ) -> StreamResult<Appended> {
        let records: Vec<_> = texts.iter().map(|text| record(producer, text)).collect();
        let mut withdrawal = Withdrawal {
            withdraw_run,
            supersession: &[],
            tasks: None,
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
                    digest: &digest(texts),
                    withdrawal,
                },
            )
            .await
    }

    pub(crate) async fn close(&self, producer: &Producer) -> StreamResult<Closed> {
        self.close_at(&self.stream_ref(), producer).await
    }

    /// Closes the Stream that `stream` names, with the owner fields it has.
    pub(crate) async fn close_at(
        &self,
        stream: &StreamRef,
        producer: &Producer,
    ) -> StreamResult<Closed> {
        let record = StreamRecord {
            kind: StreamRecordKind::Close as i32,
            producer: Some(producer.clone()),
            ..Default::default()
        }
        .encode_to_vec();
        self.store
            .close(&self.namespace, stream, producer, &record)
            .await
    }
}
