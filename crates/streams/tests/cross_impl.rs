//! The Core store and the Python SDK's Redis provider on one Redis, each reading and writing
//! what the other wrote.
//!
//! Set `STREAMS_REDIS_URL`, and `STREAMS_BROOK_PY` to a Python SDK checkout whose Redis provider
//! is the one before the store moved into Core. The Python side runs `tests/cross_impl/brook.py`
//! through `uv`.

#![cfg(feature = "redis")]

use prost::Message;
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    OwnerClient, OwnerDescription, OwnerError, OwnerId, OwnerStatus, ReadTarget, RedisStore,
    RedisStoreOptions, StreamStore, Streams, StreamsOptions, mint_cursor,
    proto::{
        AppendRecord, AppendRequest, ChainId, NamedProducer, ReadRequest, StoreAppendRequest,
        StreamAddress, StreamFailureKind, StreamOwnerKind, StreamRecordKind,
        append_request::Producer, read_record,
    },
    read_page, stored_append_record, stream_hash,
};

static IDS: AtomicUsize = AtomicUsize::new(0);

const NAMESPACE: &str = "ns";
const FIRST_RUN: &str = "run-1";

struct Both {
    url: String,
    checkout: PathBuf,
    prefix: String,
    workflow_id: String,
    retention_ms: u64,
    core: RedisStore,
}

async fn pair(retention: Duration) -> Option<Both> {
    let (Ok(url), Ok(checkout)) = (
        std::env::var("STREAMS_REDIS_URL"),
        std::env::var("STREAMS_BROOK_PY"),
    ) else {
        eprintln!(
            "set STREAMS_REDIS_URL and STREAMS_BROOK_PY to run the cross-implementation test"
        );
        return None;
    };
    let id = IDS.fetch_add(1, Ordering::Relaxed);
    let prefix = format!("cross-{}-{id}", std::process::id());
    let mut options = RedisStoreOptions::new(url.clone());
    options.key_prefix = prefix.clone();
    options.retention = retention;
    Some(Both {
        core: RedisStore::connect(options).await.unwrap(),
        url,
        checkout: checkout.into(),
        prefix,
        workflow_id: format!("wf-{id}"),
        retention_ms: retention.as_millis() as u64,
    })
}

impl Both {
    /// Runs one operation of the Python provider and answers its JSON.
    fn brook(&self, op: Value) -> Value {
        let helper = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/cross_impl/brook.py");
        let mut op = op;
        let fields = op.as_object_mut().unwrap();
        fields.insert("url".into(), json!(self.url));
        fields.insert("prefix".into(), json!(self.prefix));
        fields.insert("namespace".into(), json!(NAMESPACE));
        fields.insert("workflow_id".into(), json!(self.workflow_id));
        fields.insert("first_run".into(), json!(FIRST_RUN));
        fields.insert("retention_ms".into(), json!(self.retention_ms));
        let output = Command::new("uv")
            .args(["run", "--no-sync", "python"])
            .arg(helper)
            .arg(op.to_string())
            .current_dir(&self.checkout)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        serde_json::from_str(stdout.lines().last().unwrap()).unwrap()
    }

    fn chain(&self) -> ChainId {
        ChainId {
            namespace: NAMESPACE.to_string(),
            workflow_id: self.workflow_id.clone(),
            first_run_id: FIRST_RUN.to_string(),
            ..Default::default()
        }
    }

    fn target(&self, topic: &str) -> ReadTarget {
        ReadTarget {
            chain: self.chain(),
            topic: topic.to_string(),
            stream_hash: stream_hash(NAMESPACE, "workflow", &self.workflow_id, topic),
        }
    }

    fn cursor(&self, topic: &str, position: &str) -> String {
        mint_cursor("redis", &self.target(topic).stream_hash, position)
    }

    /// Appends JSON values as producer `p` the way lang hands them to Core.
    async fn core_append(
        &self,
        sequence: i64,
        values: &[Value],
        digest: &[u8],
    ) -> (String, String) {
        let records: Vec<_> = values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                stored_append_record(
                    "events",
                    "p",
                    1,
                    sequence + index as i64,
                    &json_record(value),
                )
                .unwrap()
                .encode_to_vec()
            })
            .collect();
        let landed = self
            .core
            .append(StoreAppendRequest {
                chain: Some(self.chain()),
                topic: "events".to_string(),
                producer_id: "p".to_string(),
                attempt: 1,
                sequence,
                digest: digest.to_vec(),
                records,
            })
            .await
            .unwrap();
        (landed.first_position, landed.last_position)
    }

    /// Reads a topic from `after` with the Core reader, as JSON values.
    async fn core_read(&self, topic: &str, after: &str) -> Result<Vec<Value>, StreamFailureKind> {
        let page = read_page(
            &self.core,
            &self.target(topic),
            &ReadRequest {
                after: after.to_string(),
                ..Default::default()
            },
        )
        .await
        .map_err(|error| error.kind)?;
        Ok(page
            .records
            .iter()
            .filter_map(|record| match record.record.as_ref()? {
                read_record::Record::Stored(stored) => {
                    let body = stored.body.as_ref()?;
                    serde_json::from_slice(&body.data).ok()
                }
                read_record::Record::Superseded(_) => None,
            })
            .collect())
    }
}

fn json_record(value: &Value) -> AppendRecord {
    let payload = Payload {
        metadata: [("encoding".to_string(), b"json/plain".to_vec())].into(),
        data: value.to_string().into_bytes(),
        ..Default::default()
    };
    use sha2::{Digest, Sha256};
    AppendRecord {
        kind: StreamRecordKind::Data as i32,
        content_hash: Sha256::digest(payload.encode_to_vec()).to_vec(),
        body: Some(payload),
        ..Default::default()
    }
}

fn values(records: &Value) -> Vec<Value> {
    records
        .as_array()
        .unwrap_or_else(|| panic!("not records: {records}"))
        .iter()
        .map(|record| record["value"].clone())
        .collect()
}

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

const WEEK: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// Long enough that starting the Python side never outlasts it.
const SHORT: Duration = Duration::from_secs(2);

#[tokio::test]
async fn what_python_wrote_core_reads_and_resumes() {
    let Some(both) = pair(WEEK).await else {
        return;
    };
    let first =
        both.brook(json!({"op": "append", "producer": "p", "values": [{"n": 1}, {"n": 2}]}));
    both.brook(json!({"op": "append", "producer": "p", "sequence": 3, "values": [{"n": 3}]}));
    assert_eq!(
        both.core_read("events", "").await.unwrap(),
        [json!({"n": 1}), json!({"n": 2}), json!({"n": 3})]
    );
    // A cursor the Python provider minted resumes on the Core store.
    let cursor = first["cursor"].as_str().unwrap();
    assert_eq!(
        both.core_read("events", cursor).await.unwrap(),
        [json!({"n": 3})]
    );
    let page = read_page(&both.core, &both.target("events"), &ReadRequest::default())
        .await
        .unwrap();
    let Some(read_record::Record::Stored(stored)) = &page.records[2].record else {
        panic!("no record");
    };
    assert_eq!(
        (stored.producer_id.as_str(), stored.attempt, stored.sequence),
        ("p", 1, 3)
    );
}

#[tokio::test]
async fn what_core_wrote_python_reads_and_resumes() {
    let Some(both) = pair(WEEK).await else {
        return;
    };
    let (_, first) = both.core_append(1, &[json!({"n": 1})], b"\x01").await;
    both.core_append(2, &[json!({"n": 2})], b"\x02").await;
    let read = both.brook(json!({"op": "read", "count": 2}));
    assert_eq!(values(&read), [json!({"n": 1}), json!({"n": 2})]);
    assert_eq!(read[0]["producer_id"], "p");
    // A cursor the Core store minted resumes on the Python provider.
    let after =
        both.brook(json!({"op": "read", "count": 1, "after": both.cursor("events", &first)}));
    assert_eq!(values(&after), [json!({"n": 2})]);
}

/// Temporal as the stream layer asks it: one Workflow, running in its first run.
struct Running;

#[async_trait::async_trait]
impl OwnerClient for Running {
    async fn describe(
        &self,
        _namespace: &str,
        _owner: OwnerId<'_>,
        _run_id: &str,
    ) -> Result<OwnerDescription, OwnerError> {
        Ok(OwnerDescription {
            run_id: FIRST_RUN.to_string(),
            first_run_id: FIRST_RUN.to_string(),
            status: OwnerStatus::Running,
        })
    }
}

impl Both {
    /// The stream layer over a store on the same keys, as lang reaches it.
    async fn streams(&self) -> Streams {
        let mut options = RedisStoreOptions::new(self.url.clone());
        options.key_prefix = self.prefix.clone();
        let store = RedisStore::connect(options).await.unwrap();
        Streams::new(
            Arc::new(store),
            Arc::new(Running),
            StreamsOptions::default(),
        )
    }

    fn append_request(&self, values: &[Value], digest: Vec<u8>) -> AppendRequest {
        AppendRequest {
            stream: Some(StreamAddress {
                namespace: NAMESPACE.to_string(),
                owner_kind: StreamOwnerKind::Workflow as i32,
                workflow_id: self.workflow_id.clone(),
                run_id: String::new(),
                topic: "events".to_string(),
                ..Default::default()
            }),
            producer: Some(Producer::Named(NamedProducer {
                producer_id: "p".to_string(),
                attempt: 1,
            })),
            sequence: 1,
            records: values.iter().map(json_record).collect(),
            digest,
        }
    }
}

#[tokio::test]
async fn a_batch_python_wrote_dedupes_through_core_with_its_digest() {
    // Lang takes the digest and the store keeps it as given, so a retry that carries the same
    // digest is recognized whichever SDK wrote first.
    let Some(both) = pair(WEEK).await else {
        return;
    };
    let streams = both.streams().await;
    let landed = both.brook(json!({"op": "append", "producer": "p", "values": [{"n": 1}]}));
    let digest = hex(landed["digest"].as_str().unwrap());
    let retry = streams
        .append(both.append_request(&[json!({"n": 1})], digest))
        .await
        .unwrap();
    assert_eq!(retry.last_cursor, landed["cursor"].as_str().unwrap());
    let error = streams
        .append(both.append_request(&[json!({"n": 1})], vec![7; 32]))
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::ProducerDivergent);
    assert_eq!(both.core_read("events", "").await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_batch_core_wrote_dedupes_through_python() {
    let Some(both) = pair(WEEK).await else {
        return;
    };
    let streams = both.streams().await;
    let digest = both.brook(json!({"op": "digest", "producer": "p", "values": [{"n": 1}]}));
    let landed = streams
        .append(both.append_request(&[json!({"n": 1})], hex(digest.as_str().unwrap())))
        .await
        .unwrap();
    let retry = both.brook(json!({"op": "append", "producer": "p", "values": [{"n": 1}]}));
    assert_eq!(retry["cursor"].as_str().unwrap(), landed.last_cursor);
    assert_eq!(both.core_read("events", "").await.unwrap().len(), 1);
}

#[tokio::test]
async fn either_side_sees_a_chain_the_other_closed() {
    let Some(both) = pair(WEEK).await else {
        return;
    };
    both.core_append(1, &[json!({"n": 1})], b"\x01").await;
    both.core.close_chain(&both.chain()).await.unwrap();
    let refused = both.brook(json!({"op": "append", "producer": "q", "values": [{"n": 2}]}));
    assert_eq!(refused["error"], "StreamClosedError", "{refused}");

    let other = pair(WEEK).await.unwrap();
    other.brook(json!({"op": "close"}));
    let error = other
        .core
        .append(StoreAppendRequest {
            chain: Some(other.chain()),
            topic: "events".to_string(),
            producer_id: "p".to_string(),
            attempt: 1,
            sequence: 1,
            digest: b"\x01".to_vec(),
            records: vec![b"\x1a\x06events".to_vec()],
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Closed);
}

#[tokio::test]
async fn a_trim_core_made_expires_a_python_cursor() {
    let Some(both) = pair(SHORT).await else {
        return;
    };
    both.brook(json!({"op": "append", "producer": "p", "values": [{"n": 1}, {"n": 2}]}));
    let delivered = both.brook(json!({"op": "read", "count": 1}));
    tokio::time::sleep(SHORT + Duration::from_millis(200)).await;
    // This append trims both records, and the reader never got the second.
    both.core_append(3, &[json!({"n": 3})], b"\x03").await;
    let expired = both.brook(json!({
        "op": "read",
        "count": 1,
        "after": delivered[0]["cursor"],
    }));
    assert_eq!(expired["error"], "StreamExpiredError", "{expired}");
}

#[tokio::test]
async fn a_trim_python_made_expires_a_core_cursor() {
    let Some(both) = pair(SHORT).await else {
        return;
    };
    let (first, _) = both
        .core_append(1, &[json!({"n": 1}), json!({"n": 2})], b"\x01")
        .await;
    tokio::time::sleep(SHORT + Duration::from_millis(200)).await;
    both.brook(json!({"op": "append", "producer": "q", "values": [{"n": 3}]}));
    assert_eq!(
        both.core_read("events", &both.cursor("events", &first))
            .await,
        Err(StreamFailureKind::Expired)
    );
}
