//! The fallbacks under faults, through the stream layer, on stores with and without producer
//! state: lost answers, late copies, copies after a close, a crash between a topic name and its
//! record, readers that never write, where readers end when the owner ends, and reads that give
//! the same records however they page.

use prost::Message;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::protos::temporal::api::{
    common::v1::Payload, enums::v1::WorkflowExecutionStatus,
};
use temporalio_streams::{
    BEGINNING, CHAIN_LOG, Capabilities, MemoryStore, OwnerClient, OwnerDescription, OwnerError,
    ReadTarget, StreamError, StreamResult, StreamStore, Streams, StreamsOptions, close_chain,
    proto::*, read_page, stream_hash,
};

/// A store that forwards to another and injects the faults a test asks for. It also counts
/// writes, so a test can tell that a reader made none.
struct Faulty {
    inner: Arc<dyn StreamStore>,
    /// How many of the next appends land and then answer as if the answer was lost.
    lose: AtomicUsize,
    /// The next append of a topic other than the control log fails before it lands.
    fail_next_data: AtomicBool,
    /// The next append also keeps a copy to land later.
    keep_copy: AtomicBool,
    copies: Mutex<Vec<StoreAppendRequest>>,
    writes: AtomicUsize,
}

impl Faulty {
    fn new(inner: Arc<dyn StreamStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            lose: AtomicUsize::new(0),
            fail_next_data: AtomicBool::new(false),
            keep_copy: AtomicBool::new(false),
            copies: Mutex::default(),
            writes: AtomicUsize::new(0),
        })
    }

    /// Lands the copies kept so far, as a network does with a delayed request. A store with
    /// producer state refuses a copy of a batch older than the newest.
    async fn land_copies(&self) {
        let copies = std::mem::take(&mut *self.copies.lock().unwrap());
        for copy in copies {
            match self.inner.append(copy).await {
                Err(error) if self.inner.capabilities().producer_state => {
                    assert_eq!(error.kind, StreamFailureKind::ProducerStale);
                }
                landed => drop(landed.unwrap()),
            }
        }
    }

    fn wrote(&self) {
        self.writes.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait::async_trait]
impl StreamStore for Faulty {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn append(&self, request: StoreAppendRequest) -> StreamResult<StoreAppendResponse> {
        self.wrote();
        if request.topic != CHAIN_LOG && self.fail_next_data.swap(false, Ordering::Relaxed) {
            return Err(StreamError::storage("the Worker crashed before the append"));
        }
        if self.keep_copy.swap(false, Ordering::Relaxed) {
            self.copies.lock().unwrap().push(request.clone());
        }
        let landed = self.inner.append(request).await?;
        let lost = self
            .lose
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1));
        if lost.is_ok() {
            return Err(StreamError::outcome_unknown("the answer was lost"));
        }
        Ok(landed)
    }

    async fn read(&self, request: StoreReadRequest) -> StreamResult<StoreReadResponse> {
        self.inner.read(request).await
    }

    async fn latest(&self, request: StoreLatestRequest) -> StreamResult<StoreLatestResponse> {
        self.inner.latest(request).await
    }

    async fn record_at(
        &self,
        chain: &ChainId,
        topic: &str,
        position: &str,
    ) -> StreamResult<Option<Vec<u8>>> {
        self.inner.record_at(chain, topic, position).await
    }

    async fn newest_ever(&self, chain: &ChainId, topic: &str) -> StreamResult<String> {
        self.inner.newest_ever(chain, topic).await
    }

    async fn dropped_after(
        &self,
        chain: &ChainId,
        topic: &str,
        mark: &str,
    ) -> StreamResult<Option<String>> {
        self.inner.dropped_after(chain, topic, mark).await
    }

    async fn close_chain(&self, chain: &ChainId) -> StreamResult<()> {
        self.wrote();
        self.inner.close_chain(chain).await
    }

    async fn close_topic(&self, chain: &ChainId, topic: &str) -> StreamResult<()> {
        self.wrote();
        self.inner.close_topic(chain, topic).await
    }

    async fn retire_producer(
        &self,
        chain: &ChainId,
        topic: &str,
        producer_id: &str,
        attempt: i64,
    ) -> StreamResult<()> {
        self.wrote();
        self.inner
            .retire_producer(chain, topic, producer_id, attempt)
            .await
    }

    async fn list_topics(&self, chain: &ChainId) -> StreamResult<Vec<String>> {
        self.inner.list_topics(chain).await
    }

    async fn delete_owner(&self, request: DeleteOwnerRequest) -> StreamResult<DeleteOwnerResponse> {
        self.wrote();
        self.inner.delete_owner(request).await
    }
}

/// Temporal, with one Workflow whose only run is `run-1`.
struct Owner {
    status: Mutex<Result<WorkflowExecutionStatus, ()>>,
}

impl Owner {
    fn running() -> Arc<Self> {
        Arc::new(Self {
            status: Mutex::new(Ok(WorkflowExecutionStatus::Running)),
        })
    }

    fn end(&self, status: WorkflowExecutionStatus) {
        *self.status.lock().unwrap() = Ok(status);
    }

    fn forget(&self) {
        *self.status.lock().unwrap() = Err(());
    }
}

#[async_trait::async_trait]
impl OwnerClient for Owner {
    async fn describe(
        &self,
        _: &str,
        workflow_id: &str,
        _: &str,
    ) -> Result<OwnerDescription, OwnerError> {
        match *self.status.lock().unwrap() {
            Ok(status) => Ok(OwnerDescription {
                run_id: "run-1".to_string(),
                first_run_id: "run-1".to_string(),
                status,
            }),
            Err(()) => Err(OwnerError::NotFound(workflow_id.to_string())),
        }
    }
}

static WORKFLOWS: AtomicUsize = AtomicUsize::new(0);

/// One stream of a Workflow no other test uses, so a shared Redis keeps tests apart.
fn new_stream() -> StreamAddress {
    StreamAddress {
        namespace: "ns".to_string(),
        owner_kind: StreamOwnerKind::Workflow as i32,
        workflow_id: format!(
            "fallbacks-{}-{}",
            std::process::id(),
            WORKFLOWS.fetch_add(1, Ordering::Relaxed)
        ),
        run_id: String::new(),
        topic: "out".to_string(),
    }
}

fn chain(stream: &StreamAddress) -> ChainId {
    ChainId {
        namespace: stream.namespace.clone(),
        workflow_id: stream.workflow_id.clone(),
        first_run_id: "run-1".to_string(),
    }
}

fn options() -> StreamsOptions {
    StreamsOptions {
        owner_recheck: Duration::from_millis(300),
        owner_check_min: Duration::from_millis(20),
        owner_check_max: Duration::from_millis(80),
        ..StreamsOptions::default()
    }
}

/// The stores each test runs on: memory with every capability, memory with no producer state,
/// memory with only the log, and Redis in plain mode when `STREAMS_REDIS_URL` is set.
async fn stores() -> Vec<(&'static str, Arc<dyn StreamStore>)> {
    let full = MemoryStore::new().capabilities();
    let mut stores: Vec<(&'static str, Arc<dyn StreamStore>)> = vec![
        ("memory", Arc::new(MemoryStore::new())),
        (
            "memory without producer state",
            Arc::new(MemoryStore::with_capabilities(Capabilities {
                producer_state: false,
                ..full
            })),
        ),
        (
            "memory with only the log",
            Arc::new(MemoryStore::with_capabilities(Capabilities {
                dense_positions: true,
                ..Capabilities::log_only()
            })),
        ),
    ];
    #[cfg(feature = "redis")]
    if let Ok(url) = std::env::var("STREAMS_REDIS_URL") {
        let mut options = temporalio_streams::RedisStoreOptions::new(url);
        options.key_prefix = format!("fallbacks-{}", std::process::id());
        options.plain = true;
        let store = temporalio_streams::RedisStore::connect(options)
            .await
            .unwrap();
        stores.push(("plain Redis", Arc::new(store)));
    }
    stores
}

fn data(value: &str) -> AppendRecord {
    AppendRecord {
        kind: StreamRecordKind::Data as i32,
        body: Some(Payload {
            data: value.as_bytes().to_vec(),
            ..Default::default()
        }),
        content_hash: Sha256::digest(value.as_bytes()).to_vec(),
    }
}

fn batch(stream: &StreamAddress, producer: &str, sequence: i64, values: &[&str]) -> AppendRequest {
    AppendRequest {
        stream: Some(stream.clone()),
        producer: Some(append_request::Producer::Named(NamedProducer {
            producer_id: producer.to_string(),
            attempt: 1,
        })),
        sequence,
        records: values.iter().map(|value| data(value)).collect(),
        digest: Sha256::digest(values.concat().as_bytes()).to_vec(),
    }
}

/// Appends as lang does: a failure that may have landed is sent again.
async fn append(streams: &Streams, request: AppendRequest) -> StreamResult<AppendResponse> {
    loop {
        match streams.append(request.clone()).await {
            Err(error)
                if matches!(
                    error.kind,
                    StreamFailureKind::OutcomeUnknown | StreamFailureKind::Storage
                ) => {}
            answer => return answer,
        }
    }
}

/// Reads the stream until the read is done, and answers what it delivered.
async fn read_to_end(streams: &Streams, stream: &StreamAddress) -> Vec<String> {
    let mut request = ReadRequest {
        stream: Some(stream.clone()),
        wait: Some(Duration::from_millis(500).try_into().unwrap()),
        ..Default::default()
    };
    let mut delivered = vec![];
    for _ in 0..100 {
        let page = streams.read(request.clone()).await.unwrap();
        delivered.extend(values(&page.records));
        if page.done {
            return delivered;
        }
        request.after = page.cursor;
        request.state = page.state;
    }
    panic!("the read never ended");
}

fn values(records: &[ReadRecord]) -> Vec<String> {
    records
        .iter()
        .filter_map(|record| match record.record.as_ref()? {
            read_record::Record::Stored(stored) => {
                Some(String::from_utf8(stored.body.clone()?.data).unwrap())
            }
            read_record::Record::Superseded(_) => None,
        })
        .collect()
}

#[tokio::test]
async fn a_lost_answer_and_a_late_copy_reach_readers_once() {
    for (label, store) in stores().await {
        let faulty = Faulty::new(store);
        let streams = Streams::new(faulty.clone(), Owner::running(), options());
        let stream = new_stream();
        faulty.lose.store(1, Ordering::Relaxed);
        append(&streams, batch(&stream, "p", 1, &["1"]))
            .await
            .unwrap();
        faulty.keep_copy.store(true, Ordering::Relaxed);
        append(&streams, batch(&stream, "p", 2, &["2", "3"]))
            .await
            .unwrap();
        append(&streams, batch(&stream, "p", 4, &["4"]))
            .await
            .unwrap();
        // A copy of the second batch lands after the third, as a delayed request does.
        faulty.land_copies().await;
        append(&streams, batch(&stream, "p", 5, &["5"]))
            .await
            .unwrap();
        let page = streams
            .read(ReadRequest {
                stream: Some(stream.clone()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(values(&page.records), ["1", "2", "3", "4", "5"], "{label}");
    }
}

#[tokio::test]
async fn a_copy_after_a_close_is_not_delivered() {
    for (label, store) in stores().await {
        let faulty = Faulty::new(store.clone());
        let streams = Streams::new(faulty.clone(), Owner::running(), options());
        let stream = new_stream();
        faulty.keep_copy.store(true, Ordering::Relaxed);
        append(&streams, batch(&stream, "p", 1, &["1"]))
            .await
            .unwrap();
        streams
            .close(CloseRequest {
                stream: Some(stream.clone()),
            })
            .await
            .unwrap();
        faulty.land_copies().await;
        let after = append(&streams, batch(&stream, "p", 2, &["2"])).await;
        // With close flags the store refuses the append. Without them it lands unread.
        assert_eq!(
            after.is_err(),
            store.capabilities().producer_state,
            "{label}"
        );
        assert_eq!(read_to_end(&streams, &stream).await, ["1"], "{label}");
    }
}

#[tokio::test]
async fn a_crash_between_a_topic_name_and_its_record_leaves_a_harmless_name() {
    let store = Arc::new(MemoryStore::with_capabilities(Capabilities {
        producer_state: false,
        topic_listing: false,
        ..MemoryStore::new().capabilities()
    }));
    let faulty = Faulty::new(store.clone());
    let streams = Streams::new(faulty.clone(), Owner::running(), options());
    let stream = new_stream();
    faulty.fail_next_data.store(true, Ordering::Relaxed);
    append(&streams, batch(&stream, "p", 1, &["1"]))
        .await
        .unwrap();
    // A producer that crashed for good after naming its topic.
    let quiet = StreamAddress {
        topic: "quiet".to_string(),
        ..stream.clone()
    };
    faulty.fail_next_data.store(true, Ordering::Relaxed);
    let crashed = streams.append(batch(&quiet, "q", 1, &["x"])).await;
    assert_eq!(crashed.unwrap_err().kind, StreamFailureKind::Storage);
    let names = store
        .read(StoreReadRequest {
            chain: Some(chain(&stream)),
            topic: CHAIN_LOG.to_string(),
            max_records: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(names.records.len(), 3);
    close_chain(&*store, &chain(&stream), "@run-1", 1)
        .await
        .unwrap();
    assert_eq!(read_to_end(&streams, &stream).await, ["1"]);
    assert!(read_to_end(&streams, &quiet).await.is_empty());
}

#[tokio::test]
async fn readers_write_nothing() {
    for (label, store) in stores().await {
        for end in ["completed", "terminated", "gone"] {
            let faulty = Faulty::new(store.clone());
            let owner = Owner::running();
            let streams = Streams::new(faulty.clone(), owner.clone(), options());
            let stream = new_stream();
            append(&streams, batch(&stream, "p", 1, &["1"]))
                .await
                .unwrap();
            let before = faulty.writes.load(Ordering::Relaxed);
            let mut request = ReadRequest {
                stream: Some(stream.clone()),
                wait: Some(Duration::from_millis(200).try_into().unwrap()),
                ..Default::default()
            };
            let first = streams.read(request.clone()).await.unwrap();
            let mut delivered = values(&first.records);
            request.after = first.cursor;
            request.state = first.state;
            match end {
                "completed" => owner.end(WorkflowExecutionStatus::Completed),
                "terminated" => owner.end(WorkflowExecutionStatus::Terminated),
                _ => owner.forget(),
            }
            // A gone owner ends a read only on a second answer a recheck apart.
            let mut done = false;
            for _ in 0..20 {
                let page = streams.read(request.clone()).await.unwrap();
                delivered.extend(values(&page.records));
                if page.done {
                    done = true;
                    break;
                }
                request.after = page.cursor;
                request.state = page.state;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(done, "{label} {end}");
            assert_eq!(delivered, ["1"], "{label} {end}");
            assert_eq!(
                faulty.writes.load(Ordering::Relaxed),
                before,
                "{label} {end}"
            );
        }
    }
}

#[tokio::test]
async fn readers_agree_on_the_end_only_after_a_close() {
    for (label, store) in stores().await {
        // The owner's Core closes the chain after the task that ends it, so every reader ends
        // at the same record.
        let owner = Owner::running();
        let streams = Streams::new(store.clone(), owner.clone(), options());
        let stream = new_stream();
        append(&streams, batch(&stream, "p", 1, &["1"]))
            .await
            .unwrap();
        close_chain(&*store, &chain(&stream), "@run-1", 1)
            .await
            .unwrap();
        owner.end(WorkflowExecutionStatus::Completed);
        let _ = streams.append(batch(&stream, "p", 2, &["2"])).await;
        assert_eq!(read_to_end(&streams, &stream).await, ["1"], "{label}");
        assert_eq!(read_to_end(&streams, &stream).await, ["1"], "{label}");

        // A terminate, or a Worker crash before the close, runs no Core step. Readers end on
        // the owner's status, each where it drains, until a producer's owner check closes.
        for status in [
            WorkflowExecutionStatus::Terminated,
            WorkflowExecutionStatus::Completed,
        ] {
            let owner = Owner::running();
            let streams = Streams::new(store.clone(), owner.clone(), options());
            let stream = new_stream();
            append(&streams, batch(&stream, "p", 1, &["1"]))
                .await
                .unwrap();
            owner.end(status);
            let early = read_to_end(&streams, &stream).await;
            // The producer's last owner check is fresh, so it still writes.
            append(&streams, batch(&stream, "p", 2, &["2"]))
                .await
                .unwrap();
            let later = read_to_end(&streams, &stream).await;
            assert_eq!(early, ["1"], "{label} {status:?}");
            assert_eq!(later, ["1", "2"], "{label} {status:?}");
            tokio::time::sleep(options().owner_recheck).await;
            let refused = streams.append(batch(&stream, "p", 3, &["3"])).await;
            assert_eq!(refused.unwrap_err().kind, StreamFailureKind::Closed);
            let other = streams.append(batch(&stream, "q", 1, &["4"])).await;
            assert_eq!(other.unwrap_err().kind, StreamFailureKind::Closed);
            assert_eq!(read_to_end(&streams, &stream).await, ["1", "2"], "{label}");
        }
    }
}

/// Reads every page of a topic with `max_records` per call, carrying the state.
async fn paged(store: &dyn StreamStore, target: &ReadTarget, max_records: u32) -> Vec<String> {
    let mut request = ReadRequest {
        after: BEGINNING.to_string(),
        max_records,
        ..Default::default()
    };
    let mut delivered = vec![];
    for _ in 0..100 {
        let page = read_page(store, target, &request).await.unwrap();
        if page.cursor == request.after && page.records.is_empty() {
            return delivered;
        }
        delivered.extend(values(&page.records));
        request.after = page.cursor;
        request.state = page.state;
    }
    panic!("the read never ended");
}

#[tokio::test]
async fn a_read_gives_the_same_records_however_it_pages() {
    for (label, store) in stores().await {
        if store.capabilities().producer_state {
            continue;
        }
        let stream = new_stream();
        let chain = chain(&stream);
        let record = |producer: &str, attempt: i64, sequence: i64, kind: StreamRecordKind| {
            let value = format!("{producer}{attempt}.{sequence}");
            StreamRecord {
                topic: "out".to_string(),
                kind: kind as i32,
                producer_id: producer.to_string(),
                attempt,
                sequence,
                body: (kind == StreamRecordKind::Data).then(|| Payload {
                    data: value.into_bytes(),
                    ..Default::default()
                }),
                ..Default::default()
            }
        };
        let data = StreamRecordKind::Data;
        let close = StreamRecordKind::Close;
        let log = [
            record("p", 1, 1, data),
            record("q", 1, 1, data),
            record("p", 1, 1, data),
            record("p", 2, 1, data),
            record("p", 1, 2, data),
            record("q", 1, 2, data),
            record("q", 1, 2, data),
            record("c", 1, 0, close),
            record("p", 2, 2, data),
            record("d", 1, 0, close),
        ];
        let mut producers = HashMap::new();
        for record in log {
            let bytes = record.encode_to_vec();
            let attempt = producers.entry(record.producer_id.clone()).or_insert(0);
            *attempt += 1;
            store
                .append(StoreAppendRequest {
                    chain: Some(chain.clone()),
                    topic: "out".to_string(),
                    producer_id: record.producer_id,
                    attempt: record.attempt,
                    sequence: *attempt,
                    digest: Sha256::digest(&bytes).to_vec(),
                    records: vec![bytes],
                })
                .await
                .unwrap();
        }
        let target = ReadTarget {
            chain: chain.clone(),
            topic: "out".to_string(),
            stream_hash: stream_hash("ns", "workflow", &stream.workflow_id, "out"),
        };
        let whole = paged(&*store, &target, 1000).await;
        assert_eq!(whole, ["p1.1", "q1.1", "p2.1", "p1.2", "q1.2"], "{label}");
        for max_records in [1, 2, 3] {
            assert_eq!(
                paged(&*store, &target, max_records).await,
                whole,
                "{label} {max_records}"
            );
        }
    }
}
