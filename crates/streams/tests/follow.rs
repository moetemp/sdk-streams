//! `Follow` and `FollowRange` through the stream service, on memory, and on Redis when
//! `STREAMS_REDIS_URL` and `STREAMS_REDIS_CLUSTER_URL` name servers.

use prost::Message;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::protos::temporal::api::{
    common::v1::Payload, enums::v1::StreamOwnerKind as RefKind, stream::v1::StreamReference,
};
use temporalio_streams::{
    MemoryStore, OwnerClient, OwnerDescription, OwnerError, OwnerId, OwnerStatus, StreamService,
    StreamStore, StreamsOptions,
    proto::{
        AppendRecord, AppendRequest, CloseRequest, FollowRangeRequest, FollowRequest,
        FollowResponse, NamedProducer, StreamAddress, StreamFailure, StreamFailureKind, StreamLink,
        StreamOwnerKind, StreamRecordKind, append_request::Producer, followed_record, link_state,
        read_record, stream_link,
    },
};

static IDS: AtomicUsize = AtomicUsize::new(0);

/// Every owner's latest run is `run-1`, the first of its chain, and still running. A Workflow
/// Activity is pending.
struct Running;

#[async_trait::async_trait]
impl OwnerClient for Running {
    async fn describe(
        &self,
        _: &str,
        _: OwnerId<'_>,
        _: &str,
    ) -> Result<OwnerDescription, OwnerError> {
        Ok(OwnerDescription {
            run_id: "run-1".to_string(),
            first_run_id: "run-1".to_string(),
            status: OwnerStatus::Running,
        })
    }
}

struct Streams {
    service: StreamService,
    agent: String,
}

fn service(store: Arc<dyn StreamStore>) -> Streams {
    Streams {
        service: StreamService::new(store, Arc::new(Running), StreamsOptions::default()),
        agent: format!(
            "agent-{}-{}",
            std::process::id(),
            IDS.fetch_add(1, Ordering::Relaxed)
        ),
    }
}

async fn on_memory() -> Option<Streams> {
    Some(service(Arc::new(MemoryStore::new())))
}

#[cfg(feature = "redis")]
async fn on_redis(variable: &str, cluster: bool) -> Option<Streams> {
    let Ok(url) = std::env::var(variable) else {
        eprintln!("set {variable} to run the follow on Redis");
        return None;
    };
    let mut options = temporalio_streams::RedisStoreOptions::new(url);
    options.cluster = cluster;
    options.key_prefix = format!("follow-{}", std::process::id());
    Some(service(Arc::new(
        temporalio_streams::RedisStore::connect(options)
            .await
            .unwrap(),
    )))
}

impl Streams {
    async fn call<T: Message + Default>(&self, rpc: &str, request: impl Message) -> T {
        T::decode(self.try_call(rpc, request).await.unwrap().as_slice()).unwrap()
    }

    async fn try_call(&self, rpc: &str, request: impl Message) -> Result<Vec<u8>, StreamFailure> {
        self.service.call(rpc, &request.encode_to_vec()).await
    }

    fn agent_stream(&self) -> StreamAddress {
        StreamAddress {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: self.agent.clone(),
            topic: "events".to_string(),
            ..Default::default()
        }
    }

    /// Tool call `tool-42` of the agent's run `run-1`.
    fn tool_stream(&self) -> StreamAddress {
        StreamAddress {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::WorkflowActivity as i32,
            workflow_id: self.agent.clone(),
            run_id: "run-1".to_string(),
            topic: "tokens".to_string(),
            activity_id: "tool-42".to_string(),
        }
    }

    fn child_stream(&self) -> StreamAddress {
        StreamAddress {
            workflow_id: format!("{}-child", self.agent),
            ..self.agent_stream()
        }
    }

    async fn append(&self, stream: &StreamAddress, sequence: i64, records: Vec<AppendRecord>) {
        let _: temporalio_streams::proto::AppendResponse = self
            .call(
                "Append",
                AppendRequest {
                    stream: Some(stream.clone()),
                    producer: Some(Producer::Named(NamedProducer {
                        producer_id: "p".to_string(),
                        attempt: 1,
                    })),
                    sequence,
                    digest: vec![sequence as u8; 32],
                    records,
                },
            )
            .await;
    }

    async fn close(&self, stream: &StreamAddress, result: &str) {
        let _: temporalio_streams::proto::CloseResponse = self
            .call(
                "Close",
                CloseRequest {
                    stream: Some(stream.clone()),
                    result: Some(Payload {
                        data: result.as_bytes().to_vec(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await;
    }

    /// Follows the agent to its end, `max_records` per call. Answers each call's request state
    /// and its answer.
    async fn follow_to_end(&self, max_records: u32) -> Vec<(Vec<u8>, FollowResponse)> {
        let mut state = vec![];
        let mut calls = vec![];
        for _ in 0..200 {
            let answer: FollowResponse = self
                .call(
                    "Follow",
                    FollowRequest {
                        stream: Some(self.agent_stream()),
                        state: state.clone(),
                        max_records,
                        ..Default::default()
                    },
                )
                .await;
            let next = answer.state.clone();
            let done = answer.done;
            calls.push((state, answer));
            if done {
                return calls;
            }
            state = next;
        }
        panic!("the follow never ended");
    }
}

fn data(value: &str) -> AppendRecord {
    AppendRecord {
        kind: StreamRecordKind::Data as i32,
        body: Some(Payload {
            data: value.as_bytes().to_vec(),
            ..Default::default()
        }),
        content_hash: vec![value.as_bytes()[0]; 32],
        link: None,
    }
}

fn reference(stream: &StreamAddress) -> StreamReference {
    StreamReference {
        owner_kind: match StreamOwnerKind::try_from(stream.owner_kind).unwrap() {
            StreamOwnerKind::WorkflowActivity => RefKind::WorkflowActivity as i32,
            _ => RefKind::Workflow as i32,
        },
        workflow_id: stream.workflow_id.clone(),
        run_id: stream.run_id.clone(),
        topic: stream.topic.clone(),
        activity_id: stream.activity_id.clone(),
    }
}

fn link(event: stream_link::Event, stream: Option<&StreamAddress>, name: &str) -> AppendRecord {
    AppendRecord {
        kind: StreamRecordKind::Link as i32,
        link: Some(StreamLink {
            event: event as i32,
            stream: stream.map(reference),
            name: name.to_string(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Each item as text: a record's body, or a link state's kind and the link's name.
fn shape(calls: &[(Vec<u8>, FollowResponse)]) -> Vec<String> {
    calls
        .iter()
        .flat_map(|(_, answer)| answer.records.iter())
        .map(|item| match &item.item {
            Some(followed_record::Item::Record(record)) => match &record.record {
                Some(read_record::Record::Stored(stored)) => match &stored.link {
                    Some(link) => {
                        let event = stream_link::Event::try_from(link.event).unwrap();
                        format!("{}:{}", event.as_str_name(), link.name)
                    }
                    None => String::from_utf8(stored.body.clone().unwrap().data).unwrap(),
                },
                _ => "SUPERSEDED".to_string(),
            },
            Some(followed_record::Item::LinkState(state)) => {
                let kind = link_state::Kind::try_from(state.kind)
                    .unwrap()
                    .as_str_name();
                let name = state.link.as_ref().map(|l| l.name.as_str()).unwrap_or("");
                format!("{kind}:{name}")
            }
            None => "none".to_string(),
        })
        .collect()
}

/// The agent writes `d1`, opens tool call `tool-42` and child `sub`, writes `d2`, joins the tool,
/// writes `d3`, joins the child at its second record, writes `d4` and closes. The tool writes
/// `t1 t2` and closes. The child writes `c1 c2 c3`.
async fn write_agent(streams: &Streams) {
    use stream_link::Event::{Join, Open};
    let (agent, tool, child) = (
        streams.agent_stream(),
        streams.tool_stream(),
        streams.child_stream(),
    );
    streams
        .append(&child, 1, vec![data("c1"), data("c2"), data("c3")])
        .await;
    streams.append(&tool, 1, vec![data("t1"), data("t2")]).await;
    streams.close(&tool, "ok").await;
    let latest: temporalio_streams::proto::LatestResponse = streams
        .call(
            "Latest",
            temporalio_streams::proto::LatestRequest {
                stream: Some(child.clone()),
            },
        )
        .await;
    // `c2` is the record before the newest.
    let read: temporalio_streams::proto::ReadResponse = streams
        .call(
            "Read",
            temporalio_streams::proto::ReadRequest {
                stream: Some(child.clone()),
                max_records: 2,
                ..Default::default()
            },
        )
        .await;
    assert_ne!(read.cursor, latest.cursor);
    let mut join_child = link(Join, None, "sub");
    join_child.link.as_mut().unwrap().until = read.cursor;
    streams
        .append(
            &agent,
            1,
            vec![
                data("d1"),
                link(Open, Some(&tool), "tool-42"),
                link(Open, Some(&child), "sub"),
                data("d2"),
                link(Join, None, "tool-42"),
                data("d3"),
                join_child,
                data("d4"),
            ],
        )
        .await;
    streams.close(&agent, "root").await;
}

/// Order within each stream, the open gate and the close gate, with and without `until`.
fn check_gates(order: &[String]) {
    let position = |item: &str| {
        order
            .iter()
            .position(|x| x == item)
            .unwrap_or_else(|| panic!("{item} missing from {order:?}"))
    };
    let mut unique = order.to_vec();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), order.len(), "a repeat in {order:?}");
    assert!(position("KIND_OPENED:tool-42") < position("t1"));
    assert!(position("KIND_OPENED:sub") < position("c1"));
    assert!(position("KIND_DONE:tool-42") < position("EVENT_JOIN:tool-42"));
    assert!(position("EVENT_JOIN:tool-42") < position("d3"));
    assert!(position("c2") < position("KIND_DONE:sub"));
    assert!(position("KIND_DONE:sub") < position("d4"));
    // A child read before its JOIN may run past `until`, but nothing of it comes after its
    // segment ended.
    if let Some(c3) = order.iter().position(|x| x == "c3") {
        assert!(c3 < position("KIND_DONE:sub"), "{order:?}");
    }
    for (a, b) in [
        ("d1", "d2"),
        ("d2", "d3"),
        ("d3", "d4"),
        ("t1", "t2"),
        ("c1", "c2"),
    ] {
        assert!(position(a) < position(b), "{a} {b}: {order:?}");
    }
}

async fn follows_in_one_order(streams: Option<Streams>) {
    let Some(streams) = streams else { return };
    write_agent(&streams).await;
    let whole = streams.follow_to_end(100).await;
    let order = shape(&whole);
    check_gates(&order);
    let last = &whole.last().unwrap().1;
    assert!(last.done);
    assert_eq!(last.close_result.as_ref().unwrap().data, b"root");
    let tool_done = whole
        .iter()
        .flat_map(|(_, answer)| answer.records.iter())
        .find_map(|item| match &item.item {
            Some(followed_record::Item::LinkState(state))
                if state.kind == link_state::Kind::Done as i32
                    && state.link.as_ref().unwrap().name == "tool-42" =>
            {
                Some(state.clone())
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(tool_done.close_result.unwrap().data, b"ok");

    // A follower that resumes from every position, one record per call, loses nothing and
    // repeats nothing.
    // The order across streams is read order, so it moves with the batch size, but every gate
    // holds and only the child's read-ahead past `until` differs.
    let stepped = streams.follow_to_end(1).await;
    let stepped_order = shape(&stepped);
    check_gates(&stepped_order);
    let without_read_ahead = |order: &[String]| -> Vec<String> {
        let mut kept: Vec<String> = order.iter().filter(|x| *x != "c3").cloned().collect();
        kept.sort();
        kept
    };
    assert_eq!(
        without_read_ahead(&stepped_order),
        without_read_ahead(&order)
    );

    // Each call reads again from its ranges alone, with the same records in the same order.
    for (state_in, answer) in &stepped {
        let replayed: FollowResponse = streams
            .call(
                "FollowRange",
                FollowRangeRequest {
                    stream: Some(streams.agent_stream()),
                    state_in: state_in.clone(),
                    ranges: answer.ranges.clone(),
                    follow_digest: answer.follow_digest.clone(),
                    ..Default::default()
                },
            )
            .await;
        // The state differs only in the live read's owner check clock, which replay doesn't run.
        assert_eq!(replayed.records, answer.records);
        assert_eq!(replayed.sources, answer.sources);
    }
    // Another order fails as a mismatch.
    let (state_in, answer) = stepped.iter().find(|(_, a)| !a.records.is_empty()).unwrap();
    let failure = streams
        .try_call(
            "FollowRange",
            FollowRangeRequest {
                stream: Some(streams.agent_stream()),
                state_in: state_in.clone(),
                ranges: answer.ranges.clone(),
                follow_digest: vec![0; 32],
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(failure.kind, StreamFailureKind::ReplayMismatch as i32);
}

async fn a_missing_child_is_unavailable_after_the_grace(streams: Option<Streams>) {
    let Some(mut streams) = streams else { return };
    // A grace of zero ends the wait for a child that never starts at once.
    streams.service = StreamService::new(
        streams.service.store(),
        Arc::new(Missing),
        StreamsOptions {
            owner_start_grace: Duration::ZERO,
            ..StreamsOptions::default()
        },
    );
    let agent = streams.agent_stream();
    let mut gone = streams.agent_stream();
    gone.workflow_id = "missing".to_string();
    streams
        .append(
            &agent,
            1,
            vec![
                link(stream_link::Event::Open, Some(&gone), "gone"),
                data("d1"),
            ],
        )
        .await;
    streams.close(&agent, "root").await;
    let order = shape(&streams.follow_to_end(100).await);
    assert_eq!(
        order,
        [
            "EVENT_OPEN:gone",
            "KIND_OPENED:gone",
            "d1",
            "KIND_UNAVAILABLE:gone"
        ]
    );
}

/// Every Workflow is `Running` but `missing`, which Temporal doesn't know.
struct Missing;

#[async_trait::async_trait]
impl OwnerClient for Missing {
    async fn describe(
        &self,
        namespace: &str,
        owner: OwnerId<'_>,
        run_id: &str,
    ) -> Result<OwnerDescription, OwnerError> {
        if owner == OwnerId::Workflow("missing") {
            return Err(OwnerError::NotFound("missing".to_string()));
        }
        Running.describe(namespace, owner, run_id).await
    }
}

#[tokio::test]
async fn memory_follows_in_one_order() {
    follows_in_one_order(on_memory().await).await;
}

#[tokio::test]
async fn memory_a_missing_child_is_unavailable_after_the_grace() {
    a_missing_child_is_unavailable_after_the_grace(on_memory().await).await;
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn standalone_redis_follows_in_one_order() {
    follows_in_one_order(on_redis("STREAMS_REDIS_URL", false).await).await;
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn cluster_redis_follows_in_one_order() {
    follows_in_one_order(on_redis("STREAMS_REDIS_CLUSTER_URL", true).await).await;
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn standalone_redis_a_missing_child_is_unavailable_after_the_grace() {
    a_missing_child_is_unavailable_after_the_grace(on_redis("STREAMS_REDIS_URL", false).await)
        .await;
}

/// A memory store that writes `racing` into the agent's stream right after the first `latest`
/// call answers, as an append between a `$end` read's `latest` and its read would.
struct RacingStore {
    inner: MemoryStore,
    racing: std::sync::Mutex<Option<temporalio_streams::proto::StoreAppendRequest>>,
}

#[async_trait::async_trait]
impl StreamStore for RacingStore {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn capabilities(&self) -> temporalio_streams::Capabilities {
        self.inner.capabilities()
    }

    async fn append(
        &self,
        request: temporalio_streams::proto::StoreAppendRequest,
    ) -> temporalio_streams::StreamResult<temporalio_streams::proto::StoreAppendResponse> {
        self.inner.append(request).await
    }

    async fn read(
        &self,
        request: temporalio_streams::proto::StoreReadRequest,
    ) -> temporalio_streams::StreamResult<temporalio_streams::proto::StoreReadResponse> {
        self.inner.read(request).await
    }

    async fn latest(
        &self,
        request: temporalio_streams::proto::StoreLatestRequest,
    ) -> temporalio_streams::StreamResult<temporalio_streams::proto::StoreLatestResponse> {
        let latest = self.inner.latest(request).await;
        let racing = self.racing.lock().unwrap().take();
        if let Some(racing) = racing {
            self.inner.append(racing).await.unwrap();
        }
        latest
    }

    async fn record_at(
        &self,
        chain: &temporalio_streams::proto::ChainId,
        topic: &str,
        position: &str,
    ) -> temporalio_streams::StreamResult<Option<Vec<u8>>> {
        self.inner.record_at(chain, topic, position).await
    }

    async fn close_chain(
        &self,
        chain: &temporalio_streams::proto::ChainId,
    ) -> temporalio_streams::StreamResult<()> {
        self.inner.close_chain(chain).await
    }

    async fn close_topic(
        &self,
        chain: &temporalio_streams::proto::ChainId,
        topic: &str,
    ) -> temporalio_streams::StreamResult<()> {
        self.inner.close_topic(chain, topic).await
    }

    async fn retire_producer(
        &self,
        chain: &temporalio_streams::proto::ChainId,
        topic: &str,
        producer_id: &str,
        attempt: i64,
    ) -> temporalio_streams::StreamResult<()> {
        self.inner
            .retire_producer(chain, topic, producer_id, attempt)
            .await
    }

    async fn written_topics(
        &self,
        chain: &temporalio_streams::proto::ChainId,
    ) -> temporalio_streams::StreamResult<Vec<String>> {
        self.inner.written_topics(chain).await
    }

    async fn delete_owner(
        &self,
        request: temporalio_streams::proto::DeleteOwnerRequest,
    ) -> temporalio_streams::StreamResult<temporalio_streams::proto::DeleteOwnerResponse> {
        self.inner.delete_owner(request).await
    }
}

#[tokio::test]
async fn a_follow_from_the_end_keeps_records_that_landed_during_its_first_read() {
    use stream_link::Event::{Join, Open};
    let store = Arc::new(RacingStore {
        inner: MemoryStore::new(),
        racing: std::sync::Mutex::new(None),
    });
    let streams = service(store.clone());
    let (agent, tool) = (streams.agent_stream(), streams.tool_stream());
    // The open tool's JOIN gates the page, so the follow reads it again up to the gate.
    let records = [
        data("d1"),
        link(Open, Some(&tool), "tool-42"),
        link(Join, None, "tool-42"),
        data("d2"),
    ];
    *store.racing.lock().unwrap() = Some(temporalio_streams::proto::StoreAppendRequest {
        chain: Some(temporalio_streams::proto::ChainId {
            namespace: agent.namespace.clone(),
            workflow_id: agent.workflow_id.clone(),
            first_run_id: "run-1".to_string(),
            ..Default::default()
        }),
        topic: agent.topic.clone(),
        producer_id: "p".to_string(),
        attempt: 1,
        sequence: 1,
        digest: vec![1; 32],
        records: records
            .iter()
            .enumerate()
            .map(|(at, record)| {
                temporalio_streams::stored_append_record(
                    &agent.topic,
                    "p",
                    1,
                    1 + at as i64,
                    record,
                )
                .unwrap()
                .encode_to_vec()
            })
            .collect(),
    });
    let first: FollowResponse = streams
        .call(
            "Follow",
            FollowRequest {
                stream: Some(agent.clone()),
                after: "$end".to_string(),
                ..Default::default()
            },
        )
        .await;
    let order = shape(&[(vec![], first)]);
    assert!(order.contains(&"d1".to_string()), "{order:?}");
    assert!(
        order.contains(&"EVENT_OPEN:tool-42".to_string()),
        "{order:?}"
    );
}
