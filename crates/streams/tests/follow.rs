//! `Follow` through the stream service, on memory, and on Redis when `STREAMS_REDIS_URL`,
//! `STREAMS_REDIS_CLUSTER_URL` or `STREAMS_KVROCKS_URL` names a server.

#[cfg(feature = "redis")]
#[path = "support/server.rs"]
mod server;

use prost::Message;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::protos::temporal::api::{
    common::v1::Payload, enums::v1::WorkflowExecutionStatus,
};
use temporalio_streams::protos::temporal::api::{
    enums::v1::StreamOwnerKind as RefKind, stream::v1::StreamReference,
};
use temporalio_streams::{
    MemoryStore, OwnerClient, OwnerDescription, OwnerError, StreamService, StreamStore,
    StreamsOptions,
    proto::{
        AppendRecord, AppendRequest, CloseRequest, FollowRequest, FollowResponse, NamedProducer,
        StreamAddress, StreamFailure, StreamFailureKind, StreamLink, StreamOwnerKind,
        StreamRecordKind, append_request::Producer, followed_record, link_state, read_record,
        stream_link,
    },
};

static IDS: AtomicUsize = AtomicUsize::new(0);

/// Every Workflow's latest run is `run-1`, the first of its chain, and still running.
struct Running;

#[async_trait::async_trait]
impl OwnerClient for Running {
    async fn describe(&self, _: &str, _: &str, _: &str) -> Result<OwnerDescription, OwnerError> {
        Ok(OwnerDescription {
            run_id: "run-1".to_string(),
            first_run_id: "run-1".to_string(),
            status: WorkflowExecutionStatus::Running,
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
    let url = server::server_url(variable)?;
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

    /// A child Workflow of the agent, `<agent>-<name>`.
    fn workflow_stream(&self, name: &str) -> StreamAddress {
        StreamAddress {
            workflow_id: format!("{}-{name}", self.agent),
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
                        ..Default::default()
                    })),
                    sequence,
                    digest: vec![sequence as u8; 32],
                    records,
                },
            )
            .await;
    }

    /// Appends one batch as attempt `attempt` of producer `producer`.
    async fn append_as(
        &self,
        stream: &StreamAddress,
        producer: &str,
        attempt: i64,
        records: Vec<AppendRecord>,
    ) {
        let _: temporalio_streams::proto::AppendResponse = self
            .call(
                "Append",
                AppendRequest {
                    stream: Some(stream.clone()),
                    producer: Some(Producer::Named(NamedProducer {
                        producer_id: producer.to_string(),
                        attempt,
                        ..Default::default()
                    })),
                    sequence: 1,
                    digest: vec![attempt as u8; 32],
                    records,
                },
            )
            .await;
    }

    async fn close(&self, stream: &StreamAddress) {
        let _: temporalio_streams::proto::CloseResponse = self
            .call(
                "Close",
                CloseRequest {
                    stream: Some(stream.clone()),
                },
            )
            .await;
    }

    /// The cursor of the `count`th record of `stream`.
    async fn cursor_at(&self, stream: &StreamAddress, count: u32) -> String {
        let read: temporalio_streams::proto::ReadResponse = self
            .call(
                "Read",
                temporalio_streams::proto::ReadRequest {
                    stream: Some(stream.clone()),
                    max_records: count,
                    ..Default::default()
                },
            )
            .await;
        assert_eq!(read.records.len(), count as usize);
        read.cursor
    }

    /// Follows the agent to its end, `max_records` per call and links `max_depth` deep. Answers
    /// each call's request state and its answer.
    async fn follow_to_end(
        &self,
        max_records: u32,
        max_depth: u32,
    ) -> Vec<(Vec<u8>, FollowResponse)> {
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
                        max_depth,
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
        owner_kind: RefKind::Workflow as i32,
        workflow_id: stream.workflow_id.clone(),
        run_id: stream.run_id.clone(),
        topic: stream.topic.clone(),
        ..Default::default()
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

fn join_until(name: &str, until: String) -> AppendRecord {
    let mut join = link(stream_link::Event::Join, None, name);
    join.link.as_mut().unwrap().until = until;
    join
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
                if state.reason.is_empty() {
                    format!("{kind}:{name}")
                } else {
                    format!("{kind}:{name}:{}", state.reason)
                }
            }
            None => "none".to_string(),
        })
        .collect()
}

/// The agent writes `d1`, opens child Workflows `one` and `two`, writes `d2`, joins `one`, writes
/// `d3`, joins `two` at its fifth record, writes `d4` and closes. Child `one` writes `s1 s2` and
/// closes. Child `two` writes `c1`, opens grandchild `three`, writes `c2`, joins `three`, then
/// writes `c3 c4`. Grandchild `three` writes `g1 g2` and closes.
async fn write_agent(streams: &Streams) {
    use stream_link::Event::{Join, Open};
    let (agent, one, two, three) = (
        streams.agent_stream(),
        streams.workflow_stream("one"),
        streams.workflow_stream("two"),
        streams.workflow_stream("three"),
    );
    streams
        .append(&three, 1, vec![data("g1"), data("g2")])
        .await;
    streams.close(&three).await;
    streams
        .append(
            &two,
            1,
            vec![
                data("c1"),
                link(Open, Some(&three), "three"),
                data("c2"),
                link(Join, None, "three"),
                data("c3"),
                data("c4"),
            ],
        )
        .await;
    streams.append(&one, 1, vec![data("s1"), data("s2")]).await;
    streams.close(&one).await;
    // `c3` is the fifth record of `two`, before its newest.
    let until = streams.cursor_at(&two, 5).await;
    streams
        .append(
            &agent,
            1,
            vec![
                data("d1"),
                link(Open, Some(&one), "one"),
                link(Open, Some(&two), "two"),
                data("d2"),
                link(Join, None, "one"),
                data("d3"),
                join_until("two", until),
                data("d4"),
            ],
        )
        .await;
    streams.close(&agent).await;
}

/// Order within each stream, the open gates and the close gates, with and without `until`.
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
    assert!(position("KIND_OPENED:one") < position("s1"));
    assert!(position("KIND_OPENED:two") < position("c1"));
    assert!(position("KIND_OPENED:three") < position("g1"));
    assert!(position("KIND_DONE:one") < position("EVENT_JOIN:one"));
    assert!(position("EVENT_JOIN:one") < position("d3"));
    assert!(position("KIND_DONE:three") < position("EVENT_JOIN:three"));
    assert!(position("EVENT_JOIN:three") < position("c3"));
    assert!(position("c3") < position("KIND_DONE:two"));
    assert!(position("KIND_DONE:two") < position("d4"));
    // A child read before its JOIN may run past `until`, but nothing of it comes after its
    // segment ended.
    if let Some(c4) = order.iter().position(|x| x == "c4") {
        assert!(c4 < position("KIND_DONE:two"), "{order:?}");
    }
    for (a, b) in [
        ("d1", "d2"),
        ("d2", "d3"),
        ("d3", "d4"),
        ("s1", "s2"),
        ("c1", "c2"),
        ("c2", "c3"),
        ("g1", "g2"),
    ] {
        assert!(position(a) < position(b), "{a} {b}: {order:?}");
    }
}

async fn follows_in_one_order(streams: Option<Streams>) {
    let Some(streams) = streams else { return };
    write_agent(&streams).await;
    let whole = streams.follow_to_end(100, 0).await;
    let order = shape(&whole);
    check_gates(&order);
    assert!(whole.last().unwrap().1.done);
    // Each answer names the sources its records belong to, the grandchild under its child.
    let sources: Vec<_> = whole
        .iter()
        .flat_map(|(_, answer)| answer.sources.iter())
        .collect();
    let three = sources.iter().find(|s| s.name == "three").unwrap();
    let two = sources.iter().find(|s| s.name == "two").unwrap();
    assert_eq!((three.parent, three.depth), (two.number, 2));
    assert_eq!(
        three.stream.as_ref().unwrap().workflow_id,
        format!("{}-three", streams.agent)
    );

    // A follower that resumes from every position, one record per call, loses nothing and
    // repeats nothing. The order across streams is read order, so it moves with the batch size,
    // but every gate holds and only the child's read-ahead past `until` differs.
    let stepped = streams.follow_to_end(1, 0).await;
    let stepped_order = shape(&stepped);
    check_gates(&stepped_order);
    let without_read_ahead = |order: &[String]| -> Vec<String> {
        let mut kept: Vec<String> = order.iter().filter(|x| *x != "c4").cloned().collect();
        kept.sort();
        kept
    };
    assert_eq!(
        without_read_ahead(&stepped_order),
        without_read_ahead(&order)
    );
    // The same store content and the same batch size give the same order.
    assert_eq!(shape(&streams.follow_to_end(1, 0).await), stepped_order);
}

async fn a_link_past_the_depth_is_skipped(streams: Option<Streams>) {
    let Some(streams) = streams else { return };
    write_agent(&streams).await;
    let order = shape(&streams.follow_to_end(100, 1).await);
    assert!(
        order.contains(&"KIND_SKIPPED:three:depth".to_string()),
        "{order:?}"
    );
    assert!(!order.iter().any(|x| x.starts_with('g')), "{order:?}");
    // A JOIN of a link that was skipped waits for nothing.
    let at = |item: &str| order.iter().position(|x| x == item).unwrap();
    assert!(at("EVENT_JOIN:three") < at("c3"));
}

async fn a_link_back_to_an_open_stream_is_a_cycle(streams: Option<Streams>) {
    let Some(streams) = streams else { return };
    use stream_link::Event::{Join, Open};
    let (agent, child) = (streams.agent_stream(), streams.workflow_stream("child"));
    streams
        .append(
            &child,
            1,
            vec![data("c1"), link(Open, Some(&agent), "back"), data("c2")],
        )
        .await;
    streams.close(&child).await;
    streams
        .append(
            &agent,
            1,
            vec![
                link(Open, Some(&child), "child"),
                link(Join, None, "child"),
                data("d1"),
            ],
        )
        .await;
    streams.close(&agent).await;
    let order = shape(&streams.follow_to_end(100, 0).await);
    assert_eq!(
        order,
        [
            "EVENT_OPEN:child",
            "KIND_OPENED:child",
            "c1",
            "EVENT_OPEN:back",
            "KIND_SKIPPED:back:cycle",
            "c2",
            "KIND_DONE:child",
            "EVENT_JOIN:child",
            "d1",
        ]
    );
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
    streams.close(&agent).await;
    let order = shape(&streams.follow_to_end(100, 0).await);
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

/// Activity attempt 2 sends turn 1 and its OPEN, and the Workflow joins turn 1. Then attempt 1's
/// late append of the same OPEN comes in, and turn 2 opens the same child from turn 1's end.
async fn a_stale_open_after_the_join_opens_nothing(streams: Option<Streams>) {
    let Some(streams) = streams else { return };
    use stream_link::Event::Open;
    let (agent, child) = (streams.agent_stream(), streams.workflow_stream("child"));
    streams
        .append(&child, 1, vec![data("t1"), data("t2")])
        .await;
    let (end_1, end_2) = (
        streams.cursor_at(&child, 1).await,
        streams.cursor_at(&child, 2).await,
    );
    let open = |name: &str, after: &str| {
        let mut open = link(Open, Some(&child), name);
        open.link.as_mut().unwrap().after = after.to_string();
        open
    };
    streams
        .append_as(&agent, "turn-1", 2, vec![data("m1"), open("h#1", "")])
        .await;
    streams
        .append_as(&agent, "wf", 1, vec![join_until("h#1", end_1.clone())])
        .await;
    streams
        .append_as(&agent, "turn-1", 1, vec![data("late"), open("h#1", "")])
        .await;
    streams
        .append_as(&agent, "turn-2", 1, vec![data("m2"), open("h#2", &end_1)])
        .await;
    streams
        .append(&agent, 2, vec![join_until("h#2", end_2), data("r2")])
        .await;
    streams.close(&agent).await;
    let whole = streams.follow_to_end(100, 0).await;
    let late: Vec<_> = whole
        .iter()
        .flat_map(|(_, answer)| answer.records.iter())
        .filter_map(|item| match &item.item {
            Some(followed_record::Item::Record(record)) if record.stale => Some(record),
            _ => None,
        })
        .collect();
    assert_eq!(
        late.len(),
        2,
        "the late DATA and the late OPEN read as stale"
    );
    let order = shape(&whole);
    assert_eq!(
        order,
        [
            "m1",
            "EVENT_OPEN:h#1",
            "KIND_OPENED:h#1",
            "t1",
            "KIND_DONE:h#1",
            "EVENT_JOIN:h#1",
            "late",
            "EVENT_OPEN:h#1",
            "m2",
            "EVENT_OPEN:h#2",
            "KIND_OPENED:h#2",
            "t2",
            "KIND_DONE:h#2",
            "EVENT_JOIN:h#2",
            "r2",
        ]
    );
    // A follower that resumes after each record gets the same order.
    assert_eq!(shape(&streams.follow_to_end(1, 0).await), order);
}

/// Activity attempt 1 sends `m1` and opens the child. Attempt 2 sends `m2` and writes the same
/// OPEN again, and the Workflow joins the child at its third record.
async fn an_activity_retry_that_writes_the_same_open_resumes_the_segment(streams: Option<Streams>) {
    let Some(streams) = streams else { return };
    use stream_link::Event::Open;
    let (agent, child) = (streams.agent_stream(), streams.workflow_stream("child"));
    streams
        .append(
            &child,
            1,
            vec![data("c1"), data("c2"), data("c3"), data("c4")],
        )
        .await;
    let until = streams.cursor_at(&child, 3).await;
    streams
        .append_as(
            &agent,
            "act",
            1,
            vec![data("m1"), link(Open, Some(&child), "b")],
        )
        .await;
    streams
        .append_as(
            &agent,
            "act",
            2,
            vec![data("m2"), link(Open, Some(&child), "b")],
        )
        .await;
    streams
        .append_as(&agent, "wf", 1, vec![join_until("b", until), data("r")])
        .await;
    streams.close(&agent).await;
    let order = shape(&streams.follow_to_end(100, 0).await);
    assert_eq!(
        order,
        [
            "m1",
            "EVENT_OPEN:b",
            "KIND_OPENED:b",
            "SUPERSEDED",
            "KIND_DONE:b:withdrawn",
            "m2",
            "EVENT_OPEN:b",
            "KIND_OPENED:b:resumed",
            "c1",
            "c2",
            "c3",
            "KIND_DONE:b",
            "EVENT_JOIN:b",
            "r",
        ]
    );
    // A follower that resumes after each record reads the child between the two OPENs, and it
    // still shows each child record once.
    let one_by_one = shape(&streams.follow_to_end(1, 0).await);
    for record in ["c1", "c2", "c3"] {
        let count = one_by_one.iter().filter(|item| *item == record).count();
        assert_eq!(count, 1, "{record} in {one_by_one:?}");
    }
    assert!(!one_by_one.contains(&"c4".to_string()));
    let pause = one_by_one
        .iter()
        .position(|item| item == "KIND_DONE:b:withdrawn");
    let resume = one_by_one
        .iter()
        .position(|item| item == "KIND_OPENED:b:resumed");
    assert!(pause < resume && pause.is_some(), "{one_by_one:?}");
}

/// Every Workflow is `Running` but `missing`, which Temporal doesn't know.
struct Missing;

#[async_trait::async_trait]
impl OwnerClient for Missing {
    async fn describe(
        &self,
        namespace: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<OwnerDescription, OwnerError> {
        if workflow_id == "missing" {
            return Err(OwnerError::NotFound("missing".to_string()));
        }
        Running.describe(namespace, workflow_id, run_id).await
    }
}

async fn a_follow_resumes_only_on_its_root(streams: Option<Streams>) {
    let Some(streams) = streams else { return };
    let agent = streams.agent_stream();
    streams.append(&agent, 1, vec![data("d1")]).await;
    let first: FollowResponse = streams
        .call(
            "Follow",
            FollowRequest {
                stream: Some(agent),
                ..Default::default()
            },
        )
        .await;
    let failure = streams
        .try_call(
            "Follow",
            FollowRequest {
                stream: Some(streams.workflow_stream("other")),
                state: first.state,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(failure.kind, StreamFailureKind::InvalidArgument as i32);
}

macro_rules! follow_cases {
    ($case:expr; $($name:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                super::$name($case.await).await;
            }
        )*
    };
}

macro_rules! all_follow_cases {
    ($case:expr) => {
        follow_cases!($case;
            follows_in_one_order,
            a_link_past_the_depth_is_skipped,
            a_link_back_to_an_open_stream_is_a_cycle,
            a_missing_child_is_unavailable_after_the_grace,
            a_follow_resumes_only_on_its_root,
            a_stale_open_after_the_join_opens_nothing,
            an_activity_retry_that_writes_the_same_open_resumes_the_segment,
        );
    };
}

mod memory {
    all_follow_cases!(super::on_memory());
}

#[cfg(feature = "redis")]
mod standalone_redis {
    all_follow_cases!(super::on_redis("STREAMS_REDIS_URL", false));
}

#[cfg(feature = "redis")]
mod cluster_redis {
    all_follow_cases!(super::on_redis("STREAMS_REDIS_CLUSTER_URL", true));
}

/// Apache Kvrocks, a test lane with no support claim.
#[cfg(feature = "redis")]
mod kvrocks {
    all_follow_cases!(super::on_redis("STREAMS_KVROCKS_URL", false));
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
    let (agent, child) = (streams.agent_stream(), streams.workflow_stream("child"));
    // The open child's JOIN gates the page, so the follow reads it again up to the gate.
    let records = [
        data("d1"),
        link(Open, Some(&child), "child"),
        link(Join, None, "child"),
        data("d2"),
    ];
    *store.racing.lock().unwrap() = Some(temporalio_streams::proto::StoreAppendRequest {
        chain: Some(temporalio_streams::proto::ChainId {
            namespace: agent.namespace.clone(),
            workflow_id: agent.workflow_id.clone(),
            first_run_id: "run-1".to_string(),
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
    assert!(order.contains(&"EVENT_OPEN:child".to_string()), "{order:?}");
    assert!(!order.contains(&"d2".to_string()), "{order:?}");
}
