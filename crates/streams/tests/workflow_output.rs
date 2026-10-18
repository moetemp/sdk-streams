//! A Workflow's own stream output through the service calls lang makes: the publish before each
//! live completion, the eviction and the Worker shutdown.

mod support;

use prost::Message;
use std::{sync::Arc, time::Duration};
use support::*;
use temporalio_common::protos::temporal::api::enums::v1::WorkflowExecutionStatus;
use temporalio_streams::{
    CONTENT_HASH_KEY, Capabilities, MemoryStore, StreamError, StreamService, StreamStore,
    StreamsOptions, content_hash_text,
    proto::{
        PublishWorkflowTaskRequest, PublishWorkflowTaskResponse, RunEvictedRequest,
        ShutdownWorkerRequest, StreamFailure, StreamFailureKind, StreamRecord,
        WorkflowTerminalCommand,
    },
    workflow_batch_digest,
};

const RUN: &str = "run-1";

fn service_on(store: &Arc<RecordingStore>, owner: &Arc<Owner>) -> StreamService {
    StreamService::new(store.clone(), owner.clone(), StreamsOptions::default())
}

/// The publish of a live activation of task `started` of [RUN], after `completed` tasks and
/// `failed` attempts.
fn publish(
    started: i64,
    failed: u32,
    completed: &[i64],
    values: &[&str],
) -> PublishWorkflowTaskRequest {
    PublishWorkflowTaskRequest {
        namespace: "ns".to_string(),
        worker_id: "worker-1".to_string(),
        task_queue: "q".to_string(),
        workflow_type: "wf".to_string(),
        workflow_id: "wf-id".to_string(),
        run_id: RUN.to_string(),
        task_started_event_id: started,
        failed_attempts: failed,
        completed_started_event_ids: completed.to_vec(),
        records: records(values),
        ..Default::default()
    }
}

fn ending(
    mut request: PublishWorkflowTaskRequest,
    command: WorkflowTerminalCommand,
) -> PublishWorkflowTaskRequest {
    request.terminal_command = command as i32;
    request
}

async fn send(
    service: &StreamService,
    request: PublishWorkflowTaskRequest,
) -> Result<PublishWorkflowTaskResponse, StreamFailure> {
    service
        .call("PublishWorkflowTask", &request.encode_to_vec())
        .await
        .map(|bytes| PublishWorkflowTaskResponse::decode(bytes.as_slice()).unwrap())
}

async fn evict(service: &StreamService, worker_id: &str, ending: bool) {
    let request = RunEvictedRequest {
        worker_id: worker_id.to_string(),
        run_id: RUN.to_string(),
        workflow_execution_ending: ending,
    };
    service
        .call("RunEvicted", &request.encode_to_vec())
        .await
        .unwrap();
}

async fn shutdown(service: &StreamService, worker_id: &str) {
    let request = ShutdownWorkerRequest {
        worker_id: worker_id.to_string(),
    };
    service
        .call("ShutdownWorker", &request.encode_to_vec())
        .await
        .unwrap();
}

#[tokio::test]
async fn a_publish_appends_under_the_attempt_history_gives() {
    let store = Arc::new(RecordingStore::default());
    let service = service_on(&store, &Owner::with(WorkflowExecutionStatus::Running));
    send(&service, publish(3, 0, &[], &["ab", "c"]))
        .await
        .unwrap();

    let [append] = store.appends().try_into().unwrap();
    assert_eq!(
        (append.topic.as_str(), append.producer_id.as_str()),
        (TOPIC, "@run-1")
    );
    assert_eq!((append.attempt, append.sequence), (1, 1));
    let committed = records(&["ab", "c"]);
    assert_eq!(
        append.digest,
        workflow_batch_digest(
            TOPIC,
            committed
                .iter()
                .map(|r| (r.kind, r.content_hash.as_slice()))
        ),
        "the digest never depends on the codec, so a repeat of the batch deduplicates"
    );
    let chain = append.chain.unwrap();
    assert_eq!(
        (
            chain.namespace.as_str(),
            chain.workflow_id.as_str(),
            chain.first_run_id.as_str()
        ),
        ("ns", "wf-id", RUN),
        "a run with no first run id starts its chain"
    );
    for ((stored, committed), sequence) in append.records.iter().zip(&committed).zip(1..) {
        let stored = StreamRecord::decode(stored.as_slice()).unwrap();
        assert_eq!(
            stored.body, committed.body,
            "the body stays as the codec left it"
        );
        assert_eq!((stored.attempt, stored.sequence), (1, sequence));
        assert_eq!(stored.task_started_event_id, 3);
        let expected: &[i64] = if sequence == 1 { &[0] } else { &[] };
        assert_eq!(
            stored.completed_tasks, expected,
            "only a batch's first record names them"
        );
        assert_eq!(
            stored.metadata[CONTENT_HASH_KEY].data,
            content_hash_text(&committed.content_hash).into_bytes()
        );
    }
    assert_eq!(store.read(TOPIC).await, ["encrypted ab", "encrypted c"]);
}

#[tokio::test]
async fn a_task_s_activations_share_its_attempt_and_the_next_task_retires_it() {
    let store = Arc::new(RecordingStore::default());
    let service = service_on(&store, &Owner::with(WorkflowExecutionStatus::Running));
    send(&service, publish(3, 0, &[], &["a"])).await.unwrap();
    send(&service, publish(3, 0, &[], &["b"])).await.unwrap();
    send(&service, publish(8, 0, &[3], &["c"])).await.unwrap();
    shutdown(&service, "worker-1").await;
    let producer = "@run-1".to_string();
    assert_eq!(
        store.append_log(),
        [
            (TOPIC.to_string(), producer.clone(), 1, 1, 1),
            (TOPIC.to_string(), producer.clone(), 1, 2, 1),
            (TOPIC.to_string(), producer.clone(), attempt(3, 1), 1, 1),
        ]
    );
    assert_eq!(
        store.retired(),
        [(TOPIC.to_string(), producer, 1)],
        "the later attempt retires the earlier one, and the shutdown keeps the current one"
    );
    assert_eq!(
        store.read(TOPIC).await,
        ["encrypted a", "encrypted b", "encrypted c"]
    );
}

#[tokio::test]
async fn failed_attempts_raise_the_retry() {
    let store = Arc::new(RecordingStore::default());
    let service = service_on(&store, &Owner::with(WorkflowExecutionStatus::Running));
    send(&service, publish(14, 4, &[3], &["c"])).await.unwrap();
    assert_eq!(store.appends()[0].attempt, attempt(3, 5));
}

#[tokio::test]
async fn two_workers_never_share_a_run_s_counters() {
    let store = Arc::new(RecordingStore::default());
    let service = service_on(&store, &Owner::with(WorkflowExecutionStatus::Running));
    send(&service, publish(3, 0, &[], &["a"])).await.unwrap();
    let mut other = publish(3, 0, &[], &["a"]);
    other.worker_id = "worker-2".to_string();
    send(&service, other).await.unwrap();
    evict(&service, "worker-2", false).await;
    send(&service, publish(3, 0, &[], &["b"])).await.unwrap();
    let sequences: Vec<i64> = store.appends().iter().map(|a| a.sequence).collect();
    assert_eq!(
        sequences,
        [1, 1, 2],
        "the second Worker's eviction leaves the first Worker's sequence alone"
    );
}

#[tokio::test]
async fn an_append_that_may_not_have_landed_is_sent_again_and_a_refusal_is_not() {
    let store = Arc::new(RecordingStore::failing_first([
        StreamError::outcome_unknown("the answer was lost"),
        StreamError::storage("redis is restarting"),
    ]));
    let service = service_on(&store, &Owner::with(WorkflowExecutionStatus::Running));
    send(&service, publish(3, 0, &[], &["a"])).await.unwrap();
    assert_eq!(store.calls(), ["append", "append", "append"]);

    let store = Arc::new(RecordingStore::refusing(StreamError::refused(
        "the ACL denies the key",
    )));
    let service = service_on(&store, &Owner::with(WorkflowExecutionStatus::Running));
    let failure = send(&service, publish(3, 0, &[], &["a"]))
        .await
        .unwrap_err();
    assert_eq!(failure.kind, StreamFailureKind::Refused as i32);
    assert!(
        failure.message.contains("Could not append"),
        "{}",
        failure.message
    );
    assert!(
        failure.message.contains("ACL denies"),
        "{}",
        failure.message
    );
    assert_eq!(store.calls(), ["append"]);
}

#[tokio::test]
async fn a_failed_publish_leaves_the_sequences_where_they_were() {
    let store = Arc::new(RecordingStore::failing_first([StreamError::refused(
        "the ACL denies the key",
    )]));
    let service = service_on(&store, &Owner::with(WorkflowExecutionStatus::Running));
    send(&service, publish(3, 0, &[], &["a"]))
        .await
        .unwrap_err();
    send(&service, publish(3, 0, &[], &["a"])).await.unwrap();
    assert_eq!(
        store.append_log(),
        [(TOPIC.to_string(), "@run-1".to_string(), 1, 1, 1)]
    );
}

#[tokio::test]
async fn a_record_no_writer_stores_is_refused_before_any_store_call() {
    let store = Arc::new(RecordingStore::default());
    let service = service_on(&store, &Owner::with(WorkflowExecutionStatus::Running));
    let mut request = publish(3, 0, &[], &["a"]);
    request.records[0].body = None;
    let failure = send(&service, request).await.unwrap_err();
    assert_eq!(failure.kind, StreamFailureKind::InvalidArgument as i32);
    assert!(failure.message.contains("record 0 is DATA without a body"));
    assert!(store.calls().is_empty());
}

#[tokio::test]
async fn a_closed_topic_drops_its_records_and_the_other_topics_go_on() {
    let store = Arc::new(RecordingStore::default());
    let service = service_on(&store, &Owner::with(WorkflowExecutionStatus::Running));
    send(&service, publish(3, 0, &[], &["a"])).await.unwrap();
    let chain = store.appends()[0].chain.clone().unwrap();
    temporalio_streams::StreamStore::close_topic(&*store, &chain, TOPIC)
        .await
        .unwrap();
    let mut request = publish(3, 0, &[], &["b"]);
    request.records.extend(records_on("other", &["c"]));
    let response = send(&service, request).await.unwrap();
    assert_eq!(response.dropped_topics, [TOPIC]);
    assert_eq!(store.read("other").await, ["encrypted c"]);
}

/// The started event id of the task whose live completion [chain_closed] publishes last.
const LAST_TASK: i64 = 8;

/// Publishes a run's last live completion with `command` in the task that started at
/// [LAST_TASK], evicts it, and answers whether the chain closed. The run's History shows
/// `status` and a close by the task that started at `closing_task`.
async fn chain_closed(
    command: WorkflowTerminalCommand,
    status: WorkflowExecutionStatus,
    closing_task: Option<i64>,
    eviction_ends_run: bool,
    cron: bool,
    has_retry_policy: bool,
) -> bool {
    let store = Arc::new(RecordingStore::default());
    let owner = Owner::closed_by(status, closing_task);
    let service = service_on(&store, &owner);
    send(&service, publish(3, 0, &[], &["a"])).await.unwrap();
    let mut last = ending(publish(LAST_TASK, 0, &[3], &[]), command);
    last.cron = cron;
    last.has_retry_policy = has_retry_policy;
    send(&service, last).await.unwrap();
    evict(&service, "worker-1", eviction_ends_run).await;
    shutdown(&service, "worker-1").await;
    store.calls().contains(&"close_chain")
}

#[tokio::test]
async fn the_chain_closes_only_when_history_shows_this_completion_closed_the_run() {
    use WorkflowExecutionStatus as S;
    use WorkflowTerminalCommand as C;
    let ours = Some(LAST_TASK);
    assert!(chain_closed(C::Complete, S::Completed, ours, true, false, false).await);
    assert!(chain_closed(C::Fail, S::Failed, ours, true, false, false).await);
    assert!(chain_closed(C::Cancel, S::Canceled, ours, true, false, false).await);
    assert!(
        !chain_closed(C::Complete, S::Completed, Some(12), true, false, false).await,
        "a later task closed the run"
    );
    assert!(
        !chain_closed(C::Complete, S::Completed, None, true, false, false).await,
        "History shows no task before the close"
    );
    assert!(
        !chain_closed(C::Complete, S::Terminated, ours, true, false, false).await,
        "the close does not match the command"
    );
    assert!(!chain_closed(C::Complete, S::Failed, ours, true, false, false).await);
    assert!(!chain_closed(C::Fail, S::TimedOut, ours, true, false, false).await);
    assert!(
        !chain_closed(C::Complete, S::Running, ours, true, false, false).await,
        "a rejected completion leaves the run open"
    );
    assert!(!chain_closed(C::Complete, S::ContinuedAsNew, ours, true, false, false).await);
    assert!(!chain_closed(C::Complete, S::Completed, ours, false, false, false).await);
    assert!(!chain_closed(C::Complete, S::Completed, ours, true, true, false).await);
    assert!(!chain_closed(C::Fail, S::Failed, ours, true, false, true).await);
    assert!(
        !chain_closed(
            C::ContinueAsNew,
            S::ContinuedAsNew,
            ours,
            true,
            false,
            false
        )
        .await
    );
    assert!(!chain_closed(C::Unspecified, S::Completed, ours, true, false, false).await);
}

#[tokio::test]
async fn the_close_uses_the_attempt_and_run_of_the_terminal_completion() {
    let store = Arc::new(RecordingStore::default());
    let owner = Owner::closed_by(WorkflowExecutionStatus::Completed, Some(3));
    let service = service_on(&store, &owner);
    send(
        &service,
        ending(
            publish(3, 0, &[], &["a"]),
            WorkflowTerminalCommand::Complete,
        ),
    )
    .await
    .unwrap();
    evict(&service, "worker-1", true).await;
    shutdown(&service, "worker-1").await;
    assert_eq!(
        *owner.asked.lock(),
        [("ns".to_string(), "wf-id".to_string(), RUN.to_string())]
    );
    assert_eq!(store.calls(), ["append", "list_topics", "close_chain"]);
}

#[tokio::test]
async fn a_run_that_wrote_no_stream_closes_no_chain_unless_the_store_cannot_list() {
    for listing in [true, false] {
        let store = Arc::new(RecordingStore {
            inner: MemoryStore::with_capabilities(Capabilities {
                topic_listing: listing,
                ..MemoryStore::default().capabilities()
            }),
            ..Default::default()
        });
        let owner = Owner::closed_by(WorkflowExecutionStatus::Completed, Some(3));
        let service = service_on(&store, &owner);
        let last = ending(publish(3, 0, &[], &[]), WorkflowTerminalCommand::Complete);
        send(&service, last).await.unwrap();
        evict(&service, "worker-1", true).await;
        shutdown(&service, "worker-1").await;
        assert_eq!(owner.asked.lock().is_empty(), listing, "{listing}");
        assert_eq!(
            store.calls().contains(&"close_chain"),
            !listing,
            "{listing}"
        );
    }
}

#[tokio::test]
async fn an_eviction_of_a_run_with_no_publish_does_nothing() {
    let store = Arc::new(RecordingStore::default());
    let owner = Owner::with(WorkflowExecutionStatus::Completed);
    let service = service_on(&store, &owner);
    evict(&service, "worker-1", true).await;
    shutdown(&service, "worker-1").await;
    assert!(store.calls().is_empty());
    assert!(owner.asked.lock().is_empty());
}

#[tokio::test]
async fn a_publish_does_not_wait_for_the_retire_and_shutdown_does() {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let store = Arc::new(RecordingStore {
        retire_gate: Some(gate.clone()),
        ..Default::default()
    });
    let service = service_on(&store, &Owner::with(WorkflowExecutionStatus::Running));
    send(&service, publish(3, 0, &[], &["a"])).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        send(&service, publish(8, 0, &[3], &["b"])),
    )
    .await
    .expect("the publish returns while the retire waits")
    .unwrap();
    assert!(store.retired().is_empty());
    let shutdown = shutdown(&service, "worker-1");
    tokio::pin!(shutdown);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut shutdown)
            .await
            .is_err(),
        "shutdown waits for the retire"
    );
    gate.add_permits(10);
    shutdown.await;
    assert_eq!(store.retired().len(), 1);
}

#[tokio::test]
async fn a_shutdown_without_the_eviction_closes_a_chain_the_run_ended() {
    for (status, closes) in [
        (WorkflowExecutionStatus::Completed, true),
        (WorkflowExecutionStatus::Running, false),
    ] {
        let store = Arc::new(RecordingStore::default());
        let service = service_on(&store, &Owner::closed_by(status, Some(3)));
        let last = ending(
            publish(3, 0, &[], &["a"]),
            WorkflowTerminalCommand::Complete,
        );
        send(&service, last).await.unwrap();
        shutdown(&service, "worker-1").await;
        assert_eq!(store.calls().contains(&"close_chain"), closes, "{status:?}");
    }
}

#[tokio::test]
async fn an_eviction_returns_before_the_history_read_and_shutdown_waits_for_it() {
    let store = Arc::new(RecordingStore::default());
    let owner = Owner::closed_by(WorkflowExecutionStatus::Completed, Some(3));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    *owner.read_gate.lock() = Some(gate.clone());
    let service = service_on(&store, &owner);
    let last = ending(
        publish(3, 0, &[], &["a"]),
        WorkflowTerminalCommand::Complete,
    );
    send(&service, last).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), evict(&service, "worker-1", true))
        .await
        .expect("the eviction returns while the History read waits");
    let shutdown = shutdown(&service, "worker-1");
    tokio::pin!(shutdown);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut shutdown)
            .await
            .is_err(),
        "shutdown waits for the History read and the close"
    );
    assert!(!store.calls().contains(&"close_chain"));
    gate.add_permits(1);
    shutdown.await;
    assert_eq!(store.calls(), ["append", "list_topics", "close_chain"]);
}
