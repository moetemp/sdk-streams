//! A Workflow's stream output around Core's real activations, through Core's mock Worker. The
//! test plays lang. It publishes each live activation's records before it completes the
//! activation, and reports each eviction and the Worker shutdown to the service.

mod support;

use assert_matches::assert_matches;
use futures_util::FutureExt;
use parking_lot::Mutex;
use prost::Message;
use std::{collections::HashMap, sync::Arc, time::Duration};
use support::*;
use temporalio_common::{
    protos::{
        coresdk::{
            ActivityTaskCompletion,
            activity_result::{ActivityExecutionResult, ActivityResolution, activity_resolution},
            activity_task::activity_task,
            workflow_activation::{
                ResolveActivity, WorkflowActivation, WorkflowActivationJob,
                remove_from_cache::EvictionReason, workflow_activation_job,
            },
            workflow_commands::{
                ActivityCancellationType, CompleteWorkflowExecution,
                ContinueAsNewWorkflowExecution, FailWorkflowExecution, RequestCancelActivity,
                RequestCancelLocalActivity, ScheduleActivity, workflow_command,
            },
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::{Payload, RetryPolicy},
            enums::v1::{EventType, TimeoutType, WorkflowExecutionStatus, WorkflowTaskFailedCause},
            failure::v1::{Failure, failure::FailureInfo},
            history::v1::{
                WorkflowExecutionStartedEventAttributes, WorkflowTaskTimedOutEventAttributes,
                history_event,
            },
            query::v1::WorkflowQuery,
            workflowservice::v1::RespondWorkflowTaskCompletedResponse,
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_sdk_core::{
    PollError, Worker, advance_fut,
    replay::{TestHistoryBuilder, canned_histories, default_act_sched, default_wes_attribs},
    test_help::{
        MockPollCfg, MocksHolder, NAMESPACE, ResponseType, build_mock_pollers, hist_to_poll_resp,
        mock_worker, query_ok, schedule_local_activity_cmd, start_timer_cmd,
    },
};
use temporalio_streams::{
    STREAM_OWNER_HEADER, StreamError, StreamService, StreamStore, StreamsOptions,
    proto::{
        ChainId, OutputRecord, PublishWorkflowTaskRequest, ResolvedActivityOutcome,
        ResolvedOwnerActivity, StoreAppendRequest, StoreReadRequest, StoredClose, StreamOwnerKind,
        StreamRecord, StreamRecordKind, WorkflowTerminalCommand,
    },
};
use tokio::sync::Notify;

const WORKER_ID: &str = "worker-1";
const WORKFLOW_ID: &str = "fake_wf_id";

/// What lang learns from a run's `InitializeWorkflow`.
#[derive(Clone, Default)]
struct RunFacts {
    first_execution_run_id: String,
    cron: bool,
    has_retry_policy: bool,
}

/// Core's mock Worker and lang's side of the stream service.
struct Lang {
    worker: Worker,
    service: Arc<StreamService>,
    runs: Mutex<HashMap<String, RunFacts>>,
    /// The Activities lang scheduled with [STREAM_OWNER_HEADER], by run and sequence number.
    owner_activities: Mutex<HashMap<(String, u32), String>>,
}

impl Lang {
    fn new(mut mock: MocksHolder, store: Arc<RecordingStore>, owner: Arc<Owner>) -> Self {
        mock.worker_cfg(|w| {
            w.task_types = WorkerTaskTypes {
                enable_local_activities: true,
                ..WorkerTaskTypes::workflow_only()
            };
            w.max_cached_workflows = 1;
        });
        Self {
            worker: mock_worker(mock),
            service: Arc::new(StreamService::new(store, owner, StreamsOptions::default())),
            runs: Default::default(),
            owner_activities: Default::default(),
        }
    }

    /// The next activation. Lang keeps the run facts of a run's first activation.
    async fn poll(&self) -> WorkflowActivation {
        let activation = self.worker.poll_workflow_activation().await.unwrap();
        self.learn(&activation);
        activation
    }

    fn learn(&self, activation: &WorkflowActivation) {
        for job in &activation.jobs {
            if let Some(workflow_activation_job::Variant::InitializeWorkflow(init)) = &job.variant {
                self.runs.lock().insert(
                    activation.run_id.clone(),
                    RunFacts {
                        first_execution_run_id: init.first_execution_run_id.clone(),
                        cron: !init.cron_schedule.is_empty(),
                        has_retry_policy: init.retry_policy.is_some(),
                    },
                );
            }
        }
    }

    /// Completes `activation` with `commands`. A live activation with records, a terminal
    /// command or an owner Activity's resolution publishes first, and a failed publish fails the
    /// activation instead.
    async fn complete(
        &self,
        activation: &WorkflowActivation,
        records: Vec<OutputRecord>,
        commands: Vec<workflow_command::Variant>,
    ) {
        for command in &commands {
            if let workflow_command::Variant::ScheduleActivity(schedule) = command
                && schedule.headers.contains_key(STREAM_OWNER_HEADER)
            {
                self.owner_activities.lock().insert(
                    (activation.run_id.clone(), schedule.seq),
                    schedule.activity_id.clone(),
                );
            }
        }
        let terminal = commands.iter().find_map(terminal_command);
        let resolved = self.resolved_owners(activation);
        let publishes = !records.is_empty() || terminal.is_some() || !resolved.is_empty();
        let completion = if !activation.is_replaying && publishes {
            match self.publish(activation, records, terminal, resolved).await {
                Ok(()) => {
                    WorkflowActivationCompletion::from_cmds(activation.run_id.clone(), commands)
                }
                Err(message) => WorkflowActivationCompletion::fail(
                    activation.run_id.clone(),
                    Failure {
                        message,
                        ..Default::default()
                    },
                    Some(WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure),
                ),
            }
        } else {
            WorkflowActivationCompletion::from_cmds(activation.run_id.clone(), commands)
        };
        self.worker
            .complete_workflow_activation(completion)
            .await
            .unwrap();
    }

    /// The owner Activities that `activation` resolves, as the server gave their outcomes.
    fn resolved_owners(&self, activation: &WorkflowActivation) -> Vec<ResolvedOwnerActivity> {
        let owners = self.owner_activities.lock();
        activation
            .jobs
            .iter()
            .filter_map(|job| match &job.variant {
                Some(workflow_activation_job::Variant::ResolveActivity(resolve))
                    if !resolve.is_local =>
                {
                    let activity_id = owners.get(&(activation.run_id.clone(), resolve.seq))?;
                    let status = resolve.result.as_ref()?.status.as_ref()?;
                    let (outcome, failure) = match status {
                        activity_resolution::Status::Completed(_) => {
                            (ResolvedActivityOutcome::Completed, None)
                        }
                        activity_resolution::Status::Failed(failed) => {
                            (ResolvedActivityOutcome::Failed, failed.failure.clone())
                        }
                        activity_resolution::Status::Cancelled(cancelled) => (
                            ResolvedActivityOutcome::Cancelled,
                            cancelled.failure.clone(),
                        ),
                        activity_resolution::Status::Backoff(_) => return None,
                    };
                    Some(ResolvedOwnerActivity {
                        activity_id: activity_id.clone(),
                        outcome: outcome as i32,
                        failure,
                    })
                }
                _ => None,
            })
            .collect()
    }

    async fn publish(
        &self,
        activation: &WorkflowActivation,
        records: Vec<OutputRecord>,
        terminal: Option<WorkflowTerminalCommand>,
        resolved_owner_activities: Vec<ResolvedOwnerActivity>,
    ) -> Result<(), String> {
        let facts = self
            .runs
            .lock()
            .get(&activation.run_id)
            .cloned()
            .unwrap_or_default();
        let task = activation.workflow_task.clone().unwrap();
        let request = PublishWorkflowTaskRequest {
            namespace: NAMESPACE.to_string(),
            worker_id: WORKER_ID.to_string(),
            task_queue: "q".to_string(),
            workflow_type: "wf".to_string(),
            workflow_id: WORKFLOW_ID.to_string(),
            run_id: activation.run_id.clone(),
            first_execution_run_id: facts.first_execution_run_id,
            cron: facts.cron,
            has_retry_policy: facts.has_retry_policy,
            task_started_event_id: task.started_event_id,
            failed_attempts: task.failed_attempts,
            completed_started_event_ids: task.completed_started_event_ids,
            records,
            terminal_command: terminal.map(|t| t as i32).unwrap_or_default(),
            resolved_owner_activities,
        };
        self.service
            .call("PublishWorkflowTask", &request.encode_to_vec())
            .await
            .map(|_| ())
            .map_err(|failure| failure.message)
    }

    /// Reports the eviction `activation` holds, then completes it.
    async fn complete_eviction(&self, activation: &WorkflowActivation) {
        let reason = activation.eviction_reason().expect("an eviction");
        self.service
            .run_evicted(
                WORKER_ID,
                &activation.run_id,
                reason == EvictionReason::WorkflowExecutionEnding,
            )
            .await;
        self.worker
            .complete_workflow_activation(WorkflowActivationCompletion::empty(
                activation.run_id.clone(),
            ))
            .await
            .unwrap();
    }

    /// Shuts the Worker down as lang does, reporting each eviction on the way, then the Worker
    /// shutdown.
    async fn shutdown(self) {
        self.worker.initiate_shutdown();
        tokio::join!(
            async {
                assert_matches!(
                    self.worker.poll_activity_task().await.unwrap_err(),
                    PollError::ShutDown
                );
            },
            async {
                loop {
                    match self.worker.poll_workflow_activation().await {
                        Err(PollError::ShutDown) => break,
                        Ok(a) if a.is_only_eviction() => self.complete_eviction(&a).await,
                        other => panic!("Unexpected activation while draining: {other:?}"),
                    }
                }
            }
        );
        self.worker.shutdown().await;
        self.worker.finalize_shutdown().await;
        self.service.shutdown_worker(WORKER_ID).await;
    }

    /// Shuts down a Worker whose task failed, so it holds no evictions to drain.
    async fn shutdown_failed(self) {
        self.worker.shutdown().await;
        self.worker.finalize_shutdown().await;
        self.service.shutdown_worker(WORKER_ID).await;
    }
}

fn terminal_command(command: &workflow_command::Variant) -> Option<WorkflowTerminalCommand> {
    match command {
        workflow_command::Variant::CompleteWorkflowExecution(_) => {
            Some(WorkflowTerminalCommand::Complete)
        }
        workflow_command::Variant::FailWorkflowExecution(_) => Some(WorkflowTerminalCommand::Fail),
        workflow_command::Variant::CancelWorkflowExecution(_) => {
            Some(WorkflowTerminalCommand::Cancel)
        }
        workflow_command::Variant::ContinueAsNewWorkflowExecution(_) => {
            Some(WorkflowTerminalCommand::ContinueAsNew)
        }
        _ => None,
    }
}

fn complete_workflow() -> workflow_command::Variant {
    CompleteWorkflowExecution::default().into()
}

fn fail_workflow() -> workflow_command::Variant {
    FailWorkflowExecution {
        failure: Some(Failure::default()),
    }
    .into()
}

fn cancel_workflow() -> workflow_command::Variant {
    workflow_command::Variant::CancelWorkflowExecution(Default::default())
}

fn continue_as_new() -> workflow_command::Variant {
    ContinueAsNewWorkflowExecution {
        workflow_type: "next".to_string(),
        ..Default::default()
    }
    .into()
}

fn schedule_local_activity(seq: u32) -> workflow_command::Variant {
    schedule_local_activity_cmd(
        seq,
        &seq.to_string(),
        ActivityCancellationType::TryCancel,
        Duration::from_secs(10),
    )
}

/// Start, a task that starts a timer, the timer firing, and the next task.
fn timer_history() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    t
}

/// A mock on `history` that records how many appends `store` held when each completion was sent.
fn mock_appending_to(
    history: TestHistoryBuilder,
    batches: Vec<ResponseType>,
    store: Arc<RecordingStore>,
    appended_at_completion: Arc<Mutex<Vec<usize>>>,
) -> MocksHolder {
    let mut mock_cfg = MockPollCfg::from_resps(history, batches);
    mock_cfg.completion_mock_fn = Some(Box::new(move |_| {
        appended_at_completion.lock().push(store.appends().len());
        Ok(RespondWorkflowTaskCompletedResponse::default())
    }));
    build_mock_pollers(mock_cfg)
}

fn lang_appending_to(
    history: TestHistoryBuilder,
    batches: Vec<ResponseType>,
    store: &Arc<RecordingStore>,
    appended_at_completion: Arc<Mutex<Vec<usize>>>,
) -> Lang {
    let mock = mock_appending_to(history, batches, store.clone(), appended_at_completion);
    // The task that started at event 8 closes the run in the histories that complete in their
    // second task.
    Lang::new(
        mock,
        store.clone(),
        Owner::closed_by(WorkflowExecutionStatus::Completed, Some(8)),
    )
}

/// Accepts only a failure whose message holds each of `parts`, and signals `failed` when one comes.
fn failing_with(mock_cfg: &mut MockPollCfg, parts: &'static [&'static str], failed: Arc<Notify>) {
    mock_cfg.num_expected_fails = 1;
    mock_cfg.expect_fail_wft_matcher = Box::new(move |_, _, failure| {
        let matches = failure
            .as_ref()
            .is_some_and(|f| parts.iter().all(|part| f.message.contains(part)));
        if matches {
            failed.notify_one();
        }
        matches
    });
}

/// Waits a bounded time for the expected Workflow Task failure. Without it, a test whose failure
/// never comes waits forever in shutdown instead of failing.
async fn failed_within_deadline(failed: &Notify) {
    tokio::time::timeout(Duration::from_secs(10), failed.notified())
        .await
        .expect("the expected Workflow Task failure never came");
}

/// A Worker whose one completion must fail its Workflow Task with a message holding `parts`.
fn lang_failing_with(
    store: &Arc<RecordingStore>,
    parts: &'static [&'static str],
) -> (Lang, Arc<Notify>) {
    let failed = Arc::new(Notify::new());
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let mut mock_cfg = MockPollCfg::from_resps(t, [1]);
    mock_cfg.num_expected_completions = Some(0.into());
    failing_with(&mut mock_cfg, parts, failed.clone());
    let lang = Lang::new(
        build_mock_pollers(mock_cfg),
        store.clone(),
        Owner::with(WorkflowExecutionStatus::Running),
    );
    (lang, failed)
}

/// Runs a task that publishes `first` and starts a timer, then a task that publishes `second` and
/// completes the Workflow when the timer fires. Returns the run id.
async fn publish_in_two_tasks(lang: Lang, first: &[&str], second: &[&str]) -> String {
    let activation = lang.poll().await;
    let run_id = activation.run_id.clone();
    lang.complete(
        &activation,
        records(first),
        vec![start_timer_cmd(1, Duration::from_secs(10))],
    )
    .await;
    let fired = lang.poll().await;
    lang.complete(&fired, records(second), vec![complete_workflow()])
        .await;
    lang.shutdown().await;
    run_id
}

#[tokio::test]
async fn a_publish_is_in_the_store_before_its_completion_is_sent() {
    let store = Arc::new(RecordingStore::default());
    let appended_at_completion: Arc<Mutex<Vec<usize>>> = Default::default();
    let lang = lang_appending_to(
        timer_history(),
        vec![1.into(), 2.into()],
        &store,
        appended_at_completion.clone(),
    );
    let run_id = publish_in_two_tasks(lang, &["ab", "c"], &[]).await;
    assert_eq!(
        appended_at_completion.lock()[0],
        1,
        "the records are in the store before the completion goes out"
    );
    let [append] = store.appends().try_into().unwrap();
    assert_eq!(append.producer_id, format!("@{run_id}"));
    assert_eq!((append.attempt, append.sequence), (1, 1));
    let chain = append.chain.unwrap();
    assert_eq!(
        (chain.workflow_id.as_str(), chain.first_run_id.as_str()),
        (WORKFLOW_ID, run_id.as_str())
    );
    assert_eq!(store.read(TOPIC).await, ["encrypted ab", "encrypted c"]);
}

#[tokio::test]
async fn an_append_that_may_not_have_landed_is_sent_again() {
    let store = Arc::new(RecordingStore::failing_first([
        StreamError::outcome_unknown("the answer was lost"),
        StreamError::storage("redis is restarting"),
    ]));
    let lang = lang_appending_to(
        timer_history(),
        vec![1.into(), 2.into()],
        &store,
        Default::default(),
    );
    publish_in_two_tasks(lang, &["a"], &[]).await;
    assert_eq!(
        store.calls(),
        [
            "append",
            "append",
            "append",
            "retire_producer",
            "close_chain"
        ]
    );
    assert_eq!(store.read(TOPIC).await, ["encrypted a"]);
}

#[tokio::test]
async fn a_store_that_refuses_the_append_fails_the_task_and_sends_no_completion() {
    let store = Arc::new(RecordingStore::refusing(StreamError::refused(
        "the ACL denies the key",
    )));
    let (lang, failed) = lang_failing_with(&store, &["Could not append", "ACL denies"]);
    let first = lang.poll().await;
    lang.complete(
        &first,
        records(&["a"]),
        vec![start_timer_cmd(1, Duration::from_secs(10))],
    )
    .await;
    failed_within_deadline(&failed).await;
    lang.shutdown_failed().await;
    assert_eq!(store.calls(), ["append"], "a refusal is not sent again");
}

#[tokio::test]
async fn a_record_above_the_stores_record_limit_fails_the_task() {
    let store = Arc::new(RecordingStore {
        inner: temporalio_streams::MemoryStore::with_capabilities(
            temporalio_streams::Capabilities {
                max_record_bytes: 64,
                ..temporalio_streams::MemoryStore::new().capabilities()
            },
        ),
        ..Default::default()
    });
    let (lang, failed) = lang_failing_with(&store, &["record limit of 64"]);
    let first = lang.poll().await;
    lang.complete(
        &first,
        records(&["a"]),
        vec![start_timer_cmd(1, Duration::from_secs(10))],
    )
    .await;
    failed_within_deadline(&failed).await;
    lang.shutdown_failed().await;
    assert!(store.appends().is_empty());
}

#[tokio::test]
async fn a_record_no_writer_stores_fails_the_task_with_its_reason() {
    let store = Arc::new(RecordingStore::default());
    let (lang, failed) = lang_failing_with(&store, &["record 0 is DATA without a body"]);
    let first = lang.poll().await;
    let mut unencoded = records(&["a"]);
    unencoded[0].body = None;
    lang.complete(&first, unencoded, vec![]).await;
    failed_within_deadline(&failed).await;
    lang.shutdown_failed().await;
    assert!(store.calls().is_empty());
}

/// Runs the local activity lang just scheduled and returns the activation that resolves it.
async fn run_local_activity(lang: &Lang) -> WorkflowActivation {
    let task = lang.worker.poll_activity_task().await.unwrap();
    lang.worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: task.task_token,
            result: Some(ActivityExecutionResult::ok(Payload {
                data: b"done".to_vec(),
                ..Default::default()
            })),
        })
        .await
        .unwrap();
    lang.poll().await
}

fn only_resolves_local_activity(activation: &WorkflowActivation, seq: u32) -> bool {
    matches!(
        activation.jobs.as_slice(),
        [job] if matches!(
            &job.variant,
            Some(workflow_activation_job::Variant::ResolveActivity(r)) if r.seq == seq
        )
    )
}

#[tokio::test]
async fn each_publish_around_a_local_activity_is_visible_before_the_task_completes() {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_workflow_task_scheduled_and_started();
    let store = Arc::new(RecordingStore::default());
    let appended_at_completion: Arc<Mutex<Vec<usize>>> = Default::default();
    let lang = lang_appending_to(
        history,
        vec![1.into()],
        &store,
        appended_at_completion.clone(),
    );
    let first = lang.poll().await;
    let run_id = first.run_id.clone();
    lang.complete(&first, records(&["a"]), vec![schedule_local_activity(1)])
        .await;
    // The task is still open while the local activity runs, and a reader already sees the records.
    assert_eq!(store.read(TOPIC).await, ["encrypted a"]);
    let resolved = run_local_activity(&lang).await;
    assert!(only_resolves_local_activity(&resolved, 1));
    lang.complete(&resolved, records(&["b"]), vec![complete_workflow()])
        .await;
    lang.shutdown().await;

    assert_eq!(*appended_at_completion.lock(), [2]);
    let producer = format!("@{run_id}");
    assert_eq!(
        store.append_log()[..2],
        [
            (TOPIC.to_string(), producer.clone(), 1, 1, 1),
            (TOPIC.to_string(), producer, 1, 2, 1),
        ],
        "one task's activations share its attempt, and the sequence goes on"
    );
}

#[tokio::test]
async fn replay_makes_no_store_call() {
    let store = Arc::new(RecordingStore::default());
    let lang = lang_appending_to(timer_history(), vec![2.into()], &store, Default::default());
    let first = lang.poll().await;
    assert!(first.is_replaying);
    lang.complete(
        &first,
        records(&["a"]),
        vec![start_timer_cmd(1, Duration::from_secs(10))],
    )
    .await;
    let fired = lang.poll().await;
    lang.complete(&fired, vec![], vec![complete_workflow()])
        .await;
    lang.shutdown().await;
    // The live task that completes the run closes its chain, and nothing else reaches the store.
    assert_eq!(store.calls(), ["close_chain"]);
}

/// The owner's Worker closes the chain after a live completion that ends it, once the server shows
/// the run closed, and leaves a chain that can go on to the producers' owner checks.
#[rstest::rstest]
#[case::completed(
    complete_workflow(),
    false,
    false,
    WorkflowExecutionStatus::Completed,
    true
)]
#[case::failed(fail_workflow(), false, false, WorkflowExecutionStatus::Failed, true)]
#[case::canceled(
    cancel_workflow(),
    false,
    false,
    WorkflowExecutionStatus::Canceled,
    true
)]
#[case::failed_with_retries(fail_workflow(), true, false, WorkflowExecutionStatus::Failed, false)]
#[case::completed_with_cron(
    complete_workflow(),
    false,
    true,
    WorkflowExecutionStatus::Completed,
    false
)]
#[case::continued_as_new(
    continue_as_new(),
    false,
    false,
    WorkflowExecutionStatus::ContinuedAsNew,
    false
)]
#[tokio::test]
async fn the_owner_closes_a_chain_the_run_ended(
    #[case] command: workflow_command::Variant,
    #[case] retries: bool,
    #[case] cron: bool,
    #[case] status: WorkflowExecutionStatus,
    #[case] closes: bool,
) {
    let store = Arc::new(RecordingStore::default());
    let mut t = TestHistoryBuilder::default();
    t.add(WorkflowExecutionStartedEventAttributes {
        retry_policy: retries.then(RetryPolicy::default),
        cron_schedule: if cron {
            "@hourly".to_string()
        } else {
            String::new()
        },
        ..default_wes_attribs()
    });
    t.add_workflow_task_scheduled_and_started();
    let mock = mock_appending_to(
        t,
        vec![ResponseType::AllHistory],
        store.clone(),
        Default::default(),
    );
    let lang = Lang::new(mock, store.clone(), Owner::closed_by(status, Some(3)));
    let activation = lang.poll().await;
    lang.complete(&activation, vec![], vec![command]).await;
    lang.shutdown().await;
    let expected: &[&str] = if closes { &["close_chain"] } else { &[] };
    assert_eq!(store.calls(), expected);
}

#[tokio::test]
async fn the_next_task_moves_the_prefix_and_starts_the_sequence_again() {
    let store = Arc::new(RecordingStore::default());
    let lang = lang_appending_to(
        timer_history(),
        vec![1.into(), 2.into()],
        &store,
        Default::default(),
    );
    let run_id = publish_in_two_tasks(lang, &["a", "b"], &["c"]).await;
    let producer = format!("@{run_id}");
    assert_eq!(
        store.append_log(),
        [
            (TOPIC.to_string(), producer.clone(), 1, 1, 2),
            // The first task started at event 3.
            (TOPIC.to_string(), producer.clone(), attempt(3, 1), 1, 1),
        ]
    );
    assert_eq!(
        store.retired(),
        [(TOPIC.to_string(), producer, 1)],
        "the second task's publish retires the first task's session, and keeps its own"
    );
    assert_eq!(
        store.read(TOPIC).await,
        ["encrypted a", "encrypted b", "encrypted c"],
        "a later task goes on with no supersession"
    );
}

#[tokio::test]
async fn a_completion_does_not_wait_for_the_retire_and_shutdown_does() {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let store = Arc::new(RecordingStore {
        retire_gate: Some(gate.clone()),
        ..Default::default()
    });
    let lang = lang_appending_to(
        timer_history(),
        vec![1.into(), 2.into()],
        &store,
        Default::default(),
    );
    let activation = lang.poll().await;
    lang.complete(
        &activation,
        records(&["a"]),
        vec![start_timer_cmd(1, Duration::from_secs(10))],
    )
    .await;
    let fired = lang.poll().await;
    tokio::time::timeout(
        Duration::from_secs(5),
        lang.complete(&fired, records(&["b"]), vec![complete_workflow()]),
    )
    .await
    .expect("the completion returns while the retire waits");
    assert!(store.retired().is_empty());
    let shutdown = lang.shutdown();
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

/// Replays the first task of `history`, then publishes `["c"]` live in the task after it, and
/// answers each append's attempt and sequence.
async fn publish_after_replay(history: TestHistoryBuilder) -> Vec<(i64, i64)> {
    let store = Arc::new(RecordingStore::default());
    let lang = lang_appending_to(history, vec![2.into()], &store, Default::default());
    let first = lang.poll().await;
    lang.complete(
        &first,
        vec![],
        vec![start_timer_cmd(1, Duration::from_secs(10))],
    )
    .await;
    let fired = lang.poll().await;
    assert!(!fired.is_replaying);
    lang.complete(&fired, records(&["c"]), vec![complete_workflow()])
        .await;
    lang.shutdown().await;
    store
        .appends()
        .into_iter()
        .map(|a| (a.attempt, a.sequence))
        .collect()
}

/// A first task that starts a timer, the timer firing, whatever `between` adds, then the task
/// that publishes, scheduled at `attempt`.
fn history_with_reruns(
    between: impl FnOnce(&mut TestHistoryBuilder),
    attempt: i32,
) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started, "1".to_string());
    between(&mut t);
    t.add_workflow_task_scheduled();
    let scheduled = t.current_event_id();
    t.modify_event(scheduled, |e| {
        if let Some(history_event::Attributes::WorkflowTaskScheduledEventAttributes(a)) =
            e.attributes.as_mut()
        {
            a.attempt = attempt;
        }
    });
    t.add_workflow_task_started();
    t
}

#[tokio::test]
async fn a_task_after_failed_attempts_publishes_under_a_higher_retry() {
    // An unhandled command, a crash, then two attempts the server writes no event for.
    let history = history_with_reruns(
        |t| {
            t.add_workflow_task_scheduled_and_started();
            t.add_workflow_task_failed_with_failure(
                WorkflowTaskFailedCause::UnhandledCommand,
                Default::default(),
            );
            t.add_workflow_task_scheduled_and_started();
            t.add_workflow_task_timed_out();
        },
        4,
    );
    assert_eq!(
        publish_after_replay(history).await,
        [(attempt(3, 5), 1)],
        "1 + an unhandled command + a timeout + the 2 unwritten failures before attempt 4"
    );
}

#[tokio::test]
async fn a_schedule_to_start_timeout_keeps_the_retry() {
    // No Worker ran the task that timed out, so it published nothing.
    let history = history_with_reruns(
        |t| {
            t.add_workflow_task_scheduled();
            let scheduled = t.current_event_id();
            t.add(WorkflowTaskTimedOutEventAttributes {
                scheduled_event_id: scheduled,
                timeout_type: TimeoutType::ScheduleToStart as i32,
                ..Default::default()
            });
        },
        1,
    );
    assert_eq!(publish_after_replay(history).await, [(attempt(3, 1), 1)]);
}

/// The first task's poll response of `history`, from its first `events` events.
fn first_task_of(history: &TestHistoryBuilder, events: usize) -> ResponseType {
    let first = TestHistoryBuilder::from_history(
        history.get_full_history_info().unwrap().events()[..events].to_vec(),
    );
    ResponseType::Raw(hist_to_poll_resp(&first, WORKFLOW_ID, 1.into()).resp)
}

/// A history whose first task the server refuses with an unhandled command, then a signal and a
/// task that reruns it.
fn refused_then_rerun() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::UnhandledCommand,
        Default::default(),
    );
    t.add_we_signaled("go", vec![]);
    t.add_workflow_task_scheduled_and_started();
    t
}

/// A mock that refuses the first completion with an unhandled command and takes the rest.
fn refusing_first_completion(mock_cfg: &mut MockPollCfg, completions: Arc<Mutex<usize>>) {
    mock_cfg.completion_mock_fn = Some(Box::new(move |_| {
        let mut count = completions.lock();
        *count += 1;
        if *count == 1 {
            Err(tonic::Status::invalid_argument("UnhandledCommand"))
        } else {
            Ok(RespondWorkflowTaskCompletedResponse::default())
        }
    }));
}

#[tokio::test]
async fn an_unhandled_command_rerun_supersedes_what_the_refused_task_appended() {
    // The first completion is refused because a signal came in. Core evicts, and the rerun
    // replays History, which shows the failure, and publishes under the next retry.
    let t = refused_then_rerun();
    let first_task = first_task_of(&t, 3);
    let store = Arc::new(RecordingStore::default());
    let mut mock_cfg = MockPollCfg::from_resps(t, [first_task, ResponseType::AllHistory]);
    refusing_first_completion(&mut mock_cfg, Default::default());
    let lang = Lang::new(
        build_mock_pollers(mock_cfg),
        store.clone(),
        Owner::with(WorkflowExecutionStatus::Completed),
    );

    let first = lang.poll().await;
    lang.complete(&first, records(&["a", "b"]), vec![complete_workflow()])
        .await;
    let eviction = lang.poll().await;
    assert!(eviction.eviction_reason().is_some());
    lang.complete_eviction(&eviction).await;
    let rerun = lang.poll().await;
    lang.complete(&rerun, records(&["a", "b"]), vec![complete_workflow()])
        .await;
    lang.shutdown().await;

    let attempts: Vec<_> = store
        .appends()
        .iter()
        .map(|a| (a.attempt, a.sequence))
        .collect();
    assert_eq!(
        attempts,
        [(1, 1), (2, 1)],
        "the rerun starts its sequence at 1"
    );
    assert_eq!(
        store.read(TOPIC).await,
        [
            "encrypted a",
            "encrypted b",
            "SUPERSEDED",
            "encrypted a",
            "encrypted b"
        ]
    );
}

#[tokio::test]
async fn a_rerun_that_skips_the_topic_still_withdraws_what_the_failed_task_appended() {
    // The first completion is refused. The rerun takes another branch, writes nothing to the
    // topic and completes. The task after it writes again under a new prefix.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::UnhandledCommand,
        Default::default(),
    );
    t.add_we_signaled("go", vec![]);
    t.add_full_wf_task();
    let timer = t.add_timer_started("1".to_string());
    t.add_timer_fired(timer, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    let events = t.get_full_history_info().unwrap().events().to_vec();
    let first_task = first_task_of(&t, 3);
    let rerun_task = TestHistoryBuilder::from_history(events[..7].to_vec());
    let rerun_task = hist_to_poll_resp(&rerun_task, WORKFLOW_ID, ResponseType::AllHistory).resp;
    let store = Arc::new(RecordingStore::default());
    let mut mock_cfg = MockPollCfg::from_resps(
        t,
        [
            first_task,
            ResponseType::Raw(rerun_task),
            ResponseType::AllHistory,
        ],
    );
    refusing_first_completion(&mut mock_cfg, Default::default());
    let lang = Lang::new(
        build_mock_pollers(mock_cfg),
        store.clone(),
        Owner::with(WorkflowExecutionStatus::Completed),
    );

    let first = lang.poll().await;
    lang.complete(&first, records(&["draft"]), vec![complete_workflow()])
        .await;
    let eviction = lang.poll().await;
    assert!(eviction.eviction_reason().is_some());
    lang.complete_eviction(&eviction).await;
    let rerun = lang.poll().await;
    lang.complete(
        &rerun,
        vec![],
        vec![start_timer_cmd(1, Duration::from_secs(10))],
    )
    .await;
    let fired = lang.poll().await;
    lang.complete(&fired, records(&["final"]), vec![complete_workflow()])
        .await;
    lang.shutdown().await;

    assert_eq!(
        store
            .appends()
            .iter()
            .map(|a| a.attempt)
            .collect::<Vec<_>>(),
        [attempt(0, 1), attempt(7, 1)]
    );
    assert_eq!(
        store.read(TOPIC).await,
        ["encrypted draft", "SUPERSEDED", "encrypted final"]
    );
}

#[tokio::test]
async fn a_rerun_after_a_crash_supersedes_what_the_stopped_worker_appended() {
    let store = Arc::new(RecordingStore::default());
    // The first Worker appends, then stops before its completion reaches the server.
    let mut first_history = TestHistoryBuilder::default();
    first_history.add_by_type(EventType::WorkflowExecutionStarted);
    first_history.add_workflow_task_scheduled_and_started();
    let run_id = first_history.get_orig_run_id().to_string();
    let mut mock_cfg = MockPollCfg::from_resps(first_history, [1]);
    mock_cfg.num_expected_completions = Some((0..=1).into());
    let stopped = Lang::new(
        build_mock_pollers(mock_cfg),
        store.clone(),
        Owner::with(WorkflowExecutionStatus::Running),
    );
    let first = stopped.poll().await;
    stopped
        .complete(
            &first,
            records(&["a", "b"]),
            vec![schedule_local_activity(1)],
        )
        .await;
    assert_eq!(store.read(TOPIC).await, ["encrypted a", "encrypted b"]);
    drop(stopped);

    // The task times out, and another Worker runs it again on the same store.
    let mut t = TestHistoryBuilder::default();
    t.add(WorkflowExecutionStartedEventAttributes {
        original_execution_run_id: run_id.clone(),
        first_execution_run_id: run_id.clone(),
        ..default_wes_attribs()
    });
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_timed_out();
    t.add_workflow_task_scheduled_and_started();
    let rerun = lang_appending_to(
        t,
        vec![ResponseType::AllHistory],
        &store,
        Default::default(),
    );
    let activation = rerun.poll().await;
    rerun
        .complete(&activation, records(&["a", "b"]), vec![complete_workflow()])
        .await;
    rerun.shutdown().await;

    let producer = format!("@{run_id}");
    assert_eq!(
        store.append_log(),
        [
            (TOPIC.to_string(), producer.clone(), 1, 1, 2),
            (TOPIC.to_string(), producer.clone(), 2, 1, 2),
        ],
        "no append was refused as stale"
    );
    assert_eq!(
        store.read(TOPIC).await,
        [
            "encrypted a",
            "encrypted b",
            "SUPERSEDED",
            "encrypted a",
            "encrypted b"
        ]
    );
    assert_eq!(
        store.retired(),
        [],
        "the rerun's Worker never appended under the first attempt, so it has nothing to retire"
    );
}

#[tokio::test]
async fn a_failed_task_leaves_what_it_appended() {
    // The first activation publishes and waits on a local activity. The second fails the task, so
    // nothing reaches History, but readers already saw the records.
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_workflow_task_scheduled_and_started();
    let store = Arc::new(RecordingStore::default());
    let failed = Arc::new(Notify::new());
    let mut mock_cfg = MockPollCfg::from_resps(history, [1]);
    mock_cfg.num_expected_completions = Some(0.into());
    failing_with(&mut mock_cfg, &["the workflow broke"], failed.clone());
    let lang = Lang::new(
        build_mock_pollers(mock_cfg),
        store.clone(),
        Owner::with(WorkflowExecutionStatus::Running),
    );

    let first = lang.poll().await;
    lang.complete(&first, records(&["a"]), vec![schedule_local_activity(1)])
        .await;
    let resolved = run_local_activity(&lang).await;
    assert!(only_resolves_local_activity(&resolved, 1));
    lang.worker
        .complete_workflow_activation(WorkflowActivationCompletion::fail(
            first.run_id.clone(),
            Failure {
                message: "the workflow broke".to_string(),
                ..Default::default()
            },
            None,
        ))
        .await
        .unwrap();
    failed_within_deadline(&failed).await;
    lang.shutdown_failed().await;
    assert_eq!(store.read(TOPIC).await, ["encrypted a"]);
}

#[tokio::test]
async fn a_lost_task_repeats_its_attempt_and_the_rerun_supersedes() {
    // The server loses the first task after the append, so History doesn't move and the next
    // task publishes under the same attempt. Other records diverge, the task fails, and the
    // failure the server writes moves the rerun to the next retry.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let lost_task = hist_to_poll_resp(&t, WORKFLOW_ID, 1.into()).resp;
    // The server schedules a new task, which has a new token.
    let mut next_task = lost_task.clone();
    next_task.task_token = b"next-task".to_vec();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
        Default::default(),
    );
    t.add_workflow_task_scheduled_and_started();
    let store = Arc::new(RecordingStore::default());
    let failed = Arc::new(Notify::new());
    let completions: Arc<Mutex<usize>> = Default::default();
    let mut mock_cfg = MockPollCfg::from_resps(
        t,
        [
            ResponseType::Raw(lost_task),
            ResponseType::Raw(next_task),
            ResponseType::AllHistory,
        ],
    );
    mock_cfg.completion_mock_fn = Some(Box::new(move |_| {
        let mut count = completions.lock();
        *count += 1;
        if *count == 1 {
            Err(tonic::Status::not_found("Workflow task not found"))
        } else {
            Ok(RespondWorkflowTaskCompletedResponse::default())
        }
    }));
    failing_with(
        &mut mock_cfg,
        &["Could not append", "different content"],
        failed.clone(),
    );
    let lang = Lang::new(
        build_mock_pollers(mock_cfg),
        store.clone(),
        Owner::with(WorkflowExecutionStatus::Completed),
    );

    let mut evictions = vec![];
    for values in [["a"], ["b"]] {
        let activation = lang.poll().await;
        lang.complete(&activation, records(&values), vec![complete_workflow()])
            .await;
        let eviction = lang.poll().await;
        evictions.push(eviction.eviction_reason());
        lang.complete_eviction(&eviction).await;
    }
    assert_eq!(
        evictions,
        [
            Some(EvictionReason::TaskNotFound),
            Some(EvictionReason::LangFail)
        ],
        "lang fails the task whose publish diverged"
    );
    failed_within_deadline(&failed).await;
    let rerun = lang.poll().await;
    lang.complete(&rerun, records(&["b"]), vec![complete_workflow()])
        .await;
    lang.shutdown().await;

    assert_eq!(store.calls()[..3], ["append", "append", "append"]);
    let attempts: Vec<_> = store
        .appends()
        .iter()
        .map(|a| (a.attempt, a.sequence))
        .collect();
    assert_eq!(
        attempts,
        [(1, 1), (2, 1)],
        "the store took no divergent batch"
    );
    assert_eq!(
        store.read(TOPIC).await,
        ["encrypted a", "SUPERSEDED", "encrypted b"]
    );
}

#[tokio::test]
async fn a_local_activity_starts_only_once_its_activations_output_is_in_the_store() {
    // The append is slow, so a Local Activity that started before it would find nothing.
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_workflow_task_scheduled_and_started();
    let store = Arc::new(RecordingStore {
        append_delay: Some(Duration::from_millis(300)),
        ..Default::default()
    });
    let lang = Arc::new(lang_appending_to(
        history,
        vec![1.into()],
        &store,
        Default::default(),
    ));
    let first = lang.poll().await;
    let completing = {
        let lang = lang.clone();
        let first = first.clone();
        tokio::spawn(async move {
            lang.complete(&first, records(&["a"]), vec![schedule_local_activity(1)])
                .await
        })
    };
    let task = lang.worker.poll_activity_task().await.unwrap();
    assert_eq!(
        store.appends().len(),
        1,
        "the activity starts after its activation's records landed"
    );
    completing.await.unwrap();
    lang.worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: task.task_token,
            result: Some(ActivityExecutionResult::ok(Payload::default())),
        })
        .await
        .unwrap();
    let resolved = lang.poll().await;
    assert!(only_resolves_local_activity(&resolved, 1));
    lang.complete(&resolved, vec![], vec![complete_workflow()])
        .await;
    let Ok(lang) = Arc::try_unwrap(lang) else {
        panic!("the completing task let go of the worker");
    };
    lang.shutdown().await;
}

#[tokio::test]
async fn a_topic_closed_before_the_publish_drops_its_records_and_the_task_goes_on() {
    let store = Arc::new(RecordingStore::default());
    let appended_at_completion: Arc<Mutex<Vec<usize>>> = Default::default();
    let history = timer_history();
    let first_run_id = history.get_orig_run_id().to_string();
    // Another party closed the topic before the Workflow published.
    StreamStore::close_topic(
        &*store,
        &temporalio_streams::proto::ChainId {
            namespace: NAMESPACE.to_string(),
            workflow_id: WORKFLOW_ID.to_string(),
            first_run_id,
            ..Default::default()
        },
        TOPIC,
    )
    .await
    .unwrap();
    let lang = lang_appending_to(
        history,
        vec![1.into(), 2.into()],
        &store,
        appended_at_completion.clone(),
    );
    let first = lang.poll().await;
    lang.complete(
        &first,
        records(&["a"])
            .into_iter()
            .chain(records_on("open", &["b"]))
            .collect(),
        vec![start_timer_cmd(1, Duration::from_secs(10))],
    )
    .await;
    let fired = lang.poll().await;
    lang.complete(&fired, vec![], vec![complete_workflow()])
        .await;
    lang.shutdown().await;
    assert_eq!(
        appended_at_completion.lock().len(),
        2,
        "both tasks completed"
    );
    let landed: Vec<_> = store.appends().iter().map(|a| a.topic.clone()).collect();
    assert_eq!(landed, ["open"], "the closed topic's records were dropped");
}

#[tokio::test]
async fn a_rerun_after_the_run_closed_its_own_topic_drops_its_records_and_goes_on() {
    // The task publishes and closes its topic, then the server refuses the completion. The rerun
    // publishes the same records under the next retry, which the closed topic refuses.
    let t = refused_then_rerun();
    let first_task = first_task_of(&t, 3);
    let store = Arc::new(RecordingStore::default());
    let completions: Arc<Mutex<usize>> = Default::default();
    let mut mock_cfg = MockPollCfg::from_resps(t, [first_task, ResponseType::AllHistory]);
    refusing_first_completion(&mut mock_cfg, completions.clone());
    let lang = Lang::new(
        build_mock_pollers(mock_cfg),
        store.clone(),
        Owner::with(WorkflowExecutionStatus::Completed),
    );

    let first = lang.poll().await;
    let mut finished = records(&["a"]);
    finished.push(OutputRecord {
        topic: TOPIC.to_string(),
        kind: StreamRecordKind::Finish as i32,
        ..Default::default()
    });
    lang.complete(&first, finished.clone(), vec![complete_workflow()])
        .await;
    // The run's close, as lang's close does after the FINISH landed.
    let chain = store.appends()[0].chain.clone().unwrap();
    StreamStore::close_topic(&*store, &chain, TOPIC)
        .await
        .unwrap();
    let eviction = lang.poll().await;
    assert!(eviction.eviction_reason().is_some());
    lang.complete_eviction(&eviction).await;
    let rerun = lang.poll().await;
    lang.complete(&rerun, finished, vec![complete_workflow()])
        .await;
    lang.shutdown().await;

    assert_eq!(*completions.lock(), 2, "the rerun's completion went out");
    assert_eq!(
        store.append_log(),
        [(TOPIC.to_string(), format!("@{}", first.run_id), 1, 1, 2)],
        "the rerun's records were dropped"
    );
}

// The chain-close contract. The eviction reason alone proves nothing, so the close needs a live
// terminal completion, and History must show that this completion closed the run.

/// Runs a terminal completion that the server refuses while a Local Activity cancel that lang has
/// not polled holds the eviction back. The run's end then overwrites the refusal's eviction
/// reason. Answers the store and the run id. The terminal completion's task started at event 3.
async fn refused_completion_with_held_eviction(owner: Arc<Owner>) -> (Arc<RecordingStore>, String) {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(60));
    t.add_workflow_task_scheduled_and_started();
    let mut mock_cfg = MockPollCfg::from_resps(t, [1]);
    mock_cfg.make_poll_stream_interminable = true;
    mock_cfg.num_expected_completions = Some(1.into());
    mock_cfg.completion_mock_fn = Some(Box::new(|_| {
        Err(tonic::Status::invalid_argument("UnhandledCommand"))
    }));
    let store = Arc::new(RecordingStore::default());
    let lang = Lang::new(build_mock_pollers(mock_cfg), store.clone(), owner);

    let activation = lang.poll().await;
    let run_id = activation.run_id.clone();
    let abandoned = schedule_local_activity_cmd(
        1,
        "1",
        ActivityCancellationType::Abandon,
        Duration::from_secs(60),
    );
    let waited = schedule_local_activity_cmd(
        2,
        "2",
        ActivityCancellationType::WaitCancellationCompleted,
        Duration::from_secs(60),
    );
    lang.complete(&activation, records(&["a"]), vec![abandoned, waited])
        .await;
    let mut starts = HashMap::new();
    for _ in 0..2 {
        let task = lang.worker.poll_activity_task().await.unwrap();
        let Some(activity_task::Variant::Start(start)) = &task.variant else {
            panic!("Expected a Local Activity start, got {task:?}");
        };
        starts.insert(start.activity_id.clone(), task.task_token);
    }
    lang.worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: starts.remove("2").unwrap(),
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    let resolved = lang.poll().await;
    assert!(only_resolves_local_activity(&resolved, 2));
    lang.complete(
        &resolved,
        vec![],
        vec![RequestCancelLocalActivity { seq: 1 }.into()],
    )
    .await;
    let cancelled = lang.poll().await;
    assert_matches!(
        cancelled.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveActivity(
                ResolveActivity {
                    seq: 1,
                    result: Some(ActivityResolution {
                        status: Some(activity_resolution::Status::Cancelled(_)),
                    }),
                    ..
                }
            )),
        }]
    );
    {
        let activity_poll = lang.worker.poll_activity_task();
        advance_fut!(activity_poll);
    }
    lang.complete(&cancelled, records(&["b"]), vec![complete_workflow()])
        .await;

    let (cancel, eviction) = {
        let wf_poll = lang.worker.poll_workflow_activation();
        advance_fut!(wf_poll);
        let cancel = lang.worker.poll_activity_task().await.unwrap();
        (cancel, wf_poll.await.unwrap())
    };
    assert_eq!(
        eviction.eviction_reason(),
        Some(EvictionReason::WorkflowExecutionEnding),
        "the run's end overwrote the refusal"
    );
    lang.complete_eviction(&eviction).await;
    lang.worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: cancel.task_token,
            result: Some(ActivityExecutionResult::cancel_from_details(None)),
        })
        .await
        .unwrap();
    lang.shutdown().await;
    (store, run_id)
}

#[tokio::test]
async fn a_refused_terminal_completion_with_an_undelivered_cancel_keeps_the_chain_open() {
    // History shows no close event, since the server took no terminal command.
    let owner = Owner::with(WorkflowExecutionStatus::Running);
    let (store, run_id) = refused_completion_with_held_eviction(owner.clone()).await;
    assert_eq!(
        *owner.asked.lock(),
        [(NAMESPACE.to_string(), WORKFLOW_ID.to_string(), run_id)],
        "the service read the run's History"
    );
    assert!(
        !store.calls().contains(&"close_chain"),
        "History shows the run open, so the chain stays open"
    );
}

#[tokio::test]
async fn a_refused_terminal_completion_then_a_later_task_that_closes_the_run_keeps_the_chain_open()
{
    // While the eviction waits, the run's next task completes the run. Its close follows the
    // task that started at event 10, not the refused one at event 3.
    let owner = Owner::closed_by(WorkflowExecutionStatus::Completed, Some(10));
    let (store, run_id) = refused_completion_with_held_eviction(owner.clone()).await;
    assert_eq!(
        *owner.asked.lock(),
        [(NAMESPACE.to_string(), WORKFLOW_ID.to_string(), run_id)]
    );
    assert!(
        !store.calls().contains(&"close_chain"),
        "the closing task is not ours, so the chain does not close at the stale attempt"
    );
}

#[tokio::test]
async fn a_legacy_query_after_the_run_completed_keeps_the_chain_as_it_was() {
    // A Worker that replays a closed run for a query evicts it with the run's end, but its
    // activations replay, so it holds no live terminal completion.
    let t = {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        let timer_started = t.add_by_type(EventType::TimerStarted);
        t.add_timer_fired(timer_started, "1".to_string());
        t.add_full_wf_task();
        t.add_workflow_execution_completed();
        t
    };
    let query_task = {
        let mut pr = hist_to_poll_resp(&t, WORKFLOW_ID, ResponseType::AllHistory);
        pr.query = Some(WorkflowQuery {
            query_type: "query-type".to_string(),
            query_args: Some(b"hi".into()),
            header: None,
        });
        pr.resp
    };
    let mut mock_cfg = MockPollCfg::from_resps(t, [ResponseType::Raw(query_task)]);
    mock_cfg.num_expected_legacy_query_resps = 1;
    let store = Arc::new(RecordingStore::default());
    let owner = Owner::with(WorkflowExecutionStatus::Completed);
    let lang = Lang::new(build_mock_pollers(mock_cfg), store.clone(), owner.clone());

    let first = lang.poll().await;
    assert!(first.is_replaying);
    lang.complete(
        &first,
        records(&["a"]),
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;
    let second = lang.poll().await;
    assert!(second.is_replaying);
    lang.complete(&second, records(&["b"]), vec![complete_workflow()])
        .await;
    let query = lang.poll().await;
    let query_id = assert_matches!(
        query.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
        }] => q.query_id.clone()
    );
    lang.complete(&query, vec![], vec![query_ok(query_id, "whatever")])
        .await;
    lang.shutdown().await;

    assert!(store.calls().is_empty(), "the replaying Worker never wrote");
    assert!(owner.asked.lock().is_empty());
}

#[tokio::test]
async fn an_accepted_terminal_completion_closes_the_chain() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let store = Arc::new(RecordingStore::default());
    let owner = Owner::closed_by(WorkflowExecutionStatus::Completed, Some(3));
    let mut mock_cfg = MockPollCfg::from_resps(t, [1]);
    // Keeps the Worker up, so it hands lang the eviction of the ended run.
    mock_cfg.make_poll_stream_interminable = true;
    let lang = Lang::new(build_mock_pollers(mock_cfg), store.clone(), owner.clone());
    let activation = lang.poll().await;
    lang.complete(&activation, records(&["a"]), vec![complete_workflow()])
        .await;
    let eviction = lang.poll().await;
    assert_eq!(
        eviction.eviction_reason(),
        Some(EvictionReason::WorkflowExecutionEnding)
    );
    lang.complete_eviction(&eviction).await;
    lang.shutdown().await;
    assert_eq!(store.calls(), ["append", "close_chain"]);
    assert_eq!(owner.asked.lock().len(), 1);
}

/// What a Workflow Activity's stream holds once [WORKFLOW_ID] scheduled it in `run_id`.
fn activity_chain(run_id: &str, activity_id: &str) -> ChainId {
    ChainId {
        namespace: NAMESPACE.to_string(),
        workflow_id: WORKFLOW_ID.to_string(),
        first_run_id: run_id.to_string(),
        owner_kind: StreamOwnerKind::WorkflowActivity as i32,
        activity_id: activity_id.to_string(),
    }
}

/// A `ScheduleActivity` that marks Activity `activity_id` as the owner of its streams, or not.
fn schedule_activity(activity_id: &str, marked: bool) -> workflow_command::Variant {
    let mut schedule = ScheduleActivity {
        seq: 1,
        activity_id: activity_id.to_string(),
        cancellation_type: ActivityCancellationType::TryCancel as i32,
        ..default_act_sched()
    };
    if marked {
        schedule
            .headers
            .insert(STREAM_OWNER_HEADER.to_string(), Payload::default());
    }
    schedule.into()
}

/// Appends one record to each of `topics` of the Activity's stream, as its producer would.
async fn activity_wrote(store: &RecordingStore, run_id: &str, activity_id: &str, topics: &[&str]) {
    for topic in topics {
        store
            .inner
            .append(StoreAppendRequest {
                chain: Some(activity_chain(run_id, activity_id)),
                topic: topic.to_string(),
                producer_id: format!("{activity_id}@{run_id}"),
                attempt: 1,
                sequence: 1,
                digest: vec![1; 32],
                records: vec![b"record".to_vec()],
            })
            .await
            .unwrap();
    }
}

/// The stream closes, which the Worker shutdown waited for.
fn activity_closes(store: &RecordingStore) -> Vec<(String, StoredClose)> {
    store
        .closes
        .lock()
        .iter()
        .map(|(chain, topic, close)| {
            assert_eq!(chain.owner_kind, StreamOwnerKind::WorkflowActivity as i32);
            (topic.clone(), close.clone())
        })
        .collect()
}

/// Lang over `history`, in which the run's last task started at event 9.
fn lang_for_activity(
    history: TestHistoryBuilder,
    batches: Vec<ResponseType>,
    store: &Arc<RecordingStore>,
    completions: Arc<Mutex<Vec<usize>>>,
) -> Lang {
    let mock = mock_appending_to(history, batches, store.clone(), completions);
    Lang::new(
        mock,
        store.clone(),
        Owner::closed_by(WorkflowExecutionStatus::Completed, Some(9)),
    )
}

/// Runs a Workflow that schedules Activity `tool`, which writes `topics`, and completes once the
/// Activity resolves. Returns the run id.
async fn run_activity_owner(
    history: TestHistoryBuilder,
    store: &Arc<RecordingStore>,
    marked: bool,
    topics: &[&str],
) -> String {
    let lang = lang_for_activity(history, vec![1.into(), 2.into()], store, Default::default());
    let start = lang.poll().await;
    let run_id = start.run_id.clone();
    activity_wrote(store, &run_id, "tool", topics).await;
    lang.complete(&start, vec![], vec![schedule_activity("tool", marked)])
        .await;
    let resolved = lang.poll().await;
    assert!(!resolved.is_replaying);
    assert!(resolved.jobs.iter().any(|job| matches!(
        job.variant,
        Some(workflow_activation_job::Variant::ResolveActivity(_))
    )));
    lang.complete(&resolved, vec![], vec![complete_workflow()])
        .await;
    lang.shutdown().await;
    run_id
}

#[tokio::test]
async fn a_marked_activity_that_completes_closes_each_topic_it_wrote_with_an_empty_result() {
    let store = Arc::new(RecordingStore::default());
    let run_id = run_activity_owner(
        canned_histories::single_activity("tool"),
        &store,
        true,
        &["status", "tokens"],
    )
    .await;
    let closes = activity_closes(&store);
    assert_eq!(
        closes
            .iter()
            .map(|(topic, _)| topic.as_str())
            .collect::<Vec<_>>(),
        ["status", "tokens"]
    );
    for (_, close) in &closes {
        assert_eq!(close.result, Some(temporalio_streams::empty_close_result()));
        assert!(close.failure.is_none());
        assert_eq!(close.closed_by, format!("tool@{run_id}"));
    }
    let chain = store.closes.lock()[0].0.clone();
    assert_eq!(chain, activity_chain(&run_id, "tool"));
    // A reader of the stream finds the close.
    let read = store
        .inner
        .read(StoreReadRequest {
            chain: Some(chain),
            topic: "tokens".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(read.closed);
}

#[tokio::test]
async fn a_worker_shutdown_waits_for_the_activity_stream_closes() {
    let store = Arc::new(RecordingStore {
        close_delay: Some(Duration::from_millis(200)),
        ..Default::default()
    });
    run_activity_owner(
        canned_histories::single_activity("tool"),
        &store,
        true,
        &["tokens"],
    )
    .await;
    assert_eq!(
        store.closes.lock().len(),
        1,
        "closed before the shutdown returned"
    );
}

#[tokio::test]
async fn a_marked_activity_that_fails_closes_with_the_failure_the_workflow_saw() {
    let store = Arc::new(RecordingStore::default());
    run_activity_owner(
        canned_histories::single_failed_activity("tool"),
        &store,
        true,
        &["tokens"],
    )
    .await;
    let closes = activity_closes(&store);
    let failure = closes[0].1.failure.clone().unwrap();
    assert!(closes[0].1.result.is_none());
    assert!(
        matches!(
            failure.failure_info,
            Some(FailureInfo::ActivityFailureInfo(_))
        ),
        "{failure:?}"
    );
}

#[tokio::test]
async fn a_marked_activity_that_times_out_closes_with_its_timeout() {
    let store = Arc::new(RecordingStore::default());
    run_activity_owner(
        canned_histories::started_activity_timeout("tool"),
        &store,
        true,
        &["tokens"],
    )
    .await;
    let closes = activity_closes(&store);
    let failure = closes[0].1.failure.clone().unwrap();
    assert_eq!(failure.message, "Activity task timed out", "{failure:?}");
}

#[tokio::test]
async fn an_unmarked_activity_costs_no_store_call() {
    let store = Arc::new(RecordingStore::default());
    run_activity_owner(
        canned_histories::single_activity("tool"),
        &store,
        false,
        &[],
    )
    .await;
    // Completing the run closes its own chain, and nothing else reaches the store.
    assert_eq!(store.calls(), ["close_chain"]);
}

#[tokio::test]
async fn a_store_that_refuses_the_close_leaves_the_task_completed() {
    let store = Arc::new(RecordingStore {
        refuse_closes: true,
        ..Default::default()
    });
    let completions: Arc<Mutex<Vec<usize>>> = Default::default();
    let lang = lang_for_activity(
        canned_histories::single_activity("tool"),
        vec![1.into(), 2.into()],
        &store,
        completions.clone(),
    );
    let start = lang.poll().await;
    activity_wrote(&store, &start.run_id, "tool", &["tokens"]).await;
    lang.complete(&start, vec![], vec![schedule_activity("tool", true)])
        .await;
    let resolved = lang.poll().await;
    lang.complete(&resolved, vec![], vec![complete_workflow()])
        .await;
    lang.shutdown().await;
    assert!(
        store.calls().contains(&"close_topic_with"),
        "{:?}",
        store.calls()
    );
    assert_eq!(completions.lock().len(), 2, "both tasks completed");
}

#[tokio::test]
async fn replay_of_a_resolution_makes_no_close() {
    let mut history = canned_histories::single_activity("tool");
    // Another task after the resolution, so the resolution replays.
    history.add_workflow_task_completed();
    let timer_started = history.add_by_type(EventType::TimerStarted);
    history.add_timer_fired(timer_started, "1".to_string());
    history.add_workflow_task_scheduled_and_started();
    let store = Arc::new(RecordingStore::default());
    let lang = Lang::new(
        mock_appending_to(history, vec![3.into()], store.clone(), Default::default()),
        store.clone(),
        Owner::with(WorkflowExecutionStatus::Running),
    );
    let start = lang.poll().await;
    assert!(start.is_replaying);
    lang.complete(&start, vec![], vec![schedule_activity("tool", true)])
        .await;
    let resolved = lang.poll().await;
    assert!(resolved.is_replaying);
    lang.complete(
        &resolved,
        vec![],
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;
    let fired = lang.poll().await;
    lang.complete(&fired, vec![], vec![complete_workflow()])
        .await;
    lang.shutdown().await;
    assert!(
        store.closes.lock().is_empty(),
        "a replayed resolution closes nothing: {:?}",
        store.calls()
    );
}

#[tokio::test]
async fn a_try_cancel_closes_the_streams_with_the_canceled_failure() {
    let store = Arc::new(RecordingStore::default());
    let lang = lang_for_activity(
        canned_histories::cancel_scheduled_activity("tool", "signal"),
        vec![1.into(), 2.into()],
        &store,
        Default::default(),
    );
    let start = lang.poll().await;
    activity_wrote(&store, &start.run_id, "tool", &["tokens"]).await;
    lang.complete(&start, vec![], vec![schedule_activity("tool", true)])
        .await;
    let signaled = lang.poll().await;
    lang.complete(
        &signaled,
        vec![],
        vec![RequestCancelActivity { seq: 1 }.into()],
    )
    .await;
    // A try-cancel resolves the Activity at once, and lang publishes that resolution.
    let resolved = lang.poll().await;
    lang.complete(&resolved, vec![], vec![complete_workflow()])
        .await;
    lang.shutdown().await;
    let closes = activity_closes(&store);
    let failure = closes[0].1.failure.clone().unwrap();
    assert!(
        matches!(
            failure
                .cause
                .as_deref()
                .and_then(|cause| cause.failure_info.as_ref()),
            Some(FailureInfo::CanceledFailureInfo(_))
        ),
        "{failure:?}"
    );
}

#[tokio::test]
async fn a_publish_with_a_link_lands_before_the_completion() {
    use temporalio_common::protos::temporal::api::{
        enums::v1::StreamOwnerKind as Kind, stream::v1::StreamReference,
    };
    use temporalio_streams::proto::{StreamLink, stream_link};
    let store = Arc::new(RecordingStore::default());
    let appended_at_completion: Arc<Mutex<Vec<usize>>> = Default::default();
    let lang = lang_appending_to(
        timer_history(),
        vec![1.into(), 2.into()],
        &store,
        appended_at_completion.clone(),
    );
    let link = StreamLink {
        event: stream_link::Event::Open as i32,
        stream: Some(StreamReference {
            owner_kind: Kind::WorkflowActivity as i32,
            workflow_id: WORKFLOW_ID.to_string(),
            run_id: "run".to_string(),
            topic: "tokens".to_string(),
            activity_id: "tool-42".to_string(),
        }),
        name: "tool-42".to_string(),
        ..Default::default()
    };
    let mut committed = records(&["a"]);
    committed.push(OutputRecord {
        topic: TOPIC.to_string(),
        kind: StreamRecordKind::Link as i32,
        link: Some(link.clone()),
        ..Default::default()
    });
    let activation = lang.poll().await;
    lang.complete(
        &activation,
        committed.clone(),
        vec![start_timer_cmd(1, Duration::from_secs(10))],
    )
    .await;
    let fired = lang.poll().await;
    lang.complete(&fired, vec![], vec![complete_workflow()])
        .await;
    lang.shutdown().await;
    assert_eq!(appended_at_completion.lock()[0], 1);
    let [append] = store.appends().try_into().unwrap();
    assert_eq!(
        append.digest,
        temporalio_streams::workflow_batch_digest(
            TOPIC,
            committed
                .iter()
                .map(|r| (r.kind, r.content_hash.as_slice(), r.link.as_ref()))
        )
    );
    let stored = StreamRecord::decode(append.records[1].as_slice()).unwrap();
    assert_eq!(stored.kind, StreamRecordKind::Link as i32);
    assert_eq!(stored.link, Some(link));
}
