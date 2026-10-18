//! Nexus operation progress as lang gets it from Core's mock Worker. The server puts each progress
//! on a Workflow Task's scheduled event as an attachment keyed by endpoint, service, operation and
//! token. Core hands lang the task's attachments as one job after the task's resolutions, and
//! lang hands the folded progress to its started operations with the crate's reference delivery.

use prost::Message;
use std::time::Duration;
use temporalio_common::{
    protos::{
        coresdk::{
            nexus::NexusOperationCancellationType,
            workflow_activation::{
                WorkflowActivation, resolve_nexus_operation_start, workflow_activation_job,
            },
            workflow_commands::{
                CompleteWorkflowExecution, RequestCancelNexusOperation, ScheduleNexusOperation,
                workflow_command,
            },
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            enums::v1::{EventType, WorkflowTaskFailedCause},
            history::v1::{
                NexusOperationCancelRequestedEventAttributes,
                NexusOperationCompletedEventAttributes, NexusOperationScheduledEventAttributes,
                NexusOperationStartedEventAttributes,
            },
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_sdk_core::{
    Worker,
    replay::TestHistoryBuilder,
    test_help::{
        MockPollCfg, ResponseType, WorkerExt, build_mock_pollers, mock_worker, start_timer_cmd,
    },
};
use temporalio_streams::{
    NEXUS_OPERATION_PROGRESS_TYPE_URL, NexusProgressOperations, STREAM_WAKE_TYPE_URL,
    fold_nexus_progresses, fold_stream_wakes,
    protos::temporal::api::{
        nexus::v1::{
            NexusOperationProgress,
            nexus_operation_progress::{Operation, OperationKey},
        },
        stream::v1::StreamWake,
    },
};

/// The token the server records for operation `id`.
fn token(id: i64) -> String {
    format!("token-{id}")
}

/// Progress of operation `id`, keyed by its token as the server keys it.
fn progress(id: i64, counter: i64) -> NexusOperationProgress {
    NexusOperationProgress {
        operation: Some(Operation::OperationKey(OperationKey {
            endpoint: "endpoint".to_string(),
            service: "service".to_string(),
            operation: "operation".to_string(),
            operation_token: token(id),
        })),
        position: format!("position-{counter}"),
        counter,
        metadata: [("records".to_string(), counter.to_string())].into(),
    }
}

fn attached(progress: &NexusOperationProgress) -> prost_types::Any {
    prost_types::Any {
        type_url: NEXUS_OPERATION_PROGRESS_TYPE_URL.to_string(),
        value: progress.encode_to_vec(),
    }
}

fn schedule_operation(seq: u32) -> ScheduleNexusOperation {
    ScheduleNexusOperation {
        seq,
        endpoint: "endpoint".to_string(),
        service: "service".to_string(),
        operation: "operation".to_string(),
        ..Default::default()
    }
}

fn add_operation_scheduled(t: &mut TestHistoryBuilder) -> i64 {
    t.add(NexusOperationScheduledEventAttributes {
        endpoint: "endpoint".to_string(),
        service: "service".to_string(),
        operation: "operation".to_string(),
        ..Default::default()
    })
}

fn add_operation_started(t: &mut TestHistoryBuilder, scheduled_event_id: i64) {
    t.add(NexusOperationStartedEventAttributes {
        scheduled_event_id,
        operation_token: token(scheduled_event_id),
        ..Default::default()
    });
}

fn add_progress_task(t: &mut TestHistoryBuilder, progress: &[NexusOperationProgress]) {
    t.add_workflow_task_scheduled_with_attachments(progress.iter().map(attached).collect());
    t.add_workflow_task_started();
}

fn worker(history: TestHistoryBuilder, batches: Vec<ResponseType>) -> Worker {
    let mut mock = build_mock_pollers(MockPollCfg::from_resps(history, batches));
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    mock_worker(mock)
}

/// The test in lang's place: it tracks the operations it schedules and hands each task's
/// progress to them, as a lang's Workflow instance does.
struct Lang {
    worker: Worker,
    run_id: String,
    operations: NexusProgressOperations,
}

impl Lang {
    fn new(worker: Worker) -> Self {
        Self {
            worker,
            run_id: String::new(),
            operations: NexusProgressOperations::default(),
        }
    }

    /// The next activation as job names, after lang applied it. `Progress(seq, counter)` is a
    /// delivery of progress, and `Wake(subscription, counter)` a folded stream wake.
    async fn next(&mut self) -> (Vec<String>, bool) {
        let activation = tokio::time::timeout(
            Duration::from_secs(10),
            self.worker.poll_workflow_activation(),
        )
        .await
        .expect("an activation")
        .unwrap();
        self.run_id = activation.run_id.clone();
        (self.apply(&activation), activation.is_replaying)
    }

    fn apply(&mut self, activation: &WorkflowActivation) -> Vec<String> {
        use workflow_activation_job::Variant;
        let mut names = vec![];
        for job in &activation.jobs {
            match &job.variant {
                Some(Variant::ResolveNexusOperationStart(start)) => {
                    match &start.status {
                        Some(resolve_nexus_operation_start::Status::OperationToken(token)) => {
                            self.operations.start(start.seq, token);
                        }
                        Some(resolve_nexus_operation_start::Status::Failed(_)) => {
                            self.operations.resolve(start.seq);
                        }
                        _ => {}
                    }
                    names.push(format!("Start({})", start.seq));
                }
                Some(Variant::ResolveNexusOperation(resolved)) => {
                    self.operations.resolve(resolved.seq);
                    names.push(format!("Resolve({})", resolved.seq));
                }
                Some(Variant::WorkflowTaskAttachments(attached)) => {
                    let folded = fold_nexus_progresses(&attached.attachments).unwrap();
                    for (seq, progress) in self.operations.deliver(&folded) {
                        names.push(format!("Progress({seq}, {})", progress.counter));
                    }
                    for wake in fold_stream_wakes(&attached.attachments).unwrap() {
                        names.push(format!("Wake({}, {})", wake.subscription_id, wake.counter));
                    }
                }
                Some(other) => names.push(other.to_string()),
                None => names.push("none".to_string()),
            }
        }
        names
    }

    async fn complete(&mut self, commands: Vec<workflow_command::Variant>) {
        for command in &commands {
            if let workflow_command::Variant::ScheduleNexusOperation(schedule) = command {
                self.operations.schedule(
                    schedule.seq,
                    &schedule.endpoint,
                    &schedule.service,
                    &schedule.operation,
                );
            }
        }
        self.worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
                self.run_id.clone(),
                commands,
            ))
            .await
            .unwrap();
    }

    async fn finish(mut self) {
        self.complete(vec![CompleteWorkflowExecution::default().into()])
            .await;
        self.worker.drain_pollers_and_shutdown().await;
    }
}

/// A started operation, then a task with nothing for lang to do, then two tasks scheduled only
/// because progress arrived.
fn progress_after_a_quiet_task() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled = add_operation_scheduled(&mut t);
    add_operation_started(&mut t, scheduled);
    t.add_full_wf_task();
    add_progress_task(&mut t, &[progress(scheduled, 1)]);
    t.add_workflow_task_completed();
    add_progress_task(&mut t, &[progress(scheduled, 2)]);
    t
}

async fn run_progress_after_a_quiet_task(batches: Vec<ResponseType>, replaying: bool) {
    let mut lang = Lang::new(worker(progress_after_a_quiet_task(), batches));
    lang.next().await;
    lang.complete(vec![schedule_operation(1).into()]).await;
    assert_eq!(lang.next().await, (vec!["Start(1)".to_string()], replaying));
    lang.complete(vec![]).await;
    assert_eq!(
        lang.next().await,
        (vec!["Progress(1, 1)".to_string()], replaying),
        "the progress-only task gets an activation of its own"
    );
    lang.complete(vec![]).await;
    assert_eq!(
        lang.next().await,
        (vec!["Progress(1, 2)".to_string()], false)
    );
    lang.finish().await;
}

#[tokio::test]
async fn progress_reaches_lang_once_per_task_live() {
    run_progress_after_a_quiet_task(vec![1.into(), 2.into(), 3.into(), 4.into()], false).await;
}

#[tokio::test]
async fn progress_reaches_lang_in_the_same_activations_on_replay() {
    run_progress_after_a_quiet_task(vec![4.into()], true).await;
}

async fn run_progress_and_a_wake_on_one_task(batches: Vec<ResponseType>, replaying: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled = add_operation_scheduled(&mut t);
    add_operation_started(&mut t, scheduled);
    t.add_full_wf_task();
    let wake = StreamWake {
        subscription_id: "reader-1".to_string(),
        position: "position-9".to_string(),
        counter: 9,
        ..Default::default()
    };
    t.add_workflow_task_scheduled_with_attachments(vec![
        attached(&progress(scheduled, 1)),
        prost_types::Any {
            type_url: STREAM_WAKE_TYPE_URL.to_string(),
            value: wake.encode_to_vec(),
        },
    ]);
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    add_progress_task(&mut t, &[progress(scheduled, 2)]);
    let mut lang = Lang::new(worker(t, batches));
    lang.next().await;
    lang.complete(vec![schedule_operation(1).into()]).await;
    assert_eq!(lang.next().await.0, vec!["Start(1)"]);
    lang.complete(vec![]).await;
    assert_eq!(
        lang.next().await,
        (
            vec![
                "Progress(1, 1)".to_string(),
                "Wake(reader-1, 9)".to_string()
            ],
            replaying
        )
    );
    lang.complete(vec![]).await;
    assert_eq!(
        lang.next().await,
        (vec!["Progress(1, 2)".to_string()], false)
    );
    lang.finish().await;
}

#[tokio::test]
async fn progress_and_a_stream_wake_on_one_task_reach_lang_together_live() {
    run_progress_and_a_wake_on_one_task(vec![1.into(), 2.into(), 3.into(), 4.into()], false).await;
}

#[tokio::test]
async fn progress_and_a_stream_wake_on_one_task_replay_in_the_activation_they_had_live() {
    run_progress_and_a_wake_on_one_task(vec![4.into()], true).await;
}

#[tokio::test]
async fn progress_follows_the_start_it_arrived_with() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled = add_operation_scheduled(&mut t);
    add_operation_started(&mut t, scheduled);
    add_progress_task(&mut t, &[progress(scheduled, 7)]);
    let mut lang = Lang::new(worker(t, vec![1.into(), 2.into()]));
    lang.next().await;
    lang.complete(vec![schedule_operation(1).into()]).await;
    assert_eq!(lang.next().await.0, vec!["Start(1)", "Progress(1, 7)"]);
    lang.finish().await;
}

#[tokio::test]
async fn a_burst_across_a_failed_task_and_its_retry_is_one_delivery() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled = add_operation_scheduled(&mut t);
    add_operation_started(&mut t, scheduled);
    // The failed task's scheduled event and its retry's go out in one job. The first holds two
    // entries for the same operation, out of order, so the fold does not rely on the server's.
    add_progress_task(&mut t, &[progress(scheduled, 4), progress(scheduled, 2)]);
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::Unspecified,
        Default::default(),
    );
    add_progress_task(&mut t, &[progress(scheduled, 5), progress(scheduled, 6)]);
    let mut lang = Lang::new(worker(t, vec![1.into(), ResponseType::AllHistory]));
    lang.next().await;
    lang.complete(vec![schedule_operation(1).into()]).await;
    assert_eq!(lang.next().await.0, vec!["Start(1)", "Progress(1, 6)"]);
    lang.finish().await;
}

#[tokio::test]
async fn progress_on_a_failed_task_reaches_lang_when_the_retry_carries_none() {
    // The server keeps progress on the scheduled event it went on and does not repeat it on the
    // retry, so the failed task's scheduled event is the only record of it.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled = add_operation_scheduled(&mut t);
    add_operation_started(&mut t, scheduled);
    add_progress_task(&mut t, &[progress(scheduled, 4)]);
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::Unspecified,
        Default::default(),
    );
    t.add_workflow_task_scheduled_and_started();
    for batches in [
        vec![1.into(), ResponseType::AllHistory],
        vec![ResponseType::AllHistory],
    ] {
        let mut lang = Lang::new(worker(t.clone(), batches));
        lang.next().await;
        lang.complete(vec![schedule_operation(1).into()]).await;
        assert_eq!(lang.next().await.0, vec!["Start(1)", "Progress(1, 4)"]);
        lang.finish().await;
    }
}

#[tokio::test]
async fn progress_at_or_below_a_delivered_counter_is_dropped_in_later_tasks() {
    // After a failover the new active cluster can send a counter the Workflow already saw.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled = add_operation_scheduled(&mut t);
    let first_timer = t.add_timer_started("1".to_string());
    add_operation_started(&mut t, scheduled);
    add_progress_task(&mut t, &[progress(scheduled, 5)]);
    t.add_workflow_task_completed();
    let second_timer = t.add_timer_started("2".to_string());
    t.add_timer_fired(first_timer, "1".to_string());
    add_progress_task(&mut t, &[progress(scheduled, 5)]);
    t.add_workflow_task_completed();
    t.add_timer_fired(second_timer, "2".to_string());
    add_progress_task(&mut t, &[progress(scheduled, 4)]);
    t.add_workflow_task_completed();
    add_progress_task(&mut t, &[progress(scheduled, 6)]);

    let mut runs = vec![];
    for batches in [
        vec![1.into(), 2.into(), 3.into(), 4.into(), 5.into()],
        vec![5.into()],
    ] {
        let mut lang = Lang::new(worker(t.clone(), batches));
        let mut seen = vec![];
        lang.next().await;
        lang.complete(vec![
            schedule_operation(1).into(),
            start_timer_cmd(1, Duration::from_secs(60)),
        ])
        .await;
        seen.push(lang.next().await.0);
        lang.complete(vec![start_timer_cmd(2, Duration::from_secs(60))])
            .await;
        for _ in 0..2 {
            seen.push(lang.next().await.0);
            lang.complete(vec![]).await;
        }
        seen.push(lang.next().await.0);
        lang.finish().await;
        runs.push(seen);
    }
    assert_eq!(
        runs[0],
        vec![
            vec!["Start(1)".to_string(), "Progress(1, 5)".to_string()],
            vec!["FireTimer(1)".to_string()],
            vec!["FireTimer(2)".to_string()],
            vec!["Progress(1, 6)".to_string()],
        ]
    );
    assert_eq!(runs[0], runs[1]);
}

#[tokio::test]
async fn progress_for_an_unknown_closed_or_stale_operation_is_dropped() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let open = add_operation_scheduled(&mut t);
    let closing = add_operation_scheduled(&mut t);
    t.add_by_type(EventType::TimerStarted);
    add_operation_started(&mut t, open);
    add_operation_started(&mut t, closing);
    add_progress_task(&mut t, &[progress(open, 5)]);
    t.add_workflow_task_completed();
    t.add(NexusOperationCompletedEventAttributes {
        scheduled_event_id: closing,
        ..Default::default()
    });
    add_progress_task(
        &mut t,
        &[
            // Resolved in this same task, so lang gets the result and no progress.
            progress(closing, 1),
            // Lower than what lang already has.
            progress(open, 3),
            // No operation has this token.
            progress(999, 1),
        ],
    );
    let mut lang = Lang::new(worker(t, vec![1.into(), 2.into(), 3.into()]));
    lang.next().await;
    lang.complete(vec![
        schedule_operation(1).into(),
        schedule_operation(2).into(),
        start_timer_cmd(1, Duration::from_secs(60)),
    ])
    .await;
    assert_eq!(
        lang.next().await.0,
        vec!["Start(1)", "Start(2)", "Progress(1, 5)"]
    );
    lang.complete(vec![]).await;
    assert_eq!(lang.next().await.0, vec!["Resolve(2)"]);
    lang.finish().await;
}

/// Lang cancels a started operation, then a later task carries progress for it, beside a timer so
/// the task has an activation to look at.
fn progress_after_a_cancel(cancel_type: NexusOperationCancellationType) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled = add_operation_scheduled(&mut t);
    add_operation_started(&mut t, scheduled);
    t.add_full_wf_task();
    // Commands land in the order lang sent them: the cancel request, then the timer.
    if cancel_type == NexusOperationCancellationType::TryCancel {
        t.add(NexusOperationCancelRequestedEventAttributes {
            scheduled_event_id: scheduled,
            ..Default::default()
        });
    }
    let timer = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer, "1".to_string());
    add_progress_task(&mut t, &[progress(scheduled, 1)]);
    t
}

async fn run_progress_after_a_cancel(
    cancel_type: NexusOperationCancellationType,
    batches: Vec<ResponseType>,
) -> Vec<Vec<String>> {
    let mut lang = Lang::new(worker(progress_after_a_cancel(cancel_type), batches));
    let mut seen = vec![lang.next().await.0];
    lang.complete(vec![
        ScheduleNexusOperation {
            cancellation_type: cancel_type as i32,
            ..schedule_operation(1)
        }
        .into(),
    ])
    .await;
    seen.push(lang.next().await.0);
    lang.complete(vec![
        RequestCancelNexusOperation { seq: 1 }.into(),
        start_timer_cmd(1, Duration::from_secs(60)),
    ])
    .await;
    loop {
        let jobs = lang.next().await.0;
        seen.push(jobs.clone());
        if jobs.iter().any(|job| job.starts_with("FireTimer")) {
            break;
        }
        lang.complete(vec![]).await;
    }
    lang.finish().await;
    seen
}

/// Once lang cancels with `TryCancel` or `Abandon`, the operation is resolved for lang, so later
/// progress for it is dropped, live and on replay.
#[tokio::test]
async fn progress_after_a_cancelled_operation_is_dropped() {
    for cancel_type in [
        NexusOperationCancellationType::TryCancel,
        NexusOperationCancellationType::Abandon,
    ] {
        let live =
            run_progress_after_a_cancel(cancel_type, vec![1.into(), 2.into(), 3.into()]).await;
        let replayed =
            run_progress_after_a_cancel(cancel_type, vec![ResponseType::AllHistory]).await;
        assert_eq!(live, replayed, "{cancel_type:?}");
        assert!(
            live.iter()
                .flatten()
                .all(|job| !job.starts_with("Progress")),
            "{cancel_type:?}: {live:?}"
        );
        assert!(
            live.iter().flatten().any(|job| job == "Resolve(1)"),
            "{cancel_type:?}: {live:?}"
        );
    }
}
