//! Stream wakes as lang gets them from Core's mock Worker. The server puts each wake on a Workflow
//! Task's scheduled event as an attachment, Core hands lang the task's attachments as one job,
//! and lang folds the wakes with the crate's fold.

use prost::Message;
use std::time::Duration;
use temporalio_common::{
    protos::{
        coresdk::{
            workflow_activation::{WorkflowActivation, workflow_activation_job},
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::Payload,
            enums::v1::{EventType, WorkflowTaskFailedCause},
            failure::v1::Failure,
            stream::v1::StreamWake,
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_sdk_core::{
    Worker,
    replay::TestHistoryBuilder,
    test_help::{MockPollCfg, ResponseType, build_mock_pollers, mock_worker},
};
use temporalio_streams::{STREAM_WAKE_TYPE_URL, fold_stream_wakes};

fn wake(subscription_id: &str, counter: i64) -> StreamWake {
    StreamWake {
        subscription_id: subscription_id.to_string(),
        position: format!("position-{counter}"),
        counter,
        metadata: [("records".to_string(), counter.to_string())].into(),
        ..Default::default()
    }
}

fn attached(wake: &StreamWake) -> prost_types::Any {
    prost_types::Any {
        type_url: STREAM_WAKE_TYPE_URL.to_string(),
        value: wake.encode_to_vec(),
    }
}

/// The wakes lang folds from `activation`'s attachments job, empty without one.
fn folded_wakes(activation: &WorkflowActivation) -> Vec<StreamWake> {
    let jobs: Vec<_> = activation
        .jobs
        .iter()
        .filter_map(|job| match &job.variant {
            Some(workflow_activation_job::Variant::WorkflowTaskAttachments(a)) => Some(a),
            _ => None,
        })
        .collect();
    assert!(jobs.len() <= 1, "one attachments job per task: {jobs:?}");
    jobs.first()
        .map(|job| fold_stream_wakes(&job.attachments).unwrap())
        .unwrap_or_default()
}

fn job_names(activation: &WorkflowActivation) -> Vec<String> {
    activation
        .jobs
        .iter()
        .map(|job| match &job.variant {
            Some(workflow_activation_job::Variant::SignalWorkflow(s)) => {
                format!("Signal({})", s.signal_name)
            }
            Some(other) => other.to_string(),
            None => "none".to_string(),
        })
        .collect()
}

fn worker(history: TestHistoryBuilder, batches: Vec<ResponseType>) -> Worker {
    let mut mock = build_mock_pollers(MockPollCfg::from_resps(history, batches));
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    mock_worker(mock)
}

async fn complete_empty(worker: &Worker, run_id: &str) {
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(run_id.to_string()))
        .await
        .unwrap();
}

async fn next(worker: &Worker) -> WorkflowActivation {
    tokio::time::timeout(Duration::from_secs(10), worker.poll_workflow_activation())
        .await
        .expect("an activation")
        .unwrap()
}

#[tokio::test]
async fn a_failed_task_and_its_retry_fold_their_wakes_per_subscription() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_with_attachments(vec![
        attached(&wake("a", 5)),
        attached(&wake("b", 1)),
    ]);
    t.add_workflow_task_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
        Failure::default(),
    );
    let closed = StreamWake {
        closed: true,
        close_result: Some(Payload {
            data: b"summary".to_vec(),
            ..Default::default()
        }),
        ..wake("a", 7)
    };
    t.add_workflow_task_scheduled_with_attachments(vec![attached(&closed)]);
    t.add_workflow_task_started();
    let worker = worker(t, vec![ResponseType::AllHistory]);

    let init = next(&worker).await;
    assert!(folded_wakes(&init).is_empty());
    complete_empty(&worker, &init.run_id).await;
    let woken = next(&worker).await;
    assert_eq!(
        folded_wakes(&woken),
        vec![closed, wake("b", 1)],
        "{:?}",
        job_names(&woken)
    );
}

#[tokio::test]
async fn a_wake_and_a_signal_of_one_task_share_its_activation() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_we_signaled("go", vec![]);
    let recheck = StreamWake {
        recheck: true,
        ..wake("1", 0)
    };
    t.add_workflow_task_scheduled_with_attachments(vec![attached(&recheck)]);
    t.add_workflow_task_started();
    let worker = worker(
        t,
        vec![ResponseType::ToTaskNum(1), ResponseType::ToTaskNum(2)],
    );

    let init = next(&worker).await;
    complete_empty(&worker, &init.run_id).await;
    let woken = next(&worker).await;
    assert_eq!(
        job_names(&woken),
        ["Signal(go)", "WorkflowTaskAttachments(1)"]
    );
    assert_eq!(folded_wakes(&woken), vec![recheck]);
}

/// Two tasks scheduled only for their wakes, after a quiet task. Each wake task completes with no
/// commands, the shape of a heartbeat chain, so replay must still keep their activations apart.
async fn run_wake_tasks(batches: Vec<ResponseType>, replaying: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_with_attachments(vec![attached(&wake("1", 41))]);
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    t.add_workflow_task_scheduled_with_attachments(vec![
        attached(&wake("1", 42)),
        // Lang skips an attachment it does not know.
        prost_types::Any {
            type_url: "type.googleapis.com/temporal.api.nexus.v1.Other".to_string(),
            value: vec![1, 2, 3],
        },
    ]);
    t.add_workflow_task_started();
    let worker = worker(t, batches);

    let init = next(&worker).await;
    let run_id = init.run_id.clone();
    complete_empty(&worker, &run_id).await;
    let first = next(&worker).await;
    assert_eq!(folded_wakes(&first), vec![wake("1", 41)]);
    assert_eq!(first.is_replaying, replaying);
    complete_empty(&worker, &run_id).await;
    let second = next(&worker).await;
    assert_eq!(folded_wakes(&second), vec![wake("1", 42)]);
    assert!(!second.is_replaying);
}

#[tokio::test]
async fn each_wake_task_gets_its_own_activation_live() {
    run_wake_tasks(
        vec![
            ResponseType::ToTaskNum(1),
            ResponseType::ToTaskNum(2),
            ResponseType::ToTaskNum(3),
        ],
        false,
    )
    .await;
}

#[tokio::test]
async fn each_wake_task_gets_its_own_activation_on_replay() {
    run_wake_tasks(vec![ResponseType::AllHistory], true).await;
}
