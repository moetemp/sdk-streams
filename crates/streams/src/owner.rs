//! What the stream layer asks Temporal about a stream's owner.
//!
//! A store can't see an owner close, so the layer above it asks: to find the chain a stream is
//! keyed by, to refuse a producer once the chain ended, and to end a read. The crate implements
//! this over its client, and tests use a fake.

use temporalio_common::protos::temporal::api::enums::v1::{
    ActivityExecutionStatus, WorkflowExecutionStatus,
};

/// The owner of a stream, by kind and id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerId<'a> {
    /// A Workflow id.
    Workflow(&'a str),
    /// A standalone Activity id.
    Activity(&'a str),
    /// An Activity that a Workflow run scheduled. The run is the one a describe names.
    WorkflowActivity {
        /// The scheduling Workflow.
        workflow_id: &'a str,
        /// The Activity id in the scheduling run.
        activity_id: &'a str,
    },
}

/// How an owner's run stands, as the stream layer needs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerStatus {
    /// The run has not reached a terminal status.
    Running,
    /// The run closed, and its successor in the chain writes on.
    ContinuedAsNew,
    /// The run closed and ended its chain.
    Ended,
    /// The scheduling run of a Workflow Activity runs, but the Activity is not pending in it. It
    /// either ended or is not scheduled yet, and the describe can't tell which.
    ActivityGone,
}

impl OwnerStatus {
    /// The status of a Workflow run. A paused run is running, since it can resume and write.
    pub fn of_workflow(status: WorkflowExecutionStatus) -> Self {
        use WorkflowExecutionStatus::*;
        match status {
            Completed | Failed | Canceled | Terminated | TimedOut => Self::Ended,
            ContinuedAsNew => Self::ContinuedAsNew,
            Unspecified | Running | Paused => Self::Running,
        }
    }

    /// The status of a standalone Activity run. Retries, backoff, pause and reset keep the run, so
    /// only a terminal status ends it.
    pub fn of_activity(status: ActivityExecutionStatus) -> Self {
        use ActivityExecutionStatus::*;
        match status {
            Completed | Failed | Canceled | Terminated | TimedOut => Self::Ended,
            Unspecified | Running | Paused => Self::Running,
        }
    }

    /// The status of a Workflow Activity, from its scheduling run's status and whether the run
    /// lists the Activity as pending. A closed run has no pending Activities, so it ends the
    /// Activity's streams, also when it continued as new.
    pub fn of_workflow_activity(status: WorkflowExecutionStatus, pending: bool) -> Self {
        match Self::of_workflow(status) {
            Self::Running if pending => Self::Running,
            Self::Running => Self::ActivityGone,
            _ => Self::Ended,
        }
    }
}

/// One run of an owner, as Temporal describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerDescription {
    /// The run described.
    pub run_id: String,
    /// The first run of the chain the run belongs to. Streams are keyed by it. An Activity run is
    /// its own chain. The stream layer keys a Workflow Activity by its scheduling run, whatever
    /// this holds.
    pub first_run_id: String,
    /// How the run stands.
    pub status: OwnerStatus,
}

impl OwnerDescription {
    /// Whether the run ended its chain. A run that continued as new did not, since its successor
    /// writes on.
    pub fn chain_ended(&self) -> bool {
        self.status == OwnerStatus::Ended
    }
}

/// How a run closed, from the tail of its History.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunClose {
    /// The status the run's close event gives, such as completed for `WorkflowExecutionCompleted`.
    pub status: WorkflowExecutionStatus,
    /// The `started_event_id` of the last `WorkflowTaskCompleted` before the close event. For a
    /// run that a Workflow Task closed, it names that task. `None` when History has none.
    pub closing_task_started_event_id: Option<i64>,
}

/// Why a describe failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OwnerError {
    /// Temporal holds no such owner or run, or no longer does.
    #[error("not found: {0}")]
    NotFound(String),
    /// Temporal could not answer.
    #[error("{0}")]
    Failed(String),
}

/// Temporal, as the stream layer asks it about owners.
#[async_trait::async_trait]
pub trait OwnerClient: Send + Sync {
    /// Describes run `run_id` of `owner`, or the owner's latest run when `run_id` is empty.
    async fn describe(
        &self,
        namespace: &str,
        owner: OwnerId<'_>,
        run_id: &str,
    ) -> Result<OwnerDescription, OwnerError>;

    /// How run `run_id` of `workflow_id` closed, from the tail of its History, or `None` while
    /// the run's last event is not a close event. A client that reads no History fails, so the
    /// caller closes nothing.
    async fn run_close(
        &self,
        _namespace: &str,
        _workflow_id: &str,
        _run_id: &str,
    ) -> Result<Option<RunClose>, OwnerError> {
        Err(OwnerError::Failed(
            "this owner client reads no History".to_string(),
        ))
    }
}
