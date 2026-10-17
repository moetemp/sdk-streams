//! What the stream layer asks Temporal about a stream's owner.
//!
//! A store can't see a Workflow close, so the layer above it asks: to find the chain a stream is
//! keyed by, to refuse a producer once the chain ended, and to end a read. The crate implements
//! this over its client, and tests use a fake.

use temporalio_common::protos::temporal::api::enums::v1::WorkflowExecutionStatus;

/// One run of a Workflow, as Temporal describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerDescription {
    /// The run described.
    pub run_id: String,
    /// The first run of the chain the run belongs to. Streams are keyed by it.
    pub first_run_id: String,
    /// How the run stands.
    pub status: WorkflowExecutionStatus,
}

impl OwnerDescription {
    /// Whether the run ended its chain. A run that continued as new did not, since its successor
    /// writes on.
    pub fn chain_ended(&self) -> bool {
        matches!(
            self.status,
            WorkflowExecutionStatus::Completed
                | WorkflowExecutionStatus::Failed
                | WorkflowExecutionStatus::Canceled
                | WorkflowExecutionStatus::Terminated
                | WorkflowExecutionStatus::TimedOut
        )
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
    /// Temporal holds no such Workflow or run, or no longer does.
    #[error("not found: {0}")]
    NotFound(String),
    /// Temporal could not answer.
    #[error("{0}")]
    Failed(String),
}

/// Temporal, as the stream layer asks it about owners.
#[async_trait::async_trait]
pub trait OwnerClient: Send + Sync {
    /// Describes run `run_id` of `workflow_id`, or the Workflow id's latest run when `run_id` is
    /// empty.
    async fn describe(
        &self,
        namespace: &str,
        workflow_id: &str,
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
