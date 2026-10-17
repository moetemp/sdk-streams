//! Streams for lang: one process-wide stream service over the configured store, asking the
//! client's server about streams' owners.

use crate::{OwnerClient, OwnerDescription, OwnerError, StreamResult, StreamService, proto};
use prost::Message;
use std::sync::Arc;
use temporalio_client::Connection;
use temporalio_common::{
    protos::{
        google::rpc::Status as RpcStatus,
        temporal::api::{
            common::v1::WorkflowExecution,
            workflowservice::v1::{
                DescribeWorkflowExecutionRequest, DescribeWorkflowExecutionResponse,
            },
        },
    },
    telemetry::metrics::TemporalMeter,
};
use tonic::IntoRequest;

/// Connects to the store `config` names. The service asks `connection`'s server about streams'
/// owners, and counts on `meter` when lang gives its runtime's meter.
pub async fn connect_stream_service(
    config: proto::StreamStoreConfig,
    connection: Connection,
    meter: Option<TemporalMeter>,
) -> StreamResult<Arc<StreamService>> {
    let owner = Arc::new(ConnectionOwnerClient { connection });
    let mut service = StreamService::connect(config, owner).await?;
    if let Some(meter) = meter {
        service = service.with_meter(meter);
    }
    Ok(Arc::new(service))
}

/// Asks a Temporal server about owners through a client connection.
struct ConnectionOwnerClient {
    connection: Connection,
}

#[async_trait::async_trait]
impl OwnerClient for ConnectionOwnerClient {
    async fn describe(
        &self,
        namespace: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<OwnerDescription, OwnerError> {
        let response = self
            .connection
            .workflow_service()
            .describe_workflow_execution(
                DescribeWorkflowExecutionRequest {
                    namespace: namespace.to_string(),
                    execution: Some(WorkflowExecution {
                        workflow_id: workflow_id.to_string(),
                        run_id: run_id.to_string(),
                    }),
                }
                .into_request(),
            )
            .await
            .map_err(owner_error)?;
        owner_description(response.into_inner())
    }
}

fn owner_error(status: tonic::Status) -> OwnerError {
    // A missing namespace says nothing about the Workflow. A failover or a namespace being
    // set up answers it for a while, and a chain must not close on it.
    if status.code() == tonic::Code::NotFound && !namespace_not_found(&status) {
        OwnerError::NotFound(status.message().to_string())
    } else {
        OwnerError::Failed(status.to_string())
    }
}

fn namespace_not_found(status: &tonic::Status) -> bool {
    RpcStatus::decode(status.details()).is_ok_and(|details| {
        details.details.iter().any(|detail| {
            detail
                .type_url
                .ends_with("/temporal.api.errordetails.v1.NamespaceNotFoundFailure")
        })
    })
}

fn owner_description(
    response: DescribeWorkflowExecutionResponse,
) -> Result<OwnerDescription, OwnerError> {
    let info = response
        .workflow_execution_info
        .ok_or_else(|| OwnerError::Failed("the server described no execution".to_string()))?;
    let run_id = info
        .execution
        .as_ref()
        .map(|execution| execution.run_id.clone())
        .unwrap_or_default();
    // A run that started its chain may leave the first run id unset.
    let first_run_id = if info.first_run_id.is_empty() {
        run_id.clone()
    } else {
        info.first_run_id.clone()
    };
    Ok(OwnerDescription {
        status: info.status(),
        run_id,
        first_run_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_common::protos::temporal::api::{
        enums::v1::WorkflowExecutionStatus, workflow::v1::WorkflowExecutionInfo,
    };

    fn described(run_id: &str, first_run_id: &str) -> DescribeWorkflowExecutionResponse {
        DescribeWorkflowExecutionResponse {
            workflow_execution_info: Some(WorkflowExecutionInfo {
                execution: Some(WorkflowExecution {
                    workflow_id: "wf".to_string(),
                    run_id: run_id.to_string(),
                }),
                first_run_id: first_run_id.to_string(),
                status: WorkflowExecutionStatus::ContinuedAsNew as i32,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn a_description_names_the_run_its_chain_and_its_status() {
        assert_eq!(
            owner_description(described("run-2", "run-1")).unwrap(),
            OwnerDescription {
                run_id: "run-2".to_string(),
                first_run_id: "run-1".to_string(),
                status: WorkflowExecutionStatus::ContinuedAsNew,
            }
        );
    }

    #[test]
    fn a_run_without_a_first_run_id_starts_its_chain() {
        assert_eq!(
            owner_description(described("run-1", ""))
                .unwrap()
                .first_run_id,
            "run-1"
        );
    }

    #[test]
    fn a_missing_namespace_is_a_failure_not_a_gone_owner() {
        let details = RpcStatus {
            code: tonic::Code::NotFound as i32,
            message: "namespace ns not found".to_string(),
            details: vec![prost_types::Any {
                type_url:
                    "type.googleapis.com/temporal.api.errordetails.v1.NamespaceNotFoundFailure"
                        .to_string(),
                value: Vec::new(),
            }],
        };
        let status = tonic::Status::with_details(
            tonic::Code::NotFound,
            "namespace ns not found",
            details.encode_to_vec().into(),
        );
        assert!(matches!(owner_error(status), OwnerError::Failed(_)));
    }

    #[test]
    fn not_found_says_the_owner_is_gone_and_anything_else_that_the_server_failed() {
        assert!(matches!(
            owner_error(tonic::Status::not_found("no such workflow")),
            OwnerError::NotFound(message) if message == "no such workflow"
        ));
        assert!(matches!(
            owner_error(tonic::Status::unavailable("down")),
            OwnerError::Failed(_)
        ));
        assert!(matches!(
            owner_description(DescribeWorkflowExecutionResponse::default()),
            Err(OwnerError::Failed(_))
        ));
    }
}
