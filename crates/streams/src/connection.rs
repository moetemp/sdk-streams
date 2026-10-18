//! Streams for lang: one process-wide stream service over the configured store, asking the
//! client's server about streams' owners and sending it the stream notifications.

use crate::{
    CallerAttachment, CloseOutcome, NotifierClient, NotifierPoll, NotifyError, OwnerClient,
    OwnerDescription, OwnerError, OwnerId, OwnerStatus, RunClose, StreamNotification, StreamResult,
    StreamService, WorkflowAttachment,
    proto::{self, ChainId},
    protos::temporal::api::{
        enums::v1::StreamOwnerKind,
        stream::v1::{self, StreamReference, WorkflowWakeTarget},
        workflowservice::v1::{
            AttachStreamCallbackRequest, AttachStreamCallbackResponse,
            DescribeStreamNotifierRequest, DescribeStreamNotifierResponse,
            DetachStreamCallbackRequest, DetachStreamCallbackResponse, NotifyStreamRequest,
            NotifyStreamResponse, NotifyStreamsRequest, NotifyStreamsResponse,
            PollStreamNotifierRequest, PollStreamNotifierResponse, WaitStreamCallbackRequest,
            WaitStreamCallbackResponse,
        },
        workflowservice::v1::{
            ExecuteSyncNexusOperationRequest, ExecuteSyncNexusOperationResponse,
        },
    },
};
use prost::Message;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use temporalio_client::Connection;
use temporalio_common::{
    protos::{
        google::rpc::Status as RpcStatus,
        temporal::api::{
            common::v1::{WorkflowExecution, callback},
            enums::v1::{EventType, WorkflowExecutionStatus},
            history::v1::{HistoryEvent, history_event},
            workflowservice::v1::{
                DescribeActivityExecutionRequest, DescribeActivityExecutionResponse,
                DescribeWorkflowExecutionRequest, DescribeWorkflowExecutionResponse,
                GetWorkflowExecutionHistoryReverseRequest,
            },
        },
    },
    telemetry::metrics::TemporalMeter,
};
use tonic::IntoRequest;

/// The method paths of the stream notifier RPCs and the sync Nexus call. sdk-rust lists none of
/// them, so the crate calls each one by its path.
mod method {
    macro_rules! path {
        ($name:ident, $rpc:literal) => {
            pub(super) const $name: &str =
                concat!("/temporal.api.workflowservice.v1.WorkflowService/", $rpc);
        };
    }
    path!(ATTACH, "AttachStreamCallback");
    path!(WAIT, "WaitStreamCallback");
    path!(DETACH, "DetachStreamCallback");
    path!(NOTIFY, "NotifyStream");
    path!(NOTIFY_MANY, "NotifyStreams");
    path!(POLL, "PollStreamNotifier");
    path!(DESCRIBE, "DescribeStreamNotifier");
    path!(EXECUTE_SYNC_NEXUS, "ExecuteSyncNexusOperation");
}

/// How long a notifier poll may wait, as the client gives its own long polls. The server answers
/// at its own wait limit before that.
const POLL_TIMEOUT: Duration = Duration::from_secs(70);

/// Calls the RPC at `method` through `connection`, with its TLS, API key, headers and retries. The
/// namespace header routes the call, as the client's own RPCs do.
async fn notifier_call<Resp: Message + Default>(
    connection: &Connection,
    method: &'static str,
    namespace: &str,
    request: impl Message,
    timeout: Option<Duration>,
) -> Result<Resp, tonic::Status> {
    let mut request = tonic::Request::new(bytes::Bytes::from(request.encode_to_vec()));
    if let Ok(namespace) = namespace.parse() {
        request
            .metadata_mut()
            .insert("temporal-namespace", namespace);
    }
    if let Some(timeout) = timeout {
        request.set_timeout(timeout);
    }
    let response = connection.raw_unary(method, request).await?;
    Resp::decode(response.into_inner()).map_err(|error| {
        tonic::Status::internal(format!(
            "The server's answer to {method} did not decode: {error}"
        ))
    })
}

/// Runs one synchronous Nexus operation through `connection`'s server and answers with its result
/// or its failure. sdk-rust lists no stub for the RPC, so the call goes by its method path, with
/// the namespace header. The connection's retry policy applies, so the server can run one call
/// twice. That is safe for the calls the crate sends, which are reads, follows and attaches keyed
/// by the start's request id.
pub async fn execute_sync_nexus_operation(
    connection: &Connection,
    request: ExecuteSyncNexusOperationRequest,
) -> Result<ExecuteSyncNexusOperationResponse, tonic::Status> {
    let namespace = request.namespace.clone();
    notifier_call(
        connection,
        method::EXECUTE_SYNC_NEXUS,
        &namespace,
        request,
        None,
    )
    .await
}

/// Connects to the store `config` names. The service asks `connection`'s server about streams'
/// owners, sends the stream notifications the config turns on to it, attaches Workflow readers
/// there, and counts on `meter` when lang gives its runtime's meter.
pub async fn connect_stream_service(
    config: proto::StreamStoreConfig,
    connection: Connection,
    meter: Option<TemporalMeter>,
) -> StreamResult<Arc<StreamService>> {
    let owner = Arc::new(ConnectionOwnerClient {
        connection: connection.clone(),
    });
    let notifier = Arc::new(ConnectionNotifierClient { connection });
    let mut service = StreamService::connect_with_notifier(config, owner, Some(notifier)).await?;
    if let Some(meter) = meter {
        service = service.with_meter(meter);
    }
    Ok(Arc::new(service))
}

/// Sends stream notifications to a Temporal server's stream notifier through a client
/// connection.
struct ConnectionNotifierClient {
    connection: Connection,
}

#[async_trait::async_trait]
impl NotifierClient for ConnectionNotifierClient {
    async fn notify(&self, notification: StreamNotification) -> Result<(), NotifyError> {
        let request = notify_request(notification);
        let namespace = request.namespace.clone();
        notifier_call::<NotifyStreamResponse>(
            &self.connection,
            method::NOTIFY,
            &namespace,
            request,
            None,
        )
        .await
        .map(drop)
        .map_err(notify_error)
    }

    async fn notify_streams(
        &self,
        notifications: Vec<StreamNotification>,
    ) -> Result<(), NotifyError> {
        self.notify_streams_each(notifications)
            .await?
            .into_iter()
            .collect()
    }

    async fn notify_streams_each(
        &self,
        notifications: Vec<StreamNotification>,
    ) -> Result<Vec<Result<(), NotifyError>>, NotifyError> {
        let namespaces: Vec<_> = notifications
            .iter()
            .map(|n| n.chain.namespace.clone())
            .collect();
        let mut results: Vec<_> = namespaces.iter().map(|_| Ok(())).collect();
        for request in notify_streams_requests(notifications, self.connection.identity()) {
            // A request keeps the order of its namespace's notifications.
            let indexes: Vec<_> = (0..namespaces.len())
                .filter(|&index| namespaces[index] == request.namespace)
                .collect();
            let namespace = request.namespace.clone();
            let response: NotifyStreamsResponse = notifier_call(
                &self.connection,
                method::NOTIFY_MANY,
                &namespace,
                request,
                None,
            )
            .await
            .map_err(notify_error)?;
            // A server that answers no results applied every entry.
            for (index, result) in indexes.into_iter().zip(response.results) {
                if result.code != tonic::Code::Ok as i32 {
                    let code = tonic::Code::from(result.code);
                    results[index] = Err(notify_error(tonic::Status::new(code, result.message)));
                }
            }
        }
        Ok(results)
    }

    async fn attach_workflow(&self, attachment: WorkflowAttachment) -> Result<(), NotifyError> {
        let request = attach_request(attachment);
        let namespace = request.namespace.clone();
        notifier_call::<AttachStreamCallbackResponse>(
            &self.connection,
            method::ATTACH,
            &namespace,
            request,
            None,
        )
        .await
        .map(drop)
        .map_err(notify_error)
    }

    async fn attach_caller(&self, attachment: CallerAttachment) -> Result<(), NotifyError> {
        let request = caller_attach_request(attachment);
        let namespace = request.namespace.clone();
        notifier_call::<AttachStreamCallbackResponse>(
            &self.connection,
            method::ATTACH,
            &namespace,
            request,
            None,
        )
        .await
        .map(drop)
        .map_err(notify_error)
    }

    async fn wait_callback(
        &self,
        chain: ChainId,
        topic: String,
        request_id: String,
        after_counter: i64,
    ) -> Result<(), NotifyError> {
        let request = WaitStreamCallbackRequest {
            namespace: chain.namespace.clone(),
            stream_ref: Some(chain_reference(&chain, topic)),
            request_id,
            after_counter,
            identity: self.connection.identity().to_string(),
        };
        notifier_call::<WaitStreamCallbackResponse>(
            &self.connection,
            method::WAIT,
            &chain.namespace,
            request,
            None,
        )
        .await
        .map(drop)
        .map_err(notify_error)
    }

    async fn detach(
        &self,
        chain: ChainId,
        topic: String,
        request_id: String,
    ) -> Result<(), NotifyError> {
        let request = DetachStreamCallbackRequest {
            namespace: chain.namespace.clone(),
            stream_ref: Some(chain_reference(&chain, topic)),
            request_id,
            ..Default::default()
        };
        notifier_call::<DetachStreamCallbackResponse>(
            &self.connection,
            method::DETACH,
            &chain.namespace,
            request,
            None,
        )
        .await
        .map(drop)
        .map_err(notify_error)
    }

    async fn describe_notifier(
        &self,
        request: DescribeStreamNotifierRequest,
    ) -> Result<DescribeStreamNotifierResponse, NotifyError> {
        let namespace = request.namespace.clone();
        notifier_call(
            &self.connection,
            method::DESCRIBE,
            &namespace,
            request,
            None,
        )
        .await
        .map_err(notify_error)
    }

    async fn poll(
        &self,
        chain: ChainId,
        topic: String,
        after_counter: i64,
    ) -> Result<Option<NotifierPoll>, NotifyError> {
        let request = PollStreamNotifierRequest {
            namespace: chain.namespace.clone(),
            stream_ref: Some(chain_reference(&chain, topic)),
            after_counter,
            identity: self.connection.identity().to_string(),
        };
        let answer = notifier_call(
            &self.connection,
            method::POLL,
            &chain.namespace,
            request,
            Some(POLL_TIMEOUT),
        )
        .await;
        match answer {
            Ok(response) => Ok(Some(poll_answer(response))),
            // The server answers at its own wait limit, before the client's deadline. A deadline
            // that passes anyway asks for the same re-read.
            Err(status) if status.code() == tonic::Code::DeadlineExceeded => Ok(None),
            Err(status) => Err(notify_error(status)),
        }
    }
}

fn poll_answer(response: PollStreamNotifierResponse) -> NotifierPoll {
    NotifierPoll {
        counter: response.counter,
        position: response.position,
        closed: response.closed,
        close_result: response.close_result,
        close_failure: response.close_failure,
    }
}

/// The notifier of `chain`'s stream on `topic`. The run chain's first run keys the notifier, so one
/// notifier serves the stream across Continue-as-New and a new chain on the same Workflow id gets
/// its own. An Activity run is its own chain, so its notifier is keyed by the run. A Workflow
/// Activity's notifier is keyed by its Workflow, its scheduling run and its Activity id.
fn chain_reference(chain: &ChainId, topic: String) -> StreamReference {
    if chain.owner_kind == proto::StreamOwnerKind::WorkflowActivity as i32 {
        return StreamReference {
            owner_kind: StreamOwnerKind::WorkflowActivity as i32,
            workflow_id: chain.workflow_id.clone(),
            run_id: chain.first_run_id.clone(),
            topic,
            activity_id: chain.activity_id.clone(),
        };
    }
    if chain.owner_kind == proto::StreamOwnerKind::Activity as i32 {
        return StreamReference {
            owner_kind: StreamOwnerKind::Activity as i32,
            activity_id: chain.activity_id.clone(),
            run_id: chain.first_run_id.clone(),
            topic,
            ..Default::default()
        };
    }
    StreamReference {
        owner_kind: StreamOwnerKind::Workflow as i32,
        workflow_id: chain.workflow_id.clone(),
        run_id: chain.first_run_id.clone(),
        topic,
        ..Default::default()
    }
}

fn attach_request(attachment: WorkflowAttachment) -> AttachStreamCallbackRequest {
    AttachStreamCallbackRequest {
        namespace: attachment.chain.namespace.clone(),
        stream_ref: Some(chain_reference(&attachment.chain, attachment.topic)),
        request_id: attachment.request_id,
        workflow_target: Some(WorkflowWakeTarget {
            workflow_id: attachment.workflow_id,
            run_id: attachment.run_id,
            subscription_id: attachment.subscription_id,
        }),
        idle: attachment.idle,
        ..Default::default()
    }
}

fn caller_attach_request(attachment: CallerAttachment) -> AttachStreamCallbackRequest {
    AttachStreamCallbackRequest {
        namespace: attachment.chain.namespace.clone(),
        stream_ref: Some(chain_reference(&attachment.chain, attachment.topic)),
        request_id: attachment.request_id,
        callback: Some(callback::Nexus {
            url: attachment.callback_url,
            header: attachment.callback_headers,
        }),
        operation_token: attachment.operation_token,
        start_time: Some(attachment.start_time.into()),
        idle: attachment.idle,
        ..Default::default()
    }
}

/// One `NotifyStreams` request per namespace, since a call names one.
fn notify_streams_requests(
    notifications: Vec<StreamNotification>,
    identity: &str,
) -> Vec<NotifyStreamsRequest> {
    let mut by_namespace: BTreeMap<String, Vec<v1::StreamNotification>> = BTreeMap::new();
    for notification in notifications {
        by_namespace
            .entry(notification.chain.namespace.clone())
            .or_default()
            .push(v1::StreamNotification {
                stream_ref: Some(chain_reference(&notification.chain, notification.topic)),
                position: notification.position,
                counter: notification.counter,
                ..Default::default()
            });
    }
    by_namespace
        .into_iter()
        .map(|(namespace, notifications)| NotifyStreamsRequest {
            namespace,
            notifications,
            identity: identity.to_string(),
        })
        .collect()
}

fn notify_request(notification: StreamNotification) -> NotifyStreamRequest {
    NotifyStreamRequest {
        stream_ref: Some(chain_reference(&notification.chain, notification.topic)),
        namespace: notification.chain.namespace,
        position: notification.position,
        counter: notification.counter,
        close: notification.close.is_some(),
        close_result: notification.close.as_ref().and_then(CloseOutcome::result),
        close_failure: match notification.close {
            Some(CloseOutcome::Failure(failure)) => Some(failure),
            _ => None,
        },
        ..Default::default()
    }
}

/// Answers that mean the server will never take the notification, so a retry can't help.
fn notify_error(status: tonic::Status) -> NotifyError {
    use tonic::Code;
    match status.code() {
        Code::Unimplemented => NotifyError::Unsupported(status.to_string()),
        Code::FailedPrecondition if namespace_in_transition(&status) => {
            NotifyError::Failed(status.to_string())
        }
        Code::InvalidArgument
        | Code::NotFound
        | Code::PermissionDenied
        | Code::Unauthenticated
        | Code::FailedPrecondition => NotifyError::Refused(status.to_string()),
        _ => NotifyError::Failed(status.to_string()),
    }
}

/// A namespace that's failing over answers `FailedPrecondition` too, but only until the
/// redirection sees the active cluster, so a close mustn't stop on it.
fn namespace_in_transition(status: &tonic::Status) -> bool {
    use temporalio_common::protos::{
        google::rpc::Status as RpcStatus,
        temporal::api::errordetails::v1::{
            NamespaceInvalidStateFailure, NamespaceNotActiveFailure,
        },
    };
    let Ok(details) = <RpcStatus as prost::Message>::decode(status.details()) else {
        return false;
    };
    details.details.iter().any(|detail| {
        detail.to_msg::<NamespaceNotActiveFailure>().is_ok()
            || detail.to_msg::<NamespaceInvalidStateFailure>().is_ok()
    })
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
        owner: OwnerId<'_>,
        run_id: &str,
    ) -> Result<OwnerDescription, OwnerError> {
        match owner {
            OwnerId::Workflow(workflow_id) => {
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
            OwnerId::Activity(activity_id) => {
                let response = self
                    .connection
                    .workflow_service()
                    .describe_activity_execution(
                        DescribeActivityExecutionRequest {
                            namespace: namespace.to_string(),
                            activity_id: activity_id.to_string(),
                            run_id: run_id.to_string(),
                            ..Default::default()
                        }
                        .into_request(),
                    )
                    .await
                    .map_err(owner_error)?;
                activity_owner_description(response.into_inner())
            }
            OwnerId::WorkflowActivity {
                workflow_id,
                activity_id,
            } => {
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
                workflow_activity_owner_description(response.into_inner(), activity_id)
            }
        }
    }

    async fn run_close(
        &self,
        namespace: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<Option<RunClose>, OwnerError> {
        let mut scan = CloseScan::default();
        let mut next_page_token = vec![];
        loop {
            let response = self
                .connection
                .workflow_service()
                .get_workflow_execution_history_reverse(
                    GetWorkflowExecutionHistoryReverseRequest {
                        namespace: namespace.to_string(),
                        execution: Some(WorkflowExecution {
                            workflow_id: workflow_id.to_string(),
                            run_id: run_id.to_string(),
                        }),
                        maximum_page_size: REVERSE_PAGE_SIZE,
                        next_page_token,
                    }
                    .into_request(),
                )
                .await
                .map_err(owner_error)?
                .into_inner();
            let events = response.history.map(|h| h.events).unwrap_or_default();
            if let Some(done) = scan.page(&events) {
                return Ok(done);
            }
            if response.next_page_token.is_empty() {
                return Ok(scan.end());
            }
            next_page_token = response.next_page_token;
        }
    }
}

/// The events one reverse History page asks for. A final task with many commands can push its
/// `WorkflowTaskCompleted` past the first page, so the read goes on until it finds it.
const REVERSE_PAGE_SIZE: i32 = 100;

/// Reads a run's History from its last event back, as far as the run's close needs.
#[derive(Default)]
struct CloseScan {
    /// The status of the close event, once the first event was read.
    status: Option<WorkflowExecutionStatus>,
}

impl CloseScan {
    /// Reads one page, newest first. Answers when the scan is done, with `None` inside for a run
    /// whose last event is not a close event.
    fn page(&mut self, events: &[HistoryEvent]) -> Option<Option<RunClose>> {
        for event in events {
            let Some(status) = self.status else {
                match close_status(event.event_type()) {
                    Some(status) => self.status = Some(status),
                    None => return Some(None),
                }
                continue;
            };
            if let Some(history_event::Attributes::WorkflowTaskCompletedEventAttributes(attrs)) =
                &event.attributes
            {
                return Some(Some(RunClose {
                    status,
                    closing_task_started_event_id: Some(attrs.started_event_id),
                }));
            }
        }
        None
    }

    /// Answers when History ended before the scan found a `WorkflowTaskCompleted`.
    fn end(self) -> Option<RunClose> {
        self.status.map(|status| RunClose {
            status,
            closing_task_started_event_id: None,
        })
    }
}

fn close_status(event_type: EventType) -> Option<WorkflowExecutionStatus> {
    Some(match event_type {
        EventType::WorkflowExecutionCompleted => WorkflowExecutionStatus::Completed,
        EventType::WorkflowExecutionFailed => WorkflowExecutionStatus::Failed,
        EventType::WorkflowExecutionCanceled => WorkflowExecutionStatus::Canceled,
        EventType::WorkflowExecutionTerminated => WorkflowExecutionStatus::Terminated,
        EventType::WorkflowExecutionTimedOut => WorkflowExecutionStatus::TimedOut,
        EventType::WorkflowExecutionContinuedAsNew => WorkflowExecutionStatus::ContinuedAsNew,
        _ => return None,
    })
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
        status: OwnerStatus::of_workflow(info.status()),
        run_id,
        first_run_id,
    })
}

/// A Workflow Activity is pending while its scheduling run lists it, through retries and backoff.
fn workflow_activity_owner_description(
    response: DescribeWorkflowExecutionResponse,
    activity_id: &str,
) -> Result<OwnerDescription, OwnerError> {
    let pending = response
        .pending_activities
        .iter()
        .any(|activity| activity.activity_id == activity_id);
    let status = response
        .workflow_execution_info
        .as_ref()
        .map(|info| info.status())
        .unwrap_or_default();
    Ok(OwnerDescription {
        status: OwnerStatus::of_workflow_activity(status, pending),
        ..owner_description(response)?
    })
}

/// A standalone Activity run is a chain of one run, so it is its own first run.
fn activity_owner_description(
    response: DescribeActivityExecutionResponse,
) -> Result<OwnerDescription, OwnerError> {
    let info = response
        .info
        .ok_or_else(|| OwnerError::Failed("the server described no Activity".to_string()))?;
    let run_id = if response.run_id.is_empty() {
        info.run_id.clone()
    } else {
        response.run_id
    };
    if run_id.is_empty() {
        return Err(OwnerError::Failed(
            "the server described an Activity with no run id".to_string(),
        ));
    }
    Ok(OwnerDescription {
        status: OwnerStatus::of_activity(info.status()),
        first_run_id: run_id.clone(),
        run_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_client::callback_based::GrpcRequest;
    use temporalio_common::protos::temporal::api::{
        history::v1::{
            WorkflowExecutionCompletedEventAttributes, WorkflowTaskCompletedEventAttributes,
        },
        workflow::v1::WorkflowExecutionInfo,
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
                status: OwnerStatus::ContinuedAsNew,
            }
        );
    }

    #[test]
    fn an_activity_run_is_its_own_chain_and_any_terminal_status_ends_it() {
        use temporalio_common::protos::temporal::api::{
            activity::v1::ActivityExecutionInfo, enums::v1::ActivityExecutionStatus,
        };
        let described = |status: ActivityExecutionStatus| DescribeActivityExecutionResponse {
            run_id: "act-run".to_string(),
            info: Some(ActivityExecutionInfo {
                activity_id: "gen".to_string(),
                run_id: "act-run".to_string(),
                status: status as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            activity_owner_description(described(ActivityExecutionStatus::Paused)).unwrap(),
            OwnerDescription {
                run_id: "act-run".to_string(),
                first_run_id: "act-run".to_string(),
                status: OwnerStatus::Running,
            }
        );
        for status in [
            ActivityExecutionStatus::Completed,
            ActivityExecutionStatus::Failed,
            ActivityExecutionStatus::Canceled,
            ActivityExecutionStatus::Terminated,
            ActivityExecutionStatus::TimedOut,
        ] {
            assert!(
                activity_owner_description(described(status))
                    .unwrap()
                    .chain_ended(),
                "{status:?}"
            );
        }
        assert!(matches!(
            activity_owner_description(DescribeActivityExecutionResponse::default()),
            Err(OwnerError::Failed(_))
        ));
    }

    #[test]
    fn a_workflow_activity_runs_while_its_running_run_lists_it_as_pending() {
        use temporalio_common::protos::temporal::api::workflow::v1::PendingActivityInfo;
        let described = |status: WorkflowExecutionStatus, pending: &[&str]| {
            let mut response = described("run-1", "run-1");
            if let Some(info) = &mut response.workflow_execution_info {
                info.status = status as i32;
            }
            response.pending_activities = pending
                .iter()
                .map(|id| PendingActivityInfo {
                    activity_id: id.to_string(),
                    ..Default::default()
                })
                .collect();
            response
        };
        let status = |status, pending: &[&str]| {
            workflow_activity_owner_description(described(status, pending), "tool")
                .unwrap()
                .status
        };
        use WorkflowExecutionStatus::*;
        assert_eq!(status(Running, &["other", "tool"]), OwnerStatus::Running);
        assert_eq!(status(Paused, &["tool"]), OwnerStatus::Running);
        assert_eq!(status(Running, &["other"]), OwnerStatus::ActivityGone);
        for closed in [Completed, Failed, ContinuedAsNew, Terminated, TimedOut] {
            assert_eq!(status(closed, &["tool"]), OwnerStatus::Ended, "{closed:?}");
        }
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

    #[test]
    fn a_notification_names_the_stream_by_its_chains_first_run() {
        let request = notify_request(StreamNotification {
            chain: proto::ChainId {
                namespace: "ns".to_string(),
                workflow_id: "wf".to_string(),
                first_run_id: "run-1".to_string(),
                ..Default::default()
            },
            topic: "tokens".to_string(),
            position: "cursor".to_string(),
            counter: 7,
            close: None,
        });
        assert_eq!(request.namespace, "ns");
        assert_eq!(
            request.stream_ref,
            Some(StreamReference {
                owner_kind: StreamOwnerKind::Workflow as i32,
                workflow_id: "wf".to_string(),
                run_id: "run-1".to_string(),
                topic: "tokens".to_string(),
                ..Default::default()
            })
        );
        assert_eq!((request.position.as_str(), request.counter), ("cursor", 7));
        assert!(!request.close);
        assert!(request.close_result.is_none());
    }

    fn chain() -> ChainId {
        ChainId {
            namespace: "default".to_string(),
            workflow_id: "owner-1".to_string(),
            first_run_id: "chain-1".to_string(),
            ..Default::default()
        }
    }

    fn caller_attachment() -> CallerAttachment {
        CallerAttachment {
            chain: chain(),
            topic: "tokens".to_string(),
            request_id: "attach-1".to_string(),
            callback_url: "temporal://system".to_string(),
            callback_headers: [("nexus-callback-token".to_string(), "abc".to_string())].into(),
            operation_token: "token".to_string(),
            start_time: std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000),
            idle: true,
        }
    }

    #[test]
    fn a_caller_attach_names_the_chains_notifier_and_carries_the_start() {
        let request = caller_attach_request(caller_attachment());
        assert_eq!(request.namespace, "default");
        assert_eq!(
            request.stream_ref,
            Some(StreamReference {
                owner_kind: StreamOwnerKind::Workflow as i32,
                workflow_id: "owner-1".to_string(),
                run_id: "chain-1".to_string(),
                topic: "tokens".to_string(),
                ..Default::default()
            })
        );
        assert_eq!(request.request_id, "attach-1");
        let callback = request.callback.unwrap();
        assert_eq!(callback.url, "temporal://system");
        assert_eq!(callback.header["nexus-callback-token"], "abc");
        assert_eq!(request.operation_token, "token");
        assert_eq!(request.start_time.unwrap().seconds, 1_700_000_000);
        assert!(request.workflow_target.is_none(), "exactly one target");
        assert!(
            request.idle,
            "the reader wakes the callback when it is caught up"
        );
    }

    #[test]
    fn a_notification_of_a_workflow_activity_stream_names_its_scheduling_run() {
        let request = notify_request(StreamNotification {
            chain: proto::ChainId {
                namespace: "ns".to_string(),
                workflow_id: "agent".to_string(),
                first_run_id: "run-2".to_string(),
                owner_kind: proto::StreamOwnerKind::WorkflowActivity as i32,
                activity_id: "tool-42".to_string(),
            },
            topic: "tokens".to_string(),
            position: "cursor".to_string(),
            counter: 3,
            close: None,
        });
        assert_eq!(
            request.stream_ref,
            Some(StreamReference {
                owner_kind: StreamOwnerKind::WorkflowActivity as i32,
                workflow_id: "agent".to_string(),
                run_id: "run-2".to_string(),
                topic: "tokens".to_string(),
                activity_id: "tool-42".to_string(),
            })
        );
    }

    #[test]
    fn a_notification_of_an_activity_stream_names_the_activity_run() {
        let request = notify_request(StreamNotification {
            chain: proto::ChainId {
                namespace: "ns".to_string(),
                first_run_id: "act-run".to_string(),
                owner_kind: proto::StreamOwnerKind::Activity as i32,
                activity_id: "gen".to_string(),
                ..Default::default()
            },
            topic: "tokens".to_string(),
            position: "cursor".to_string(),
            counter: 7,
            close: Some(CloseOutcome::Failure(Default::default())),
        });
        assert_eq!(
            request.stream_ref,
            Some(StreamReference {
                owner_kind: StreamOwnerKind::Activity as i32,
                workflow_id: String::new(),
                run_id: "act-run".to_string(),
                topic: "tokens".to_string(),
                activity_id: "gen".to_string(),
            })
        );
        assert!(request.close && request.close_failure.is_some());
    }

    #[test]
    fn a_batch_of_notifications_goes_out_once_per_namespace() {
        let notification = |namespace: &str, workflow_id: &str, counter: i64| StreamNotification {
            chain: proto::ChainId {
                namespace: namespace.to_string(),
                workflow_id: workflow_id.to_string(),
                first_run_id: "run-1".to_string(),
                ..Default::default()
            },
            topic: "tokens".to_string(),
            position: format!("cursor-{counter}"),
            counter,
            close: None,
        };
        let requests = notify_streams_requests(
            vec![
                notification("b", "wf-1", 1),
                notification("a", "wf-2", 2),
                notification("b", "wf-3", 3),
            ],
            "driver",
        );
        let shape: Vec<_> = requests
            .iter()
            .map(|r| {
                (
                    r.namespace.as_str(),
                    r.identity.as_str(),
                    r.notifications
                        .iter()
                        .map(|n| n.counter)
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        assert_eq!(
            shape,
            [("a", "driver", vec![2]), ("b", "driver", vec![1, 3])]
        );
        let first = &requests[1].notifications[0];
        assert_eq!(first.position, "cursor-1");
        assert_eq!(first.stream_ref.as_ref().unwrap().workflow_id, "wf-1");
        assert_eq!(first.stream_ref.as_ref().unwrap().run_id, "run-1");
    }

    #[test]
    fn an_attach_names_the_chains_notifier_and_wakes_the_reader() {
        let request = attach_request(WorkflowAttachment {
            chain: proto::ChainId {
                namespace: "ns".to_string(),
                workflow_id: "owner".to_string(),
                first_run_id: "chain-1".to_string(),
                ..Default::default()
            },
            topic: "tokens".to_string(),
            request_id: "jetty/reader-run/1".to_string(),
            workflow_id: "reader".to_string(),
            run_id: "reader-run".to_string(),
            subscription_id: "1".to_string(),
            idle: true,
        });
        assert_eq!(request.namespace, "ns");
        assert_eq!(
            request.stream_ref,
            Some(StreamReference {
                owner_kind: StreamOwnerKind::Workflow as i32,
                workflow_id: "owner".to_string(),
                run_id: "chain-1".to_string(),
                topic: "tokens".to_string(),
                ..Default::default()
            })
        );
        assert_eq!(request.request_id, "jetty/reader-run/1");
        assert_eq!(
            request.workflow_target,
            Some(WorkflowWakeTarget {
                workflow_id: "reader".to_string(),
                run_id: "reader-run".to_string(),
                subscription_id: "1".to_string(),
            })
        );
        assert!(request.callback.is_none(), "exactly one target");
        assert!(
            request.idle,
            "the reader asks for each wake once it is caught up"
        );
    }

    #[test]
    fn only_answers_a_retry_cant_change_refuse_a_notification() {
        assert!(matches!(
            notify_error(tonic::Status::new(tonic::Code::Unimplemented, "")),
            NotifyError::Unsupported(_)
        ));
        for code in [
            tonic::Code::InvalidArgument,
            tonic::Code::NotFound,
            tonic::Code::PermissionDenied,
            tonic::Code::Unauthenticated,
            tonic::Code::FailedPrecondition,
        ] {
            assert!(
                matches!(
                    notify_error(tonic::Status::new(code, "")),
                    NotifyError::Refused(_)
                ),
                "{code:?}"
            );
        }
        for code in [
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
            tonic::Code::ResourceExhausted,
            tonic::Code::Internal,
        ] {
            assert!(
                matches!(
                    notify_error(tonic::Status::new(code, "")),
                    NotifyError::Failed(_)
                ),
                "{code:?}"
            );
        }
    }

    fn failed_precondition_with<T: prost::Message + prost::Name>(detail: &T) -> tonic::Status {
        let status = temporalio_common::protos::google::rpc::Status {
            code: tonic::Code::FailedPrecondition as i32,
            message: "namespace".to_string(),
            details: vec![prost_types::Any::from_msg(detail).unwrap()],
        };
        tonic::Status::with_details(
            tonic::Code::FailedPrecondition,
            "namespace",
            prost::Message::encode_to_vec(&status).into(),
        )
    }

    #[test]
    fn a_namespace_in_failover_fails_a_notification_for_now() {
        use temporalio_common::protos::temporal::api::errordetails::v1::{
            NamespaceInvalidStateFailure, NamespaceNotActiveFailure,
        };
        for status in [
            failed_precondition_with(&NamespaceNotActiveFailure::default()),
            failed_precondition_with(&NamespaceInvalidStateFailure::default()),
        ] {
            assert!(matches!(notify_error(status), NotifyError::Failed(_)));
        }
    }

    /// A connection whose server answers each notifier call with `answer`, and the calls it saw.
    async fn served(
        answer: impl Fn(&GrpcRequest) -> Result<Vec<u8>, tonic::Status> + Send + Sync + 'static,
    ) -> (
        ConnectionNotifierClient,
        Arc<std::sync::Mutex<Vec<GrpcRequest>>>,
    ) {
        use temporalio_client::callback_based::{CallbackBasedGrpcService, GrpcSuccessResponse};
        let seen: Arc<std::sync::Mutex<Vec<GrpcRequest>>> = Default::default();
        let recorded = seen.clone();
        let answer = Arc::new(answer);
        let service = CallbackBasedGrpcService {
            callback: Arc::new(move |request| {
                let (recorded, answer) = (recorded.clone(), answer.clone());
                Box::pin(async move {
                    let proto = if request.rpc == "GetSystemInfo" {
                        Ok(vec![])
                    } else {
                        let answered = answer(&request);
                        recorded.lock().unwrap().push(request);
                        answered
                    };
                    proto.map(|proto| GrpcSuccessResponse {
                        headers: Default::default(),
                        proto,
                    })
                })
            }),
        };
        let options = temporalio_client::ConnectionOptions::new(
            url::Url::parse("http://localhost:7233").unwrap(),
        )
        .service_override(service)
        .dns_load_balancing(None)
        .build();
        let connection = Connection::connect(options).await.unwrap();
        (ConnectionNotifierClient { connection }, seen)
    }

    fn workflow_chain() -> ChainId {
        proto::ChainId {
            namespace: "ns".to_string(),
            workflow_id: "wf".to_string(),
            first_run_id: "run-1".to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_notification_goes_to_the_rpc_by_its_path_with_the_namespace_header() {
        let (client, seen) = served(|_| Ok(vec![])).await;
        client
            .notify(StreamNotification {
                chain: workflow_chain(),
                topic: "tokens".to_string(),
                position: "cursor".to_string(),
                counter: 7,
                close: None,
            })
            .await
            .unwrap();
        let seen = seen.lock().unwrap();
        let [call] = seen.as_slice() else {
            panic!("one call, not {seen:?}");
        };
        assert_eq!(
            (call.service.as_str(), call.rpc.as_str()),
            (
                "temporal.api.workflowservice.v1.WorkflowService",
                "NotifyStream"
            )
        );
        assert_eq!(call.headers["temporal-namespace"], "ns");
        let request = NotifyStreamRequest::decode(call.proto.clone()).unwrap();
        assert_eq!((request.namespace.as_str(), request.counter), ("ns", 7));
    }

    #[tokio::test]
    async fn a_caller_attach_goes_to_the_rpc_by_its_path_and_a_refusal_keeps_its_code() {
        let (client, seen) = served(|request| match request.rpc.as_str() {
            "AttachStreamCallback" if request.headers["temporal-namespace"] == "default" => {
                Ok(vec![])
            }
            _ => Err(tonic::Status::permission_denied("not this namespace")),
        })
        .await;
        client.attach_caller(caller_attachment()).await.unwrap();
        {
            let seen = seen.lock().unwrap();
            let [call] = seen.as_slice() else {
                panic!("one call, not {seen:?}");
            };
            assert_eq!(
                (call.service.as_str(), call.rpc.as_str()),
                (
                    "temporal.api.workflowservice.v1.WorkflowService",
                    "AttachStreamCallback"
                )
            );
            let request = AttachStreamCallbackRequest::decode(call.proto.clone()).unwrap();
            assert_eq!(request, caller_attach_request(caller_attachment()));
        }
        let mut other = caller_attachment();
        other.chain.namespace = "other".to_string();
        let refused = client.attach_caller(other).await.unwrap_err();
        assert!(
            matches!(refused, NotifyError::Refused(_)),
            "a refusal is final: {refused:?}"
        );
    }

    #[tokio::test]
    async fn a_sync_nexus_call_goes_by_its_path_and_answers_the_outcome() {
        use crate::protos::temporal::api::workflowservice::v1::{
            execute_sync_nexus_operation_response::Outcome,
        };
        use temporalio_common::protos::temporal::api::common::v1::Payload;
        let (client, seen) = served(|request| match request.rpc.as_str() {
            "ExecuteSyncNexusOperation" if request.headers["temporal-namespace"] == "ns" => {
                Ok(ExecuteSyncNexusOperationResponse {
                    outcome: Some(Outcome::Result(Payload {
                        data: b"done".to_vec(),
                        ..Default::default()
                    })),
                }
                .encode_to_vec())
            }
            _ => Err(tonic::Status::permission_denied("not this namespace")),
        })
        .await;
        let call = |namespace: &str| ExecuteSyncNexusOperationRequest {
            namespace: namespace.to_string(),
            request_id: "call-1".to_string(),
            endpoint: "streams".to_string(),
            service: "temporal.streams.v1.StreamService".to_string(),
            operation: "read".to_string(),
            ..Default::default()
        };
        let answer = execute_sync_nexus_operation(&client.connection, call("ns"))
            .await
            .unwrap();
        let Some(Outcome::Result(result)) = answer.outcome else {
            panic!("a result, not {answer:?}");
        };
        assert_eq!(result.data, b"done");
        {
            let seen = seen.lock().unwrap();
            let [sent] = seen.as_slice() else {
                panic!("one call, not {seen:?}");
            };
            assert_eq!(
                (sent.service.as_str(), sent.rpc.as_str()),
                (
                    "temporal.api.workflowservice.v1.WorkflowService",
                    "ExecuteSyncNexusOperation"
                )
            );
            let request = ExecuteSyncNexusOperationRequest::decode(sent.proto.clone()).unwrap();
            assert_eq!(request, call("ns"));
        }
        let refused = execute_sync_nexus_operation(&client.connection, call("other"))
            .await
            .unwrap_err();
        assert_eq!(refused.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn a_poll_decodes_the_answer_and_a_refusal_keeps_its_code() {
        let (client, seen) = served(|request| match request.rpc.as_str() {
            "PollStreamNotifier" => Ok(PollStreamNotifierResponse {
                counter: 9,
                position: "p".to_string(),
                ..Default::default()
            }
            .encode_to_vec()),
            _ => Err(tonic::Status::not_found("no notifier")),
        })
        .await;
        let poll = client
            .poll(workflow_chain(), "tokens".to_string(), 3)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((poll.counter, poll.position.as_str()), (9, "p"));
        let refused = client
            .attach_workflow(WorkflowAttachment {
                chain: workflow_chain(),
                topic: "tokens".to_string(),
                request_id: "r".to_string(),
                workflow_id: "reader".to_string(),
                run_id: "reader-run".to_string(),
                subscription_id: "1".to_string(),
                idle: true,
            })
            .await
            .unwrap_err();
        assert!(matches!(refused, NotifyError::Refused(_)), "{refused:?}");
        let described = client
            .describe_notifier(DescribeStreamNotifierRequest {
                namespace: "ns".to_string(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(
            matches!(described, NotifyError::Refused(_)),
            "{described:?}"
        );
        let rpcs: Vec<_> = seen.lock().unwrap().iter().map(|c| c.rpc.clone()).collect();
        assert_eq!(
            rpcs,
            [
                "PollStreamNotifier",
                "AttachStreamCallback",
                "DescribeStreamNotifier"
            ]
        );
    }

    /// One HTTP request the test listener got: its `Nexus-Operation-State` and its body.
    type Delivery = (String, String);

    /// Answers every request on `127.0.0.1:7899` with `200` and hands each one on, so a test sees
    /// what the server's callbacks deliver. One listener serves every live test, since the
    /// address can't be bound twice.
    fn deliveries() -> &'static std::sync::Mutex<std::sync::mpsc::Receiver<Delivery>> {
        static DELIVERIES: std::sync::OnceLock<
            std::sync::Mutex<std::sync::mpsc::Receiver<Delivery>>,
        > = std::sync::OnceLock::new();
        DELIVERIES.get_or_init(|| std::sync::Mutex::new(listen("127.0.0.1:7899")))
    }

    fn listen(address: &str) -> std::sync::mpsc::Receiver<Delivery> {
        use std::io::{BufRead, BufReader, Read, Write};
        let listener = std::net::TcpListener::bind(address).unwrap();
        let (sender, deliveries) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let (mut state, mut length, mut chunked) = (String::new(), 0usize, false);
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    let lower = line.to_ascii_lowercase();
                    if let Some(value) = lower.strip_prefix("nexus-operation-state:") {
                        state = value.trim().to_string();
                    }
                    if let Some(value) = lower.strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap_or(0);
                    }
                    if lower.starts_with("transfer-encoding:") && lower.contains("chunked") {
                        chunked = true;
                    }
                }
                let mut body = vec![];
                if chunked {
                    // A failed completion streams its failure in chunks.
                    loop {
                        let mut size = String::new();
                        if reader.read_line(&mut size).unwrap_or(0) == 0 {
                            break;
                        }
                        let size = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
                        let mut chunk = vec![0; size + 2];
                        let _ = reader.read_exact(&mut chunk);
                        if size == 0 {
                            break;
                        }
                        body.extend_from_slice(&chunk[..size]);
                    }
                } else {
                    body.resize(length, 0);
                    let _ = reader.read_exact(&mut body);
                }
                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n");
                if sender
                    .send((state, String::from_utf8_lossy(&body).to_string()))
                    .is_err()
                {
                    return;
                }
            }
        });
        deliveries
    }

    fn next_delivery() -> Delivery {
        deliveries()
            .lock()
            .unwrap()
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("the server delivered nothing")
    }

    /// A running owner Workflow that no Worker polls, a caller attached to its stream on
    /// `tokens`, and a stream service that notifies through the live server.
    struct Live {
        connection: Connection,
        service: Arc<StreamService>,
        stream: proto::StreamAddress,
        workflow_id: String,
        run_id: String,
        /// The chain the caller's operation token names.
        token_first_run_id: String,
        operation_token: String,
        // The live tests share the callback listener's address.
        _turn: tokio::sync::MutexGuard<'static, ()>,
    }

    static LIVE_TURN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Needs a server with the stream notifier and progress on, at `STREAMS_NOTIFIER_SERVER`
    /// (such as `http://127.0.0.1:7861`), whose `callback.allowedAddresses` takes
    /// `127.0.0.1:7899`. Answers `None` without one. Also answers `None` without
    /// `STREAMS_NOTIFIER_NEXUS_PROGRESS`, since a Jetty server sends progress only to Workflow
    /// wake targets.
    async fn live() -> Option<Live> {
        live_after(0).await
    }

    /// `live()`, with `appends` records written before the caller attaches. The server keeps no
    /// notifier before an attach, so it drops the notifications of these records.
    async fn live_after(appends: i64) -> Option<Live> {
        use temporalio_common::protos::temporal::api::{
            common::v1::WorkflowType, taskqueue::v1::TaskQueue,
            workflowservice::v1::StartWorkflowExecutionRequest,
        };
        let Ok(server) = std::env::var("STREAMS_NOTIFIER_SERVER") else {
            eprintln!("set STREAMS_NOTIFIER_SERVER to run the live notifier test");
            return None;
        };
        if std::env::var("STREAMS_NOTIFIER_NEXUS_PROGRESS").is_err() {
            eprintln!("set STREAMS_NOTIFIER_NEXUS_PROGRESS for a server with Nexus progress");
            return None;
        }
        let turn = LIVE_TURN.lock().await;
        let connection = Connection::connect(
            temporalio_client::ConnectionOptions::new(url::Url::parse(&server).unwrap()).build(),
        )
        .await
        .unwrap();
        let workflow_id = format!("cn-notifier-{}", uuid::Uuid::new_v4());
        // No Worker polls the owner, so the stream stays open.
        let started = connection
            .workflow_service()
            .start_workflow_execution(
                StartWorkflowExecutionRequest {
                    namespace: "default".to_string(),
                    workflow_id: workflow_id.clone(),
                    workflow_type: Some(WorkflowType {
                        name: "owner".to_string(),
                    }),
                    task_queue: Some(TaskQueue {
                        name: "cn-notifier-nobody".to_string(),
                        ..Default::default()
                    }),
                    request_id: uuid::Uuid::new_v4().to_string(),
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .unwrap()
            .into_inner();
        // An earlier live test may have left deliveries behind.
        while deliveries().lock().unwrap().try_recv().is_ok() {}
        let service = connect_stream_service(
            proto::StreamStoreConfig {
                store: Some(proto::stream_store_config::Store::Memory(
                    proto::MemoryStoreConfig::default(),
                )),
                notify_on_append: true,
                ..Default::default()
            },
            connection.clone(),
            None,
        )
        .await
        .unwrap();
        let stream = proto::StreamAddress {
            namespace: "default".to_string(),
            owner_kind: proto::StreamOwnerKind::Workflow as i32,
            workflow_id: workflow_id.clone(),
            run_id: String::new(),
            topic: "tokens".to_string(),
            ..Default::default()
        };
        use prost::Message;
        for sequence in 1..=appends {
            service
                .call("Append", &one_append(&stream, sequence).encode_to_vec())
                .await
                .unwrap();
        }
        let attached = proto::AttachCallerResponse::decode(
            service
                .call(
                    "AttachCaller",
                    &proto::AttachCallerRequest {
                        stream: Some(stream.clone()),
                        request_id: "caller".to_string(),
                        callback_url: "http://127.0.0.1:7899/callback".to_string(),
                        // The caller reads nothing, so only a first wait from the start gets
                        // its idle callback the first append's progress.
                        wake_after_counter: Some(0),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                )
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let token = crate::OperationToken::decode("default", &attached.operation_token).unwrap();
        Some(Live {
            connection,
            service,
            stream,
            workflow_id,
            run_id: started.run_id,
            token_first_run_id: token.chain_first_run_id,
            operation_token: attached.operation_token,
            _turn: turn,
        })
    }

    /// One record from one producer, as its batch at `sequence`.
    fn one_append(stream: &proto::StreamAddress, sequence: i64) -> proto::AppendRequest {
        proto::AppendRequest {
            stream: Some(stream.clone()),
            producer: Some(proto::append_request::Producer::Named(
                proto::NamedProducer {
                    producer_id: "producer".to_string(),
                    attempt: 1,
                    ..Default::default()
                },
            )),
            sequence,
            records: vec![proto::AppendRecord {
                kind: proto::StreamRecordKind::Data as i32,
                body: Some(
                    temporalio_common::protos::temporal::api::common::v1::Payload {
                        data: b"token".to_vec(),
                        ..Default::default()
                    },
                ),
                content_hash: vec![7; 32],
                ..Default::default()
            }],
            // Any value names the batch, since the producer writes each batch once.
            digest: vec![sequence as u8; 32],
        }
    }

    impl Live {
        async fn append_one(&self) {
            use prost::Message;
            self.service
                .call("Append", &one_append(&self.stream, 1).encode_to_vec())
                .await
                .unwrap();
            let (state, body) = next_delivery();
            assert_eq!(state, "running", "{body}");
            assert!(body.contains(r#""counter":"1""#), "{body}");
        }

        async fn close(&self, close: proto::CloseRequest) -> Delivery {
            use prost::Message;
            self.service
                .call("Close", &close.encode_to_vec())
                .await
                .unwrap();
            loop {
                // A progress retry may still arrive before the completion.
                let delivery = next_delivery();
                if delivery.0 != "running" {
                    return delivery;
                }
            }
        }

        async fn end(self) {
            use prost::Message;
            use temporalio_common::protos::temporal::api::workflowservice::v1 as service;
            // The owner check closes a terminated owner's stream after the test ended. Its
            // completion to a closed listener fails, and enough such failures open the server's
            // circuit breaker for the address the next run listens on. A detached caller gets
            // no completion, and a caller already completed is not found, which is fine.
            self.service
                .call(
                    "DetachCaller",
                    &proto::DetachCallerRequest {
                        namespace: "default".to_string(),
                        operation_token: self.operation_token.clone(),
                    }
                    .encode_to_vec(),
                )
                .await
                .unwrap();
            self.connection
                .workflow_service()
                .terminate_workflow_execution(
                    service::TerminateWorkflowExecutionRequest {
                        namespace: "default".to_string(),
                        workflow_execution: Some(WorkflowExecution {
                            workflow_id: self.workflow_id,
                            run_id: self.run_id,
                        }),
                        ..Default::default()
                    }
                    .into_request(),
                )
                .await
                .unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_live_notifier_hears_appends_as_progress_and_the_close_as_the_result() {
        use temporalio_common::protos::temporal::api::common::v1::Payload;
        let Some(live) = live().await else { return };
        assert_eq!(live.token_first_run_id, live.run_id);
        live.append_one().await;
        let (state, body) = live
            .close(proto::CloseRequest {
                stream: Some(live.stream.clone()),
                result: Some(Payload {
                    metadata: [("encoding".to_string(), b"json/plain".to_vec())].into(),
                    data: br#""summary""#.to_vec(),
                    ..Default::default()
                }),
                failure: None,
                closed_by: String::new(),
            })
            .await;
        assert_eq!(state, "succeeded", "{body}");
        assert!(body.contains("summary"), "{body}");
        live.end().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_live_caller_hears_the_records_that_landed_before_its_attach() {
        let Some(live) = live_after(2).await else {
            return;
        };
        let (state, body) = next_delivery();
        assert_eq!(state, "running", "{body}");
        assert!(body.contains(r#""counter":"2""#), "{body}");
        live.end().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_live_close_with_a_failure_fails_the_callers() {
        use temporalio_common::protos::temporal::api::failure::v1::{
            ApplicationFailureInfo, Failure, failure::FailureInfo,
        };
        let Some(live) = live().await else { return };
        live.append_one().await;
        let (state, body) = live
            .close(proto::CloseRequest {
                stream: Some(live.stream.clone()),
                result: None,
                failure: Some(Failure {
                    message: "the agent gave up".to_string(),
                    failure_info: Some(FailureInfo::ApplicationFailureInfo(
                        ApplicationFailureInfo {
                            r#type: "AgentGaveUp".to_string(),
                            non_retryable: true,
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                }),
                closed_by: String::new(),
            })
            .await;
        assert_eq!(state, "failed", "{body}");
        assert!(body.contains("the agent gave up"), "{body}");
        let described: DescribeStreamNotifierResponse = notifier_call(
            &live.connection,
            method::DESCRIBE,
            "default",
            DescribeStreamNotifierRequest {
                namespace: "default".to_string(),
                stream_ref: Some(StreamReference {
                    owner_kind: StreamOwnerKind::Workflow as i32,
                    workflow_id: live.workflow_id.clone(),
                    run_id: live.run_id.clone(),
                    topic: "tokens".to_string(),
                    ..Default::default()
                }),
            },
            None,
        )
        .await
        .unwrap();
        assert!(described.closed);
        assert_eq!(
            described.close_failure.map(|failure| failure.message),
            Some("the agent gave up".to_string())
        );
        live.end().await;
    }

    /// Needs a server with idle Workflow targets at `STREAMS_NOTIFIER_SERVER`, and a recheck
    /// interval longer than the test, since a recheck wakes the reader too.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_live_workflow_reader_gets_one_wake_per_idle_stretch() {
        use prost::Message;
        use std::time::Duration;
        use temporalio_common::protos::temporal::api::{
            common::v1::WorkflowType,
            taskqueue::v1::TaskQueue,
            workflowservice::v1::{
                StartWorkflowExecutionRequest, TerminateWorkflowExecutionRequest,
            },
        };
        let Ok(server) = std::env::var("STREAMS_NOTIFIER_SERVER") else {
            eprintln!("set STREAMS_NOTIFIER_SERVER to run the live notifier test");
            return;
        };
        let connection = Connection::connect(
            temporalio_client::ConnectionOptions::new(url::Url::parse(&server).unwrap()).build(),
        )
        .await
        .unwrap();
        // No Worker polls either Workflow, so the owner's stream stays open and the reader's
        // wakes stay pending.
        let mut runs = vec![];
        for role in ["owner", "reader"] {
            let workflow_id = format!("cn-idle-{role}-{}", uuid::Uuid::new_v4());
            let started = connection
                .workflow_service()
                .start_workflow_execution(
                    StartWorkflowExecutionRequest {
                        namespace: "default".to_string(),
                        workflow_id: workflow_id.clone(),
                        workflow_type: Some(WorkflowType {
                            name: role.to_string(),
                        }),
                        task_queue: Some(TaskQueue {
                            name: "cn-notifier-nobody".to_string(),
                            ..Default::default()
                        }),
                        request_id: uuid::Uuid::new_v4().to_string(),
                        ..Default::default()
                    }
                    .into_request(),
                )
                .await
                .unwrap()
                .into_inner();
            runs.push(WorkflowExecution {
                workflow_id,
                run_id: started.run_id,
            });
        }
        let (owner, reader) = (runs[0].clone(), runs[1].clone());
        let service = connect_stream_service(
            proto::StreamStoreConfig {
                store: Some(proto::stream_store_config::Store::Memory(
                    proto::MemoryStoreConfig::default(),
                )),
                notify_on_append: true,
                ..Default::default()
            },
            connection.clone(),
            None,
        )
        .await
        .unwrap();
        let stream = proto::StreamAddress {
            namespace: "default".to_string(),
            owner_kind: proto::StreamOwnerKind::Workflow as i32,
            workflow_id: owner.workflow_id.clone(),
            topic: "tokens".to_string(),
            ..Default::default()
        };
        let read = |after: String| {
            let request = proto::ReadRequest {
                stream: Some(stream.clone()),
                after,
                subscriber: Some(proto::ReadSubscriber {
                    workflow_id: reader.workflow_id.clone(),
                    run_id: reader.run_id.clone(),
                    subscription_id: "1".to_string(),
                    generation: 0,
                }),
                ..Default::default()
            };
            let service = service.clone();
            async move {
                proto::ReadResponse::decode(
                    service
                        .call("Read", &request.encode_to_vec())
                        .await
                        .unwrap()
                        .as_slice(),
                )
                .unwrap()
            }
        };
        let append = |sequence: i64| {
            let request = one_append(&stream, sequence);
            let service = service.clone();
            async move {
                service
                    .call("Append", &request.encode_to_vec())
                    .await
                    .unwrap();
            }
        };
        let delivered = || async {
            let described: DescribeStreamNotifierResponse = notifier_call(
                &connection,
                method::DESCRIBE,
                "default",
                DescribeStreamNotifierRequest {
                    namespace: "default".to_string(),
                    stream_ref: Some(StreamReference {
                        owner_kind: StreamOwnerKind::Workflow as i32,
                        workflow_id: owner.workflow_id.clone(),
                        run_id: owner.run_id.clone(),
                        topic: "tokens".to_string(),
                        ..Default::default()
                    }),
                },
                None,
            )
            .await
            .unwrap();
            let [callback] = described.callbacks.as_slice() else {
                panic!("one callback, not {:?}", described.callbacks);
            };
            assert_eq!(
                callback.workflow_target.as_ref().unwrap().workflow_id,
                reader.workflow_id
            );
            callback.delivered_counter
        };
        let delivered_reaches = |counter: i64| async move {
            for _ in 0..100 {
                if delivered().await >= counter {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            panic!("no wake after counter {counter}");
        };
        // A debounce window is one second, so a wake that is not sent by then is held.
        let settle = || tokio::time::sleep(Duration::from_secs(3));

        let empty = read(String::new()).await;
        assert!(empty.records.is_empty() && !empty.done);
        assert_eq!(delivered().await, 0, "an idle attach wakes nothing");
        append(1).await;
        delivered_reaches(1).await;
        append(2).await;
        settle().await;
        assert_eq!(delivered().await, 1, "the wait was used up");

        let busy = read(empty.cursor).await;
        assert_eq!(busy.records.len(), 2);
        let idle = read(busy.cursor).await;
        assert!(idle.records.is_empty());
        assert_eq!(idle.wake_after, 2);
        settle().await;
        assert_eq!(delivered().await, 1, "a wait at the counter wakes nothing");
        append(3).await;
        delivered_reaches(3).await;

        for run in [owner, reader] {
            connection
                .workflow_service()
                .terminate_workflow_execution(
                    TerminateWorkflowExecutionRequest {
                        namespace: "default".to_string(),
                        workflow_execution: Some(run),
                        ..Default::default()
                    }
                    .into_request(),
                )
                .await
                .unwrap();
        }
    }

    fn event(event_id: i64, attributes: history_event::Attributes) -> HistoryEvent {
        HistoryEvent {
            event_id,
            event_type: match &attributes {
                history_event::Attributes::WorkflowExecutionCompletedEventAttributes(_) => {
                    EventType::WorkflowExecutionCompleted
                }
                history_event::Attributes::WorkflowExecutionTerminatedEventAttributes(_) => {
                    EventType::WorkflowExecutionTerminated
                }
                history_event::Attributes::WorkflowTaskCompletedEventAttributes(_) => {
                    EventType::WorkflowTaskCompleted
                }
                history_event::Attributes::TimerStartedEventAttributes(_) => {
                    EventType::TimerStarted
                }
                _ => EventType::Unspecified,
            } as i32,
            attributes: Some(attributes),
            ..Default::default()
        }
    }

    fn completed() -> HistoryEvent {
        event(
            20,
            history_event::Attributes::WorkflowExecutionCompletedEventAttributes(
                WorkflowExecutionCompletedEventAttributes::default(),
            ),
        )
    }

    fn task_completed(started_event_id: i64) -> HistoryEvent {
        event(
            19,
            history_event::Attributes::WorkflowTaskCompletedEventAttributes(
                WorkflowTaskCompletedEventAttributes {
                    started_event_id,
                    ..Default::default()
                },
            ),
        )
    }

    fn timer() -> HistoryEvent {
        event(
            18,
            history_event::Attributes::TimerStartedEventAttributes(Default::default()),
        )
    }

    #[test]
    fn the_scan_finds_the_task_before_the_close_across_pages() {
        let mut scan = CloseScan::default();
        assert_eq!(scan.page(&[completed(), timer(), timer()]), None);
        assert_eq!(
            scan.page(&[timer(), task_completed(17), task_completed(12)]),
            Some(Some(RunClose {
                status: WorkflowExecutionStatus::Completed,
                closing_task_started_event_id: Some(17),
            }))
        );
    }

    #[test]
    fn a_run_whose_last_event_is_no_close_has_no_close() {
        let mut scan = CloseScan::default();
        assert_eq!(scan.page(&[timer(), task_completed(17)]), Some(None));
    }

    #[test]
    fn a_history_with_no_task_before_the_close_names_none() {
        let mut scan = CloseScan::default();
        assert_eq!(scan.page(&[completed(), timer()]), None);
        assert_eq!(
            scan.end(),
            Some(RunClose {
                status: WorkflowExecutionStatus::Completed,
                closing_task_started_event_id: None,
            })
        );
        assert_eq!(CloseScan::default().end(), None);
    }
}
