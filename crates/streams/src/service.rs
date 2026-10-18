//! The stream calls lang makes, served in process.

use crate::{
    DEFAULT_MAX_NOTIFIERS, Notifier, NotifierClient, NotifyingStore, OwnerClient, StreamError,
    StreamResult, StreamStore, Streams, StreamsOptions,
    proto::{
        AppendRequest, CloseRequest, DeleteOwnerRequest, DescribeStoreRequest,
        DescribeStoreResponse, FlushNotificationsRequest, FlushNotificationsResponse,
        FollowRangeRequest, FollowRequest, LatestRequest, PublishWorkflowTaskRequest,
        ReadRangeRequest, ReadRequest, ReleaseFloorRequest, RetireProducerRequest,
        RunEvictedRequest, RunEvictedResponse, ShutdownWorkerRequest, ShutdownWorkerResponse,
        StreamFailure, StreamStoreConfig, SubscribeRequest, UnsubscribeRequest,
        WaitForNotificationRequest, stream_store_config,
    },
    subscribe::Subscriptions,
    workflow::WorkflowOutput,
};
use prost::Message;
use std::{sync::Arc, time::Duration};
use temporalio_common::telemetry::metrics::TemporalMeter;

/// How long a store keeps records when its configuration doesn't say.
const DEFAULT_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// The most records one read answers with, as the stream service contract allows.
const MAX_READ_RECORDS: u32 = 1000;
/// The longest one read waits, as the stream service contract allows. A longer wait would hold a
/// store connection that other reads need.
const MAX_READ_WAIT: Duration = Duration::from_secs(60);

/// One process's stream store and the calls lang makes on it.
///
/// Lang builds one from a [StreamStoreConfig] and sends its stream calls to [StreamService::call]
/// as serialized `coresdk.streams` requests. Its Workers publish their Workflows' output through
/// the same calls.
pub struct StreamService {
    store: Arc<dyn StreamStore>,
    owner: Arc<dyn OwnerClient>,
    streams: Streams,
    workflows: WorkflowOutput,
    meter: TemporalMeter,
    subscriptions: Subscriptions,
}

impl std::fmt::Debug for StreamService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamService")
            .field("store", &self.store.name())
            .finish()
    }
}

impl StreamService {
    /// Connects to the store `config` names, asking `owner` about streams' owners. Refuses a
    /// config that turns notifications on, since there's no client to send them with.
    pub async fn connect(
        config: StreamStoreConfig,
        owner: Arc<dyn OwnerClient>,
    ) -> StreamResult<Self> {
        Self::connect_with_notifier(config, owner, None).await
    }

    /// Like [StreamService::connect], closing each stream's notifier through `notifier` after
    /// the store closes the stream, sending progress too when the config turns it on, attaching
    /// and detaching Workflow readers through it, and waiting on it for client readers.
    pub async fn connect_with_notifier(
        config: StreamStoreConfig,
        owner: Arc<dyn OwnerClient>,
        notifier: Option<Arc<dyn NotifierClient>>,
    ) -> StreamResult<Self> {
        let (store, retention): (Arc<dyn StreamStore>, Duration) = match config.store {
            Some(stream_store_config::Store::Memory(memory)) => {
                let store = match &memory.capabilities {
                    Some(mask) => crate::MemoryStore::with_capabilities(mask.into()),
                    None => crate::MemoryStore::new(),
                };
                (Arc::new(store), DEFAULT_RETENTION)
            }
            Some(stream_store_config::Store::Redis(redis)) => connect_redis(redis).await?,
            None => {
                return Err(StreamError::refused(
                    "the stream store config names no store",
                ));
            }
        };
        if config.notify_on_append && notifier.is_none() {
            return Err(StreamError::refused(
                "stream notifications need a client to send them with",
            ));
        }
        let store: Arc<dyn StreamStore> = match notifier.clone() {
            Some(client) => {
                let max_notifiers = match config.max_notifiers {
                    0 => DEFAULT_MAX_NOTIFIERS,
                    max => max as usize,
                };
                Arc::new(
                    NotifyingStore::new(store, Arc::new(Notifier::new(client, max_notifiers)))
                        .with_progress(config.notify_on_append),
                )
            }
            None => store,
        };
        let mut options = StreamsOptions::for_retention(retention);
        if let Some(grace) = config.owner_start_grace {
            options.owner_start_grace = Duration::try_from(grace).map_err(|_| {
                StreamError::refused("the stream store's owner start grace must not be negative")
            })?;
        }
        let service = Self::new(store, owner, options);
        Ok(match notifier {
            Some(client) => service.with_subscriptions(client),
            None => service,
        })
    }

    /// Serves calls on `store`.
    pub fn new(
        store: Arc<dyn StreamStore>,
        owner: Arc<dyn OwnerClient>,
        options: StreamsOptions,
    ) -> Self {
        let streams = Streams::new(store.clone(), owner.clone(), options);
        let meter = TemporalMeter::no_op();
        let workflows = WorkflowOutput::new(store.clone(), owner.clone(), meter.clone());
        Self {
            store,
            owner,
            streams,
            workflows,
            meter,
            subscriptions: Subscriptions::default(),
        }
    }

    /// Attaches and detaches Workflow readers, and waits for client readers, through `client`.
    pub fn with_subscriptions(mut self, client: Arc<dyn NotifierClient>) -> Self {
        self.subscriptions = Subscriptions::new(Some(client));
        self
    }

    /// Counts on `meter`, which is the no-op meter until lang gives one. Call it before any
    /// Workflow publishes.
    pub fn with_meter(mut self, meter: TemporalMeter) -> Self {
        self.workflows = WorkflowOutput::new(self.store.clone(), self.owner.clone(), meter.clone());
        self.meter = meter;
        self
    }

    /// The meter the service counts on.
    pub fn meter(&self) -> &TemporalMeter {
        &self.meter
    }

    /// The store the service serves.
    pub fn store(&self) -> Arc<dyn StreamStore> {
        self.store.clone()
    }

    /// Serves one call: `rpc` is a `StreamService` method name and `request` its serialized
    /// request. Answers with the serialized response, or the failure the call ended with.
    pub async fn call(&self, rpc: &str, request: &[u8]) -> Result<Vec<u8>, StreamFailure> {
        let response = match rpc {
            "Append" => self
                .streams
                .append(decode::<AppendRequest>(rpc, request)?)
                .await
                .map(|r| r.encode_to_vec()),
            "Read" => self
                .subscriptions
                .read(&self.streams, capped(decode::<ReadRequest>(rpc, request)?))
                .await
                .map(|r| r.encode_to_vec()),
            "Latest" => self
                .streams
                .latest(decode::<LatestRequest>(rpc, request)?)
                .await
                .map(|r| r.encode_to_vec()),
            "Close" => self
                .streams
                .close(decode::<CloseRequest>(rpc, request)?)
                .await
                .map(|r| r.encode_to_vec()),
            "DeleteOwner" => self
                .streams
                .delete_owner(decode::<DeleteOwnerRequest>(rpc, request)?)
                .await
                .map(|r| r.encode_to_vec()),
            "ReadRange" => self
                .streams
                .read_range(decode::<ReadRangeRequest>(rpc, request)?)
                .await
                .map(|r| r.encode_to_vec()),
            "Follow" => {
                let mut request = decode::<FollowRequest>(rpc, request)?;
                request.max_records = request.max_records.min(MAX_READ_RECORDS);
                request.wait = capped_wait(request.wait);
                crate::follow::follow(&self.streams, &self.subscriptions, request)
                    .await
                    .map(|r| r.encode_to_vec())
            }
            "FollowRange" => crate::follow::follow_range(
                &self.streams,
                decode::<FollowRangeRequest>(rpc, request)?,
            )
            .await
            .map(|r| r.encode_to_vec()),
            "ReleaseFloor" => self
                .streams
                .release_floor(decode::<ReleaseFloorRequest>(rpc, request)?)
                .await
                .map(|r| r.encode_to_vec()),
            "RetireProducer" => self
                .streams
                .retire_producer(decode::<RetireProducerRequest>(rpc, request)?)
                .await
                .map(|r| r.encode_to_vec()),
            "FlushNotifications" => {
                decode::<FlushNotificationsRequest>(rpc, request)?;
                // A close retries until the server takes it, so a stopping process waits only
                // so long for one against a server that's down.
                let _ = tokio::time::timeout(crate::FLUSH_LIMIT, self.store.flush_notifications())
                    .await;
                Ok(FlushNotificationsResponse {}.encode_to_vec())
            }
            "Subscribe" => self
                .subscriptions
                .subscribe(&self.streams, decode::<SubscribeRequest>(rpc, request)?)
                .await
                .map(|r| r.encode_to_vec()),
            "Unsubscribe" => self
                .subscriptions
                .unsubscribe(&self.streams, decode::<UnsubscribeRequest>(rpc, request)?)
                .await
                .map(|r| r.encode_to_vec()),
            "WaitForNotification" => self
                .subscriptions
                .wait(
                    &self.streams,
                    decode::<WaitForNotificationRequest>(rpc, request)?,
                )
                .await
                .map(|r| r.encode_to_vec()),
            "DescribeStore" => {
                decode::<DescribeStoreRequest>(rpc, request)?;
                Ok(DescribeStoreResponse {
                    name: self.store.name().to_string(),
                    capabilities: Some(self.store.capabilities().into()),
                }
                .encode_to_vec())
            }
            "PublishWorkflowTask" => self
                .workflows
                .publish(decode::<PublishWorkflowTaskRequest>(rpc, request)?)
                .await
                .map(|r| r.encode_to_vec()),
            "RunEvicted" => {
                let request = decode::<RunEvictedRequest>(rpc, request)?;
                self.run_evicted(
                    &request.worker_id,
                    &request.run_id,
                    request.workflow_execution_ending,
                )
                .await;
                Ok(RunEvictedResponse {}.encode_to_vec())
            }
            "ShutdownWorker" => {
                let request = decode::<ShutdownWorkerRequest>(rpc, request)?;
                self.shutdown_worker(&request.worker_id).await;
                Ok(ShutdownWorkerResponse {}.encode_to_vec())
            }
            other => Err(StreamError::unsupported(format!(
                "the stream service has no call {other:?}"
            ))),
        };
        response.map_err(StreamFailure::from)
    }

    /// Drops `worker_id`'s state for `run_id` and returns without a server call. With
    /// `workflow_execution_ending`, closes the run's chain in the background when the run's last
    /// live completion ended the chain and the run's History shows that completion closed the
    /// run. Lang waits for the call before it completes the eviction activation, so it never
    /// races the first publish of the run's next cache entry.
    pub async fn run_evicted(
        &self,
        worker_id: &str,
        run_id: &str,
        workflow_execution_ending: bool,
    ) {
        self.workflows
            .run_evicted(worker_id, run_id, workflow_execution_ending)
            .await;
    }

    /// Drops all of `worker_id`'s state, and waits for the store calls its runs left running,
    /// such as a chain close. Replay never sends a close again, so a Worker shutdown must wait
    /// for it. A Worker can shut down before it hands lang the eviction of a run that just ended,
    /// so the shutdown also closes the chain of each run whose last live completion ended it,
    /// with the same History check.
    pub async fn shutdown_worker(&self, worker_id: &str) {
        self.workflows.shutdown_worker(worker_id).await;
    }
}

/// `request` with its record count and wait cut to what the service allows.
fn capped(mut request: ReadRequest) -> ReadRequest {
    request.max_records = request.max_records.min(MAX_READ_RECORDS);
    request.wait = capped_wait(request.wait);
    request
}

fn capped_wait<D>(wait: Option<D>) -> Option<D>
where
    D: Copy + TryFrom<Duration>,
    Duration: TryFrom<D>,
{
    let too_long = wait
        .and_then(|wait| Duration::try_from(wait).ok())
        .is_some_and(|wait| wait > MAX_READ_WAIT);
    if too_long {
        MAX_READ_WAIT.try_into().ok()
    } else {
        wait
    }
}

fn decode<T: Message + Default>(rpc: &str, request: &[u8]) -> Result<T, StreamFailure> {
    T::decode(request).map_err(|error| {
        StreamError::invalid_argument(format!("the {rpc} request did not decode ({error})")).into()
    })
}

#[cfg(feature = "redis")]
async fn connect_redis(
    config: crate::proto::RedisStoreConfig,
) -> StreamResult<(Arc<dyn StreamStore>, Duration)> {
    let mut options = crate::RedisStoreOptions::new(String::new());
    options.urls = config.urls;
    options.cluster = config.cluster;
    if !config.key_prefix.is_empty() {
        options.key_prefix = config.key_prefix;
    }
    if let Some(retention) = config.retention {
        options.retention = duration(retention.seconds, retention.nanos, "retention")?;
    }
    if config.blocking_reads_per_node > 0 {
        options.blocking_reads_per_node = config.blocking_reads_per_node as usize;
    }
    if let Some(timeout) = config.response_timeout {
        options.response_timeout = duration(timeout.seconds, timeout.nanos, "response timeout")?;
    }
    if let Some(lease) = config.floor_lease {
        options.floor_lease = Some(duration(lease.seconds, lease.nanos, "floor lease")?);
    }
    if let Some(grace) = config.floor_release_grace {
        options.floor_release_grace = duration(grace.seconds, grace.nanos, "floor release grace")?;
    }
    options.plain = config.plain;
    let retention = options.retention;
    let store = crate::RedisStore::connect(options).await?;
    Ok((Arc::new(store), retention))
}

#[cfg(not(feature = "redis"))]
async fn connect_redis(
    _: crate::proto::RedisStoreConfig,
) -> StreamResult<(Arc<dyn StreamStore>, Duration)> {
    Err(StreamError::unsupported(
        "this build has no Redis store; build temporalio-streams with its redis feature",
    ))
}

#[cfg(feature = "redis")]
fn duration(seconds: i64, nanos: i32, name: &str) -> StreamResult<Duration> {
    match (u64::try_from(seconds), u32::try_from(nanos)) {
        (Ok(seconds), Ok(nanos)) if seconds > 0 || nanos > 0 => Ok(Duration::new(seconds, nanos)),
        _ => Err(StreamError::refused(format!(
            "the Redis store's {name} must be positive"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        OwnerDescription, OwnerError, OwnerId, OwnerStatus,
        proto::{
            AppendRecord, AppendResponse, LatestResponse, MemoryStoreConfig, NamedProducer,
            ReadResponse, StreamAddress, StreamFailureKind, StreamOwnerKind, StreamRecordKind,
            append_request, read_record,
        },
    };
    use temporalio_common::protos::temporal::api::common::v1::Payload;

    /// Every Workflow's latest run is `run-1`, the first of its chain, and still running.
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

    async fn service() -> StreamService {
        StreamService::connect(
            StreamStoreConfig {
                store: Some(stream_store_config::Store::Memory(
                    MemoryStoreConfig::default(),
                )),
                ..Default::default()
            },
            Arc::new(Running),
        )
        .await
        .unwrap()
    }

    fn address() -> StreamAddress {
        StreamAddress {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: "wf".to_string(),
            run_id: String::new(),
            topic: "out".to_string(),
            ..Default::default()
        }
    }

    fn append(sequence: i64, values: &[&str]) -> Vec<u8> {
        AppendRequest {
            stream: Some(address()),
            producer: Some(append_request::Producer::Named(NamedProducer {
                producer_id: "p".to_string(),
                attempt: 1,
                ..Default::default()
            })),
            sequence,
            digest: vec![sequence as u8; 32],
            records: values
                .iter()
                .map(|value| AppendRecord {
                    kind: StreamRecordKind::Data as i32,
                    body: Some(Payload {
                        data: value.as_bytes().to_vec(),
                        ..Default::default()
                    }),
                    content_hash: vec![value.as_bytes()[0]; 32],
                    ..Default::default()
                })
                .collect(),
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn an_append_reads_back_through_the_service() {
        let service = service().await;
        let appended = AppendResponse::decode(
            service
                .call("Append", &append(1, &["a", "b"]))
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let read = ReadResponse::decode(
            service
                .call(
                    "Read",
                    &ReadRequest {
                        stream: Some(address()),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                )
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let bodies: Vec<_> = read
            .records
            .iter()
            .map(|r| match &r.record {
                Some(read_record::Record::Stored(stored)) => stored.body.clone().unwrap().data,
                other => panic!("expected a stored record, got {other:?}"),
            })
            .collect();
        assert_eq!(bodies, [b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(read.cursor, appended.last_cursor);

        let latest = LatestResponse::decode(
            service
                .call(
                    "Latest",
                    &LatestRequest {
                        stream: Some(address()),
                    }
                    .encode_to_vec(),
                )
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        assert_eq!(latest.cursor, appended.last_cursor);
    }

    fn read(request: ReadRequest) -> Vec<u8> {
        ReadRequest {
            stream: Some(address()),
            ..request
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn a_read_answers_at_most_a_thousand_records() {
        let service = service().await;
        let values: Vec<String> = (0..1001).map(|i| format!("v{i}")).collect();
        let values: Vec<&str> = values.iter().map(String::as_str).collect();
        service.call("Append", &append(1, &values)).await.unwrap();
        let answer = service
            .call(
                "Read",
                &read(ReadRequest {
                    max_records: 1_000_000,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        let read = ReadResponse::decode(answer.as_slice()).unwrap();
        assert_eq!(read.records.len(), 1000);
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_waits_at_most_a_minute() {
        let service = service().await;
        let started = tokio::time::Instant::now();
        service
            .call(
                "Read",
                &read(ReadRequest {
                    after: "$end".to_string(),
                    wait: Some(Duration::from_secs(3600).try_into().unwrap()),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        assert!(
            started.elapsed() <= Duration::from_secs(61),
            "{:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn a_failure_crosses_with_its_kind() {
        let service = service().await;
        service
            .call("Append", &append(1, &["a", "b"]))
            .await
            .unwrap();
        let failure = service
            .call("Append", &append(2, &["c"]))
            .await
            .unwrap_err();
        assert_eq!(
            failure.kind(),
            StreamFailureKind::ProducerStale,
            "{failure:?}"
        );
    }

    #[tokio::test]
    async fn an_unknown_call_or_a_bad_request_is_refused() {
        let service = service().await;
        let unknown = service.call("Truncate", &[]).await.unwrap_err();
        assert_eq!(unknown.kind(), StreamFailureKind::Unsupported);
        assert!(unknown.message.contains("Truncate"));
        let garbled = service.call("Append", &[0xff, 0xff]).await.unwrap_err();
        assert_eq!(garbled.kind(), StreamFailureKind::InvalidArgument);
        assert!(garbled.message.contains("did not decode"));
    }

    #[tokio::test]
    async fn a_retired_producer_writes_its_batch_again() {
        let service = service().await;
        let first = AppendResponse::decode(
            service
                .call("Append", &append(1, &["a"]))
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let retire = crate::proto::RetireProducerRequest {
            stream: Some(address()),
            producer: Some(crate::proto::retire_producer_request::Producer::Named(
                NamedProducer {
                    producer_id: "p".to_string(),
                    attempt: 1,
                    ..Default::default()
                },
            )),
        };
        let answer = service
            .call("RetireProducer", &retire.encode_to_vec())
            .await
            .unwrap();
        assert!(answer.is_empty());
        let again = AppendResponse::decode(
            service
                .call("Append", &append(1, &["a"]))
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        assert_ne!(again.first_cursor, first.first_cursor);
    }

    #[tokio::test]
    async fn a_delete_is_checked_before_it_reaches_the_store() {
        let service = service().await;
        let other_owner = crate::proto::DeleteOwnerRequest {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Unspecified as i32,
            workflow_id: "wf".to_string(),
            ..Default::default()
        };
        let failure = service
            .call("DeleteOwner", &other_owner.encode_to_vec())
            .await
            .unwrap_err();
        assert_eq!(failure.kind(), StreamFailureKind::Unsupported);
        let no_id = crate::proto::DeleteOwnerRequest {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: String::new(),
            ..Default::default()
        };
        let failure = service
            .call("DeleteOwner", &no_id.encode_to_vec())
            .await
            .unwrap_err();
        assert_eq!(failure.kind(), StreamFailureKind::InvalidArgument);
    }

    #[tokio::test]
    async fn describe_answers_the_stores_capabilities_and_limits() {
        use crate::proto::StoreCapabilities;
        let describe = async |service: &StreamService| {
            let answer = service
                .call("DescribeStore", &DescribeStoreRequest {}.encode_to_vec())
                .await
                .unwrap();
            DescribeStoreResponse::decode(answer.as_slice()).unwrap()
        };
        let full = describe(&service().await).await;
        assert_eq!(full.name, "memory");
        let capabilities = full.capabilities.unwrap();
        assert!(capabilities.producer_state && capabilities.kept_last);
        assert!(capabilities.dense_positions && capabilities.holds);
        assert!(
            !capabilities.change_feed,
            "a store with no notification log has no change feed"
        );
        assert_eq!(capabilities.max_record_bytes, 1536 * 1024);
        assert_eq!(capabilities.max_batch_bytes, 4 << 20);
        let masked = StreamService::connect(
            StreamStoreConfig {
                store: Some(stream_store_config::Store::Memory(MemoryStoreConfig {
                    capabilities: Some(StoreCapabilities {
                        long_poll: true,
                        max_record_bytes: 1024,
                        ..Default::default()
                    }),
                })),
                ..Default::default()
            },
            Arc::new(Running),
        )
        .await
        .unwrap();
        let capabilities = describe(&masked).await.capabilities.unwrap();
        assert!(!capabilities.producer_state && !capabilities.kept_last);
        assert!(capabilities.long_poll);
        assert_eq!(capabilities.max_record_bytes, 1024);
        assert_eq!(capabilities.max_batch_bytes, 4 << 20);
    }

    #[tokio::test]
    async fn a_config_that_names_no_store_is_refused() {
        let error = StreamService::connect(StreamStoreConfig::default(), Arc::new(Running))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Refused);
    }

    #[tokio::test]
    async fn a_negative_owner_start_grace_is_refused() {
        let mut config = StreamStoreConfig {
            store: Some(stream_store_config::Store::Memory(
                MemoryStoreConfig::default(),
            )),
            owner_start_grace: Some(Default::default()),
            ..Default::default()
        };
        config.owner_start_grace.as_mut().unwrap().seconds = -1;
        let error = StreamService::connect(config, Arc::new(Running))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Refused);
        assert!(error.message.contains("owner start grace"), "{error}");
    }

    #[cfg(not(feature = "redis"))]
    #[tokio::test]
    async fn redis_needs_the_redis_feature() {
        let error = StreamService::connect(
            StreamStoreConfig {
                store: Some(stream_store_config::Store::Redis(Default::default())),
                ..Default::default()
            },
            Arc::new(Running),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Unsupported);
    }

    /// Records the notifications it was sent.
    #[derive(Default)]
    struct Recorder(std::sync::Mutex<Vec<crate::StreamNotification>>);

    #[async_trait::async_trait]
    impl NotifierClient for Recorder {
        async fn notify(
            &self,
            notification: crate::StreamNotification,
        ) -> Result<(), crate::NotifyError> {
            self.0.lock().unwrap().push(notification);
            Ok(())
        }
    }

    async fn service_notifying(notify_on_append: bool, recorder: Arc<Recorder>) -> StreamService {
        StreamService::connect_with_notifier(
            StreamStoreConfig {
                store: Some(stream_store_config::Store::Memory(
                    MemoryStoreConfig::default(),
                )),
                notify_on_append,
                ..Default::default()
            },
            Arc::new(Running),
            Some(recorder),
        )
        .await
        .unwrap()
    }

    async fn flush(service: &StreamService) {
        let response = service
            .call(
                "FlushNotifications",
                &FlushNotificationsRequest {}.encode_to_vec(),
            )
            .await
            .unwrap();
        FlushNotificationsResponse::decode(response.as_slice()).unwrap();
    }

    #[tokio::test]
    async fn an_append_notifies_its_streams_notifier_once_notifications_are_on() {
        let recorder = Arc::new(Recorder::default());
        let service = service_notifying(true, recorder.clone()).await;
        let appended = AppendResponse::decode(
            service
                .call("Append", &append(1, &["a", "b"]))
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        flush(&service).await;
        let sent = recorder.0.lock().unwrap().clone();
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0].chain.first_run_id, "run-1",
            "the chain the append wrote to"
        );
        assert_eq!(sent[0].topic, "out");
        assert_eq!(sent[0].position, appended.last_cursor);
        assert_eq!(sent[0].counter, 2);
    }

    #[tokio::test]
    async fn every_append_notifies_with_no_reader_waiting() {
        let recorder = Arc::new(Recorder::default());
        let service = service_notifying(true, recorder.clone()).await;
        service.call("Append", &append(1, &["a"])).await.unwrap();
        service.call("Append", &append(2, &["b"])).await.unwrap();
        flush(&service).await;
        let counters: Vec<i64> = recorder
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|n| n.counter)
            .collect();
        assert_eq!(counters.last(), Some(&2), "{counters:?}");
    }

    #[tokio::test]
    async fn notifications_are_off_by_default() {
        let recorder = Arc::new(Recorder::default());
        let service = service_notifying(false, recorder.clone()).await;
        service.call("Append", &append(1, &["a"])).await.unwrap();
        flush(&service).await;
        assert!(recorder.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn notifications_without_a_client_are_refused() {
        let error = StreamService::connect(
            StreamStoreConfig {
                store: Some(stream_store_config::Store::Memory(
                    MemoryStoreConfig::default(),
                )),
                notify_on_append: true,
                ..Default::default()
            },
            Arc::new(Running),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Refused);
    }

    #[tokio::test]
    async fn a_close_reaches_the_notifier_with_notifications_off() {
        let recorder = Arc::new(Recorder::default());
        let service = service_notifying(false, recorder.clone()).await;
        service.call("Append", &append(1, &["a"])).await.unwrap();
        let result = Payload {
            data: b"summary".to_vec(),
            ..Default::default()
        };
        service
            .call(
                "Close",
                &CloseRequest {
                    stream: Some(address()),
                    result: Some(result.clone()),
                    failure: None,
                    closed_by: Default::default(),
                }
                .encode_to_vec(),
            )
            .await
            .unwrap();
        flush(&service).await;
        let sent = recorder.0.lock().unwrap().clone();
        assert_eq!(sent.len(), 1, "no progress, only the close");
        assert_eq!(sent[0].close_result(), Some(result));
    }

    /// Answers every notification like a server without the stream notifier.
    #[derive(Default)]
    struct Stock(std::sync::atomic::AtomicUsize);

    #[async_trait::async_trait]
    impl NotifierClient for Stock {
        async fn notify(&self, _: crate::StreamNotification) -> Result<(), crate::NotifyError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(crate::NotifyError::Unsupported("unimplemented".to_string()))
        }
    }

    #[tokio::test]
    async fn a_server_without_the_notifier_costs_one_call() {
        let stock = Arc::new(Stock::default());
        let service = StreamService::connect_with_notifier(
            StreamStoreConfig {
                store: Some(stream_store_config::Store::Memory(
                    MemoryStoreConfig::default(),
                )),
                notify_on_append: true,
                ..Default::default()
            },
            Arc::new(Running),
            Some(stock.clone()),
        )
        .await
        .unwrap();
        service.call("Append", &append(1, &["a"])).await.unwrap();
        flush(&service).await;
        service.call("Append", &append(2, &["b"])).await.unwrap();
        service
            .call(
                "Close",
                &CloseRequest {
                    stream: Some(address()),
                    result: Some(Payload::default()),
                    failure: None,
                    closed_by: Default::default(),
                }
                .encode_to_vec(),
            )
            .await
            .unwrap();
        flush(&service).await;
        assert_eq!(stock.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// The Workflow's chain is `run-1`. It runs until `ended` is set, then it's completed.
    #[derive(Default)]
    struct Ends(std::sync::atomic::AtomicBool);

    #[async_trait::async_trait]
    impl OwnerClient for Ends {
        async fn describe(
            &self,
            _: &str,
            _: OwnerId<'_>,
            _: &str,
        ) -> Result<OwnerDescription, OwnerError> {
            let ended = self.0.load(std::sync::atomic::Ordering::SeqCst);
            Ok(OwnerDescription {
                run_id: "run-1".to_string(),
                first_run_id: "run-1".to_string(),
                status: if ended {
                    OwnerStatus::Ended
                } else {
                    OwnerStatus::Running
                },
            })
        }
    }

    #[tokio::test]
    async fn a_readers_owner_check_right_after_the_final_task_sends_no_notifier_close() {
        let recorder = Arc::new(Recorder::default());
        let owner = Arc::new(Ends::default());
        let store = Arc::new(NotifyingStore::new(
            Arc::new(crate::MemoryStore::new()),
            Arc::new(Notifier::new(recorder.clone(), 10)),
        ));
        let service = StreamService::new(
            store,
            owner.clone(),
            StreamsOptions {
                owner_recheck: Duration::ZERO,
                owner_check_min: Duration::from_millis(1),
                owner_check_max: Duration::from_millis(1),
                ..Default::default()
            },
        );
        let appended = AppendResponse::decode(
            service
                .call("Append", &append(1, &["a"]))
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        flush(&service).await;
        // The owner completed, and no close reached the topic.
        owner.0.store(true, std::sync::atomic::Ordering::SeqCst);
        let read = ReadResponse::decode(
            service
                .call(
                    "Read",
                    &ReadRequest {
                        stream: Some(address()),
                        after: appended.last_cursor,
                        wait: Some(Duration::from_millis(200).try_into().unwrap()),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                )
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        assert!(read.done, "the reader saw the owner end");
        let refused = service
            .call("Append", &append(2, &["b"]))
            .await
            .unwrap_err();
        assert_eq!(refused.kind, StreamFailureKind::Closed as i32);
        flush(&service).await;
        assert!(
            recorder
                .0
                .lock()
                .unwrap()
                .iter()
                .all(|n| n.close_result().is_none()),
            "an empty close would beat the outcome the notifier's owner check finds"
        );
    }

    #[tokio::test]
    async fn a_replay_read_and_a_release_go_through_the_one_call_entry() {
        use crate::proto::{ReadRangeRequest, ReleaseFloorRequest, ReleaseFloorResponse};
        let service = service().await;
        service
            .call("Append", &append(1, &["a", "b"]))
            .await
            .unwrap();
        let request = ReadRequest {
            stream: Some(address()),
            floor_holder: "reader-run/1".to_string(),
            ..Default::default()
        };
        let page = ReadResponse::decode(
            service
                .call("Read", &request.encode_to_vec())
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let replayed = ReadResponse::decode(
            service
                .call(
                    "ReadRange",
                    &ReadRangeRequest {
                        stream: Some(address()),
                        chain_first_run_id: page.chain_first_run_id.clone(),
                        after: page.start_cursor.clone(),
                        to: page.cursor.clone(),
                        stored_records: 2,
                        state: Vec::new(),
                        records_digest: page.records_digest.clone(),
                        floor_holder: "reader-run/1".to_string(),
                        start_from_end: false,
                    }
                    .encode_to_vec(),
                )
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        assert_eq!(replayed.records, page.records);
        let released = service
            .call(
                "ReleaseFloor",
                &ReleaseFloorRequest {
                    stream: Some(address()),
                    chain_first_run_id: page.chain_first_run_id,
                    floor_holder: "reader-run/1".to_string(),
                }
                .encode_to_vec(),
            )
            .await
            .unwrap();
        ReleaseFloorResponse::decode(released.as_slice()).unwrap();
    }

    #[tokio::test]
    async fn a_read_after_the_close_ends_with_the_outcome_the_notifier_got() {
        let recorder = Arc::new(Recorder::default());
        let service = service_notifying(true, recorder.clone()).await;
        let appended = AppendResponse::decode(
            service
                .call("Append", &append(1, &["a"]))
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let result = Payload {
            data: b"summary".to_vec(),
            ..Default::default()
        };
        service
            .call(
                "Close",
                &CloseRequest {
                    stream: Some(address()),
                    result: Some(result.clone()),
                    failure: None,
                    closed_by: "run-1".to_string(),
                }
                .encode_to_vec(),
            )
            .await
            .unwrap();
        let read = ReadResponse::decode(
            service
                .call(
                    "Read",
                    &ReadRequest {
                        stream: Some(address()),
                        after: appended.last_cursor,
                        ..Default::default()
                    }
                    .encode_to_vec(),
                )
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        assert!(read.done && read.records.is_empty());
        assert_eq!(read.close_result, Some(result.clone()));
        assert_eq!(read.closed_by, "run-1");
        let sent = recorder.0.lock().unwrap().clone();
        assert_eq!(sent.last().unwrap().close_result(), Some(result));
        let refused = service
            .call("Append", &append(2, &["b"]))
            .await
            .unwrap_err();
        assert_eq!(refused.kind, StreamFailureKind::Closed as i32);
        assert_eq!(refused.closed_by, "run-1");
    }

    #[tokio::test]
    async fn a_close_with_a_result_closes_the_store_and_then_the_notifier() {
        let recorder = Arc::new(Recorder::default());
        let service = service_notifying(true, recorder.clone()).await;
        service
            .call("Append", &append(1, &["a", "b"]))
            .await
            .unwrap();
        flush(&service).await;
        let result = Payload {
            data: b"summary".to_vec(),
            ..Default::default()
        };
        service
            .call(
                "Close",
                &CloseRequest {
                    stream: Some(address()),
                    result: Some(result.clone()),
                    failure: None,
                    closed_by: Default::default(),
                }
                .encode_to_vec(),
            )
            .await
            .unwrap();
        let sent = recorder.0.lock().unwrap().clone();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[1].close_result(), Some(result));
        assert_eq!(sent[1].counter, 3, "above the newest record's counter");
        let refused = service
            .call("Append", &append(3, &["c"]))
            .await
            .unwrap_err();
        assert_eq!(refused.kind, StreamFailureKind::Closed as i32);
    }

    #[tokio::test]
    async fn a_close_without_a_result_ends_the_stream_on_the_server_with_an_empty_result() {
        let recorder = Arc::new(Recorder::default());
        let service = service_notifying(true, recorder.clone()).await;
        service
            .call(
                "Close",
                &CloseRequest {
                    stream: Some(address()),
                    result: None,
                    failure: None,
                    closed_by: Default::default(),
                }
                .encode_to_vec(),
            )
            .await
            .unwrap();
        flush(&service).await;
        let sent = recorder.0.lock().unwrap().clone();
        assert_eq!(sent.len(), 1);
        // A caller with a typed result decodes it as None, which an empty payload can't give.
        assert_eq!(sent[0].close_result(), Some(crate::empty_close_result()));
        assert_eq!(
            sent[0].close_result().unwrap().metadata["encoding"],
            b"binary/null"
        );
    }

    /// A store whose notifications never go out, as when the server is down and a close retries.
    struct NeverFlushes(crate::MemoryStore);

    #[async_trait::async_trait]
    impl StreamStore for NeverFlushes {
        fn name(&self) -> &str {
            self.0.name()
        }
        async fn append(
            &self,
            r: crate::proto::StoreAppendRequest,
        ) -> StreamResult<crate::proto::StoreAppendResponse> {
            self.0.append(r).await
        }
        async fn read(
            &self,
            r: crate::proto::StoreReadRequest,
        ) -> StreamResult<crate::proto::StoreReadResponse> {
            self.0.read(r).await
        }
        async fn latest(
            &self,
            r: crate::proto::StoreLatestRequest,
        ) -> StreamResult<crate::proto::StoreLatestResponse> {
            self.0.latest(r).await
        }
        async fn record_at(
            &self,
            c: &crate::proto::ChainId,
            t: &str,
            p: &str,
        ) -> StreamResult<Option<Vec<u8>>> {
            self.0.record_at(c, t, p).await
        }
        async fn close_chain(&self, c: &crate::proto::ChainId) -> StreamResult<()> {
            self.0.close_chain(c).await
        }
        async fn close_topic(&self, c: &crate::proto::ChainId, t: &str) -> StreamResult<()> {
            self.0.close_topic(c, t).await
        }
        async fn retire_producer(
            &self,
            c: &crate::proto::ChainId,
            t: &str,
            p: &str,
            a: i64,
        ) -> StreamResult<()> {
            self.0.retire_producer(c, t, p, a).await
        }
        async fn written_topics(&self, c: &crate::proto::ChainId) -> StreamResult<Vec<String>> {
            self.0.written_topics(c).await
        }
        async fn delete_owner(
            &self,
            r: DeleteOwnerRequest,
        ) -> StreamResult<crate::proto::DeleteOwnerResponse> {
            self.0.delete_owner(r).await
        }
        async fn flush_notifications(&self) {
            std::future::pending::<()>().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_flush_waits_only_so_long_for_notifications_that_never_go_out() {
        let service = StreamService::new(
            Arc::new(NeverFlushes(crate::MemoryStore::new())),
            Arc::new(Running),
            StreamsOptions::default(),
        );
        let started = tokio::time::Instant::now();
        flush(&service).await;
        assert_eq!(
            started.elapsed(),
            crate::FLUSH_LIMIT,
            "a stopping process mustn't hang on a close retrying against a server that's down"
        );
    }
}
