//! The stream calls lang makes, served in process.

use crate::{
    OwnerClient, StreamError, StreamResult, StreamStore, Streams, StreamsOptions,
    proto::{
        AppendRequest, CloseRequest, DeleteOwnerRequest, DescribeStoreRequest,
        DescribeStoreResponse, LatestRequest, ReadRequest, RetireProducerRequest, StreamFailure,
        StreamStoreConfig, stream_store_config,
    },
};
use prost::Message;
use std::{sync::Arc, time::Duration};

/// How long a store keeps records when its configuration doesn't say.
const DEFAULT_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// The most records one read answers with, as the stream service contract allows.
const MAX_READ_RECORDS: u32 = 1000;
/// The longest one read waits, as the stream service contract allows. A longer wait would hold a
/// store connection that other reads need.
const MAX_READ_WAIT: Duration = Duration::from_secs(60);

/// One process's stream store and the calls lang makes on it.
///
/// Lang builds one from a [StreamStoreConfig], hands [StreamService::store] to its Workers, and
/// sends its stream calls to [StreamService::call] as serialized `coresdk.streams` requests.
pub struct StreamService {
    store: Arc<dyn StreamStore>,
    streams: Streams,
}

impl std::fmt::Debug for StreamService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamService")
            .field("store", &self.store.name())
            .finish()
    }
}

impl StreamService {
    /// Connects to the store `config` names, asking `owner` about streams' owners.
    pub async fn connect(
        config: StreamStoreConfig,
        owner: Arc<dyn OwnerClient>,
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
        let mut options = StreamsOptions::for_retention(retention);
        if let Some(grace) = config.owner_start_grace {
            options.owner_start_grace = Duration::try_from(grace).map_err(|_| {
                StreamError::refused("the stream store's owner start grace must not be negative")
            })?;
        }
        Ok(Self::new(store, owner, options))
    }

    /// Serves calls on `store`.
    pub fn new(
        store: Arc<dyn StreamStore>,
        owner: Arc<dyn OwnerClient>,
        options: StreamsOptions,
    ) -> Self {
        let streams = Streams::new(store.clone(), owner, options);
        Self { store, streams }
    }

    /// The store, for the Workers that append Workflow output to it.
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
                .streams
                .read(capped(decode::<ReadRequest>(rpc, request)?))
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
            "RetireProducer" => self
                .streams
                .retire_producer(decode::<RetireProducerRequest>(rpc, request)?)
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
            other => Err(StreamError::unsupported(format!(
                "the stream service has no call {other:?}"
            ))),
        };
        response.map_err(StreamFailure::from)
    }
}

/// `request` with its record count and wait cut to what Core allows.
fn capped(mut request: ReadRequest) -> ReadRequest {
    request.max_records = request.max_records.min(MAX_READ_RECORDS);
    let too_long = request
        .wait
        .and_then(|wait| Duration::try_from(wait).ok())
        .is_some_and(|wait| wait > MAX_READ_WAIT);
    if too_long {
        request.wait = MAX_READ_WAIT.try_into().ok();
    }
    request
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
        OwnerDescription, OwnerError,
        proto::{
            AppendRecord, AppendResponse, LatestResponse, MemoryStoreConfig, NamedProducer,
            ReadResponse, StreamAddress, StreamFailureKind, StreamOwnerKind, StreamRecordKind,
            append_request, read_record,
        },
    };
    use temporalio_common::protos::temporal::api::{
        common::v1::Payload, enums::v1::WorkflowExecutionStatus,
    };

    /// Every Workflow's latest run is `run-1`, the first of its chain, and still running.
    struct Running;

    #[async_trait::async_trait]
    impl OwnerClient for Running {
        async fn describe(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<OwnerDescription, OwnerError> {
            Ok(OwnerDescription {
                run_id: "run-1".to_string(),
                first_run_id: "run-1".to_string(),
                status: WorkflowExecutionStatus::Running,
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
        }
    }

    fn append(sequence: i64, values: &[&str]) -> Vec<u8> {
        AppendRequest {
            stream: Some(address()),
            producer: Some(append_request::Producer::Named(NamedProducer {
                producer_id: "p".to_string(),
                attempt: 1,
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
        assert!(capabilities.dense_positions && !capabilities.holds);
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
}
