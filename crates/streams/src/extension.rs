//! The Worker extension that lang's bridge hands to Core and calls by method name.

use crate::{
    StreamError, StreamResult,
    metrics::Metrics,
    proto::{
        AppendRecordsRequest, BufferWorkflowCallsRequest, BufferWorkflowCallsResponse,
        CloseStreamRequest, CloseWriterRequest, CloseWriterResponse, DescribeStreamRequest,
        DrainClosesRequest, DrainClosesResponse, FlushWriterRequest, FlushWriterResponse,
        OpenWriterRequest, OpenWriterResponse, PublishRequest, PublishResponse, ReadRecordsRequest,
        RedisStreamConfig,
    },
    redis::{RedisStore, RedisStoreOptions},
    service::StreamService,
    workflow::{FlushOptions, WorkflowHooks, WorkflowRuns},
    writer::Writers,
};
use prost::Message;
use std::{sync::Arc, time::Duration};
use temporalio_common::telemetry::metrics::TemporalMeter;
use temporalio_sdk_core::{ExtensionError, WorkerExtension, WorkflowTaskInterceptor};
use tokio::sync::OnceCell;

/// The name of the extension, for logs and for a bridge that selects an extension by name.
pub const EXTENSION_NAME: &str = "temporalio-streams-redis";

/// What the extension needs besides the Store options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionOptions {
    /// How to reach Redis.
    pub store: RedisStoreOptions,
    /// When above zero, each Workflow flush waits until this many replicas have it.
    pub durable_writes: u32,
    /// How long that wait may take before the flush fails.
    pub durable_timeout: Duration,
}

impl ExtensionOptions {
    /// The options from the plugin's config message.
    pub fn from_config(config: RedisStreamConfig) -> StreamResult<Self> {
        let Some(seed) = config.urls.first() else {
            return Err(StreamError::invalid("The Redis Stream plugin needs a URL"));
        };
        let mut store = RedisStoreOptions::new(seed.clone());
        store.urls = config.urls;
        store.cluster = config.cluster;
        if !config.key_prefix.is_empty() {
            store.key_prefix = config.key_prefix;
        }
        if let Some(retention) = duration(config.retention) {
            store.retention = retention;
        }
        if let Some(safety_ttl) = duration(config.safety_ttl) {
            store.safety_ttl = safety_ttl;
        }
        if config.blocking_reads_per_node > 0 {
            store.blocking_reads_per_node = config.blocking_reads_per_node as usize;
        }
        if let Some(timeout) = duration(config.response_timeout) {
            store.response_timeout = timeout;
        }
        store.durable_connection = config.durable_writes > 0;
        Ok(Self {
            store,
            durable_writes: config.durable_writes.max(0) as u32,
            durable_timeout: duration(config.durable_timeout).unwrap_or(Duration::from_secs(1)),
        })
    }
}

fn duration(value: Option<prost_types::Duration>) -> Option<Duration> {
    let value = value?;
    let duration = Duration::new(value.seconds.max(0) as u64, value.nanos.max(0) as u32);
    (!duration.is_zero()).then_some(duration)
}

/// What the extension and its Workflow Task interceptor share.
pub(crate) struct Shared {
    options: ExtensionOptions,
    metrics: Metrics,
    service: OnceCell<Arc<StreamService>>,
    pub(crate) runs: WorkflowRuns,
    writers: Writers,
}

impl Shared {
    /// The service, which connects to Redis on its first use. A failed connect is tried again
    /// on the next call.
    pub(crate) async fn service(&self) -> StreamResult<&Arc<StreamService>> {
        self.service
            .get_or_try_init(|| async {
                let store = RedisStore::connect(self.options.store.clone()).await?;
                Ok(Arc::new(StreamService {
                    store: Arc::new(store),
                    metrics: self.metrics.clone(),
                }))
            })
            .await
    }

    pub(crate) fn flush_options(&self) -> FlushOptions {
        FlushOptions {
            durable_writes: self.options.durable_writes,
            durable_timeout: self.options.durable_timeout,
        }
    }
}

/// The Stream runtime on Redis, as a Worker extension.
///
/// It connects on its first call that needs Redis, so a bridge can build it with no runtime,
/// and a Workflow that never publishes never needs Redis. One extension serves every Worker and
/// client of a plugin.
pub struct RedisStreamExtension {
    shared: Arc<Shared>,
    hooks: Arc<WorkflowHooks>,
}

impl std::fmt::Debug for RedisStreamExtension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisStreamExtension")
            .field("options", &self.shared.options)
            .finish_non_exhaustive()
    }
}

impl RedisStreamExtension {
    /// Builds the extension. It counts on `meter`, lang's runtime meter, when given.
    pub fn new(options: ExtensionOptions, meter: Option<TemporalMeter>) -> Arc<Self> {
        let meter = meter.unwrap_or_else(TemporalMeter::no_op);
        let shared = Arc::new(Shared {
            options,
            metrics: Metrics::new(&meter),
            service: OnceCell::new(),
            runs: WorkflowRuns::default(),
            writers: Writers::default(),
        });
        let hooks = Arc::new(WorkflowHooks {
            shared: shared.clone(),
        });
        Arc::new(Self { shared, hooks })
    }

    /// Builds the extension from an encoded [RedisStreamConfig].
    pub fn from_config_bytes(
        config: &[u8],
        meter: Option<TemporalMeter>,
    ) -> StreamResult<Arc<Self>> {
        let config = RedisStreamConfig::decode(config)
            .map_err(|error| StreamError::invalid(format!("The config is bad: {error}")))?;
        Ok(Self::new(ExtensionOptions::from_config(config)?, meter))
    }

    /// The options the extension runs with.
    pub fn options(&self) -> &ExtensionOptions {
        &self.shared.options
    }

    /// Runs one synchronous method on encoded input. It never waits on I/O.
    pub fn dispatch(&self, method: &str, input: &[u8]) -> Result<Vec<u8>, ExtensionError> {
        let output = match method {
            "BufferWorkflowCalls" => {
                self.shared
                    .runs
                    .buffer(decode::<BufferWorkflowCallsRequest>(input)?)?;
                BufferWorkflowCallsResponse {}.encode_to_vec()
            }
            "Publish" => {
                let request = decode::<PublishRequest>(input)?;
                self.shared.writers.publish(
                    request.writer_id,
                    request.records,
                    request.force_flush,
                )?;
                PublishResponse {}.encode_to_vec()
            }
            _ => return Err(ExtensionError::unknown_method(method)),
        };
        Ok(output)
    }

    /// Runs one asynchronous method on encoded input.
    pub async fn dispatch_async(
        &self,
        method: &str,
        input: &[u8],
    ) -> Result<Vec<u8>, ExtensionError> {
        if method == "DrainCloses" {
            let request = decode::<DrainClosesRequest>(input)?;
            let timeout = duration(request.timeout).unwrap_or(Duration::from_secs(10));
            let pending = self.shared.runs.drain(timeout).await;
            return Ok(DrainClosesResponse {
                pending: pending as i32,
            }
            .encode_to_vec());
        }
        let service = self.shared.service().await?;
        let output = match method {
            "CreateStream" => service.create_stream(decode(input)?).await?.encode_to_vec(),
            "AppendRecords" => service
                .append_records(decode::<AppendRecordsRequest>(input)?)
                .await?
                .encode_to_vec(),
            "CloseStream" => service
                .close_stream(decode::<CloseStreamRequest>(input)?)
                .await?
                .encode_to_vec(),
            "ReadRecords" => service
                .read_records(decode::<ReadRecordsRequest>(input)?)
                .await?
                .encode_to_vec(),
            "DescribeStream" => service
                .describe_stream(decode::<DescribeStreamRequest>(input)?)
                .await?
                .encode_to_vec(),
            "OpenWriter" => {
                let request = decode::<OpenWriterRequest>(input)?;
                let writer_id = self.shared.writers.open(service, request).await?;
                OpenWriterResponse { writer_id }.encode_to_vec()
            }
            "FlushWriter" => {
                let request = decode::<FlushWriterRequest>(input)?;
                let last_offset = self.shared.writers.flush(request.writer_id).await?;
                FlushWriterResponse { last_offset }.encode_to_vec()
            }
            "CloseWriter" => {
                let request = decode::<CloseWriterRequest>(input)?;
                let last_offset = self.shared.writers.close(request.writer_id).await?;
                CloseWriterResponse { last_offset }.encode_to_vec()
            }
            _ => return Err(ExtensionError::unknown_method(method)),
        };
        Ok(output)
    }
}

pub(crate) fn decode<M: Message + Default>(input: &[u8]) -> StreamResult<M> {
    M::decode(input).map_err(|error| StreamError::invalid(format!("The request is bad: {error}")))
}

#[async_trait::async_trait]
impl WorkerExtension for RedisStreamExtension {
    fn name(&self) -> &str {
        EXTENSION_NAME
    }

    fn workflow_task_interceptor(&self) -> Option<Arc<dyn WorkflowTaskInterceptor>> {
        Some(self.hooks.clone())
    }

    fn call(&self, method: &str, input: &[u8]) -> Result<Vec<u8>, ExtensionError> {
        self.dispatch(method, input)
    }

    async fn call_async(&self, method: String, input: Vec<u8>) -> Result<Vec<u8>, ExtensionError> {
        self.dispatch_async(&method, &input).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_config_fills_the_defaults() {
        let options = ExtensionOptions::from_config(RedisStreamConfig {
            urls: vec!["redis://x:1".to_string()],
            durable_writes: 2,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(options.store.key_prefix, "temporal-streams");
        assert_eq!(options.store.retention, Duration::from_secs(7 * 86_400));
        assert_eq!(options.store.safety_ttl, Duration::from_secs(90 * 86_400));
        assert_eq!(options.store.blocking_reads_per_node, 256);
        assert_eq!(options.durable_writes, 2);
        assert!(options.store.durable_connection);
        assert_eq!(options.durable_timeout, Duration::from_secs(1));
        assert!(ExtensionOptions::from_config(RedisStreamConfig::default()).is_err());
    }
}
