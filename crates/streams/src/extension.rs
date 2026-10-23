//! The Stream runtime as a plain object, which lang's bridge builds per client and calls by
//! method name. Core has no hook for it.

use crate::{
    Reason, StreamError, StreamResult,
    memory::MemoryStore,
    metrics::Metrics,
    proto::{
        AppendRecordsRequest, CloseStreamRequest, CloseWriterRequest, CloseWriterResponse,
        CloseWritersRequest, CloseWritersResponse, DescribeStreamRequest, OpenWriterRequest,
        OpenWriterResponse, ReadRecordsRequest, RedisStreamConfig, WriteRecordsRequest,
        WriteRecordsResponse,
    },
    redis::{RedisStore, RedisStoreOptions},
    service::StreamService,
    store::Store,
    writer::Writers,
};
use prost::Message;
use std::{sync::Arc, time::Duration};
use temporalio_common::telemetry::metrics::TemporalMeter;
use tokio::sync::OnceCell;

/// The name of the Redis runtime, for logs and for a bridge that selects a runtime by name.
pub const EXTENSION_NAME: &str = "temporalio-streams-redis";

/// What the extension needs besides the Store options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionOptions {
    /// How to reach Redis.
    pub store: RedisStoreOptions,
    /// The backend name that lang registered the extension under, for the refs it builds.
    pub backend: String,
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
        Ok(Self {
            store,
            backend: config.backend,
        })
    }
}

fn duration(value: Option<prost_types::Duration>) -> Option<Duration> {
    let value = value?;
    let duration = Duration::new(value.seconds.max(0) as u64, value.nanos.max(0) as u32);
    (!duration.is_zero()).then_some(duration)
}

/// What the extension's calls share.
pub(crate) struct Shared {
    name: &'static str,
    options: ExtensionOptions,
    metrics: Metrics,
    service: OnceCell<Arc<StreamService>>,
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
                    backend: self.options.backend.clone(),
                }))
            })
            .await
    }
}

/// The Stream runtime on Redis or, for tests, on the memory Store.
///
/// On Redis it connects on its first call that needs Redis, so a bridge can build it with no
/// runtime, and a Workflow that never publishes never needs Redis. A bridge builds one for each
/// client of a plugin, and that client's Workers share it.
pub struct StreamExtension {
    shared: Arc<Shared>,
}

/// The Stream runtime on Redis. [StreamExtension::new] builds it.
pub type RedisStreamExtension = StreamExtension;

impl std::fmt::Debug for StreamExtension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamExtension")
            .field("name", &self.shared.name)
            .field("options", &self.shared.options)
            .finish_non_exhaustive()
    }
}

impl StreamExtension {
    fn build(
        name: &'static str,
        options: ExtensionOptions,
        meter: Option<TemporalMeter>,
        store: Option<Arc<dyn Store>>,
    ) -> Arc<Self> {
        let meter = meter.unwrap_or_else(TemporalMeter::no_op);
        let metrics = Metrics::new(&meter);
        let service = store.map(|store| {
            Arc::new(StreamService {
                store,
                metrics: metrics.clone(),
                backend: options.backend.clone(),
            })
        });
        let shared = Arc::new(Shared {
            name,
            options,
            metrics,
            service: OnceCell::new_with(service),
            writers: Writers::default(),
        });
        Arc::new(Self { shared })
    }

    /// Builds the extension on Redis. It counts on `meter`, lang's runtime meter, when given.
    pub fn new(options: ExtensionOptions, meter: Option<TemporalMeter>) -> Arc<Self> {
        Self::build(EXTENSION_NAME, options, meter, None)
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

    /// The options the runtime runs with.
    pub fn options(&self) -> &ExtensionOptions {
        &self.shared.options
    }

    /// A name that identifies the runtime in logs.
    pub fn name(&self) -> &str {
        self.shared.name
    }

    /// Runs one method on encoded input and answers the encoded output. The bridge raises an
    /// error with [StreamError::code] and [StreamError::details].
    pub async fn call(&self, method: &str, input: &[u8]) -> StreamResult<Vec<u8>> {
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
            "WriteRecords" => {
                let request = decode::<WriteRecordsRequest>(input)?;
                let (first_offset, last_offset) = self
                    .shared
                    .writers
                    .write(request.writer_id, request.records)
                    .await?;
                WriteRecordsResponse {
                    first_offset,
                    last_offset,
                }
                .encode_to_vec()
            }
            "CloseWriters" => {
                decode::<CloseWritersRequest>(input)?;
                let failed = self.shared.writers.close_all().await;
                CloseWritersResponse {
                    failed: failed as i32,
                }
                .encode_to_vec()
            }
            "CloseWriter" => {
                let request = decode::<CloseWriterRequest>(input)?;
                let last_offset = self.shared.writers.close(request.writer_id).await?;
                CloseWriterResponse { last_offset }.encode_to_vec()
            }
            _ => {
                return Err(StreamError::new(
                    Reason::Unsupported,
                    format!("Unknown method {method}"),
                ));
            }
        };
        Ok(output)
    }
}

/// The name of the memory extension, for a bridge that selects an extension by name.
pub const MEMORY_EXTENSION_NAME: &str = "temporalio-streams-memory";

/// The Stream runtime on the memory Store, for tests. It serves the same calls with the same
/// rules as on Redis, and it keeps its Streams in this process only.
#[derive(Debug)]
pub struct MemoryStreamExtension;

impl MemoryStreamExtension {
    /// Builds an extension on an empty memory Store.
    pub fn create(
        retention: Duration,
        safety_ttl: Duration,
        meter: Option<TemporalMeter>,
    ) -> Arc<StreamExtension> {
        let memory = Arc::new(MemoryStore::new(retention, safety_ttl));
        Self::with_store(memory, meter)
    }

    /// Builds an extension from an encoded [RedisStreamConfig]. The URLs don't count, so a
    /// plugin can pass the config it builds for Redis.
    pub fn from_config_bytes(
        config: &[u8],
        meter: Option<TemporalMeter>,
    ) -> StreamResult<Arc<StreamExtension>> {
        let config = RedisStreamConfig::decode(config)
            .map_err(|error| StreamError::invalid(format!("The config is bad: {error}")))?;
        let defaults = RedisStoreOptions::new(String::new());
        let memory = Arc::new(MemoryStore::new(
            duration(config.retention).unwrap_or(defaults.retention),
            duration(config.safety_ttl).unwrap_or(defaults.safety_ttl),
        ));
        Ok(Self::with_options(memory, config.backend, meter))
    }

    /// Builds an extension on any Store, for tests of a Store that lacks some capabilities.
    #[doc(hidden)]
    pub fn with_store(store: Arc<dyn Store>, meter: Option<TemporalMeter>) -> Arc<StreamExtension> {
        Self::with_options(store, String::new(), meter)
    }

    fn with_options(
        store: Arc<dyn Store>,
        backend: String,
        meter: Option<TemporalMeter>,
    ) -> Arc<StreamExtension> {
        let mut options = RedisStoreOptions::new(String::new());
        options.retention = store.default_retention();
        let options = ExtensionOptions {
            store: options,
            backend,
        };
        StreamExtension::build(MEMORY_EXTENSION_NAME, options, meter, Some(store))
    }
}

pub(crate) fn decode<M: Message + Default>(input: &[u8]) -> StreamResult<M> {
    M::decode(input).map_err(|error| StreamError::invalid(format!("The request is bad: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_config_fills_the_defaults() {
        let options = ExtensionOptions::from_config(RedisStreamConfig {
            urls: vec!["redis://x:1".to_string()],
            backend: "redis-a".to_string(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(options.backend, "redis-a");
        assert_eq!(options.store.key_prefix, "temporal-streams");
        assert_eq!(options.store.retention, Duration::from_secs(7 * 86_400));
        assert_eq!(options.store.safety_ttl, Duration::from_secs(90 * 86_400));
        assert_eq!(options.store.blocking_reads_per_node, 256);
        assert!(ExtensionOptions::from_config(RedisStreamConfig::default()).is_err());
    }
}
