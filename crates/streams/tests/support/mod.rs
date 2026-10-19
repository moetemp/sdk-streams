//! What the Workflow output tests share: a store that logs every call, an owner whose status a
//! test sets, and the records a Workflow publishes.

#![allow(dead_code, unreachable_pub)]

use parking_lot::Mutex;
use std::{collections::VecDeque, sync::Arc, time::Duration};
use temporalio_common::protos::temporal::api::{
    common::v1::Payload, enums::v1::WorkflowExecutionStatus,
};
use temporalio_streams::{
    BEGINNING, Capabilities, MemoryStore, OwnerClient, OwnerDescription, OwnerError, ReadTarget,
    RunClose, StreamError, StreamResult, StreamStore,
    proto::{
        ChainId, DeleteOwnerRequest, DeleteOwnerResponse, OutputRecord, ReadRequest,
        StoreAppendRequest, StoreAppendResponse, StoreLatestRequest, StoreLatestResponse,
        StoreReadRequest, StoreReadResponse, StreamRecordKind, read_record,
    },
    read_page, stream_hash,
};

pub const TOPIC: &str = "results";

/// The attempt of a task whose last completed task started at event `prefix`, after `retry - 1`
/// reruns.
pub fn attempt(prefix: i64, retry: i64) -> i64 {
    (prefix << 16) | retry
}

/// What a Workflow publishing `values` on one topic commits, bodies already through the codec.
pub fn records(values: &[&str]) -> Vec<OutputRecord> {
    records_on(TOPIC, values)
}

pub fn records_on(topic: &str, values: &[&str]) -> Vec<OutputRecord> {
    values
        .iter()
        .map(|value| OutputRecord {
            topic: topic.to_string(),
            kind: StreamRecordKind::Data as i32,
            body: Some(Payload {
                data: format!("encrypted {value}").into_bytes(),
                ..Default::default()
            }),
            content_hash: vec![value.as_bytes()[0]; 32],
            ..Default::default()
        })
        .collect()
}

/// A memory stream store that logs every call, and can be told to fail appends.
#[derive(Default)]
pub struct RecordingStore {
    pub inner: MemoryStore,
    pub appends: Mutex<Vec<StoreAppendRequest>>,
    pub calls: Mutex<Vec<&'static str>>,
    /// Each retired session as `(topic, producer, attempt)`.
    pub retired: Mutex<Vec<(String, String, i64)>>,
    /// Failures the next appends answer with, one each, before the store takes them.
    pub failures: Mutex<VecDeque<StreamError>>,
    /// The failure every append answers with.
    pub refuse: Option<StreamError>,
    /// How long each append takes.
    pub append_delay: Option<Duration>,
    /// A retire waits for a permit here, when set.
    pub retire_gate: Option<Arc<tokio::sync::Semaphore>>,
}

impl RecordingStore {
    pub fn refusing(error: StreamError) -> Self {
        Self {
            refuse: Some(error),
            ..Default::default()
        }
    }

    pub fn failing_first(errors: impl IntoIterator<Item = StreamError>) -> Self {
        Self {
            failures: Mutex::new(errors.into_iter().collect()),
            ..Default::default()
        }
    }

    pub fn appends(&self) -> Vec<StoreAppendRequest> {
        self.appends.lock().clone()
    }

    pub fn calls(&self) -> Vec<&'static str> {
        self.calls.lock().clone()
    }

    pub fn retired(&self) -> Vec<(String, String, i64)> {
        self.retired.lock().clone()
    }

    /// Each append as `(topic, producer, attempt, sequence, record count)`.
    pub fn append_log(&self) -> Vec<(String, String, i64, i64, usize)> {
        self.appends()
            .into_iter()
            .map(|a| {
                (
                    a.topic,
                    a.producer_id,
                    a.attempt,
                    a.sequence,
                    a.records.len(),
                )
            })
            .collect()
    }

    /// What a reader of `topic` gets from the start, as each body's text or `SUPERSEDED`.
    pub async fn read(&self, topic: &str) -> Vec<String> {
        let chain = self.appends()[0].chain.clone().unwrap();
        let target = ReadTarget {
            stream_hash: stream_hash(&chain.namespace, "workflow", &chain.workflow_id, topic),
            chain,
            topic: topic.to_string(),
        };
        let request = ReadRequest {
            after: BEGINNING.to_string(),
            ..Default::default()
        };
        read_page(&self.inner, &target, &request)
            .await
            .unwrap()
            .records
            .into_iter()
            .map(|record| match record.record.unwrap() {
                read_record::Record::Stored(stored) => {
                    String::from_utf8(stored.body.unwrap().data).unwrap()
                }
                read_record::Record::Superseded(_) => "SUPERSEDED".to_string(),
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl StreamStore for RecordingStore {
    fn name(&self) -> &str {
        "recording"
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn append(&self, request: StoreAppendRequest) -> StreamResult<StoreAppendResponse> {
        self.calls.lock().push("append");
        if let Some(error) = &self.refuse {
            return Err(error.clone());
        }
        let failure = self.failures.lock().pop_front();
        if let Some(error) = failure {
            return Err(error);
        }
        if let Some(delay) = self.append_delay {
            tokio::time::sleep(delay).await;
        }
        let response = self.inner.append(request.clone()).await?;
        self.appends.lock().push(request);
        Ok(response)
    }

    async fn read(&self, request: StoreReadRequest) -> StreamResult<StoreReadResponse> {
        self.calls.lock().push("read");
        self.inner.read(request).await
    }

    async fn latest(&self, request: StoreLatestRequest) -> StreamResult<StoreLatestResponse> {
        self.calls.lock().push("latest");
        self.inner.latest(request).await
    }

    async fn record_at(
        &self,
        chain: &ChainId,
        topic: &str,
        position: &str,
    ) -> StreamResult<Option<Vec<u8>>> {
        self.inner.record_at(chain, topic, position).await
    }

    async fn close_chain(&self, chain: &ChainId) -> StreamResult<()> {
        self.calls.lock().push("close_chain");
        self.inner.close_chain(chain).await
    }

    async fn close_topic(&self, chain: &ChainId, topic: &str) -> StreamResult<()> {
        self.calls.lock().push("close_topic");
        self.inner.close_topic(chain, topic).await
    }

    async fn retire_producer(
        &self,
        chain: &ChainId,
        topic: &str,
        producer_id: &str,
        attempt: i64,
    ) -> StreamResult<()> {
        self.calls.lock().push("retire_producer");
        if let Some(gate) = &self.retire_gate {
            let _permit = gate.acquire().await.unwrap();
        }
        self.retired
            .lock()
            .push((topic.to_string(), producer_id.to_string(), attempt));
        self.inner
            .retire_producer(chain, topic, producer_id, attempt)
            .await
    }

    async fn list_topics(&self, chain: &ChainId) -> StreamResult<Vec<String>> {
        self.calls.lock().push("list_topics");
        self.inner.list_topics(chain).await
    }

    async fn delete_owner(&self, request: DeleteOwnerRequest) -> StreamResult<DeleteOwnerResponse> {
        self.calls.lock().push("delete_owner");
        self.inner.delete_owner(request).await
    }
}

/// An owner every run of which stands as the test sets it. Its History read logs what it was
/// asked and answers with the close the test set.
pub struct Owner {
    pub status: Mutex<WorkflowExecutionStatus>,
    /// The started event id of the `WorkflowTaskCompleted` before the close event.
    pub closing_task: Mutex<Option<i64>>,
    pub asked: Mutex<Vec<(String, String, String)>>,
    /// A History read waits for a permit here, when set.
    pub read_gate: Mutex<Option<Arc<tokio::sync::Semaphore>>>,
}

impl Owner {
    /// A run that stands at `status`, with no Workflow Task before its close event.
    pub fn with(status: WorkflowExecutionStatus) -> Arc<Self> {
        Self::closed_by(status, None)
    }

    /// A run that stands at `status`, closed by the task that started at `closing_task`.
    pub fn closed_by(status: WorkflowExecutionStatus, closing_task: Option<i64>) -> Arc<Self> {
        Arc::new(Self {
            status: Mutex::new(status),
            closing_task: Mutex::new(closing_task),
            asked: Default::default(),
            read_gate: Default::default(),
        })
    }
}

#[async_trait::async_trait]
impl OwnerClient for Owner {
    async fn describe(
        &self,
        _namespace: &str,
        _workflow_id: &str,
        run_id: &str,
    ) -> Result<OwnerDescription, OwnerError> {
        Ok(OwnerDescription {
            run_id: run_id.to_string(),
            first_run_id: run_id.to_string(),
            status: *self.status.lock(),
        })
    }

    async fn run_close(
        &self,
        namespace: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<Option<RunClose>, OwnerError> {
        self.asked.lock().push((
            namespace.to_string(),
            workflow_id.to_string(),
            run_id.to_string(),
        ));
        let gate = self.read_gate.lock().clone();
        if let Some(gate) = gate {
            let _permit = gate.acquire().await.unwrap();
        }
        let status = *self.status.lock();
        Ok(
            (status != WorkflowExecutionStatus::Running).then(|| RunClose {
                status,
                closing_task_started_event_id: *self.closing_task.lock(),
            }),
        )
    }
}
