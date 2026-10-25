//! The Stream service operations on the Store, for Activities, clients and the Local Activity that
//! runs each Workflow call.

use crate::{
    Reason, StreamError, StreamResult,
    digest::batch_digest,
    ids::check_producer,
    ids::check_stream_id,
    metrics::Metrics,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        Producer, StreamProducerKind, StreamRef, StreamStatus,
    },
    record::{caller_status, close_record, data_records, supersession},
    store::{Batch, Capabilities, CreateOutcome, MAX_SEQUENCE, Owner, Store, Withdrawal},
};
use std::{sync::Arc, time::Duration};

/// The Stream service on one Store.
pub(crate) struct StreamService {
    pub(crate) store: Arc<dyn Store>,
    pub(crate) metrics: Metrics,
    /// The backend name for the refs the service builds.
    pub(crate) backend: String,
}

fn duration(value: Option<prost_types::Duration>) -> Option<Duration> {
    let value = value?;
    let duration = Duration::new(value.seconds.max(0) as u64, value.nanos.max(0) as u32);
    (!duration.is_zero()).then_some(duration)
}

fn proto_duration(value: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: value.as_secs() as i64,
        nanos: value.subsec_nanos() as i32,
    }
}

fn stream_id(stream: Option<&StreamRef>) -> StreamResult<&str> {
    let id = stream.map(|stream| stream.id.as_str()).unwrap_or_default();
    check_stream_id(id)?;
    Ok(id)
}

/// The `Superseded` facts of a create that withdraws a retry run's previous run. It has no epoch,
/// so a Reader withdraws only that run's Records.
fn run_supersession(owner: Option<&Producer>, withdraw_run: &str) -> Vec<u8> {
    let producer = Producer {
        epoch: None,
        ..owner.cloned().unwrap_or_default()
    };
    supersession(&producer, withdraw_run)
}

/// Checks a Producer that writes outside Workflow code.
fn outside_producer(producer: Option<&Producer>) -> StreamResult<&Producer> {
    let producer = producer.ok_or_else(|| StreamError::invalid("The request has no Producer"))?;
    check_producer(producer)?;
    if producer.kind == StreamProducerKind::Workflow as i32 {
        return Err(StreamError::invalid(
            "A Workflow writes with the Producer of its Local Activity",
        ));
    }
    Ok(producer)
}

/// Checks the Producer of a close. A Workflow's close Activity closes as the run's Workflow
/// Producer, so a Workflow Producer may close.
fn close_producer(producer: Option<&Producer>) -> StreamResult<&Producer> {
    let producer = producer.ok_or_else(|| StreamError::invalid("The request has no Producer"))?;
    check_producer(producer)?;
    Ok(producer)
}

impl StreamService {
    pub(crate) async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse> {
        check_stream_id(&request.stream_id)?;
        let stream = request
            .r#ref
            .clone()
            .filter(|stream| stream.id == request.stream_id)
            .unwrap_or_else(|| StreamRef {
                id: request.stream_id.clone(),
                ..Default::default()
            });
        // A Workflow's call from outside Workflow code, such as from a Local Activity, sets the
        // owner run. Then the Stream has the safety TTL of a Workflow-owned Stream.
        let owner = match &request.owner {
            Some(_) => {
                self.store
                    .capabilities()
                    .require(Capabilities::SAFETY_TTL)?;
                Owner::Workflow
            }
            None => Owner::Client,
        };
        if !request.withdraw_run.is_empty() {
            // The `Superseded` item names its Producer, which here can only be the owner.
            if request.owner.is_none() {
                return Err(StreamError::invalid(
                    "A create with withdraw_run needs the owner run",
                ));
            }
            self.store.capabilities().require(Capabilities::RUN_FENCE)?;
        }
        let bytes = run_supersession(request.owner.as_ref(), &request.withdraw_run);
        let withdrawal = Withdrawal {
            withdraw_run: &request.withdraw_run,
            supersession: &bytes,
            tasks: None,
        };
        let created = self
            .store
            .create(
                &request.namespace,
                &stream,
                duration(request.retention),
                owner,
                &withdrawal,
            )
            .await?;
        if created.flagged {
            self.metrics.superseded();
        }
        let status = match created.outcome {
            CreateOutcome::Created | CreateOutcome::Exists => StreamStatus::Open,
            CreateOutcome::Closed => StreamStatus::Closed,
            CreateOutcome::Expired => StreamStatus::Expired,
            CreateOutcome::Conflict => {
                return Err(StreamError::new(
                    Reason::AlreadyExists,
                    format!(
                        "The Stream exists with the Retention {:?}",
                        created.retention
                    ),
                ));
            }
        };
        Ok(CreateStreamResponse {
            stream: Some(StreamRef {
                id: request.stream_id,
                backend: self.backend.clone(),
                ..Default::default()
            }),
            created: created.outcome == CreateOutcome::Created,
            status: status as i32,
        })
    }

    pub(crate) async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse> {
        stream_id(request.stream.as_ref())?;
        let stream = request.stream.clone().unwrap_or_default();
        let producer = outside_producer(request.producer.as_ref())?;
        if !(1..=MAX_SEQUENCE).contains(&request.sequence) {
            return Err(StreamError::invalid(
                "The sequence must be from 1 to 2^53 - 1",
            ));
        }
        // Without the fence and the flag, a copy or a stale batch would reach Readers.
        self.store
            .capabilities()
            .require(Capabilities::PRODUCER_FENCE.union(Capabilities::WITHDRAWAL))?;
        if !request.withdraw_run.is_empty() {
            // Only a retry run's Local Activity withdraws its previous run, as only the owner's
            // create does. Any other Producer could withdraw and fence an unrelated run.
            if producer.kind != StreamProducerKind::LocalActivity as i32
                || request.withdraw_run == producer.run_id
            {
                return Err(StreamError::invalid(
                    "Only a Local Activity of a later run withdraws a run",
                ));
            }
            self.store.capabilities().require(Capabilities::RUN_FENCE)?;
        }
        let digest = batch_digest(&request.records);
        let records = data_records(producer, request.records)?;
        let bytes = supersession(producer, &request.withdraw_run);
        let batch = Batch {
            producer,
            sequence: request.sequence,
            records: &records,
            digest: &digest,
            withdrawal: Withdrawal {
                withdraw_run: &request.withdraw_run,
                supersession: &bytes,
                tasks: None,
            },
        };
        let appended = self.store.append(&request.namespace, &stream, &batch).await;
        let appended = self.count(appended)?;
        let (first_offset, last_offset) = appended.offsets.unwrap_or_default();
        Ok(AppendRecordsResponse {
            first_offset,
            last_offset,
        })
    }

    /// Counts a fence or a flag of an append on the metrics.
    pub(crate) fn count<T: Flagged>(&self, result: StreamResult<T>) -> StreamResult<T> {
        match &result {
            Ok(value) if value.flagged() => self.metrics.superseded(),
            Err(error) if error.reason == Reason::Fenced => self.metrics.fenced(),
            _ => {}
        }
        result
    }

    pub(crate) async fn close_stream(
        &self,
        request: CloseStreamRequest,
    ) -> StreamResult<CloseStreamResponse> {
        stream_id(request.stream.as_ref())?;
        let stream = request.stream.clone().unwrap_or_default();
        let producer = close_producer(request.producer.as_ref())?;
        self.store.capabilities().require(Capabilities::CLOSE)?;
        let status = caller_status(request.status, request.failure.is_some())?;
        let record = close_record(producer, request.result, request.failure, status);
        self.store
            .close(&request.namespace, &stream, producer, &record)
            .await?;
        Ok(CloseStreamResponse {})
    }

    pub(crate) async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        let id = stream_id(request.stream.as_ref())?;
        let (state, _) = self.store.read(&request.namespace, id, 0, 0).await?;
        if !state.exists {
            return Err(StreamError::new(
                Reason::NotFound,
                "Nobody created the Stream",
            ));
        }
        let status = if state.closed_at.is_some() {
            StreamStatus::Closed
        } else if state.expired() {
            StreamStatus::Expired
        } else {
            StreamStatus::Open
        };
        Ok(DescribeStreamResponse {
            status: status as i32,
            last_offset: state.newest,
            retention: Some(proto_duration(state.retention)),
        })
    }
}

/// A Store answer that may have withdrawn Records.
pub(crate) trait Flagged {
    fn flagged(&self) -> bool;
}

impl Flagged for crate::redis::Appended {
    fn flagged(&self) -> bool {
        self.flagged
    }
}
