//! The five Stream service operations on the Store, for Activities, clients and the Local
//! Activity that runs each Workflow call.

use crate::{
    Reason, StreamError, StreamResult,
    digest::batch_digest,
    epoch::withdraws,
    ids::check_producer,
    ids::check_stream_id,
    metrics::Metrics,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        Producer, ReadRecordsRequest, ReadRecordsResponse, StreamProducerKind, StreamReadStart,
        StreamRecord, StreamRecordKind, StreamRef, StreamStatus, Supersession, WorkflowTaskEpoch,
    },
    record::{caller_status, close_record, data_records, supersession},
    store::{
        Batch, Capabilities, CreateOutcome, FORMAT_VERSION, MAX_SEQUENCE, Owner, Store,
        StreamState, TaskFacts, Withdrawal,
    },
};
use prost::Message;
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

/// The longest wait of one read, for new Records and for the create.
pub const MAX_WAIT: Duration = Duration::from_secs(60);
/// The maximum number of Records in one read.
pub const MAX_RECORDS: usize = 1000;
/// The maximum number of log entries that one page scans.
const MAX_SCAN: usize = 10 * MAX_RECORDS;
/// The number of log entries that a filtered page reads at a time, whatever its `max_records`.
const SCAN_CHUNK: usize = MAX_RECORDS;
/// How often a read looks again for a Stream that nobody created yet.
const CREATE_POLL: Duration = Duration::from_millis(100);

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

/// The `Superseded` facts that withdraw a run's orphaned Local Activity Records. They name the
/// run's Workflow Producer in the batch's task, so that a Reader applies the Workflow rule.
fn orphan_supersession(
    producer: &Producer,
    completed: Option<&WorkflowTaskEpoch>,
    failed_tasks: &[i64],
) -> Vec<u8> {
    let epoch = producer.epoch.unwrap_or_default();
    Supersession {
        producer_kind: StreamProducerKind::Workflow as i32,
        producer_id: crate::ids::workflow_producer_id(&producer.run_id),
        epoch: Some(crate::proto::ProducerEpoch {
            task_started_event_id: epoch.task_started_event_id,
            task_attempt: epoch.task_attempt,
            task_started_time: epoch.task_started_time,
            ..Default::default()
        }),
        previous_completed_task: completed.copied(),
        failed_tasks: failed_tasks.to_vec(),
        run_id: producer.run_id.clone(),
        ..Default::default()
    }
    .encode_to_vec()
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
        // A Local Activity sends its run's task facts, so that the batch withdraws the run's
        // orphaned Local Activity Records. These are the Records of the tasks that never completed.
        let completed = request
            .previous_completed_task
            .filter(|c| c.started_event_id > 0);
        let orphans = (producer.kind == StreamProducerKind::LocalActivity as i32
            && request.previous_completed_task.is_some())
        .then(|| orphan_supersession(producer, completed.as_ref(), &request.failed_tasks));
        let batch = Batch {
            producer,
            sequence: request.sequence,
            records: &records,
            digest: &digest,
            withdrawal: Withdrawal {
                withdraw_run: &request.withdraw_run,
                supersession: &bytes,
                tasks: orphans.as_ref().map(|orphans| TaskFacts {
                    completed: completed.as_ref(),
                    failed_tasks: &request.failed_tasks,
                    supersession: orphans,
                }),
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

    /// A long poll that returns the Records it found, the close Record, or nothing after `wait`.
    pub(crate) async fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> StreamResult<ReadRecordsResponse> {
        let id = stream_id(request.stream.as_ref())?.to_string();
        let namespace = request.namespace.as_str();
        if request.from_offset < 0 {
            return Err(StreamError::new(Reason::Offset, "The offset is below zero"));
        }
        let max_bytes = max_bytes(request.max_bytes)?;
        if request.last < 0 {
            return Err(StreamError::invalid("last must be 0 or more"));
        }
        if request.last > 0 {
            if request.wake_only || request.from_offset > 0 {
                return Err(StreamError::invalid(
                    "A read of the last Records takes no from_offset and no wake_only",
                ));
            }
            return self.read_last(request, max_bytes).await;
        }
        if request.wake_only || request.to_offset != 0 {
            return Err(StreamError::new(
                Reason::Unsupported,
                "The Store reads no to_offset or wake_only, which the native backend serves",
            ));
        }
        let max = match request.max_records {
            n if n <= 0 => MAX_RECORDS,
            n => (n as usize).min(MAX_RECORDS),
        };
        let started = Instant::now();
        let wait = duration(request.wait).unwrap_or_default().min(MAX_WAIT);
        let create_wait = duration(request.create_wait)
            .unwrap_or_default()
            .min(MAX_WAIT);
        let deadline = started + wait;

        let mut state = self.created(namespace, &id, started + create_wait).await?;
        if state.version > FORMAT_VERSION {
            return Err(StreamError::new(
                Reason::FormatTooNew,
                format!("format {} is newer than {FORMAT_VERSION}", state.version),
            ));
        }
        let from = match request.from_offset {
            0 if request.start == StreamReadStart::End as i32 => state.newest + 1,
            from => from,
        };
        if from > state.newest + 1 {
            return Err(StreamError::new(
                Reason::Offset,
                format!(
                    "The offset {from} is past the newest offset + 1, {}",
                    state.newest + 1
                ),
            ));
        }
        let topics = TopicFilter {
            topics: request.topics,
            prefixes: request.topic_prefixes,
        };
        // Where the next scan starts. A scan that finds only other Topics advances it, so that the
        // next wait doesn't wake for the same entries.
        let mut cursor = from;
        loop {
            check_expiry(&state, cursor)?;
            let (page, scanned) = self
                .page(namespace, &id, &state, cursor, (max, max_bytes), &topics)
                .await?;
            if let Some(page) = page {
                return Ok(page);
            }
            // A scan that stopped at its cap left entries, so the next scan starts with no wait.
            let more = scanned > cursor && scanned <= state.newest;
            cursor = scanned;
            if deadline <= Instant::now() {
                return Ok(ReadRecordsResponse {
                    next_offset: cursor,
                    ..Default::default()
                });
            }
            if more {
                continue;
            }
            let after = (cursor - 1).max(state.newest);
            self.metrics.read_waits(true);
            let woke = self.store.wait_after(namespace, &id, after, deadline).await;
            self.metrics.read_waits(false);
            woke?;
            state = self.store.read(namespace, &id, 0, 0).await?.0;
        }
    }

    /// The newest `last` Records that pass the filter, oldest first, and the close Record at the
    /// end when the Stream is closed. A Record that a newer `Superseded` item withdraws doesn't
    /// count. It never waits for new Records.
    async fn read_last(
        &self,
        request: ReadRecordsRequest,
        max_bytes: usize,
    ) -> StreamResult<ReadRecordsResponse> {
        let id = stream_id(request.stream.as_ref())?.to_string();
        let namespace = request.namespace.as_str();
        let count = (request.last as usize).min(MAX_RECORDS);
        let create_wait = duration(request.create_wait)
            .unwrap_or_default()
            .min(MAX_WAIT);
        let state = self
            .created(namespace, &id, Instant::now() + create_wait)
            .await?;
        if state.version > FORMAT_VERSION {
            return Err(StreamError::new(
                Reason::FormatTooNew,
                format!("format {} is newer than {FORMAT_VERSION}", state.version),
            ));
        }
        check_expiry(&state, 0)?;
        let topics = TopicFilter {
            topics: request.topics,
            prefixes: request.topic_prefixes,
        };
        let retained = state.retained_from();
        let (mut picked, mut close) = (vec![], None);
        let mut withdrawals: Vec<Supersession> = vec![];
        let (mut bytes, mut scanned) = (0, 0);
        let mut before = state.newest;
        'scan: while before >= retained && picked.len() < count && scanned < MAX_SCAN {
            let entries = self
                .store
                .read_back(namespace, &id, before, count.clamp(16, MAX_RECORDS))
                .await?;
            if entries.is_empty() {
                break;
            }
            scanned += entries.len();
            for entry in entries {
                if entry.offset < retained {
                    break 'scan;
                }
                before = entry.offset - 1;
                // An entry's `Superseded` item withdraws earlier Records, not the entry's own.
                if let Some(record) = entry.record {
                    let record = decode(&record, entry.offset)?;
                    let size = record.encoded_len();
                    // The close Record ends the page and doesn't count toward the byte cap.
                    if record.kind == StreamRecordKind::Close as i32 {
                        close = Some(record);
                    } else if topics.passes(&record.topic)
                        && !withdrawals.iter().any(|s| withdraws(s, &record))
                    {
                        // A Record over the cap is still returned alone, so that the read always
                        // returns a Record.
                        if !picked.is_empty() && bytes + size > max_bytes {
                            break 'scan;
                        }
                        bytes += size;
                        picked.push(record);
                        if picked.len() == count {
                            break 'scan;
                        }
                    }
                }
                if let Some(supersession) = entry.supersession {
                    withdrawals.push(
                        Supersession::decode(supersession.as_slice())
                            .map_err(|error| StreamError::storage(error.to_string()))?,
                    );
                }
            }
        }
        picked.reverse();
        picked.extend(close);
        Ok(ReadRecordsResponse {
            records: picked,
            next_offset: state.newest + 1,
            done: state.closed_at.is_some(),
            ..Default::default()
        })
    }

    /// The state of a Stream, after waiting up to `until` for its create.
    async fn created(
        &self,
        namespace: &str,
        id: &str,
        until: Instant,
    ) -> StreamResult<StreamState> {
        loop {
            let (state, _) = self.store.read(namespace, id, 0, 0).await?;
            if state.exists {
                return Ok(state);
            }
            let now = Instant::now();
            if now >= until {
                return Err(StreamError::new(
                    Reason::NotFound,
                    "Nobody created the Stream",
                ));
            }
            tokio::time::sleep(CREATE_POLL.min(until - now)).await;
        }
    }

    /// The Records from `from`, or `None` when there is none yet, with the offset after the last
    /// entry it scanned. A page that ends in the close Record is done. `(max, max_bytes)` caps
    /// the items and their bytes.
    async fn page(
        &self,
        namespace: &str,
        id: &str,
        state: &StreamState,
        from: i64,
        (max, max_bytes): (usize, usize),
        topics: &TopicFilter,
    ) -> StreamResult<(Option<ReadRecordsResponse>, i64)> {
        // A read past the close returns the close Record again, so that every read ends with it.
        if let Some(closed_at) = state.closed_at.filter(|at| from > *at) {
            let (_, entries) = self.store.read(namespace, id, closed_at, 1).await?;
            let mut records = vec![];
            for entry in entries {
                if let Some(bytes) = entry.record {
                    records.push(decode(&bytes, entry.offset)?);
                }
            }
            let page = ReadRecordsResponse {
                records,
                next_offset: from,
                done: true,
                ..Default::default()
            };
            return Ok((Some(page), from));
        }
        // A read from the start skips the Records that the Retention hides. A read from an offset
        // fails on them, since the Reader asked for those Records.
        let retained = state.retained_from();
        let mut start = if from > 0 { from } else { retained };
        let mut records = vec![];
        let mut next = from;
        let (mut done, mut full) = (false, false);
        let mut bytes = 0;
        // A Topic filter can skip many entries, so one page scans a bounded number of them, and
        // reads more than `max` at a time.
        let chunk = if topics.filters() { SCAN_CHUNK } else { max };
        let mut scanned = 0;
        while records.len() < max && !done && !full && scanned < MAX_SCAN {
            let (chunk_state, entries) = self.store.read(namespace, id, start, chunk).await?;
            // A trim after the state read leaves a gap before these entries.
            check_expiry(&chunk_state, if from > 0 { start } else { 0 })?;
            if entries.is_empty() {
                break;
            }
            scanned += entries.len();
            for entry in entries {
                if entry.offset < retained {
                    if from > 0 {
                        return Err(expired());
                    }
                    next = entry.offset + 1;
                    start = next;
                    continue;
                }
                let mut items = vec![];
                if let Some(bytes) = entry.supersession {
                    let supersession = Supersession::decode(bytes.as_slice())
                        .map_err(|error| StreamError::storage(error.to_string()))?;
                    items.push(StreamRecord {
                        kind: StreamRecordKind::Superseded as i32,
                        offset: entry.offset,
                        supersession: Some(supersession),
                        ..Default::default()
                    });
                }
                if let Some(bytes) = entry.record {
                    let record = decode(&bytes, entry.offset)?;
                    done = record.kind == StreamRecordKind::Close as i32;
                    if done || topics.passes(&record.topic) {
                        items.push(record);
                    }
                }
                // An entry over the cap is still returned alone, so that the read always makes
                // progress. The next read starts at the entry that didn't fit.
                let size: usize = items.iter().map(Message::encoded_len).sum();
                if !records.is_empty() && bytes + size > max_bytes {
                    (done, full) = (false, true);
                    next = entry.offset;
                    break;
                }
                bytes += size;
                records.extend(items);
                next = entry.offset + 1;
                start = next;
                if done || records.len() >= max {
                    break;
                }
            }
        }
        if records.is_empty() {
            return Ok((None, next));
        }
        let page = ReadRecordsResponse {
            records,
            next_offset: next,
            done,
            ..Default::default()
        };
        Ok((Some(page), next))
    }
}

/// Which Topics a read returns. A `Superseded` item passes every filter, so the page adds it before
/// the filter runs.
pub(crate) struct TopicFilter {
    topics: Vec<String>,
    prefixes: Vec<String>,
}

impl TopicFilter {
    fn filters(&self) -> bool {
        !self.topics.is_empty() || !self.prefixes.is_empty()
    }

    fn passes(&self, topic: &str) -> bool {
        (self.topics.is_empty() && self.prefixes.is_empty())
            || self.topics.iter().any(|t| t == topic)
            || self.prefixes.iter().any(|p| topic.starts_with(p.as_str()))
    }
}

fn decode(bytes: &[u8], offset: i64) -> StreamResult<StreamRecord> {
    let mut record = StreamRecord::decode(bytes)
        .map_err(|error| StreamError::storage(format!("A stored Record is bad: {error}")))?;
    record.offset = offset;
    Ok(record)
}

/// Fails a read whose Records the Retention or the expiry removed, so that the Reader always sees
/// the gap. A read from the start of the Stream has no gap.
///
/// A Tombstone fails every read. An open Stream that expired gets no more Records, so the Reader
/// would wait forever. A closed one lost its log and its close Record.
fn check_expiry(state: &StreamState, from: i64) -> StreamResult<()> {
    if state.expired() || (from > 0 && state.trimmed >= from) {
        return Err(expired());
    }
    Ok(())
}

/// The byte cap of a page, from a request's `max_bytes`. Zero is no cap.
fn max_bytes(value: i64) -> StreamResult<usize> {
    match value {
        0 => Ok(usize::MAX),
        n if n < 0 => Err(StreamError::invalid("max_bytes must be 0 or more")),
        n => Ok(usize::try_from(n).unwrap_or(usize::MAX)),
    }
}

fn expired() -> StreamError {
    StreamError::new(Reason::Expired, "The Records at the offset expired")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_topic_passes_by_exact_name_or_by_prefix() {
        let filter = |topics: &[&str], prefixes: &[&str]| TopicFilter {
            topics: topics.iter().map(|t| t.to_string()).collect(),
            prefixes: prefixes.iter().map(|p| p.to_string()).collect(),
        };
        assert!(filter(&[], &[]).passes("any"));
        let both = filter(&["turns"], &["tool/"]);
        assert!(both.passes("turns"));
        assert!(both.passes("tool/search"));
        assert!(!both.passes("turns/1"));
        assert!(!both.passes("tool"));
        assert!(filter(&[], &[""]).passes("x"));
    }
}
