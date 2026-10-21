//! The five Stream service operations on the Redis Store, for Activities and clients. Workflow
//! code reaches the same Store through the Workflow Task interceptor.

use crate::{
    Reason, StreamError, StreamResult,
    ids::check_producer,
    ids::check_stream_id,
    metrics::Metrics,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        Producer, ProducerKind, ReadRecordsRequest, ReadRecordsResponse, ReadStart, StreamRecord,
        StreamRecordKind, StreamRef, StreamStatus, Supersession,
    },
    record::{close_record, data_records, supersession},
    store::{
        Batch, Capabilities, CreateOutcome, FORMAT_VERSION, Owner, Store, StreamState, Withdrawal,
    },
};
use prost::Message;
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

/// The longest wait of one read, for new Records and for the create.
pub const MAX_WAIT: Duration = Duration::from_secs(60);
/// The most Records one read gives.
pub const MAX_RECORDS: usize = 1000;
/// The most log entries one page scans.
const MAX_SCAN: usize = 10 * MAX_RECORDS;
/// How often a read looks again for a Stream that nobody created yet.
const CREATE_POLL: Duration = Duration::from_millis(100);

/// The Stream service on one Store.
pub(crate) struct StreamService {
    pub(crate) store: Arc<dyn Store>,
    pub(crate) metrics: Metrics,
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

/// Checks a Producer that writes outside Workflow code.
fn outside_producer(producer: Option<&Producer>) -> StreamResult<&Producer> {
    let producer = producer.ok_or_else(|| StreamError::invalid("The request has no Producer"))?;
    check_producer(producer)?;
    if producer.kind == ProducerKind::Workflow as i32 {
        return Err(StreamError::invalid(
            "A Workflow Producer writes through the Workflow Task interceptor only",
        ));
    }
    Ok(producer)
}

impl StreamService {
    pub(crate) async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse> {
        check_stream_id(&request.stream_id)?;
        let created = self
            .store
            .create(
                &request.namespace,
                &request.stream_id,
                duration(request.retention),
                Owner::Client,
                &Withdrawal::default(),
            )
            .await?;
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
        let id = stream_id(request.stream.as_ref())?;
        let producer = outside_producer(request.producer.as_ref())?;
        if request.sequence < 1 {
            return Err(StreamError::invalid("The sequence must be 1 or more"));
        }
        // Without the fence and the flag, a copy or a stale batch would reach Readers.
        self.store
            .capabilities()
            .require(Capabilities::PRODUCER_FENCE.union(Capabilities::WITHDRAWAL))?;
        let records = data_records(producer, request.records)?;
        let bytes = supersession(producer, None, &[], "");
        let batch = Batch {
            producer,
            sequence: request.sequence,
            records: &records,
            withdrawal: Withdrawal {
                supersession: &bytes,
                ..Default::default()
            },
        };
        let appended = self.store.append(&request.namespace, id, &batch).await;
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
        let id = stream_id(request.stream.as_ref())?;
        let producer = outside_producer(request.producer.as_ref())?;
        self.store.capabilities().require(Capabilities::CLOSE)?;
        let record = close_record(producer, request.result, request.failure);
        let bytes = supersession(producer, None, &[], "");
        let withdrawal = Withdrawal {
            supersession: &bytes,
            ..Default::default()
        };
        self.store
            .close(&request.namespace, id, producer, &record, &withdrawal)
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

    /// A long poll. It ends with the Records it found, a close Record, an Idle Record after
    /// `idle_timeout`, or nothing after `wait`.
    pub(crate) async fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> StreamResult<ReadRecordsResponse> {
        let id = stream_id(request.stream.as_ref())?.to_string();
        let namespace = request.namespace.as_str();
        if request.from_offset < 0 {
            return Err(StreamError::new(Reason::Offset, "The offset is below zero"));
        }
        let max = match request.max_records {
            n if n <= 0 => MAX_RECORDS,
            n => (n as usize).min(MAX_RECORDS),
        };
        let started = Instant::now();
        let wait = duration(request.wait).unwrap_or_default().min(MAX_WAIT);
        let idle = duration(request.idle_timeout);
        let create_wait = duration(request.create_wait)
            .unwrap_or_default()
            .min(MAX_WAIT);
        // The read can't end on idle later than on its wait, so an idle timeout past the wait
        // is the facade's to count across reads.
        let idle_at = idle.filter(|idle| *idle <= wait).map(|idle| started + idle);
        let deadline = started + wait;

        let mut state = self.created(namespace, &id, started + create_wait).await?;
        if state.version > FORMAT_VERSION {
            return Err(StreamError::new(
                Reason::FormatTooNew,
                format!("format {} is newer than {FORMAT_VERSION}", state.version),
            ));
        }
        let from = match request.from_offset {
            0 if request.start == ReadStart::End as i32 => state.newest + 1,
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
        // Where the next scan starts. A scan that finds only other Topics moves it on, so the
        // wait after it doesn't wake for the same entries.
        let mut cursor = from;
        loop {
            check_expiry(&state, cursor)?;
            let (page, scanned) = self
                .page(namespace, &id, &state, cursor, max, &topics)
                .await?;
            if let Some(page) = page {
                return Ok(page);
            }
            cursor = scanned;
            let now = Instant::now();
            if idle_at.is_some_and(|at| at <= now) {
                return Ok(idle_response(cursor));
            }
            let until = idle_at.map_or(deadline, |idle_at| idle_at.min(deadline));
            if until <= now {
                return Ok(ReadRecordsResponse {
                    records: vec![],
                    next_offset: cursor,
                    done: false,
                });
            }
            let after = (cursor - 1).max(state.newest);
            self.metrics.read_waits(true);
            let woke = self.store.wait_after(namespace, &id, after, until).await;
            self.metrics.read_waits(false);
            woke?;
            state = self.store.read(namespace, &id, 0, 0).await?.0;
        }
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
    /// entry it scanned. A page that ends in the close Record is done.
    async fn page(
        &self,
        namespace: &str,
        id: &str,
        state: &StreamState,
        from: i64,
        max: usize,
        topics: &TopicFilter,
    ) -> StreamResult<(Option<ReadRecordsResponse>, i64)> {
        // A read past the close gives the close Record again, so every read ends with it.
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
            };
            return Ok((Some(page), from));
        }
        // A read from the start skips what the Retention hides. A read from an offset fails on
        // it, since the Reader asked for those Records.
        let retained = state.retained_from();
        let mut start = if from > 0 { from } else { retained };
        let mut records = vec![];
        let mut next = from;
        let mut done = false;
        // A Topic filter can skip many entries, so one page scans a bounded number of them.
        let mut scanned = 0;
        while records.len() < max && !done && scanned < MAX_SCAN {
            let (_, entries) = self.store.read(namespace, id, start, max).await?;
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
                next = entry.offset + 1;
                start = next;
                if let Some(bytes) = entry.supersession {
                    let supersession = Supersession::decode(bytes.as_slice())
                        .map_err(|error| StreamError::storage(error.to_string()))?;
                    records.push(StreamRecord {
                        kind: StreamRecordKind::Superseded as i32,
                        offset: entry.offset,
                        supersession: Some(supersession),
                        ..Default::default()
                    });
                }
                let Some(bytes) = entry.record else {
                    continue;
                };
                let record = decode(&bytes, entry.offset)?;
                if record.kind == StreamRecordKind::Close as i32 {
                    records.push(record);
                    done = true;
                    break;
                }
                if topics.passes(&record.topic) {
                    records.push(record);
                }
                if records.len() >= max {
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
        };
        Ok((Some(page), next))
    }
}

/// Which Topics a read gives. A `Superseded` item passes every filter, so the page adds it
/// before the filter runs.
pub(crate) struct TopicFilter {
    topics: Vec<String>,
    prefixes: Vec<String>,
}

impl TopicFilter {
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

fn idle_response(next_offset: i64) -> ReadRecordsResponse {
    ReadRecordsResponse {
        records: vec![StreamRecord {
            kind: StreamRecordKind::Idle as i32,
            offset: next_offset,
            ..Default::default()
        }],
        next_offset,
        done: false,
    }
}

/// Fails a read whose Records the Retention or the expiry removed, so a gap never passes in
/// silence. A read from the start of the Stream has no gap.
///
/// An open Stream that expired gets no more Records, so a read of it fails as well, or the
/// Reader would wait on it for good.
fn check_expiry(state: &StreamState, from: i64) -> StreamResult<()> {
    let open_tombstone = state.expired() && state.closed_at.is_none();
    if open_tombstone || (from > 0 && state.trimmed >= from) {
        return Err(expired());
    }
    Ok(())
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

impl Flagged for crate::redis::Closed {
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
