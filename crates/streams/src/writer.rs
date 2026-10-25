//! Writers for Activities and clients. `write` appends its Records at once and returns their
//! offsets, as an awaited publish needs. A writer keeps one Producer epoch and one `sequence`
//! order, so that the Store writes a retried batch only once. Lang can buffer Records on its own
//! and write each batch with one call.
//!
//! The handles of one Producer epoch on one Stream share one writer. Each open adds a handle and
//! each close or free releases one. The last release drops the writer.

use crate::{
    Reason, StreamError, StreamResult,
    proto::{
        AppendRecord, AppendRecordsRequest, OpenWriterRequest, Producer, StreamProducerKind,
        StreamRef, WorkflowTaskEpoch,
    },
    service::StreamService,
    store::{MAX_BATCH_BYTES, MAX_RECORD_BYTES},
};
use parking_lot::Mutex;
use prost::Message;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

const DEFAULT_BATCH: usize = 1000;
const DEFAULT_RETRY: Duration = Duration::from_secs(10);
const FIRST_BACKOFF: Duration = Duration::from_millis(50);

/// The last `sequence` of any batch in this process. Every writer numbers its batches from it, so
/// that a writer that opens again after its close continues above its earlier batches.
static LAST_SEQUENCE: AtomicI64 = AtomicI64::new(0);

fn next_sequence() -> i64 {
    LAST_SEQUENCE.fetch_add(1, Ordering::Relaxed) + 1
}

#[derive(Default)]
struct State {
    last_offset: i64,
    /// A write that failed permanently. Every later call fails with it, since the batch may be in
    /// the Store under a `sequence` that the writer can't reuse.
    failed: Option<StreamError>,
    closed: bool,
}

struct Writer {
    namespace: String,
    stream: StreamRef,
    producer: Producer,
    previous_completed_task: Option<WorkflowTaskEpoch>,
    failed_tasks: Vec<i64>,
    max_batch: usize,
    max_retry: Duration,
    service: Arc<StreamService>,
    state: Mutex<State>,
    /// Allows one write at a time, so that the batches of one epoch reach the Store in order.
    writing: tokio::sync::Mutex<()>,
}

fn duration(value: Option<prost_types::Duration>) -> Option<Duration> {
    let value = value?;
    let duration = Duration::new(value.seconds.max(0) as u64, value.nanos.max(0) as u32);
    (!duration.is_zero()).then_some(duration)
}

impl Writer {
    /// The number of `records` in the next batch. The Store refuses a batch over its limit, so a
    /// batch ends at the count limit or the byte limit, whichever comes first.
    fn batch_len<'a>(&self, records: impl Iterator<Item = &'a AppendRecord>) -> usize {
        let mut count = 0;
        let mut bytes = 0;
        for record in records.take(self.max_batch) {
            let size = stored_size(&self.producer, record);
            if count > 0 && bytes + size > MAX_BATCH_BYTES {
                break;
            }
            bytes += size;
            count += 1;
        }
        count
    }

    /// Appends `records` now and returns the offsets of the first and the last one. Records past
    /// the Store's batch limit go in more batches, in order.
    async fn write(&self, mut records: Vec<AppendRecord>) -> StreamResult<(i64, i64)> {
        let _writing = self.writing.lock().await;
        // A close that started while this write waited has ended the writer.
        if self.state.lock().closed {
            return Err(StreamError::new(
                Reason::UnknownWriter,
                "The writer closed while the write waited",
            ));
        }
        let (mut first, mut last) = (0, 0);
        while !records.is_empty() {
            let count = self.batch_len(records.iter());
            let rest = records.split_off(count);
            if let Some(error) = &self.state.lock().failed {
                return Err(error.clone());
            }
            let sequence = next_sequence();
            let (batch_first, batch_last) = match self.send(records, sequence).await {
                Ok(offsets) => offsets,
                // The batch may be in the Store, so the writer can't continue in this epoch.
                Err(error) => {
                    self.state.lock().failed = Some(error.clone());
                    return Err(error);
                }
            };
            if first == 0 {
                first = batch_first;
            }
            last = batch_last;
            records = rest;
        }
        let mut state = self.state.lock();
        state.last_offset = state.last_offset.max(last);
        Ok((first, last))
    }

    /// Appends one batch and returns its first and last offset. After a Store failure it sends the
    /// batch again with the same `sequence`, so the dedupe key stops a second write.
    async fn send(&self, records: Vec<AppendRecord>, sequence: i64) -> StreamResult<(i64, i64)> {
        let request = AppendRecordsRequest {
            namespace: self.namespace.clone(),
            stream: Some(self.stream.clone()),
            producer: Some(self.producer.clone()),
            sequence,
            records,
            previous_completed_task: self.previous_completed_task,
            failed_tasks: self.failed_tasks.clone(),
            ..Default::default()
        };
        let until = Instant::now() + self.max_retry;
        let mut backoff = FIRST_BACKOFF;
        loop {
            match self.service.append_records(request.clone()).await {
                Ok(response) => return Ok((response.first_offset, response.last_offset)),
                Err(error)
                    if error.reason == Reason::Storage && Instant::now() + backoff < until =>
                {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(1));
                }
                Err(error) => return Err(error),
            }
        }
    }
}

/// The size of a Record once stored, with a margin for the envelope's own fields.
fn stored_size(producer: &Producer, record: &AppendRecord) -> usize {
    record.encoded_len() + producer.encoded_len() + 32
}

/// Refuses a Record over the Store's Record limit, before it reaches the Store.
fn check_sizes(producer: &Producer, records: &[AppendRecord]) -> StreamResult<()> {
    if let Some(big) = records
        .iter()
        .map(|record| stored_size(producer, record))
        .find(|size| *size > MAX_RECORD_BYTES)
    {
        return Err(StreamError::new(
            Reason::TooLarge,
            format!("A Record has about {big} bytes, more than {MAX_RECORD_BYTES}"),
        ));
    }
    Ok(())
}

/// One open writer and the handles that share it.
struct Open {
    writer: Arc<Writer>,
    key: String,
    handles: usize,
}

#[derive(Default)]
struct Table {
    open: HashMap<u64, Open>,
    /// The writer of each (namespace, Stream, Producer id, epoch), so a second open shares its
    /// sequence instead of reusing it.
    by_producer: HashMap<String, u64>,
}

impl Table {
    /// Releases one handle of a writer, or every handle with `all`. Returns the writer when it has
    /// no handle left, so that the caller ends it.
    fn release(&mut self, id: u64, all: bool) -> StreamResult<Option<Arc<Writer>>> {
        let open = self
            .open
            .get_mut(&id)
            .ok_or_else(|| StreamError::new(Reason::UnknownWriter, format!("No writer {id}")))?;
        open.handles -= 1;
        if open.handles > 0 && !all {
            return Ok(None);
        }
        let open = self.open.remove(&id).expect("found above");
        self.by_producer.remove(&open.key);
        Ok(Some(open.writer))
    }
}

/// The open writers of one runtime.
#[derive(Default)]
pub(crate) struct Writers {
    next: AtomicU64,
    table: Mutex<Table>,
}

impl Writers {
    /// Opens a writer, or adds a handle to the one that this Producer epoch has on the Stream. A
    /// writer of an Activity attempt above 1 first sends an empty batch, so that the new epoch
    /// withdraws the earlier attempt's Records even when it publishes nothing. On a closed or
    /// expired Stream the open still succeeds, and the first write fails with the reason.
    pub(crate) async fn open(
        &self,
        service: &Arc<StreamService>,
        request: OpenWriterRequest,
    ) -> StreamResult<u64> {
        let stream = request
            .stream
            .ok_or_else(|| StreamError::invalid("The writer has no Stream"))?;
        crate::ids::check_stream_id(&stream.id)?;
        let producer = request
            .producer
            .ok_or_else(|| StreamError::invalid("The writer has no Producer"))?;
        crate::ids::check_producer(&producer)?;
        if producer.kind == StreamProducerKind::Workflow as i32 {
            return Err(StreamError::invalid("Workflow code doesn't open a writer"));
        }
        let epoch = producer.epoch.unwrap_or_default();
        let key = format!(
            "{}\n{}\n{}\n{}",
            request.namespace,
            stream.id,
            producer.id,
            crate::epoch::epoch_key(&epoch)
        );
        if let Some(id) = self.share(&key) {
            return Ok(id);
        }
        let writer = Arc::new(Writer {
            namespace: request.namespace,
            stream,
            producer,
            previous_completed_task: request.previous_completed_task,
            failed_tasks: request.failed_tasks,
            max_batch: match request.max_batch_size {
                n if n > 0 => n as usize,
                _ => DEFAULT_BATCH,
            },
            max_retry: duration(request.max_retry_duration).unwrap_or(DEFAULT_RETRY),
            service: service.clone(),
            state: Mutex::default(),
            writing: tokio::sync::Mutex::new(()),
        });
        let retry = matches!(
            StreamProducerKind::try_from(writer.producer.kind).unwrap_or_default(),
            StreamProducerKind::Activity
                | StreamProducerKind::LocalActivity
                | StreamProducerKind::StandaloneActivity
        ) && epoch.attempt > 1;
        if retry {
            match writer.send(vec![], next_sequence()).await {
                // A Stream with no key has nothing to withdraw yet. A closed or expired one accepts
                // no more Records, and a retry that only reads it must still run.
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.reason,
                        Reason::NotFound | Reason::Closed | Reason::Expired
                    ) => {}
                Err(error) => return Err(error),
            }
        }
        let mut table = self.table.lock();
        if let Some(id) = table.by_producer.get(&key).copied() {
            table.open.get_mut(&id).expect("indexed").handles += 1;
            return Ok(id);
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        table.by_producer.insert(key.clone(), id);
        table.open.insert(
            id,
            Open {
                writer,
                key,
                handles: 1,
            },
        );
        Ok(id)
    }

    /// Adds a handle to the open writer of `key`, if there is one.
    fn share(&self, key: &str) -> Option<u64> {
        let mut table = self.table.lock();
        let id = table.by_producer.get(key).copied()?;
        table.open.get_mut(&id).expect("indexed").handles += 1;
        Some(id)
    }

    fn get(&self, id: u64) -> StreamResult<Arc<Writer>> {
        self.table
            .lock()
            .open
            .get(&id)
            .map(|open| open.writer.clone())
            .ok_or_else(|| StreamError::new(Reason::UnknownWriter, format!("No writer {id}")))
    }

    /// Appends Records at once and returns the first and the last offset. A retry after a Store
    /// failure resends the same batch, so that the Store writes it once.
    pub(crate) async fn write(
        &self,
        id: u64,
        records: Vec<AppendRecord>,
    ) -> StreamResult<(i64, i64)> {
        let writer = self.get(id)?;
        {
            let state = writer.state.lock();
            if let Some(error) = &state.failed {
                return Err(error.clone());
            }
            if state.closed {
                return Err(StreamError::new(
                    Reason::UnknownWriter,
                    format!("Writer {id} is closed"),
                ));
            }
        }
        check_sizes(&writer.producer, &records)?;
        writer.write(records).await
    }

    /// Closes every open writer, whatever its handles. Returns how many had failed.
    pub(crate) async fn close_all(&self) -> usize {
        let ids: Vec<u64> = self.table.lock().open.keys().copied().collect();
        let mut failed = 0;
        for id in ids {
            let released = self.table.lock().release(id, true);
            let Ok(Some(writer)) = released else {
                continue;
            };
            if end(&writer).await.is_err() {
                failed += 1;
            }
        }
        failed
    }

    /// Releases one handle. The last one waits for a write in flight, then drops the writer.
    /// Returns its last offset, or the error it failed with.
    pub(crate) async fn close(&self, id: u64) -> StreamResult<i64> {
        let writer = self.get(id)?;
        if self.table.lock().release(id, false)?.is_none() {
            let state = writer.state.lock();
            return match &state.failed {
                Some(error) => Err(error.clone()),
                None => Ok(state.last_offset),
            };
        }
        end(&writer).await
    }

    /// Releases one handle with no wait, for a handle that lang drops. A write in flight still
    /// completes, and a write that waits behind it is refused once no handle is left.
    pub(crate) fn free(&self, id: u64) -> StreamResult<()> {
        if let Some(writer) = self.table.lock().release(id, false)? {
            writer.state.lock().closed = true;
        }
        Ok(())
    }
}

/// Ends a writer that no handle uses. It waits for a write in flight, then returns its last offset
/// or the error it failed with.
async fn end(writer: &Writer) -> StreamResult<i64> {
    writer.state.lock().closed = true;
    let _writing = writer.writing.lock().await;
    let state = writer.state.lock();
    match &state.failed {
        Some(error) => Err(error.clone()),
        None => Ok(state.last_offset),
    }
}
