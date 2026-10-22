//! Writers for Activities and clients. `write` appends its Records at once and answers their
//! offsets, which is what an awaited publish needs. A writer keeps one Producer epoch and one
//! `sequence` order, so a retry of a batch goes in once. Lang buffers on its own, if it wants
//! to, and writes each batch with one call.

use crate::{
    Reason, StreamError, StreamResult,
    proto::{
        AppendRecord, AppendRecordsRequest, OpenWriterRequest, Producer, ProducerKind, StreamRef,
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
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

const DEFAULT_BATCH: usize = 1000;
const DEFAULT_RETRY: Duration = Duration::from_secs(10);
const FIRST_BACKOFF: Duration = Duration::from_millis(50);

#[derive(Default)]
struct State {
    /// The last `sequence` the writer sent in its epoch.
    sequence: i64,
    last_offset: i64,
    /// A write that failed for good. Every later call fails with it, since the batch may be in
    /// the Store under a `sequence` the writer can't reuse.
    failed: Option<StreamError>,
    closed: bool,
}

struct Writer {
    namespace: String,
    stream: StreamRef,
    producer: Producer,
    max_batch: usize,
    max_retry: Duration,
    service: Arc<StreamService>,
    state: Mutex<State>,
    /// One write at a time, so the batches of one epoch go out in `sequence` order.
    writing: tokio::sync::Mutex<()>,
}

fn duration(value: Option<prost_types::Duration>) -> Option<Duration> {
    let value = value?;
    let duration = Duration::new(value.seconds.max(0) as u64, value.nanos.max(0) as u32);
    (!duration.is_zero()).then_some(duration)
}

impl Writer {
    /// How many of `records` go in the next batch. The Store refuses a batch over its limit, so
    /// a batch ends at the count or the bytes, whichever comes first.
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

    /// Appends `records` now and answers the offsets of the first and the last one. Records
    /// past the Store's batch limit go as more batches, in order.
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
            let sequence = {
                let mut state = self.state.lock();
                if let Some(error) = &state.failed {
                    return Err(error.clone());
                }
                state.sequence += 1;
                state.sequence
            };
            let (batch_first, batch_last) = match self.send(records, sequence).await {
                Ok(offsets) => offsets,
                // The batch may be in the Store, so the writer can't go on in this epoch.
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

    /// Appends one batch, and sends it again with the same `sequence` after a Store failure, so
    /// the dedupe key keeps it from going in twice. Answers the first and the last offset.
    async fn send(&self, records: Vec<AppendRecord>, sequence: i64) -> StreamResult<(i64, i64)> {
        let request = AppendRecordsRequest {
            namespace: self.namespace.clone(),
            stream: Some(self.stream.clone()),
            producer: Some(self.producer.clone()),
            sequence,
            records,
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

/// The open writers of one extension.
#[derive(Default)]
pub(crate) struct Writers {
    next: AtomicU64,
    open: Mutex<HashMap<u64, Arc<Writer>>>,
    /// The writer of each (namespace, Stream, Producer id, epoch), so a second open shares its
    /// sequence instead of reusing it.
    by_producer: Mutex<HashMap<String, u64>>,
    /// The last `sequence` of each closed writer, so a reopen of the same epoch goes on from
    /// it instead of sending a used `sequence` again.
    closed_sequences: Mutex<HashMap<String, i64>>,
    keys: Mutex<HashMap<u64, String>>,
}

impl Writers {
    /// Opens a writer, or finds the one this Producer epoch has on the Stream. A writer of an
    /// Activity attempt above 1 first sends an empty batch, so the new epoch withdraws the
    /// earlier attempt's Records also when it publishes nothing.
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
        if producer.kind == ProducerKind::Workflow as i32 {
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
        if let Some(id) = self.by_producer.lock().get(&key) {
            return Ok(*id);
        }
        let writer = Arc::new(Writer {
            namespace: request.namespace,
            stream,
            producer,
            max_batch: match request.max_batch_size {
                n if n > 0 => n as usize,
                _ => DEFAULT_BATCH,
            },
            max_retry: duration(request.max_retry_duration).unwrap_or(DEFAULT_RETRY),
            service: service.clone(),
            state: Mutex::default(),
            writing: tokio::sync::Mutex::new(()),
        });
        let reopened = self.closed_sequences.lock().get(&key).copied();
        if let Some(sequence) = reopened {
            writer.state.lock().sequence = sequence;
        }
        let retry = reopened.is_none()
            && matches!(
                ProducerKind::try_from(writer.producer.kind).unwrap_or_default(),
                ProducerKind::Activity
                    | ProducerKind::LocalActivity
                    | ProducerKind::StandaloneActivity
            )
            && epoch.attempt > 1;
        if retry {
            writer.state.lock().sequence = 1;
            match writer.send(vec![], 1).await {
                // A Stream with no key has nothing to withdraw yet.
                Ok(_) => {}
                Err(error) if error.reason == Reason::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        let mut by_producer = self.by_producer.lock();
        if let Some(id) = by_producer.get(&key) {
            return Ok(*id);
        }
        by_producer.insert(key.clone(), id);
        self.keys.lock().insert(id, key);
        self.open.lock().insert(id, writer);
        Ok(id)
    }

    fn get(&self, id: u64) -> StreamResult<Arc<Writer>> {
        self.open
            .lock()
            .get(&id)
            .cloned()
            .ok_or_else(|| StreamError::new(Reason::UnknownWriter, format!("No writer {id}")))
    }

    /// Appends Records at once and answers the first and the last offset. A retry after a Store failure resends the same batch, so it goes in once.
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

    /// Closes every open writer. Answers how many had failed.
    pub(crate) async fn close_all(&self) -> usize {
        let ids: Vec<u64> = self.open.lock().keys().copied().collect();
        let mut failed = 0;
        for id in ids {
            if self.close(id).await.is_err() {
                failed += 1;
            }
        }
        failed
    }

    /// Waits for a write in flight, then drops the writer. Answers its last offset, or the
    /// error it failed with.
    pub(crate) async fn close(&self, id: u64) -> StreamResult<i64> {
        let writer = self.get(id)?;
        writer.state.lock().closed = true;
        let result = {
            let _writing = writer.writing.lock().await;
            let state = writer.state.lock();
            match &state.failed {
                Some(error) => Err(error.clone()),
                None => Ok(state.last_offset),
            }
        };
        self.open.lock().remove(&id);
        if let Some(key) = self.keys.lock().remove(&id) {
            self.by_producer.lock().remove(&key);
            let sequence = writer.state.lock().sequence;
            self.closed_sequences.lock().insert(key, sequence);
        }
        result
    }
}
