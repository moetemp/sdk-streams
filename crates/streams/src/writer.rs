//! Buffered writers for Activities and clients. `publish` only buffers, and a background flusher
//! appends every `batch_interval`, so a Producer pays one Store call per batch, not per Record.

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
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Notify, time::Instant};

const DEFAULT_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_BATCH: usize = 1000;
const DEFAULT_RETRY: Duration = Duration::from_secs(10);
const DEFAULT_BUFFERED: usize = 10_000;
const FIRST_BACKOFF: Duration = Duration::from_millis(50);

#[derive(Default)]
struct State {
    buffer: VecDeque<AppendRecord>,
    /// The last `sequence` the writer sent in its epoch.
    sequence: i64,
    last_offset: i64,
    /// A flush that failed for good. Every later call fails with it, since its Records are lost.
    failed: Option<StreamError>,
    closed: bool,
}

struct Writer {
    namespace: String,
    stream: StreamRef,
    producer: Producer,
    interval: Duration,
    max_batch: usize,
    max_retry: Duration,
    max_buffered: usize,
    service: Arc<StreamService>,
    state: Mutex<State>,
    wake: Notify,
    /// One flush at a time, so the batches of one epoch go out in `sequence` order.
    flushing: tokio::sync::Mutex<()>,
}

fn duration(value: Option<prost_types::Duration>) -> Option<Duration> {
    let value = value?;
    let duration = Duration::new(value.seconds.max(0) as u64, value.nanos.max(0) as u32);
    (!duration.is_zero()).then_some(duration)
}

impl Writer {
    /// Sends the buffer, one batch after another, until it is empty or a batch fails for good.
    async fn flush(&self) -> StreamResult<i64> {
        let _flushing = self.flushing.lock().await;
        loop {
            let (records, sequence) = {
                let mut state = self.state.lock();
                if let Some(error) = &state.failed {
                    return Err(error.clone());
                }
                if state.buffer.is_empty() {
                    return Ok(state.last_offset);
                }
                // The Store refuses a batch over its limit, so a batch ends at the count or the
                // bytes, whichever comes first.
                let mut count = 0;
                let mut bytes = 0;
                for record in state.buffer.iter().take(self.max_batch) {
                    let size = stored_size(&self.producer, record);
                    if count > 0 && bytes + size > MAX_BATCH_BYTES {
                        break;
                    }
                    bytes += size;
                    count += 1;
                }
                let records: Vec<_> = state.buffer.drain(..count).collect();
                state.sequence += 1;
                (records, state.sequence)
            };
            match self.send(records, sequence).await {
                Ok(last) => {
                    let mut state = self.state.lock();
                    state.last_offset = state.last_offset.max(last);
                }
                Err(error) => {
                    self.state.lock().failed = Some(error.clone());
                    return Err(error);
                }
            }
        }
    }

    /// Appends one batch, and sends it again with the same `sequence` after a Store failure, so
    /// the dedupe key keeps it from going in twice.
    async fn send(&self, records: Vec<AppendRecord>, sequence: i64) -> StreamResult<i64> {
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
                Ok(response) => return Ok(response.last_offset),
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

    async fn run(self: Arc<Self>) {
        loop {
            let _ = tokio::time::timeout(self.interval, self.wake.notified()).await;
            {
                let state = self.state.lock();
                // A writer that failed for good writes nothing more, so its task ends too.
                if state.closed || state.failed.is_some() {
                    return;
                }
            }
            // A failure stays in the state, and the next publish or flush gives it to lang.
            let _ = self.flush().await;
        }
    }
}

/// The size of a Record once stored, with a margin for the envelope's own fields.
fn stored_size(producer: &Producer, record: &AppendRecord) -> usize {
    record.encoded_len() + producer.encoded_len() + 32
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
            interval: duration(request.batch_interval).unwrap_or(DEFAULT_INTERVAL),
            max_batch: match request.max_batch_size {
                n if n > 0 => n as usize,
                _ => DEFAULT_BATCH,
            },
            max_retry: duration(request.max_retry_duration).unwrap_or(DEFAULT_RETRY),
            max_buffered: match request.max_buffered_records {
                n if n > 0 => n as usize,
                _ => DEFAULT_BUFFERED,
            },
            service: service.clone(),
            state: Mutex::default(),
            wake: Notify::new(),
            flushing: tokio::sync::Mutex::new(()),
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
        self.open.lock().insert(id, writer.clone());
        tokio::spawn(writer.run());
        Ok(id)
    }

    fn get(&self, id: u64) -> StreamResult<Arc<Writer>> {
        self.open
            .lock()
            .get(&id)
            .cloned()
            .ok_or_else(|| StreamError::new(Reason::UnknownWriter, format!("No writer {id}")))
    }

    /// Buffers Records. It never waits, so lang may call it from any thread.
    pub(crate) fn publish(
        &self,
        id: u64,
        records: Vec<AppendRecord>,
        force_flush: bool,
    ) -> StreamResult<()> {
        let writer = self.get(id)?;
        {
            let mut state = writer.state.lock();
            if let Some(error) = &state.failed {
                return Err(error.clone());
            }
            if state.closed {
                return Err(StreamError::new(
                    Reason::UnknownWriter,
                    format!("Writer {id} is closed"),
                ));
            }
            if let Some(big) = records
                .iter()
                .map(|record| stored_size(&writer.producer, record))
                .find(|size| *size > MAX_RECORD_BYTES)
            {
                return Err(StreamError::new(
                    Reason::TooLarge,
                    format!("A Record has about {big} bytes, more than {MAX_RECORD_BYTES}"),
                ));
            }
            if state.buffer.len() + records.len() > writer.max_buffered {
                writer.service.metrics.buffer_full();
                return Err(StreamError::new(
                    Reason::BufferFull,
                    format!(
                        "The writer has {} Records waiting, and takes at most {}",
                        state.buffer.len(),
                        writer.max_buffered
                    ),
                ));
            }
            state.buffer.extend(records);
            if !force_flush && state.buffer.len() < writer.max_batch {
                return Ok(());
            }
        }
        writer.wake.notify_one();
        Ok(())
    }

    /// Waits until every Record published so far is in the Store.
    pub(crate) async fn flush(&self, id: u64) -> StreamResult<i64> {
        self.get(id)?.flush().await
    }

    /// Closes every open writer. Answers how many failed their last flush.
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

    /// Flushes, then drops the writer.
    pub(crate) async fn close(&self, id: u64) -> StreamResult<i64> {
        let writer = self.get(id)?;
        // A publish checks the closed state under the same lock, so every Record it took is
        // in the buffer that this flush sends.
        writer.state.lock().closed = true;
        let result = writer.flush().await;
        writer.wake.notify_one();
        self.open.lock().remove(&id);
        if let Some(key) = self.keys.lock().remove(&id) {
            self.by_producer.lock().remove(&key);
            let sequence = writer.state.lock().sequence;
            self.closed_sequences.lock().insert(key, sequence);
        }
        result
    }
}
