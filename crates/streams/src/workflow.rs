//! The Workflow path. Lang hands the crate each Activation's Stream calls, the crate writes the
//! creates and appends in `before_complete`, and it sends the closes in `after_complete` once
//! the server accepted the completion. Before a Local Activity of the run starts, lang asks the
//! crate to write the buffer early, so the run's Records come before the Local Activity's.

use crate::{
    Reason, StreamError, StreamResult,
    epoch::{task_key, timestamp},
    ids::{check_stream_id, workflow_producer_id},
    proto::{
        AppendRecord, BufferWorkflowCallsRequest, CloseStreamRequest, FlushRunBufferRequest,
        Producer, ProducerEpoch, ProducerKind, WorkflowCall, WorkflowTaskEpoch,
        workflow_call::Call,
    },
    record::{close_record, data_record, supersession},
    service::StreamService,
    store::{
        Batch, Capabilities, CreateOutcome, MAX_BATCH_BYTES, MAX_RECORD_BYTES, Owner, Store,
        Withdrawal,
    },
};
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use temporalio_sdk_core::{
    WorkflowTaskCompletionInfo, WorkflowTaskFailedInfo, WorkflowTaskInterceptError,
};
use tokio::time::Instant;
use tokio_util::task::TaskTracker;
use tracing::warn;

/// How long before the task's deadline the flush gives up at most, so it fails with its own
/// error before Core's deadline does.
const DEADLINE_MARGIN: Duration = Duration::from_millis(200);
/// The first backoff of a Store call that failed. It doubles on each try.
const FIRST_BACKOFF: Duration = Duration::from_millis(50);
/// How many times a close goes out before the crate leaves the Stream to its safety TTL.
const CLOSE_ATTEMPTS: u32 = 6;
/// How long a Local Activity flush may take when lang gives no timeout.
const LOCAL_FLUSH_TIMEOUT: Duration = Duration::from_secs(10);

/// What one run handed over since its last `before_complete`, and the closes that wait for the
/// server's answer.
#[derive(Default)]
struct RunBuffer {
    withdraw_run: String,
    chain_continues: bool,
    calls: Vec<WorkflowCall>,
    closes: Vec<(CloseStreamRequest, bool)>,
    /// What a Local Activity flush wrote in the task that has not completed yet.
    early: Option<EarlyWrite>,
    /// The task key of the run's last write, and the next `sequence` of each Stream in that
    /// epoch. A failed task keeps it, so a late write in that epoch never reuses a number.
    numbered: Option<(String, HashMap<String, i64>)>,
    /// The key of the newest task whose `before_complete` ran. A Local Activity of that task or
    /// an older one writes nothing more, since its task is done.
    completed: Option<String>,
    /// A Local Activity flush that failed. The run's Workflow Task fails with it, so the
    /// retry withdraws what the flush wrote.
    failed: Option<StreamError>,
    /// Tells this buffer apart from the one a failed task or an eviction left behind, so a
    /// write that ends after either of them leaves nothing in the new buffer.
    id: u64,
    /// One write of the run at a time, so a Local Activity that starts while another one's
    /// flush runs waits for it, and the batch numbers of one task stay in order.
    writing: Arc<tokio::sync::Mutex<()>>,
}

/// The calls that Local Activity flushes wrote in one task, with no withdrawal facts yet.
struct EarlyWrite {
    /// The task key of the write's epoch.
    task: String,
    /// The creates and appends it wrote. The task's own completion sends a batch with the
    /// facts to each of these Streams. Any other task writes them again, since a task that
    /// never completes has its Records withdrawn.
    calls: Vec<WorkflowCall>,
}

/// The buffers of every run on the Workers of one extension.
#[derive(Default)]
pub(crate) struct WorkflowRuns {
    runs: Mutex<HashMap<String, RunBuffer>>,
    next_id: std::sync::atomic::AtomicU64,
    closes: TaskTracker,
}

/// The settings of the Workflow flush.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FlushOptions {
    pub(crate) durable_writes: u32,
    pub(crate) durable_timeout: Duration,
}

/// The Workflow Task a write runs for.
struct WriteTask<'a> {
    namespace: &'a str,
    run_id: &'a str,
    producer: Producer,
    /// The task key of the Producer's epoch.
    key: String,
    /// The task's previous completed task and failed tasks. `None` for a Local Activity flush,
    /// since Core gives them only with the completion.
    facts: Option<(Option<WorkflowTaskEpoch>, &'a [i64])>,
    deadline: Instant,
}

/// What one write takes from a run's buffer.
struct Taken {
    /// The id of the buffer it came from.
    id: u64,
    calls: Vec<WorkflowCall>,
    withdraw_run: String,
    chain_continues: bool,
    /// The next `sequence` of each Stream, when an earlier write of this task used some.
    next: HashMap<String, i64>,
    /// The Streams that an earlier write of this task left without withdrawal facts.
    unsettled: Vec<String>,
    /// The calls of this write that a later task must write again if this task never
    /// completes. Empty for a write with facts.
    early: Vec<WorkflowCall>,
}

impl WorkflowRuns {
    /// Adds an Activation's calls to the run's buffer.
    pub(crate) fn buffer(&self, request: BufferWorkflowCallsRequest) -> StreamResult<()> {
        if request.run_id.is_empty() {
            return Err(StreamError::invalid("The calls have no run id"));
        }
        for call in &request.calls {
            match &call.call {
                Some(Call::Create(create)) => check_stream_id(&create.stream_id)?,
                Some(Call::Append(append)) => {
                    check_stream_id(append.stream.as_ref().map_or("", |s| s.id.as_str()))?
                }
                Some(Call::Close(close)) => {
                    check_stream_id(close.stream.as_ref().map_or("", |s| s.id.as_str()))?
                }
                None => return Err(StreamError::invalid("A Workflow call is empty")),
            }
        }
        let mut runs = self.runs.lock();
        let run = runs.entry(request.run_id).or_insert_with(|| RunBuffer {
            id: self.fresh_id(),
            ..Default::default()
        });
        if !request.withdraw_run.is_empty() {
            run.withdraw_run = request.withdraw_run;
        }
        run.chain_continues = request.chain_continues;
        run.calls.extend(request.calls);
        Ok(())
    }

    fn fresh_id(&self) -> u64 {
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1
    }

    /// The lock that orders the run's writes, or `None` for a run with no buffer.
    fn writing(&self, run_id: &str) -> Option<Arc<tokio::sync::Mutex<()>>> {
        self.runs.lock().get(run_id).map(|run| run.writing.clone())
    }

    /// Takes what a write of task `key` sends, or `None` when it sends nothing. A Local
    /// Activity flush of a task that is done, or older than the task of an earlier flush,
    /// writes nothing.
    fn take(&self, run_id: &str, key: &str, facts: bool) -> StreamResult<Option<Taken>> {
        let mut runs = self.runs.lock();
        let Some(run) = runs.get_mut(run_id) else {
            return Ok(None);
        };
        if let Some(error) = &run.failed {
            return Err(error.clone());
        }
        if facts {
            let done = run.completed.take().unwrap_or_default();
            run.completed = Some(done.max(key.to_string()));
        } else if run.completed.as_deref().is_some_and(|done| done >= key)
            || run
                .early
                .as_ref()
                .is_some_and(|early| early.task.as_str() > key)
        {
            return Ok(None);
        }
        let mut calls = std::mem::take(&mut run.calls);
        let next = match &run.numbered {
            Some((task, next)) if task == key => next.clone(),
            _ => HashMap::new(),
        };
        let (mut unsettled, mut written) = (vec![], vec![]);
        match run.early.take() {
            Some(early) if early.task == key => {
                unsettled = streams_with_records(&early.calls);
                written = early.calls;
            }
            // That task never completed, so its Records go again in this task's epoch. Core path:
            // a Local Activity of a failed attempt flushes late, after the retry buffered its
            // calls, and takes them. With no failure or eviction, no later task buffers these
            // calls again, so this writes nothing twice.
            Some(early) => drop(calls.splice(0..0, early.calls)),
            None => {}
        }
        if facts {
            if calls.is_empty() && unsettled.is_empty() {
                return Ok(None);
            }
            return Ok(Some(Taken {
                id: run.id,
                calls,
                withdraw_run: run.withdraw_run.clone(),
                chain_continues: run.chain_continues,
                next,
                unsettled,
                early: vec![],
            }));
        }
        if calls.is_empty() {
            if !written.is_empty() {
                run.early = Some(EarlyWrite {
                    task: key.to_string(),
                    calls: written,
                });
            }
            return Ok(None);
        }
        written.extend(
            calls
                .iter()
                .filter(|call| !matches!(call.call, Some(Call::Close(_))))
                .cloned(),
        );
        Ok(Some(Taken {
            id: run.id,
            calls,
            withdraw_run: run.withdraw_run.clone(),
            chain_continues: run.chain_continues,
            next,
            unsettled: vec![],
            early: written,
        }))
    }

    /// Keeps what a write left for later: the closes, and for a Local Activity flush the
    /// calls it wrote and the next batch numbers.
    fn keep(
        &self,
        run_id: &str,
        id: u64,
        key: &str,
        early: Vec<WorkflowCall>,
        next: HashMap<String, i64>,
        closes: Vec<(CloseStreamRequest, bool)>,
    ) {
        let mut runs = self.runs.lock();
        // A failed task or an eviction while the write ran drops what it left. The retry runs
        // the Activations again and buffers the same calls.
        let Some(run) = runs.get_mut(run_id).filter(|run| run.id == id) else {
            return;
        };
        run.closes.extend(closes);
        run.numbered = Some((key.to_string(), next));
        if !early.is_empty() {
            run.early = Some(EarlyWrite {
                task: key.to_string(),
                calls: early,
            });
        }
    }

    /// Fails the run's next completion, after a Local Activity flush failed.
    fn fail(&self, run_id: &str, id: u64, error: StreamError) {
        if let Some(run) = self.runs.lock().get_mut(run_id).filter(|run| run.id == id) {
            run.failed = Some(error);
        }
    }

    fn take_closes(&self, run_id: &str) -> Vec<(CloseStreamRequest, bool)> {
        let mut runs = self.runs.lock();
        runs.get_mut(run_id)
            .map(|run| std::mem::take(&mut run.closes))
            .unwrap_or_default()
    }

    /// Drops what a failed task buffered and wrote early. Its retry runs the Activations
    /// again, and its writes withdraw what this task wrote.
    pub(crate) fn task_failed(&self, task: &WorkflowTaskFailedInfo) {
        let id = self.fresh_id();
        if let Some(run) = self.runs.lock().get_mut(&task.run_id) {
            run.id = id;
            run.calls.clear();
            run.closes.clear();
            run.early = None;
            run.failed = None;
        }
    }

    pub(crate) fn evicted(&self, run_id: &str) {
        self.runs.lock().remove(run_id);
    }

    /// Waits up to `timeout` for the closes that are still going out. Answers how many are left.
    pub(crate) async fn drain(&self, timeout: Duration) -> usize {
        /// Opens the tracker again also when the caller drops the drain, for example when
        /// lang cancels it at shutdown.
        struct Reopen<'a>(&'a TaskTracker);
        impl Drop for Reopen<'_> {
            fn drop(&mut self) {
                self.0.reopen();
            }
        }
        self.closes.close();
        let _reopen = Reopen(&self.closes);
        let _ = tokio::time::timeout(timeout, self.closes.wait()).await;
        self.closes.len()
    }

    /// Whether the run has something to write or to fail with.
    pub(crate) fn has_work(&self, run_id: &str) -> bool {
        self.runs
            .lock()
            .get(run_id)
            .is_some_and(|run| !run.calls.is_empty() || run.early.is_some() || run.failed.is_some())
    }
}

/// The Streams that have appends among `calls`, in order.
fn streams_with_records(calls: &[WorkflowCall]) -> Vec<String> {
    let mut seen = HashSet::new();
    calls
        .iter()
        .filter_map(|call| match &call.call {
            Some(Call::Append(append)) if !append.records.is_empty() => {
                append.stream.as_ref().map(|s| s.id.clone())
            }
            _ => None,
        })
        .filter(|id| seen.insert(id.clone()))
        .collect()
}

/// The run's Producer in a task.
fn producer(run_id: &str, task: &WorkflowTaskEpoch) -> Producer {
    Producer {
        kind: ProducerKind::Workflow as i32,
        id: workflow_producer_id(run_id),
        epoch: Some(ProducerEpoch {
            task_started_event_id: task.started_event_id,
            task_attempt: task.attempt,
            task_started_time: task.started_time,
            ..Default::default()
        }),
        run_id: run_id.to_string(),
    }
}

fn this_task(task: &WorkflowTaskCompletionInfo) -> WorkflowTaskEpoch {
    WorkflowTaskEpoch {
        started_event_id: task.started_event_id,
        attempt: task.attempt as i32,
        started_time: task.started_time.map(timestamp),
    }
}

fn previous_completed(task: &WorkflowTaskCompletionInfo) -> Option<WorkflowTaskEpoch> {
    task.previous_completed.as_ref().map(|c| WorkflowTaskEpoch {
        started_event_id: c.started_event_id,
        attempt: c.attempt as i32,
        started_time: c.started_time.map(timestamp),
    })
}

/// The calls of one Stream in one write, in the order Workflow code made them.
#[derive(Default)]
struct StreamCalls {
    /// The Retention and the auto close of each create.
    creates: Vec<(Option<Duration>, Option<Duration>)>,
    records: Vec<AppendRecord>,
    /// Send a batch also with no Records, so the task's withdrawal facts reach the Stream.
    settle: bool,
}

fn duration(value: Option<prost_types::Duration>) -> Option<Duration> {
    let value = value?;
    let duration = Duration::new(value.seconds.max(0) as u64, value.nanos.max(0) as u32);
    (!duration.is_zero()).then_some(duration)
}

/// Splits encoded Records into batches that fit the Store's batch limit, and drops the ones
/// over the Record limit. Answers the batches and how many it dropped.
pub(crate) fn split(records: Vec<Vec<u8>>) -> (Vec<Vec<Vec<u8>>>, usize) {
    let mut batches: Vec<Vec<Vec<u8>>> = vec![];
    let (mut size, mut dropped) = (0, 0);
    for record in records {
        if record.len() > MAX_RECORD_BYTES {
            dropped += 1;
            continue;
        }
        if batches.is_empty() || size + record.len() > MAX_BATCH_BYTES {
            batches.push(vec![]);
            size = 0;
        }
        size += record.len();
        batches.last_mut().expect("a batch").push(record);
    }
    (batches, dropped)
}

/// Retries a Store call on a Store failure until `deadline`. The dedupe key makes a resend safe.
async fn retried<T, F, Fut>(deadline: Instant, mut call: F) -> StreamResult<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = StreamResult<T>>,
{
    let mut backoff = FIRST_BACKOFF;
    loop {
        match call().await {
            Err(error)
                if error.reason == Reason::Storage && Instant::now() + backoff < deadline =>
            {
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
            result => return result,
        }
    }
}

/// The Topic of a batch's first Record, for an error message.
fn batch_topic(records: &[Vec<u8>]) -> String {
    use prost::Message;
    records
        .first()
        .and_then(|bytes| crate::proto::StreamRecord::decode(bytes.as_slice()).ok())
        .map(|record| record.topic)
        .unwrap_or_default()
}

/// Whether a Workflow flush drops its Records for this refusal instead of failing the task. A
/// raise at the flush would differ between a live run and its replay.
fn dropped_for(reason: Reason) -> bool {
    matches!(reason, Reason::Closed | Reason::Expired | Reason::NotFound)
}

impl StreamService {
    /// Writes the run's buffer at the end of a Workflow Task, with the task's withdrawal facts.
    pub(crate) async fn flush(
        &self,
        runs: &WorkflowRuns,
        task: &WorkflowTaskCompletionInfo,
        options: FlushOptions,
    ) -> StreamResult<()> {
        // A Local Activity heartbeat completion comes late in the task, with little time left,
        // so the margin is at most a quarter of what is left.
        let left = Instant::from_std(task.deadline).saturating_duration_since(Instant::now());
        let deadline = Instant::now() + left - DEADLINE_MARGIN.min(left / 4);
        let Some(writing) = runs.writing(&task.run_id) else {
            return Ok(());
        };
        // A Local Activity flush may hold the lock, so the wait for it counts against the
        // task's deadline too.
        let Ok(_writing) = tokio::time::timeout_at(deadline, writing.lock()).await else {
            self.metrics.flush_failed();
            return Err(StreamError::storage(
                "A Local Activity flush of the run did not end before the Workflow Task deadline",
            ));
        };
        // Without the started time, a dropped speculative task and the task that reuses its
        // started event id would get one epoch, and the second batch would count as a resend.
        if task.started_time.is_none() {
            self.metrics.flush_failed();
            return Err(StreamError::invalid(
                "The Workflow Task has no started time, which the Stream epoch needs",
            ));
        }
        let epoch = this_task(task);
        let key = task_key(&epoch);
        let Some(taken) = runs.take(&task.run_id, &key, true)? else {
            return Ok(());
        };
        let completed = previous_completed(task);
        let write = WriteTask {
            namespace: &task.namespace,
            run_id: &task.run_id,
            producer: producer(&task.run_id, &epoch),
            key,
            facts: Some((completed, &task.failed_tasks)),
            deadline,
        };
        self.write(runs, write, taken, options).await.map(|_| ())
    }

    /// Writes the run's buffer before a Local Activity of the run starts, in the epoch of the
    /// Workflow Task that dispatched it. Core gives the withdrawal facts only with the
    /// completion, so the write withdraws nothing, and the task's completion sends the facts.
    /// Answers how many Records it wrote.
    pub(crate) async fn flush_for_local_activity(
        &self,
        runs: &WorkflowRuns,
        request: FlushRunBufferRequest,
        options: FlushOptions,
    ) -> StreamResult<usize> {
        if request.run_id.is_empty() {
            return Err(StreamError::invalid("The flush has no run id"));
        }
        let epoch = request
            .workflow_task
            .ok_or_else(|| StreamError::invalid("The flush has no dispatching Workflow Task"))?;
        if epoch.started_time.is_none() {
            return Err(StreamError::invalid(
                "The Workflow Task has no started time, which the Stream epoch needs",
            ));
        }
        let timeout = duration(request.timeout).unwrap_or(LOCAL_FLUSH_TIMEOUT);
        let deadline = Instant::now() + timeout;
        let Some(writing) = runs.writing(&request.run_id) else {
            return Ok(0);
        };
        let Ok(_writing) = tokio::time::timeout_at(deadline, writing.lock()).await else {
            return Err(StreamError::storage(
                "Another flush of the run did not end before this flush's timeout",
            ));
        };
        let key = task_key(&epoch);
        let Some(taken) = runs.take(&request.run_id, &key, false)? else {
            return Ok(0);
        };
        let id = taken.id;
        let write = WriteTask {
            namespace: &request.namespace,
            run_id: &request.run_id,
            producer: producer(&request.run_id, &epoch),
            key,
            facts: None,
            deadline,
        };
        let result = self.write(runs, write, taken, options).await;
        if let Err(error) = &result {
            runs.fail(&request.run_id, id, error.clone());
        }
        result
    }

    /// Writes what one write took. One Lua call per batch, one Stream after another. Answers
    /// how many Records it wrote.
    async fn write(
        &self,
        runs: &WorkflowRuns,
        task: WriteTask<'_>,
        taken: Taken,
        options: FlushOptions,
    ) -> StreamResult<usize> {
        let started = std::time::Instant::now();
        let Taken {
            id: buffer,
            calls,
            withdraw_run,
            chain_continues,
            mut next,
            unsettled,
            early,
        } = taken;
        let mut order: Vec<String> = vec![];
        let mut streams: HashMap<String, StreamCalls> = HashMap::new();
        let mut closes = vec![];
        for call in calls {
            let when_run_closes = call.when_run_closes;
            match call.call {
                Some(Call::Create(create)) => {
                    if !streams.contains_key(&create.stream_id) {
                        order.push(create.stream_id.clone());
                    }
                    let entry = streams.entry(create.stream_id).or_default();
                    entry
                        .creates
                        .push((duration(create.retention), duration(create.auto_close)));
                }
                Some(Call::Append(append)) => {
                    let id = append.stream.map(|s| s.id).unwrap_or_default();
                    if !streams.contains_key(&id) {
                        order.push(id.clone());
                    }
                    streams
                        .entry(id)
                        .or_default()
                        .records
                        .extend(append.records);
                }
                // A failure that the server retries, or a cron run, goes on with the Run Chain,
                // so the auto close doesn't go out.
                Some(Call::Close(_)) if when_run_closes && chain_continues => {}
                Some(Call::Close(close)) => closes.push((close, when_run_closes)),
                None => {}
            }
        }
        for id in unsettled {
            if !streams.contains_key(&id) {
                order.push(id.clone());
            }
            streams.entry(id).or_default().settle = true;
        }
        let durable = (options.durable_writes > 0)
            .then(|| self.store.durable_store())
            .flatten();
        let store: &dyn Store = durable.as_deref().unwrap_or(self.store.as_ref());
        // All checks come before the first write, so a Store that lacks a capability fails the
        // task with nothing written.
        let mut needed = Capabilities::default();
        for calls in streams.values() {
            if !calls.creates.is_empty() {
                needed = needed.union(Capabilities::SAFETY_TTL);
            }
            if calls
                .creates
                .iter()
                .any(|(_, auto_close)| auto_close.is_some())
            {
                needed = needed.union(Capabilities::AUTO_CLOSE);
            }
            if !calls.records.is_empty() || calls.settle {
                needed = needed.union(Capabilities::PRODUCER_FENCE.union(Capabilities::WITHDRAWAL));
            }
        }
        if !withdraw_run.is_empty() && !streams.is_empty() {
            needed = needed.union(Capabilities::RUN_FENCE);
        }
        if !closes.is_empty() {
            needed = needed.union(Capabilities::CLOSE);
        }
        if options.durable_writes > 0 {
            needed = needed.union(Capabilities::REPLICA_WAIT);
        }
        if let Err(error) = store.capabilities().require(needed) {
            self.metrics.flush_failed();
            return Err(error);
        }
        let deadline = task.deadline;
        let flush = async {
            let mut written = 0;
            for id in &order {
                let calls = streams.remove(id).unwrap_or_default();
                let first = next.get(id).copied().unwrap_or(1);
                let (count, after) = self
                    .flush_stream(store, &task, id, calls, &withdraw_run, first)
                    .await?;
                written += count;
                next.insert(id.clone(), after);
            }
            if options.durable_writes > 0 {
                for id in &order {
                    Self::wait_durable(store, task.namespace, id, options).await?;
                }
            }
            Ok::<_, StreamError>(written)
        };
        let result = match tokio::time::timeout_at(deadline, flush).await {
            Ok(result) => result,
            Err(_) => Err(StreamError::storage(
                "The flush did not end before its deadline",
            )),
        };
        self.metrics.hook_latency(started.elapsed());
        match &result {
            Ok(_) => runs.keep(task.run_id, buffer, &task.key, early, next, closes),
            Err(error) => {
                self.metrics.flush_failed();
                warn!(run_id = %task.run_id, %error, "A Workflow flush failed the Workflow Task");
            }
        }
        result
    }

    /// Writes one Stream's creates and batches, numbered from `first`. Answers how many
    /// Records went in and the next `sequence`.
    async fn flush_stream(
        &self,
        store: &dyn Store,
        task: &WriteTask<'_>,
        id: &str,
        calls: StreamCalls,
        withdraw_run: &str,
        first: i64,
    ) -> StreamResult<(usize, i64)> {
        let namespace = task.namespace;
        let producer = &task.producer;
        let (completed, failed) = match &task.facts {
            Some((completed, failed)) => (completed.as_ref(), *failed),
            None => (None, &[][..]),
        };
        // A deferred batch can still flag a retry run's withdrawal. Its item has no epoch, so a
        // Reader withdraws only the previous run, not every earlier task of this run.
        let bytes = if task.facts.is_some() {
            supersession(producer, completed, failed, withdraw_run)
        } else {
            let bare = Producer {
                epoch: None,
                ..producer.clone()
            };
            supersession(&bare, None, &[], withdraw_run)
        };
        let withdrawal = Withdrawal {
            previous_completed: completed,
            failed_tasks: failed,
            withdraw_run,
            supersession: &bytes,
            defer: task.facts.is_none(),
        };
        for (retention, auto_close) in calls.creates {
            let created = retried(task.deadline, || {
                store.create(
                    namespace,
                    id,
                    retention,
                    auto_close,
                    Owner::Workflow,
                    &withdrawal,
                )
            })
            .await?;
            if created.flagged {
                self.metrics.superseded();
            }
            match created.outcome {
                CreateOutcome::Conflict => warn!(
                    stream = id,
                    retention = ?created.retention,
                    auto_close = ?created.auto_close,
                    "The Stream exists with another Retention or auto close, which stays"
                ),
                // A replay after a cache miss sends the run's creates again, also for the
                // Streams the run closed since, so this is no warning.
                CreateOutcome::Closed | CreateOutcome::Expired => tracing::debug!(
                    stream = id,
                    outcome = ?created.outcome,
                    "A Workflow created a Stream that is closed or expired"
                ),
                CreateOutcome::Created | CreateOutcome::Exists => {}
            }
        }
        if calls.records.is_empty() && !calls.settle {
            return Ok((0, first));
        }
        let records = calls
            .records
            .into_iter()
            .map(|record| data_record(producer, record))
            .collect();
        let (mut batches, oversize) = split(records);
        if oversize > 0 {
            self.metrics.records_dropped(oversize);
            warn!(
                stream = id,
                count = oversize,
                "Dropped Records over the Store's Record limit after the payload codec"
            );
        }
        if batches.is_empty() {
            // An empty batch takes the task's facts to a Stream an earlier write left without.
            batches.push(vec![]);
        }
        let mut written = 0;
        for (n, records) in batches.iter().enumerate() {
            let batch = Batch {
                producer,
                sequence: first + n as i64,
                records,
                withdrawal: withdrawal.clone(),
            };
            let appended = retried(task.deadline, || store.append(namespace, id, &batch)).await;
            match self.count(appended) {
                Ok(_) => written += records.len(),
                Err(error) if dropped_for(error.reason) => {
                    let count = batches[n..].iter().map(Vec::len).sum();
                    self.metrics.records_dropped(count);
                    warn!(
                        stream = id,
                        count,
                        %error,
                        "Dropped a Workflow's Records for a Stream that is closed, expired or gone"
                    );
                    break;
                }
                // Lang can't add the Stream to the task failure, since the flush runs in Core.
                Err(error) => {
                    let topic = batch_topic(records);
                    return Err(StreamError::new(
                        error.reason,
                        format!("{} (Stream {id}, Topic {topic:?})", error.message),
                    ));
                }
            }
        }
        Ok((written, first + batches.len() as i64))
    }

    async fn wait_durable(
        store: &dyn Store,
        namespace: &str,
        id: &str,
        options: FlushOptions,
    ) -> StreamResult<()> {
        let replicas = store
            .wait_replicas(
                namespace,
                id,
                options.durable_writes,
                options.durable_timeout,
            )
            .await?;
        if replicas < options.durable_writes as i64 {
            return Err(StreamError::storage(format!(
                "Only {replicas} of {} replicas have the flush",
                options.durable_writes
            )));
        }
        Ok(())
    }

    /// Sends the closes of an accepted task. A close that closes the run goes out only when
    /// this completion closes the run.
    async fn send_closes(
        self: Arc<Self>,
        task: WorkflowTaskCompletionInfo,
        closes: Vec<(CloseStreamRequest, bool)>,
    ) {
        let producer = producer(&task.run_id, &this_task(&task));
        let completed = previous_completed(&task);
        let bytes = supersession(&producer, completed.as_ref(), &task.failed_tasks, "");
        let withdrawal = Withdrawal {
            previous_completed: completed.as_ref(),
            failed_tasks: &task.failed_tasks,
            withdraw_run: "",
            supersession: &bytes,
            defer: false,
        };
        for (close, when_run_closes) in closes {
            if when_run_closes && task.closes_run.is_none() {
                continue;
            }
            let id = close
                .stream
                .as_ref()
                .map(|s| s.id.clone())
                .unwrap_or_default();
            let record = close_record(&producer, close.result, close.failure);
            let mut backoff = FIRST_BACKOFF * 4;
            for attempt in 1..=CLOSE_ATTEMPTS {
                let closed = self
                    .store
                    .close(&task.namespace, &id, &producer, &record, &withdrawal)
                    .await;
                match self.count(closed) {
                    Ok(_) => break,
                    Err(error) if error.reason == Reason::Storage && attempt < CLOSE_ATTEMPTS => {
                        self.metrics.close_retried();
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                    }
                    // An auto close of a Stream that has no key does nothing.
                    Err(error) if error.reason == Reason::NotFound => break,
                    Err(error) => {
                        warn!(
                            stream = %id,
                            %error,
                            "A Workflow's close did not go out, so the safety TTL removes the \
                             Stream"
                        );
                        break;
                    }
                }
            }
        }
    }
}

/// The Workflow Task interceptor that Core calls for every run of the extension's Workers.
pub(crate) struct WorkflowHooks {
    pub(crate) shared: Arc<crate::extension::Shared>,
}

#[async_trait::async_trait]
impl temporalio_sdk_core::WorkflowTaskInterceptor for WorkflowHooks {
    async fn before_complete(
        &self,
        task: &WorkflowTaskCompletionInfo,
    ) -> Result<(), WorkflowTaskInterceptError> {
        let shared = &self.shared;
        if !shared.runs.has_work(&task.run_id) {
            return Ok(());
        }
        let service = shared
            .service()
            .await
            .map_err(|error| WorkflowTaskInterceptError::new(error.to_string()))?;
        service
            .flush(&shared.runs, task, shared.flush_options())
            .await
            .map_err(|error| WorkflowTaskInterceptError::new(error.to_string()))
    }

    async fn after_complete(&self, task: &WorkflowTaskCompletionInfo, accepted: bool) {
        let shared = &self.shared;
        let closes = shared.runs.take_closes(&task.run_id);
        if !accepted || closes.is_empty() {
            return;
        }
        // The hook holds the run's next Workflow Task, so the closes and their retries go out
        // in the background, in order.
        let Ok(service) = shared.service().await.cloned() else {
            warn!(run_id = %task.run_id, "No Store for the closes of an accepted Workflow Task");
            return;
        };
        shared
            .runs
            .closes
            .spawn(service.send_closes(task.clone(), closes));
    }

    fn on_task_failed(&self, task: &WorkflowTaskFailedInfo) {
        self.shared.runs.task_failed(task);
    }

    fn on_evicted(&self, run_id: &str) {
        self.shared.runs.evicted(run_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_flush_splits_at_the_batch_limit_and_drops_oversize_records() {
        let record = |bytes: usize| vec![0u8; bytes];
        let big = MAX_RECORD_BYTES - 10;
        let (batches, dropped) = split(vec![
            record(10),
            record(MAX_RECORD_BYTES + 1),
            record(big),
            record(big),
            record(big),
        ]);
        assert_eq!(dropped, 1);
        let sizes: Vec<_> = batches.iter().map(Vec::len).collect();
        assert_eq!(sizes, [3, 1]);
        assert!(
            batches
                .iter()
                .all(|b| b.iter().map(Vec::len).sum::<usize>() <= MAX_BATCH_BYTES)
        );
        assert!(split(vec![]).0.is_empty());
    }
}
