//! The Workflow path. Lang hands the crate each Activation's Stream calls, the crate writes the
//! creates and appends in `before_complete`, and it sends the closes in `after_complete` once
//! the server accepted the completion.

use crate::{
    Reason, StreamError, StreamResult,
    epoch::timestamp,
    ids::{check_stream_id, workflow_producer_id},
    proto::{
        AppendRecord, BufferWorkflowCallsRequest, CloseStreamRequest, Producer, ProducerEpoch,
        ProducerKind, WorkflowCall, WorkflowTaskEpoch, workflow_call::Call,
    },
    record::{close_record, data_record, supersession},
    redis::{
        Batch, CreateOutcome, MAX_BATCH_BYTES, MAX_RECORD_BYTES, Owner, RedisStore, Withdrawal,
    },
    service::StreamService,
};
use parking_lot::Mutex;
use std::{collections::HashMap, sync::Arc, time::Duration};
use temporalio_sdk_core::{
    WorkflowTaskCompletionInfo, WorkflowTaskFailedInfo, WorkflowTaskInterceptError,
};
use tokio::time::Instant;
use tokio_util::task::TaskTracker;
use tracing::warn;

/// How long before the task's deadline the flush gives up, so Core still fails the task in
/// time.
const DEADLINE_MARGIN: Duration = Duration::from_millis(200);
/// The first backoff of a Store call that failed. It doubles on each try.
const FIRST_BACKOFF: Duration = Duration::from_millis(50);
/// How many times a close goes out before the crate leaves the Stream to its safety TTL.
const CLOSE_ATTEMPTS: u32 = 6;

/// What one run handed over since its last `before_complete`, and the closes that wait for the
/// server's answer.
#[derive(Default)]
struct RunBuffer {
    withdraw_run: String,
    chain_continues: bool,
    calls: Vec<WorkflowCall>,
    closes: Vec<(CloseStreamRequest, bool)>,
}

/// The buffers of every run on the Workers of one extension.
#[derive(Default)]
pub(crate) struct WorkflowRuns {
    runs: Mutex<HashMap<String, RunBuffer>>,
    closes: TaskTracker,
}

/// The settings of the Workflow flush.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FlushOptions {
    pub(crate) durable_writes: u32,
    pub(crate) durable_timeout: Duration,
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
        let run = runs.entry(request.run_id).or_default();
        if !request.withdraw_run.is_empty() {
            run.withdraw_run = request.withdraw_run;
        }
        run.chain_continues = request.chain_continues;
        run.calls.extend(request.calls);
        Ok(())
    }

    /// The run's calls, its withdrawn run and whether its Run Chain goes on.
    fn take_calls(&self, run_id: &str) -> (Vec<WorkflowCall>, String, bool) {
        let mut runs = self.runs.lock();
        match runs.get_mut(run_id) {
            Some(run) => (
                std::mem::take(&mut run.calls),
                run.withdraw_run.clone(),
                run.chain_continues,
            ),
            None => (vec![], String::new(), false),
        }
    }

    fn keep_closes(&self, run_id: &str, closes: Vec<(CloseStreamRequest, bool)>) {
        if closes.is_empty() {
            return;
        }
        let mut runs = self.runs.lock();
        runs.entry(run_id.to_string()).or_default().closes = closes;
    }

    fn take_closes(&self, run_id: &str) -> Vec<(CloseStreamRequest, bool)> {
        let mut runs = self.runs.lock();
        runs.get_mut(run_id)
            .map(|run| std::mem::take(&mut run.closes))
            .unwrap_or_default()
    }

    /// Drops what a failed task buffered. Its retry runs the Activations again.
    pub(crate) fn task_failed(&self, task: &WorkflowTaskFailedInfo) {
        if let Some(run) = self.runs.lock().get_mut(&task.run_id) {
            run.calls.clear();
            run.closes.clear();
        }
    }

    pub(crate) fn evicted(&self, run_id: &str) {
        self.runs.lock().remove(run_id);
    }

    /// Waits up to `timeout` for the closes that are still going out. Answers how many are left.
    pub(crate) async fn drain(&self, timeout: Duration) -> usize {
        self.closes.close();
        let _ = tokio::time::timeout(timeout, self.closes.wait()).await;
        self.closes.reopen();
        self.closes.len()
    }
}

/// The run's Producer in this task.
fn producer(task: &WorkflowTaskCompletionInfo) -> Producer {
    Producer {
        kind: ProducerKind::Workflow as i32,
        id: workflow_producer_id(&task.run_id),
        epoch: Some(ProducerEpoch {
            task_started_event_id: task.started_event_id,
            task_attempt: task.attempt as i32,
            task_started_time: task.started_time.map(timestamp),
            ..Default::default()
        }),
        run_id: task.run_id.clone(),
    }
}

fn previous_completed(task: &WorkflowTaskCompletionInfo) -> Option<WorkflowTaskEpoch> {
    task.previous_completed.as_ref().map(|c| WorkflowTaskEpoch {
        started_event_id: c.started_event_id,
        attempt: c.attempt as i32,
        started_time: c.started_time.map(timestamp),
    })
}

/// The calls of one Stream in one flush, in the order Workflow code made them.
#[derive(Default)]
struct StreamCalls {
    creates: Vec<Option<prost_types::Duration>>,
    records: Vec<AppendRecord>,
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

/// Whether a Workflow flush drops its Records for this refusal instead of failing the task. A
/// raise at the flush would differ between a live run and its replay.
fn dropped_for(reason: Reason) -> bool {
    matches!(reason, Reason::Closed | Reason::Expired | Reason::NotFound)
}

impl StreamService {
    /// Writes the run's buffer. One Lua call per batch, one Stream after another.
    pub(crate) async fn flush(
        &self,
        runs: &WorkflowRuns,
        task: &WorkflowTaskCompletionInfo,
        options: FlushOptions,
    ) -> StreamResult<()> {
        let (calls, withdraw_run, chain_continues) = runs.take_calls(&task.run_id);
        if calls.is_empty() {
            return Ok(());
        }
        // Without the started time, a dropped speculative task and the task that reuses its
        // started event id would get one epoch, and the second batch would count as a resend.
        if task.started_time.is_none() {
            self.metrics.flush_failed();
            return Err(StreamError::invalid(
                "The Workflow Task has no started time, which the Stream epoch needs",
            ));
        }
        let started = std::time::Instant::now();
        let deadline = Instant::from_std(task.deadline)
            .checked_sub(DEADLINE_MARGIN)
            .unwrap_or_else(Instant::now);
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
                    entry.creates.push(create.retention);
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
        let durable = (options.durable_writes > 0)
            .then(|| self.store.durable())
            .flatten();
        let store = durable.as_ref().unwrap_or(&self.store);
        let flush = async {
            for id in &order {
                let calls = streams.remove(id).unwrap_or_default();
                self.flush_stream(store, task, id, calls, &withdraw_run, deadline)
                    .await?;
            }
            if options.durable_writes > 0 {
                for id in &order {
                    Self::wait_durable(store, task, id, options).await?;
                }
            }
            Ok::<_, StreamError>(())
        };
        let result = match tokio::time::timeout_at(deadline, flush).await {
            Ok(result) => result,
            Err(_) => Err(StreamError::storage(
                "The flush did not end before the Workflow Task deadline",
            )),
        };
        self.metrics.hook_latency(started.elapsed());
        if let Err(error) = &result {
            self.metrics.flush_failed();
            warn!(run_id = %task.run_id, %error, "A Workflow flush failed the Workflow Task");
            return result;
        }
        runs.keep_closes(&task.run_id, closes);
        Ok(())
    }

    async fn flush_stream(
        &self,
        store: &RedisStore,
        task: &WorkflowTaskCompletionInfo,
        id: &str,
        calls: StreamCalls,
        withdraw_run: &str,
        deadline: Instant,
    ) -> StreamResult<()> {
        let namespace = task.namespace.as_str();
        let producer = producer(task);
        let completed = previous_completed(task);
        let bytes = supersession(
            &producer,
            completed.as_ref(),
            &task.failed_tasks,
            withdraw_run,
        );
        let withdrawal = Withdrawal {
            previous_completed: completed.as_ref(),
            failed_tasks: &task.failed_tasks,
            withdraw_run,
            supersession: &bytes,
        };
        for retention in calls.creates {
            let created = retried(deadline, || {
                store.create(
                    namespace,
                    id,
                    duration(retention),
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
                    "The Stream exists with another Retention, which stays"
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
        if calls.records.is_empty() {
            return Ok(());
        }
        let records = calls
            .records
            .into_iter()
            .map(|record| data_record(&producer, record))
            .collect();
        let (batches, oversize) = split(records);
        if oversize > 0 {
            self.metrics.records_dropped(oversize);
            warn!(
                stream = id,
                count = oversize,
                "Dropped Records over the Store's Record limit after the payload codec"
            );
        }
        for (n, records) in batches.iter().enumerate() {
            let batch = Batch {
                producer: &producer,
                sequence: n as i64 + 1,
                records,
                withdrawal: withdrawal.clone(),
            };
            let appended = retried(deadline, || store.append(namespace, id, &batch)).await;
            match self.count(appended) {
                Ok(_) => {}
                Err(error) if dropped_for(error.reason) => {
                    let count = batches[n..].iter().map(Vec::len).sum();
                    self.metrics.records_dropped(count);
                    warn!(
                        stream = id,
                        count,
                        %error,
                        "Dropped a Workflow's Records for a Stream that is closed, expired or gone"
                    );
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    async fn wait_durable(
        store: &RedisStore,
        task: &WorkflowTaskCompletionInfo,
        id: &str,
        options: FlushOptions,
    ) -> StreamResult<()> {
        let replicas = store
            .wait_replicas(
                &task.namespace,
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
        let producer = producer(&task);
        let completed = previous_completed(&task);
        let bytes = supersession(&producer, completed.as_ref(), &task.failed_tasks, "");
        let withdrawal = Withdrawal {
            previous_completed: completed.as_ref(),
            failed_tasks: &task.failed_tasks,
            withdraw_run: "",
            supersession: &bytes,
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
        if !shared.runs.has_calls(&task.run_id) {
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

impl WorkflowRuns {
    fn has_calls(&self, run_id: &str) -> bool {
        self.runs
            .lock()
            .get(run_id)
            .is_some_and(|run| !run.calls.is_empty())
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
