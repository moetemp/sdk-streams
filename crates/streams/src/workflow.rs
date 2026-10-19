//! A Workflow's own stream output. Lang publishes what each live activation committed before it
//! completes the activation, and the service appends it under an attempt that History alone
//! gives, as the activation's `WorkflowTaskHistoryInfo` names it.

use crate::{
    OwnerClient, RunClose, StreamError, StreamResult, StreamStore, append_batch,
    check_record_limit, check_topic, close_chain,
    proto::{
        AppendRecord, ChainId, OutputRecord, PublishWorkflowTaskRequest,
        PublishWorkflowTaskResponse, StoreAppendRequest, StreamFailureKind, StreamRecordKind,
        WorkflowTerminalCommand,
    },
    record::completed_tasks,
    stored_append_record, workflow_batch_digest, workflow_producer_id,
};
use parking_lot::Mutex;
use prost::Message;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};
use temporalio_common::{
    protos::temporal::api::enums::v1::WorkflowExecutionStatus,
    telemetry::metrics::{Counter, MetricKeyValue, MetricParameters, NewAttributes, TemporalMeter},
};
use tokio_util::task::TaskTracker;
use tracing::{debug, warn};

/// The bits of a Workflow producer's attempt that hold the reruns since the last completed task.
pub const STREAM_RETRY_BITS: u32 = 16;

/// A Workflow producer's attempt, with the prefix in the high bits, then 1 plus the reruns since
/// it. The prefix is the WorkflowTaskStarted event id of the last completed task, 0 before one
/// did. Each completed task moves the producer on with no supersession.
pub fn stream_attempt(prefix: i64, reruns: i64) -> i64 {
    let retry = (1 + reruns).min((1 << STREAM_RETRY_BITS) - 1);
    (prefix << STREAM_RETRY_BITS) | retry
}

const CONTENT_HASH_BYTES: usize = 32;

/// What the store adds to each record besides its body, topic and hash, counted high. That is the
/// producer id, the attempt, the sequence, the hash's metadata entry and the completed tasks.
const ENVELOPE_BYTES: usize = 256;

/// How often an append that may not have landed is sent again before the publish fails.
const APPEND_TRIES: u32 = 4;
#[cfg(not(test))]
const APPEND_RETRY_FIRST: Duration = Duration::from_millis(100);
// Keeps the retry tests fast.
#[cfg(test)]
const APPEND_RETRY_FIRST: Duration = Duration::from_millis(1);

/// The records one activation committed to one topic.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TopicOutput {
    pub(crate) topic: String,
    /// The first record's sequence in the attempt. Each attempt has its own dedupe session in the
    /// store, so the count starts at 1 per attempt and goes on across the task's activations.
    pub(crate) sequence: i64,
    pub(crate) records: Vec<OutputRecord>,
}

/// Splits `records` into appends per topic, in order of first publish, each of at most
/// `max_batch_bytes` past its first record, and counts them into `committed`, the records the
/// attempt committed to each topic so far.
///
/// Refuses a record no writer stores. Each refusal names what to fix, since the task fails the
/// same way on every retry.
pub(crate) fn output_by_topic(
    records: Vec<OutputRecord>,
    committed: &mut HashMap<String, i64>,
    max_batch_bytes: usize,
) -> StreamResult<Vec<TopicOutput>> {
    for (index, record) in records.iter().enumerate() {
        check_record(record).map_err(|reason| {
            StreamError::invalid_argument(format!(
                "Refusing a stream output commit, since record {index} {reason}"
            ))
        })?;
    }
    let mut topics: Vec<TopicOutput> = vec![];
    for record in records {
        match topics.iter_mut().find(|t| t.topic == record.topic) {
            Some(topic) => topic.records.push(record),
            None => topics.push(TopicOutput {
                topic: record.topic.clone(),
                sequence: 0,
                records: vec![record],
            }),
        }
    }
    let mut appends = vec![];
    for topic in topics {
        let mut bytes = 0;
        for record in topic.records {
            let size = record.encoded_len() + ENVELOPE_BYTES;
            let full = appends.last().is_some_and(|last: &TopicOutput| {
                last.topic == topic.topic && bytes + size > max_batch_bytes
            });
            if full || appends.last().is_none_or(|last| last.topic != topic.topic) {
                appends.push(TopicOutput {
                    topic: topic.topic.clone(),
                    sequence: 0,
                    records: vec![],
                });
                bytes = 0;
            }
            bytes += size;
            appends.last_mut().unwrap().records.push(record);
        }
    }
    for append in &mut appends {
        let count = committed.entry(append.topic.clone()).or_default();
        append.sequence = *count + 1;
        *count += append.records.len() as i64;
    }
    Ok(appends)
}

fn check_record(record: &OutputRecord) -> Result<(), String> {
    if record.topic.is_empty() {
        return Err("has no topic".to_string());
    }
    check_topic(&record.topic)?;
    match StreamRecordKind::try_from(record.kind) {
        Ok(StreamRecordKind::Data) => {
            if record.body.is_none() {
                return Err("is DATA without a body".to_string());
            }
            if record.content_hash.len() != CONTENT_HASH_BYTES {
                return Err(format!(
                    "is DATA with a {}-byte content hash, not the {CONTENT_HASH_BYTES}-byte \
                     SHA-256 of its plaintext body",
                    record.content_hash.len()
                ));
            }
        }
        Ok(StreamRecordKind::Finish) => {
            if record.body.is_some() || !record.content_hash.is_empty() {
                return Err("is FINISH with a body".to_string());
            }
        }
        _ => return Err(format!("has kind {}, not DATA or FINISH", record.kind)),
    }
    Ok(())
}

/// Whether a completion whose first terminal command is `command` ends the run's chain. A failure
/// with a retry policy and a run with a cron schedule can go on in the same chain, so they leave
/// the close to the producers' owner checks.
fn ends_chain(command: WorkflowTerminalCommand, cron: bool, has_retry_policy: bool) -> bool {
    match command {
        WorkflowTerminalCommand::Complete => !cron,
        WorkflowTerminalCommand::Fail => !cron && !has_retry_policy,
        WorkflowTerminalCommand::Cancel => true,
        WorkflowTerminalCommand::ContinueAsNew | WorkflowTerminalCommand::Unspecified => false,
    }
}

/// What one Worker holds for one run between its activations.
#[derive(Debug, Default)]
struct RunState {
    chain: ChainId,
    run_id: String,
    /// The producer attempt `committed_by_topic` counts for.
    attempt: i64,
    /// How many records the run appended to each topic under `attempt`, which is where its next
    /// commit's sequences start.
    committed_by_topic: HashMap<String, i64>,
    /// The attempts the run appended records under, with their topics, whose store sessions are
    /// not retired yet.
    sessions: BTreeMap<i64, BTreeSet<String>>,
    /// What closing the chain needs, when the run's last live completion ended the chain.
    chain_end: Option<ChainEnd>,
}

/// A live completion of this Worker whose terminal command ends the run's chain.
#[derive(Debug, Clone, Copy)]
struct ChainEnd {
    /// The attempt to close the chain at.
    attempt: i64,
    command: WorkflowTerminalCommand,
    /// The WorkflowTaskStarted event id of the completed task. Only a close of the run by this
    /// task proves that the server took the command.
    task_started: i64,
}

impl ChainEnd {
    /// Whether the run's close is the one this completion asked for. The close event must match
    /// the terminal command, and the task before it must be this one. A later task that closed
    /// the run, or a terminate or timeout, fails the check.
    fn closed_by(&self, close: &RunClose) -> bool {
        let status = match self.command {
            WorkflowTerminalCommand::Complete => WorkflowExecutionStatus::Completed,
            WorkflowTerminalCommand::Fail => WorkflowExecutionStatus::Failed,
            WorkflowTerminalCommand::Cancel => WorkflowExecutionStatus::Canceled,
            WorkflowTerminalCommand::ContinueAsNew | WorkflowTerminalCommand::Unspecified => {
                return false;
            }
        };
        close.status == status && close.closing_task_started_event_id == Some(self.task_started)
    }
}

impl RunState {
    /// The sessions of attempts below `current`. A later attempt in History proves the earlier
    /// tasks ended, so they never write again.
    fn sessions_below(&mut self, current: i64) -> BTreeMap<i64, BTreeSet<String>> {
        let kept = self.sessions.split_off(&current);
        std::mem::replace(&mut self.sessions, kept)
    }
}

/// A Worker id and a run id.
type RunKey = (String, String);

/// The Workflow output of every Worker in the process.
pub(crate) struct WorkflowOutput {
    store: Arc<dyn StreamStore>,
    owner: Arc<dyn OwnerClient>,
    output_dropped: Counter,
    meter: TemporalMeter,
    /// Each run's state, by Worker and run. A run has one outstanding activation at a time, so its
    /// own lock never waits on another publish of the same run.
    runs: Mutex<HashMap<RunKey, Arc<tokio::sync::Mutex<RunState>>>>,
    /// Store calls that run after the call that caused them, by Worker. A Worker shutdown waits
    /// for its own.
    tasks: Mutex<HashMap<String, TaskTracker>>,
}

impl WorkflowOutput {
    pub(crate) fn new(
        store: Arc<dyn StreamStore>,
        owner: Arc<dyn OwnerClient>,
        meter: TemporalMeter,
    ) -> Self {
        let output_dropped = meter.counter(MetricParameters {
            name: "workflow_stream_output_dropped".into(),
            description: "Count of a Workflow Task's per-topic stream output dropped, since the \
                          store has closed the topic"
                .into(),
            unit: "".into(),
        });
        Self {
            store,
            owner,
            output_dropped,
            meter,
            runs: Default::default(),
            tasks: Default::default(),
        }
    }

    fn spawn(&self, worker_id: &str, task: impl Future<Output = ()> + Send + 'static) {
        self.tasks
            .lock()
            .entry(worker_id.to_string())
            .or_default()
            .spawn(task);
    }

    /// Appends what `request` carries as its run's producer, one topic after another, and keeps
    /// what the run's next activation and its eviction need.
    pub(crate) async fn publish(
        &self,
        request: PublishWorkflowTaskRequest,
    ) -> StreamResult<PublishWorkflowTaskResponse> {
        if request.worker_id.is_empty() || request.run_id.is_empty() {
            return Err(StreamError::invalid_argument(
                "a Workflow Task publish needs a worker id and a run id",
            ));
        }
        let run = self
            .runs
            .lock()
            .entry((request.worker_id.clone(), request.run_id.clone()))
            .or_default()
            .clone();
        let mut run = run.lock().await;
        let first_run_id = if request.first_execution_run_id.is_empty() {
            request.run_id.clone()
        } else {
            request.first_execution_run_id.clone()
        };
        run.chain = ChainId {
            namespace: request.namespace.clone(),
            workflow_id: request.workflow_id.clone(),
            first_run_id,
        };
        run.run_id = request.run_id.clone();
        let prefix = request
            .completed_started_event_ids
            .first()
            .copied()
            .unwrap_or(0);
        let attempt = stream_attempt(prefix, i64::from(request.failed_attempts));
        if attempt != run.attempt {
            run.attempt = attempt;
            run.committed_by_topic.clear();
            let retired = run.sessions_below(attempt);
            self.retire(&request.worker_id, &run, retired);
        }
        run.chain_end = request
            .terminal_command
            .try_into()
            .ok()
            .filter(|command| ends_chain(*command, request.cron, request.has_retry_policy))
            .map(|command| ChainEnd {
                attempt,
                command,
                task_started: request.task_started_event_id,
            });
        let max_batch_bytes = self.store.capabilities().max_batch_bytes;
        // The counts move only once every append landed, so a failed publish leaves no gap in
        // the attempt's sequences.
        let mut committed = run.committed_by_topic.clone();
        let topics = output_by_topic(request.records, &mut committed, max_batch_bytes)?;
        run.sessions
            .entry(attempt)
            .or_default()
            .extend(topics.iter().map(|t| t.topic.clone()));
        let completed_tasks = completed_tasks(&request.completed_started_event_ids);
        let mut dropped_topics = vec![];
        for topic in &topics {
            let closed = self
                .append(&run, topic, request.task_started_event_id, &completed_tasks)
                .await?;
            if closed && !dropped_topics.contains(&topic.topic) {
                warn!(
                    run_id = %request.run_id,
                    topic = %topic.topic,
                    "Dropped the Workflow Task's stream output for a topic the store has closed"
                );
                let attributes = self.meter.extend_attributes(
                    self.meter.get_default_attributes().clone(),
                    NewAttributes {
                        attributes: vec![
                            MetricKeyValue::new("namespace", request.namespace.clone()),
                            MetricKeyValue::new("task_queue", request.task_queue.clone()),
                            MetricKeyValue::new("workflow_type", request.workflow_type.clone()),
                        ],
                    },
                );
                self.output_dropped.add(1, &attributes);
                dropped_topics.push(topic.topic.clone());
            }
        }
        run.committed_by_topic = committed;
        Ok(PublishWorkflowTaskResponse { dropped_topics })
    }

    /// Appends one topic's records, and answers whether the store has closed the topic.
    ///
    /// A failure that may have landed is sent again with a short backoff. The store answers a
    /// repeat of the attempt's newest batch with its positions, so this never writes twice. A
    /// topic the store has closed refuses every retry of the task too, so its records are dropped
    /// and the other topics go on. That holds when the run closed the topic itself before its
    /// task reran. Any other failure, or one that lasts, is returned, and the Workflow Task must
    /// fail.
    async fn append(
        &self,
        run: &RunState,
        topic: &TopicOutput,
        task_started: i64,
        completed_tasks: &[i64],
    ) -> StreamResult<bool> {
        let producer_id = workflow_producer_id(&run.run_id);
        let records = topic
            .records
            .iter()
            .zip(topic.sequence..)
            .map(|(record, sequence)| {
                let append = AppendRecord {
                    kind: record.kind,
                    body: record.body.clone(),
                    content_hash: record.content_hash.clone(),
                    link: record.link.clone(),
                };
                stored_append_record(&topic.topic, &producer_id, run.attempt, sequence, &append)
                    .map(|mut stored| {
                        // The attempt names both, so a repeat of the batch stores the same.
                        stored.task_started_event_id = task_started;
                        if sequence == topic.sequence {
                            stored.completed_tasks = completed_tasks.to_vec();
                        }
                        stored.encode_to_vec()
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        check_record_limit(&records, &self.store.capabilities())?;
        let request = StoreAppendRequest {
            chain: Some(run.chain.clone()),
            topic: topic.topic.clone(),
            producer_id,
            attempt: run.attempt,
            sequence: topic.sequence,
            digest: workflow_batch_digest(
                &topic.topic,
                topic
                    .records
                    .iter()
                    .map(|r| (r.kind, r.content_hash.as_slice())),
            ),
            records,
        };
        let mut delay = APPEND_RETRY_FIRST;
        let mut tries = 1;
        loop {
            match append_batch(&*self.store, request.clone()).await {
                Ok(_) => return Ok(false),
                Err(error) if error.kind == StreamFailureKind::Closed => return Ok(true),
                Err(error)
                    if tries < APPEND_TRIES
                        && matches!(
                            error.kind,
                            StreamFailureKind::OutcomeUnknown | StreamFailureKind::Storage
                        ) =>
                {
                    debug!(topic = %topic.topic, %error, "Appending stream output again");
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                    tries += 1;
                }
                Err(error) => {
                    return Err(StreamError::new(
                        error.kind,
                        format!("Could not append the Workflow Task's stream output ({error})"),
                    ));
                }
            }
        }
    }

    /// Drops the store's dedupe sessions in `sessions` in the background, so the next activation
    /// doesn't wait a store round trip per session. A failure only leaves a session in the store
    /// until retention, so it is logged.
    fn retire(&self, worker_id: &str, run: &RunState, sessions: BTreeMap<i64, BTreeSet<String>>) {
        // A store without producer state keeps no sessions to drop.
        if sessions.is_empty() || !self.store.capabilities().producer_state {
            return;
        }
        let store = self.store.clone();
        let chain = run.chain.clone();
        let producer_id = workflow_producer_id(&run.run_id);
        self.spawn(worker_id, async move {
            for (attempt, topics) in &sessions {
                for topic in topics {
                    if let Err(error) = store
                        .retire_producer(&chain, topic, &producer_id, *attempt)
                        .await
                    {
                        debug!(%topic, attempt, %error, "Could not retire a stream output session");
                    }
                }
            }
        });
    }

    /// Drops `worker_id`'s state for `run_id` and returns. When the eviction comes from the run's
    /// end, a background task closes the chain once the run's History proves that this Worker's
    /// terminal completion closed the run.
    ///
    /// The eviction reason alone is no proof that the server accepted the terminal command. The
    /// close needs a live terminal completion of this Worker that ended the chain. Then the run's
    /// close event must match the command, and the `WorkflowTaskCompleted` before it must belong
    /// to the completed task. A run that is still open, that continued as new, or that another
    /// task closed keeps its chain open, and the producers' owner checks cover the rest.
    pub(crate) async fn run_evicted(&self, worker_id: &str, run_id: &str, ending: bool) {
        let Some(run) = self
            .runs
            .lock()
            .remove(&(worker_id.to_string(), run_id.to_string()))
        else {
            return;
        };
        // Lang awaits each publish before it completes an activation, so this lock is free.
        let mut run = run.lock().await;
        let current = run.attempt;
        let retired = run.sessions_below(current);
        self.retire(worker_id, &run, retired);
        if let Some(end) = run.chain_end.filter(|_| ending) {
            self.close_if_closed_by(worker_id, &run, end);
        }
    }

    /// Closes the run's chain in the background, when the run's History shows that the completion
    /// `end` describes closed the run.
    fn close_if_closed_by(&self, worker_id: &str, run: &RunState, end: ChainEnd) {
        let store = self.store.clone();
        let owner = self.owner.clone();
        let chain = run.chain.clone();
        let run_id = run.run_id.clone();
        self.spawn(worker_id, async move {
            // Lang publishes every terminal completion, also of a run that wrote no stream. Such
            // a chain has nothing to close. A close would cost a History read and leave a marker
            // in the store for the whole retention.
            if store.capabilities().topic_listing
                && store
                    .list_topics(&chain)
                    .await
                    .is_ok_and(|topics| topics.is_empty())
            {
                return;
            }
            match owner
                .run_close(&chain.namespace, &chain.workflow_id, &run_id)
                .await
            {
                Ok(Some(close)) if end.closed_by(&close) => {
                    let producer_id = workflow_producer_id(&run_id);
                    if let Err(error) =
                        close_chain(&*store, &chain, &producer_id, end.attempt).await
                    {
                        warn!(%error, "Could not close the streams of a run that ended its chain");
                    }
                }
                Ok(close) => {
                    debug!(
                        %run_id,
                        ?close,
                        "Kept the chain open, since History shows no close by this completion"
                    );
                }
                Err(error) => {
                    warn!(
                        %run_id,
                        %error,
                        "Kept the chain open, since the run's History could not be read"
                    );
                }
            }
        });
    }

    /// Drops all of `worker_id`'s runs, then waits for the store calls its runs left running.
    pub(crate) async fn shutdown_worker(&self, worker_id: &str) {
        let runs: Vec<_> = {
            let mut all = self.runs.lock();
            let keys: Vec<_> = all
                .keys()
                .filter(|(w, _)| w == worker_id)
                .cloned()
                .collect();
            keys.into_iter()
                .filter_map(|key| all.remove(&key))
                .collect()
        };
        for run in runs {
            let mut run = run.lock().await;
            let current = run.attempt;
            let retired = run.sessions_below(current);
            self.retire(worker_id, &run, retired);
            // A Worker that shuts down may never hand lang the eviction of a run that just ended,
            // so the shutdown takes its place. The History check still guards the close.
            if let Some(end) = run.chain_end {
                self.close_if_closed_by(worker_id, &run, end);
            }
        }
        let tasks = self.tasks.lock().remove(worker_id);
        if let Some(tasks) = tasks {
            tasks.close();
            tasks.wait().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_common::protos::temporal::api::common::v1::Payload;

    fn data(topic: &str, hash: u8) -> OutputRecord {
        OutputRecord {
            topic: topic.to_string(),
            kind: StreamRecordKind::Data as i32,
            body: Some(Payload {
                data: b"ciphertext".to_vec(),
                ..Default::default()
            }),
            content_hash: vec![hash; CONTENT_HASH_BYTES],
            ..Default::default()
        }
    }

    fn finish(topic: &str) -> OutputRecord {
        OutputRecord {
            topic: topic.to_string(),
            kind: StreamRecordKind::Finish as i32,
            ..Default::default()
        }
    }

    #[test]
    fn the_attempt_bits_match_the_shared_vectors() {
        let vectors: serde_json::Value =
            serde_json::from_str(include_str!("../testdata/workflow_attempts.json")).unwrap();
        assert_eq!(vectors["retry_bits"], STREAM_RETRY_BITS);
        for case in vectors["attempts"].as_array().unwrap() {
            let (prefix, reruns) = (
                case["prefix"].as_i64().unwrap(),
                case["reruns"].as_i64().unwrap(),
            );
            assert_eq!(
                stream_attempt(prefix, reruns),
                case["attempt"].as_i64().unwrap(),
                "{case}"
            );
        }
    }

    #[test]
    fn the_completed_tasks_end_in_zero_only_when_they_reach_the_start() {
        assert_eq!(completed_tasks(&[]), [0]);
        assert_eq!(completed_tasks(&[8, 3]), [8, 3, 0]);
        let eight: Vec<i64> = (1..=8).rev().collect();
        assert_eq!(completed_tasks(&eight), eight);
    }

    #[test]
    fn only_a_terminal_command_that_ends_the_chain_closes_it() {
        use WorkflowTerminalCommand::*;
        assert!(ends_chain(Complete, false, true));
        assert!(!ends_chain(Complete, true, false));
        assert!(ends_chain(Fail, false, false));
        assert!(!ends_chain(Fail, false, true));
        assert!(!ends_chain(Fail, true, false));
        assert!(ends_chain(Cancel, true, true));
        assert!(!ends_chain(ContinueAsNew, false, false));
        assert!(!ends_chain(Unspecified, false, false));
    }

    #[test]
    fn a_large_commit_goes_as_several_appends_per_topic() {
        let big = |topic: &str, hash: u8| OutputRecord {
            body: Some(Payload {
                data: vec![7; 3 << 20],
                ..Default::default()
            }),
            ..data(topic, hash)
        };
        let mut committed = HashMap::new();
        let appends = output_by_topic(
            vec![big("a", 1), big("a", 2), big("a", 3), data("b", 4)],
            &mut committed,
            crate::MAX_BATCH_BYTES,
        )
        .unwrap();
        let shape: Vec<(&str, i64, usize)> = appends
            .iter()
            .map(|append| (append.topic.as_str(), append.sequence, append.records.len()))
            .collect();
        assert_eq!(shape, [("a", 1, 1), ("a", 2, 1), ("a", 3, 1), ("b", 1, 1)]);
        assert_eq!(committed["a"], 3);
    }

    #[test]
    fn a_commit_splits_by_the_stores_batch_limit() {
        let mut committed = HashMap::new();
        let records = (0..4).map(|n| data("a", n)).collect();
        let appends = output_by_topic(records, &mut committed, 700).unwrap();
        let sizes: Vec<usize> = appends.iter().map(|append| append.records.len()).collect();
        assert_eq!(sizes, [2, 2]);
        assert_eq!((appends[1].sequence, committed["a"]), (3, 4));
    }

    #[test]
    fn topics_keep_their_order_of_first_publish_and_sequences_go_on() {
        let mut committed = HashMap::new();
        let first = output_by_topic(
            vec![data("b", 1), data("a", 2), finish("b")],
            &mut committed,
            crate::MAX_BATCH_BYTES,
        )
        .unwrap();
        let shape: Vec<_> = first
            .iter()
            .map(|t| (t.topic.as_str(), t.sequence, t.records.len()))
            .collect();
        assert_eq!(shape, [("b", 1, 2), ("a", 1, 1)]);
        let second =
            output_by_topic(vec![data("a", 3)], &mut committed, crate::MAX_BATCH_BYTES).unwrap();
        assert_eq!((second[0].topic.as_str(), second[0].sequence), ("a", 2));
    }

    #[test]
    fn each_record_no_writer_stores_is_refused_with_its_own_message() {
        let mut no_body = data("t", 1);
        no_body.body = None;
        let mut short_hash = data("t", 1);
        short_hash.content_hash.truncate(3);
        let mut finish_with_body = finish("t");
        finish_with_body.body = Some(Payload::default());
        let unspecified = OutputRecord {
            kind: StreamRecordKind::Unspecified as i32,
            ..data("t", 1)
        };
        for (record, expected) in [
            (data("", 1), "record 1 has no topic"),
            (data("a\u{1f}b", 1), "control character"),
            (no_body, "record 1 is DATA without a body"),
            (short_hash, "record 1 is DATA with a 3-byte content hash"),
            (finish_with_body, "record 1 is FINISH with a body"),
            (unspecified, "record 1 has kind 0"),
        ] {
            let error = output_by_topic(
                vec![data("t", 9), record],
                &mut HashMap::new(),
                crate::MAX_BATCH_BYTES,
            )
            .unwrap_err();
            assert_eq!(error.kind, StreamFailureKind::InvalidArgument);
            assert!(error.message.contains(expected), "{expected}: {error}");
        }
    }
}
