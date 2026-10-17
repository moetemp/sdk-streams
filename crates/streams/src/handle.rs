//! The stream calls lang makes, from the service protos down to a store.
//!
//! [Streams] resolves a stream's address to the run chain its store keys it by, checks the owner
//! where the contract says so, and mints and checks cursors. Stores only move records.

use crate::{
    BEGINNING, END, ReadTarget, StreamError, StreamResult, StreamStore, WORKFLOW_OWNER_KIND,
    activity_producer_id, append_batch, check_append_digest, check_store_limits, close_topic,
    mint_cursor,
    owner::{OwnerClient, OwnerDescription, OwnerError},
    proto::{
        ActivityProducer, AppendRequest, AppendResponse, ChainId, CloseRequest, CloseResponse,
        DeleteOwnerRequest, DeleteOwnerResponse, LatestRequest, LatestResponse, NamedProducer,
        ReadRequest, ReadResponse, RetireProducerRequest, RetireProducerResponse,
        StoreAppendRequest, StoreLatestRequest, StreamAddress, StreamFailureKind, StreamOwnerKind,
        append_request, retire_producer_request,
    },
    reader::{ReadState, read_with},
    stored_append_record, stream_hash,
};
use prost::Message;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use temporalio_common::protos::temporal::api::enums::v1::WorkflowExecutionStatus;
use tokio::time::Instant;

/// The longest a producer writes without asking whether its owner's chain ended.
pub const OWNER_RECHECK: Duration = Duration::from_secs(60);

/// How long a read waits by default for an owner that Temporal doesn't know yet.
pub const OWNER_START_GRACE: Duration = Duration::from_secs(60);

/// How the stream layer behaves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsOptions {
    /// How long a producer writes before it asks again whether its owner's chain ended. Nothing
    /// else ends the writes of a Workflow that never published itself.
    pub owner_recheck: Duration,
    /// How many owners, producer attempts and chains to remember the last owner check of.
    pub remembered: usize,
    /// How long an idle read waits before it first asks whether its owner ended.
    pub owner_check_min: Duration,
    /// The longest an idle read waits between two such questions.
    pub owner_check_max: Duration,
    /// How long after its first owner check a read takes a Workflow that Temporal doesn't know
    /// as one that has not started yet, and answers empty. Zero fails the read at once.
    pub owner_start_grace: Duration,
}

impl Default for StreamsOptions {
    fn default() -> Self {
        Self {
            owner_recheck: OWNER_RECHECK,
            remembered: 10_000,
            owner_check_min: Duration::from_millis(500),
            owner_check_max: Duration::from_secs(5),
            owner_start_grace: OWNER_START_GRACE,
        }
    }
}

impl StreamsOptions {
    /// The options for a store that keeps records for `retention`. A close mark lives as long
    /// as the stream, so a producer asks again before a mark it could miss expires.
    pub fn for_retention(retention: Duration) -> Self {
        Self {
            owner_recheck: retention.min(OWNER_RECHECK),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OwnerKey {
    namespace: String,
    workflow_id: String,
    run_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProducerKey {
    owner: OwnerKey,
    producer_id: String,
    attempt: i64,
}

#[derive(Debug, Clone)]
struct Checked {
    first_run_id: String,
    at: Instant,
}

/// Remembers when each key was last checked, forgetting the oldest when full.
#[derive(Debug)]
struct Remembered<K> {
    entries: Mutex<HashMap<K, Checked>>,
    limit: usize,
}

impl<K: std::hash::Hash + Eq + Clone> Remembered<K> {
    fn new(limit: usize) -> Self {
        Self {
            entries: Mutex::default(),
            limit: limit.max(1),
        }
    }

    fn get(&self, key: &K) -> Option<Checked> {
        self.entries.lock().unwrap().get(key).cloned()
    }

    fn put(&self, key: K, first_run_id: &str, fresh_for: Duration) {
        let mut entries = self.entries.lock().unwrap();
        if entries.len() >= self.limit && !entries.contains_key(&key) {
            entries.retain(|_, checked| checked.at.elapsed() < fresh_for);
            if entries.len() >= self.limit
                && let Some(oldest) = entries
                    .iter()
                    .min_by_key(|(_, checked)| checked.at)
                    .map(|(key, _)| key.clone())
            {
                entries.remove(&oldest);
            }
        }
        entries.insert(
            key,
            Checked {
                first_run_id: first_run_id.to_string(),
                at: Instant::now(),
            },
        );
    }

    fn forget(&self, key: &K) {
        self.entries.lock().unwrap().remove(key);
    }
}

/// The producer attempt a call writes as, and the topic it writes, so a close at its owner check
/// reaches the topic on a store without close flags.
#[derive(Debug, Clone, Copy)]
struct Writer<'a> {
    topic: &'a str,
    producer_id: &'a str,
    attempt: i64,
}

/// The stream layer over one store.
pub struct Streams {
    store: Arc<dyn StreamStore>,
    owner: Arc<dyn OwnerClient>,
    options: StreamsOptions,
    /// The last owner check per owner, which all its producers share.
    owners: Remembered<OwnerKey>,
    /// The chain each producer attempt began on.
    producers: Remembered<ProducerKey>,
    chains: Remembered<OwnerKey>,
    /// When Temporal first answered that an owner is gone, per Workflow id. A failover can
    /// answer `NotFound` until replication catches up, so one answer doesn't close a chain.
    missing: Remembered<OwnerKey>,
}

impl std::fmt::Debug for Streams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Streams")
            .field("store", &self.store.name())
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl Streams {
    /// The stream layer over `store`, asking `owner` about owners.
    pub fn new(
        store: Arc<dyn StreamStore>,
        owner: Arc<dyn OwnerClient>,
        options: StreamsOptions,
    ) -> Self {
        Self {
            store,
            owner,
            owners: Remembered::new(options.remembered),
            producers: Remembered::new(options.remembered),
            chains: Remembered::new(options.remembered),
            missing: Remembered::new(options.remembered),
            options,
        }
    }

    /// Appends one batch for one producer attempt.
    ///
    /// The first batch for an owner, and the first after [StreamsOptions::owner_recheck], asks
    /// whether the owner's chain ended. All producers of the owner share that check. If it did,
    /// or Temporal no longer holds the owner, or a new chain took the Workflow id, the producer
    /// closes the chain, or its own topic on a store without close flags, and the batch is
    /// refused as closed.
    pub async fn append(&self, request: AppendRequest) -> StreamResult<AppendResponse> {
        let stream = address(request.stream.as_ref())?;
        let (producer_id, attempt) = match &request.producer {
            Some(append_request::Producer::Named(named)) => producer(Some(named), None)?,
            Some(append_request::Producer::Activity(activity)) => producer(None, Some(activity))?,
            None => {
                return Err(StreamError::invalid_argument(
                    "an append needs its producer",
                ));
            }
        };
        if request.sequence < 1 {
            return Err(StreamError::invalid_argument(format!(
                "a producer's sequence starts at one, not {}",
                request.sequence
            )));
        }
        if request.records.is_empty() {
            return Err(StreamError::invalid_argument(
                "an append needs at least one record",
            ));
        }
        check_append_digest(&request.digest)?;
        let writer = Writer {
            topic: &stream.topic,
            producer_id: &producer_id,
            attempt,
        };
        let chain = self
            .producer_chain(
                producer_key(owner_key(stream), &producer_id, attempt),
                writer,
            )
            .await?;
        let records = request
            .records
            .iter()
            .enumerate()
            .map(|(index, record)| {
                stored_append_record(
                    &stream.topic,
                    &producer_id,
                    attempt,
                    request.sequence + index as i64,
                    record,
                )
                .map(|stored| stored.encode_to_vec())
            })
            .collect::<StreamResult<Vec<_>>>()?;
        check_store_limits(&records, &self.store.capabilities())?;
        let landed = append_batch(
            &*self.store,
            StoreAppendRequest {
                chain: Some(chain),
                topic: stream.topic.clone(),
                producer_id,
                attempt,
                sequence: request.sequence,
                // Lang takes it before the codec, and only lang's own retries compare it, so the
                // store keeps it as given.
                digest: request.digest.clone(),
                records,
            },
        )
        .await?;
        Ok(AppendResponse {
            first_cursor: self.cursor(stream, &landed.first_position),
            last_cursor: self.cursor(stream, &landed.last_position),
        })
    }

    /// Reads one page of a stream, waiting up to the request's `wait` for a record.
    ///
    /// The first call resolves the chain, and the state carries it. While nothing arrives, the
    /// read asks whether its owner ended, first after [StreamsOptions::owner_check_min] and
    /// then twice as long each time, up to [StreamsOptions::owner_check_max]. A record starts
    /// the interval over. So many readers parked on a quiet stream don't load the server. Once
    /// the owner ended, or the store marked the stream closed, the read delivers what is left
    /// and answers `done`. A read pinned to a run ends when that run closes, even if the chain
    /// continued as new. A read whose owner Temporal doesn't know yet answers empty within
    /// [StreamsOptions::owner_start_grace], so it can start before its owner does.
    pub async fn read(&self, request: ReadRequest) -> StreamResult<ReadResponse> {
        let stream = address(request.stream.as_ref())?;
        let mut state = ReadState::decode_from(&request.state)?;
        let wait = request
            .wait
            .and_then(|wait| Duration::try_from(wait).ok())
            .unwrap_or_default();
        let deadline = Instant::now() + wait;
        if state.first_run_id.is_empty() {
            let Some(chain) = self.started_chain(stream, &mut state, deadline).await? else {
                // The owner has not started, so its stream holds nothing and its end is its start.
                let cursor = if request.after == END {
                    BEGINNING.to_string()
                } else {
                    request.after.clone()
                };
                return Ok(ReadResponse {
                    cursor,
                    state: state.encode_to_vec(),
                    ..Default::default()
                });
            };
            state.first_run_id = chain.first_run_id;
        }
        let target = ReadTarget {
            chain: chain_of(stream, &state.first_run_id),
            topic: stream.topic.clone(),
            stream_hash: self.hash(stream),
        };
        let minimum = self.options.owner_check_min;
        let mut page_request = request.clone();
        let response = loop {
            let now = unix_ms();
            if state.check_interval_ms == 0 {
                state.check_interval_ms = millis(minimum);
                state.next_check_ms = now + state.check_interval_ms;
            }
            let until_check = Duration::from_millis(state.next_check_ms.saturating_sub(now));
            let remaining = deadline.saturating_duration_since(Instant::now());
            let wait = if state.owner_ended {
                Duration::ZERO
            } else {
                remaining.min(until_check)
            };
            page_request.wait = wait.try_into().ok();
            let (mut page, closed) =
                read_with(&*self.store, &target, &page_request, &mut state).await?;
            page_request.after = page.cursor.clone();
            if !page.records.is_empty() {
                state.check_interval_ms = millis(minimum);
                state.next_check_ms = unix_ms() + state.check_interval_ms;
                break page;
            }
            if closed && !state.owner_ended {
                // One more pass after learning of the close, as after learning the owner ended.
                state.owner_ended = true;
                continue;
            }
            if state.owner_ended {
                page.done = true;
                break page;
            }
            if unix_ms() >= state.next_check_ms {
                state.owner_ended = self.owner_ended(stream, &target.chain).await?;
                state.check_interval_ms = (state.check_interval_ms * 2)
                    .clamp(millis(minimum), millis(self.options.owner_check_max));
                state.next_check_ms = unix_ms() + state.check_interval_ms;
                // One more pass after learning the owner ended, so a record that landed
                // between the read and the describe is delivered.
                if state.owner_ended {
                    continue;
                }
            }
            if Instant::now() >= deadline {
                break page;
            }
        };
        Ok(ReadResponse {
            state: state.encode_to_vec(),
            ..response
        })
    }

    /// The chain a read follows, or `None` when Temporal doesn't know its owner yet, within
    /// [StreamsOptions::owner_start_grace] of the read's first owner check. It asks again as
    /// an idle read does until `deadline`. After the grace the read fails as not found.
    async fn started_chain(
        &self,
        stream: &StreamAddress,
        state: &mut ReadState,
        deadline: Instant,
    ) -> StreamResult<Option<ChainId>> {
        let mut interval = self.options.owner_check_min;
        loop {
            let error = match self.chain(stream).await {
                Ok(chain) => {
                    state.owner_missing_since_ms = 0;
                    return Ok(Some(chain));
                }
                Err(error) if error.kind == StreamFailureKind::NotFound => error,
                Err(error) => return Err(error),
            };
            let now = unix_ms();
            if state.owner_missing_since_ms == 0 {
                state.owner_missing_since_ms = now;
            }
            let missing_for = now.saturating_sub(state.owner_missing_since_ms);
            if missing_for >= millis(self.options.owner_start_grace) {
                return Err(error);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            tokio::time::sleep(interval.min(remaining)).await;
            interval = (interval * 2).min(self.options.owner_check_max);
        }
    }

    /// Whether the owner a read follows ended. Following the chain, a run that continued as
    /// new is not the end, and a new chain on the Workflow id is. A reader only ends its own read,
    /// so it needs no write access. Producers and the owner's Core close the chain.
    async fn owner_ended(&self, stream: &StreamAddress, chain: &ChainId) -> StreamResult<bool> {
        let key = owner_key(stream);
        match self.describe(&key, &key.run_id).await {
            // The chain came from this Workflow, so History has since dropped it, or a failover
            // hides it for a while.
            Err(OwnerError::NotFound(_)) => Ok(self.gone_for_good(chain)),
            Err(error) => Err(describe_failed(&error)),
            Ok(described) if !key.run_id.is_empty() => {
                Ok(described.status != WorkflowExecutionStatus::Running)
            }
            Ok(latest) => Ok(latest.chain_ended() || latest.first_run_id != chain.first_run_id),
        }
    }

    /// The cursor of a stream's newest record, empty when it holds none.
    pub async fn latest(&self, request: LatestRequest) -> StreamResult<LatestResponse> {
        let stream = address(request.stream.as_ref())?;
        let chain = self.chain(stream).await?;
        let position = self
            .store
            .latest(StoreLatestRequest {
                chain: Some(chain),
                topic: stream.topic.clone(),
            })
            .await?
            .position;
        Ok(LatestResponse {
            cursor: match position.as_str() {
                "" => String::new(),
                position => self.cursor(stream, position),
            },
        })
    }

    /// Closes one stream, so reads end. With close flags, producers' new batches are refused.
    /// Without them, a batch that lands after the close is one no reader delivers.
    pub async fn close(&self, request: CloseRequest) -> StreamResult<CloseResponse> {
        let stream = address(request.stream.as_ref())?;
        let chain = self.chain(stream).await?;
        close_topic(&*self.store, &chain, &stream.topic, "", 0).await?;
        Ok(CloseResponse {})
    }

    /// Drops one producer attempt's dedupe state on a stream, on the chain that attempt wrote
    /// on. It refuses nothing on a closed stream, and a stream or attempt with no state answers
    /// the same.
    ///
    /// Only safe once the attempt's last append returned: a retry of an earlier batch would land
    /// a second time. Nothing else is lost. A store compares a batch with its own attempt's
    /// state only, and readers find a newer attempt from the records. A store without producer
    /// state keeps none, so the call does nothing there.
    pub async fn retire_producer(
        &self,
        request: RetireProducerRequest,
    ) -> StreamResult<RetireProducerResponse> {
        let stream = address(request.stream.as_ref())?;
        let (producer_id, attempt) = match &request.producer {
            Some(retire_producer_request::Producer::Named(named)) => producer(Some(named), None)?,
            Some(retire_producer_request::Producer::Activity(activity)) => {
                producer(None, Some(activity))?
            }
            None => {
                return Err(StreamError::invalid_argument("a retire needs its producer"));
            }
        };
        if !self.store.capabilities().producer_state {
            return Ok(RetireProducerResponse {});
        }
        let key = producer_key(owner_key(stream), &producer_id, attempt);
        // The chain the attempt's appends resolved, even when a newer chain took the Workflow
        // id since, so the state goes from where it was written.
        let chain = match self.producers.get(&key) {
            Some(checked) => chain_of_key(&key.owner, &checked.first_run_id),
            None => self.chain(stream).await?,
        };
        self.store
            .retire_producer(&chain, &stream.topic, &key.producer_id, attempt)
            .await?;
        Ok(RetireProducerResponse {})
    }

    /// Deletes every stream of one owner, across all its chains. It asks Temporal nothing, so it
    /// works after the owner is gone. Delete only after the chain closed and its producers
    /// stopped, since a late append writes a fresh stream with no dedupe state and no close mark.
    pub async fn delete_owner(
        &self,
        request: DeleteOwnerRequest,
    ) -> StreamResult<DeleteOwnerResponse> {
        if request.owner_kind != StreamOwnerKind::Workflow as i32 {
            return Err(StreamError::unsupported(format!(
                "this release keeps streams of Workflows only, not owner kind {}",
                request.owner_kind
            )));
        }
        if request.namespace.is_empty() || request.workflow_id.is_empty() {
            return Err(StreamError::invalid_argument(
                "a delete needs the namespace and the owner's Workflow id",
            ));
        }
        self.store.delete_owner(request).await
    }

    /// The chain a producer writes on. A producer attempt stays on the chain it began on, and
    /// is refused once a new chain took the Workflow id.
    async fn producer_chain(&self, key: ProducerKey, writer: Writer<'_>) -> StreamResult<ChainId> {
        let chain = self.owner_chain(&key.owner, writer).await?;
        if let Some(began) = self.producers.get(&key)
            && began.first_run_id != chain.first_run_id
        {
            let began = chain_of_key(&key.owner, &began.first_run_id);
            return Err(self.closed(&began, "has closed", writer).await);
        }
        self.producers
            .put(key, &chain.first_run_id, self.options.owner_recheck);
        Ok(chain)
    }

    /// The chain an owner writes on now, asking about the owner when no producer of it asked
    /// within the recheck interval.
    async fn owner_chain(&self, owner: &OwnerKey, writer: Writer<'_>) -> StreamResult<ChainId> {
        let recheck = self.options.owner_recheck;
        let known = self.owners.get(owner);
        if let Some(checked) = &known
            && checked.at.elapsed() < recheck
        {
            return Ok(chain_of_key(owner, &checked.first_run_id));
        }
        let chain = if owner.run_id.is_empty() {
            // The latest run names the chain and tells whether it ended, in one call.
            let latest = self.describe(owner, "").await;
            if let (Err(OwnerError::NotFound(_)), Some(checked)) = (&latest, &known) {
                // The chain came from this Workflow, so History has since dropped it.
                let gone = chain_of_key(owner, &checked.first_run_id);
                return Err(self.gone(&gone, writer).await);
            }
            let latest = latest.map_err(not_found)?;
            let chain = chain_of_key(owner, &first_run_id(owner, &latest)?);
            if latest.chain_ended() {
                return Err(self.closed(&chain, "has closed", writer).await);
            }
            chain
        } else if let Some(checked) = known {
            // A pinned run's chain never changes, so only its end is left to check.
            let chain = chain_of_key(owner, &checked.first_run_id);
            self.refuse_if_ended(&chain, writer).await?;
            chain
        } else {
            let pinned = self
                .describe(owner, &owner.run_id)
                .await
                .map_err(not_found)?;
            let chain = chain_of_key(owner, &first_run_id(owner, &pinned)?);
            // A running run is its chain's latest, so it answers the end check too.
            if pinned.status != WorkflowExecutionStatus::Running {
                self.refuse_if_ended(&chain, writer).await?;
            }
            chain
        };
        self.owners.put(owner.clone(), &chain.first_run_id, recheck);
        Ok(chain)
    }

    /// Closes the chain and refuses when its latest run ended, when Temporal no longer holds it,
    /// or when a new chain took its Workflow id.
    async fn refuse_if_ended(&self, chain: &ChainId, writer: Writer<'_>) -> StreamResult<()> {
        let key = OwnerKey {
            namespace: chain.namespace.clone(),
            workflow_id: chain.workflow_id.clone(),
            run_id: String::new(),
        };
        match self.describe(&key, "").await {
            // The chain came from this Workflow, so History has since dropped it.
            Err(OwnerError::NotFound(_)) => Err(self.gone(chain, writer).await),
            Err(error) => Err(describe_failed(&error)),
            Ok(latest) if latest.chain_ended() || latest.first_run_id != chain.first_run_id => {
                Err(self.closed(chain, "has closed", writer).await)
            }
            Ok(_) => Ok(()),
        }
    }

    /// Closes a chain whose owner Temporal answered is gone, but only on the second such answer
    /// at least [StreamsOptions::owner_recheck] after the first. The first answer fails the call
    /// as a storage failure, which the caller retries.
    async fn gone(&self, chain: &ChainId, writer: Writer<'_>) -> StreamError {
        if self.gone_for_good(chain) {
            self.closed(chain, "is gone from History", writer).await
        } else {
            missing(chain)
        }
    }

    /// Whether Temporal answered that `chain`'s owner is gone twice, at least
    /// [StreamsOptions::owner_recheck] apart, with no answer that holds it in between.
    fn gone_for_good(&self, chain: &ChainId) -> bool {
        let key = OwnerKey {
            namespace: chain.namespace.clone(),
            workflow_id: chain.workflow_id.clone(),
            run_id: String::new(),
        };
        let recheck = self.options.owner_recheck;
        match self.missing.get(&key) {
            Some(seen) if seen.first_run_id == chain.first_run_id => seen.at.elapsed() >= recheck,
            _ => {
                self.missing.put(key, &chain.first_run_id, recheck * 2);
                false
            }
        }
    }

    /// Closes what a producer that learned its owner ended can reach: the chain with close
    /// flags, else its own topic.
    async fn closed(&self, chain: &ChainId, how: &str, writer: Writer<'_>) -> StreamError {
        let closed = if self.store.capabilities().producer_state {
            self.store.close_chain(chain).await
        } else {
            let (topic, producer_id) = (writer.topic, writer.producer_id);
            close_topic(&*self.store, chain, topic, producer_id, writer.attempt).await
        };
        if let Err(error) = closed {
            return error;
        }
        StreamError::closed(format!(
            "the Workflow {:?} that owns this stream {how}",
            chain.workflow_id
        ))
    }

    /// The chain a stream address names now.
    async fn chain(&self, stream: &StreamAddress) -> StreamResult<ChainId> {
        let key = owner_key(stream);
        // A pinned run's chain never changes. The latest run's chain changes when a new chain
        // takes the Workflow id.
        if let Some(checked) = self.chains.get(&key)
            && (!key.run_id.is_empty() || checked.at.elapsed() < self.options.owner_recheck)
        {
            return Ok(chain_of(stream, &checked.first_run_id));
        }
        let described = self.describe(&key, &key.run_id).await.map_err(not_found)?;
        let first_run_id = first_run_id(&key, &described)?;
        self.chains
            .put(key, &first_run_id, self.options.owner_recheck);
        Ok(chain_of(stream, &first_run_id))
    }

    async fn describe(
        &self,
        owner: &OwnerKey,
        run_id: &str,
    ) -> Result<OwnerDescription, OwnerError> {
        let described = self
            .owner
            .describe(&owner.namespace, &owner.workflow_id, run_id)
            .await;
        if described.is_ok() {
            self.missing.forget(&OwnerKey {
                namespace: owner.namespace.clone(),
                workflow_id: owner.workflow_id.clone(),
                run_id: String::new(),
            });
        }
        described
    }

    fn hash(&self, stream: &StreamAddress) -> String {
        stream_hash(
            &stream.namespace,
            WORKFLOW_OWNER_KIND,
            &stream.workflow_id,
            &stream.topic,
        )
    }

    fn cursor(&self, stream: &StreamAddress, position: &str) -> String {
        mint_cursor(self.store.name(), &self.hash(stream), position)
    }
}

/// The address of a stream this release keeps, checked.
fn address(stream: Option<&StreamAddress>) -> StreamResult<&StreamAddress> {
    let stream =
        stream.ok_or_else(|| StreamError::invalid_argument("a stream call needs its stream"))?;
    if stream.owner_kind != StreamOwnerKind::Workflow as i32 {
        return Err(StreamError::unsupported(format!(
            "this release keeps streams of Workflows only, not owner kind {}",
            stream.owner_kind
        )));
    }
    if stream.namespace.is_empty() || stream.workflow_id.is_empty() {
        return Err(StreamError::invalid_argument(
            "a stream needs its namespace and its owner's Workflow id",
        ));
    }
    check_topic(&stream.topic)?;
    Ok(stream)
}

/// Refuses a topic no store keys reliably, by the rules Core's Workflow path also applies.
/// The producer id and attempt a named producer or an Activity attempt writes as.
fn producer(
    named: Option<&NamedProducer>,
    activity: Option<&ActivityProducer>,
) -> StreamResult<(String, i64)> {
    let (producer_id, attempt) = match (named, activity) {
        (Some(named), _) => {
            if named.producer_id.is_empty() {
                return Err(StreamError::invalid_argument("a producer needs an id"));
            }
            // A Workflow run writes its own output as `@<run id>`. A named producer with that id
            // would share the run's dedupe session and could withdraw its records.
            if named.producer_id.starts_with('@') {
                return Err(StreamError::invalid_argument(format!(
                    "a named producer's id can't start with `@`, which names a Workflow run's \
                     own output, not {:?}",
                    named.producer_id
                )));
            }
            (named.producer_id.clone(), named.attempt)
        }
        (None, Some(activity)) => (activity_producer_id(activity), i64::from(activity.attempt)),
        (None, None) => return Err(StreamError::invalid_argument("a call needs its producer")),
    };
    if attempt < 1 {
        return Err(StreamError::invalid_argument(format!(
            "a producer's attempt starts at one, not {attempt}"
        )));
    }
    Ok((producer_id, attempt))
}

/// The key a producer attempt's chain is remembered by. A Workflow writes through a new local
/// producer per flush, `__temporal_<kind>_<n>@<run id>`, and every flush of a run shares the
/// run's chain. So they share one entry, and a busy run can't push the long-lived producers out.
fn producer_key(owner: OwnerKey, producer_id: &str, attempt: i64) -> ProducerKey {
    match producer_id
        .strip_prefix("__temporal_")
        .and_then(|rest| rest.rsplit_once('@'))
    {
        Some((_, run_id)) => ProducerKey {
            owner,
            producer_id: format!("__temporal_@{run_id}"),
            attempt: 0,
        },
        None => ProducerKey {
            owner,
            producer_id: producer_id.to_string(),
            attempt,
        },
    }
}

fn check_topic(topic: &str) -> StreamResult<()> {
    temporalio_common::streams::check_topic(topic).map_err(StreamError::invalid_argument)
}

fn unix_ms() -> u64 {
    millis(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default(),
    )
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn owner_key(stream: &StreamAddress) -> OwnerKey {
    OwnerKey {
        namespace: stream.namespace.clone(),
        workflow_id: stream.workflow_id.clone(),
        run_id: stream.run_id.clone(),
    }
}

fn chain_of(stream: &StreamAddress, first_run_id: &str) -> ChainId {
    chain_of_key(&owner_key(stream), first_run_id)
}

fn chain_of_key(owner: &OwnerKey, first_run_id: &str) -> ChainId {
    ChainId {
        namespace: owner.namespace.clone(),
        workflow_id: owner.workflow_id.clone(),
        first_run_id: first_run_id.to_string(),
    }
}

fn first_run_id(owner: &OwnerKey, described: &OwnerDescription) -> StreamResult<String> {
    if described.first_run_id.is_empty() {
        // The Workflow's own output is keyed by the chain's first run, so an empty one would
        // split the stream in two without an error.
        return Err(StreamError::unsupported(format!(
            "Temporal did not report the first run id of Workflow {:?}, which streams are \
             keyed by",
            owner.workflow_id
        )));
    }
    Ok(described.first_run_id.clone())
}

fn not_found(error: OwnerError) -> StreamError {
    match error {
        OwnerError::NotFound(message) => StreamError::not_found(format!(
            "the Workflow that owns the stream was not found, and its stream is keyed by its \
             run chain: {message}"
        )),
        error => describe_failed(&error),
    }
}

fn missing(chain: &ChainId) -> StreamError {
    StreamError::storage(format!(
        "Temporal doesn't hold the Workflow {:?} that owns this stream for now, so the call \
         fails until it holds it again or answers the same after a recheck",
        chain.workflow_id
    ))
}

fn describe_failed(error: &OwnerError) -> StreamError {
    // Callers catch stream errors only, and the owner check is part of the call.
    StreamError::storage(format!(
        "Temporal could not describe the stream's owner: {error}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MemoryStore,
        proto::{
            ActivityProducer, AppendRecord, NamedProducer, StoreReadRequest, StreamFailureKind,
            StreamRecord, StreamRecordKind, append_request::Producer,
        },
    };
    use sha2::{Digest, Sha256};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use temporalio_common::protos::temporal::api::{
        common::v1::Payload, enums::v1::WorkflowExecutionStatus,
    };

    /// Temporal as the tests need it: the latest run of each Workflow id, and pinned runs.
    #[derive(Default)]
    struct Owners {
        latest: Mutex<HashMap<String, Result<OwnerDescription, OwnerError>>>,
        runs: Mutex<HashMap<String, OwnerDescription>>,
        describes: AtomicUsize,
    }

    impl Owners {
        fn run(&self, workflow_id: &str, run_id: &str, first_run_id: &str) {
            let described = OwnerDescription {
                run_id: run_id.to_string(),
                first_run_id: first_run_id.to_string(),
                status: WorkflowExecutionStatus::Running,
            };
            self.runs
                .lock()
                .unwrap()
                .insert(run_id.to_string(), described.clone());
            self.latest
                .lock()
                .unwrap()
                .insert(workflow_id.to_string(), Ok(described));
        }

        fn end(&self, workflow_id: &str, status: WorkflowExecutionStatus) {
            if let Some(Ok(latest)) = self.latest.lock().unwrap().get_mut(workflow_id) {
                latest.status = status;
            }
        }

        fn fail(&self, workflow_id: &str, error: OwnerError) {
            self.latest
                .lock()
                .unwrap()
                .insert(workflow_id.to_string(), Err(error));
        }

        fn describes(&self) -> usize {
            self.describes.load(Ordering::Relaxed)
        }
    }

    #[async_trait::async_trait]
    impl OwnerClient for Owners {
        async fn describe(
            &self,
            _namespace: &str,
            workflow_id: &str,
            run_id: &str,
        ) -> Result<OwnerDescription, OwnerError> {
            self.describes.fetch_add(1, Ordering::Relaxed);
            if !run_id.is_empty() {
                return self
                    .runs
                    .lock()
                    .unwrap()
                    .get(run_id)
                    .cloned()
                    .ok_or_else(|| OwnerError::NotFound(run_id.to_string()));
            }
            self.latest
                .lock()
                .unwrap()
                .get(workflow_id)
                .cloned()
                .unwrap_or_else(|| Err(OwnerError::NotFound(workflow_id.to_string())))
        }
    }

    struct Setup {
        store: Arc<MemoryStore>,
        owners: Arc<Owners>,
        streams: Streams,
    }

    fn setup(options: StreamsOptions) -> Setup {
        setup_on(MemoryStore::new(), options)
    }

    /// A store with no close flags and no dedupe entries.
    fn without_producer_state() -> MemoryStore {
        MemoryStore::with_capabilities(crate::Capabilities {
            producer_state: false,
            ..MemoryStore::new().capabilities()
        })
    }

    fn setup_on(store: MemoryStore, options: StreamsOptions) -> Setup {
        let store = Arc::new(store);
        let owners = Arc::new(Owners::default());
        owners.run("wf", "run-1", "run-1");
        let streams = Streams::new(store.clone(), owners.clone(), options);
        Setup {
            store,
            owners,
            streams,
        }
    }

    fn stream(topic: &str) -> StreamAddress {
        StreamAddress {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: "wf".to_string(),
            run_id: String::new(),
            topic: topic.to_string(),
        }
    }

    fn data(value: &str) -> AppendRecord {
        AppendRecord {
            kind: StreamRecordKind::Data as i32,
            body: Some(Payload {
                data: value.as_bytes().to_vec(),
                ..Default::default()
            }),
            content_hash: Sha256::digest(value.as_bytes()).to_vec(),
        }
    }

    fn append(producer: &str, sequence: i64, value: &str) -> AppendRequest {
        AppendRequest {
            stream: Some(stream("out")),
            producer: Some(Producer::Named(NamedProducer {
                producer_id: producer.to_string(),
                attempt: 1,
            })),
            sequence,
            records: vec![data(value)],
            digest: Sha256::digest(value.as_bytes()).to_vec(),
        }
    }

    fn chain(first_run_id: &str) -> ChainId {
        ChainId {
            namespace: "ns".to_string(),
            workflow_id: "wf".to_string(),
            first_run_id: first_run_id.to_string(),
        }
    }

    async fn chain_closed(store: &MemoryStore, first_run_id: &str) -> bool {
        store
            .read(StoreReadRequest {
                chain: Some(chain(first_run_id)),
                topic: "out".to_string(),
                max_records: 1,
                ..Default::default()
            })
            .await
            .unwrap()
            .closed
    }

    fn kind<T: std::fmt::Debug>(result: StreamResult<T>) -> StreamFailureKind {
        result.unwrap_err().kind
    }

    #[tokio::test]
    async fn an_append_lands_on_the_chain_of_the_latest_run() {
        let setup = setup(StreamsOptions::default());
        let landed = setup.streams.append(append("p", 1, "1")).await.unwrap();
        let read = setup
            .streams
            .read(ReadRequest {
                stream: Some(stream("out")),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(read.records.len(), 1);
        assert_eq!(read.records[0].cursor, landed.last_cursor);
        let latest = setup
            .streams
            .latest(LatestRequest {
                stream: Some(stream("out")),
            })
            .await
            .unwrap();
        assert_eq!(latest.cursor, landed.last_cursor);
        // The record landed on the chain of the first run.
        let held = setup
            .store
            .latest(StoreLatestRequest {
                chain: Some(chain("run-1")),
                topic: "out".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(held.position, "0");
    }

    #[tokio::test]
    async fn an_activity_writes_as_its_id_at_its_run() {
        let setup = setup(StreamsOptions::default());
        let mut request = append("", 1, "1");
        request.producer = Some(Producer::Activity(ActivityProducer {
            workflow_id: "wf".to_string(),
            run_id: "run-1".to_string(),
            activity_id: "fetch".to_string(),
            attempt: 2,
        }));
        setup.streams.append(request).await.unwrap();
        let read = setup
            .streams
            .read(ReadRequest {
                stream: Some(stream("out")),
                ..Default::default()
            })
            .await
            .unwrap();
        let Some(crate::proto::read_record::Record::Stored(stored)) = &read.records[0].record
        else {
            panic!("no record");
        };
        assert_eq!(
            (stored.producer_id.as_str(), stored.attempt),
            ("fetch@run-1", 2)
        );
    }

    #[tokio::test]
    async fn requests_no_store_keeps_are_refused() {
        let setup = setup(StreamsOptions::default());
        let cases: Vec<(AppendRequest, StreamFailureKind)> = vec![
            (append("", 1, "1"), StreamFailureKind::InvalidArgument),
            (
                AppendRequest {
                    producer: Some(Producer::Named(NamedProducer {
                        producer_id: "p".to_string(),
                        attempt: 0,
                    })),
                    ..append("p", 1, "1")
                },
                StreamFailureKind::InvalidArgument,
            ),
            (append("p", 0, "1"), StreamFailureKind::InvalidArgument),
            (
                AppendRequest {
                    records: Vec::new(),
                    ..append("p", 1, "1")
                },
                StreamFailureKind::InvalidArgument,
            ),
            (
                AppendRequest {
                    producer: None,
                    ..append("p", 1, "1")
                },
                StreamFailureKind::InvalidArgument,
            ),
        ];
        for (request, expected) in cases {
            assert_eq!(kind(setup.streams.append(request).await), expected);
        }
        for topic in [
            "",
            "a\u{1f}b",
            &"t".repeat(temporalio_common::streams::MAX_TOPIC_BYTES + 1),
        ] {
            let request = AppendRequest {
                stream: Some(stream(topic)),
                ..append("p", 1, "1")
            };
            assert_eq!(
                kind(setup.streams.append(request).await),
                StreamFailureKind::InvalidArgument,
                "{topic:?}"
            );
        }
        // A topic counts bytes, so 256 two-byte characters are too many.
        assert!(check_topic(&"é".repeat(128)).is_ok());
        assert!(check_topic(&"é".repeat(129)).is_err());
        let mut other_kind = stream("out");
        other_kind.owner_kind = StreamOwnerKind::Unspecified as i32;
        assert_eq!(
            kind(
                setup
                    .streams
                    .latest(LatestRequest {
                        stream: Some(other_kind)
                    })
                    .await
            ),
            StreamFailureKind::Unsupported
        );
        assert_eq!(setup.owners.describes(), 0);
    }

    #[tokio::test]
    async fn a_stream_of_a_missing_workflow_is_not_found() {
        let setup = setup(StreamsOptions::default());
        let mut request = append("p", 1, "1");
        request.stream.as_mut().unwrap().workflow_id = "missing".to_string();
        assert_eq!(
            kind(setup.streams.append(request).await),
            StreamFailureKind::NotFound
        );
    }

    #[tokio::test]
    async fn rule_17_1_an_empty_first_run_id_is_unsupported() {
        let setup = setup(StreamsOptions::default());
        setup.owners.run("wf", "run-1", "");
        let error = setup
            .streams
            .latest(LatestRequest {
                stream: Some(stream("out")),
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Unsupported);
        assert!(error.message.contains("first run id"), "{error}");
    }

    #[tokio::test]
    async fn rule_17_2_a_describe_failure_arrives_as_a_storage_failure() {
        let setup = setup(StreamsOptions::default());
        setup
            .owners
            .fail("wf", OwnerError::Failed("unavailable".to_string()));
        let error = setup
            .streams
            .latest(LatestRequest {
                stream: Some(stream("out")),
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Storage);
        assert!(error.message.contains("unavailable"), "{error}");
        assert_eq!(
            kind(setup.streams.append(append("p", 1, "1")).await),
            StreamFailureKind::Storage
        );
    }

    #[tokio::test(start_paused = true)]
    async fn rule_19_1_a_long_lived_producer_rechecks_its_owner_within_a_minute() {
        // Nothing marks the chain of a Workflow that never published itself, so only the
        // producer's own recheck ends its writes.
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.owners.end("wf", WorkflowExecutionStatus::Terminated);
        tokio::time::advance(Duration::from_secs(59)).await;
        setup.streams.append(append("p", 2, "2")).await.unwrap();
        assert_eq!(setup.owners.describes(), 1);
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(
            kind(setup.streams.append(append("p", 3, "3")).await),
            StreamFailureKind::Closed
        );
        assert!(chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test(start_paused = true)]
    async fn a_producer_rechecks_its_owner_once_retention_passed() {
        // The close mark expires with the stream, so a producer can't wait longer than that.
        let setup = setup(StreamsOptions::for_retention(Duration::from_millis(300)));
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.owners.end("wf", WorkflowExecutionStatus::Completed);
        tokio::time::advance(Duration::from_millis(301)).await;
        assert_eq!(
            kind(setup.streams.append(append("p", 2, "2")).await),
            StreamFailureKind::Closed
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_producer_closes_an_ended_chain_the_worker_missed() {
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.owners.end("wf", WorkflowExecutionStatus::Terminated);
        tokio::time::advance(OWNER_RECHECK + Duration::from_secs(1)).await;
        assert!(!chain_closed(&setup.store, "run-1").await);
        assert_eq!(
            kind(setup.streams.append(append("q", 1, "2")).await),
            StreamFailureKind::Closed
        );
        assert!(chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test]
    async fn the_producers_of_one_owner_share_its_check() {
        // A Workflow that writes each flush as a new producer asks about its owner once.
        let setup = setup(StreamsOptions::default());
        for producer in 0..50 {
            let request = append(&format!("__temporal_local_{producer}@run-1"), 1, "x");
            setup.streams.append(request).await.unwrap();
        }
        assert_eq!(setup.owners.describes(), 1);
    }

    #[tokio::test]
    async fn a_producer_pinned_to_a_running_run_asks_once() {
        let setup = setup(StreamsOptions::default());
        setup.owners.run("wf", "run-2", "run-1");
        for producer in ["p", "q"] {
            let mut request = append(producer, 1, "1");
            request.stream.as_mut().unwrap().run_id = "run-2".to_string();
            setup.streams.append(request).await.unwrap();
        }
        assert_eq!(setup.owners.describes(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_producer_whose_workflow_id_was_reused_is_refused() {
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        // A new chain under the same id: the old chain is over even though the id's latest run
        // is running.
        setup.owners.run("wf", "run-9", "run-9");
        tokio::time::advance(OWNER_RECHECK + Duration::from_secs(1)).await;
        assert_eq!(
            kind(setup.streams.append(append("p", 2, "2")).await),
            StreamFailureKind::Closed
        );
        assert!(chain_closed(&setup.store, "run-1").await);
        // A producer that starts now writes on the new chain.
        setup.streams.append(append("q", 1, "3")).await.unwrap();
        assert!(!chain_closed(&setup.store, "run-9").await);
    }

    #[tokio::test(start_paused = true)]
    async fn rule_19_2_an_owner_history_no_longer_holds_closes_the_stream() {
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup
            .owners
            .fail("wf", OwnerError::NotFound("workflow not found".to_string()));
        tokio::time::advance(OWNER_RECHECK + Duration::from_secs(1)).await;
        // The first answer may be replication lag after a failover, so it closes nothing.
        let error = setup.streams.append(append("p", 2, "2")).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Storage);
        assert!(!chain_closed(&setup.store, "run-1").await);
        tokio::time::advance(OWNER_RECHECK).await;
        let error = setup.streams.append(append("p", 2, "2")).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Closed);
        assert!(error.message.contains("gone from History"), "{error}");
        assert!(chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test]
    async fn a_workflows_local_producers_share_one_memory_entry() {
        let setup = setup(StreamsOptions {
            remembered: 2,
            ..StreamsOptions::default()
        });
        setup
            .streams
            .append(append("long-lived", 1, "1"))
            .await
            .unwrap();
        for flush in 1..=20 {
            let producer = format!("__temporal_local_{flush}@run-1");
            setup
                .streams
                .append(append(&producer, 1, "x"))
                .await
                .unwrap();
        }
        let key = producer_key(owner_key(&stream("out")), "long-lived", 1);
        assert!(setup.streams.producers.get(&key).is_some());
    }

    #[tokio::test]
    async fn a_named_producer_can_not_write_as_a_workflow_run() {
        let setup = setup(StreamsOptions::default());
        let error = setup
            .streams
            .append(append("@run-1", 1, "1"))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::InvalidArgument);
        assert!(error.message.contains("@run-1"), "{error}");
    }

    #[tokio::test(start_paused = true)]
    async fn one_not_found_from_the_owner_check_closes_nothing() {
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup
            .owners
            .fail("wf", OwnerError::NotFound("workflow not found".to_string()));
        tokio::time::advance(OWNER_RECHECK + Duration::from_secs(1)).await;
        let error = setup.streams.append(append("p", 2, "2")).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Storage);
        // Replication caught up, and the owner is back.
        setup.owners.run("wf", "run-1", "run-1");
        setup.streams.append(append("p", 2, "2")).await.unwrap();
        assert!(!chain_closed(&setup.store, "run-1").await);
        // A later lone `NotFound` starts the grace over.
        setup
            .owners
            .fail("wf", OwnerError::NotFound("workflow not found".to_string()));
        tokio::time::advance(OWNER_RECHECK + Duration::from_secs(1)).await;
        let error = setup.streams.append(append("p", 3, "3")).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Storage);
        assert!(!chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_memory_forgets_its_oldest_entry_only() {
        let remembered = Remembered::new(3);
        for key in 0..4 {
            remembered.put(key, "run-1", Duration::from_secs(3600));
            tokio::time::advance(Duration::from_millis(1)).await;
        }
        assert!(remembered.get(&0).is_none());
        for key in 1..4 {
            assert!(remembered.get(&key).is_some(), "{key}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn rule_19_2_an_owner_check_that_fails_is_a_storage_failure() {
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup
            .owners
            .fail("wf", OwnerError::Failed("unavailable".to_string()));
        tokio::time::advance(OWNER_RECHECK + Duration::from_secs(1)).await;
        let error = setup.streams.append(append("p", 2, "2")).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Storage);
        assert!(error.message.contains("unavailable"), "{error}");
        assert!(!chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test(start_paused = true)]
    async fn a_later_run_of_the_chain_keeps_producers_writing() {
        // A retried run, a cron run and a run that continued as new all keep the chain's first
        // run, so the chain is still open.
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.owners.run("wf", "run-2", "run-1");
        tokio::time::advance(OWNER_RECHECK + Duration::from_secs(1)).await;
        setup.streams.append(append("p", 2, "2")).await.unwrap();
        assert!(!chain_closed(&setup.store, "run-1").await);
    }

    #[test]
    fn only_a_closed_run_that_did_not_continue_ends_its_chain() {
        use WorkflowExecutionStatus::*;
        let ended = |status| {
            OwnerDescription {
                run_id: String::new(),
                first_run_id: String::new(),
                status,
            }
            .chain_ended()
        };
        for status in [Completed, Failed, Canceled, Terminated, TimedOut] {
            assert!(ended(status), "{status:?}");
        }
        for status in [Running, ContinuedAsNew, Unspecified] {
            assert!(!ended(status), "{status:?}");
        }
    }

    #[tokio::test]
    async fn a_producer_pinned_to_a_run_writes_on_that_runs_chain() {
        let setup = setup(StreamsOptions::default());
        setup.owners.run("wf", "run-2", "run-1");
        let mut request = append("p", 1, "1");
        request.stream.as_mut().unwrap().run_id = "run-2".to_string();
        setup.streams.append(request).await.unwrap();
        let held = setup
            .store
            .latest(StoreLatestRequest {
                chain: Some(chain("run-1")),
                topic: "out".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(held.position, "0");
    }

    #[tokio::test]
    async fn a_read_resolves_its_chain_once() {
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let before = setup.owners.describes();
        let first = setup
            .streams
            .read(ReadRequest {
                stream: Some(stream("out")),
                max_records: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        setup.streams.append(append("p", 2, "2")).await.unwrap();
        let next = setup
            .streams
            .read(ReadRequest {
                stream: Some(stream("out")),
                after: first.cursor.clone(),
                state: first.state.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(next.records.len(), 1);
        assert!(setup.owners.describes() <= before + 1);
    }

    #[tokio::test]
    async fn a_closed_stream_refuses_new_batches_but_answers_a_repeat() {
        let setup = setup(StreamsOptions::default());
        let landed = setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup
            .streams
            .close(CloseRequest {
                stream: Some(stream("out")),
            })
            .await
            .unwrap();
        assert_eq!(
            setup.streams.append(append("p", 1, "1")).await.unwrap(),
            landed
        );
        assert_eq!(
            kind(setup.streams.append(append("p", 2, "2")).await),
            StreamFailureKind::Closed
        );
        let mut other = append("p", 1, "1");
        other.stream.as_mut().unwrap().topic = "other".to_string();
        setup.streams.append(other).await.unwrap();
    }

    async fn last_kind(store: &MemoryStore, topic: &str) -> (i32, String) {
        let page = store
            .read(StoreReadRequest {
                chain: Some(chain("run-1")),
                topic: topic.to_string(),
                max_records: 100,
                ..Default::default()
            })
            .await
            .unwrap();
        let record = StreamRecord::decode(&*page.records.last().unwrap().record).unwrap();
        (record.kind, record.producer_id)
    }

    #[tokio::test]
    async fn without_close_flags_a_close_is_a_record_that_ends_every_read() {
        let setup = setup_on(without_producer_state(), quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup
            .streams
            .close(CloseRequest {
                stream: Some(stream("out")),
            })
            .await
            .unwrap();
        assert_eq!(
            last_kind(&setup.store, "out").await,
            (StreamRecordKind::Close as i32, String::new())
        );
        // The store can't refuse it, so it lands where no reader delivers it, and so does a
        // repeat of the batch that landed before the close.
        setup.streams.append(append("p", 2, "2")).await.unwrap();
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let before = setup.owners.describes();
        let (delivered, _) = read_to_end(&setup.streams, Duration::from_secs(5)).await;
        assert_eq!(delivered, 1);
        assert_eq!(setup.owners.describes(), before);
    }

    #[tokio::test]
    async fn a_batch_above_the_stores_limits_is_refused_before_it_lands() {
        let store = MemoryStore::with_capabilities(crate::Capabilities {
            max_record_bytes: 200,
            max_batch_bytes: 300,
            ..MemoryStore::new().capabilities()
        });
        let setup = setup_on(store, StreamsOptions::default());
        let error = setup
            .streams
            .append(append("p", 1, &"x".repeat(300)))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::InvalidArgument);
        assert!(error.message.contains("record limit of 200"), "{error}");
        let mut batch = append("p", 1, "small");
        batch.records = vec![data("a"), data("b"), data("c")];
        let error = setup.streams.append(batch).await.unwrap_err();
        assert!(error.message.contains("batch limit of 300"), "{error}");
        setup.streams.append(append("p", 1, "fits")).await.unwrap();
    }

    #[tokio::test]
    async fn without_producer_state_a_retire_does_nothing() {
        let setup = setup_on(without_producer_state(), StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup
            .streams
            .retire_producer(RetireProducerRequest {
                stream: Some(stream("out")),
                producer: Some(retire_producer_request::Producer::Named(NamedProducer {
                    producer_id: "p".to_string(),
                    attempt: 1,
                })),
            })
            .await
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn without_close_flags_a_producer_that_sees_its_owner_end_closes_its_topic() {
        let setup = setup_on(without_producer_state(), StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.owners.end("wf", WorkflowExecutionStatus::Terminated);
        tokio::time::advance(OWNER_RECHECK + Duration::from_secs(1)).await;
        assert_eq!(
            kind(setup.streams.append(append("p", 2, "2")).await),
            StreamFailureKind::Closed
        );
        assert_eq!(
            last_kind(&setup.store, "out").await,
            (StreamRecordKind::Close as i32, "p".to_string())
        );
    }

    #[tokio::test]
    async fn a_retire_drops_the_attempts_state_on_the_chain_it_wrote() {
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup
            .streams
            .close(CloseRequest {
                stream: Some(stream("out")),
            })
            .await
            .unwrap();
        let before = setup.owners.describes();
        let retire = |producer: Option<retire_producer_request::Producer>| RetireProducerRequest {
            stream: Some(stream("out")),
            producer,
        };
        let named = |attempt| {
            Some(retire_producer_request::Producer::Named(NamedProducer {
                producer_id: "p".to_string(),
                attempt,
            }))
        };
        // A closed stream refuses no retire, and a repeat answers the same.
        for _ in 0..2 {
            setup
                .streams
                .retire_producer(retire(named(1)))
                .await
                .unwrap();
        }
        // The append resolved the chain, so the retire asks Temporal nothing.
        assert_eq!(setup.owners.describes(), before);
        // With the state gone, the repeat is a new batch, which the closed stream refuses.
        assert_eq!(
            kind(setup.streams.append(append("p", 1, "1")).await),
            StreamFailureKind::Closed
        );
        for bad in [retire(None), retire(named(0))] {
            assert_eq!(
                kind(setup.streams.retire_producer(bad).await),
                StreamFailureKind::InvalidArgument
            );
        }
    }

    fn quick() -> StreamsOptions {
        StreamsOptions {
            owner_check_min: Duration::from_millis(20),
            owner_check_max: Duration::from_millis(80),
            ..StreamsOptions::default()
        }
    }

    fn read_after(after: &str, state: &[u8], wait: Duration) -> ReadRequest {
        ReadRequest {
            stream: Some(stream("out")),
            after: after.to_string(),
            state: state.to_vec(),
            wait: Some(wait.try_into().unwrap()),
            ..Default::default()
        }
    }

    /// A setup whose Workflow `wf` has not started yet, with an owner start grace of `grace`.
    fn before_owner_starts(grace: Duration) -> Setup {
        let store = Arc::new(MemoryStore::new());
        let owners = Arc::new(Owners::default());
        let options = StreamsOptions {
            owner_start_grace: grace,
            ..quick()
        };
        let streams = Streams::new(store.clone(), owners.clone(), options);
        Setup {
            store,
            owners,
            streams,
        }
    }

    #[tokio::test]
    async fn a_read_before_its_owner_starts_answers_empty_until_the_owner_writes() {
        let setup = before_owner_starts(Duration::from_secs(60));
        let empty = setup
            .streams
            .read(read_after(END, &[], Duration::ZERO))
            .await
            .unwrap();
        assert!(empty.records.is_empty() && !empty.done);
        assert_eq!(
            empty.cursor, BEGINNING,
            "the end of a stream that holds nothing is its start"
        );
        setup.owners.run("wf", "run-1", "run-1");
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let read = setup
            .streams
            .read(read_after(&empty.cursor, &empty.state, Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(read.records.len(), 1, "the first record is not missed");
    }

    #[tokio::test]
    async fn a_waiting_read_gets_the_first_record_of_an_owner_that_starts_later() {
        let setup = before_owner_starts(Duration::from_secs(60));
        let owners = setup.owners.clone();
        let streams = Arc::new(setup.streams);
        let writer = streams.clone();
        let late_start = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            owners.run("wf", "run-1", "run-1");
            writer.append(append("p", 1, "1")).await.unwrap();
        });
        let read = streams
            .read(read_after("", &[], Duration::from_secs(5)))
            .await
            .unwrap();
        late_start.await.unwrap();
        assert_eq!(read.records.len(), 1);
    }

    #[tokio::test]
    async fn a_read_of_an_owner_that_never_starts_is_not_found_after_the_grace() {
        let setup = before_owner_starts(Duration::from_millis(200));
        let empty = setup
            .streams
            .read(read_after("", &[], Duration::from_millis(50)))
            .await
            .unwrap();
        assert!(empty.records.is_empty());
        let error = setup
            .streams
            .read(read_after("", &empty.state, Duration::from_secs(5)))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::NotFound);
        // The grace counts from the read's first owner check, so a new read waits again.
        assert!(
            setup
                .streams
                .read(read_after("", &[], Duration::ZERO))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn no_grace_fails_a_read_of_a_missing_owner_at_once() {
        let setup = before_owner_starts(Duration::ZERO);
        let error = setup
            .streams
            .read(read_after("", &[], Duration::from_secs(5)))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::NotFound);
    }

    /// Reads until the read says it is done, or fails after `limit` calls.
    async fn read_to_end(streams: &Streams, wait: Duration) -> (usize, ReadResponse) {
        let mut delivered = 0;
        let mut last = read_after("", &[], wait);
        for _ in 0..50 {
            let page = streams.read(last.clone()).await.unwrap();
            delivered += page.records.len();
            if page.done {
                return (delivered, page);
            }
            last = read_after(&page.cursor, &page.state, wait);
        }
        panic!("the read never ended");
    }

    #[tokio::test]
    async fn a_read_ends_when_its_owner_chain_ends_and_writes_nothing() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.owners.end("wf", WorkflowExecutionStatus::Completed);
        let (delivered, end) = read_to_end(&setup.streams, Duration::from_secs(5)).await;
        assert_eq!(delivered, 1);
        assert!(end.records.is_empty());
        // A reader needs no write access, so the producers' owner check closes the chain.
        assert!(!chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test]
    async fn a_read_follows_a_chain_that_continued_as_new() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup
            .owners
            .end("wf", WorkflowExecutionStatus::ContinuedAsNew);
        setup.owners.run("wf", "run-2", "run-1");
        let page = setup
            .streams
            .read(read_after("", &[], Duration::from_millis(200)))
            .await
            .unwrap();
        let idle = setup
            .streams
            .read(read_after(
                &page.cursor,
                &page.state,
                Duration::from_millis(200),
            ))
            .await
            .unwrap();
        assert!(!idle.done);
        assert!(!chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test]
    async fn a_read_pinned_to_a_run_ends_when_that_run_continues() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup
            .owners
            .runs
            .lock()
            .unwrap()
            .get_mut("run-1")
            .unwrap()
            .status = WorkflowExecutionStatus::ContinuedAsNew;
        setup.owners.run("wf", "run-2", "run-1");
        let mut request = read_after("", &[], Duration::from_secs(5));
        request.stream.as_mut().unwrap().run_id = "run-1".to_string();
        let mut done = false;
        for _ in 0..10 {
            let page = setup.streams.read(request.clone()).await.unwrap();
            if page.done {
                done = true;
                break;
            }
            request.after = page.cursor;
            request.state = page.state;
        }
        assert!(done);
        // It stopped with the first run, though the chain still runs.
        assert!(!chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test]
    async fn a_read_ends_when_a_new_chain_reuses_the_workflow_id() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let first = setup
            .streams
            .read(read_after("", &[], Duration::ZERO))
            .await
            .unwrap();
        // The id's latest run is running, but it belongs to another chain.
        setup.owners.run("wf", "run-9", "run-9");
        let mut request = read_after(&first.cursor, &first.state, Duration::from_secs(5));
        let mut done = false;
        for _ in 0..10 {
            let page = setup.streams.read(request.clone()).await.unwrap();
            assert!(page.records.is_empty());
            if page.done {
                done = true;
                break;
            }
            request = read_after(&page.cursor, &page.state, Duration::from_secs(5));
        }
        assert!(done);
        assert!(!chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test]
    async fn rule_20_3_a_read_ends_when_history_no_longer_holds_its_owner() {
        let setup = setup(StreamsOptions {
            owner_recheck: Duration::from_millis(200),
            ..quick()
        });
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let first = setup
            .streams
            .read(read_after("", &[], Duration::ZERO))
            .await
            .unwrap();
        setup
            .owners
            .fail("wf", OwnerError::NotFound("workflow not found".to_string()));
        let page = setup
            .streams
            .read(read_after(
                &first.cursor,
                &first.state,
                Duration::from_secs(5),
            ))
            .await
            .unwrap();
        assert!(page.done);
        // The reader's check ends its own read only. A producer's check closes the chain.
        assert!(!chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test]
    async fn a_read_goes_on_after_one_not_found_from_its_owner_check() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let first = setup
            .streams
            .read(read_after("", &[], Duration::ZERO))
            .await
            .unwrap();
        setup
            .owners
            .fail("wf", OwnerError::NotFound("workflow not found".to_string()));
        let page = setup
            .streams
            .read(read_after(
                &first.cursor,
                &first.state,
                Duration::from_millis(300),
            ))
            .await
            .unwrap();
        assert!(!page.done);
        setup.owners.run("wf", "run-1", "run-1");
        setup.streams.append(append("p", 2, "2")).await.unwrap();
        let page = setup
            .streams
            .read(read_after(
                &page.cursor,
                &page.state,
                Duration::from_secs(5),
            ))
            .await
            .unwrap();
        assert_eq!(page.records.len(), 1);
        assert!(!chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test]
    async fn rule_17_2_an_owner_check_during_a_read_that_fails_is_a_storage_failure() {
        let setup = setup(quick());
        let first = setup
            .streams
            .read(read_after("", &[], Duration::ZERO))
            .await
            .unwrap();
        setup
            .owners
            .fail("wf", OwnerError::Failed("unavailable".to_string()));
        let error = setup
            .streams
            .read(read_after(
                &first.cursor,
                &first.state,
                Duration::from_secs(5),
            ))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Storage);
        assert!(error.message.contains("unavailable"), "{error}");
    }

    #[tokio::test]
    async fn an_idle_read_backs_off_its_owner_checks() {
        let setup = setup(quick());
        let first = setup
            .streams
            .read(read_after("", &[], Duration::ZERO))
            .await
            .unwrap();
        let before = setup.owners.describes();
        let idle = setup
            .streams
            .read(read_after(
                &first.cursor,
                &first.state,
                Duration::from_secs(1),
            ))
            .await
            .unwrap();
        // Fixed checks every 20 ms would be about 50, and doubling to 80 ms is about 15.
        let checks = setup.owners.describes() - before;
        assert!((8..=20).contains(&checks), "{checks} checks");
        let state = ReadState::decode_from(&idle.state).unwrap();
        assert_eq!(state.check_interval_ms, 80);
        // A record starts the interval over.
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let page = setup
            .streams
            .read(read_after(
                &idle.cursor,
                &idle.state,
                Duration::from_secs(1),
            ))
            .await
            .unwrap();
        assert_eq!(page.records.len(), 1);
        let state = ReadState::decode_from(&page.state).unwrap();
        assert_eq!(state.check_interval_ms, 20);
    }

    #[tokio::test]
    async fn a_closed_stream_ends_its_reads_without_asking_temporal() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup
            .streams
            .close(CloseRequest {
                stream: Some(stream("out")),
            })
            .await
            .unwrap();
        let before = setup.owners.describes();
        let (delivered, _) = read_to_end(&setup.streams, Duration::from_secs(5)).await;
        assert_eq!(delivered, 1);
        assert_eq!(setup.owners.describes(), before);
    }

    #[tokio::test]
    async fn an_owner_delete_asks_temporal_nothing() {
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let before = setup.owners.describes();
        let request = DeleteOwnerRequest {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: "wf".to_string(),
        };
        assert!(
            setup
                .streams
                .delete_owner(request.clone())
                .await
                .unwrap()
                .deleted
                > 0
        );
        assert_eq!(setup.owners.describes(), before);
        for (field, expected) in [
            ("namespace", StreamFailureKind::InvalidArgument),
            ("workflow_id", StreamFailureKind::InvalidArgument),
            ("owner_kind", StreamFailureKind::Unsupported),
        ] {
            let mut bad = request.clone();
            match field {
                "namespace" => bad.namespace.clear(),
                "workflow_id" => bad.workflow_id.clear(),
                _ => bad.owner_kind = StreamOwnerKind::Unspecified as i32,
            }
            assert_eq!(
                kind(setup.streams.delete_owner(bad).await),
                expected,
                "{field}"
            );
        }
    }

    /// The digest the Python SDK's Redis provider took over `[{"n": 1}, {"n": 2}]` from producer
    /// `p`, attempt 1, sequence 1.
    const PYTHON_DIGEST: &str = "0499768af1856bc6fd19cf6cf90e28e971099f1680e8347d70bdb4b65273f30f";

    fn from_hex(text: &str) -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn a_retry_carrying_langs_digest_dedupes_and_the_store_keeps_it_unchanged() {
        let setup = setup(StreamsOptions::default());
        let mut request = append("p", 1, "1");
        request.records.push(data("2"));
        request.digest = from_hex(PYTHON_DIGEST);
        let first = setup.streams.append(request.clone()).await.unwrap();
        // A retry whose codec made other ciphertext still carries the same digest.
        let mut retry = request.clone();
        retry.records[0].body.as_mut().unwrap().data = b"other ciphertext".to_vec();
        assert_eq!(setup.streams.append(retry).await.unwrap(), first);
        // The store compares the bytes lang sent, so a store-level retry with them dedupes too.
        let landed = setup
            .store
            .append(StoreAppendRequest {
                chain: Some(chain("run-1")),
                topic: "out".to_string(),
                producer_id: "p".to_string(),
                attempt: 1,
                sequence: 1,
                digest: from_hex(PYTHON_DIGEST),
                records: vec![b"\x1a\x03out".to_vec()],
            })
            .await
            .unwrap();
        assert_eq!(landed.first_position, "0");
        let mut divergent = request.clone();
        divergent.digest = vec![7; 32];
        assert_eq!(
            kind(setup.streams.append(divergent).await),
            StreamFailureKind::ProducerDivergent
        );
    }

    #[tokio::test]
    async fn an_append_without_langs_digest_is_refused_before_any_call() {
        let setup = setup(StreamsOptions::default());
        for digest in [Vec::new(), vec![1; 16]] {
            let mut request = append("p", 1, "1");
            request.digest = digest;
            assert_eq!(
                kind(setup.streams.append(request).await),
                StreamFailureKind::InvalidArgument
            );
        }
        assert_eq!(setup.owners.describes(), 0);
    }
}
