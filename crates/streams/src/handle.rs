//! The stream calls lang makes, from the service protos down to a store.
//!
//! [Streams] resolves a stream's address to the run chain its store keys it by, checks the owner
//! where the contract says so, and mints and checks cursors. Stores only move records.

use crate::{
    BEGINNING, END, MAX_CLOSE_BYTES, ReadTarget, StreamError, StreamResult, StreamStore,
    activity_attempt, activity_producer_id, append_batch, chain_owner, check_append_digest,
    check_store_limits, close_topic, cursor_position, mint_cursor,
    owner::{OwnerClient, OwnerDescription, OwnerError, OwnerId, OwnerStatus},
    proto::{
        ActivityProducer, AppendRequest, AppendResponse, ChainId, CloseRequest, CloseResponse,
        DeleteOwnerRequest, DeleteOwnerResponse, LatestRequest, LatestResponse, NamedProducer,
        ReadRangeRequest, ReadRequest, ReadResponse, ReleaseFloorRequest, ReleaseFloorResponse,
        RetireProducerRequest, RetireProducerResponse, StoreAppendRequest, StoreLatestRequest,
        StoredClose, StreamAddress, StreamFailureKind, StreamOwnerKind, append_request,
        read_record, retire_producer_request,
    },
    reader::{ReadState, read_bounded, read_with},
    stored_append_record, stream_hash,
};
use prost::Message;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;

/// The longest `floor_holder` a read takes.
const MAX_FLOOR_HOLDER_BYTES: usize = 512;

/// A reader that named itself as the archive could end the archive's hold.
fn refuse_archive_holder(holder: &str) -> StreamResult<()> {
    if holder == crate::ARCHIVE_HOLDER {
        return Err(StreamError::invalid_argument(format!(
            "the floor holder {holder:?} is the archive's"
        )));
    }
    Ok(())
}

/// The longest `closed_by` a close takes.
const MAX_CLOSED_BY_BYTES: usize = 256;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Workflow,
    Activity,
    WorkflowActivity,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OwnerKey {
    namespace: String,
    kind: Kind,
    /// The Workflow id, the standalone Activity id, or the Workflow that scheduled a Workflow
    /// Activity.
    id: String,
    /// A Workflow Activity's id. Empty for other kinds.
    activity_id: String,
    /// The pinned run. A Workflow Activity always names its scheduling run.
    run_id: String,
}

impl OwnerKey {
    fn owner(&self) -> OwnerId<'_> {
        match self.kind {
            Kind::Workflow => OwnerId::Workflow(&self.id),
            Kind::Activity => OwnerId::Activity(&self.id),
            Kind::WorkflowActivity => OwnerId::WorkflowActivity {
                workflow_id: &self.id,
                activity_id: &self.activity_id,
            },
        }
    }

    /// The owner as error texts name it.
    fn label(&self) -> String {
        match self.kind {
            Kind::Workflow => format!("Workflow {:?}", self.id),
            Kind::Activity => format!("Activity {:?}", self.id),
            Kind::WorkflowActivity => format!(
                "Activity {:?} of run {:?} of Workflow {:?}",
                self.activity_id, self.run_id, self.id
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProducerKey {
    owner: OwnerKey,
    producer_id: String,
    attempt: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct Checked {
    pub(crate) first_run_id: String,
    at: Instant,
}

/// Remembers when each key was last checked, forgetting the oldest when full.
#[derive(Debug)]
pub(crate) struct Remembered<K> {
    entries: Mutex<HashMap<K, Checked>>,
    limit: usize,
}

impl<K: std::hash::Hash + Eq + Clone> Remembered<K> {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            entries: Mutex::default(),
            limit: limit.max(1),
        }
    }

    pub(crate) fn get(&self, key: &K) -> Option<Checked> {
        self.entries.lock().unwrap().get(key).cloned()
    }

    pub(crate) fn forget(&self, key: &K) {
        self.entries.lock().unwrap().remove(key);
    }

    pub(crate) fn put(&self, key: K, first_run_id: &str, fresh_for: Duration) {
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
    /// whether the owner's chain ended. All producers of the owner share that check. If it did, or
    /// Temporal no longer holds the owner, or a new chain took the owner id, the producer closes
    /// the chain, or its own topic on a store without close flags, and the batch is refused as
    /// closed. A standalone Activity run is a chain of one run, so a new run of the Activity id
    /// ends the earlier run's streams.
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
    /// continued as new. An empty answer carries the notification counter of its position as
    /// `wake_after`. A read whose owner Temporal doesn't know yet answers empty within
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
        if !request.floor_holder.is_empty() {
            self.hold_floor(&target, &request.after, &request.floor_holder)
                .await?;
        }
        let minimum = self.options.owner_check_min;
        let mut page_request = request.clone();
        let mut topic_close = None;
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
            let (mut page, closed, close) =
                read_with(&*self.store, &target, &page_request, &mut state).await?;
            if close.is_some() {
                topic_close = close;
            }
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
                page.wake_after = 0;
                // A reader can reach the end before a close wake does, so the end carries what the
                // stream closed with.
                if let Some(close) = topic_close.take() {
                    page.close_result = close.result;
                    page.close_failure = close.failure;
                    page.closed_by = close.closed_by;
                }
                break page;
            }
            if unix_ms() >= state.next_check_ms {
                state.owner_ended = self.owner_ended(stream, &target.chain, &mut state).await?;
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

    /// Holds the records after `after` against retention for `holder`, a positions-only reader.
    /// A store that can't hold serves the reader only when it declares a replay horizon, and
    /// refuses it otherwise, so the read fails at once and not at replay.
    async fn hold_floor(&self, target: &ReadTarget, after: &str, holder: &str) -> StreamResult<()> {
        let capabilities = self.store.capabilities();
        if !capabilities.holds {
            if capabilities.replay_horizon.is_some() {
                return Ok(());
            }
            return Err(StreamError::unsupported(
                "a positions-only read needs a store that holds records for a replay or declares \
                 a replay horizon, and this store does neither",
            ));
        }
        if holder.len() > MAX_FLOOR_HOLDER_BYTES {
            return Err(StreamError::invalid_argument(format!(
                "a floor holder is at most {MAX_FLOOR_HOLDER_BYTES} bytes, not {}",
                holder.len()
            )));
        }
        refuse_archive_holder(holder)?;
        let position = if after == END {
            self.store
                .latest(StoreLatestRequest {
                    chain: Some(target.chain.clone()),
                    topic: target.topic.clone(),
                })
                .await?
                .position
        } else {
            cursor_position(after, self.store.name(), &target.stream_hash)?
                .unwrap_or_default()
                .to_string()
        };
        self.store
            .hold_floor(&target.chain, &target.topic, holder, &position)
            .await
    }

    /// Reads again the exact range of an earlier answer and checks it against that answer's digest
    /// (RR-2). It runs the read that the answer came from, with the same input state and the start
    /// that read found, so it gives the same supersessions and stale marks, and it waits for
    /// nothing and checks no owner. Records with other content fail as a mismatch. Records the
    /// store no longer holds fail as lost (REC-01).
    pub async fn read_range(&self, request: ReadRangeRequest) -> StreamResult<ReadResponse> {
        let stream = address(request.stream.as_ref())?;
        if request.chain_first_run_id.is_empty() {
            return Err(StreamError::invalid_argument(
                "a replay read needs the chain its read read",
            ));
        }
        if request.after == END {
            return Err(StreamError::invalid_argument(
                "a replay read starts at the read's resolved start_cursor, not at $end",
            ));
        }
        let mut state = ReadState::decode_from(&request.state)?;
        if state.first_run_id.is_empty() {
            state.first_run_id = request.chain_first_run_id.clone();
        }
        if request.start_from_end {
            state.started = true;
        }
        let target = ReadTarget {
            chain: chain_of(stream, &request.chain_first_run_id),
            topic: stream.topic.clone(),
            stream_hash: self.hash(stream),
        };
        if !request.floor_holder.is_empty() {
            self.hold_floor(&target, &request.after, &request.floor_holder)
                .await?;
        }
        let expected = request.stored_records as usize;
        let lost = |message: String| {
            StreamError::new(
                StreamFailureKind::ReplayRangeLost,
                format!("REC-01: {message}"),
            )
        };
        let mismatch =
            |message: String| StreamError::new(StreamFailureKind::ReplayMismatch, message);
        let page = if expected == 0 {
            ReadResponse {
                cursor: request.after.clone(),
                records_digest: crate::reader::records_digest(&[]),
                chain_first_run_id: request.chain_first_run_id.clone(),
                start_cursor: request.after.clone(),
                ..Default::default()
            }
        } else {
            // Up to the recorded end, not a record count, since copies and control records the
            // read dropped can put the store's page edges elsewhere.
            let mut records = vec![];
            let mut after = request.after.clone();
            let mut start_cursor = None;
            loop {
                let page_request = ReadRequest {
                    stream: Some(stream.clone()),
                    after: after.clone(),
                    max_records: request.stored_records.max(1),
                    ..Default::default()
                };
                let page = match read_bounded(
                    &*self.store,
                    &target,
                    &page_request,
                    &mut state,
                    Some(&request.to),
                )
                .await
                {
                    Ok((page, _, _)) => page,
                    Err(error)
                        if matches!(
                            error.kind,
                            StreamFailureKind::Expired | StreamFailureKind::NotFound
                        ) =>
                    {
                        return Err(lost(error.message));
                    }
                    Err(error) => return Err(error),
                };
                start_cursor.get_or_insert(page.start_cursor);
                let moved = page.cursor != after;
                records.extend(page.records);
                after = page.cursor;
                let stored = records
                    .iter()
                    .filter(|record| matches!(record.record, Some(read_record::Record::Stored(_))))
                    .count();
                if after == request.to || !moved || stored > expected {
                    break;
                }
            }
            ReadResponse {
                cursor: after,
                records_digest: crate::reader::records_digest(&records),
                records,
                chain_first_run_id: request.chain_first_run_id.clone(),
                start_cursor: start_cursor.unwrap_or_default(),
                ..Default::default()
            }
        };
        let stored = page
            .records
            .iter()
            .filter(|record| matches!(record.record, Some(read_record::Record::Stored(_))))
            .count();
        if stored < expected {
            return Err(lost(format!(
                "the store holds {stored} of the {expected} records after {:?} that the read got",
                request.after
            )));
        }
        if stored > expected
            || page.cursor != request.to
            || page.records_digest != request.records_digest
        {
            // A read from the beginning has no position to check, so a trim since the read shows
            // as other records, which end elsewhere or differ.
            if request.after.is_empty()
                && self
                    .store
                    .dropped_after(&target.chain, &target.topic, "")
                    .await?
                    .is_some()
            {
                return Err(lost(
                    "retention trimmed the start of the stream, which the read began at"
                        .to_string(),
                ));
            }
            // A store that replicates late can lose acked records in a failover. The record the
            // read ended at is gone then, and records written since read as other records.
            if let Some(end) = cursor_position(&request.to, self.store.name(), &target.stream_hash)?
                && self
                    .store
                    .record_at(&target.chain, &target.topic, end)
                    .await?
                    .is_none()
            {
                return Err(lost(format!(
                    "the store no longer holds the record at {:?} that the read ended at",
                    request.to
                )));
            }
            if stored > expected {
                return Err(mismatch(format!(
                    "the replayed read holds {stored} records up to {:?}, not the {expected} the \
                     read got",
                    request.to
                )));
            }
            if page.cursor != request.to {
                return Err(mismatch(format!(
                    "the replayed read ends at {:?}, not at {:?}",
                    page.cursor, request.to
                )));
            }
            return Err(mismatch(
                "the replayed records differ from the records the read got".to_string(),
            ));
        }
        Ok(ReadResponse {
            state: state.encode_to_vec(),
            ..page
        })
    }

    /// Ends a positions-only reader's hold after the store's release grace.
    pub async fn release_floor(
        &self,
        request: ReleaseFloorRequest,
    ) -> StreamResult<ReleaseFloorResponse> {
        let stream = address(request.stream.as_ref())?;
        if request.chain_first_run_id.is_empty() || request.floor_holder.is_empty() {
            return Err(StreamError::invalid_argument(
                "a release needs the chain and the floor holder",
            ));
        }
        refuse_archive_holder(&request.floor_holder)?;
        self.store
            .release_floor(
                &chain_of(stream, &request.chain_first_run_id),
                &stream.topic,
                &request.floor_holder,
            )
            .await?;
        Ok(ReleaseFloorResponse {})
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
    /// new is not the end, and a new chain on the Workflow id is. A Workflow Activity that its
    /// running run no longer lists as pending ends the read only after
    /// [StreamsOptions::owner_start_grace] from the first check that found it so, since a
    /// Workflow can write a link to the stream before the server takes the Activity's schedule.
    /// A reader only ends its own read, so it needs no write access. Producers and the owner's
    /// Worker close the chain.
    async fn owner_ended(
        &self,
        stream: &StreamAddress,
        chain: &ChainId,
        state: &mut ReadState,
    ) -> StreamResult<bool> {
        let key = owner_key(stream);
        match self.describe(&key, &key.run_id).await {
            Ok(described) if key.kind == Kind::WorkflowActivity => match described.status {
                OwnerStatus::Running => {
                    state.owner_missing_since_ms = 0;
                    Ok(false)
                }
                OwnerStatus::ActivityGone => {
                    let now = unix_ms();
                    if state.owner_missing_since_ms == 0 {
                        state.owner_missing_since_ms = now;
                    }
                    let gone_for = now.saturating_sub(state.owner_missing_since_ms);
                    Ok(gone_for >= millis(self.options.owner_start_grace))
                }
                OwnerStatus::ContinuedAsNew | OwnerStatus::Ended => Ok(true),
            },
            // The chain came from this owner, so Temporal has since dropped it, or a failover
            // hides it for a while.
            Err(OwnerError::NotFound(_)) => Ok(self.gone_for_good(chain)),
            Err(error) => Err(describe_failed(&error)),
            Ok(described) if !key.run_id.is_empty() => Ok(described.status != OwnerStatus::Running),
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
    /// Without them, a batch that lands after the close is one no reader delivers. With a result
    /// or a failure, the stream's notifier closes too, and fails the call when the server doesn't
    /// take it, since the close completes every callback attached there.
    pub async fn close(&self, request: CloseRequest) -> StreamResult<CloseResponse> {
        let stream = address(request.stream.as_ref())?;
        if request.result.is_some() && request.failure.is_some() {
            return Err(StreamError::invalid_argument(
                "a close carries a result or a failure, not both",
            ));
        }
        if request.closed_by.len() > MAX_CLOSED_BY_BYTES {
            return Err(StreamError::invalid_argument(format!(
                "closed_by is at most {MAX_CLOSED_BY_BYTES} bytes, not {}",
                request.closed_by.len()
            )));
        }
        let has_outcome = request.result.is_some() || request.failure.is_some();
        let close = StoredClose {
            result: request.result,
            failure: request.failure,
            closed_by: request.closed_by,
        };
        if close.encoded_len() > MAX_CLOSE_BYTES {
            return Err(StreamError::invalid_argument(format!(
                "a close's outcome is at most {MAX_CLOSE_BYTES} bytes, not {}",
                close.encoded_len()
            )));
        }
        let chain = self.chain(stream).await?;
        if has_outcome {
            self.store
                .close_stream(&chain, &stream.topic, close)
                .await?
        } else if self.store.capabilities().producer_state {
            self.store
                .close_topic_with(&chain, &stream.topic, close)
                .await?;
        } else {
            close_topic(&*self.store, &chain, &stream.topic, &close.closed_by, 0).await?;
        }
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
        let (id, other) = match StreamOwnerKind::try_from(request.owner_kind) {
            Ok(StreamOwnerKind::Workflow) => (&request.workflow_id, &request.activity_id),
            Ok(StreamOwnerKind::Activity) => (&request.activity_id, &request.workflow_id),
            Ok(StreamOwnerKind::WorkflowActivity) => {
                return Err(StreamError::invalid_argument(
                    "a Workflow Activity's streams go with its Workflow's delete",
                ));
            }
            _ => return Err(unsupported_kind(request.owner_kind)),
        };
        if request.namespace.is_empty() || id.is_empty() {
            return Err(StreamError::invalid_argument(
                "a delete needs the namespace and the owner's id of its kind",
            ));
        }
        if !other.is_empty() {
            return Err(StreamError::invalid_argument(
                "a delete names the owner's id of its kind only",
            ));
        }
        self.store.delete_owner(request).await
    }

    /// The chain a producer writes on. A producer attempt stays on the chain it began on, and
    /// is refused once a new chain took the owner id.
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
        let chain = if owner.kind == Kind::WorkflowActivity {
            // A Workflow Activity writes only while it runs, so a run that no longer lists it as
            // pending refuses at once.
            let chain = chain_of_key(owner, &owner.run_id);
            match self.describe(owner, &owner.run_id).await {
                Err(OwnerError::NotFound(_)) if known.is_some() => {
                    return Err(self.closed(&chain, gone_text(owner), writer).await);
                }
                Err(error) => return Err(not_found(owner, error)),
                Ok(described) if described.status != OwnerStatus::Running => {
                    return Err(self.closed(&chain, "has ended", writer).await);
                }
                Ok(_) => chain,
            }
        } else if owner.run_id.is_empty() {
            // The latest run names the chain and tells whether it ended, in one call.
            let latest = self.describe(owner, "").await;
            if let (Err(OwnerError::NotFound(_)), Some(checked)) = (&latest, &known) {
                // The chain came from this owner, so Temporal has since dropped it.
                let gone = chain_of_key(owner, &checked.first_run_id);
                return Err(self.gone(&gone, writer).await);
            }
            let latest = latest.map_err(|error| not_found(owner, error))?;
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
                .map_err(|error| not_found(owner, error))?;
            let chain = chain_of_key(owner, &first_run_id(owner, &pinned)?);
            // A running run is its chain's latest, so it answers the end check too.
            if pinned.status != OwnerStatus::Running {
                self.refuse_if_ended(&chain, writer).await?;
            }
            chain
        };
        self.owners.put(owner.clone(), &chain.first_run_id, recheck);
        Ok(chain)
    }

    /// Closes the chain and refuses when its latest run ended, when Temporal no longer holds it,
    /// or when a new chain took its owner id.
    async fn refuse_if_ended(&self, chain: &ChainId, writer: Writer<'_>) -> StreamResult<()> {
        let key = chain_owner_key(chain);
        match self.describe(&key, "").await {
            // The chain came from this owner, so Temporal has since dropped it.
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
            self.closed(chain, gone_text(&chain_owner_key(chain)), writer)
                .await
        } else {
            missing(chain)
        }
    }

    /// Whether Temporal answered that `chain`'s owner is gone twice, at least
    /// [StreamsOptions::owner_recheck] apart, with no answer that holds it in between.
    fn gone_for_good(&self, chain: &ChainId) -> bool {
        let key = chain_owner_key(chain);
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
            "the {} that owns this stream {how}",
            chain_owner_key(chain).label()
        ))
    }

    /// The run chain a checked stream address names now. A pinned run's chain comes from the
    /// owner cache, since it never changes. The latest run's chain comes from a fresh describe,
    /// for a caller that must not land on a chain that already ended.
    pub(crate) async fn chain_now(&self, stream: &StreamAddress) -> StreamResult<ChainId> {
        let key = owner_key(stream);
        if !key.run_id.is_empty() {
            return self.chain(stream).await;
        }
        let described = self
            .describe(&key, "")
            .await
            .map_err(|error| not_found(&key, error))?;
        let first_run_id = first_run_id(&key, &described)?;
        self.chains
            .put(key, &first_run_id, self.options.owner_recheck);
        Ok(chain_of(stream, &first_run_id))
    }

    /// The chain a stream address names now.
    pub(crate) async fn chain(&self, stream: &StreamAddress) -> StreamResult<ChainId> {
        let key = owner_key(stream);
        // A pinned run's chain never changes. The latest run's chain changes when a new chain
        // takes the owner id.
        if let Some(checked) = self.chains.get(&key)
            && (!key.run_id.is_empty() || checked.at.elapsed() < self.options.owner_recheck)
        {
            return Ok(chain_of(stream, &checked.first_run_id));
        }
        let described = self
            .describe(&key, &key.run_id)
            .await
            .map_err(|error| not_found(&key, error))?;
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
            .describe(&owner.namespace, owner.owner(), run_id)
            .await;
        if described.is_ok() {
            self.missing.forget(&OwnerKey {
                run_id: String::new(),
                ..owner.clone()
            });
        }
        described
    }

    fn hash(&self, stream: &StreamAddress) -> String {
        let chain = chain_of(stream, "");
        let (kind, id) = chain_owner(&chain);
        stream_hash(&stream.namespace, kind, &id, &stream.topic)
    }

    fn cursor(&self, stream: &StreamAddress, position: &str) -> String {
        mint_cursor(self.store.name(), &self.hash(stream), position)
    }

    /// The notification counter and the cursor of the newest record of `stream` on `chain`, or
    /// zero and an empty cursor when it holds none.
    pub(crate) async fn newest(
        &self,
        stream: &StreamAddress,
        chain: &ChainId,
    ) -> StreamResult<(i64, String)> {
        let position = self
            .store
            .latest(StoreLatestRequest {
                chain: Some(chain.clone()),
                topic: stream.topic.clone(),
            })
            .await?
            .position;
        if position.is_empty() {
            return Ok((0, String::new()));
        }
        let counter = crate::progress_counter(&position).unwrap_or(0);
        Ok((counter, self.cursor(stream, &position)))
    }
}

/// The address of a stream this release keeps, checked.
pub(crate) fn address(stream: Option<&StreamAddress>) -> StreamResult<&StreamAddress> {
    let stream =
        stream.ok_or_else(|| StreamError::invalid_argument("a stream call needs its stream"))?;
    match StreamOwnerKind::try_from(stream.owner_kind) {
        Ok(StreamOwnerKind::Workflow) => {
            if stream.namespace.is_empty() || stream.workflow_id.is_empty() {
                return Err(StreamError::invalid_argument(
                    "a stream needs its namespace and its owner's Workflow id",
                ));
            }
            if !stream.activity_id.is_empty() {
                return Err(StreamError::invalid_argument(
                    "a stream that a Workflow owns names no Activity id",
                ));
            }
        }
        Ok(StreamOwnerKind::Activity) => {
            if stream.namespace.is_empty() || stream.activity_id.is_empty() {
                return Err(StreamError::invalid_argument(
                    "a stream needs its namespace and its owner's Activity id",
                ));
            }
            if !stream.workflow_id.is_empty() {
                return Err(StreamError::invalid_argument(
                    "a stream that an Activity owns names no Workflow id",
                ));
            }
        }
        Ok(StreamOwnerKind::WorkflowActivity) => {
            if stream.namespace.is_empty()
                || stream.workflow_id.is_empty()
                || stream.activity_id.is_empty()
            {
                return Err(StreamError::invalid_argument(
                    "a stream that a Workflow Activity owns needs its namespace, its Workflow id \
                     and its Activity id",
                ));
            }
            if stream.run_id.is_empty() {
                // An Activity id names nothing across runs, since ids such as `1` repeat after
                // Continue-as-New.
                return Err(StreamError::invalid_argument(
                    "a stream that a Workflow Activity owns needs the run that scheduled it",
                ));
            }
        }
        _ => return Err(unsupported_kind(stream.owner_kind)),
    }
    check_topic(&stream.topic)?;
    Ok(stream)
}

fn unsupported_kind(owner_kind: i32) -> StreamError {
    StreamError::unsupported(format!(
        "this release keeps streams of Workflows, standalone Activities and Workflow Activities \
         only, not owner kind {owner_kind}"
    ))
}

/// Refuses a topic no store keys reliably, by the rules the Workflow publish also applies.
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
        (None, Some(activity)) => (activity_producer_id(activity), activity_attempt(activity)?),
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
    crate::check_topic(topic).map_err(StreamError::invalid_argument)
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

fn kind_of(owner_kind: i32) -> Kind {
    match StreamOwnerKind::try_from(owner_kind) {
        Ok(StreamOwnerKind::Activity) => Kind::Activity,
        Ok(StreamOwnerKind::WorkflowActivity) => Kind::WorkflowActivity,
        _ => Kind::Workflow,
    }
}

fn owner_key(stream: &StreamAddress) -> OwnerKey {
    let kind = kind_of(stream.owner_kind);
    let (id, activity_id) = match kind {
        Kind::Workflow => (&stream.workflow_id, ""),
        Kind::Activity => (&stream.activity_id, ""),
        Kind::WorkflowActivity => (&stream.workflow_id, stream.activity_id.as_str()),
    };
    OwnerKey {
        namespace: stream.namespace.clone(),
        kind,
        id: id.clone(),
        activity_id: activity_id.to_string(),
        run_id: stream.run_id.clone(),
    }
}

/// The latest run of the owner of `chain`, or a Workflow Activity's scheduling run.
fn chain_owner_key(chain: &ChainId) -> OwnerKey {
    let kind = kind_of(chain.owner_kind);
    let (id, activity_id, run_id) = match kind {
        Kind::Workflow => (&chain.workflow_id, "", ""),
        Kind::Activity => (&chain.activity_id, "", ""),
        Kind::WorkflowActivity => (
            &chain.workflow_id,
            chain.activity_id.as_str(),
            chain.first_run_id.as_str(),
        ),
    };
    OwnerKey {
        namespace: chain.namespace.clone(),
        kind,
        id: id.clone(),
        activity_id: activity_id.to_string(),
        run_id: run_id.to_string(),
    }
}

/// The chain of `stream`'s owner that starts at `first_run_id`.
pub(crate) fn chain_of(stream: &StreamAddress, first_run_id: &str) -> ChainId {
    chain_of_key(&owner_key(stream), first_run_id)
}

fn chain_of_key(owner: &OwnerKey, first_run_id: &str) -> ChainId {
    // A Workflow chain keeps the kind unset, as every chain from before the Activity kind does,
    // so the two compare equal.
    match owner.kind {
        Kind::Workflow => ChainId {
            namespace: owner.namespace.clone(),
            workflow_id: owner.id.clone(),
            first_run_id: first_run_id.to_string(),
            ..Default::default()
        },
        Kind::Activity => ChainId {
            namespace: owner.namespace.clone(),
            first_run_id: first_run_id.to_string(),
            owner_kind: StreamOwnerKind::Activity as i32,
            activity_id: owner.id.clone(),
            ..Default::default()
        },
        // The scheduling run keys a Workflow Activity, whatever chain that run is in.
        Kind::WorkflowActivity => ChainId {
            namespace: owner.namespace.clone(),
            workflow_id: owner.id.clone(),
            first_run_id: owner.run_id.clone(),
            owner_kind: StreamOwnerKind::WorkflowActivity as i32,
            activity_id: owner.activity_id.clone(),
        },
    }
}

/// How a refusal says that Temporal no longer holds the owner.
fn gone_text(owner: &OwnerKey) -> &'static str {
    match owner.kind {
        Kind::Activity => "is gone from Temporal",
        Kind::Workflow | Kind::WorkflowActivity => "is gone from History",
    }
}

fn first_run_id(owner: &OwnerKey, described: &OwnerDescription) -> StreamResult<String> {
    if owner.kind == Kind::WorkflowActivity {
        return Ok(owner.run_id.clone());
    }
    if described.first_run_id.is_empty() {
        // The owner's own output is keyed by the chain's first run, so an empty one would split
        // the stream in two without an error.
        return Err(StreamError::unsupported(format!(
            "Temporal did not report the first run id of {}, which streams are keyed by",
            owner.label()
        )));
    }
    Ok(described.first_run_id.clone())
}

fn not_found(owner: &OwnerKey, error: OwnerError) -> StreamError {
    match error {
        OwnerError::NotFound(message) => StreamError::not_found(format!(
            "the {} that owns the stream was not found, and its stream is keyed by its run \
             chain: {message}",
            owner.label()
        )),
        error => describe_failed(&error),
    }
}

fn missing(chain: &ChainId) -> StreamError {
    StreamError::storage(format!(
        "Temporal doesn't hold the owner {:?} of this stream for now, so the call fails until \
         it holds it again or answers the same after a recheck",
        chain_owner_key(chain).id
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
        common::v1::Payload,
        enums::v1::{ActivityExecutionStatus, WorkflowExecutionStatus},
    };

    /// Temporal as the tests need it: the latest run of each owner, and pinned runs. A Workflow
    /// owner is keyed by its id, and an Activity owner by `activity/<id>`.
    #[derive(Default)]
    struct Owners {
        latest: Mutex<HashMap<String, Result<OwnerDescription, OwnerError>>>,
        runs: Mutex<HashMap<String, OwnerDescription>>,
        /// The Activities each run lists as pending, by run id and Activity id.
        pending: Mutex<std::collections::HashSet<(String, String)>>,
        describes: AtomicUsize,
    }

    impl Owners {
        fn run(&self, workflow_id: &str, run_id: &str, first_run_id: &str) {
            let described = OwnerDescription {
                run_id: run_id.to_string(),
                first_run_id: first_run_id.to_string(),
                status: OwnerStatus::Running,
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

        /// Starts run `run_id` of Activity `activity_id`, which is its own chain.
        fn activity_run(&self, activity_id: &str, run_id: &str) {
            self.run(&format!("activity/{activity_id}"), run_id, run_id);
        }

        fn end(&self, workflow_id: &str, status: WorkflowExecutionStatus) {
            self.end_as(workflow_id, OwnerStatus::of_workflow(status));
        }

        fn end_activity(&self, activity_id: &str, status: ActivityExecutionStatus) {
            let status = OwnerStatus::of_activity(status);
            let key = format!("activity/{activity_id}");
            if let Some(Ok(latest)) = self.latest.lock().unwrap().get(&key)
                && let Some(run) = self.runs.lock().unwrap().get_mut(&latest.run_id)
            {
                run.status = status;
            }
            self.end_as(&key, status);
        }

        fn end_as(&self, key: &str, status: OwnerStatus) {
            if let Some(Ok(latest)) = self.latest.lock().unwrap().get_mut(key) {
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

        /// Sets whether run `run_id` lists Activity `activity_id` as pending.
        fn pending(&self, run_id: &str, activity_id: &str, pending: bool) {
            let key = (run_id.to_string(), activity_id.to_string());
            let mut all = self.pending.lock().unwrap();
            if pending {
                all.insert(key);
            } else {
                all.remove(&key);
            }
        }
    }

    #[async_trait::async_trait]
    impl OwnerClient for Owners {
        async fn describe(
            &self,
            _namespace: &str,
            owner: OwnerId<'_>,
            run_id: &str,
        ) -> Result<OwnerDescription, OwnerError> {
            self.describes.fetch_add(1, Ordering::Relaxed);
            let workflow_id = &match owner {
                OwnerId::Workflow(id) => id.to_string(),
                OwnerId::Activity(id) => format!("activity/{id}"),
                OwnerId::WorkflowActivity { activity_id, .. } => {
                    let run = self
                        .runs
                        .lock()
                        .unwrap()
                        .get(run_id)
                        .cloned()
                        .ok_or_else(|| OwnerError::NotFound(run_id.to_string()))?;
                    let pending = self
                        .pending
                        .lock()
                        .unwrap()
                        .contains(&(run_id.to_string(), activity_id.to_string()));
                    let status = match run.status {
                        OwnerStatus::Running if pending => OwnerStatus::Running,
                        OwnerStatus::Running => OwnerStatus::ActivityGone,
                        _ => OwnerStatus::Ended,
                    };
                    return Ok(OwnerDescription { status, ..run });
                }
            };
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
        for topic in ["", "a\u{1f}b", &"t".repeat(crate::MAX_TOPIC_BYTES + 1)] {
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
                status: OwnerStatus::of_workflow(status),
            }
            .chain_ended()
        };
        for status in [Completed, Failed, Canceled, Terminated, TimedOut] {
            assert!(ended(status), "{status:?}");
        }
        for status in [Running, ContinuedAsNew, Unspecified, Paused] {
            assert!(!ended(status), "{status:?}");
        }
        assert_eq!(OwnerStatus::of_workflow(Paused), OwnerStatus::Running);
    }

    #[test]
    fn only_a_terminal_activity_status_ends_its_run() {
        use ActivityExecutionStatus::*;
        for status in [Completed, Failed, Canceled, Terminated, TimedOut] {
            assert_eq!(
                OwnerStatus::of_activity(status),
                OwnerStatus::Ended,
                "{status:?}"
            );
        }
        for status in [Running, Paused, Unspecified] {
            assert_eq!(
                OwnerStatus::of_activity(status),
                OwnerStatus::Running,
                "{status:?}"
            );
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
                result: None,
                failure: None,
                closed_by: Default::default(),
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
                ..Default::default()
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
                ..Default::default()
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

    fn gave_up() -> temporalio_common::protos::temporal::api::failure::v1::Failure {
        temporalio_common::protos::temporal::api::failure::v1::Failure {
            message: "the agent gave up".to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_reader_that_reaches_the_end_sees_the_first_close() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup
            .streams
            .close(CloseRequest {
                stream: Some(stream("out")),
                failure: Some(gave_up()),
                closed_by: "run-1".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
        setup
            .streams
            .close(CloseRequest {
                stream: Some(stream("out")),
                closed_by: "run-2".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
        let (delivered, end) = read_to_end(&setup.streams, Duration::from_millis(50)).await;
        assert_eq!(delivered, 1);
        assert!(end.done);
        assert_eq!(end.close_failure, Some(gave_up()));
        assert_eq!(end.close_result, None);
        assert_eq!(end.closed_by, "run-1", "the first close stays");
        let refused = setup.streams.append(append("q", 1, "2")).await.unwrap_err();
        assert_eq!(refused.kind, StreamFailureKind::Closed);
        assert_eq!(refused.closed_by.as_deref(), Some("run-1"));
    }

    #[tokio::test]
    async fn a_reader_whose_owner_ended_with_no_close_sees_no_outcome() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.owners.end("wf", WorkflowExecutionStatus::Completed);
        let (_, end) = read_to_end(&setup.streams, Duration::from_millis(50)).await;
        assert!(end.done);
        assert_eq!(
            (end.close_result, end.close_failure, end.closed_by.as_str()),
            (None, None, "")
        );
    }

    #[tokio::test]
    async fn a_close_too_large_to_keep_is_refused_and_closes_nothing() {
        let setup = setup(quick());
        for close in [
            CloseRequest {
                stream: Some(stream("out")),
                closed_by: "x".repeat(MAX_CLOSED_BY_BYTES + 1),
                ..Default::default()
            },
            CloseRequest {
                stream: Some(stream("out")),
                result: Some(Payload {
                    data: vec![0; MAX_CLOSE_BYTES],
                    ..Default::default()
                }),
                ..Default::default()
            },
        ] {
            let error = setup.streams.close(close).await.unwrap_err();
            assert_eq!(error.kind, StreamFailureKind::InvalidArgument);
        }
        setup.streams.append(append("p", 1, "1")).await.unwrap();
    }

    /// The replay read that repeats the live `read` of `request` that answered `page`.
    fn replay_of(request: &ReadRequest, page: &ReadResponse) -> ReadRangeRequest {
        ReadRangeRequest {
            stream: Some(stream("out")),
            chain_first_run_id: page.chain_first_run_id.clone(),
            after: page.start_cursor.clone(),
            to: page.cursor.clone(),
            stored_records: page
                .records
                .iter()
                .filter(|r| matches!(r.record, Some(read_record::Record::Stored(_))))
                .count() as u32,
            state: request.state.clone(),
            records_digest: page.records_digest.clone(),
            floor_holder: String::new(),
            start_from_end: request.after == crate::END,
        }
    }

    #[tokio::test]
    async fn a_replay_reads_to_the_recorded_end_past_dropped_copies() {
        let setup = setup_on(without_producer_state(), StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        // The answer was lost, so the producer sent the batch again.
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.streams.append(append("p", 2, "2")).await.unwrap();
        let request = ReadRequest {
            stream: Some(stream("out")),
            max_records: 3,
            ..Default::default()
        };
        let page = setup.streams.read(request.clone()).await.unwrap();
        assert_eq!(page.records.len(), 2);
        let replayed = setup
            .streams
            .read_range(replay_of(&request, &page))
            .await
            .unwrap();
        assert_eq!(replayed.cursor, page.cursor);
        assert_eq!(replayed.records_digest, page.records_digest);
        assert_eq!(replayed.records, page.records);
    }

    #[tokio::test]
    async fn a_replay_whose_end_retention_dropped_is_lost_not_a_mismatch() {
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.streams.append(append("p", 2, "2")).await.unwrap();
        let request = ReadRequest {
            stream: Some(stream("out")),
            ..Default::default()
        };
        let page = setup.streams.read(request.clone()).await.unwrap();
        // A lapsed floor let retention take both records, and the stream went on.
        setup.store.truncate(&chain("run-1"), "out", 0);
        setup.streams.append(append("p", 3, "3")).await.unwrap();
        setup.streams.append(append("p", 4, "4")).await.unwrap();
        setup.streams.append(append("p", 5, "5")).await.unwrap();
        let error = setup
            .streams
            .read_range(replay_of(&request, &page))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::ReplayRangeLost, "{error}");
    }

    #[tokio::test]
    async fn a_positions_only_read_needs_holds_or_a_replay_horizon() {
        let no_holds = crate::Capabilities {
            holds: false,
            ..MemoryStore::new().capabilities()
        };
        let request = ReadRequest {
            stream: Some(stream("out")),
            floor_holder: "reader-run/1".to_string(),
            ..Default::default()
        };
        let setup = setup_on(
            MemoryStore::with_capabilities(no_holds),
            StreamsOptions::default(),
        );
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let error = setup.streams.read(request.clone()).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Unsupported);
        let with_horizon = crate::Capabilities {
            replay_horizon: Some(Duration::from_secs(30 * 24 * 60 * 60)),
            ..no_holds
        };
        let setup = setup_on(
            MemoryStore::with_capabilities(with_horizon),
            StreamsOptions::default(),
        );
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let page = setup.streams.read(request).await.unwrap();
        assert_eq!(page.records.len(), 1);
    }

    #[tokio::test]
    async fn without_close_flags_a_close_record_carries_its_outcome_to_readers() {
        let setup = setup_on(without_producer_state(), quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let result = Payload {
            data: b"done".to_vec(),
            ..Default::default()
        };
        setup
            .streams
            .close(CloseRequest {
                stream: Some(stream("out")),
                result: Some(result.clone()),
                ..Default::default()
            })
            .await
            .unwrap();
        // A second close lands too, and readers keep the first.
        setup
            .streams
            .close(CloseRequest {
                stream: Some(stream("out")),
                ..Default::default()
            })
            .await
            .unwrap();
        let (delivered, end) = read_to_end(&setup.streams, Duration::from_secs(5)).await;
        assert_eq!(delivered, 1);
        assert!(end.done);
        assert_eq!(end.close_result, Some(result));
    }

    fn attempt(producer: &str, attempt: i64, sequence: i64, value: &str) -> AppendRequest {
        AppendRequest {
            producer: Some(Producer::Named(NamedProducer {
                producer_id: producer.to_string(),
                attempt,
            })),
            ..append(producer, sequence, value)
        }
    }

    /// A read after the stream's first record, with its replay request.
    async fn first_page(setup: &Setup, holder: &str) -> (ReadRequest, ReadResponse) {
        let first = setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.streams.append(append("p", 2, "2")).await.unwrap();
        // A newer attempt makes the read synthesize a supersession.
        setup.streams.append(attempt("p", 2, 1, "3")).await.unwrap();
        let request = ReadRequest {
            floor_holder: holder.to_string(),
            ..read_after(&first.last_cursor, &[], Duration::ZERO)
        };
        let page = setup.streams.read(request.clone()).await.unwrap();
        (request, page)
    }

    #[tokio::test]
    async fn a_replay_read_gives_the_records_the_read_got() {
        let setup = setup(quick());
        let (request, page) = first_page(&setup, "").await;
        assert!(
            page.records
                .iter()
                .any(|r| matches!(r.record, Some(read_record::Record::Superseded(_)))),
            "{page:?}"
        );
        assert_eq!(page.chain_first_run_id, "run-1");
        assert_eq!(page.start_cursor, request.after);
        assert_eq!(page.records_digest.len(), 32);
        // More records after the range change nothing in it.
        setup.streams.append(attempt("p", 2, 2, "4")).await.unwrap();
        let replayed = setup
            .streams
            .read_range(replay_of(&request, &page))
            .await
            .unwrap();
        assert_eq!(replayed.records, page.records);
        assert_eq!(replayed.cursor, page.cursor);
        assert_eq!(replayed.records_digest, page.records_digest);
    }

    #[tokio::test]
    async fn a_replay_read_of_a_read_from_the_end_gives_the_records_the_read_got() {
        let setup = setup(quick());
        setup.streams.append(attempt("p", 1, 1, "1")).await.unwrap();
        let request = read_after(crate::END, &[], Duration::from_secs(5));
        // The read finds its start at the end, then a newer attempt of the producer there lands.
        let (page, _) = tokio::join!(setup.streams.read(request.clone()), async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            setup.streams.append(attempt("p", 2, 1, "2")).await.unwrap();
        });
        let page = page.unwrap();
        assert_eq!(page.records.len(), 1, "no supersession, {page:?}");
        let replay = replay_of(&request, &page);
        assert!(replay.start_from_end);
        let replayed = setup.streams.read_range(replay).await.unwrap();
        assert_eq!(replayed.records, page.records);
    }

    #[tokio::test]
    async fn a_replay_read_needs_no_owner() {
        let setup = setup(quick());
        let (request, page) = first_page(&setup, "").await;
        // History dropped the owner, as for a Replayer with only the store.
        setup.owners.end("wf", WorkflowExecutionStatus::Completed);
        let before = setup.owners.describes();
        setup
            .streams
            .read_range(replay_of(&request, &page))
            .await
            .unwrap();
        assert_eq!(setup.owners.describes(), before);
    }

    #[tokio::test]
    async fn a_replay_read_with_other_records_is_a_mismatch() {
        let setup = setup(quick());
        let (request, page) = first_page(&setup, "").await;
        let mut other_digest = replay_of(&request, &page);
        other_digest.records_digest[0] ^= 1;
        let mut other_end = replay_of(&request, &page);
        other_end.stored_records -= 1;
        for replay in [other_digest, other_end] {
            let error = setup.streams.read_range(replay).await.unwrap_err();
            assert_eq!(error.kind, StreamFailureKind::ReplayMismatch, "{error}");
        }
    }

    #[tokio::test]
    async fn a_replay_read_of_records_retention_dropped_is_rec_01() {
        let setup = setup(quick());
        let (request, page) = first_page(&setup, "").await;
        setup.store.truncate(&chain("run-1"), "out", 1);
        let error = setup
            .streams
            .read_range(replay_of(&request, &page))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::ReplayRangeLost, "{error}");
        assert!(error.message.starts_with("REC-01"), "{error}");
    }

    #[tokio::test]
    async fn a_replay_read_from_a_trimmed_start_is_rec_01() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.streams.append(append("p", 2, "2")).await.unwrap();
        let request = read_after("", &[], Duration::ZERO);
        let page = setup.streams.read(request.clone()).await.unwrap();
        assert_eq!(page.start_cursor, "");
        // Later records stand where the range began, so the replay finds as many as it wants.
        setup.store.truncate(&chain("run-1"), "out", 0);
        for sequence in 3..=4 {
            setup
                .streams
                .append(append("p", sequence, &sequence.to_string()))
                .await
                .unwrap();
        }
        let error = setup
            .streams
            .read_range(replay_of(&request, &page))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::ReplayRangeLost, "{error}");
    }

    #[tokio::test]
    async fn a_floor_keeps_what_a_positions_only_reader_read_until_it_is_released() {
        let setup = setup(quick());
        let (request, page) = first_page(&setup, "reader-run/1").await;
        setup.store.truncate(&chain("run-1"), "out", 1);
        let replayed = setup
            .streams
            .read_range(ReadRangeRequest {
                floor_holder: "reader-run/1".to_string(),
                ..replay_of(&request, &page)
            })
            .await
            .unwrap();
        assert_eq!(replayed.records, page.records, "the floor kept the range");
        setup
            .streams
            .release_floor(ReleaseFloorRequest {
                stream: Some(stream("out")),
                chain_first_run_id: "run-1".to_string(),
                floor_holder: "reader-run/1".to_string(),
            })
            .await
            .unwrap();
        setup.store.truncate(&chain("run-1"), "out", 1);
        let error = setup
            .streams
            .read_range(replay_of(&request, &page))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::ReplayRangeLost, "{error}");
    }

    #[tokio::test]
    async fn a_replay_read_needs_its_chain_and_a_resolved_start() {
        let setup = setup(quick());
        let (request, page) = first_page(&setup, "").await;
        for replay in [
            ReadRangeRequest {
                chain_first_run_id: String::new(),
                ..replay_of(&request, &page)
            },
            ReadRangeRequest {
                after: crate::END.to_string(),
                ..replay_of(&request, &page)
            },
        ] {
            let error = setup.streams.read_range(replay).await.unwrap_err();
            assert_eq!(error.kind, StreamFailureKind::InvalidArgument, "{error}");
        }
    }

    #[tokio::test]
    async fn an_empty_read_waits_for_a_counter_above_its_position() {
        let setup = setup(StreamsOptions::default());
        let empty = setup
            .streams
            .read(read_after("", &[], Duration::from_millis(20)))
            .await
            .unwrap();
        assert!(empty.records.is_empty() && !empty.done);
        assert_eq!(empty.wake_after, 0, "an empty stream wakes on any counter");
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let landed = setup.streams.append(append("p", 2, "2")).await.unwrap();
        let page = setup
            .streams
            .read(read_after(
                &landed.last_cursor,
                &[],
                Duration::from_millis(20),
            ))
            .await
            .unwrap();
        assert!(page.records.is_empty());
        let at = |cursor: &str| {
            crate::cursor_position(
                cursor,
                setup.store.name(),
                &setup.streams.hash(&stream("out")),
            )
            .unwrap()
            .unwrap()
            .to_string()
        };
        assert_eq!(
            Some(page.wake_after),
            crate::progress_counter(&at(&landed.last_cursor))
        );
        let next = setup.streams.append(append("p", 3, "3")).await.unwrap();
        assert!(
            crate::progress_counter(&at(&next.last_cursor)).unwrap() > page.wake_after,
            "the next record's notification wakes the reader"
        );
    }

    #[tokio::test]
    async fn a_read_that_finds_its_owner_ended_answers_done_with_no_wake_counter() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.owners.end("wf", WorkflowExecutionStatus::Completed);
        let page = setup
            .streams
            .read(read_after(END, &[], Duration::from_secs(5)))
            .await
            .unwrap();
        assert!(page.done);
        assert_eq!(page.wake_after, 0);
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
            .status = OwnerStatus::ContinuedAsNew;
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
                result: None,
                failure: None,
                closed_by: Default::default(),
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
            ..Default::default()
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

    /// A stream of standalone Activity `act`, whose run `act-run-1` is running.
    fn activity_stream(topic: &str) -> StreamAddress {
        StreamAddress {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Activity as i32,
            activity_id: "act".to_string(),
            topic: topic.to_string(),
            ..Default::default()
        }
    }

    fn activity_chain(run_id: &str) -> ChainId {
        ChainId {
            namespace: "ns".to_string(),
            first_run_id: run_id.to_string(),
            owner_kind: StreamOwnerKind::Activity as i32,
            activity_id: "act".to_string(),
            ..Default::default()
        }
    }

    fn activity_setup() -> Setup {
        let setup = setup(quick());
        setup.owners.activity_run("act", "act-run-1");
        setup
    }

    fn activity_append(sequence: i64, value: &str) -> AppendRequest {
        AppendRequest {
            stream: Some(activity_stream("out")),
            producer: Some(Producer::Activity(ActivityProducer {
                run_id: "act-run-1".to_string(),
                activity_id: "act".to_string(),
                attempt: 1,
                ..Default::default()
            })),
            ..append("", sequence, value)
        }
    }

    fn activity_read(after: &str, state: &[u8], wait: Duration) -> ReadRequest {
        ReadRequest {
            stream: Some(activity_stream("out")),
            ..read_after(after, state, wait)
        }
    }

    async fn read_activity_to_end(
        streams: &Streams,
        request: ReadRequest,
    ) -> (usize, ReadResponse) {
        let mut delivered = 0;
        let mut request = request;
        for _ in 0..50 {
            let page = streams.read(request.clone()).await.unwrap();
            delivered += page.records.len();
            if page.done {
                return (delivered, page);
            }
            request.after = page.cursor;
            request.state = page.state;
        }
        panic!("the read never ended");
    }

    async fn activity_chain_closed(store: &MemoryStore, run_id: &str) -> bool {
        store
            .read(StoreReadRequest {
                chain: Some(activity_chain(run_id)),
                topic: "out".to_string(),
                max_records: 1,
                ..Default::default()
            })
            .await
            .unwrap()
            .closed
    }

    #[tokio::test]
    async fn an_activity_writes_its_own_stream_on_the_chain_of_its_run() {
        let setup = activity_setup();
        let landed = setup.streams.append(activity_append(1, "1")).await.unwrap();
        let held = setup
            .store
            .latest(StoreLatestRequest {
                chain: Some(activity_chain("act-run-1")),
                topic: "out".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(held.position, "0");
        let read = setup
            .streams
            .read(activity_read("", &[], Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(read.records.len(), 1);
        assert_eq!(read.records[0].cursor, landed.last_cursor);
        assert_eq!(read.chain_first_run_id, "act-run-1");
        // A Workflow with the same id owns other streams, and their cursors don't mix.
        setup.owners.run("act", "run-1", "run-1");
        let mut workflow = stream("out");
        workflow.workflow_id = "act".to_string();
        let workflow_read = setup
            .streams
            .read(ReadRequest {
                stream: Some(workflow.clone()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(workflow_read.records.is_empty());
        let error = setup
            .streams
            .read(ReadRequest {
                stream: Some(workflow),
                after: landed.last_cursor,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Cursor, "{error}");
    }

    #[tokio::test]
    async fn an_activity_address_names_the_activity_only() {
        let setup = activity_setup();
        let mut no_id = activity_stream("out");
        no_id.activity_id.clear();
        let mut with_workflow = activity_stream("out");
        with_workflow.workflow_id = "wf".to_string();
        let mut workflow_with_activity = stream("out");
        workflow_with_activity.activity_id = "act".to_string();
        for bad in [no_id, with_workflow, workflow_with_activity] {
            assert_eq!(
                kind(
                    setup
                        .streams
                        .latest(LatestRequest { stream: Some(bad) })
                        .await
                ),
                StreamFailureKind::InvalidArgument
            );
        }
        assert_eq!(setup.owners.describes(), 0);
    }

    #[tokio::test]
    async fn an_activity_read_ends_done_when_the_run_reaches_a_terminal_status() {
        let setup = activity_setup();
        setup.streams.append(activity_append(1, "1")).await.unwrap();
        setup
            .owners
            .end_activity("act", ActivityExecutionStatus::Failed);
        let (delivered, end) = read_activity_to_end(
            &setup.streams,
            activity_read("", &[], Duration::from_secs(5)),
        )
        .await;
        assert_eq!(delivered, 1);
        assert!(end.done);
        // A reader writes nothing, so the producers' owner check closes the chain.
        assert!(!activity_chain_closed(&setup.store, "act-run-1").await);
    }

    #[tokio::test]
    async fn an_activity_read_that_retries_and_pauses_stays_open() {
        let setup = activity_setup();
        setup
            .owners
            .end_activity("act", ActivityExecutionStatus::Paused);
        let idle = setup
            .streams
            .read(activity_read("", &[], Duration::from_millis(200)))
            .await
            .unwrap();
        assert!(!idle.done);
        assert!(!activity_chain_closed(&setup.store, "act-run-1").await);
    }

    #[tokio::test]
    async fn an_activity_read_before_the_run_exists_waits_the_grace() {
        let setup = before_owner_starts(Duration::from_secs(60));
        let empty = setup
            .streams
            .read(activity_read(END, &[], Duration::ZERO))
            .await
            .unwrap();
        assert!(empty.records.is_empty() && !empty.done);
        setup.owners.activity_run("act", "act-run-1");
        setup.streams.append(activity_append(1, "1")).await.unwrap();
        let read = setup
            .streams
            .read(activity_read(&empty.cursor, &empty.state, Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(read.records.len(), 1);
        let late = before_owner_starts(Duration::ZERO);
        let error = late
            .streams
            .read(activity_read("", &[], Duration::from_secs(5)))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::NotFound);
        assert!(error.message.contains("Activity \"act\""), "{error}");
    }

    #[tokio::test(start_paused = true)]
    async fn an_activity_producer_is_refused_once_the_run_ended() {
        let setup = activity_setup();
        setup.streams.append(activity_append(1, "1")).await.unwrap();
        setup
            .owners
            .end_activity("act", ActivityExecutionStatus::Completed);
        tokio::time::advance(OWNER_RECHECK + Duration::from_secs(1)).await;
        let error = setup
            .streams
            .append(activity_append(2, "2"))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Closed);
        assert!(error.message.contains("Activity \"act\""), "{error}");
        assert!(activity_chain_closed(&setup.store, "act-run-1").await);
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_run_of_the_activity_id_ends_the_earlier_runs_streams() {
        let setup = activity_setup();
        setup.streams.append(activity_append(1, "1")).await.unwrap();
        setup.owners.activity_run("act", "act-run-2");
        tokio::time::advance(OWNER_RECHECK + Duration::from_secs(1)).await;
        assert_eq!(
            kind(setup.streams.append(activity_append(2, "2")).await),
            StreamFailureKind::Closed
        );
        assert!(activity_chain_closed(&setup.store, "act-run-1").await);
        // A producer of the new run writes on the new run's chain.
        let mut next = activity_append(1, "3");
        if let Some(Producer::Activity(activity)) = &mut next.producer {
            activity.run_id = "act-run-2".to_string();
        }
        setup.streams.append(next).await.unwrap();
        assert!(!activity_chain_closed(&setup.store, "act-run-2").await);
    }

    #[tokio::test]
    async fn an_activity_read_pinned_to_a_run_reads_that_run() {
        let setup = activity_setup();
        setup.streams.append(activity_append(1, "1")).await.unwrap();
        setup.owners.activity_run("act", "act-run-2");
        let mut pinned = activity_read("", &[], Duration::ZERO);
        pinned.stream.as_mut().unwrap().run_id = "act-run-1".to_string();
        let page = setup.streams.read(pinned).await.unwrap();
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.chain_first_run_id, "act-run-1");
    }

    #[tokio::test]
    async fn an_activity_read_follows_the_newest_run_at_its_start_until_a_newer_run() {
        let setup = activity_setup();
        setup.streams.append(activity_append(1, "1")).await.unwrap();
        let first = setup
            .streams
            .read(activity_read("", &[], Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(first.chain_first_run_id, "act-run-1");
        setup.owners.activity_run("act", "act-run-2");
        let (delivered, end) = read_activity_to_end(
            &setup.streams,
            activity_read(&first.cursor, &first.state, Duration::from_secs(5)),
        )
        .await;
        assert_eq!(delivered, 0);
        assert!(end.done);
        assert!(!activity_chain_closed(&setup.store, "act-run-1").await);
    }

    #[tokio::test]
    async fn an_activity_owner_delete_removes_only_that_activitys_streams() {
        let setup = activity_setup();
        setup.streams.append(activity_append(1, "1")).await.unwrap();
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let request = DeleteOwnerRequest {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Activity as i32,
            activity_id: "act".to_string(),
            ..Default::default()
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
        let gone = setup
            .store
            .latest(StoreLatestRequest {
                chain: Some(activity_chain("act-run-1")),
                topic: "out".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(gone.position, "");
        let kept = setup
            .store
            .latest(StoreLatestRequest {
                chain: Some(chain("run-1")),
                topic: "out".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(kept.position, "0");
        for bad in [
            DeleteOwnerRequest {
                activity_id: String::new(),
                ..request.clone()
            },
            DeleteOwnerRequest {
                workflow_id: "wf".to_string(),
                ..request.clone()
            },
        ] {
            assert_eq!(
                kind(setup.streams.delete_owner(bad).await),
                StreamFailureKind::InvalidArgument
            );
        }
    }

    /// The start of 2026-10-14 in Unix milliseconds.
    const DAY_START_MS: i64 = 1_791_936_000_000;

    /// Attempt `attempt` of Activity `fetch` of run `run-1` of `wf`, started `after_ms` past
    /// [DAY_START_MS], writing `value` at `sequence` on the Workflow's stream.
    fn timed(attempt: i32, after_ms: i64, sequence: i64, value: &str) -> AppendRequest {
        let mut activity = ActivityProducer {
            workflow_id: "wf".to_string(),
            run_id: "run-1".to_string(),
            activity_id: "fetch".to_string(),
            attempt,
            ..Default::default()
        };
        let started = activity
            .attempt_started_time
            .get_or_insert_with(Default::default);
        let ms = DAY_START_MS + after_ms;
        started.seconds = ms / 1000;
        started.nanos = (ms % 1000) as i32 * 1_000_000;
        AppendRequest {
            producer: Some(Producer::Activity(activity)),
            ..append("", sequence, value)
        }
    }

    fn key(attempt: i64, after_ms: i64) -> i64 {
        (DAY_START_MS + after_ms) << 11 | attempt
    }

    /// The read's records, as each stored record's value and attempt, or a supersession's two
    /// attempts with an empty value.
    async fn shape(
        setup: &Setup,
        after: &str,
        state: &[u8],
    ) -> (Vec<(String, i64, i64, bool)>, ReadResponse) {
        let page = setup
            .streams
            .read(read_after(after, state, Duration::ZERO))
            .await
            .unwrap();
        let shape = page
            .records
            .iter()
            .map(|record| match &record.record {
                Some(read_record::Record::Stored(stored)) => (
                    String::from_utf8(stored.body.clone().unwrap().data).unwrap(),
                    stored.attempt,
                    0,
                    record.stale,
                ),
                Some(read_record::Record::Superseded(superseded)) => (
                    String::new(),
                    superseded.previous_attempt,
                    superseded.attempt,
                    record.stale,
                ),
                None => panic!("an empty record"),
            })
            .collect();
        (shape, page)
    }

    #[tokio::test]
    async fn a_timed_activity_attempt_writes_its_start_and_attempt_as_its_key() {
        let setup = setup(quick());
        setup.streams.append(timed(3, 0, 1, "a")).await.unwrap();
        let (records, _) = shape(&setup, "", &[]).await;
        assert_eq!(records, [("a".to_string(), key(3, 0), 0, false)]);
        let mut late = timed(70_000, 0, 1, "b");
        if let Some(Producer::Activity(activity)) = &mut late.producer {
            activity.activity_id = "other".to_string();
        }
        setup.streams.append(late).await.unwrap();
        let (records, _) = shape(&setup, "", &[]).await;
        assert_eq!(records[1].1, key(crate::MAX_KEYED_ACTIVITY_ATTEMPT, 0));
        assert_eq!(crate::MAX_KEYED_ACTIVITY_ATTEMPT, 2047);
        // A JSON number holds the key exactly, so the Nexus contract's models take it.
        assert!(records[1].1 < 1 << 53);
        let before_1970 = timed(1, -DAY_START_MS - 1000, 1, "c");
        // 2^42 ms is in May 2109, and a key past it would need more than 53 bits.
        let after_2109 = timed(1, (1 << 42) - DAY_START_MS, 1, "d");
        for refused in [before_1970, after_2109] {
            assert_eq!(
                kind(setup.streams.append(refused).await),
                StreamFailureKind::InvalidArgument
            );
        }
    }

    #[tokio::test]
    async fn an_activity_retry_supersedes_the_earlier_attempt() {
        let setup = setup(quick());
        setup.streams.append(timed(1, 0, 1, "a1")).await.unwrap();
        setup
            .streams
            .append(timed(2, 1_000, 1, "b1"))
            .await
            .unwrap();
        let (records, _) = shape(&setup, "", &[]).await;
        assert_eq!(
            records,
            [
                ("a1".to_string(), key(1, 0), 0, false),
                (String::new(), key(1, 0), key(2, 1_000), false),
                ("b1".to_string(), key(2, 1_000), 0, false),
            ]
        );
    }

    #[tokio::test]
    async fn a_reset_activity_attempt_supersedes_with_no_divergence_and_no_drop() {
        let setup = setup(quick());
        for (attempt, after_ms) in [(1, 0), (2, 1_000), (3, 2_000)] {
            setup
                .streams
                .append(timed(attempt, after_ms, 1, &format!("attempt {attempt}")))
                .await
                .unwrap();
        }
        // A reset sets the Temporal attempt back to 1, and the reset attempt starts later.
        setup
            .streams
            .append(timed(1, 3_000, 1, "reset"))
            .await
            .unwrap();
        let (records, _) = shape(&setup, "", &[]).await;
        assert_eq!(
            records.last().unwrap(),
            &("reset".to_string(), key(1, 3_000), 0, false)
        );
        assert_eq!(
            records[records.len() - 2],
            (String::new(), key(3, 2_000), key(1, 3_000), false)
        );
        // With no start time the reset attempt would repeat attempt 1 with other content.
        let mut untimed = timed(1, 0, 1, "reset");
        if let Some(Producer::Activity(activity)) = &mut untimed.producer {
            activity.attempt_started_time = None;
        }
        setup.streams.append(untimed.clone()).await.unwrap();
        untimed.digest = vec![1; 32];
        assert_eq!(
            kind(setup.streams.append(untimed).await),
            StreamFailureKind::ProducerDivergent
        );
    }

    #[tokio::test]
    async fn an_activity_attempt_that_writes_after_a_newer_one_started_is_stale() {
        let setup = setup(quick());
        setup.streams.append(timed(1, 0, 1, "a1")).await.unwrap();
        setup
            .streams
            .append(timed(2, 1_000, 1, "b1"))
            .await
            .unwrap();
        let (_, page) = shape(&setup, "", &[]).await;
        // The first attempt timed out but keeps writing.
        setup.streams.append(timed(1, 0, 2, "a2")).await.unwrap();
        let (records, _) = shape(&setup, &page.cursor, &page.state).await;
        assert_eq!(records, [("a2".to_string(), key(1, 0), 0, true)]);
    }

    /// A stream of Activity `tool` that run `run-1` of Workflow `wf` scheduled.
    fn wfa_stream(topic: &str) -> StreamAddress {
        StreamAddress {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::WorkflowActivity as i32,
            workflow_id: "wf".to_string(),
            run_id: "run-1".to_string(),
            activity_id: "tool".to_string(),
            topic: topic.to_string(),
        }
    }

    fn wfa_chain(run_id: &str) -> ChainId {
        ChainId {
            namespace: "ns".to_string(),
            workflow_id: "wf".to_string(),
            first_run_id: run_id.to_string(),
            owner_kind: StreamOwnerKind::WorkflowActivity as i32,
            activity_id: "tool".to_string(),
        }
    }

    /// Run `run-1` of `wf` lists `tool` as pending.
    fn wfa_setup(options: StreamsOptions) -> Setup {
        let setup = setup(options);
        setup.owners.pending("run-1", "tool", true);
        setup
    }

    fn wfa_append(sequence: i64, value: &str) -> AppendRequest {
        AppendRequest {
            stream: Some(wfa_stream("out")),
            producer: Some(Producer::Activity(ActivityProducer {
                workflow_id: "wf".to_string(),
                run_id: "run-1".to_string(),
                activity_id: "tool".to_string(),
                attempt: 1,
                ..Default::default()
            })),
            ..append("", sequence, value)
        }
    }

    fn wfa_read(after: &str, state: &[u8], wait: Duration) -> ReadRequest {
        ReadRequest {
            stream: Some(wfa_stream("out")),
            ..read_after(after, state, wait)
        }
    }

    fn end_run(setup: &Setup, run_id: &str) {
        setup
            .owners
            .runs
            .lock()
            .unwrap()
            .get_mut(run_id)
            .unwrap()
            .status = OwnerStatus::Ended;
    }

    async fn wfa_chain_closed(store: &MemoryStore, run_id: &str) -> bool {
        store
            .read(StoreReadRequest {
                chain: Some(wfa_chain(run_id)),
                topic: "out".to_string(),
                max_records: 1,
                ..Default::default()
            })
            .await
            .unwrap()
            .closed
    }

    #[tokio::test]
    async fn a_workflow_activity_writes_its_own_stream_keyed_by_its_run() {
        let setup = wfa_setup(quick());
        let landed = setup.streams.append(wfa_append(1, "1")).await.unwrap();
        let held = setup
            .store
            .latest(StoreLatestRequest {
                chain: Some(wfa_chain("run-1")),
                topic: "out".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(held.position, "0");
        assert_eq!(
            setup
                .store
                .written_topics(&wfa_chain("run-1"))
                .await
                .unwrap(),
            ["out"]
        );
        let read = setup
            .streams
            .read(wfa_read("", &[], Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(read.records.len(), 1);
        assert_eq!(read.records[0].cursor, landed.last_cursor);
        assert_eq!(read.chain_first_run_id, "run-1");
        // Activity ids repeat after Continue-as-New, so the next run's `tool` is another stream.
        setup.owners.run("wf", "run-2", "run-1");
        setup.owners.pending("run-2", "tool", true);
        let mut next = wfa_stream("out");
        next.run_id = "run-2".to_string();
        let error = setup
            .streams
            .read(ReadRequest {
                stream: Some(next),
                after: landed.last_cursor.clone(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Cursor, "{error}");
        // The Workflow's own stream on the same topic is another stream too.
        let error = setup
            .streams
            .read(ReadRequest {
                stream: Some(stream("out")),
                after: landed.last_cursor,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Cursor, "{error}");
    }

    #[tokio::test]
    async fn a_workflow_activity_address_needs_its_workflow_run_and_activity() {
        let setup = wfa_setup(quick());
        let mut bad = Vec::new();
        for clear in [
            |s: &mut StreamAddress| s.workflow_id.clear(),
            |s: &mut StreamAddress| s.run_id.clear(),
            |s: &mut StreamAddress| s.activity_id.clear(),
            |s: &mut StreamAddress| s.namespace.clear(),
        ] {
            let mut stream = wfa_stream("out");
            clear(&mut stream);
            bad.push(stream);
        }
        for bad in bad {
            assert_eq!(
                kind(
                    setup
                        .streams
                        .latest(LatestRequest { stream: Some(bad) })
                        .await
                ),
                StreamFailureKind::InvalidArgument
            );
        }
        assert_eq!(setup.owners.describes(), 0);
    }

    #[tokio::test]
    async fn a_workflow_activity_read_ends_done_at_a_stored_close() {
        let setup = wfa_setup(quick());
        setup.streams.append(wfa_append(1, "1")).await.unwrap();
        setup
            .streams
            .close(CloseRequest {
                stream: Some(wfa_stream("out")),
                closed_by: "tool@run-1".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
        let (delivered, end) =
            read_activity_to_end(&setup.streams, wfa_read("", &[], Duration::from_secs(5))).await;
        assert_eq!(delivered, 1);
        assert!(end.done);
        assert_eq!(end.closed_by, "tool@run-1");
    }

    #[tokio::test]
    async fn a_workflow_activity_read_ends_at_once_when_its_run_closed() {
        let setup = wfa_setup(quick());
        setup.streams.append(wfa_append(1, "1")).await.unwrap();
        end_run(&setup, "run-1");
        let started = std::time::Instant::now();
        let (delivered, end) =
            read_activity_to_end(&setup.streams, wfa_read("", &[], Duration::from_secs(5))).await;
        assert_eq!(delivered, 1);
        assert!(end.done);
        assert!(started.elapsed() < Duration::from_secs(2));
        // A pinned run's end marks nothing closed, as for other pinned reads.
        assert!(!wfa_chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test]
    async fn a_gone_workflow_activity_ends_a_read_only_after_the_grace() {
        let grace = Duration::from_millis(400);
        let setup = wfa_setup(StreamsOptions {
            owner_start_grace: grace,
            ..quick()
        });
        setup.streams.append(wfa_append(1, "1")).await.unwrap();
        setup.owners.pending("run-1", "tool", false);
        let first = setup
            .streams
            .read(wfa_read("", &[], Duration::ZERO))
            .await
            .unwrap();
        let started = std::time::Instant::now();
        let idle = setup
            .streams
            .read(wfa_read(
                &first.cursor,
                &first.state,
                Duration::from_millis(150),
            ))
            .await
            .unwrap();
        assert!(!idle.done);
        let (delivered, end) = read_activity_to_end(
            &setup.streams,
            wfa_read(&idle.cursor, &idle.state, Duration::from_secs(5)),
        )
        .await;
        assert_eq!(delivered, 0);
        assert!(end.done);
        assert!(started.elapsed() >= grace - Duration::from_millis(100));
    }

    #[tokio::test]
    async fn a_workflow_activity_read_before_its_schedule_stays_open() {
        let setup = setup(quick());
        let idle = setup
            .streams
            .read(wfa_read("", &[], Duration::from_millis(150)))
            .await
            .unwrap();
        assert!(idle.records.is_empty() && !idle.done);
        setup.owners.pending("run-1", "tool", true);
        setup.streams.append(wfa_append(1, "1")).await.unwrap();
        let read = setup
            .streams
            .read(wfa_read(&idle.cursor, &idle.state, Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(read.records.len(), 1);
    }

    #[tokio::test]
    async fn a_workflow_activity_read_of_a_missing_run_waits_the_grace() {
        let setup = before_owner_starts(Duration::ZERO);
        let error = setup
            .streams
            .read(wfa_read("", &[], Duration::from_secs(5)))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::NotFound);
        assert!(error.message.contains("Activity \"tool\""), "{error}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_workflow_activity_producer_is_refused_once_the_activity_is_gone() {
        let setup = wfa_setup(quick());
        setup.streams.append(wfa_append(1, "1")).await.unwrap();
        setup.owners.pending("run-1", "tool", false);
        tokio::time::advance(OWNER_RECHECK + Duration::from_secs(1)).await;
        let error = setup.streams.append(wfa_append(2, "2")).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Closed);
        assert!(error.message.contains("Activity \"tool\""), "{error}");
        assert!(wfa_chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test]
    async fn a_workflow_activity_producer_of_a_closed_run_is_refused() {
        let setup = wfa_setup(quick());
        end_run(&setup, "run-1");
        assert_eq!(
            kind(setup.streams.append(wfa_append(1, "1")).await),
            StreamFailureKind::Closed
        );
    }

    #[tokio::test]
    async fn a_workflow_delete_removes_the_streams_of_its_workflow_activities() {
        let setup = wfa_setup(quick());
        setup.streams.append(wfa_append(1, "1")).await.unwrap();
        let mut request = DeleteOwnerRequest {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::WorkflowActivity as i32,
            workflow_id: "wf".to_string(),
            ..Default::default()
        };
        assert_eq!(
            kind(setup.streams.delete_owner(request.clone()).await),
            StreamFailureKind::InvalidArgument
        );
        request.owner_kind = StreamOwnerKind::Workflow as i32;
        // A Workflow whose id only starts with `wf` keeps its Activities' streams.
        setup.owners.run("wf2", "run-9", "run-9");
        setup.owners.pending("run-9", "tool", true);
        let mut other = wfa_append(1, "1");
        other.stream.as_mut().unwrap().workflow_id = "wf2".to_string();
        other.stream.as_mut().unwrap().run_id = "run-9".to_string();
        setup.streams.append(other).await.unwrap();
        assert!(setup.streams.delete_owner(request).await.unwrap().deleted > 0);
        let gone = setup
            .store
            .latest(StoreLatestRequest {
                chain: Some(wfa_chain("run-1")),
                topic: "out".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(gone.position, "");
        let kept = setup
            .store
            .latest(StoreLatestRequest {
                chain: Some(ChainId {
                    workflow_id: "wf2".to_string(),
                    ..wfa_chain("run-9")
                }),
                topic: "out".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(kept.position, "0");
    }
}
