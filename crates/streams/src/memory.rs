//! The memory Store, for tests. It keeps the same fields as the Redis layout and runs the steps of
//! the Lua scripts in the same order, so that both Stores return the same results. Each call holds
//! one lock, since Redis runs one script at a time.

use crate::{
    Reason, StreamError, StreamResult,
    epoch::{epoch_key, task_key, task_of},
    ids::{StreamKeys, encode_part},
    offset::{entry_id_of_offset, offset_of_entry_id},
    proto::{Producer, StreamProducerKind, StreamRef},
    store::{
        Appended, Batch, Capabilities, Closed, CreateOutcome, Created, Entry, FORMAT_VERSION,
        MAX_SEQUENCE, Owner, Store, StreamState, TOMBSTONE, Withdrawal, owner_chain,
    },
};
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::Notify, time::Instant};

/// The number of sequence numbers in one millisecond, since an offset packs them in 20 bits.
const SEQ_LIMIT: u64 = 1 << crate::offset::SEQ_BITS;

#[derive(Clone, Default)]
struct Fields {
    record: Option<Vec<u8>>,
    supersession: Option<Vec<u8>>,
}

/// One Stream's meta hash and log, each with an expiry time, as Redis keys have.
#[derive(Default)]
struct Keys {
    meta: HashMap<String, String>,
    meta_expires: i64,
    /// `None` when the log key doesn't exist.
    log: Option<BTreeMap<(u64, u64), Fields>>,
    log_expires: i64,
    /// The newest id the log ever had, which Redis also keeps after a trim.
    last_id: (u64, u64),
}

/// A Store in process memory, with the rules of the Redis scripts.
pub struct MemoryStore {
    prefix: String,
    retention: Duration,
    safety_ttl: Duration,
    streams: Mutex<HashMap<String, Keys>>,
    appended: Notify,
}

impl std::fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStore")
            .field("retention", &self.retention)
            .field("safety_ttl", &self.safety_ttl)
            .finish_non_exhaustive()
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as i64)
}

fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn refuse(reason: Reason, message: &str) -> StreamError {
    StreamError::new(reason, message)
}

fn id_text((ms, seq): (u64, u64)) -> String {
    format!("{ms}-{seq}")
}

fn parse_id(text: &str) -> Option<(u64, u64)> {
    let (ms, seq) = text.split_once('-')?;
    Some((ms.parse().ok()?, seq.parse().ok()?))
}

fn words(text: Option<&String>) -> Vec<String> {
    text.map(|text| text.split_whitespace().map(str::to_string).collect())
        .unwrap_or_default()
}

fn started(task_key: &str) -> i64 {
    i64::from_str_radix(&task_key[..16], 16).unwrap_or(0)
}

/// Whether a task key at c's started id is a lower attempt than c, as `below_completed` in
/// `lib.lua`.
fn below_completed(key: &str, c: &str) -> bool {
    if &c[17..25] == "00000000" {
        return key[26..] < c[26..];
    }
    key < c
}

/// The orphan rule over a run's open Local Activity tasks, as `orphan_tasks` in `lib.lua`.
fn orphan_tasks(open: &[String], t: &str, c: &str, failed: &[i64]) -> (Vec<String>, Vec<String>) {
    let ts = started(t);
    let cs = if c.is_empty() { 0 } else { started(c) };
    let (mut withdrawn, mut kept) = (vec![], vec![]);
    for key in open {
        let s = started(key);
        if failed.contains(&s)
            || (cs < s && s < ts)
            || (s == ts && key.as_str() < t)
            || (!c.is_empty() && s == cs && below_completed(key, c))
        {
            withdrawn.push(key.clone());
        } else if c.is_empty() || key.as_str() > c {
            kept.push(key.clone());
        }
    }
    (withdrawn, kept)
}

impl Keys {
    fn get(&self, field: &str) -> Option<&String> {
        self.meta.get(field)
    }

    fn set(&mut self, field: &str, value: impl Into<String>) {
        self.meta.insert(field.to_string(), value.into());
    }

    /// The `check_live` step, which checks the create key, the format version and the expiry.
    fn check_live(&self, now: i64) -> StreamResult<()> {
        let Some(v) = self.get("v") else {
            return Err(refuse(Reason::NotFound, "nobody created the Stream"));
        };
        if v.parse::<u32>().unwrap_or(0) > FORMAT_VERSION {
            return Err(refuse(
                Reason::FormatTooNew,
                &format!("format {v} is newer than {FORMAT_VERSION}"),
            ));
        }
        if self
            .get("exp")
            .and_then(|exp| exp.parse::<i64>().ok())
            .is_some_and(|exp| exp <= now)
        {
            return Err(refuse(Reason::Expired, "the Stream expired"));
        }
        Ok(())
    }

    /// The `check_owner` step of a close, as in `lib.lua`.
    fn check_owner(&self, kind: i32, chain: &str) -> StreamResult<()> {
        if self.get("own").map(String::as_str) == Some("wf") {
            if kind != StreamProducerKind::Workflow as i32 {
                return Err(refuse(
                    Reason::NotOwner,
                    "only the close Activity of the owner run chain closes a Workflow-owned Stream",
                ));
            }
            if self.get("oc").is_some_and(|owner| chain != owner) {
                return Err(refuse(
                    Reason::NotOwner,
                    "the ref names another run chain than the owner",
                ));
            }
        } else if kind != StreamProducerKind::Client as i32 {
            return Err(refuse(
                Reason::NotOwner,
                "only a client closes a Stream that a client created",
            ));
        }
        Ok(())
    }

    fn refresh(&mut self, now: i64, safety: i64, tombstone: i64) {
        let mut ttl: i64 = self.get("ret").and_then(|r| r.parse().ok()).unwrap_or(0);
        if self.get("own").map(String::as_str) == Some("wf") && self.get("closed").is_none() {
            ttl = safety;
        }
        let exp = now + ttl;
        self.set("exp", exp.to_string());
        self.meta_expires = exp + tombstone;
        if self.log.is_some() {
            self.log_expires = exp;
        }
    }

    fn trim(&mut self, now: i64) {
        let retention: i64 = self.get("ret").and_then(|r| r.parse().ok()).unwrap_or(0);
        let min = now - retention;
        if min <= 0 {
            return;
        }
        let Some(log) = self.log.as_ref() else {
            return;
        };
        let old: Vec<_> = log.range(..(min as u64, 0)).map(|(id, _)| *id).collect();
        let Some(newest_old) = old.last().copied() else {
            return;
        };
        let moves = self
            .meta
            .get("trimmed")
            .and_then(|t| parse_id(t))
            .is_none_or(|trimmed| trimmed < newest_old);
        if moves {
            self.set("trimmed", id_text(newest_old));
        }
        if let Some(log) = self.log.as_mut() {
            for id in old {
                log.remove(&id);
            }
        }
    }

    fn withdraw_run(&mut self, run: &str) -> bool {
        if run.is_empty() || self.get(&format!("wr|{run}")).is_some() {
            return false;
        }
        self.set(&format!("wr|{run}"), "1");
        self.get(&format!("run|{run}")).is_some()
    }

    /// Refuses `count` entries whose ids would not fit an offset. The ids go on from the newest id
    /// the log ever had, as `check_ids_fit` in `lib.lua`.
    fn check_ids_fit(&self, now: i64, count: u64) -> StreamResult<()> {
        let (ms, seq) = self.last_id;
        let first = if now > ms as i64 { 0 } else { seq + 1 };
        if first + count > SEQ_LIMIT {
            return Err(refuse(
                Reason::Storage,
                "the entry ids of this millisecond would not fit an offset",
            ));
        }
        Ok(())
    }

    /// Adds an entry as `add` in `lib.lua` does, with the next id after the newest one, in this
    /// millisecond or a later one.
    fn add(&mut self, now: i64, fields: Fields) -> String {
        let now = now.max(0) as u64;
        let (last_ms, last_seq) = self.last_id;
        let id = if now > last_ms {
            (now, 0)
        } else {
            (last_ms, last_seq + 1)
        };
        self.last_id = id;
        self.log
            .get_or_insert_with(BTreeMap::new)
            .insert(id, fields);
        id_text(id)
    }
}

impl MemoryStore {
    /// An empty Store.
    pub fn new(retention: Duration, safety_ttl: Duration) -> Self {
        Self {
            prefix: crate::ids::DEFAULT_KEY_PREFIX.to_string(),
            retention,
            safety_ttl,
            streams: Mutex::default(),
            appended: Notify::new(),
        }
    }

    fn key(&self, namespace: &str, stream_id: &str) -> String {
        StreamKeys::new(&self.prefix, namespace, stream_id).meta()
    }

    /// Runs `step` on one Stream's keys after it drops the expired keys, as Redis does.
    fn with<T>(
        &self,
        namespace: &str,
        stream_id: &str,
        step: impl FnOnce(&mut Keys, i64) -> T,
    ) -> T {
        let now = now_ms();
        let mut streams = self.streams.lock();
        let key = self.key(namespace, stream_id);
        let keys = streams.entry(key.clone()).or_default();
        if keys.meta_expires > 0 && keys.meta_expires <= now {
            keys.meta.clear();
            keys.meta_expires = 0;
        }
        if keys.log_expires > 0 && keys.log_expires <= now {
            keys.log = None;
            keys.log_expires = 0;
        }
        let result = step(keys, now);
        // Once both keys are gone, Redis has no state of the Stream, not even its newest id.
        if keys.meta.is_empty() && keys.log.is_none() {
            streams.remove(&key);
        }
        result
    }

    /// Changes one meta field, for tests of what the scripts refuse.
    #[doc(hidden)]
    pub fn test_set_meta(&self, namespace: &str, stream_id: &str, field: &str, value: &str) {
        self.with(namespace, stream_id, |keys, _| keys.set(field, value));
    }

    /// One meta field, for tests.
    #[doc(hidden)]
    pub fn test_meta(&self, namespace: &str, stream_id: &str, field: &str) -> Option<String> {
        self.with(namespace, stream_id, |keys, _| keys.get(field).cloned())
    }

    /// The ms left on the log and on the meta, -2 for a key that doesn't exist, as `PTTL`.
    #[doc(hidden)]
    pub fn test_ttls(&self, namespace: &str, stream_id: &str) -> (i64, i64) {
        self.with(namespace, stream_id, |keys, now| {
            let log = if keys.log.is_some() {
                keys.log_expires - now
            } else {
                -2
            };
            let meta = if keys.meta.is_empty() {
                -2
            } else {
                keys.meta_expires - now
            };
            (log, meta)
        })
    }

    /// Adds an entry with an explicit id, as `XADD <id>`, for tests.
    #[doc(hidden)]
    pub fn test_add_entry(&self, namespace: &str, stream_id: &str, id: &str) {
        self.with(namespace, stream_id, |keys, _| {
            let id = parse_id(id).expect("an entry id");
            keys.last_id = keys.last_id.max(id);
            keys.log
                .get_or_insert_with(BTreeMap::new)
                .insert(id, Fields::default());
        });
    }

    /// The number of log entries, as `XLEN`, for tests.
    #[doc(hidden)]
    pub fn test_log_len(&self, namespace: &str, stream_id: &str) -> usize {
        self.with(namespace, stream_id, |keys, _| {
            keys.log.as_ref().map_or(0, BTreeMap::len)
        })
    }
}

fn encode_run(run_id: &str) -> String {
    if run_id.is_empty() {
        String::new()
    } else {
        encode_part(run_id)
    }
}

fn offsets(first: &str, last: &str) -> StreamResult<Option<(i64, i64)>> {
    if first.is_empty() || last.is_empty() {
        return Ok(None);
    }
    Ok(Some((
        offset_of_entry_id(first)?,
        offset_of_entry_id(last)?,
    )))
}

#[async_trait::async_trait]
impl Store for MemoryStore {
    fn capabilities(&self) -> Capabilities {
        Capabilities::ALL
    }

    fn default_retention(&self) -> Duration {
        self.retention
    }

    async fn create(
        &self,
        namespace: &str,
        stream: &StreamRef,
        retention: Option<Duration>,
        owner: Owner,
        withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Created> {
        let stream_id = stream.id.as_str();
        let retention = millis(retention.filter(|r| !r.is_zero()).unwrap_or(self.retention));
        let owner = match owner {
            Owner::Workflow => "wf",
            Owner::Client => "client",
        };
        let (safety, tombstone) = (millis(self.safety_ttl), millis(TOMBSTONE));
        let withdrawn_run = encode_run(withdrawal.withdraw_run);
        let chain = encode_run(owner_chain(stream));
        let created = self.with(namespace, stream_id, |keys, now| {
            let Some(stored) = keys.get("ret").cloned() else {
                keys.set("v", FORMAT_VERSION.to_string());
                keys.set("ret", retention.to_string());
                keys.set("own", owner);
                if owner == "wf" && !chain.is_empty() {
                    keys.set("oc", chain.clone());
                }
                keys.withdraw_run(&withdrawn_run);
                keys.refresh(now, safety, tombstone);
                return Ok((CreateOutcome::Created, retention, false));
            };
            let stored_ms: i64 = stored.parse().unwrap_or(0);
            match keys.check_live(now) {
                Err(error) if error.reason == Reason::Expired => {
                    return Ok((CreateOutcome::Expired, stored_ms, false));
                }
                Err(error) => return Err(error),
                Ok(()) => {}
            }
            if keys.get("closed").is_some() {
                return Ok((CreateOutcome::Closed, stored_ms, false));
            }
            let outcome = if stored_ms != retention {
                CreateOutcome::Conflict
            } else {
                CreateOutcome::Exists
            };
            if !withdrawn_run.is_empty() {
                keys.check_ids_fit(now, 1)?;
            }
            if owner == "wf" && keys.get("own").map(String::as_str) != Some("wf") {
                keys.set("own", "wf");
                if !chain.is_empty() {
                    keys.set("oc", chain.clone());
                }
                keys.refresh(now, safety, tombstone);
            }
            let mut flagged = false;
            if keys.withdraw_run(&withdrawn_run) {
                let id = keys.add(
                    now,
                    Fields {
                        record: None,
                        supersession: Some(withdrawal.supersession.to_vec()),
                    },
                );
                keys.set("newest", id);
                keys.refresh(now, safety, tombstone);
                flagged = true;
            }
            Ok((outcome, stored_ms, flagged))
        });
        let (outcome, stored, flagged) = created?;
        if flagged {
            self.appended.notify_waiters();
        }
        Ok(Created {
            outcome,
            retention: Duration::from_millis(stored.max(0) as u64),
            flagged,
        })
    }

    async fn append(
        &self,
        namespace: &str,
        stream: &StreamRef,
        batch: &Batch<'_>,
    ) -> StreamResult<Appended> {
        let stream_id = stream.id.as_str();
        let producer = batch.producer;
        let epoch = epoch_key(&producer.epoch.unwrap_or_default());
        let run = encode_run(&producer.run_id);
        let withdrawal = &batch.withdrawal;
        let withdrawn_run = encode_run(withdrawal.withdraw_run);
        let (safety, tombstone) = (millis(self.safety_ttl), millis(TOMBSTONE));
        let sequence = batch.sequence;
        let suffix = format!("|{}|{}", producer.kind, producer.id);
        let local_activity = producer.kind == StreamProducerKind::LocalActivity as i32;
        let task = if local_activity {
            task_key(&task_of(&producer.epoch.unwrap_or_default()))
        } else {
            String::new()
        };
        let facts = withdrawal
            .tasks
            .filter(|_| local_activity && !run.is_empty());
        let completed = facts
            .and_then(|facts| facts.completed)
            .map(task_key)
            .unwrap_or_default();
        let (open_field, gone_field) = (format!("oa|{run}"), format!("ow|{run}"));
        let appended = self.with(namespace, stream_id, |keys, now| {
            if !(1..=MAX_SEQUENCE).contains(&sequence) {
                return Err(refuse(
                    Reason::Invalid,
                    "the sequence must be from 1 to 2^53 - 1",
                ));
            }
            keys.check_live(now)?;
            if let Some(last) = keys.get(&format!("d{suffix}")) {
                let parts: Vec<&str> = last.split('|').collect();
                if parts.len() == 5 && parts[0] == epoch {
                    let last_sequence: i64 = parts[1].parse().unwrap_or(0);
                    if sequence == last_sequence {
                        if parts[4] != batch.digest {
                            return Err(refuse(
                                Reason::SeqDivergent,
                                &format!(
                                    "sequence {sequence} has other content than the batch the \
                                     Store has"
                                ),
                            ));
                        }
                        return Ok((true, parts[2].to_string(), parts[3].to_string(), false));
                    }
                    if sequence < last_sequence {
                        return Err(refuse(
                            Reason::Sequence,
                            &format!("sequence {sequence} is below the last one, {last_sequence}"),
                        ));
                    }
                }
            }
            if keys.get("closed").is_some() {
                return Err(refuse(Reason::Closed, "the Stream is closed"));
            }
            if !run.is_empty() && keys.get(&format!("wr|{run}")).is_some() {
                return Err(refuse(
                    Reason::Fenced,
                    "a Workflow retry withdrew the run of the Producer",
                ));
            }
            if keys
                .get(&format!("e{suffix}"))
                .is_some_and(|newest| epoch.as_str() < newest.as_str())
            {
                return Err(refuse(
                    Reason::Fenced,
                    "the epoch is lower than the newest one of the Producer",
                ));
            }
            if local_activity && !run.is_empty() && words(keys.get(&gone_field)).contains(&task) {
                return Err(refuse(
                    Reason::Fenced,
                    "a Superseded item withdrew the Workflow Task of the call",
                ));
            }
            let records = batch.records.len() as u64;
            keys.check_ids_fit(now, records + 2)?;

            // A client's newer epoch only fences, as in `append.lua`.
            let mut flagged = producer.kind != StreamProducerKind::Client as i32
                && keys
                    .get(&format!("h{suffix}"))
                    .is_some_and(|holds| holds.as_str() < epoch.as_str());
            if keys.withdraw_run(&withdrawn_run) {
                flagged = true;
            }
            let (withdrawn, mut open) = match facts {
                Some(facts) => orphan_tasks(
                    &words(keys.get(&open_field)),
                    &task,
                    &completed,
                    facts.failed_tasks,
                ),
                None => (vec![], vec![]),
            };

            keys.trim(now);
            let (mut first, mut last) = (String::new(), String::new());
            // The orphan item has an entry of its own, as in `append.lua`.
            let mut orphan = String::new();
            if let (false, Some(facts)) = (withdrawn.is_empty(), facts) {
                orphan = keys.add(
                    now,
                    Fields {
                        record: None,
                        supersession: Some(facts.supersession.to_vec()),
                    },
                );
                last = orphan.clone();
            }
            if records == 0 && flagged {
                last = keys.add(
                    now,
                    Fields {
                        record: None,
                        supersession: Some(withdrawal.supersession.to_vec()),
                    },
                );
                first = last.clone();
            }
            for (i, record) in batch.records.iter().enumerate() {
                let supersession = (i == 0 && flagged).then(|| withdrawal.supersession.to_vec());
                last = keys.add(
                    now,
                    Fields {
                        record: Some(record.clone()),
                        supersession,
                    },
                );
                if first.is_empty() {
                    first = last.clone();
                }
            }

            if first.is_empty() {
                first = orphan;
            }
            keys.set(&format!("e{suffix}"), epoch.clone());
            keys.set(
                &format!("d{suffix}"),
                format!("{epoch}|{sequence}|{first}|{last}|{}", batch.digest),
            );
            if records > 0 {
                keys.set(&format!("h{suffix}"), epoch.clone());
            } else if flagged {
                keys.meta.remove(&format!("h{suffix}"));
            }
            if !run.is_empty() && records > 0 {
                keys.set(&format!("run|{run}"), "1");
            }
            if facts.is_some() {
                if records > 0 && !open.contains(&task) {
                    open.push(task.clone());
                }
                if open.is_empty() {
                    keys.meta.remove(&open_field);
                } else {
                    keys.set(&open_field, open.join(" "));
                }
                if !withdrawn.is_empty() {
                    let mut all = words(keys.get(&gone_field));
                    all.extend(withdrawn.iter().cloned());
                    keys.set(&gone_field, all.join(" "));
                }
            }
            if !last.is_empty() {
                keys.set("newest", last.clone());
            }
            keys.refresh(now, safety, tombstone);
            Ok((false, first, last, flagged || !withdrawn.is_empty()))
        });
        let (resend, first, last, flagged) = appended?;
        if !resend && !last.is_empty() {
            self.appended.notify_waiters();
        }
        Ok(Appended {
            offsets: offsets(&first, &last)?,
            resend,
            flagged,
        })
    }

    async fn close(
        &self,
        namespace: &str,
        stream: &StreamRef,
        producer: &Producer,
        record: &[u8],
    ) -> StreamResult<Closed> {
        let stream_id = stream.id.as_str();
        let epoch = epoch_key(&producer.epoch.unwrap_or_default());
        let run = encode_run(&producer.run_id);
        let suffix = format!("|{}|{}", producer.kind, producer.id);
        let tombstone = millis(TOMBSTONE);
        let chain = encode_run(owner_chain(stream));
        let closed = self.with(namespace, stream_id, |keys, now| {
            keys.check_live(now)?;
            keys.check_owner(producer.kind, &chain)?;
            if keys.get("closed").is_some() {
                let id = keys.get("close_id").cloned().unwrap_or_default();
                return Ok((id, true));
            }
            if !run.is_empty() && keys.get(&format!("wr|{run}")).is_some() {
                return Err(refuse(
                    Reason::Fenced,
                    "a Workflow retry withdrew the run of the Producer",
                ));
            }
            if keys
                .get(&format!("e{suffix}"))
                .is_some_and(|newest| epoch.as_str() < newest.as_str())
            {
                return Err(refuse(
                    Reason::Fenced,
                    "the epoch is lower than the newest one of the Producer",
                ));
            }
            keys.check_ids_fit(now, 1)?;
            keys.trim(now);
            let id = keys.add(
                now,
                Fields {
                    record: Some(record.to_vec()),
                    supersession: None,
                },
            );
            keys.set("closed", "1");
            keys.set("close_id", id.clone());
            keys.set("newest", id.clone());
            keys.refresh(now, 0, tombstone);
            Ok((id, false))
        });
        let (id, already) = closed?;
        if !already {
            self.appended.notify_waiters();
        }
        Ok(Closed {
            offset: offset_of_entry_id(&id)?,
            already,
        })
    }

    async fn read(
        &self,
        namespace: &str,
        stream_id: &str,
        from: i64,
        count: usize,
    ) -> StreamResult<(StreamState, Vec<Entry>)> {
        let start = if from > 0 {
            parse_id(&entry_id_of_offset(from)).unwrap_or((0, 0))
        } else {
            (0, 0)
        };
        self.with(namespace, stream_id, |keys, now| {
            let offset = |field: &str| -> StreamResult<i64> {
                keys.get(field).map_or(Ok(0), |id| offset_of_entry_id(id))
            };
            let number =
                |field: &str| -> i64 { keys.get(field).and_then(|n| n.parse().ok()).unwrap_or(0) };
            let state = StreamState {
                exists: keys.get("v").is_some(),
                version: number("v") as u32,
                closed_at: if keys.get("closed").is_some() {
                    Some(offset("close_id")?)
                } else {
                    None
                },
                newest: offset("newest")?,
                trimmed: offset("trimmed")?,
                retention: Duration::from_millis(number("ret").max(0) as u64),
                expires_ms: number("exp"),
                now_ms: now,
            };
            let mut entries = vec![];
            if let Some(log) = keys.log.as_ref() {
                for (id, fields) in log.range(start..).take(count) {
                    entries.push(Entry {
                        offset: offset_of_entry_id(&id_text(*id))?,
                        record: fields.record.clone(),
                        supersession: fields.supersession.clone(),
                    });
                }
            }
            Ok((state, entries))
        })
    }

    async fn read_back(
        &self,
        namespace: &str,
        stream_id: &str,
        before: i64,
        count: usize,
    ) -> StreamResult<Vec<Entry>> {
        let before = parse_id(&entry_id_of_offset(before)).unwrap_or((0, 0));
        self.with(namespace, stream_id, |keys, _| {
            let Some(log) = keys.log.as_ref() else {
                return Ok(vec![]);
            };
            log.range(..=before)
                .rev()
                .take(count)
                .map(|(id, fields)| {
                    Ok(Entry {
                        offset: offset_of_entry_id(&id_text(*id))?,
                        record: fields.record.clone(),
                        supersession: fields.supersession.clone(),
                    })
                })
                .collect()
        })
    }

    async fn wait_after(
        &self,
        namespace: &str,
        stream_id: &str,
        after: i64,
        deadline: Instant,
    ) -> StreamResult<bool> {
        let after = if after > 0 {
            parse_id(&entry_id_of_offset(after)).unwrap_or((0, 0))
        } else {
            (0, 0)
        };
        loop {
            // Listen before the check, so an append between the two still wakes the wait.
            let appended = self.appended.notified();
            let found = self.with(namespace, stream_id, |keys, _| {
                keys.log
                    .as_ref()
                    .and_then(|log| log.keys().next_back())
                    .is_some_and(|id| *id > after)
            });
            if found {
                return Ok(true);
            }
            if tokio::time::timeout_at(deadline, appended).await.is_err() {
                return Ok(false);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_id_follows_the_newest_one_within_a_millisecond() {
        let mut keys = Keys::default();
        assert_eq!(keys.add(5, Fields::default()), "5-0");
        assert_eq!(keys.add(5, Fields::default()), "5-1");
        // The ids keep increasing when the clock steps back.
        assert_eq!(keys.add(3, Fields::default()), "5-2");
        assert_eq!(keys.add(9, Fields::default()), "9-0");
    }
}
