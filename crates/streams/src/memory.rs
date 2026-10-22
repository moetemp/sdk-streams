//! The memory Store, for tests. It keeps the same fields as the Redis layout and runs the steps
//! of the Lua scripts in the same order, so both Stores give the same answers. Each call holds
//! one lock, as one script runs alone on Redis.

use crate::{
    Reason, StreamError, StreamResult,
    epoch::{epoch_key, task_key, task_of},
    ids::{StreamKeys, encode_part},
    offset::{entry_id_of_offset, offset_of_entry_id},
    proto::{Producer, ProducerKind},
    store::{
        Appended, Batch, Capabilities, Closed, CreateOutcome, Created, Entry, FORMAT_VERSION,
        Owner, Store, StreamState, TOMBSTONE, Withdrawal,
    },
};
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::Notify, time::Instant};

/// The close Record of an auto close, as `AUTO_CLOSE_RECORD` in `lib.lua`.
const AUTO_CLOSE_RECORD: &[u8] = b"\x08\x03";

/// The most sequence numbers of one millisecond, as an offset packs them in 20 bits.
const SEQ_LIMIT: u64 = 1 << crate::offset::SEQ_BITS;

#[derive(Clone, Default)]
struct Fields {
    record: Option<Vec<u8>>,
    supersession: Option<Vec<u8>>,
}

/// One Stream: the meta hash and the log, each with the time it expires, as Redis keys have.
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

/// The Workflow withdrawal rule over a task index, as `withdraw_tasks` in `lib.lua`.
fn withdraw_tasks(
    index: &[String],
    t: &str,
    c: &str,
    failed: &[i64],
) -> (Vec<String>, Vec<String>) {
    let ts = started(t);
    let cs = if c.is_empty() { 0 } else { started(c) };
    let (mut withdrawn, mut kept) = (vec![], vec![]);
    for key in index {
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

    /// The `check_live` step: the create key, the format version and the expiry.
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

    fn keep_facts(&mut self, run: &str, t: &str, c: &str, failed: &str) {
        self.set(&format!("lf|{run}"), format!("{t}|{c}|{failed}"));
    }

    fn withdrawn_by_facts(&self, run: &str, task: &str) -> bool {
        let Some(facts) = self.get(&format!("lf|{run}")) else {
            return false;
        };
        let mut parts = facts.splitn(3, '|');
        let (t, c, failed) = (
            parts.next().unwrap_or(""),
            parts.next().unwrap_or(""),
            parts.next().unwrap_or(""),
        );
        let failed: Vec<i64> = failed
            .split_whitespace()
            .filter_map(|id| id.parse().ok())
            .collect();
        !withdraw_tasks(&[task.to_string()], t, c, &failed)
            .0
            .is_empty()
    }

    fn withdraw_run(&mut self, run: &str) -> bool {
        if run.is_empty() || self.get(&format!("wr|{run}")).is_some() {
            return false;
        }
        self.set(&format!("wr|{run}"), "1");
        self.get(&format!("run|{run}")).is_some()
    }

    fn check_ids_fit(&self, now: i64, count: u64) -> StreamResult<()> {
        let Some((ms, seq)) = self
            .log
            .as_ref()
            .and_then(|log| log.keys().next_back().copied())
        else {
            return Ok(());
        };
        if ms as i64 >= now && seq + count >= SEQ_LIMIT {
            return Err(refuse(
                Reason::Storage,
                "the entry ids of this millisecond would not fit an offset",
            ));
        }
        Ok(())
    }

    /// The `auto_close` step: closes an open Stream whose period passed since its last append.
    /// Answers true when the Stream is closed after the step.
    fn auto_close(&mut self, now: i64, tombstone: i64) -> bool {
        if self.get("closed").is_some() {
            return true;
        }
        let Some(period) = self.get("ac").and_then(|ac| ac.parse::<i64>().ok()) else {
            return false;
        };
        let last: i64 = self.get("la").and_then(|la| la.parse().ok()).unwrap_or(0);
        if now < last.saturating_add(period) || self.check_ids_fit(now, 1).is_err() {
            return false;
        }
        self.trim(now);
        let id = self.add(
            now,
            Fields {
                record: Some(AUTO_CLOSE_RECORD.to_vec()),
                supersession: None,
            },
        );
        self.set("closed", "1");
        self.set("close_id", id.clone());
        self.set("newest", id);
        self.refresh(now, 0, tombstone);
        true
    }

    /// `XADD *`: the next id after the newest one, in this millisecond or a later one.
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

    /// Runs `step` on one Stream's keys after the expired keys go, as Redis drops them.
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
        // Redis forgets a Stream, its newest id too, once both keys are gone.
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

fn failed_text(failed: &[i64]) -> String {
    failed
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(" ")
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
    // A process has no replicas to wait for.
    fn capabilities(&self) -> Capabilities {
        Capabilities::ALL.without(Capabilities::REPLICA_WAIT)
    }

    fn default_retention(&self) -> Duration {
        self.retention
    }

    async fn create(
        &self,
        namespace: &str,
        stream_id: &str,
        retention: Option<Duration>,
        auto_close: Option<Duration>,
        owner: Owner,
        withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Created> {
        let retention = millis(retention.filter(|r| !r.is_zero()).unwrap_or(self.retention));
        let period = auto_close
            .filter(|period| !period.is_zero())
            // Stored in ms, so a period below 1 ms rounds up and never stores as no wait.
            .map(|period| millis(period).max(1).to_string())
            .unwrap_or_default();
        let owner = match owner {
            Owner::Workflow => "wf",
            Owner::Client => "client",
        };
        let (safety, tombstone) = (millis(self.safety_ttl), millis(TOMBSTONE));
        let withdrawn_run = encode_run(withdrawal.withdraw_run);
        let created = self.with(namespace, stream_id, |keys, now| {
            let Some(stored) = keys.get("ret").cloned() else {
                keys.set("v", FORMAT_VERSION.to_string());
                keys.set("ret", retention.to_string());
                keys.set("own", owner);
                if !period.is_empty() {
                    keys.set("ac", period.clone());
                    keys.set("la", now.to_string());
                }
                keys.withdraw_run(&withdrawn_run);
                keys.refresh(now, safety, tombstone);
                return Ok((CreateOutcome::Created, retention, false, period.clone()));
            };
            let stored_ms: i64 = stored.parse().unwrap_or(0);
            let stored_period = keys.get("ac").cloned().unwrap_or_default();
            match keys.check_live(now) {
                Err(error) if error.reason == Reason::Expired => {
                    return Ok((CreateOutcome::Expired, stored_ms, false, stored_period));
                }
                Err(error) => return Err(error),
                Ok(()) => {}
            }
            if keys.auto_close(now, tombstone) {
                return Ok((CreateOutcome::Closed, stored_ms, false, stored_period));
            }
            let outcome = if stored_ms != retention || stored_period != period {
                CreateOutcome::Conflict
            } else {
                CreateOutcome::Exists
            };
            if owner == "wf" && keys.get("own").map(String::as_str) != Some("wf") {
                keys.set("own", "wf");
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
            Ok((outcome, stored_ms, flagged, stored_period))
        });
        let (outcome, stored, flagged, period) = created?;
        // A create that finds an auto close due writes the close Record.
        if flagged || outcome == CreateOutcome::Closed {
            self.appended.notify_waiters();
        }
        Ok(Created {
            outcome,
            retention: Duration::from_millis(stored.max(0) as u64),
            flagged,
            auto_close: period.parse().ok().map(Duration::from_millis),
        })
    }

    async fn append(
        &self,
        namespace: &str,
        stream_id: &str,
        batch: &Batch<'_>,
    ) -> StreamResult<Appended> {
        let producer = batch.producer;
        let epoch = producer.epoch.unwrap_or_default();
        let kind = ProducerKind::try_from(producer.kind).unwrap_or_default();
        let (workflow, local_activity) = (
            kind == ProducerKind::Workflow,
            kind == ProducerKind::LocalActivity,
        );
        let task = if workflow || local_activity {
            task_key(&task_of(&epoch))
        } else {
            String::new()
        };
        let epoch = epoch_key(&epoch);
        let run = encode_run(&producer.run_id);
        let withdrawal = &batch.withdrawal;
        let completed = withdrawal
            .previous_completed
            .map(task_key)
            .unwrap_or_default();
        let failed = withdrawal.failed_tasks;
        let withdrawn_run = encode_run(withdrawal.withdraw_run);
        let (safety, tombstone) = (millis(self.safety_ttl), millis(TOMBSTONE));
        let sequence = batch.sequence;
        let suffix = format!("|{}|{}", producer.kind, producer.id);
        let appended = self.with(namespace, stream_id, |keys, now| {
            if sequence < 1 {
                return Err(refuse(Reason::Invalid, "the sequence must be 1 or more"));
            }
            keys.check_live(now)?;
            if let Some(last) = keys.get(&format!("d{suffix}")) {
                let parts: Vec<&str> = last.split('|').collect();
                if parts.len() == 4 && parts[0] == epoch {
                    let last_sequence: i64 = parts[1].parse().unwrap_or(0);
                    if sequence == last_sequence {
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
            if keys.auto_close(now, tombstone) {
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
            if local_activity
                && (words(keys.get(&format!("wt|{run}"))).contains(&task)
                    || keys.withdrawn_by_facts(&run, &task))
            {
                return Err(refuse(
                    Reason::Fenced,
                    "a Superseded item withdrew the Workflow Task of the call",
                ));
            }
            let records = batch.records.len() as u64;
            keys.check_ids_fit(now, records + 1)?;

            let index_field = format!("ti|{run}");
            let withdrawn_field = format!("wt|{run}");
            let mut index = words(keys.get(&index_field));
            let mut withdrawn = vec![];
            let mut flagged;
            if workflow {
                (withdrawn, index) = withdraw_tasks(&index, &task, &completed, failed);
                flagged = !withdrawn.is_empty();
                keys.keep_facts(&run, &task, &completed, &failed_text(failed));
            } else {
                flagged = keys
                    .get(&format!("h{suffix}"))
                    .is_some_and(|holds| holds.as_str() < epoch.as_str());
            }
            if keys.withdraw_run(&withdrawn_run) {
                flagged = true;
            }

            keys.trim(now);
            let (mut first, mut last) = (String::new(), String::new());
            if records == 0 && flagged {
                first = keys.add(
                    now,
                    Fields {
                        record: None,
                        supersession: Some(withdrawal.supersession.to_vec()),
                    },
                );
                last = first.clone();
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

            keys.set(&format!("e{suffix}"), epoch.clone());
            keys.set(
                &format!("d{suffix}"),
                format!("{epoch}|{sequence}|{first}|{last}"),
            );
            if !workflow {
                if records > 0 {
                    keys.set(&format!("h{suffix}"), epoch.clone());
                } else if flagged {
                    keys.meta.remove(&format!("h{suffix}"));
                }
            }
            if (workflow || local_activity) && records > 0 && !index.contains(&task) {
                index.push(task.clone());
            }
            if workflow || local_activity {
                if index.is_empty() {
                    keys.meta.remove(&index_field);
                } else {
                    keys.set(&index_field, index.join(" "));
                }
            }
            if !withdrawn.is_empty() {
                let mut all = words(keys.get(&withdrawn_field));
                all.extend(withdrawn);
                keys.set(&withdrawn_field, all.join(" "));
            }
            if !run.is_empty() && records > 0 {
                keys.set(&format!("run|{run}"), "1");
            }
            if records > 0 && keys.get("ac").is_some() {
                keys.set("la", now.to_string());
            }
            if !last.is_empty() {
                keys.set("newest", last.clone());
            }
            keys.refresh(now, safety, tombstone);
            Ok((false, first, last, flagged))
        });
        // A refusal may come from an auto close that this step wrote, so waiters look again.
        if appended
            .as_ref()
            .is_err_and(|error| error.reason == Reason::Closed)
        {
            self.appended.notify_waiters();
        }
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
        stream_id: &str,
        producer: &Producer,
        record: &[u8],
        withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Closed> {
        let epoch = producer.epoch.unwrap_or_default();
        let kind = ProducerKind::try_from(producer.kind).unwrap_or_default();
        let task = match kind {
            ProducerKind::Workflow | ProducerKind::LocalActivity => task_key(&task_of(&epoch)),
            _ => String::new(),
        };
        let epoch = epoch_key(&epoch);
        let run = encode_run(&producer.run_id);
        let completed = withdrawal
            .previous_completed
            .map(task_key)
            .unwrap_or_default();
        let failed = withdrawal.failed_tasks;
        let tombstone = millis(TOMBSTONE);
        let closed = self.with(namespace, stream_id, |keys, now| {
            keys.check_live(now)?;
            if keys.auto_close(now, tombstone) {
                let id = keys.get("close_id").cloned().unwrap_or_default();
                return Ok((id, true, false));
            }
            if !run.is_empty() && keys.get(&format!("wr|{run}")).is_some() {
                return Err(refuse(
                    Reason::Fenced,
                    "a Workflow retry withdrew the run of the Producer",
                ));
            }
            keys.check_ids_fit(now, 1)?;
            if kind != ProducerKind::Workflow {
                let suffix = format!("|{}|{}", producer.kind, producer.id);
                if keys
                    .get(&format!("e{suffix}"))
                    .is_some_and(|newest| epoch.as_str() < newest.as_str())
                {
                    return Err(refuse(
                        Reason::Fenced,
                        "the epoch is lower than the newest one of the Producer",
                    ));
                }
                if kind == ProducerKind::LocalActivity
                    && (words(keys.get(&format!("wt|{run}"))).contains(&task)
                        || keys.withdrawn_by_facts(&run, &task))
                {
                    return Err(refuse(
                        Reason::Fenced,
                        "a Superseded item withdrew the Workflow Task of the call",
                    ));
                }
            }
            let index_field = format!("ti|{run}");
            let withdrawn_field = format!("wt|{run}");
            let mut withdrawn = vec![];
            let mut flagged = false;
            if kind == ProducerKind::Workflow {
                let index;
                (withdrawn, index) =
                    withdraw_tasks(&words(keys.get(&index_field)), &task, &completed, failed);
                flagged = !withdrawn.is_empty();
                keys.keep_facts(&run, &task, &completed, &failed_text(failed));
                if index.is_empty() {
                    keys.meta.remove(&index_field);
                } else {
                    keys.set(&index_field, index.join(" "));
                }
            }
            keys.trim(now);
            let id = keys.add(
                now,
                Fields {
                    record: Some(record.to_vec()),
                    supersession: flagged.then(|| withdrawal.supersession.to_vec()),
                },
            );
            if !withdrawn.is_empty() {
                let mut all = words(keys.get(&withdrawn_field));
                all.extend(withdrawn);
                keys.set(&withdrawn_field, all.join(" "));
            }
            keys.set("closed", "1");
            keys.set("close_id", id.clone());
            keys.set("newest", id.clone());
            keys.refresh(now, 0, tombstone);
            Ok((id, false, flagged))
        });
        let (id, already, flagged) = closed?;
        // An auto close that this step wrote also answers `already`.
        self.appended.notify_waiters();
        Ok(Closed {
            offset: offset_of_entry_id(&id)?,
            already,
            flagged,
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
                auto_close: keys
                    .get("ac")
                    .map(|_| Duration::from_millis(number("ac").max(0) as u64)),
                last_append_ms: number("la"),
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

    async fn auto_close(&self, namespace: &str, stream_id: &str) -> StreamResult<Option<i64>> {
        let tombstone = millis(TOMBSTONE);
        let closed = self.with(namespace, stream_id, |keys, now| {
            keys.check_live(now)?;
            let was_open = keys.get("closed").is_none();
            if !keys.auto_close(now, tombstone) {
                return Ok((None, false));
            }
            let id = keys.get("close_id").cloned().unwrap_or_default();
            Ok((Some(id), was_open))
        });
        let (id, wrote) = closed?;
        if wrote {
            self.appended.notify_waiters();
        }
        id.map(|id| offset_of_entry_id(&id)).transpose()
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
        // A clock that steps back keeps the ids increasing.
        assert_eq!(keys.add(3, Fields::default()), "5-2");
        assert_eq!(keys.add(9, Fields::default()), "9-0");
    }
}
