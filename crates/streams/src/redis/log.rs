//! The notification log on Redis: one stream per Cluster slot, read by drivers as one consumer
//! group, and a few doorbell streams that say which slot took its first entry.

use super::{
    conn::{BlockingPool, Node, Shared},
    errors::{Call, Mapped},
    keys::{archive_pending, chain_of_log, doorbell, notifications},
    slots::SLOTS,
};
use crate::{ArchiveMark, NotificationEntry, NotificationLog, StreamError, StreamResult};
use redis::{RedisError, Value};
use std::{collections::BTreeSet, time::Duration};
use tokio::time::Instant;

/// The consumer group every driver reads as.
pub(crate) const GROUP: &str = "streams-driver";
/// The entry fields: the log key the append wrote, and the append's last position.
const KEY_FIELD: &str = "k";
const POSITION_FIELD: &str = "p";
/// The doorbell entry field that names the slot.
const SLOT_FIELD: &str = "s";
/// A doorbell only ever needs its newest rings.
const DOORBELL_LENGTH: u64 = 1000;

/// The notification log of one [super::RedisStore].
pub(crate) struct RedisLog {
    shared: Shared,
    blocking: BlockingPool,
    prefix: String,
    doorbells: u32,
    /// Whether the store marks topics for an archive, so the slots' sets are worth reading.
    archive: bool,
}

impl RedisLog {
    pub(crate) fn new(
        shared: Shared,
        blocking: BlockingPool,
        prefix: String,
        doorbells: u32,
        archive: bool,
    ) -> Self {
        Self {
            shared,
            blocking,
            prefix,
            doorbells: doorbells.clamp(1, SLOTS as u32),
            archive,
        }
    }

    /// Rings the doorbell of `slot`, after an append made its entry the slot's only one. It runs
    /// apart from the append, so a ring can be lost. The sweep finds the entry then.
    pub(crate) async fn ring(&self, slot: u16) {
        let rung: StreamResult<String> = redis::cmd("XADD")
            .arg(doorbell(
                &self.prefix,
                u32::from(slot) % self.doorbells,
                self.doorbells,
            ))
            .arg("MAXLEN")
            .arg("~")
            .arg(DOORBELL_LENGTH)
            .arg("*")
            .arg(SLOT_FIELD)
            .arg(slot)
            .query_async(&mut self.shared.clone())
            .await
            .mapped(Call::Write);
        if let Err(error) = rung {
            tracing::debug!(slot, %error, "Could not ring a notification doorbell");
        }
    }

    /// Makes the driver group on `key`. Answers `false` when the key doesn't exist and
    /// `create_key` is off, since a slot's stream that never took an entry needs no group.
    async fn create_group(&self, key: &str, create_key: bool) -> StreamResult<bool> {
        // From the start, so entries and rings from before the first driver count.
        let mut create = redis::cmd("XGROUP");
        create.arg("CREATE").arg(key).arg(GROUP).arg("0");
        if create_key {
            create.arg("MKSTREAM");
        }
        match create.query_async::<()>(&mut self.shared.clone()).await {
            Ok(()) => Ok(true),
            Err(error) if error.code() == Some("BUSYGROUP") => Ok(true),
            Err(error)
                if !create_key && error.to_string().contains("requires the key to exist") =>
            {
                Ok(false)
            }
            Err(error) => Err(error).mapped(Call::Write),
        }
    }

    /// Reads `key` for `consumer` from `from`: `0` for the consumer's own unacked entries, `>`
    /// for new ones. Creates the group once when it is missing.
    async fn read_group(
        &self,
        key: &str,
        consumer: &str,
        from: &str,
        count: usize,
    ) -> StreamResult<Vec<(String, Vec<(String, Value)>)>> {
        for first in [true, false] {
            let read: Result<Value, RedisError> = redis::cmd("XREADGROUP")
                .arg("GROUP")
                .arg(GROUP)
                .arg(consumer)
                .arg("COUNT")
                .arg(count.max(1))
                .arg("STREAMS")
                .arg(key)
                .arg(from)
                .query_async(&mut self.shared.clone())
                .await;
            match read {
                Ok(reply) => return Ok(stream_entries(reply)),
                Err(error) if first && error.code() == Some("NOGROUP") => {
                    if !self.create_group(key, false).await? {
                        return Ok(vec![]);
                    }
                }
                Err(error) => return Err(error).mapped(Call::Read),
            }
        }
        Ok(vec![])
    }

    /// The slots each primary serves: the whole range for one server.
    async fn primaries(&self) -> StreamResult<Vec<(Option<Node>, Vec<u16>)>> {
        if let Shared::Single(_) = self.shared {
            return Ok(vec![(None, (0..SLOTS as u16).collect())]);
        }
        let reply: Value = redis::cmd("CLUSTER")
            .arg("SLOTS")
            .query_async(&mut self.shared.clone())
            .await
            .mapped(Call::Read)?;
        let mut primaries: Vec<(Option<Node>, Vec<u16>)> = vec![];
        let Value::Array(ranges) = reply else {
            return Err(StreamError::storage(format!(
                "Redis answered CLUSTER SLOTS with {reply:?}"
            )));
        };
        for range in ranges {
            let Value::Array(range) = range else { continue };
            let (Some(Value::Int(start)), Some(Value::Int(end)), Some(Value::Array(node))) =
                (range.first(), range.get(1), range.get(2))
            else {
                continue;
            };
            let host = match node.first() {
                Some(Value::BulkString(host)) => String::from_utf8_lossy(host).into_owned(),
                Some(Value::SimpleString(host)) => host.clone(),
                _ => continue,
            };
            let Some(Value::Int(port)) = node.get(1) else {
                continue;
            };
            let node = Some((host, *port as u16));
            let slots = (*start as u16..=*end as u16).collect::<Vec<_>>();
            match primaries.iter_mut().find(|(known, _)| *known == node) {
                Some((_, known)) => known.extend(slots),
                None => primaries.push((node, slots)),
            }
        }
        Ok(primaries)
    }
}

#[async_trait::async_trait]
impl NotificationLog for RedisLog {
    fn partitions(&self) -> u32 {
        SLOTS as u32
    }

    fn doorbells(&self) -> u32 {
        self.doorbells
    }

    async fn wait_doorbell(
        &self,
        doorbell_index: u32,
        consumer: &str,
        block: Duration,
    ) -> StreamResult<Vec<u32>> {
        let key = doorbell(&self.prefix, doorbell_index, self.doorbells);
        let deadline = Instant::now() + block;
        let mut read = redis::cmd("XREADGROUP");
        read.arg("GROUP")
            .arg(GROUP)
            .arg(consumer)
            .arg("COUNT")
            .arg(DOORBELL_LENGTH)
            .arg("BLOCK")
            .arg(u64::try_from(block.as_millis()).unwrap_or(u64::MAX).max(1))
            .arg("STREAMS")
            .arg(&key)
            .arg(">");
        let mut reply = None;
        for first in [true, false] {
            match self.blocking.query(&key, &read, deadline).await {
                Ok(answer) => {
                    reply = answer;
                    break;
                }
                Err(error) if first && error.code() == Some("NOGROUP") => {
                    self.create_group(&key, true).await?;
                }
                Err(error) => return Err(error).mapped(Call::Read),
            }
        }
        let rings = reply.map(stream_entries).unwrap_or_default();
        if rings.is_empty() {
            return Ok(vec![]);
        }
        // A ring is a hint, so it goes as soon as it is read.
        let ids: Vec<&str> = rings.iter().map(|(id, _)| id.as_str()).collect();
        let mut pipeline = redis::pipe();
        pipeline
            .cmd("XACK")
            .arg(&key)
            .arg(GROUP)
            .arg(&ids)
            .ignore()
            .cmd("XDEL")
            .arg(&key)
            .arg(&ids)
            .ignore();
        pipeline
            .query_async::<()>(&mut self.shared.clone())
            .await
            .mapped(Call::Write)?;
        let slots: BTreeSet<u32> = rings
            .iter()
            .filter_map(|(_, fields)| text_field(fields, SLOT_FIELD)?.parse().ok())
            .collect();
        Ok(slots.into_iter().collect())
    }

    async fn take(
        &self,
        partition: u32,
        consumer: &str,
        claim_idle: Duration,
        max: usize,
    ) -> StreamResult<Vec<NotificationEntry>> {
        let key = notifications(&self.prefix, partition as u16);
        // Claimed entries join this consumer's own unacked ones, which the read from `0` gives.
        let claim: Result<Value, RedisError> = redis::cmd("XAUTOCLAIM")
            .arg(&key)
            .arg(GROUP)
            .arg(consumer)
            .arg(u64::try_from(claim_idle.as_millis()).unwrap_or(u64::MAX))
            .arg("0-0")
            .arg("COUNT")
            .arg(max.max(1))
            .arg("JUSTID")
            .query_async(&mut self.shared.clone())
            .await;
        match claim {
            Ok(_) => {}
            Err(error) if error.code() == Some("NOGROUP") => {
                if !self.create_group(&key, false).await? {
                    return Ok(vec![]);
                }
            }
            Err(error) => return Err(error).mapped(Call::Write),
        }
        let mut entries = self.read_group(&key, consumer, "0", max).await?;
        if entries.len() < max {
            entries.extend(
                self.read_group(&key, consumer, ">", max - entries.len())
                    .await?,
            );
        }
        let mut taken = vec![];
        let mut broken = vec![];
        for (id, fields) in entries {
            let parsed = text_field(&fields, KEY_FIELD)
                .and_then(|key| chain_of_log(&self.prefix, &key))
                .zip(text_field(&fields, POSITION_FIELD));
            match parsed {
                Some(((chain, topic), position)) => taken.push(NotificationEntry {
                    id,
                    chain,
                    topic,
                    position,
                }),
                None => broken.push(id),
            }
        }
        // An entry that names no stream can never be delivered.
        if !broken.is_empty() {
            self.ack(partition, &broken).await?;
        }
        Ok(taken)
    }

    async fn ack(&self, partition: u32, ids: &[String]) -> StreamResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let key = notifications(&self.prefix, partition as u16);
        let mut pipeline = redis::pipe();
        pipeline
            .cmd("XACK")
            .arg(&key)
            .arg(GROUP)
            .arg(ids)
            .ignore()
            .cmd("XDEL")
            .arg(&key)
            .arg(ids)
            .ignore();
        pipeline
            .query_async::<()>(&mut self.shared.clone())
            .await
            .mapped(Call::Write)
    }

    async fn busy(&self) -> StreamResult<Vec<u32>> {
        let mut busy = vec![];
        for (node, slots) in self.primaries().await? {
            let mut pipeline = redis::pipe();
            for &slot in &slots {
                pipeline.cmd("XLEN").arg(notifications(&self.prefix, slot));
            }
            let lengths: Vec<u64> = self
                .shared
                .query_pipeline_on(node.as_ref(), pipeline, slots.len())
                .await
                .mapped(Call::Read)?;
            busy.extend(
                slots
                    .iter()
                    .zip(lengths)
                    .filter(|(_, length)| *length > 0)
                    .map(|(slot, _)| u32::from(*slot)),
            );
        }
        Ok(busy)
    }

    async fn archive_marks(&self, max: usize) -> StreamResult<Vec<ArchiveMark>> {
        if !self.archive || max == 0 {
            return Ok(vec![]);
        }
        let mut marks = vec![];
        for (node, slots) in self.primaries().await? {
            let mut pipeline = redis::pipe();
            for &slot in &slots {
                pipeline
                    .cmd("ZRANGE")
                    .arg(archive_pending(&self.prefix, slot))
                    .arg(0)
                    .arg(max - 1)
                    .arg("WITHSCORES");
            }
            let sets: Vec<Vec<(String, f64)>> = self
                .shared
                .query_pipeline_on(node.as_ref(), pipeline, slots.len())
                .await
                .mapped(Call::Read)?;
            // The scores come from the scripts' `TIME`, so the age is taken on the same clock.
            let mut time = redis::pipe();
            time.cmd("TIME");
            let [(seconds, micros)]: [(u64, u64); 1] = self
                .shared
                .query_pipeline_on(node.as_ref(), time, 1)
                .await
                .mapped(Call::Read)?;
            let now_ms = seconds * 1000 + micros / 1000;
            for (log, written_ms) in sets.into_iter().flatten() {
                match chain_of_log(&self.prefix, &log) {
                    Some((chain, topic)) => marks.push(ArchiveMark {
                        chain,
                        topic,
                        written_ms: written_ms as u64,
                        idle_ms: now_ms.saturating_sub(written_ms as u64),
                    }),
                    None => tracing::warn!(log, "A slot's set to archive names no log key"),
                }
            }
        }
        Ok(marks)
    }
}

/// The entries of an `XREADGROUP` reply on one stream, as their ids and fields. An entry an
/// `XDEL` dropped while it was pending has no fields.
fn stream_entries(reply: Value) -> Vec<(String, Vec<(String, Value)>)> {
    let streams = match reply {
        Value::Array(streams) => streams
            .into_iter()
            .filter_map(|stream| match stream {
                Value::Array(mut pair) if pair.len() == 2 => pair.pop(),
                _ => None,
            })
            .collect::<Vec<_>>(),
        Value::Map(streams) => streams.into_iter().map(|(_, entries)| entries).collect(),
        _ => vec![],
    };
    streams
        .into_iter()
        .flat_map(|entries| match entries {
            Value::Array(entries) => entries,
            _ => vec![],
        })
        .filter_map(|entry| {
            let Value::Array(mut parts) = entry else {
                return None;
            };
            let fields = parts.pop()?;
            let id = text(parts.pop()?)?;
            let fields = match fields {
                Value::Array(flat) => flat
                    .chunks(2)
                    .filter_map(|pair| Some((text(pair.first()?.clone())?, pair.get(1)?.clone())))
                    .collect(),
                Value::Map(pairs) => pairs
                    .into_iter()
                    .filter_map(|(name, value)| Some((text(name)?, value)))
                    .collect(),
                _ => vec![],
            };
            Some((id, fields))
        })
        .collect()
}

fn text(value: Value) -> Option<String> {
    match value {
        Value::BulkString(bytes) => String::from_utf8(bytes).ok(),
        Value::SimpleString(text) => Some(text),
        _ => None,
    }
}

fn text_field(fields: &[(String, Value)], name: &str) -> Option<String> {
    fields
        .iter()
        .find(|(field, _)| field == name)
        .and_then(|(_, value)| text(value.clone()))
}
