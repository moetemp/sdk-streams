//! The notification log on the `notifications` table, for the drivers that read it.

use super::{Inner, PARTITIONS, errors::Call, listen::Rings};
use crate::{
    ArchiveMark, NotificationEntry, NotificationLog, StreamResult,
    proto::{ChainId, StreamOwnerKind},
};
use std::{sync::Arc, time::Duration};

pub(super) struct PostgresLog {
    pub(super) inner: Arc<Inner>,
    pub(super) rings: Arc<Rings>,
    pub(super) doorbells: u32,
    /// Whether the store keeps an archive hold, so it has topics to archive.
    pub(super) archive: bool,
}

/// The chain of a `chains` row. A Workflow chain keeps no owner kind, as the Redis store's keys
/// give it back.
fn chain(namespace: String, kind: i16, owner: String, run: String, activity: String) -> ChainId {
    match kind {
        2 => ChainId {
            namespace,
            first_run_id: run,
            owner_kind: StreamOwnerKind::Activity as i32,
            activity_id: owner,
            ..Default::default()
        },
        3 => ChainId {
            namespace,
            workflow_id: owner,
            first_run_id: run,
            owner_kind: StreamOwnerKind::WorkflowActivity as i32,
            activity_id: activity,
        },
        _ => ChainId {
            namespace,
            workflow_id: owner,
            first_run_id: run,
            ..Default::default()
        },
    }
}

#[async_trait::async_trait]
impl NotificationLog for PostgresLog {
    fn partitions(&self) -> u32 {
        PARTITIONS
    }

    fn doorbells(&self) -> u32 {
        self.doorbells
    }

    async fn wait_doorbell(
        &self,
        doorbell: u32,
        _consumer: &str,
        block: Duration,
    ) -> StreamResult<Vec<u32>> {
        Ok(self.rings.wait(doorbell, block).await)
    }

    async fn take(
        &self,
        partition: u32,
        consumer: &str,
        claim_idle: Duration,
        max: usize,
    ) -> StreamResult<Vec<NotificationEntry>> {
        let inner = &self.inner;
        let partition = i16::try_from(partition).unwrap_or(i16::MAX);
        let claim_idle_ms = super::millis(claim_idle);
        let max = i64::try_from(max.max(1)).unwrap_or(i64::MAX);
        let rows = inner
            .query(
                Call::Write,
                &inner.sql.take,
                &[&partition, &consumer, &claim_idle_ms, &max],
            )
            .await?;
        let mut taken = vec![];
        let mut broken = vec![];
        for row in rows {
            let id: i64 = row.get(0);
            let namespace: Option<String> = row.get(1);
            let topic: Option<String> = row.get(6);
            match (namespace, topic) {
                (Some(namespace), Some(topic)) => taken.push(NotificationEntry {
                    id: id.to_string(),
                    chain: chain(namespace, row.get(2), row.get(3), row.get(4), row.get(5)),
                    topic,
                    position: row.get::<_, i64>(7).to_string(),
                }),
                _ => broken.push(id.to_string()),
            }
        }
        // An entry whose topic a delete took can never be delivered.
        if !broken.is_empty() {
            self.ack(partition as u32, &broken).await?;
        }
        Ok(taken)
    }

    async fn ack(&self, partition: u32, ids: &[String]) -> StreamResult<()> {
        let ids: Vec<i64> = ids.iter().filter_map(|id| id.parse().ok()).collect();
        if ids.is_empty() {
            return Ok(());
        }
        let inner = &self.inner;
        let partition = i16::try_from(partition).unwrap_or(i16::MAX);
        inner
            .query(Call::Write, &inner.sql.ack, &[&partition, &ids])
            .await
            .map(drop)
    }

    async fn archive_marks(&self, max: usize) -> StreamResult<Vec<ArchiveMark>> {
        if !self.archive || max == 0 {
            return Ok(vec![]);
        }
        let inner = &self.inner;
        let max = i64::try_from(max).unwrap_or(i64::MAX);
        let rows = inner
            .query(Call::Read, &inner.sql.archive_marks, &[&max])
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| ArchiveMark {
                chain: chain(row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)),
                topic: row.get(5),
                written_ms: row.get::<_, i64>(6).max(0) as u64,
                idle_ms: row.get::<_, i64>(7).max(0) as u64,
            })
            .collect())
    }

    async fn busy(&self) -> StreamResult<Vec<u32>> {
        let inner = &self.inner;
        let rows = inner.query(Call::Read, &inner.sql.busy, &[]).await?;
        Ok(rows.iter().map(|row| row.get::<_, i32>(0) as u32).collect())
    }
}
