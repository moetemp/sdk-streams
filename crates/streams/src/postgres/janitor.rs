//! The janitor keeps retention, since PostgreSQL has no key expiry. Every store runs one, and a
//! transaction advisory lock around each step lets one janitor work at a time across processes.

use super::{
    DELETE_BATCH, Inner, LOG_CAP,
    errors::{Call, mapped, timed_out, unreachable},
    migrations::LOCK_SPACE,
};
use crate::StreamResult;
use std::{sync::Weak, time::Duration};
use tokio_postgres::{Client, Row, types::ToSql};

/// How many times one pass trims a topic before it moves to the next one.
const TRIMS_PER_TOPIC: usize = 100;
/// How many idle chains one pass deletes at most.
const IDLE_CHAINS: i32 = 500;

/// Runs a pass every `interval` until the store goes.
pub(super) async fn run(inner: Weak<Inner>, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        let Some(inner) = inner.upgrade() else {
            return;
        };
        if let Err(error) = inner.janitor_pass().await {
            tracing::warn!("The PostgreSQL stream store's janitor pass failed: {error}");
        }
    }
}

impl Inner {
    /// One pass: trims the topics that are due, finishes purges, caps the notification log, and
    /// deletes idle chains. A pass stops when another process's janitor holds the lock, since
    /// that one does the work.
    pub(super) async fn janitor_pass(&self) -> StreamResult<u64> {
        let mut client = self.pool.get().await.map_err(unreachable)?;
        let mut deleted = 0;
        for row in self.on(&client, &self.sql.due, &[]).await? {
            let topic: i64 = row.get(0);
            for _ in 0..TRIMS_PER_TOPIC {
                let params: [&(dyn ToSql + Sync); 4] =
                    [&topic, &self.retention_ms, &self.grace_ms, &DELETE_BATCH];
                let Some(trimmed) = self.locked(&mut client, &self.sql.trim, &params).await? else {
                    return Ok(deleted);
                };
                let count: i64 = trimmed[0].get(0);
                deleted += count as u64;
                if trimmed[0].get::<_, bool>(1) || count == 0 {
                    break;
                }
            }
        }
        loop {
            let Some(purged) = self
                .locked(&mut client, &self.sql.purge, &[&DELETE_BATCH])
                .await?
            else {
                return Ok(deleted);
            };
            deleted += purged[0].get::<_, i64>(0) as u64;
            if purged[0].get::<_, bool>(1) {
                break;
            }
        }
        if self.log {
            self.locked(&mut client, &self.sql.cap_log, &[&LOG_CAP])
                .await?;
        }
        self.locked(
            &mut client,
            &self.sql.idle_chains,
            &[&self.retention_ms, &self.grace_ms, &IDLE_CHAINS],
        )
        .await?;
        Ok(deleted)
    }

    /// Runs `sql` in a transaction that holds the janitor's lock, or answers `None` when another
    /// janitor holds it. The lock is a transaction lock, since PgBouncer in transaction mode can
    /// run each statement on another server connection, where a session lock taken by one
    /// statement and released by the next would leak.
    async fn locked(
        &self,
        client: &mut deadpool_postgres::Object,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> StreamResult<Option<Vec<Row>>> {
        let key = format!("{}:janitor", self.schema);
        let work = async {
            let transaction = client.transaction().await?;
            let locked = transaction
                .query_one(
                    "SELECT pg_try_advisory_xact_lock($1, hashtext($2))",
                    &[&LOCK_SPACE, &key],
                )
                .await?;
            if !locked.get::<_, bool>(0) {
                transaction.rollback().await?;
                return Ok(None);
            }
            let rows = transaction.query(sql, params).await?;
            transaction.commit().await?;
            Ok(Some(rows))
        };
        match tokio::time::timeout(self.call_timeout, work).await {
            Ok(answer) => {
                answer.map_err(|error: tokio_postgres::Error| mapped(Call::Write, &error))
            }
            Err(_) => Err(timed_out(Call::Write)),
        }
    }

    async fn on(
        &self,
        client: &Client,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> StreamResult<Vec<Row>> {
        match tokio::time::timeout(self.call_timeout, client.query(sql, params)).await {
            Ok(answer) => answer.map_err(|error| mapped(Call::Write, &error)),
            Err(_) => Err(timed_out(Call::Write)),
        }
    }
}
