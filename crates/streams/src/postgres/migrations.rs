//! The embedded schema migrations and their runner.
//!
//! Each migration runs once per schema, in its own transaction, under a session advisory lock,
//! so many Workers that start at once apply it once. A function's name carries its version, so a
//! migration that changes a contract adds a new function and keeps the old one for a release.

use super::errors::{Call, mapped, unreachable};
use crate::{StreamError, StreamResult};
use sha2::{Digest, Sha256};
use tokio_postgres::Client;

/// One embedded migration.
#[derive(Debug)]
pub(super) struct Migration {
    pub(super) version: i32,
    pub(super) name: &'static str,
    pub(super) sql: &'static str,
    /// The oldest build version that still works on a database with this migration.
    pub(super) compatible_from: i32,
}

pub(super) const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "init",
    sql: include_str!("migrations/0001_init.sql"),
    compatible_from: 1,
}];

/// The first key of every advisory lock the store takes.
pub(super) const LOCK_SPACE: i32 = 1953653099;

pub(super) fn checksum(sql: &str) -> Vec<u8> {
    Sha256::digest(sql.as_bytes()).to_vec()
}

/// One applied migration, as `schema_migrations` keeps it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Applied {
    pub(super) version: i32,
    pub(super) checksum: Vec<u8>,
    pub(super) compatible_from: i32,
}

/// What to apply, or why this build can't use the database.
pub(super) fn plan<'a>(
    embedded: &'a [Migration],
    applied: &[Applied],
) -> StreamResult<Vec<&'a Migration>> {
    let newest = embedded.iter().map(|m| m.version).max().unwrap_or(0);
    for row in applied {
        match embedded.iter().find(|m| m.version == row.version) {
            Some(migration) if checksum(migration.sql) != row.checksum => {
                return Err(StreamError::refused(format!(
                    "migration {} of the PostgreSQL store was applied with other content than \
                     this build embeds",
                    row.version
                )));
            }
            Some(_) => {}
            None if row.compatible_from > newest => {
                return Err(StreamError::unsupported(format!(
                    "the PostgreSQL store's schema has migration {}, which needs a build that \
                     knows migration {} or later, and this build knows {newest}",
                    row.version, row.compatible_from
                )));
            }
            None => {}
        }
    }
    Ok(embedded
        .iter()
        .filter(|m| applied.iter().all(|row| row.version != m.version))
        .collect())
}

/// Applies the missing migrations to `schema`, or with `apply` off, only checks that none is
/// missing. Runs on a connection of its own, so the session lock ends with it.
pub(super) async fn run(client: &mut Client, schema: &str, apply: bool) -> StreamResult<()> {
    let call = |error: tokio_postgres::Error| mapped(Call::Write, &error);
    if apply {
        client
            .execute(
                "SELECT pg_advisory_lock($1, hashtext($2))",
                &[&LOCK_SPACE, &schema],
            )
            .await
            .map_err(call)?;
        let exists = client
            .query_opt("SELECT 1 FROM pg_namespace WHERE nspname = $1", &[&schema])
            .await
            .map_err(call)?
            .is_some();
        if !exists {
            client
                .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
                .await
                .map_err(call)?;
        }
    }
    let applied = applied(client, schema).await?;
    let missing = plan(MIGRATIONS, &applied)?;
    if missing.is_empty() {
        return Ok(());
    }
    if !apply {
        return Err(StreamError::unsupported(format!(
            "the PostgreSQL store's schema {schema} lacks migration {} ({}), and the store was \
             told not to apply migrations",
            missing[0].version, missing[0].name
        )));
    }
    for migration in missing {
        let transaction = client.transaction().await.map_err(call)?;
        transaction
            .batch_execute(&format!("SET LOCAL search_path = {schema}, pg_temp"))
            .await
            .map_err(call)?;
        transaction
            .batch_execute(migration.sql)
            .await
            .map_err(call)?;
        transaction
            .execute(
                &format!(
                    "INSERT INTO {schema}.schema_migrations (version, name, checksum, \
                     compatible_from) VALUES ($1, $2, $3, $4)"
                ),
                &[
                    &migration.version,
                    &migration.name,
                    &checksum(migration.sql),
                    &migration.compatible_from,
                ],
            )
            .await
            .map_err(call)?;
        transaction.commit().await.map_err(call)?;
        tracing::info!(
            "Applied migration {} ({}) of the PostgreSQL stream store to schema {schema}",
            migration.version,
            migration.name
        );
    }
    Ok(())
}

async fn applied(client: &Client, schema: &str) -> StreamResult<Vec<Applied>> {
    let table = format!("{schema}.schema_migrations");
    let exists = client
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&table])
        .await
        .map_err(unreachable)?
        .get::<_, bool>(0);
    if !exists {
        return Ok(vec![]);
    }
    let rows = client
        .query(
            &format!("SELECT version, checksum, compatible_from FROM {table} ORDER BY version"),
            &[],
        )
        .await
        .map_err(|error| mapped(Call::Read, &error))?;
    Ok(rows
        .iter()
        .map(|row| Applied {
            version: row.get(0),
            checksum: row.get(1),
            compatible_from: row.get(2),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::StreamFailureKind;

    fn applied(migration: &Migration) -> Applied {
        Applied {
            version: migration.version,
            checksum: checksum(migration.sql),
            compatible_from: migration.compatible_from,
        }
    }

    #[test]
    fn a_fresh_schema_gets_every_migration_in_order() {
        let plan = plan(MIGRATIONS, &[]).unwrap();
        let versions: Vec<_> = plan.iter().map(|m| m.version).collect();
        let mut sorted = versions.clone();
        sorted.sort();
        assert_eq!(versions, sorted);
        assert_eq!(versions.len(), MIGRATIONS.len());
        assert_eq!(versions[0], 1);
    }

    #[test]
    fn an_applied_migration_is_not_applied_again() {
        let done: Vec<_> = MIGRATIONS.iter().map(applied).collect();
        assert!(plan(MIGRATIONS, &done).unwrap().is_empty());
    }

    #[test]
    fn a_changed_checksum_is_refused() {
        let mut done = applied(&MIGRATIONS[0]);
        done.checksum[0] ^= 1;
        let error = plan(MIGRATIONS, &[done]).unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Refused);
        assert!(error.message.contains("other content"), "{error}");
    }

    #[test]
    fn a_newer_schema_is_refused_only_past_its_compatible_version() {
        let newest = MIGRATIONS.iter().map(|m| m.version).max().unwrap();
        let mut done: Vec<_> = MIGRATIONS.iter().map(applied).collect();
        done.push(Applied {
            version: newest + 1,
            checksum: vec![],
            compatible_from: newest,
        });
        assert!(plan(MIGRATIONS, &done).unwrap().is_empty());
        done.last_mut().unwrap().compatible_from = newest + 1;
        let error = plan(MIGRATIONS, &done).unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Unsupported);
    }
}
