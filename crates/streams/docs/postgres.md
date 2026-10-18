# The PostgreSQL stream store

The store on the application's PostgreSQL database, 14 or later.

Records live in the database and never pass through Temporal. Every stream shares one set of
tables in one schema, `temporal_streams` by default. The schema plays the role of the Redis key
prefix, so one database can hold several stores, and a `GRANT` scopes a role to one.

**Tables.** `chains` keeps one row per run chain, keyed by the namespace, the owner kind, the
owner id, the first run id and, for a Workflow Activity, the Activity id. A Workflow Activity keeps
its Workflow's id as the owner id, so a Workflow's delete finds its Activities by an exact match.
`topics` keeps one row per stream with the newest position ever, the newest dropped one, the
close flag, the first close and the topic's base. `records` keeps one row per record, keyed by the
topic and the position. `producers` keeps the newest batch of each producer attempt. `floors`
keeps the holds of positions-only readers. `purge` keeps the topics an owner delete removed whose
records are still to delete. `schema_migrations` keeps the applied migrations.

**Positions.** A position is a decimal number, the topic's base plus the record's place in the
topic. The base is the clock in milliseconds shifted left by 20 bits when the topic row is made.
So a topic made again after its tombstone went starts above every position it had before, and a
position never names two records. Positions of one topic have no gaps. A cursor reads
`postgres:<stream hash>:<position>`.

**Appends.** Each write is one PL/pgSQL function call in autocommit, as each Redis write is one
script. The append takes the topic row lock, checks the producer attempt's newest batch, checks
the close flags, writes the batch at the next positions and moves the topic's newest position,
all in one transaction. So appends to one topic commit in position order, and a reader never sees
a new position below one it read. The function answers a refusal as a status, not an error. Every
append also takes the chain's advisory lock shared, and a chain close takes it exclusive, so no
batch lands after a chain close returned.

**Isolation.** Each pooled connection runs `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION
LEVEL READ COMMITTED` and sets `statement_timeout` to `response_timeout` when the pool makes it.
The functions need `READ COMMITTED`, since a statement that waited for a row lock then reads the
newest version of the row.

**Reads.** One statement reads the topic's state and the page in one snapshot. A read that finds
nothing and may wait holds no connection. It waits for an append or a close through the same
store, or for the store's poll. While any read waits, the store runs one statement every
`poll_interval` (100 ms) for every stream with a waiter and wakes those that moved. So a reader in
another process wakes within about `poll_interval`. The server's stream notifier is the main wake
path, and this wait is the path for a reader without it.

**`notify_reads`.** With it, each append also sends `NOTIFY` on `<schema>_read_<topic id % 64>`
with the topic id, and the store's listener wakes the waiters of that topic. A reader in another
process then wakes in a few milliseconds. A reader that waits on a topic with no row yet wakes
from the poll, since a `NOTIFY` can't name it. It is off by default, since a transaction that sent
`NOTIFY` takes a lock on the server's notification queue as it commits, so such commits run one
at a time across the database. With many producers at once, it costs append throughput.

**Notification log.** With `notification_log` on, the append also writes a row to
`notifications` in its transaction: the topic and the append's last position, in one of 64
partitions fixed in the schema. Every topic of a chain lands in the partition that the first four
bytes of SHA-256 over the chain's fields give. A repeat of the newest batch writes none. When the
partition held no entry that a driver hasn't taken, the append also sends `NOTIFY` on
`<schema>_bell_<partition % doorbells>` with the partition, so the doorbell costs nothing while a
partition stays busy, and a driver that drains taken entries still hears of the next one. Drivers
take entries with `FOR UPDATE SKIP LOCKED`: a consumer's own unacked entries, those another
consumer left for `claim_idle`, and new ones. An ack deletes them. The janitor keeps the newest
10,000 entries per partition, so a log with no driver stays bounded. It keeps older entries that a
driver took within the last minute too, so a quiet stream that shares a partition with a busy one
keeps its only entry while a slow driver works. An entry whose topic an owner
delete removed is acked as broken when a driver takes it.

**Listener.** One connection per store listens for the doorbells and, with `notify_reads`, the read
channels. It connects with the store, so a store whose listener can't connect fails at once. When
the connection ends, it connects again with backoff, rings every doorbell for every partition and
wakes every waiting read, since what was sent while it was away is gone. The driver's sweep finds a
lost ring too. The listener sets TCP keepalive to notice a dead connection within a minute.
`LISTEN` keeps state on the server session, so the listener uses `listen_url` when it is set, and
`url` otherwise. Both must name the primary, since a standby refuses `LISTEN`.

**Retention.** PostgreSQL has no key expiry, so each store runs a janitor every `janitor_interval`
(one minute). A transaction advisory lock around each step lets one janitor work at a time across
processes, also through PgBouncer in transaction mode. A pass trims the topics that are due, at most
10,000 records per topic per transaction, and keeps what a hold holds. It moves the topic's newest
dropped position in the same transaction. A topic whose records are all gone keeps its row as the
tombstone for the retention plus 30 days after its last write, as Redis keeps a topic's meta. Then
the janitor deletes it, and a chain row with no topics that nothing touched for as long.
`PostgresStore::run_janitor` runs a pass at once.

**Floors.** `floors` keeps one row per holder of a topic, with the lowest position it held and the
end of its lease. Position `0` holds the whole topic. A release moves the end to now plus the
release grace. The janitor deletes ended holds before each trim. A topic keeps at most 1,000 live
holders, and a new holder past them is refused. A topic whose oldest record a hold keeps is due
again when the first hold ends, so held topics don't take every janitor pass.

**Owner deletes.** One function deletes an owner's chain rows, their topics and their holds, and
puts the topics in `purge`. Then the store deletes their records and producer rows in batches of
10,000. The janitor finishes a purge that a crash cut short. No reader sees those rows, since no
topic row names them.

**Errors.** An append that fails with a doubtful outcome reports `OUTCOME_UNKNOWN`: a lost
connection, a timeout, a serialization failure or deadlock after three tries, a server shutdown,
a read-only server after a failover, or too many connections. Lang then retries with the same
sequence, and the dedupe answers it. Another error reply means the server rolled the call back,
so it reports `REFUSED`. A read reports `STORAGE`. A server below PostgreSQL 14 is refused as
`UNSUPPORTED` at connect.

**Archive hold.** With `archive_lease` set, a driver copies each topic into an archive, and the
store holds what the archive doesn't hold yet. A topic's first record makes the `floors` row
`archive` at position `0`, in the append's transaction, so it holds the whole topic until the
archive's first pass. `advance_archive` moves it to the archive's newest position with no minimum,
renews its lease and sets the topic's `archived`. The janitor reads it as any other hold. When the
driver stops for longer than the lease, the hold ends and retention goes on. `archive_pending`
marks the topics with records to archive, set by appends and closes, and cleared by an advance to
the topic's newest position and by the archive's release. `archive_marks` reads it, closed topics
first. A topic made again after its tombstone starts above every archived position, so an archive
segment name never stands for two records. The archive is optional on PostgreSQL, since records
already live on disk. It helps above about 1 TB of records, for retention in years, and to keep
backups small. An archive needs `notification_log`.

## Migrations

The store embeds its migrations and applies the missing ones at connect, each in its own
transaction, under a session advisory lock on the schema's name. They run on a connection of their
own to `listen_url` when it is set, since a pooler in transaction mode can't keep a session lock.
The store refuses to start when an applied migration differs from the embedded one, or when the
database has a migration that needs a newer build. A function's name carries its version, such
as `append_v1`, so a migration that changes a contract adds a new function and keeps the old one
for a release.

With `skip_migrations`, the store only checks that every migration it knows is applied. A DBA then
applies the files in `src/postgres/migrations/` in order, after `SET search_path = <schema>,
pg_temp`, and adds a row to `schema_migrations` for each. Running the store once with a role that
may create the schema does the same.

## Roles

With `skip_migrations`, the application role needs no `CREATE`. These lines, with your schema and
role, give it what the store uses. The conformance suite runs once as such a role.

```sql
CREATE ROLE streams_app LOGIN PASSWORD 'secret';
GRANT USAGE ON SCHEMA temporal_streams TO streams_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA temporal_streams TO streams_app;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA temporal_streams TO streams_app;
```

Grant again after a migration adds a table or a function.

## TLS

The store reads `sslmode` and `sslrootcert` from the URL, as libpq does. The default is `prefer`.

| `sslmode` | Encrypts | Checks the chain | Checks the name |
|---|---|---|---|
| `disable` | no | no | no |
| `prefer` | when the server offers it | no | no |
| `require` | yes | only with `sslrootcert` | no |
| `verify-ca` | yes | yes | no |
| `verify-full` | yes | yes | yes |

`sslrootcert` names a PEM file. Without it, or with `sslrootcert=system`, the chain checks use the
platform's roots. TLS uses the rustls crypto provider that the process installs, as Core does.
With no provider, `prefer` connects without TLS and the other modes are refused. The store warns
when it connects to a host that is not local with a mode other than `verify-full`.

## Managed PostgreSQL

**RDS and Aurora.** RDS for PostgreSQL and Aurora PostgreSQL 14 and later work. Their
certificates chain to the Amazon RDS roots, which platform trust stores don't hold, so
`verify-full` needs the bundle.

```text
postgres://streams_app:<password>@<writer>:5432/<db>?sslmode=verify-full&sslrootcert=/etc/rds.pem
```

The URL must name the writer. Aurora reader endpoints and RDS read replicas refuse writes. After a
failover, calls fail as connection errors or with `25006` until the pool reaches the new writer.
IAM database authentication is not in this release, so the password comes from the URL.

**PgBouncer.** Session mode works with no setting. Transaction mode works from PgBouncer 1.21 with
`max_prepared_statements` set, since the store prepares its statements. The session settings
apply to the server connection that made each pooled connection, so all clients of that
PgBouncer database should use the same `READ COMMITTED` default. Set `listen_url` to a direct
connection, which the listener and the migrations use. The conformance suite runs through
PgBouncer 1.26 in transaction mode with `max_prepared_statements = 200` and a direct
`listen_url`.

**Servers that speak the protocol.** CockroachDB, YugabyteDB and other servers that speak the
PostgreSQL protocol are not supported. The store needs advisory locks, PL/pgSQL and PostgreSQL's
row lock modes.

## Sizing

A 256-byte record takes about 360 bytes with its row and its primary key entry. PostgreSQL keeps
records on disk, so a long retention costs disk, not memory. Seven days is the default, as for
Redis.

## Tests

`scripts/postgres-test-server.sh` starts PostgreSQL 14 and 17 in Docker on free ports, and
PgBouncer in transaction mode in front of 17, and prints the URLs to export. The suite reads
`STREAMS_POSTGRES_URL`, a role that may create schemas, and `STREAMS_POSTGRES_PGBOUNCER_URL`. Each
case runs in a schema of its own. A schema name takes at most 55 bytes, so that the channel names
stay within 63.

```sh
eval "$(crates/streams/scripts/postgres-test-server.sh)"
cargo test -p temporalio-streams --features postgres
crates/streams/scripts/postgres-test-server.sh stop
```
