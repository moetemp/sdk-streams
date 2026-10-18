# The stream archive

The archive keeps streams in object storage after the hot store, such as Redis, drops them. A
`TieredStore` wraps the hot store and a `StreamArchive`, and implements `StreamStore`, so readers,
cursors and the user API stay as they are.

## Positions

The archive keeps the positions that the hot store minted. A cursor stays
`<store>:<stream hash>:<position>`, and a read moves from the archive to the hot store with no gap.
A replay read of a range below the hot store's floor reads the same records from the archive, with
the same digest.

## Writes

`archive_topic` copies the records that the archive does not hold yet into segments. After each
segment, it moves the hot store's archive hold to the segment's last position with
`StreamStore::advance_archive`. The hold is the floor holder `archive`, so retention drops only
records that the archive holds. Readers can't name themselves `archive`.

A segment is named by its last position and written only when no object has that name. Records
never change in the hot store, so two writers on one topic write the same objects, and a repeat
is safe. When the topic is closed and the archive holds it to its end, `archive_topic` writes
`close.pb` and releases the hold.

When retention dropped records that the archive never took, because the hold's lease ran out, the
next segment starts at what is left. Its trailer records that start, so a read from before it
fails as expired.

## Reads

- A read from the beginning starts at the archive's first segment, when there is one.
- A read after a position asks the hot store first. When the hot store fails as expired or as not
  found, the read takes the records after the position from the first segment that holds them.
  It answers at once, with at most `max_records` records. The next read goes back to the hot
  store.
- A read at the end of a closed topic that only the archive holds answers `closed` and the close
  from `close.pb`.
- `record_at`, `latest` and `newest_ever` ask the hot store first, then the archive.
- `dropped_after` answers `None` when the archive holds the dropped record.
- An owner delete also deletes the owner's objects.

Records that neither tier holds fail as expired, as on the hot store alone. A bucket lifecycle
rule that deletes old segments causes that.

## Names

```text
v1/<namespace>/w/<workflow id>/<first run id>/<topic>/seg-<last position>.tsr
v1/<namespace>/a/<activity id>/<run id>/<topic>/seg-<last position>.tsr
v1/<namespace>/wa/<workflow id>/<run id>/<activity id>/<topic>/seg-<last position>.tsr
.../<topic>/close.pb
```

Each part is percent-encoded as the Redis keys are, and `.` is encoded too, so no part is a path
step. A position in a name is its two numbers, each zero-padded to 20 digits, such as
`00000001700000000000-00000000000000000003` for the Redis entry id `1700000000000-3`. So names sort
in log order, and the segment after a position is the first name after
`seg-<that position>.tsr`. A memory store offset has `0` as its second number.

## Objects

A segment (`.tsr`) is, in order:

1. The magic `TSR1`.
2. A flags byte. `0` is the only value. A reader refuses other values.
3. The records, each a length-delimited `coresdk.streams.StoredRecord`, in log order. Bodies are
   as lang's codec encoded them.
4. The trailer, a `SegmentTrailer`: `count` (1, `uint64`), `after_position` (2), `first_position`
   (3), `last_position` (4), and `sha256` (5, `bytes`) over the records of item 3.
5. The trailer's length, as 4 big-endian bytes.

A segment holds every record after `after_position` up to `last_position`. `close.pb` is an
`ArchivedClose`: `close` (1, `coresdk.streams.StoredClose`, unset for a chain close),
`last_position` (2), and `newest_ever` (3).

`testdata/archive/names.json` and `testdata/archive/objects.json` pin the names and the bytes.

## The sink

The `StreamDriver` writes the archive when the service has an `ArchiveConfig`. A store with an
archive marks each topic with records to archive, with the time of its newest write, and marks a
closed topic first. At each sweep, the driver reads the marks and runs `archive_topic` on each
topic that is closed, idle for `segment_idle`, or not taken for `segment_idle`. So a busy topic
gets a segment at least that often. The hot store unmarks a topic once the archive holds its last
record, and once the archive released a closed topic.

Every driver archives every mark. The writes are create-once, so two drivers on one topic are safe
and only cost reads. Without a running driver, nothing is archived, and the hold's lease,
`stall_limit`, ends after its time. Then retention goes on as without an archive.

## Config

`StreamStoreConfig.archive` takes `url` (`s3://<bucket>/<prefix>` or `file:///<directory>`),
`segment_bytes` (4 MiB), `segment_idle` (five minutes) and `stall_limit` (seven days). It needs
`notification_log`. S3 settings come from the environment, as `object_store` reads them:
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ENDPOINT_URL` for an
S3-compatible store, and `AWS_ALLOW_HTTP` for one without TLS. The archive deletes nothing but an
owner's delete, so a bucket lifecycle rule sets how long it keeps streams. Bodies are as lang's
codec encoded them, and bucket encryption, such as SSE-KMS, is the bucket's setting.

## Archives

`LocalFileArchive` keeps objects in a directory. It writes each object to a hidden file and then
links it to its name, which fails when the name exists. So a name is written once and no reader
sees part of an object.

`ObjectStoreArchive` keeps objects in S3 or an S3-compatible store through the `object_store`
crate, behind the `archive` feature. A write sends `If-None-Match: *`, and a `412` answer means the
name exists. Its listings take the whole listing after their start, since `object_store` makes no
promise about the order.

## Tests

`tests/archive_sink.rs` runs the driver's sink on memory, on Redis with `STREAMS_REDIS_URL`, on
PostgreSQL with `STREAMS_POSTGRES_URL`, and on S3 with `STREAMS_S3_URL`. The conformance suite runs
the archive cases on a local directory, and on S3 with `STREAMS_S3_URL`, such as
`s3://archive/tests` on a local S3-compatible server with `AWS_ENDPOINT_URL` and
`AWS_ALLOW_HTTP=true`.
