# The Redis layout

Every SDK that reads or writes Brook Streams on Redis must use this layout. The Lua scripts in
`lua/` are its canonical form, and `testdata/*.json` pins the ids, keys and fields.

## Keys

```text
<prefix>:{<namespace>:<stream id>}:meta
<prefix>:{<namespace>:<stream id>}:log
```

Each part is percent-encoded. The part in braces is the hash tag, so both keys are in one cluster
slot. `CreateStream` writes `:meta`, and the first entry makes `:log`.

## The meta hash

| Field | Value |
|---|---|
| `v` | The format version, 1 |
| `ret` | The Retention, in ms |
| `own` | `wf` for a Workflow-owned Stream, `client` otherwise |
| `exp` | When the Stream stops taking writes, in ms since 1970 |
| `closed`, `close_id` | Set by the close, with the entry id of the close Record |
| `newest` | The entry id of the newest entry |
| `trimmed` | The entry id of the newest entry that Retention dropped |
| `e\|<kind>\|<producer id>` | The newest epoch key of the Producer, the fence |
| `d\|<kind>\|<producer id>` | `<epoch key>\|<sequence>\|<first id>\|<last id>` of the last batch |
| `h\|<kind>\|<producer id>` | The newest epoch key that still has Records here |
| `ti\|<run>` | The task keys of the run and its Local Activities that have Records here |
| `wt\|<run>` | The task keys of the run that a `Superseded` item withdrew |
| `run\|<run>` | Set when a Producer of the run wrote a Record here |
| `wr\|<run>` | Set when a Workflow retry withdrew the run |

`<kind>` is the `ProducerKind` number, and `<run>` is the percent-encoded run id. An epoch key is
the epoch's fields as fixed-width hex, in the order of comparison, so two keys compare as
strings. A task key is the first four fields of an epoch key.

## Log entries

Each entry has `r`, the encoded `temporal.api.streamservice.v1.StreamRecord` with no offset, and
`s`, the encoded `Supersession`, on the first entry of a batch that withdraws. An entry with
only `s` withdraws and has no Record. A reader gives a `SUPERSEDED` item for `s` just before the
Record of the same entry. An offset is the entry id `<ms>-<seq>` packed as `ms << 20 | seq`.

## TTLs

The log key expires at `exp`, and the meta key 30 days later, as the Tombstone. Each write sets
`exp` to now plus the safety TTL for an open Workflow-owned Stream, or plus the Retention
otherwise. Each append drops the Records older than the Retention, and a read hides them too.

## Access

A Producer needs `+evalsha` and `+script|load` on `~<prefix>:{<namespace>:*`. A Reader needs
`+evalsha_ro` or `+evalsha`, `+xread` and the read keys. A cluster also needs `+cluster|slots`.
