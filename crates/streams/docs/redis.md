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
| `ac` | The auto close period in ms, absent when the Stream has none |
| `la` | When the create or the newest append with Records ran, in ms since 1970, with `ac` only |
| `e\|<kind>\|<producer id>` | The newest epoch key of the Producer, the fence |
| `d\|<kind>\|<producer id>` | `<epoch key>\|<sequence>\|<first id>\|<last id>` of the last batch |
| `h\|<kind>\|<producer id>` | The newest epoch key that still has Records here |
| `ti\|<run>` | The task keys of the run and its Local Activities that have Records here |
| `wt\|<run>` | The task keys of the run that a `Superseded` item withdrew |
| `lf\|<run>` | `<task key>\|<completed task key>\|<failed ids>` of the run's newest Workflow write with facts |
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

## Workflow batches before a Local Activity

A Workflow batch that a Local Activity flush writes has no withdrawal facts yet, since Core
gives them only with the task's completion. The append script gets `*` as its completed task
key. It then withdraws no task, flags nothing and keeps no `lf`, but it still fences, dedupes,
adds the task to `ti` and applies a retry run's withdrawal. The task's completion sends the next
batch of the same epoch with the facts, an empty one if it has no Records, so the withdrawal
reaches the Stream then.

## Auto close

A Stream with `ac` closes with no result when `la + ac` passes. Nothing runs at that time. The
next create, append, close or auto close step finds it due and first writes the close Record
`08 03` (a `StreamRecord` with only `kind = CLOSE`), then acts on the closed Stream. So an append
is refused with `stream_closed`, and a close answers that the Stream was already closed. The
read script writes nothing, so a read or a describe that finds the close due runs the auto close
step, then reads again. A long-polling read wakes when the period passes.

## TTLs

The log key expires at `exp`, and the meta key 30 days later, as the Tombstone. Each write sets
`exp` to now plus the safety TTL for an open Workflow-owned Stream, or plus the Retention
otherwise. Each append drops the Records older than the Retention, and a read hides them too.

## Access

A Producer needs `+evalsha` and `+script|load` on `~<prefix>:{<namespace>:*`. A Reader needs
`+evalsha_ro` or `+evalsha`, `+xread` and the read keys. A Reader of a Stream with an auto close
also needs `+evalsha` with write access, since its read may apply the close. A cluster also needs `+cluster|slots`.
