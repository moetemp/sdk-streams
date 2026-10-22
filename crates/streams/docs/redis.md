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
| `oa\|<run>` | The run's Local Activity task keys here that may not have completed |
| `ow\|<run>` | The task keys of the run whose Local Activity Records an orphan item withdrew |
| `run\|<run>` | Set when a Producer of the run wrote a Record here |
| `wr\|<run>` | Set when a Workflow retry withdrew the run |

`<kind>` is the `ProducerKind` number, and `<run>` is the percent-encoded run id. An epoch key is
the epoch's fields as fixed-width hex, in the order of comparison, so two keys compare as
strings.

## Log entries

Each entry has `r`, the encoded `temporal.api.streamservice.v1.StreamRecord` with no offset, and
`s`, the encoded `Supersession`, on the first entry of a batch that withdraws. An entry with
only `s` withdraws and has no Record. A reader gives a `SUPERSEDED` item for `s` just before the
Record of the same entry. An offset is the entry id `<ms>-<seq>` packed as `ms << 20 | seq`.

## Orphaned Local Activity Records

A Local Activity batch that gives its run's last completed Workflow Task (`previous_completed_task`,
0 before the first) and failed tasks names its dispatching task in its epoch. Before it writes,
the append script withdraws the Records of the run's Local Activity tasks in `oa` that never
completed: those after the completed task and before the batch's own task, the failed ones and the
lower attempts. It writes one entry with only `s`, a Workflow-kind `Supersession` (the run's
Workflow Producer, the batch's task, the completed and failed tasks), so a Reader's Workflow rule
withdraws them. It drops the tasks at or before the completed one, adds its own task, and keeps the
withdrawn ones in `ow`, which fences a late write of such a task. A batch with no completed task
skips the rule. A Stream that the run never writes again keeps its orphans.

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
`+evalsha_ro` or `+evalsha`, `+xread` and the read keys. A cluster also needs `+cluster|slots`.
See the known limits for a Reader of a Stream with an auto close.

## Known limits

- A Local Activity call of a failed Workflow Task is withdrawn only when the retry task runs that
  call again, or when a later Local Activity write names the failed task. A call that no later
  write covers, for example because a Signal changed the path and no Local Activity writes
  again, keeps its Records. The memory Store has the same limit.
- Core names only the last 100 failed Workflow Tasks of a run. An older failed task before the
  last completed one keeps its Local Activity Records.
- A Reader of a Stream with an auto close needs `+evalsha` with write access to the Stream's
  keys, since its read or describe may apply a due close. A Reader with read-only access gets
  an error from that step.
- A read or a describe served by a replica that is behind can report a Stream open just after
  its close.
