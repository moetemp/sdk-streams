# The Redis layout

Every SDK that reads or writes Brook Streams on Redis must use this layout. The Lua scripts in
`lua/` are its canonical form, and `testdata/*.json` has the expected ids, keys and fields.

## Keys

```text
<prefix>:{<namespace>:<stream id>}:meta
<prefix>:{<namespace>:<stream id>}:log
```

Each part is percent-encoded. The part in braces is the hash tag, so both keys are in one cluster
slot. `CreateStream` writes `:meta`, and the first append creates `:log`.

## The meta hash

| Field | Value |
|---|---|
| `v` | The format version, 1 |
| `ret` | The Retention, in ms |
| `own` | `wf` for a Workflow-owned Stream, `client` otherwise |
| `oc` | The encoded Workflow id of the owner's run chain, from the create's ref |
| `exp` | When the Stream stops taking writes, in ms since 1970 |
| `closed`, `close_id` | Set by the close, with the entry id of the close Record |
| `newest` | The entry id of the newest entry |
| `trimmed` | The entry id of the newest entry that Retention dropped |
| `e\|<kind>\|<producer id>` | The newest epoch key of the Producer, the fence |
| `d\|<kind>\|<producer id>` | `<epoch key>\|<sequence>\|<first id>\|<last id>\|<digest>`, last batch |
| `h\|<kind>\|<producer id>` | The newest epoch key that still has Records here |
| `oa\|<run>` | The run's Local Activity task keys here that may not have completed |
| `ow\|<run>` | The task keys of the run whose Local Activity Records an orphan item withdrew |
| `run\|<run>` | Set when a Producer of the run wrote a Record here |
| `wr\|<run>` | Set when a Workflow retry withdrew the run |

`<kind>` is the `ProducerKind` number, and `<run>` is the percent-encoded run id. The digest is the
batch's content digest (`testdata/digests.json`). A `sequence` is from 1 to 2^53 - 1, which a script
compares exactly, and the entry stores its digits as sent. A resend with the last `sequence` and the
same digest gets the stored ids. Another digest is refused with `seq_divergent`. An entry with no
digest, written by an older crate, accepts any resend. An epoch key is the epoch's fields as
fixed-width hex, in the order of comparison, so two keys compare as strings.

## Log entries

Each entry has `r`, the encoded `temporal.api.streamservice.v1.StreamRecord` with no offset, and
`s`, the encoded `Supersession`, on the first entry of a batch that withdraws. An entry with only
`s` withdraws and has no Record. A reader returns a `SUPERSEDED` item for `s` just before the Record
of the same entry. An offset is the entry id `<ms>-<seq>` packed as `ms << 20 | seq`.

The first batch of a Producer's newer epoch withdraws its earlier epochs' Records, for a Workflow, a
Local Activity and an Activity. A client's newer epoch only fences. It writes no `s`, since a
restarted client's earlier Records are real messages, not a failed attempt's.

## Orphaned Local Activity Records

A Local Activity batch can send its run's last completed Workflow Task (`previous_completed_task`, 0
before the first) and failed tasks. Its epoch names its dispatching task. Before it writes, the
append script withdraws the Records of the run's Local Activity tasks in `oa` that never completed.
These are the tasks after the completed task and before the batch's own task, the failed tasks and
the lower attempts. The script writes one entry with only `s`, a Workflow-kind `Supersession` with
the run's Workflow Producer, the batch's task, and the completed and failed tasks. So a Reader
withdraws them with the Workflow rule. Then the script drops the tasks at or before the completed
one from `oa` and adds the batch's own task. It stores the withdrawn tasks in `ow`, which fences a
late write of such a task. A batch with no completed task skips the rule. A Stream that the run
never writes again keeps its orphans.

## Close

Only the owner closes a Stream. A Workflow-owned Stream closes only from the close Activity of
its run chain, which closes as the run's Workflow Producer (kind 1). A Local Activity can't close
it, since it writes before its Workflow Task commits. When `oc` is set, a close whose ref names
another chain, or no chain, is refused. Any other Stream closes only from a client. The script
refuses others with `stream_not_owner` before it writes.

## TTLs

The log key expires at `exp`, and the meta key 30 days later, as the Tombstone. Each write sets
`exp` to now plus the safety TTL for an open Workflow-owned Stream, or plus the Retention
otherwise. Each append drops the Records older than the Retention, and a read hides them too.

## Access

Redis checks the ACL of the script call and of each command the script runs. The crate sends
`EVALSHA`, and `SCRIPT LOAD` when Redis doesn't have the script yet. `EVALSHA` counts as a write to
its keys, so each role, a Reader too, needs read and write access to `~<prefix>:{<namespace>:*`.
These are the commands of each role.

- A Producer, which creates, appends and closes, needs `+evalsha`, `+script|load`, `+time`,
  `+hget`, `+hset`, `+hdel`, `+exists`, `+pexpireat`, `+xadd`, `+xtrim` and `+xrevrange`.
- A Reader, which reads and describes, needs `+evalsha`, `+script|load`, `+time`, `+hmget` and
  `+xrange`, with `+xread` for a read that waits and `+xrevrange` for a read of the last Records.

A process with both roles needs both sets, and a cluster also needs `+cluster|slots`. On connect,
the Store runs `INFO server` to check the Redis version and `CONFIG GET maxmemory-policy` to warn
about eviction. Neither is required. Without `+info` the Store skips the version check, and it
ignores a refused `CONFIG`. `tests/acl.rs` runs each role with exactly these commands.

## Known limits

- A Local Activity call of a failed Workflow Task is withdrawn only when the retry task runs that
  call again, or when a later Local Activity write names the failed task. A call that no later
  write covers, for example because a Signal changed the path and no Local Activity writes
  again, keeps its Records. The memory Store has the same limit.
- Core sends only the last 100 failed Workflow Tasks of a run. An older failed task before the last
  completed one keeps its Local Activity Records.
- A read or a describe served by a replica that is behind can report a Stream open just after
  its close.
- Redis can't tie a run to its run chain. A run of another chain that holds the owner's ref, such
  as a child Workflow with its parent's Stream, can still close it. The native server checks the
  run.
