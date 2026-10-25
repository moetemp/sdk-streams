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
| `oc` | The encoded Workflow id of the owner's run chain, from the create ref's owner |
| `exp` | When the Stream stops taking writes, in ms since 1970 |
| `closed`, `close_id` | Set by the close, with the entry id of the close Record |
| `newest` | The entry id of the newest entry |
| `trimmed` | The entry id of the newest entry that Retention dropped |
| `e\|<kind>\|<producer id>` | The newest epoch key of the Producer, the fence |
| `d\|<kind>\|<producer id>` | `<epoch key>\|<sequence>\|<first id>\|<last id>\|<digest>`, last batch |
| `h\|<kind>\|<producer id>` | The newest epoch key that still has Records here |

`<kind>` is the `ProducerKind` number. The digest is the
batch's content digest (`testdata/digests.json`). A `sequence` is from 1 to 2^53 - 1, which a script
compares exactly, and the entry stores its digits as sent. A resend with the last `sequence` and the
same digest gets the stored ids. Another digest is refused with `seq_divergent`. An epoch key is
the epoch's fields as fixed-width hex, in the order of comparison, so two keys compare as strings.

## Log entries

Each entry has `r`, the encoded `temporal.api.streamservice.v1.StreamRecord` with no offset, and
`s`, the encoded `Supersession`, on the first entry of a batch that withdraws. An entry with only
`s` withdraws and has no Record. A reader returns a `SUPERSEDED` item for `s` just before the Record
of the same entry. An offset is the entry id `<ms>-<seq>` packed as `ms << 20 | seq`.

A script sets each entry id itself, from its one `TIME` and the newest id, which is the newer of the
log's newest entry and `newest`. The first id is `<now>-0` when `now` is past the newest id's
millisecond, and the newest id's next sequence otherwise. Each later entry of the script takes the
next sequence. So the Records of one batch have consecutive offsets that end at the append's
`last_offset`. `XADD *` doesn't give that on Redis 7.0, where the ids of one script can cross a
millisecond.

The first batch of a Producer's newer epoch withdraws its earlier epochs' Records, for a Workflow, a
Local Activity and an Activity. A client's newer epoch only fences. It writes no `s`, since a
restarted client's earlier Records are real messages, not a failed attempt's.

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

- A read or a describe served by a replica that is behind can report a Stream open just after
  its close.
- Redis can't tie a run to its run chain. A run of another chain that holds the owner's ref, such
  as a child Workflow with its parent's Stream, can still close it. The native server checks the
  run.
