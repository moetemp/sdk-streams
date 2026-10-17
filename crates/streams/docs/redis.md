# The Redis stream store

The store on the application's Redis.

Records live in Redis and never pass through Temporal. The key layout, the stored values and
the Lua scripts are the ones the SDKs used before the store moved into Core, apart from the
append's topic check, so a stream one wrote stays readable and writable by the other.

**Appends.** One script writes a whole batch, so a reader never sees part of one. The topic's
meta hash keeps one field per producer attempt: the newest batch's first sequence, its record
count, its first and last entry ids, and lang's digest of it in hex. The script refuses a
new batch once the chain or the topic is closed, in the same step as the write, so no batch
lands after a close returned. Only that topic check differs from the SDKs' script.

**Closes.** A topic close sets `closed` in the topic's meta. In the same script it sets `close`,
a serialized `StoredClose` with the close's result or failure and its `closed_by`, but only when
the meta has no `close` yet. So the first close wins, as on the server's notifier, and a close
with no outcome is a first close too. A chain close, when the owner ended, sets the chain's flag
and keeps no outcome. A read answers the first close with the closed topic, so a reader that
reaches the end sees the outcome without a wake. An append that the close refuses names its
`closed_by`. The field takes at most 1.5 MiB, lives as long as the meta, and needs no ACL rule
beyond the documented ones, since the script uses `HGET`, `HSET` and `PEXPIRE` only. An SDK's
provider ignores it.

**Floors.** A positions-only Workflow reader records which range it read, not the records, and
reads the range again on replay. So the store holds those records past retention while the
reader's run can replay. A hash `<base>:t:<topic>:floors` keeps one field per reader, its run id
and subscription id, with `<entry id>|<until ms>`: the lowest position the reader read after, and
the end of its lease. A read or a replay read renews the lease, which lasts the retention by
default, and extends the log's and the meta's expiry to it. `ReleaseFloor` moves the end to now
plus a release grace, one day by default. The append script's trim reads the hash first. It drops
the ended holds, trims to the lower of the retention floor and the lowest held position, and keeps
the log until the latest lease ends. A hold from the beginning holds `0-1`, so it keeps the whole
topic. Any position below `0-1` holds `0-1` too. A topic keeps at most 1,000 live holders, and a
new holder past them is refused, since every append reads them all. The scripts use `HGET`,
`HSET`, `HGETALL`, `HLEN`, `HDEL`, `PTTL` and `PEXPIRE`, which the documented ACL allows. Without
holds the hash doesn't exist and the trim is as before. The append script's hash changes again.
A stream close releases no hold, since the readers of a closed stream still replay. Only the
reader's `ReleaseFloor` and its grace, or a lapsed lease, end one.

**Mixed providers and floors.** An SDK's own Redis provider trims with its own append script and
doesn't read the floors hash, so its appends can trim records that a positions-only reader holds.
A topic that a positions-only reader reads must take its appends only through Core's store. Other
providers on such a topic are unsupported.

**Notification log.** With `notification_log` on, the append script also adds an entry to the
notification stream of the chain's Cluster slot, `<prefix>:{<tag>}:n`, where the tag is the one
`testdata/redis/notify_slots.json` pins for that slot. So the log, the meta and the entry share a
slot, one script writes them, and a reshard moves them together. A standalone server uses the same
keys. An entry holds `k`, the log key, and `p`, the append's last entry id. The stream keeps about
the newest 10,000 entries, also ones a driver took and has not acked. So a driver that falls more
than 10,000 appends behind on a slot loses the oldest entries, and a quiet stream that shares the
slot with a busy one can lose its only entry. Its readers then wake on their recheck. A repeat of
the newest batch adds none. The script answers the stream's length too, and when it is 1, Core adds
the slot to doorbell `d = s % doorbells`, at `<prefix>:{<tag of slot d * 16384 / doorbells>}:bell`,
apart from the script, so a lost ring is possible. The doorbells spread over the slot range, so a
cluster's primaries share them, and an application and its drivers must agree on the number of
doorbells. Drivers read the slot streams and the doorbells as the consumer group `streams-driver`
(`XREADGROUP`, `XAUTOCLAIM`, `XACK`, `XDEL`, `XGROUP CREATE`), and sweep with `XLEN` over each
primary's slots from `CLUSTER SLOTS`. The slot streams have no expiry, and a driver empties them.
The logged append script has its own hash. The plain one is unchanged, and an SDK's provider writes
no entries.

**Archive hold.** With `archive_lease` set, a driver copies each topic into an archive, and the
store holds what the archive doesn't hold yet. The floors hash keeps the holder `archive`. The
append script makes it at a topic's first write as `0-1|<now + lease>`, so it holds the whole
topic. `advance.lua` moves it to the archive's newest position with no minimum, renews the lease,
extends the keys' expiry to it, and keeps the position in the meta's `archived` field. The trim
reads the hold as it reads a reader's, so retention drops only what the archive holds. When the
archive stops for longer than the lease, the hold ends and the trim goes on, and nothing blocks
appends. The append script also adds the log key to `<prefix>:{<tag of slot s>}:a`, a sorted set
in the slot of the chain's notification stream, scored by the time of the write. A close adds it
with score `0`, so the archive takes closed topics first. `advance.lua` takes it out once the
archive holds the meta's `last`, and the archive's release takes it out after a closed topic's
end. The archive needs the notification log. The archived append script has its own hash. With an
archive, an application also needs `~temporal-streams:{*}:a +zadd +hexists`, and a driver also
needs `+zrange +zrem` on those keys. A delete takes a deleted topic's mark out of the set, so it
also needs `+zrem` there.

**Retired producers.** A field stays until a write's trim finds it older than the retention, so
a topic written by many short-lived producers holds one field per producer attempt of the last
retention window, about 235 bytes each. `RetireProducer` deletes one attempt's field with
`HDEL` and touches nothing else. A missing field or meta is no failure. Call it only once the
attempt's last append returned: a retry of an earlier batch would find no field to compare with
and land a second time. Readers lose nothing, since a store compares a batch with its own
attempt's field only and readers find a newer attempt from the records.

**Retention.** Every script that writes a log trims it with `XTRIM MINID` to the retention and
slides the expiry of the log and its meta, so a stream dies a retention after its last write and
nobody cleans up when the Workflow closes. The trim reads the oldest record with `XRANGE`, and only
when it is below the floor finds the newest record it drops with `XREVRANGE` back from the floor.
Both cost O(log n). `XREVRANGE` alone reads the whole retained log when no record is older than the
floor, and a forward read copies every dropped record into the script, which blocks Redis on a large
trim. The `+xrange` rule already covers the new call. The meta outlives its log by a 30-day grace,
as the tombstone that tells a log that expired from one that never existed.

**Activity owners.** A standalone Activity run is a chain of one run. Its keys have the hash tag
`{<namespace>:!a:<activity id>:<run id>}`, and every other part of the layout is the same. Each
part escapes `!`, so no Workflow id makes that tag, and a Workflow's delete pattern never matches
an Activity's keys. The tag starts with the namespace, so the documented ACL covers it.

**Workflow Activity owners.** An Activity that a Workflow run scheduled is a chain of one too.
Its keys have the hash tag `{<namespace>:!wa:<workflow id>:<run id>:<activity id>}`, so each
Activity lands in a slot of its own and Activity `1` of the next run is another chain. Such a
chain has one more key, `<prefix>:{<tag>}:topics`, a set of the log keys its producers wrote. The
append script adds the log key with `SADD` on every batch, since a first batch can be lost, and
slides the set's expiry to the retention plus the grace on each append. The scheduling Workflow's
Core reads it with `SMEMBERS` to close every topic when the Activity ends.

**Deletes.** Deleting an owner's streams scans every primary for the owner id's keys, across all
the Workflow's chains or all the Activity's runs. A Workflow's delete also scans for the keys of
every Activity it scheduled, in all its runs. `SCAN` makes no promise about keys that move to
another node while it runs, so a delete during a cluster reshard can miss some. Run it again once
the reshard is done.

**Server.** Redis 7.0 or later is required, and the store refuses an older server when it
connects. At the same moment it reads `maxmemory-policy` on every primary and logs a warning
for anything but `noeviction`, since an evicting Redis drops whole stream keys under memory
pressure, losing records and their dedupe state.

**ACL.** Every key of a namespace's streams starts with `<prefix>:{<namespace>:`, each part
percent-encoded. A Redis ACL user for one namespace's applications, with the rules the
conformance suite runs as:

```text
ACL SETUSER streams-app on >secret resetkeys ~temporal-streams:{my-ns:* resetchannels -@all +evalsha +eval +script|load +multi +exec +xadd +xread +xrevrange +xrange +xtrim +xlen +hget +hset +hgetall +hlen +hincrby +hdel +hscan +sadd +smembers +rpush +lrange +exists +del +unlink +pexpire +pttl +time +info +config|get +ping +hello +client|setinfo +cluster|slots
```

A cluster client reads the topology with `CLUSTER SLOTS` first. An ACL user lives on one node,
so on a cluster the user must exist on every node. Deleting an owner's streams also needs
`+scan`. With the notification log on, an application also needs `~temporal-streams:{*}:n
%W~temporal-streams:{*}:bell`, since its appends write the slot streams. Those entries name
other namespaces' streams and positions, never records. A driver needs those keys with read and
write, and `+xreadgroup +xautoclaim +xack +xdel +xgroup|create +xlen +xadd +cluster|slots`.
`+config|get` only reads `maxmemory-policy`, and the store goes on when a server refuses it.

**Plain mode.** `RedisStoreOptions::plain` runs the store with no Lua. An append is a
`MULTI` of `XADD`s that trim by age, a read is `XREAD` alone, and no meta and no close flag is
kept. So the store declares neither producer state nor a kept last position, and Core takes
their fallbacks: readers drop copies and end at the first `CLOSE` record, and a read decides
expiry from the record at its cursor. It exists so tests run those fallbacks on a real store.
Listing a chain's topics scans every primary, as a delete does, and needs `+scan`. With no script
there is no notification log, no floor hold and no archive hold, so plain mode declares neither
holds nor a change feed, and a config that asks for a log or an archive is refused.

**Tests.** The Redis tests run when `STREAMS_REDIS_URL` names a standalone server and
`STREAMS_REDIS_CLUSTER_URL` a cluster seed. `scripts/redis-test-servers.sh` starts both, the
cluster with one replica per primary. `STREAMS_REDIS_CLUSTER_ADMIN=1` lets the driver tests move
a slot and fail a primary over, so set it only for a cluster of the tests' own. The
conformance suite runs on each, once more as a user with exactly the ACL above, and once in
plain mode with the cases of the fallbacks. `tests/fallbacks.rs` adds faults on plain Redis
too. The cross-implementation test also needs `STREAMS_BROOK_PY`, a Python SDK checkout whose
provider predates the move.

**Managed Redis.** [managed-redis.md](managed-redis.md) covers Amazon MemoryDB and ElastiCache,
with the access string that those services take in place of `ACL SETUSER`.

**Kvrocks lane.** `STREAMS_KVROCKS_URL` names an Apache Kvrocks server, which keeps its data on
disk. The script starts one when it finds a `kvrocks` binary or Docker. The conformance suite runs
on it standalone, with the store cases, owner deletes and the notification log. This lane is a
test only. We make no support claim for Kvrocks. Its doorbell case does not run, since Kvrocks
answers an empty `XREADGROUP` where Redis fails with `NOGROUP`, so a driver never makes the
doorbell's group. Kvrocks has no ACL users and runs no Redis Cluster, so those lanes do not run.
