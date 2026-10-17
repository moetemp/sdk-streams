# Managed Redis services for the stream store

This page tells how the Redis store runs on the managed Redis services of AWS. The scripts, the
key layout and the commands are the ones in [redis.md](redis.md). Only the access string and the
limits below change.

**Status.** The conformance suite has not run on a real MemoryDB or ElastiCache endpoint. The
facts below come from the AWS pages that each item names, and from a local run of the suite as a
user with the MemoryDB access string. Treat MemoryDB as the target, not as a tested store, until a
run on a real cluster passes. The lane notes at the end tell how to do that run.

## Amazon MemoryDB for Valkey or Redis OSS

MemoryDB is the AWS production target for the Redis store.

- **Durability.** MemoryDB writes each command to a Multi-AZ transaction log before it answers
  (https://docs.aws.amazon.com/memorydb/latest/devguide/what-is-memorydb.html). So a failover
  does not lose an acknowledged append, which it can on OSS Redis with asynchronous replicas.
- **Retention.** All data is in memory on r6g and r7g nodes. Retention is bound by memory, as on
  OSS Redis. Size the cluster for the write rate times the retention.
- **Data tiering.** On r6gd nodes, MemoryDB moves values that are not in use to SSD and keeps the
  keys in memory (https://docs.aws.amazon.com/memorydb/latest/devguide/data-tiering.html). A topic
  log is one Redis stream, so it moves as one item. A topic that takes appends stays in memory. A
  quiet or closed topic can move to SSD. An item above 128 MiB never moves, so a topic log above
  128 MiB stays in memory. Only `noeviction`, `volatile-lru` and `allkeys-lru` are allowed. Use
  `noeviction`, since the store warns on an evicting policy.
- **Version.** MemoryDB runs Redis OSS 7 or Valkey 7.2 and later. The store refuses a server that
  reports a `redis_version` below 7. We did not see what a MemoryDB Valkey node reports.
- **Restricted commands.** MemoryDB refuses `CONFIG`
  (https://docs.aws.amazon.com/memorydb/latest/devguide/restrictedcommands.html). The store reads
  `maxmemory-policy` with `CONFIG GET` when it connects, and goes on without a warning when the
  server refuses it. `EVAL`, `EVALSHA`, streams, consumer groups, `SCAN` and `CLUSTER SLOTS` are
  not on the list.
- **Cluster mode.** A MemoryDB cluster is always a Redis Cluster. Set `cluster` in the store
  options and give the cluster endpoint as the URL. MemoryDB needs TLS for ACL users, so the URL
  is `rediss://<user>:<password>@<cluster endpoint>:6379/0`.

### The MemoryDB access string

MemoryDB takes users through its API, not `ACL SETUSER`. An access string has no password, no
`reset` rules and no `config` rule
(https://docs.aws.amazon.com/memorydb/latest/devguide/clusters.acls.html). This is the access
string for one namespace's applications, with the notification log on and owner deletes. The
conformance suite runs as a user with exactly these rules on local Redis.

```text
on ~temporal-streams:{my-ns:* ~temporal-streams:{*}:n ~temporal-streams:{*}:bell -@all +evalsha +eval +script|load +multi +exec +xadd +xread +xrevrange +xrange +xtrim +xlen +hget +hset +hgetall +hlen +hincrby +hdel +hscan +sadd +smembers +rpush +lrange +exists +del +unlink +scan +pexpire +pttl +time +info +ping +hello +client|setinfo +cluster|slots
```

It differs from the `ACL SETUSER` rules in [redis.md](redis.md) in four ways.

- It has no `>secret`. The password goes in the user's authentication mode.
- It has no `resetkeys` and no `resetchannels`. A new user has no keys and no channels.
- It has no `+config|get`. The store goes on without the eviction check.
- The doorbell keys use `~`, not `%W~`. We do not know if MemoryDB takes key selectors with
  permissions. `~` gives read access too, which the application does not need.

Without the notification log, drop the two `{*}` key patterns. A driver also needs
`+xreadgroup +xautoclaim +xack +xdel +xgroup|create` on the `{*}` keys.

Create the user and its ACL with the AWS CLI. Then give the ACL to the cluster.

```shell
aws memorydb create-user --user-name streams-app \
  --authentication-mode Type=password,Passwords=<password> \
  --access-string "<the access string above>"
aws memorydb create-acl --acl-name streams --user-names streams-app
```

## Amazon ElastiCache for Valkey or Redis OSS

- **Node-based clusters.** Replication is asynchronous and there is no durable write log. A
  failover can lose acknowledged appends, as on OSS Redis. Retention is bound by memory.
- **Data tiering.** r6gd nodes tier as MemoryDB does, with the same 128 MiB item limit, and with
  the LFU policies too. Tiering is not supported with durability-enabled clusters
  (https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/data-tiering.html).
- **Serverless.** It supports `EVAL`, `EVALSHA`, `SCRIPT LOAD`, the stream and consumer group
  commands the store uses, `HSCAN`, `SCAN`, `UNLINK` and `TIME`. `CLUSTER SLOTS` answers one
  shard with every slot
  (https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/SupportedCommands.html). The driver
  then sweeps every slot on one node. We did not run the suite on it.
- **Access.** `CONFIG` and `ACL SETUSER` are restricted. ElastiCache users take an access string
  through the API, so use the MemoryDB access string above.

ElastiCache gives capacity without a durable log. Use it as a hot store, not as the only copy of
streams that must stay for long.

## Retention is bound by memory

OSS Redis, Valkey, MemoryDB without tiering and ElastiCache keep every record in memory. AOF and
RDB copy memory to disk. They do not let the data set grow past `maxmemory`, and with
`noeviction` a full server refuses appends. A larger `retention` on such a server costs memory for
the whole window. Data tiering lifts the bound for quiet topics only.

## Lane notes

The suite runs on a managed endpoint as a user that the endpoint already has, since MemoryDB and
ElastiCache refuse `ACL SETUSER`. The user needs every command of the access string above on the
keys `temporal-streams*` and `conformance*`, since each case uses a prefix of its own. Run it from a
host in the cluster's VPC.

```shell
export STREAMS_REDIS_CLUSTER_URL=rediss://<user>:<password>@<cluster endpoint>:6379/0
cargo test -p temporalio-streams --features redis --test conformance -- cluster_redis \
  --skip documented_acl --skip memorydb_acl
```

The skipped lanes create ACL users, and the endpoint refuses that. Leave `STREAMS_REDIS_URL` and
`STREAMS_REDIS_CLUSTER_ADMIN` unset, since the other test files need a server of their own.
