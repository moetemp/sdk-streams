# Changelog

## Unreleased

- Added the `temporalio-streams` crate with the Stream service protos of the API proposal and the
  crate's extension call protos.
- Added the Stream id, Producer id, Redis key, offset and epoch rules, the withdrawal rule of a
  `Superseded` item, and their shared vectors.
- Added the crate's error, which goes to lang as an extension error with a reason token.
- Added the Redis Store, with one Lua script per create, append, close and read. The scripts
  fence lower epochs, dedupe resends, flag the first batch that withdraws, keep the task index
  and the withdrawn tasks and runs, and keep the format version, the TTLs and the Retention.
- Added `RedisStreamExtension`, the Worker extension that serves `CreateStream`,
  `AppendRecords`, `CloseStream`, `ReadRecords` and `DescribeStream` through `call_async`. A read
  is a long poll with `create_wait` and `idle_timeout`, filters by Retention, and ends with the
  close Record or an Idle Record.
- Added the Workflow path. Lang hands each Activation's calls to `BufferWorkflowCalls`, the
  Workflow Task interceptor writes the creates and appends in `before_complete`, and it sends the
  closes in the background after `after_complete` accepts. `DrainCloses` waits for them. A flush
  splits at the batch limit, drops oversize Records and Records for a Stream that someone else
  closed, and runs `WAIT` with `durable_writes`.
