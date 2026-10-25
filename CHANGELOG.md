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
  is a long poll with `create_wait`, filters by Retention, and ends with the close Record.
- Added the Workflow path. Lang hands each Activation's calls to `BufferWorkflowCalls`, the
  Workflow Task interceptor writes the creates and appends in `before_complete`, and it sends the
  closes in the background after `after_complete` accepts. `DrainCloses` waits for them. A flush
  splits at the batch limit, drops oversize Records and Records for a Stream that someone else
  closed, and runs `WAIT` with `durable_writes`.
- Added buffered writers for Activities and clients: `OpenWriter`, `Publish`, `FlushWriter` and
  `CloseWriter`. A writer flushes every `batch_interval`, at once on `force_flush`, and refuses a
  publish past its buffer limit. A writer of an Activity attempt above 1 first sends an empty
  batch, so the new epoch withdraws the earlier attempt's Records.
- Added `topic_prefixes` (field 10) to `ReadRecordsRequest`. A Record passes when its Topic is in
  `topics` or starts with a prefix.
- Added `MemoryStreamExtension` (`temporalio-streams-memory`), the Stream runtime on an in-process
  memory Store for tests. A `Store` trait has the calls of the Redis and memory Stores, and one
  test suite runs on both.
- Added `workflow_id` (2) and `first_execution_run_id` (3) to `StreamRef`. A Workflow's ref sets
  them, and the Store keys on `id` only.
- Split `Store` into the required log calls and capability calls. A call that needs a
  capability the Store lacks fails with UNIMPLEMENTED and the reason `stream_unsupported`.
- Reserved `ReadRecordsRequest.idle_timeout` (9) and `STREAM_RECORD_KIND_IDLE` (4). A read ends
  with Records, the close Record, or nothing after `wait`. A Reader that stops after a quiet
  period sets its own timer and resumes from `next_offset`.
- Added `StreamRef.backend` (4) and `RedisStreamConfig.backend` (10). The crate puts the
  configured backend name in each ref it builds, such as the one in a create answer, so every
  process routes a ref it receives. The Store never reads it.
- Added `FlushRunBuffer`. Lang calls it before a Local Activity starts, with the dispatching
  Workflow Task from `ActivityTask.Start` field 20. The crate writes the run's buffered creates
  and appends in that task's epoch, so the run's Records come before the Local Activity's. The
  task's completion writes only what came later, numbered after the early batches, and sends
  the withdrawal facts to each Stream the early write reached. A failed early write fails the
  task's completion, and a task that never completes has its early Records withdrawn and
  written again by the next task.
- Added `WriteRecords`, the awaited publish of an Activity or a client. It appends Records at
  once for an open writer, after the Records that `Publish` buffered, keeps the writer's
  Producer epoch and `sequence` order, resends the same batch after a Store failure, and answers
  the first and the last offset. The interval writer stays, but an SDK needs no interval flush
  for an awaited publish.
- A Workflow batch with withdrawal facts and no Records goes in also after a close, and a read
  that reaches the close Record gives the `Superseded` items written after it just before the
  close Record. So a task that completes after a close still withdraws a failed task's Records.
  A Workflow batch with Records that a close refuses is followed by one with only the facts.
- Added `nexus/stream-service.wit`, the source of the Stream service WIT that each SDK copies,
  with each line at most 100 columns.
- The WIT declares the Stream handle as `resource stream`, built from its `stream-ref`, with
  the methods `append-records`, `close`, `read-records` and `describe`. Each method takes its
  request without the `stream` field. `create-stream` returns the handle through
  `stream-result`. The proto messages and the operation names on the wire stay the same.
- Added `nexus/stream-writer.wit`, the writer calls as an internal, client-only WIT interface.
  `open-writer` returns `own<writer>`, a resource whose identity is `writer-id`, with
  `write-records` and `close`. The epoch and the sequence stay in the crate.
- Removed the buffered writer path: the `Publish` and `FlushWriter` calls and their messages,
  the interval flush, the buffer limit, the `stream_buffer_full` reason and its metric.
  `OpenWriterRequest` reserves `batch_interval` (4) and `max_buffered_records` (7). An SDK
  writes with `WriteRecords` and buffers on its own. `CloseWriter` and `CloseWriters` now only
  drop the writers.
- Added the native Store, `ServiceStore`, over the server's
  `temporal.api.streamservice.v1.StreamService`, called with `Connection::raw_unary` on the
  connection that `WorkerExtension::attach` gives. `NativeStreamExtension` builds an extension on
  it from a `NativeStreamConfig` (`backend`, and `retention` for creates with none), and
  `StreamExtension::for_backend` picks `redis`, `memory` or `native`. The service forwards whole
  reads and describes to the server. A Workflow create passes its owner, `close_with_run` and
  ref in `Owner::Workflow`, which Redis and memory don't read. Server errors map to the crate's
  reasons by their `StreamErrorDetails`.
- Synced `stream_service.proto` with the server's: `CreateStreamRequest.owner` (6),
  `close_with_run` (7), `ref` (8), `CloseStreamRequest.withdraw_run` (8) and
  `StreamErrorDetails`. The WIT has the same fields.
- `AppendRecordsRequest.defer_withdrawal` (9) marks the Workflow batch before a Local Activity
  on the server too, so the native Local Activity flush writes early as on Redis.
- Synced `stream_service.proto` with the server's `ReadRecordsRequest.to_offset` (11) and
  `wake_only` (12). The native Store forwards them. The Redis and memory reads don't use them.
- The native Store reads and describes over the direct System Nexus call
  (`WorkflowService.StartNexusOperation` on `__temporal_system`), not the `StreamService` gRPC
  reads, so every caller reads through the path Workflow code uses. A failed operation maps to
  the crate's reasons by its application failure type. A plain `RESOURCE_EXHAUSTED`, such as a
  direct read past the reader cap, maps to the new reason `stream_exhausted` with the server's
  message. A retry can pass.
- The Store calls that write (`create`, `append`, `close`) take the Stream's `StreamRef`, so the
  native Store sends the owner fields on every call. The server finds a Workflow's Stream by
  them. Redis and memory key on the id.
- The service `CreateStream` and `AppendRecords` take a Workflow's calls from outside Workflow
  code, such as S108's Local Activities. `CreateStreamRequest.owner` (6) makes the Stream
  Workflow-owned, and `withdraw_run` on both withdraws a retry run's previous run. The proto
  also has `close_with_run` (7) and `ref` (8), which only the server reads.
- Added `WorkflowReadRecords`, a `ReadRecords` that names the Workflow run that reads. On native
  the run goes as a `WorkflowEvent` link on the direct call, so Retention keeps a nested
  Workflow Stream from the lowest offset that the open run read (S109). Redis and memory read
  as `ReadRecords` does.
- Removed the Workflow write path that S108 replaced, since a Workflow now makes each create,
  publish and close from a Local Activity on the service calls. Gone are `BufferWorkflowCalls`,
  `FlushRunBuffer` and `DrainCloses` with their messages, the run buffer, the Workflow Task
  interceptor, the deferred withdrawal of the batch before a Local Activity, and the metrics
  only they kept (`stream_hook_latency`, `stream_flush_failures`, `stream_close_retries`,
  `stream_records_dropped`).
- Removed `durable_writes` and `durable_timeout` (`RedisStreamConfig` 6 and 7, now reserved), the
  `replica_wait` capability, `Store::wait_replicas` and `Store::durable_store`. Only the Workflow
  flush used them, and a Workflow write is now an Activity-path write with the same guarantee.
- Removed the Workflow withdrawal rule from the Store, since no caller appends with a Workflow
  Producer any more. The service refuses one, and S108's Local Activities write with their own
  Producer. Gone are the task index, the withdrawn tasks, the last facts and the Local Activity
  fence that used them, the facts-only batch after a close and the `Superseded` items that it
  left after the close Record, and the previous completed task and failed tasks on
  `Withdrawal`. A close takes no withdrawal. The `withdraws()` reader rule stays.
- A Local Activity batch that gives its run's last completed Workflow Task and failed tasks
  withdraws the run's orphaned Local Activity Records, those of tasks that never completed, with a
  Workflow-kind `Superseded` item that the reader rule already applies (S110). A late write of a
  withdrawn task is fenced. A rerun in a later task still supersedes by epoch. `OpenWriter` takes
  the same task facts, and each batch of the writer sends them.
- A dedupe entry keeps the batch's content digest, SHA-256 over its Topics and payloads with the
  metadata in key order. A resend with the last `sequence` and other content fails with
  `FAILED_PRECONDITION` and the reason `seq_divergent`, instead of answering the stored offsets.
  `testdata/digests.json` pins the digest for the server and every SDK.
- `Store::create`, `Store::append` and `Store::close` take the Stream's whole `StreamRef`, so a
  Store can check the owner fields. Redis and memory key on its id.
- Only the owner closes a Stream. A Workflow-owned Stream closes only from the close Activity of
  its run chain, as the run's Workflow Producer, and a ref that names another run chain is
  refused. A Local Activity can't close it, since it writes before its Workflow Task commits. A
  Stream that a client created closes only from a client. Others fail with `FAILED_PRECONDITION`
  and the reason `stream_not_owner`.
- Took the api a3 Stream service protos: the Activity owner fields of `StreamRef`, `CloseStatus`
  and `StreamRecord.close_status`, `task_token` on create, append and close,
  `CloseStreamRequest.status`, `ReadRecordsRequest.last`, `max_bytes` and `floor_from`, and
  `ReadRecordsResponse.continue_as_new_suggested`. The Stream service proto equals the api's
  copy byte for byte.
- A close Record has `close_status`. A close with no status is `COMPLETED`, or `FAILED` with a
  failure, and a caller may set `CANCELED`. A caller's `TERMINATED` or `TIMED_OUT` fails with
  `stream_invalid`, since only the owner's end on the server sets them. The result and the
  failure stay.
- `ReadRecordsRequest.last` reads the newest Records that pass the filter, oldest first, with the
  close Record at the end of a closed Stream. A Record that a newer `Superseded` item withdraws
  doesn't count. It never waits, and it takes no `from_offset` or `wake_only`. Redis reads it
  with `XREVRANGE` through the new `Store::read_back`.
- `ReadRecordsRequest.max_bytes` caps the bytes of a page's items. A larger entry still comes
  alone, so a read always moves on.
- The Redis and memory Stores read no `to_offset` or `wake_only`. A read with one fails with
  `stream_unsupported`, since only the native backend serves them.
- The runtime is a plain object with no Core trait. A bridge builds it per client and calls
  `StreamExtension::call` with a method name and proto bytes. An error has `code()` and
  `details()`, the encoded `StreamErrorDetails`. The crate no longer depends on
  `temporalio-sdk-core`, and it pins `temporalio-common` at sdk-rust Brook c7 `b565c433`.
- The handles of one Producer epoch on one Stream share one writer. Each `OpenWriter` adds a
  handle, and each `CloseWriter` or the new `FreeWriter` releases one. `FreeWriter` and
  `StreamExtension::free_writer` release with no wait, for a handle that lang drops. The last
  handle drops the writer, so a process keeps no writer that no handle uses.
- Every writer in a process takes its batch `sequence` from one counter, so a writer opened again
  after its close goes on above its earlier batches. The map of closed writers' sequences is
  gone, and it no longer grows for the life of the process.
- `ids::named_client_producer_id` makes `client:<name>`, a client Producer id that the caller
  keeps across restarts. With the caller's epoch in `attempt`, a new incarnation fences the
  earlier one and withdraws its Records, as an Activity retry does.
- The native runtime takes its client's connection when the bridge builds it:
  `NativeStreamExtension::from_config_bytes`, `NativeStreamExtension::create`,
  `StreamExtension::for_backend` and `ServiceStore::new` take a `Connection`. `attach` and
  `attach_connection` are gone, so one runtime never calls on another client's connection.
- The native Store sends the Activity task token on append, close and create, so the server
  checks the Producer against the Activity's current attempt. `OpenWriterRequest.task_token`
  (10) gives a writer's token, `Batch::task_token` carries it, and `Store::close` takes it.
  `Owner::Activity` is a Standalone Activity's create of its own Stream, with its token and the
  Activity owner fields of its ref. Redis and memory refuse it with `stream_unsupported`.
- The native Store sends the close Record's status as `CloseStreamRequest.status`.
- A create answers the Stream's ref with the owner fields of the create's ref, the Workflow's
  or the Standalone Activity's, so a caller routes the ref it gets on the native backend.
- A native `wake_only` read may wait up to 7 days, the server's `stream.maxWakeWait` default,
  since a parked wake adds nothing to History. Other reads keep the 60 s cap.
- A native error with the reason `seq_divergent` or `stream_not_owner` reads back to its reason.
  The native tests write with named clients, since the server refuses an Activity Producer with
  no task token.
  earlier one.
- A client Producer's newer epoch fences its earlier epochs but writes no `Superseded` item and
  withdraws nothing, in the Redis script, the memory Store and the reader rule. The withdrawal
  rule stays for Workflow, Local Activity and Activity Producers. `testdata/withdrawals.json` has
  the case.
- A Workflow read (`WorkflowReadRecords`) with `last` fails with `stream_invalid` on every
  backend, since a replay must read the same range.
- A Standalone Activity's create of its own Stream sends its owner Producer
  (`sa:<activity id>:<activity run id>` at the token's attempt) together with its task token, as
  the server checks both. `Owner::Activity` carries both. A create with a task token and no
  Standalone Activity owner, or no Activity ref, fails with `stream_invalid`, and it needs no
  safety TTL.
- Removed the auto close, so a Redis or memory Stream closes only on a close call.
  `CreateStreamRequest` reserves `auto_close` (5) and `DescribeStreamResponse` reserves
  `auto_close` (4). The WIT drops `auto-close` from the create and the describe. Gone are the
  `auto_close` capability, `Store::auto_close`, the `auto_close` argument of `Store::create`, the
  auto close script and the `ac` and `la` meta fields. A create is idempotent for one Retention
  only. A read writes nothing. The scripts ignore `ac` and `la` on a Stream that an older crate
  created.
- The crate pins `temporalio-common` at sdk-rust Brook c8 `320e725f`, which reverts the cache-off
  eviction reason. The crate uses nothing that the revert touches.
- The native Store sends no `auto_close` on create and has no `Store::auto_close`, as the server
  has no auto close either. A Workflow create, one with an owner and no task token, still passes
  its `close_with_run` to the server, which closes the Stream when the owner's run chain ends.
  The Stream service proto equals the api a4 copy byte for byte.
- The crate pins `temporalio-common` and `temporalio-client` at sdk-rust Jetty c10 `191c8b8b`,
  which takes Brook c8's revert of the cache-off eviction reason.
- A read of a closed Stream whose Retention passed fails with `stream_expired`, as a read of an
  open one does. It no longer waits out its `wait` and answers nothing.
- A read whose scan stops at the 10,000-entry cap scans on at once while entries up to the
  newest one are left, instead of waiting for a new append. A read with no `wait` still answers
  an empty page with a moved `next_offset`. A filtered page reads the log 1,000 entries at a
  time, whatever its `max_records`.
- A page checks the state that each log read gives. A trim between two reads of one page fails a
  read from an offset with `stream_expired`, so the gap doesn't pass in silence.
- `OpenWriter` of an Activity attempt above 1 takes a closed or an expired Stream, as it takes a
  Stream that nobody created. A retry that only reads the Stream still runs, and its first write
  fails with `stream_closed` or `stream_expired`.
- A batch `sequence` is from 1 to 2^53 - 1, and the append script keeps it in the dedupe entry
  as sent. A sequence of 1e14 or more lost its dedupe entry on Redis before, so a resend went in
  twice. A higher one fails with `stream_invalid`.
- `AppendRecords` takes `withdraw_run` only from a Local Activity of another run than the one it
  names, as `CreateStream` takes it only with an owner. A client or an Activity that names a run
  fails with `stream_invalid`, instead of withdrawing and fencing that run.
- A close of a Workflow-owned Stream whose ref has no `workflow_id` fails with `stream_not_owner`
  when the Stream keeps its run chain. It skipped the chain check before.
- `docs/redis.md` lists the Redis commands of a Producer and of a Reader, and `tests/acl.rs` runs
  each role with exactly those. A Reader needs write access to the keys too, since `EVALSHA`
  counts as a write. `RedisStore::connect` skips the version check when the user may not run
  `INFO`, instead of failing.
- `testdata/withdrawals.json` has the cases of a `Superseded` item with no epoch, as a retry run's
  create writes, of a Local Activity's newer epoch, and of more Local Activity Records under a
  Workflow item. A case with no `epoch` key has none, and `gen_vectors.py` reads it so.
- The crate pins `temporalio-common` and `temporalio-client` at sdk-rust Jetty c10 `2de24aef`,
  which takes Brook c8's Core CHANGELOG fix and Core tests.
- A native `FAILED_PRECONDITION` with no Stream reason, such as a namespace that isn't active on
  this cluster, maps to `stream_storage`, which a retry can pass. It mapped to `stream_closed`
  before, so a write during a failover failed for good.
