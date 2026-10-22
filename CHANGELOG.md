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
- Added `CreateStreamRequest.auto_close` (5) and `DescribeStreamResponse.auto_close` (4). A
  Stream with an auto close closes with no result when no Record was appended for that long,
  counted from the create. Each write step applies it before it acts, and a read or a describe
  that finds it due applies it with the new auto close script. A long-polling read wakes when it
  comes due. A create with another auto close is a conflict. A Store without the new
  `auto_close` capability refuses such a create with `stream_unsupported`.
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
  the crate's reasons by its application failure type.
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
  withdrawn task is fenced. A rerun in a later task still supersedes by epoch.
