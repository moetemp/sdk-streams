# Changelog

## Unreleased

- Added the `temporalio-streams` crate: the `StreamStore` trait, the memory and Redis stores, the
  cursor format, the record helpers and the lang byte call `StreamService`.
- Added the `coresdk.streams` and `temporal.sdk.streams.v1` protos, built in the crate against the
  pinned sdk-rust.
- Added `connect_stream_service`, which builds the process-wide service over a client
  `Connection` and takes lang's runtime meter.
- Added the Workflow publish calls `PublishWorkflowTask`, `RunEvicted` and `ShutdownWorker`. Lang
  appends a live activation's records before it completes the activation, under the attempt its
  `WorkflowTaskHistoryInfo` gives. The chain closes after a live terminal completion once the
  run's History shows that completion closed the run.
- Added the `fips` feature, which builds TLS on `aws-lc-rs` in FIPS mode with no `ring`.
- Read the run's History tail for the chain close, not its description. The close event must match
  the terminal command, and the task before it must be the completed one.
- Added the Core mock tests of the Workflow publish, including the four chain-close contract cases.
- Imported Jetty's stream runtime: links and follow reads, the PostgreSQL store, the S3 and
  local-file archive with the stream driver, the stream notifier clients' traits, subscriptions,
  Workflow Activity and standalone Activity owners, and the store capabilities.
- Moved the link checks into the crate, and grew `OutputRecord` with `link` for LINK records.
- `connect_stream_service` now sends stream notifications, attaches Workflow readers, waits and
  polls through the client `Connection`, and describes standalone Activity and Workflow Activity
  owners.
- The Workflow publish takes LINK records and checks each link as a follower reads it.
- Added `fold_stream_wake`, `fold_stream_wakes` and `STREAM_WAKE_TYPE_URL`, the reference fold of
  the stream wakes on a Workflow Task's attachments, with the shared vectors
  `testdata/wake_folds.json` and the codec notes in `docs/wakes.md`.
- `PublishWorkflowTask` takes `resolved_owner_activities`, the owner Activities an activation
  resolves. The service closes each one's streams after the records land, and `ShutdownWorker`
  waits for the closes. `STREAM_OWNER_HEADER` names the header lang marks an owner with.
- Ported Jetty's Core tests onto Core's mock Worker with the test as lang: owner Activity closes,
  LINK records in the publish, and the stream wakes on a task's attachments.
- The crate generates the stream API from its own copy in `protos/api_stream`, taken from
  `moetemp/api` `c1a8d975`, since sdk-rust no longer vendors it. It builds on sdk-rust kj5.
- The notifier client calls the stream notifier RPCs by method path through
  `Connection::raw_unary`, with the namespace header, since sdk-rust lists none of them.
- The stream API copy is a plain copy of `moetemp/api` `9d888ec4`, where the notifier RPC
  messages have their own `stream_request_response.proto`.
- `StreamService` serves `DescribeStreamNotifier` through the notifier client.
- Imported Bridge's stream runtime: the stream service's Nexus operations (`NexusAppend`,
  `NexusRead`, `NexusReadRange`, `NexusFollow`, `NexusFollowRange`, `NexusResume`), the caller
  attach and detach, the operation token and the `temporal_streams.nexusrpc.yaml` contract.
  It builds on sdk-rust kb1.
- The crate generates the Nexus progress API from its own copy in `protos/api_nexus`, and holds
  the Nexus WIT and YAML in `crates/streams/nexus`, all taken from `moetemp/api` `2d204ff9`. The
  stream API copy moves to the same commit, with no file change.
- CI checks that the crate's API copies share no file path and no full name with the API that
  the pinned sdk-rust vendors.
