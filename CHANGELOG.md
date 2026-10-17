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
- The crate generates the stream API from its own copy in `protos/api_stream`, taken from
  `moetemp/api` `c1a8d975`, since sdk-rust no longer vendors it. It builds on sdk-rust kj5.
