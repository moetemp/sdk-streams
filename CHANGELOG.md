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
  server shows the run closed.
