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
- Added LINK records. `AppendRecord.link` and `OutputRecord.link` carry a `StreamLink` that names a
  Workflow's stream with the stream API's `StreamReference`. The batch digest takes each link field,
  and a batch with no link keeps its digest.
- Added `Follow`, which reads a stream and the streams it links in one order and resumes from its
  `state`, and `ReadResponse.start_cursor`.
- A follow reads a stale LINK as a plain record. It opens and joins nothing, so a late OPEN from an
  older attempt can't open a segment again after its JOIN.
