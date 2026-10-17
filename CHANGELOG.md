# Changelog

## Unreleased

- Added the `temporalio-streams` crate: the `StreamStore` trait, the memory and Redis stores, the
  cursor format, the record helpers and the lang byte call `StreamService`.
- Added the `coresdk.streams` and `temporal.sdk.streams.v1` protos, built in the crate against the
  pinned sdk-rust.
- Added `connect_stream_service`, which builds the process-wide service over a client
  `Connection` and takes lang's runtime meter.
