# Changelog

## Unreleased

### Added

- The `temporalio-streams` crate, the Stream runtime that Temporal SDKs link into their bridges.
  It serves the five Stream service operations and the writer calls on Redis, and on a memory
  Store for tests.
