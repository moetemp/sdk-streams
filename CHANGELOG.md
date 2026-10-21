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
