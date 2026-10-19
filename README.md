# sdk-streams

Experimental. Not supported. The repo moves to `temporalio/sdk-streams` once the SDK team takes it.

`temporalio-streams` is the stream runtime for Temporal SDKs. A Workflow, an Activity or a client
publishes records to a stream in a store it configures, and readers follow the stream. The SDKs
link this crate through their bridges. Temporal Core holds no stream code.

## Links

A producer writes a LINK record to name another stream. An OPEN says that a follower reads the
linked stream after the OPEN, from the link's `after`. A JOIN with the same name says that the
linked stream's segment ends at `until`, or at the stream's end when `until` is empty. `Follow`
reads a stream and the streams it links in one order. Order within each stream is kept, a linked
stream's records come after its OPEN, and the records after a JOIN come after the segment ended.
The answer's `state` resumes the follow, so a client or an Activity loses nothing and repeats
nothing. A follow reads links 4 deep and 16 open at once by default, and it skips a link past a
limit, a link back to an open stream and a link to another namespace or endpoint. A linked stream
it can't read is unavailable, and the follow goes on without it.

## Stage pins

The crate depends on `temporalio-common` and `temporalio-client` from one sdk-rust commit per
stage. That commit must equal the `sdk-core` submodule commit of the sdk-python stage that links
the crate.

| Stage | sdk-streams branch | sdk-rust commit |
|---|---|---|
| Brook | `moe/brook/ss1-import-store` and up | `moe/brook/k1-workflow-task-info` `c7080de9` |
| Jetty | `moe/jetty/sj1-import-jetty` and up | `moe/jetty/kj5-early-task-heartbeat` `bfe826f7` |
| Bridge | `moe/bridge/sb1-import-bridge` and up | `moe/bridge/kb1-pin` `da5faaca` |

## Linking from a lang bridge

A bridge that also links the sdk-rust crates must make the crate use its own copy of them, so that
one type exists for each. Use the dependency URL in this exact form, with no `.git` suffix. A key
that does not match byte for byte only warns "patch was not used", and the build then fails with
two copies of each type.

```toml
[dependencies.temporalio-streams]
git = "https://github.com/moetemp/sdk-streams"
rev = "<sha>"
default-features = false
features = ["redis"]

[patch."https://github.com/moetemp/sdk-rust"]
temporalio-common = { path = "./sdk-core/crates/common" }
temporalio-client = { path = "./sdk-core/crates/client" }
```

The crate features `tls-ring`, `tls-aws-lc` and `envconfig` forward to the sdk-rust crates. A FIPS
build takes `fips` with no default features. It turns on `tls-aws-lc` and the FIPS build of
`aws-lc-rs`, which needs CMake and Go.

## Building and testing

The build needs `protoc`. The crate generates `coresdk.streams` and `temporal.sdk.streams.v1` from
`crates/streams/protos`. The Temporal API protos under `protos/api_include` are a copy for imports
only. `scripts/sync-api-protos.sh` refreshes the copy from the pinned sdk-rust commit, and CI fails
when the copy differs. sdk-rust vendors no stream API, so the crate generates its own from
`protos/api_stream`, a copy of the `moetemp/api` commit in `protos/api_stream/API_COMMIT`. It calls
the stream notifier RPCs by method path through `Connection::raw_unary`.
`scripts/sync-stream-api.sh` refreshes that copy, and CI fails when it differs. In the same way,
`protos/api_nexus` holds the Nexus progress API (`NexusOperationProgress`, the `OnProgress`
messages and the `ExecuteSyncNexusOperation` messages), and `crates/streams/nexus` holds the
Nexus WIT and YAML that a lang's Nexus codegen reads. `scripts/sync-nexus-api.sh` refreshes them
from the commit in `protos/api_nexus/API_COMMIT`. A lang loads the crate's API copies and the
API that sdk-rust vendors into one descriptor pool, so `scripts/check-api-overlap.sh` fails when
the two share a file path or a full name.

- Format and lint: `cargo fmt --all -- --check`, then
  `cargo clippy --workspace --all-features --tests -- -D warnings`.
- Memory store and Core mock tests: `cargo test --workspace --all-features`. The Core mock tests
  in `tests/core_workflow.rs` drive Core's mock Worker from the pinned sdk-rust, with the test in
  lang's place.
- FIPS: `cargo tree -e normal,build --no-default-features --features fips,redis -i ring` prints
  nothing.
- Redis: run `crates/streams/scripts/redis-test-servers.sh`, then the test command with the URLs
  it prints, two or three with Kvrocks.

The Redis script starts one standalone server, a cluster of three primaries and three replicas,
and Kvrocks when a binary or its image is there. Set `STREAMS_STANDALONE_PORT`,
`STREAMS_CLUSTER_BASE` and `STREAMS_KVROCKS_PORT` to use other ports. The `cross_impl` tests need
sdk-python, so they run in the sdk-python lane.

## License

MIT. See `LICENSE.txt`.
