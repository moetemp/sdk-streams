# sdk-streams

Internal and experimental.

`temporalio-streams` is the Stream runtime for Temporal SDKs. It serves the Stream service of the
Brook API proposal on the customer's Redis. The SDKs link this crate into their bridges. Lang
calls it through the bridge's generic extension calls. A Workflow writes through a Local Activity
per call, on the same calls. Temporal Core holds no stream code.

## Stage pins

The crate depends on `temporalio-common` and `temporalio-sdk-core` from one sdk-rust commit. That
commit must equal the `sdk-core` submodule commit of the sdk-python stage that links the crate.

| Stage | sdk-streams branch | sdk-rust branch |
|---|---|---|
| Brook v3 | `moe/brook/v3/s1-protos-and-ids` and up | `moe/brook/v3/c4-local-activity-workflow-task` `3a9203ac` |
| Brook v4 | `moe/brook/v4/s19-no-workflow-buffer` and up | `moe/brook/v4/c5-no-workflow-task-hooks` `b5b703ec` |
| Jetty v2 | `moe/jetty/v2/k1-service-store` and up | `moe/jetty/v2/c8-no-workflow-task-hooks` `dfd8399c` |

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

[patch."https://github.com/moetemp/sdk-rust"]
temporalio-common = { path = "./sdk-core/crates/common" }
temporalio-sdk-core = { path = "./sdk-core/crates/sdk-core" }
```

The crate features `tls-ring` and `tls-aws-lc` forward to the sdk-rust crates. A FIPS build takes
`fips` with no default features. It turns on `tls-aws-lc` and the FIPS build of `aws-lc-rs`, which
needs CMake and Go.

## Protos and shared vectors

- `protos/temporal/api/streamservice/v1/stream_service.proto` is the draft Stream service of the
  API proposal. sdk-rust doesn't have it, so the crate generates it. Lang generates the same file.
- `protos/temporal/sdk/streams/v1/extension.proto` has the crate's own extension calls.
- `nexus/stream-service.wit` is the Stream service as a System Nexus service, backed by the
  proto above. Each SDK copies it and generates its bindings with nexgen. A test keeps every
  line at most 100 columns.
- `nexus/stream-writer.wit` is the crate's writer calls (`OpenWriter`, `WriteRecords`,
  `CloseWriter`, `CloseWriters`) as an internal, client-only interface, backed by
  `extension.proto`. It is not a Temporal API, and no server serves it. Each SDK copies it beside
  the Stream service WIT and generates a `Writer` for Activities and clients.
- `protos/api_include` is a copy of the Temporal API protos, for imports only.
  `scripts/sync-api-protos.sh` refreshes it from the pinned sdk-rust commit, and CI fails when the
  copy differs.
- `testdata/*.json` are the shared vectors: Stream ids, Producer ids, Redis keys and meta fields,
  offsets, epoch keys and the withdrawal rule. `testdata/gen_vectors.py` writes them from a
  second implementation. Each SDK runs the same files.

## Building and testing

The build needs `protoc`.

- Format and lint: `cargo fmt --all -- --check`, then
  `cargo clippy --workspace --tests -- -D warnings`.
- Tests: `cargo test --workspace`. With no Redis set, the Store, service, Workflow and writer
  tests run on the memory Store only.
- FIPS: `cargo tree -e normal,build --no-default-features --features fips -i ring` prints
  nothing.
- Redis: run `crates/streams/scripts/redis-test-servers.sh`, then the test command with the two
  URLs it prints. It starts one standalone server and a cluster of three primaries. Set
  `STREAMS_STANDALONE_PORT` and `STREAMS_CLUSTER_PORTS` to use other ports. With
  `STREAMS_REQUIRE=redis`, a test with no URL fails instead of skipping.

`crates/streams/docs/redis.md` has the Redis layout.

## License

MIT. See `LICENSE.txt`.
