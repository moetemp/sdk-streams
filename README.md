# sdk-streams

Experimental. Not supported. The repo moves to `temporalio/sdk-streams` once the SDK team takes it.

`temporalio-streams` is the stream runtime for Temporal SDKs. A Workflow, an Activity or a client
publishes records to a stream in a store it configures, and readers follow the stream. The SDKs
link this crate through their bridges. Temporal Core holds no stream code.

## Stage pins

The crate depends on `temporalio-common` and `temporalio-client` from one sdk-rust commit per
stage. That commit must equal the `sdk-core` submodule commit of the sdk-python stage that links
the crate.

| Stage | sdk-streams branch | sdk-rust commit |
|---|---|---|
| Brook | `moe/brook/ss1-import-store` and up | `moe/brook/k1-workflow-task-info` `4ebdacab` |
| Jetty | `moe/jetty/sj1-import-jetty` and up | `moe/jetty/kj3-early-task-heartbeat` `969a0883` |

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
features = ["redis", "postgres", "archive"]

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
when the copy differs.

- Format and lint: `cargo fmt --all -- --check`, then
  `cargo clippy --workspace --all-features --tests -- -D warnings`.
- Memory store and Core mock tests: `cargo test --workspace --all-features`. The Core mock tests
  in `tests/core_workflow.rs` drive Core's mock Worker from the pinned sdk-rust, with the test in
  lang's place.
- FIPS: `cargo tree -e normal,build --no-default-features --features fips,redis,postgres,archive
  -i ring` prints nothing.
- Redis: run `crates/streams/scripts/redis-test-servers.sh`, then the test command with the two
  URLs it prints.

- PostgreSQL: `eval "$(crates/streams/scripts/postgres-test-server.sh)"` starts 14, 17 and
  PgBouncer in Docker, then run the test command.
- S3 archive: point `STREAMS_S3_URL` and the `AWS_*` variables at a bucket, such as one on MinIO.

The Redis script starts one standalone server, a cluster of three primaries and three replicas,
and Kvrocks when a binary or its image is there. Set `STREAMS_STANDALONE_PORT`,
`STREAMS_CLUSTER_BASE` and `STREAMS_KVROCKS_PORT` to use other ports. The `cross_impl` tests need
sdk-python, so they run in the sdk-python lane.

## License

MIT. See `LICENSE.txt`.
