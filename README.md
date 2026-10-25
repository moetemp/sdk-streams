# sdk-streams

Internal and experimental.

`temporalio-streams` is the Stream runtime for Temporal SDKs. It serves the Stream service on the
customer's Redis. The SDKs link this crate into their bridges. A bridge builds the runtime per
client, as a plain object, and calls `StreamRuntime::call` with a method name and proto bytes. A
Workflow makes each Stream call from a Local Activity. Temporal Core has no Stream code.

## Linking from a lang bridge

The crate depends on `temporalio-common` 1.1 from crates.io. A bridge that also links the sdk-rust
crates from its `sdk-core` submodule must patch `temporalio-common` to that copy, so that each type
exists once. The version of the patched copy must be 1.1 or a later 1.x.

```toml
[dependencies.temporalio-streams]
version = "0.1"
default-features = false

[patch.crates-io]
temporalio-common = { path = "./sdk-core/crates/common" }
```

Until the crate is on crates.io, a bridge takes it with `git` and `rev` in place of `version`. The
patch stays the same.

The crate features `tls-ring` and `tls-aws-lc` forward to `temporalio-common`. A FIPS build uses the
`fips` feature with no default features. It turns on `tls-aws-lc` and the FIPS build of `aws-lc-rs`,
which needs CMake and Go.

## Protos

- `protos/temporal/api/stream/v1/message.proto`,
  `protos/temporal/api/streamservice/v1/request_response.proto` and
  `protos/temporal/api/enums/v1/stream.proto` are the Stream protos, byte for byte copies of the api
  fork's files. sdk-rust doesn't have them, so the crate generates them. Lang generates the same
  files.
- `protos/temporal/sdk/streams/v1/runtime.proto` has the crate's own calls, such as the writer
  calls.
- `protos/api_include` is a copy of the Temporal API protos, for imports only.
  `scripts/sync-api-protos.sh` refreshes it from the sdk-rust release tag of the `temporalio-common`
  version in `Cargo.lock`, and CI fails when the copy differs.

## Building and testing

The build needs `protoc`.

- Format and lint: `cargo fmt --all -- --check`, then
  `cargo clippy --workspace --tests -- -D warnings`.
- Tests: `cargo test --workspace`.
- FIPS: `cargo tree -e normal,build --no-default-features --features fips -i ring` prints
  nothing.

## License

MIT. See `LICENSE.txt`.
