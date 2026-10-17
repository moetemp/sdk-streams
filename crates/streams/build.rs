/// A lang can log or keep these messages as JSON with the `serde_serialize` feature, which also
/// gives the API messages they hold their serde forms.
const SERDE_PACKAGES: &[&str] = &[".coresdk.streams", ".temporal.sdk.streams"];

/// The stream API types, which sdk-rust does not vendor. The crate generates them from its copy
/// in `protos/api_stream`.
const STREAM_API_TYPES: &[&str] = &[
    ".temporal.api.stream",
    ".temporal.api.enums.v1.StreamOwnerKind",
];

const SERDE_ATTRIBUTE: &str =
    "#[cfg_attr(feature = \"serde_serialize\", derive(::serde::Serialize, ::serde::Deserialize))]";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=protos");
    let includes = &["protos/api_stream", "protos", "protos/api_include"];

    // The crate's own protos. One type for each API message, so a value moves between this crate
    // and the sdk-rust crates with no conversion. The well-known types map to `prost_types`,
    // which is `prost-wkt-types` here as in the sdk-rust crates.
    let mut builder = tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        .extern_path(
            ".temporal.api",
            "::temporalio_common::protos::temporal::api",
        );
    // The longest matching path wins over `.temporal.api`.
    for path in STREAM_API_TYPES {
        builder = builder.extern_path(*path, format!("crate::protos{}", path.replace('.', "::")));
    }
    for package in SERDE_PACKAGES {
        builder = builder.type_attribute(*package, SERDE_ATTRIBUTE);
    }
    builder.compile_protos(
        &[
            "protos/temporal/sdk/core/streams/streams.proto",
            "protos/temporal/sdk/streams/v1/message.proto",
        ],
        includes,
    )?;

    // The stream API itself. prost generates no type that an extern path names, so this pass
    // names only the API types the stream API uses from `temporalio-common`.
    let mut builder = tonic_prost_build::configure()
        .build_server(false)
        .build_client(false);
    for path in [
        ".temporal.api.common",
        ".temporal.api.failure",
        ".temporal.api.enums.v1.CallbackState",
    ] {
        builder = builder.extern_path(
            path,
            format!("::temporalio_common::protos{}", path.replace('.', "::")),
        );
    }
    for path in STREAM_API_TYPES {
        builder = builder.type_attribute(*path, SERDE_ATTRIBUTE);
    }
    builder.compile_protos(
        &[
            "protos/api_stream/temporal/api/stream/v1/message.proto",
            "protos/api_stream/temporal/api/enums/v1/stream.proto",
            "protos/api_stream/temporal/api/workflowservice/v1/stream_notifier.proto",
        ],
        includes,
    )?;
    Ok(())
}
