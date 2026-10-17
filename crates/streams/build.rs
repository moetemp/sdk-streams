/// A lang can log or keep these messages as JSON with the `serde_serialize` feature, which also
/// gives the API messages they hold their serde forms.
const SERDE_PACKAGES: &[&str] = &[".coresdk.streams", ".temporal.sdk.streams"];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=protos");
    let mut builder = tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        // One type for each API message, so a value moves between this crate and the sdk-rust
        // crates with no conversion. The well-known types map to `prost_types`, which is
        // `prost-wkt-types` here as in the sdk-rust crates.
        .extern_path(
            ".temporal.api",
            "::temporalio_common::protos::temporal::api",
        );
    for package in SERDE_PACKAGES {
        builder = builder.type_attribute(
            *package,
            "#[cfg_attr(feature = \"serde_serialize\", \
             derive(::serde::Serialize, ::serde::Deserialize))]",
        );
    }
    builder.compile_protos(
        &[
            "protos/temporal/sdk/core/streams/streams.proto",
            "protos/temporal/sdk/streams/v1/message.proto",
        ],
        &["protos", "protos/api_include"],
    )?;
    Ok(())
}
