fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=protos");
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        // Use the sdk-rust types for the Temporal API messages, so that values need no conversion
        // between the crates. sdk-rust has no Stream service, so the crate generates it.
        .extern_path(
            ".temporal.api.common",
            "::temporalio_common::protos::temporal::api::common",
        )
        .extern_path(
            ".temporal.api.failure",
            "::temporalio_common::protos::temporal::api::failure",
        )
        .extern_path(
            ".temporal.api.enums",
            "::temporalio_common::protos::temporal::api::enums",
        )
        .compile_protos(
            &[
                "protos/temporal/api/streamservice/v1/stream_service.proto",
                "protos/temporal/sdk/streams/v1/extension.proto",
            ],
            &["protos", "protos/api_include"],
        )?;
    Ok(())
}
