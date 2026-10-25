fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=protos");
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        // Use the sdk-rust types for the Temporal API messages, so that values need no conversion
        // between the crates. Not `.temporal.api.enums`, since sdk-rust lacks the Stream enums.
        .extern_path(
            ".temporal.api.common",
            "::temporalio_common::protos::temporal::api::common",
        )
        .extern_path(
            ".temporal.api.failure",
            "::temporalio_common::protos::temporal::api::failure",
        )
        .compile_protos(
            &[
                "protos/temporal/api/enums/v1/stream.proto",
                "protos/temporal/api/stream/v1/message.proto",
                "protos/temporal/api/streamservice/v1/request_response.proto",
                "protos/temporal/sdk/streams/v1/runtime.proto",
            ],
            &["protos", "protos/api_include"],
        )?;
    Ok(())
}
