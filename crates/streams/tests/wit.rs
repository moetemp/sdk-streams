//! The Stream service WIT that each SDK copies and generates its bindings from.

const WIT: &str = include_str!("../nexus/stream-service.wit");
const WRITER_WIT: &str = include_str!("../nexus/stream-writer.wit");

#[test]
fn every_wit_line_fits_100_columns() {
    for wit in [WIT, WRITER_WIT] {
        let long: Vec<_> = wit
            .lines()
            .enumerate()
            .filter(|(_, line)| line.chars().count() > 100)
            .map(|(n, _)| n + 1)
            .collect();
        assert!(long.is_empty(), "lines over 100 columns: {long:?}");
    }
}

#[test]
fn the_wit_names_each_operation_of_the_service() {
    for operation in [
        "CreateStream",
        "AppendRecords",
        "CloseStream",
        "ReadRecords",
        "DescribeStream",
    ] {
        assert!(
            WIT.contains(&format!("@nexus.operation name=\"{operation}\"")),
            "{operation}"
        );
    }
    assert!(!WIT.contains("idle"), "the WIT still has the idle timeout");
}

#[test]
fn the_wit_declares_the_stream_handle_with_its_methods() {
    assert!(WIT.contains("  resource %stream {\n    constructor(%stream: stream-ref);\n"));
    for (method, operation) in [
        ("append-records", "append-records"),
        ("close", "close-stream"),
        ("read-records", "read-records"),
        ("describe", "describe-stream"),
    ] {
        assert!(
            WIT.contains(&format!(
                "    /// @nexus.operation \"{operation}\"\n    {method}: func("
            )),
            "{method}"
        );
    }
    assert!(
        WIT.contains(
            "create-stream: func(request: create-stream-request) -> create-stream-result;"
        )
    );
}

#[test]
fn the_writer_wit_is_client_only_and_names_each_writer_call() {
    assert!(WRITER_WIT.contains("/// @nexus.client-only\n"));
    assert!(!WRITER_WIT.contains("@nexus.endpoint"));
    for operation in ["OpenWriter", "WriteRecords", "CloseWriter", "CloseWriters"] {
        assert!(
            WRITER_WIT.contains(&format!("@nexus.operation name=\"{operation}\"")),
            "{operation}"
        );
    }
    assert!(WRITER_WIT.contains("  resource writer {\n    constructor(writer-id: u64);\n"));
    assert!(
        WRITER_WIT.contains("open-writer: func(request: open-writer-request) -> writer-result;")
    );
}
