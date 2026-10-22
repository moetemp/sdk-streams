//! The Stream service WIT that each SDK copies and generates its bindings from.

const WIT: &str = include_str!("../nexus/stream-service.wit");

#[test]
fn every_wit_line_fits_100_columns() {
    let long: Vec<_> = WIT
        .lines()
        .enumerate()
        .filter(|(_, line)| line.chars().count() > 100)
        .map(|(n, _)| n + 1)
        .collect();
    assert!(long.is_empty(), "lines over 100 columns: {long:?}");
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
