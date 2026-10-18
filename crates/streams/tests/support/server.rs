/// The server URL in `variable`, or `None` so a case with no server set skips. A CI job sets
/// `STREAMS_REQUIRE` to the services it starts, such as `redis`, and a missing URL of one of
/// them fails there, so a job can't pass with its cases skipped.
pub(crate) fn server_url(variable: &str) -> Option<String> {
    if let Ok(url) = std::env::var(variable) {
        return Some(url);
    }
    let required = std::env::var("STREAMS_REQUIRE").unwrap_or_default();
    for service in required.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let prefix = format!("STREAMS_{}_", service.to_uppercase());
        assert!(
            !variable.starts_with(&prefix),
            "STREAMS_REQUIRE names {service}, but {variable} is not set"
        );
    }
    eprintln!("set {variable} to run this case");
    None
}
