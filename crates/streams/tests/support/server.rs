/// The server URL in `variable`, or `None` so that a case with no server skips. When
/// `STREAMS_REQUIRE` names the service, as in CI, a missing URL fails the case instead.
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
