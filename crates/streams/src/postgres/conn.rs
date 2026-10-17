//! The connections: the URL, TLS as libpq reads it from the URL, and the pool.

use crate::{StreamError, StreamResult};
use deadpool_postgres::{Hook, HookError, Manager, ManagerConfig, Pool, RecyclingMethod};
use rustls::{
    ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::CryptoProvider,
    pki_types::{CertificateDer, ServerName, UnixTime, pem::PemObject},
};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_postgres::{
    AsyncMessage, Client, Connection, NoTls, Notification, config::SslMode, tls::TlsStream,
};
use tokio_postgres_rustls::MakeRustlsConnect;

/// The `sslmode` values libpq takes. `allow` is left out, since it trusts a plain connection
/// first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

/// Where and how to connect, read from one `postgres://` URL.
#[derive(Clone)]
pub(super) struct Target {
    pub(super) config: tokio_postgres::Config,
    tls: Option<MakeRustlsConnect>,
    pub(super) mode: Mode,
}

impl std::fmt::Debug for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Target")
            .field("hosts", &self.config.get_hosts())
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

impl Target {
    /// Reads a URL. `sslmode` and `sslrootcert` are taken out before the rest reaches
    /// `tokio-postgres`, which knows neither `verify-ca` nor `verify-full`.
    pub(super) fn from_url(url: &str) -> StreamResult<Self> {
        let (config, mode, root) = parse_url(url)?;
        let tls = match mode {
            Mode::Disable => None,
            mode => match CryptoProvider::get_default() {
                Some(provider) => Some(MakeRustlsConnect::new(client_config(
                    provider.clone(),
                    mode,
                    root.as_deref(),
                )?)),
                // A libpq built without TLS treats `prefer` as `disable` too.
                None if mode == Mode::Prefer => None,
                None => {
                    return Err(StreamError::refused(
                        "sslmode needs TLS, but the process installed no rustls crypto provider",
                    ));
                }
            },
        };
        let mut config = config;
        config.ssl_mode(match (&tls, mode) {
            (None, _) => SslMode::Disable,
            (Some(_), Mode::Prefer) => SslMode::Prefer,
            (Some(_), _) => SslMode::Require,
        });
        if mode != Mode::VerifyFull && !local(&config) {
            tracing::warn!(
                "The PostgreSQL store connects to a remote host with sslmode {mode:?}, which does \
                 not check the server's name. Use sslmode=verify-full."
            );
        }
        Ok(Self { config, tls, mode })
    }

    /// One connection of its own, outside the pool, with its connection task spawned.
    pub(super) async fn connect(&self) -> Result<Client, tokio_postgres::Error> {
        match &self.tls {
            Some(tls) => {
                let (client, connection) = self.config.connect(tls.clone()).await?;
                tokio::spawn(connection);
                Ok(client)
            }
            None => {
                let (client, connection) = self.config.connect(NoTls).await?;
                tokio::spawn(connection);
                Ok(client)
            }
        }
    }

    /// One connection of its own whose notifications arrive on the receiver. The receiver
    /// closes when the connection ends.
    pub(super) async fn listen(
        &self,
    ) -> Result<(Client, mpsc::UnboundedReceiver<Notification>), tokio_postgres::Error> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let config = self.listen_config();
        let client = match &self.tls {
            Some(tls) => {
                let (client, connection) = config.connect(tls.clone()).await?;
                tokio::spawn(forward(connection, sender));
                client
            }
            None => {
                let (client, connection) = config.connect(NoTls).await?;
                tokio::spawn(forward(connection, sender));
                client
            }
        };
        Ok((client, receiver))
    }

    /// The listener only receives, so a half-open connection shows only to TCP keepalive. The
    /// system default notices it after about two hours, and the listener's wakes stop meanwhile.
    fn listen_config(&self) -> tokio_postgres::Config {
        let mut config = self.config.clone();
        config
            .keepalives(true)
            .keepalives_idle(Duration::from_secs(30))
            .keepalives_interval(Duration::from_secs(10))
            .keepalives_retries(3);
        config
    }

    /// The pool of `size` connections. Each new connection pins the session settings the store
    /// functions depend on.
    pub(super) fn pool(&self, size: usize, timeout: Duration) -> StreamResult<Pool> {
        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let manager = match &self.tls {
            Some(tls) => Manager::from_config(self.config.clone(), tls.clone(), manager_config),
            None => Manager::from_config(self.config.clone(), NoTls, manager_config),
        };
        let settings = session_settings(timeout);
        Pool::builder(manager)
            .max_size(size.max(1))
            .runtime(deadpool_postgres::Runtime::Tokio1)
            .wait_timeout(Some(timeout))
            .create_timeout(Some(timeout))
            .recycle_timeout(Some(timeout))
            .post_create(Hook::async_fn(move |client, _| {
                let settings = settings.clone();
                Box::pin(async move {
                    client
                        .batch_execute(&settings)
                        .await
                        .map_err(HookError::Backend)
                })
            }))
            .build()
            .map_err(|error| StreamError::refused(format!("the PostgreSQL pool: {error}")))
    }
}

/// Drives a connection and hands its notifications on, until it ends.
async fn forward<S, T>(
    mut connection: Connection<S, T>,
    sender: mpsc::UnboundedSender<Notification>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    T: TlsStream + Unpin,
{
    while let Some(message) = std::future::poll_fn(|cx| connection.poll_message(cx)).await {
        match message {
            Ok(AsyncMessage::Notification(notification)) => {
                if sender.send(notification).is_err() {
                    return;
                }
            }
            Ok(_) => {}
            Err(error) => {
                tracing::debug!("The PostgreSQL store's listener connection ended: {error}");
                return;
            }
        }
    }
}

/// Every call is one statement that depends on `READ COMMITTED`: a statement that waited for a
/// row lock reads the newest version of the row. The session pins it, so a database default of
/// `SERIALIZABLE` can't change it.
pub(super) fn session_settings(timeout: Duration) -> String {
    format!(
        "SET statement_timeout = {}; SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL \
         READ COMMITTED",
        timeout.as_millis().max(1)
    )
}

fn local(config: &tokio_postgres::Config) -> bool {
    use tokio_postgres::config::Host;
    config.get_hosts().iter().all(|host| match host {
        Host::Tcp(name) => matches!(name.as_str(), "localhost" | "127.0.0.1" | "::1"),
        #[cfg(unix)]
        Host::Unix(_) => true,
    })
}

/// Splits `sslmode` and `sslrootcert` off a URL and parses the rest.
pub(super) fn parse_url(url: &str) -> StreamResult<(tokio_postgres::Config, Mode, Option<String>)> {
    if !(url.starts_with("postgres://") || url.starts_with("postgresql://")) {
        return Err(StreamError::refused(
            "the PostgreSQL store needs a postgres:// or postgresql:// URL",
        ));
    }
    let (base, query) = url.split_once('?').unwrap_or((url, ""));
    let mut mode = Mode::Prefer;
    let mut root = None;
    let mut rest = vec![];
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        match pair.split_once('=').unwrap_or((pair, "")) {
            ("sslmode", value) => {
                mode = match value {
                    "disable" => Mode::Disable,
                    "prefer" => Mode::Prefer,
                    "require" => Mode::Require,
                    "verify-ca" => Mode::VerifyCa,
                    "verify-full" => Mode::VerifyFull,
                    other => {
                        return Err(StreamError::refused(format!(
                            "the PostgreSQL store takes sslmode disable, prefer, require, \
                             verify-ca or verify-full, not {other:?}"
                        )));
                    }
                }
            }
            ("sslrootcert", value) => root = Some(unescape(value)),
            _ => rest.push(pair),
        }
    }
    let url = if rest.is_empty() {
        base.to_string()
    } else {
        format!("{base}?{}", rest.join("&"))
    };
    let mut config: tokio_postgres::Config = url
        .parse()
        .map_err(|error| StreamError::refused(format!("the PostgreSQL URL: {error}")))?;
    config.application_name("temporal-streams");
    // libpq treats `require` with a root certificate as `verify-ca`.
    if mode == Mode::Require && root.is_some() {
        mode = Mode::VerifyCa;
    }
    Ok((config, mode, root))
}

/// Percent-decodes a URL query value.
fn unescape(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match (bytes[i], bytes.get(i + 1..i + 3)) {
            (b'%', Some(hex)) => match u8::from_str_radix(&String::from_utf8_lossy(hex), 16) {
                Ok(byte) => {
                    out.push(byte);
                    i += 3;
                    continue;
                }
                Err(_) => out.push(b'%'),
            },
            (b'+', _) => out.push(b' '),
            (byte, _) => out.push(byte),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn client_config(
    provider: Arc<CryptoProvider>,
    mode: Mode,
    root: Option<&str>,
) -> StreamResult<ClientConfig> {
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|error| StreamError::refused(format!("TLS for PostgreSQL: {error}")))?;
    let config = match mode {
        Mode::Disable | Mode::Prefer | Mode::Require => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCheck(provider)))
            .with_no_client_auth(),
        Mode::VerifyCa | Mode::VerifyFull => {
            let roots = Arc::new(roots(root)?);
            let webpki =
                rustls::client::WebPkiServerVerifier::builder_with_provider(roots, provider)
                    .build()
                    .map_err(|error| {
                        StreamError::refused(format!("TLS for PostgreSQL: {error}"))
                    })?;
            if mode == Mode::VerifyFull {
                builder.with_webpki_verifier(webpki).with_no_client_auth()
            } else {
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(ChainOnly(webpki)))
                    .with_no_client_auth()
            }
        }
    };
    Ok(config)
}

/// The roots from `sslrootcert`, or the platform's for `system` or when it is absent.
fn roots(root: Option<&str>) -> StreamResult<RootCertStore> {
    let mut store = RootCertStore::empty();
    match root {
        Some(path) if path != "system" => {
            let certificates = CertificateDer::pem_file_iter(path)
                .and_then(|certificates| certificates.collect::<Result<Vec<_>, _>>())
                .map_err(|error| {
                    StreamError::refused(format!("sslrootcert {path:?} does not load: {error}"))
                })?;
            for certificate in certificates {
                store.add(certificate).map_err(|error| {
                    StreamError::refused(format!("sslrootcert {path:?}: {error}"))
                })?;
            }
        }
        _ => {
            let found = rustls_native_certs::load_native_certs();
            for certificate in found.certs {
                let _ = store.add(certificate);
            }
        }
    }
    if store.is_empty() {
        return Err(StreamError::refused(
            "sslmode verify-ca and verify-full need a root certificate, and none loaded",
        ));
    }
    Ok(store)
}

/// `prefer` and `require` encrypt and check nothing about the server, as in libpq.
#[derive(Debug)]
struct NoCheck(Arc<CryptoProvider>);

impl ServerCertVerifier for NoCheck {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// `verify-ca` checks the chain and not the name.
#[derive(Debug)]
struct ChainOnly(Arc<rustls::client::WebPkiServerVerifier>);

impl ServerCertVerifier for ChainOnly {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        use rustls::{CertificateError, Error};
        match self
            .0
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
        {
            Err(Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_url_gives_its_tls_settings_and_keeps_the_rest() {
        let (config, mode, root) = parse_url(
            "postgres://u:p@db.example.com:6432/streams?sslmode=verify-full&\
             sslrootcert=%2Fetc%2Frds.pem&connect_timeout=3",
        )
        .unwrap();
        assert_eq!(mode, Mode::VerifyFull);
        assert_eq!(root.as_deref(), Some("/etc/rds.pem"));
        assert_eq!(config.get_dbname(), Some("streams"));
        assert_eq!(config.get_ports(), [6432]);
        assert_eq!(config.get_connect_timeout(), Some(&Duration::from_secs(3)));
        assert_eq!(config.get_application_name(), Some("temporal-streams"));
        let (_, mode, _) = parse_url("postgresql://localhost/db").unwrap();
        assert_eq!(mode, Mode::Prefer);
        let (_, mode, _) = parse_url("postgres://h/db?sslmode=require&sslrootcert=ca.pem").unwrap();
        assert_eq!(mode, Mode::VerifyCa);
    }

    #[test]
    fn the_listener_notices_a_dead_connection_within_a_minute() {
        let target = Target {
            config: parse_url("postgres://u:p@localhost/db").unwrap().0,
            tls: None,
            mode: Mode::Disable,
        };
        let config = target.listen_config();
        assert!(config.get_keepalives());
        let detect = config.get_keepalives_idle()
            + config.get_keepalives_interval().unwrap() * config.get_keepalives_retries().unwrap();
        assert!(detect <= Duration::from_secs(60), "{detect:?}");
    }

    #[test]
    fn another_scheme_or_sslmode_is_refused() {
        for url in [
            "redis://localhost:6379",
            "host=localhost dbname=x",
            "postgres://h/db?sslmode=allow",
            "postgres://h/db?sslmode=bogus",
        ] {
            let error = parse_url(url).unwrap_err();
            assert_eq!(
                error.kind,
                crate::proto::StreamFailureKind::Refused,
                "{url}"
            );
        }
    }

    #[test]
    fn the_session_pins_the_timeout_and_read_committed() {
        let settings = session_settings(Duration::from_millis(1500));
        assert!(settings.contains("statement_timeout = 1500"), "{settings}");
        assert!(settings.contains("READ COMMITTED"), "{settings}");
    }
}
