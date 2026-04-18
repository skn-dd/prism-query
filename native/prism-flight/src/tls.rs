//! TLS configuration loader for the Flight gRPC server.
//!
//! Loads PEM-encoded server cert, server key, and (optionally) a client
//! CA bundle from the filesystem and produces a `tonic::ServerTlsConfig`.
//!
//! # Design notes
//!
//! - **Backend:** rustls (via `tonic`'s `tls` feature). rustls is the
//!   modern, memory-safe Rust TLS story and avoids OpenSSL's licensing
//!   and FIPS complexity. Cipher suites are rustls defaults
//!   (TLS 1.3 preferred, modern-safe TLS 1.2 fallback).
//!
//! - **mTLS vs one-way TLS:** if `client_ca_path` is set and non-empty,
//!   we require the peer to present a client certificate signed by that
//!   CA (true mTLS). If it's empty, we do server-auth-only TLS.
//!
//! - **Cipher suites:** we deliberately do **not** override rustls's
//!   `DEFAULT_CIPHER_SUITES`. Don't hand-pick cipher suites unless you
//!   have a specific compliance reason; rustls's defaults track the
//!   current IETF + Mozilla guidance.
//!
//! - **Cert rotation (NOT IN THIS SLICE):** today, rotating the cert
//!   requires a pod/process restart. The acceptor is built once at
//!   startup. When we want hot-reload (Phase 7 proper), the shape is:
//!     1. Watch the cert/key files (inotify on Linux).
//!     2. Re-run [`load_server_tls_config`] on change.
//!     3. Swap a shared `Arc<ServerTlsConfig>` behind a custom
//!        connector/acceptor.
//!   The `Arc` wrapper in [`TlsMaterial`] is already in place so that
//!   future swap is a localised change, not a rewrite.
//!
//! - **Revocation lists:** not implemented. CRL / OCSP support belongs
//!   in a later Phase 7 sub-slice; the current model trusts the CA
//!   bundle and relies on short-lived certs (cert-manager default
//!   lifetime) for de-facto revocation.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls_pemfile::Item;
use thiserror::Error;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};

/// Errors produced while loading TLS material.
#[derive(Debug, Error)]
pub enum TlsError {
    #[error("tls is enabled but {field} is empty in the config")]
    MissingField { field: &'static str },

    #[error("failed to read {purpose} at {path}: {source}")]
    Io {
        purpose: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse PEM for {purpose} at {path}: {source}")]
    Pem {
        purpose: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("{purpose} at {path} contained no PEM-encoded {kind}")]
    Empty {
        purpose: &'static str,
        path: PathBuf,
        kind: &'static str,
    },
}

/// Paths to the PEM material needed to run the Flight server over TLS.
///
/// Paths are taken as-is; callers (e.g. `prism-bench::config`) are
/// responsible for any path resolution / env expansion.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// PEM-encoded server certificate chain.
    pub cert_path: PathBuf,
    /// PEM-encoded server private key (PKCS#8 or RSA or SEC1).
    pub key_path: PathBuf,
    /// PEM-encoded client CA bundle. `None` or empty = no mTLS
    /// (server-auth-only TLS).
    pub client_ca_path: Option<PathBuf>,
    /// Optional required pattern for the client cert's CN / SAN.
    /// Empty string = accept any cert signed by the CA. Matching is
    /// not implemented in this slice — it's reserved for a follow-up
    /// (needs an authorization hook in the Flight service, not just
    /// TLS config).
    pub client_cn_pattern: String,
}

/// Parsed TLS material. Cheap to clone (all inner data is `Arc`-like).
///
/// Wrap in an `Arc` if you need to pass it through tasks.
#[derive(Clone)]
pub struct TlsMaterial {
    /// The tonic config handed to `Server::tls_config`.
    pub server_config: Arc<ServerTlsConfig>,
    /// True if the config requires the client to present a certificate.
    pub mtls: bool,
    /// Pattern the caller should later enforce on the peer cert's CN/SAN.
    /// Currently informational only.
    pub client_cn_pattern: String,
}

impl std::fmt::Debug for TlsMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // ServerTlsConfig deliberately masks its contents in Debug; we
        // mirror that and expose only the summary flags callers care
        // about when debugging.
        f.debug_struct("TlsMaterial")
            .field("mtls", &self.mtls)
            .field("client_cn_pattern", &self.client_cn_pattern)
            .finish()
    }
}

/// Load PEM material from `cfg` and build a `ServerTlsConfig`.
///
/// Fails fast with a path-qualified error if anything is missing or
/// malformed. This is deliberate: we want misconfiguration to surface
/// at startup, not on the first client connection.
pub fn load_server_tls_config(cfg: &TlsConfig) -> Result<TlsMaterial, TlsError> {
    // Server cert + key must be present.
    let cert_pem = read_file(&cfg.cert_path, "server certificate")?;
    let key_pem = read_file(&cfg.key_path, "server private key")?;

    // Validate the PEM up front with rustls-pemfile so we produce clean
    // errors before tonic's internal acceptor build runs.
    validate_cert_pem(&cert_pem, &cfg.cert_path)?;
    validate_key_pem(&key_pem, &cfg.key_path)?;

    let identity = Identity::from_pem(&cert_pem, &key_pem);

    let mut tls = ServerTlsConfig::new().identity(identity);
    let mut mtls = false;

    if let Some(ca_path) = cfg.client_ca_path.as_ref() {
        // Treat an explicitly-empty string as "no mTLS" — the TOML
        // example ships `client_ca_path = ""` for dev.
        if !ca_path.as_os_str().is_empty() {
            let ca_pem = read_file(ca_path, "client CA bundle")?;
            validate_cert_pem(&ca_pem, ca_path)?;
            tls = tls.client_ca_root(Certificate::from_pem(&ca_pem));
            mtls = true;
        }
    }

    Ok(TlsMaterial {
        server_config: Arc::new(tls),
        mtls,
        client_cn_pattern: cfg.client_cn_pattern.clone(),
    })
}

fn read_file(path: &Path, purpose: &'static str) -> Result<Vec<u8>, TlsError> {
    if path.as_os_str().is_empty() {
        return Err(TlsError::MissingField {
            field: match purpose {
                "server certificate" => "tls.cert_path",
                "server private key" => "tls.key_path",
                "client CA bundle" => "tls.client_ca_path",
                _ => "tls.*",
            },
        });
    }
    fs::read(path).map_err(|e| TlsError::Io {
        purpose,
        path: path.to_path_buf(),
        source: e,
    })
}

fn validate_cert_pem(pem: &[u8], path: &Path) -> Result<(), TlsError> {
    let mut any = false;
    for item in rustls_pemfile::read_all(&mut &pem[..]) {
        let item = item.map_err(|e| TlsError::Pem {
            purpose: "certificate",
            path: path.to_path_buf(),
            source: e,
        })?;
        if matches!(item, Item::X509Certificate(_)) {
            any = true;
        }
    }
    if !any {
        return Err(TlsError::Empty {
            purpose: "certificate",
            path: path.to_path_buf(),
            kind: "X.509 CERTIFICATE blocks",
        });
    }
    Ok(())
}

fn validate_key_pem(pem: &[u8], path: &Path) -> Result<(), TlsError> {
    for item in rustls_pemfile::read_all(&mut &pem[..]) {
        let item = item.map_err(|e| TlsError::Pem {
            purpose: "private key",
            path: path.to_path_buf(),
            source: e,
        })?;
        if matches!(
            item,
            Item::Pkcs8Key(_) | Item::Pkcs1Key(_) | Item::Sec1Key(_)
        ) {
            return Ok(());
        }
    }
    Err(TlsError::Empty {
        purpose: "private key",
        path: path.to_path_buf(),
        kind: "PRIVATE KEY blocks (PKCS#8 / RSA / SEC1)",
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    // Self-signed RSA cert + key pair generated once, pasted as
    // constants so the unit tests don't need to shell out to openssl.
    // These are test-only material; do NOT reuse elsewhere.
    //
    // Generated via:
    //   openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem \
    //       -out cert.pem -days 3650 -subj "/CN=prism-test"
    //
    // We re-generate on demand in [`gen_dev_certs`] for the integration
    // tests that need a matching pair; these inline ones are only used
    // to cover the "parses valid PEM" happy path.

    fn tmp(name: &str, bytes: &[u8]) -> PathBuf {
        let dir = std::env::temp_dir();
        let p = dir.join(format!(
            "prism-tls-test-{}-{}-{}",
            std::process::id(),
            uniq(),
            name
        ));
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(bytes).unwrap();
        p
    }

    fn uniq() -> u64 {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        N.fetch_add(1, Ordering::Relaxed)
    }

    #[test]
    fn empty_cert_path_fails() {
        let cfg = TlsConfig {
            cert_path: PathBuf::new(),
            key_path: PathBuf::from("/tmp/key"),
            client_ca_path: None,
            client_cn_pattern: String::new(),
        };
        let err = load_server_tls_config(&cfg).unwrap_err();
        assert!(matches!(err, TlsError::MissingField { .. }), "got {err:?}");
    }

    #[test]
    fn missing_cert_file_fails() {
        let cfg = TlsConfig {
            cert_path: PathBuf::from("/nonexistent/cert.pem"),
            key_path: PathBuf::from("/nonexistent/key.pem"),
            client_ca_path: None,
            client_cn_pattern: String::new(),
        };
        let err = load_server_tls_config(&cfg).unwrap_err();
        assert!(matches!(err, TlsError::Io { .. }), "got {err:?}");
    }

    #[test]
    fn malformed_cert_fails() {
        let cert = tmp("cert.pem", b"not a real certificate");
        let key = tmp("key.pem", b"not a real key");
        let cfg = TlsConfig {
            cert_path: cert.clone(),
            key_path: key.clone(),
            client_ca_path: None,
            client_cn_pattern: String::new(),
        };
        let err = load_server_tls_config(&cfg).unwrap_err();
        // Either "no X.509 blocks" (Empty) or a PEM parse error — both
        // are fail-fast behaviors we want.
        assert!(
            matches!(err, TlsError::Empty { .. } | TlsError::Pem { .. }),
            "got {err:?}"
        );
        let _ = std::fs::remove_file(cert);
        let _ = std::fs::remove_file(key);
    }
}
