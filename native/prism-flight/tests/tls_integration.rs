//! End-to-end TLS / mTLS integration test for the Flight service.
//!
//! Spins up a `ShuffleFlightService` over TLS on a loopback port and
//! exercises three paths:
//!
//! 1. **Plaintext client is rejected.** A `tonic::Channel` built
//!    without `.tls_config()` cannot establish an HTTP/2 connection to
//!    a TLS server and must fail.
//! 2. **TLS client with a valid CA + client cert succeeds.** Confirms
//!    the mTLS happy path: server identity verifies, client identity
//!    verifies, `do_action("ping")` returns `"pong"`.
//! 3. **TLS client without a client cert is rejected in mTLS mode.**
//!    The handshake must fail because the server has
//!    `client_ca_root(..)` configured.
//!
//! The test uses `openssl` at runtime to mint a fresh CA / server /
//! client pair per run (no shared on-disk state). If `openssl` isn't on
//! the PATH the test is skipped with a clear message — CI images we
//! care about ship openssl.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::Action;
use prism_flight::shuffle_writer::{PartitionStore, ShuffleFlightService};
use prism_flight::tls::{load_server_tls_config, TlsConfig};
use tokio::net::TcpListener;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, Server};

/// Small fixture holding a per-test temp dir with dev-cert material.
struct Certs {
    _dir: tempdir::TempDir,
    ca: PathBuf,
    server_cert: PathBuf,
    server_key: PathBuf,
    client_cert: PathBuf,
    client_key: PathBuf,
}

// We don't want to pull in a new crate just for tempdir — use
// env::temp_dir with a unique suffix instead.
mod tempdir {
    use std::path::{Path, PathBuf};

    pub struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        pub fn new(prefix: &str) -> std::io::Result<Self> {
            use std::sync::atomic::{AtomicU64, Ordering};
            static N: AtomicU64 = AtomicU64::new(0);
            let id = N.fetch_add(1, Ordering::Relaxed);
            let p = std::env::temp_dir().join(format!(
                "{}-{}-{}-{}",
                prefix,
                std::process::id(),
                id,
                rand_suffix()
            ));
            std::fs::create_dir_all(&p)?;
            Ok(TempDir { path: p })
        }
        pub fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn rand_suffix() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or(0)
    }
}

fn openssl_on_path() -> bool {
    std::process::Command::new("openssl")
        .arg("version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Mint a throwaway CA + server cert (CN=localhost, SAN covers
/// 127.0.0.1) + client cert. We shell out to openssl because this is a
/// test fixture and pulling in a Rust PKI crate just to generate three
/// certs would be disproportionate.
fn mint_certs() -> Certs {
    assert!(
        openssl_on_path(),
        "openssl not on PATH — install it to run the TLS integration test"
    );

    let dir = tempdir::TempDir::new("prism-tls-it").expect("mkdir tempdir");
    let base = dir.path();

    // CA
    let ca_key = base.join("ca.key");
    let ca_crt = base.join("ca.crt");
    run_ok("openssl", &["genrsa", "-out", ca_key.to_str().unwrap(), "2048"]);
    run_ok(
        "openssl",
        &[
            "req",
            "-x509",
            "-new",
            "-key",
            ca_key.to_str().unwrap(),
            "-days",
            "1",
            "-subj",
            "/CN=prism-test-ca",
            "-out",
            ca_crt.to_str().unwrap(),
        ],
    );

    // Server cert (CN=localhost, SAN=localhost,127.0.0.1,::1)
    let server_key = base.join("server.key");
    let server_csr = base.join("server.csr");
    let server_crt = base.join("server.crt");
    let server_ext = base.join("server.ext");
    std::fs::write(
        &server_ext,
        "subjectAltName = DNS:localhost, IP:127.0.0.1, IP:::1\n\
         extendedKeyUsage = serverAuth\n",
    )
    .unwrap();
    run_ok("openssl", &["genrsa", "-out", server_key.to_str().unwrap(), "2048"]);
    run_ok(
        "openssl",
        &[
            "req",
            "-new",
            "-key",
            server_key.to_str().unwrap(),
            "-subj",
            "/CN=localhost",
            "-out",
            server_csr.to_str().unwrap(),
        ],
    );
    run_ok(
        "openssl",
        &[
            "x509",
            "-req",
            "-in",
            server_csr.to_str().unwrap(),
            "-CA",
            ca_crt.to_str().unwrap(),
            "-CAkey",
            ca_key.to_str().unwrap(),
            "-CAcreateserial",
            "-days",
            "1",
            "-extfile",
            server_ext.to_str().unwrap(),
            "-out",
            server_crt.to_str().unwrap(),
        ],
    );

    // Client cert (CN=prism-client)
    let client_key = base.join("client.key");
    let client_csr = base.join("client.csr");
    let client_crt = base.join("client.crt");
    let client_ext = base.join("client.ext");
    std::fs::write(&client_ext, "extendedKeyUsage = clientAuth\n").unwrap();
    run_ok("openssl", &["genrsa", "-out", client_key.to_str().unwrap(), "2048"]);
    run_ok(
        "openssl",
        &[
            "req",
            "-new",
            "-key",
            client_key.to_str().unwrap(),
            "-subj",
            "/CN=prism-client",
            "-out",
            client_csr.to_str().unwrap(),
        ],
    );
    run_ok(
        "openssl",
        &[
            "x509",
            "-req",
            "-in",
            client_csr.to_str().unwrap(),
            "-CA",
            ca_crt.to_str().unwrap(),
            "-CAkey",
            ca_key.to_str().unwrap(),
            "-CAcreateserial",
            "-days",
            "1",
            "-extfile",
            client_ext.to_str().unwrap(),
            "-out",
            client_crt.to_str().unwrap(),
        ],
    );

    Certs {
        _dir: dir,
        ca: ca_crt,
        server_cert: server_crt,
        server_key,
        client_cert: client_crt,
        client_key,
    }
}

fn run_ok(cmd: &str, args: &[&str]) {
    let out = std::process::Command::new(cmd)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("spawn {cmd}: {e}"));
    assert!(
        out.status.success(),
        "{cmd} {args:?} failed:\n--- stdout ---\n{}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Spawn a `ShuffleFlightService` on a free loopback port with the
/// supplied TLS config. Returns the bound `host:port` and a shutdown
/// signal sender.
async fn spawn_tls_server(tls: TlsConfig) -> (String, tokio::sync::oneshot::Sender<()>) {
    let material = load_server_tls_config(&tls).expect("load tls");

    // Pick an ephemeral port without race windows: bind with tokio,
    // capture addr, close, hand port to tonic.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);

    let store = Arc::new(PartitionStore::new());
    let svc = FlightServiceServer::new(ShuffleFlightService::new(store));

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        Server::builder()
            .tls_config((*material.server_config).clone())
            .expect("install tls")
            .add_service(svc)
            .serve_with_shutdown(addr, async move {
                let _ = rx.await;
            })
            .await
            .expect("serve");
    });

    // Give the server a beat to actually start listening.
    tokio::time::sleep(Duration::from_millis(100)).await;
    (format!("127.0.0.1:{}", addr.port()), tx)
}

fn read(p: &Path) -> Vec<u8> {
    std::fs::read(p).unwrap_or_else(|e| panic!("read {p:?}: {e}"))
}

#[tokio::test]
async fn mtls_happy_path_and_rejections() {
    if !openssl_on_path() {
        eprintln!("skipping: openssl not on PATH");
        return;
    }

    let certs = mint_certs();

    let tls = TlsConfig {
        cert_path: certs.server_cert.clone(),
        key_path: certs.server_key.clone(),
        client_ca_path: Some(certs.ca.clone()),
        client_cn_pattern: String::new(),
    };

    let (addr, shutdown) = spawn_tls_server(tls).await;

    // -- 1. Plaintext client must fail to connect. --
    // `grpc://…` → no TLS config → TLS server will reject the
    // connection. We expect an error from either `connect()` or the
    // first RPC.
    let plaintext_uri = format!("http://{}", addr);
    let plain_err: Result<(), Box<dyn std::error::Error + Send + Sync>> = async {
        let ch = Endpoint::from_shared(plaintext_uri)?
            .connect_timeout(Duration::from_secs(2))
            .connect()
            .await?;
        let mut client = FlightServiceClient::new(ch);
        client
            .do_action(Action {
                r#type: "ping".into(),
                body: vec![].into(),
            })
            .await?;
        Ok(())
    }
    .await;
    assert!(
        plain_err.is_err(),
        "plaintext client should not reach a TLS-only server, got Ok"
    );

    // -- 2. TLS client with valid identity succeeds (mTLS). --
    let ca = Certificate::from_pem(read(&certs.ca));
    let ident = Identity::from_pem(read(&certs.client_cert), read(&certs.client_key));
    let tls_cfg = ClientTlsConfig::new()
        .domain_name("localhost")
        .ca_certificate(ca.clone())
        .identity(ident);

    let ch = Endpoint::from_shared(format!("https://{}", addr))
        .unwrap()
        .tls_config(tls_cfg)
        .unwrap()
        .connect()
        .await
        .expect("mtls connect");
    let mut client = FlightServiceClient::new(ch);
    let mut stream = client
        .do_action(Action {
            r#type: "ping".into(),
            body: vec![].into(),
        })
        .await
        .expect("do_action")
        .into_inner();
    let result = stream
        .message()
        .await
        .expect("recv")
        .expect("have message");
    assert_eq!(&result.body[..], b"pong");

    // -- 3. TLS client WITHOUT a client cert must be rejected. --
    let tls_cfg_no_ident = ClientTlsConfig::new()
        .domain_name("localhost")
        .ca_certificate(ca);
    let no_ident_err: Result<(), Box<dyn std::error::Error + Send + Sync>> = async {
        let ch = Endpoint::from_shared(format!("https://{}", addr))?
            .tls_config(tls_cfg_no_ident)?
            .connect_timeout(Duration::from_secs(2))
            .connect()
            .await?;
        let mut client = FlightServiceClient::new(ch);
        client
            .do_action(Action {
                r#type: "ping".into(),
                body: vec![].into(),
            })
            .await?;
        Ok(())
    }
    .await;
    assert!(
        no_ident_err.is_err(),
        "client without an identity must not pass mTLS handshake"
    );

    let _ = shutdown.send(());
}

#[tokio::test]
async fn server_auth_only_mode_works_without_client_cert() {
    if !openssl_on_path() {
        eprintln!("skipping: openssl not on PATH");
        return;
    }

    let certs = mint_certs();

    // No client CA → one-way TLS.
    let tls = TlsConfig {
        cert_path: certs.server_cert.clone(),
        key_path: certs.server_key.clone(),
        client_ca_path: None,
        client_cn_pattern: String::new(),
    };
    let (addr, shutdown) = spawn_tls_server(tls).await;

    let ca = Certificate::from_pem(read(&certs.ca));
    let tls_cfg = ClientTlsConfig::new()
        .domain_name("localhost")
        .ca_certificate(ca);

    let ch = Endpoint::from_shared(format!("https://{}", addr))
        .unwrap()
        .tls_config(tls_cfg)
        .unwrap()
        .connect()
        .await
        .expect("one-way tls connect");
    let mut client = FlightServiceClient::new(ch);
    let mut stream = client
        .do_action(Action {
            r#type: "ping".into(),
            body: vec![].into(),
        })
        .await
        .expect("ping")
        .into_inner();
    let result = stream.message().await.expect("recv").expect("message");
    assert_eq!(&result.body[..], b"pong");

    let _ = shutdown.send(());
}
