//! Worker configuration.
//!
//! Precedence (lowest to highest):
//!   1. Built-in defaults ([`WorkerConfig::default`])
//!   2. TOML file at `--config <path>`, or `/etc/prism/worker.toml` if it exists
//!   3. Environment variables with prefix `PRISM_`
//!      (`PRISM_SERVER_BIND`, `PRISM_DATA_DIR`, `PRISM_LOGGING_LEVEL`,
//!      `PRISM_RUNTIME_MAX_MEMORY_GB`)
//!   4. CLI flags
//!
//! Only [`WorkerConfig`] and [`load`] are public. Everything else is an
//! implementation detail of this module.
//!
//! Future TOML namespaces reserved but not yet implemented:
//!   `[tls]`, `[telemetry]`, `[object_store.s3]`, `[object_store.gcs]`,
//!   `[object_store.azure]`.

use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Default location the worker will probe for a TOML config when
/// `--config` is not supplied.
const DEFAULT_CONFIG_PATH: &str = "/etc/prism/worker.toml";

/// Minimal view of the CLI needed by [`load`]. The real `clap` struct in
/// `main.rs` should expose these fields (either directly or via a small
/// adapter).
#[derive(Debug, Default, Clone)]
pub struct CliArgs {
    /// Explicit `--config` path. If `None`, falls back to
    /// [`DEFAULT_CONFIG_PATH`] when that file exists.
    pub config: Option<PathBuf>,
    /// Legacy `--port`. When set, overrides the port portion of
    /// `server.bind`.
    pub port: Option<u16>,
    /// Legacy `--data-dir`. When set, overrides `data.data_dir`.
    pub data_dir: Option<PathBuf>,
}

/// Top-level worker configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WorkerConfig {
    pub server: ServerConfig,
    pub runtime: RuntimeConfig,
    pub data: DataConfig,
    pub logging: LoggingConfig,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            runtime: RuntimeConfig::default(),
            data: DataConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// `host:port` the gRPC server should bind to.
    pub bind: String,
    /// gRPC max decoding/encoding message size, in MB.
    pub max_message_size_mb: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:50051".to_string(),
            max_message_size_mb: 256,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    /// Memory budget for the worker (GB). 0 = unlimited.
    pub max_memory_gb: u64,
    /// Worker thread pool size. 0 = num_cpus.
    pub worker_threads: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_memory_gb: 0,
            worker_threads: 0,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DataConfig {
    /// Local Parquet directory used by the legacy/bench path.
    ///
    /// `None` means "no on-disk Parquet directory"; the worker relies
    /// purely on the in-memory PartitionStore. The example TOML ships
    /// `/data/prism` to document the recommended path, but omitting the
    /// field (or leaving it unset via CLI) preserves the legacy
    /// "in-memory only" behavior.
    pub data_dir: Option<PathBuf>,
}

impl Default for DataConfig {
    fn default() -> Self {
        Self { data_dir: None }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    /// `trace|debug|info|warn|error`.
    pub level: String,
    /// `pretty|json`.
    pub format: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: "pretty".to_string(),
        }
    }
}

/// Load configuration following the documented precedence rules.
pub fn load(cli: &CliArgs) -> anyhow::Result<WorkerConfig> {
    // 1. Defaults.
    let mut cfg = WorkerConfig::default();

    // 2. File (explicit --config wins; else probe the default path).
    if let Some(path) = &cli.config {
        cfg = load_from_file(path)?;
    } else if Path::new(DEFAULT_CONFIG_PATH).exists() {
        cfg = load_from_file(Path::new(DEFAULT_CONFIG_PATH))?;
    }

    // 3. Environment variables (only the four core knobs for this slice).
    apply_env_overrides(&mut cfg);

    // 4. CLI flags.
    apply_cli_overrides(&mut cfg, cli);

    Ok(cfg)
}

fn load_from_file(path: &Path) -> anyhow::Result<WorkerConfig> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("failed to read config file {}: {}", path.display(), e))?;
    let parsed: WorkerConfig = toml::from_str(&raw)
        .map_err(|e| anyhow::anyhow!("failed to parse config file {}: {}", path.display(), e))?;
    Ok(parsed)
}

fn apply_env_overrides(cfg: &mut WorkerConfig) {
    if let Ok(bind) = std::env::var("PRISM_SERVER_BIND") {
        if !bind.is_empty() {
            cfg.server.bind = bind;
        }
    }
    if let Ok(dir) = std::env::var("PRISM_DATA_DIR") {
        if !dir.is_empty() {
            cfg.data.data_dir = Some(PathBuf::from(dir));
        }
    }
    if let Ok(level) = std::env::var("PRISM_LOGGING_LEVEL") {
        if !level.is_empty() {
            cfg.logging.level = level;
        }
    }
    if let Ok(gb) = std::env::var("PRISM_RUNTIME_MAX_MEMORY_GB") {
        if let Ok(parsed) = gb.parse::<u64>() {
            cfg.runtime.max_memory_gb = parsed;
        }
    }
}

fn apply_cli_overrides(cfg: &mut WorkerConfig, cli: &CliArgs) {
    if let Some(port) = cli.port {
        cfg.server.bind = replace_port(&cfg.server.bind, port);
    }
    if let Some(dir) = &cli.data_dir {
        cfg.data.data_dir = Some(dir.clone());
    }
}

/// Replace the port suffix of a `host:port` bind string. If the existing
/// value cannot be split (no colon or malformed), fall back to
/// `0.0.0.0:<port>` to preserve the legacy behavior of `--port`.
fn replace_port(bind: &str, port: u16) -> String {
    match bind.rsplit_once(':') {
        Some((host, _)) if !host.is_empty() => format!("{}:{}", host, port),
        _ => format!("0.0.0.0:{}", port),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::Mutex;

    // Serialize env-var manipulation across tests in this module; the Rust
    // test harness otherwise runs them in parallel threads that share the
    // process env.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    const ENV_KEYS: &[&str] = &[
        "PRISM_SERVER_BIND",
        "PRISM_DATA_DIR",
        "PRISM_LOGGING_LEVEL",
        "PRISM_RUNTIME_MAX_MEMORY_GB",
    ];

    fn clear_env() {
        for k in ENV_KEYS {
            std::env::remove_var(k);
        }
    }

    fn write_tmp(contents: &str) -> PathBuf {
        let dir = std::env::temp_dir();
        let name = format!(
            "prism-worker-test-{}-{}.toml",
            std::process::id(),
            uniq_id()
        );
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).expect("create tmp file");
        f.write_all(contents.as_bytes()).expect("write tmp file");
        path
    }

    fn uniq_id() -> u64 {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        N.fetch_add(1, Ordering::Relaxed)
    }

    #[test]
    fn defaults_are_sensible() {
        let cfg = WorkerConfig::default();
        assert_eq!(cfg.server.bind, "0.0.0.0:50051");
        assert_eq!(cfg.server.max_message_size_mb, 256);
        assert_eq!(cfg.runtime.max_memory_gb, 0);
        assert_eq!(cfg.runtime.worker_threads, 0);
        assert_eq!(cfg.data.data_dir, None);
        assert_eq!(cfg.logging.level, "info");
        assert_eq!(cfg.logging.format, "pretty");
    }

    #[test]
    fn parses_full_toml() {
        let toml_text = r#"
[server]
bind = "127.0.0.1:60000"
max_message_size_mb = 512

[runtime]
max_memory_gb = 16
worker_threads = 8

[data]
data_dir = "/var/lib/prism"

[logging]
level = "debug"
format = "json"
"#;
        let cfg: WorkerConfig = toml::from_str(toml_text).expect("parse full toml");
        assert_eq!(cfg.server.bind, "127.0.0.1:60000");
        assert_eq!(cfg.server.max_message_size_mb, 512);
        assert_eq!(cfg.runtime.max_memory_gb, 16);
        assert_eq!(cfg.runtime.worker_threads, 8);
        assert_eq!(cfg.data.data_dir, Some(PathBuf::from("/var/lib/prism")));
        assert_eq!(cfg.logging.level, "debug");
        assert_eq!(cfg.logging.format, "json");
    }

    #[test]
    fn parses_minimal_toml_fills_defaults() {
        let toml_text = r#"
[server]
bind = "10.0.0.1:7777"
"#;
        let cfg: WorkerConfig = toml::from_str(toml_text).expect("parse minimal toml");
        assert_eq!(cfg.server.bind, "10.0.0.1:7777");
        // Other fields should be defaults.
        assert_eq!(cfg.server.max_message_size_mb, 256);
        assert_eq!(cfg.runtime.max_memory_gb, 0);
        assert_eq!(cfg.data.data_dir, None);
        assert_eq!(cfg.logging.level, "info");
    }

    #[test]
    fn env_overrides_file() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();

        let toml_text = r#"
[server]
bind = "10.0.0.1:7777"
"#;
        let path = write_tmp(toml_text);
        std::env::set_var("PRISM_SERVER_BIND", "1.2.3.4:99");

        let cli = CliArgs {
            config: Some(path.clone()),
            ..Default::default()
        };
        let cfg = load(&cli).expect("load");
        assert_eq!(cfg.server.bind, "1.2.3.4:99");

        let _ = std::fs::remove_file(&path);
        clear_env();
    }

    #[test]
    fn cli_overrides_env_and_file() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();

        let toml_text = r#"
[server]
bind = "10.0.0.1:7777"

[data]
data_dir = "/from/file"
"#;
        let path = write_tmp(toml_text);
        std::env::set_var("PRISM_SERVER_BIND", "1.2.3.4:99");
        std::env::set_var("PRISM_DATA_DIR", "/from/env");

        let cli = CliArgs {
            config: Some(path.clone()),
            port: Some(12345),
            data_dir: Some(PathBuf::from("/from/cli")),
        };
        let cfg = load(&cli).expect("load");
        // CLI port replaces the port portion of the env-provided bind.
        assert_eq!(cfg.server.bind, "1.2.3.4:12345");
        assert_eq!(cfg.data.data_dir, Some(PathBuf::from("/from/cli")));

        let _ = std::fs::remove_file(&path);
        clear_env();
    }

    #[test]
    fn missing_file_no_cli_returns_defaults() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();

        let cli = CliArgs::default();
        let cfg = load(&cli).expect("load without file");
        // Note: if /etc/prism/worker.toml happens to exist on the host
        // running tests, this check is still safe because we assert on
        // defaults-only semantics via a fresh cli and no env overrides.
        // On CI / dev machines the default path is not present, so cfg
        // equals the built-in defaults.
        if !Path::new(DEFAULT_CONFIG_PATH).exists() {
            assert_eq!(cfg.server.bind, "0.0.0.0:50051");
            assert_eq!(cfg.data.data_dir, None);
            assert_eq!(cfg.logging.level, "info");
        }
    }

    #[test]
    fn replace_port_preserves_host() {
        assert_eq!(replace_port("0.0.0.0:50051", 60000), "0.0.0.0:60000");
        assert_eq!(replace_port("127.0.0.1:1", 42), "127.0.0.1:42");
        // Fallback when malformed.
        assert_eq!(replace_port("garbage", 9000), "0.0.0.0:9000");
    }
}
