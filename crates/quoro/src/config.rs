//! The binary's configuration file (D-0054).
//!
//! `/etc/quoro/quoro.toml` is TOML with four optional keys; an absent file
//! means the defaults below, so a fresh host boots with `quoro serve` alone.
//! Unknown keys are refused instead of silently ignored, so a typo in a key
//! name cannot quietly fall back to a default that writes to the wrong place.

use serde::Deserialize;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// Where the installer puts the configuration (D-0054).
pub const DEFAULT_PATH: &str = "/etc/quoro/quoro.toml";

/// Where the installer puts the database, the TLS material and the token.
pub const DEFAULT_DATA_DIR: &str = "/var/lib/quoro";

/// The one HTTPS port of D-0054.
pub const DEFAULT_LISTEN: &str = "0.0.0.0:8443";

/// Which engine the local node driver runs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EngineKind {
    /// The in-memory engine: no hypervisor, used for development and tests.
    #[default]
    Fake,
    /// Cloud Hypervisor over KVM; not built into this binary yet.
    CloudHypervisor,
}

impl EngineKind {
    /// The name used in logs and in the configuration file.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fake => "fake",
            Self::CloudHypervisor => "cloud-hypervisor",
        }
    }
}

/// The parsed `quoro.toml`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Address of the single HTTPS listener.
    #[serde(default = "default_listen")]
    pub listen: SocketAddr,
    /// Database, TLS material and the setup token.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    /// Engine the in-process node driver runs.
    #[serde(default)]
    pub engine: EngineKind,
    /// Name this node reports to the control plane.
    #[serde(default = "default_node_name")]
    pub node_name: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            data_dir: default_data_dir(),
            engine: EngineKind::default(),
            node_name: default_node_name(),
        }
    }
}

impl Config {
    /// Reads `path`; a missing file is the defaults (D-0054).
    ///
    /// # Errors
    ///
    /// An I/O error other than "not found", or a file that is not valid TOML
    /// for this shape.
    pub fn load(path: &Path) -> io::Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => toml::from_str(&text)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{path:?}: {e}"))),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e),
        }
    }
}

/// The hostname from `/etc/hostname`, or `quoro` when it cannot be read.
fn default_node_name() -> String {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "quoro".to_string())
}

fn default_listen() -> SocketAddr {
    DEFAULT_LISTEN
        .parse()
        .expect("DEFAULT_LISTEN is a socket address")
}

fn default_data_dir() -> PathBuf {
    PathBuf::from(DEFAULT_DATA_DIR)
}

/// Creates `path` (and parents) if needed and restricts it to the owner, as
/// `/var/lib/quoro` holds the database, the TLS key and the setup token.
///
/// # Errors
///
/// An I/O error from creating the directory or setting its mode.
pub fn ensure_data_dir(path: &Path) -> io::Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_overrides() {
        let config = Config::default();
        assert_eq!(config.listen.to_string(), DEFAULT_LISTEN);
        assert_eq!(config.data_dir, PathBuf::from(DEFAULT_DATA_DIR));
        assert_eq!(config.engine, EngineKind::Fake);

        let parsed: Config = toml::from_str(
            "listen = \"127.0.0.1:9000\"\ndata_dir = \"/tmp/q\"\nengine = \"cloud-hypervisor\"\nnode_name = \"n1\"\n",
        )
        .unwrap();
        assert_eq!(parsed.listen.to_string(), "127.0.0.1:9000");
        assert_eq!(parsed.data_dir, PathBuf::from("/tmp/q"));
        assert_eq!(parsed.engine, EngineKind::CloudHypervisor);
        assert_eq!(parsed.node_name, "n1");
    }

    #[test]
    fn unknown_keys_are_refused() {
        assert!(toml::from_str::<Config>("liste = \"127.0.0.1:9000\"\n").is_err());
        assert!(toml::from_str::<Config>("engine = \"firecracker\"\n").is_err());
    }

    #[test]
    fn missing_file_is_default() {
        let path = std::env::temp_dir().join("quoro-config-that-does-not-exist.toml");
        let config = Config::load(&path).unwrap();
        assert_eq!(config.listen.to_string(), DEFAULT_LISTEN);
    }
}
