//! The TLS material of the single HTTPS listener (D-0054).
//!
//! The installer generates a self-signed certificate; a `quoro join` command
//! will pin its SHA-256 fingerprint, so the fingerprint printed at start is
//! part of the contract. The certificate and key live beside the database in
//! `<data_dir>/tls/` with the key at `0600` and the directory at `0700`, so a
//! copied data directory never leaks the key.
//!
//! The provider is `ring` throughout: `rustls` and `rcgen` are built without
//! `aws-lc-rs` because its license tree is outside the D-0080 allowlist.

use quoro_control_plane::store::sha256_hex;
use rcgen::{CertificateParams, DistinguishedName, DnType, IsCa, KeyPair};
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

/// The listener's rustls configuration and the certificate's fingerprint.
pub struct Material {
    /// Configuration for `axum-server`'s TLS acceptor.
    pub config: Arc<ServerConfig>,
    /// `sha256:<lowercase hex>` of the certificate DER, as `quoro join` pins it.
    pub fingerprint: String,
}

/// Loads `<data_dir>/tls/cert.pem` and `key.pem`, generating both on first
/// start. `listen` contributes a SAN only when it names a specific address.
///
/// # Errors
///
/// An I/O error creating the directory or reading/writing the files, or a
/// certificate that cannot be parsed or used.
pub fn load_or_generate(
    data_dir: &Path,
    node_name: &str,
    listen: SocketAddr,
) -> io::Result<Material> {
    let dir = data_dir.join("tls");
    create_private_dir(&dir)?;
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");

    let (cert_der, key_der) = match (std::fs::read(&cert_path), std::fs::read(&key_path)) {
        (Ok(cert_pem), Ok(key_pem)) => (
            decode_pem(&cert_pem, "CERTIFICATE")?,
            decode_pem(&key_pem, "PRIVATE KEY")?,
        ),
        _ => generate(&cert_path, &key_path, node_name, listen)?,
    };

    Ok(Material {
        config: server_config(cert_der.clone(), key_der)?,
        fingerprint: format!("sha256:{}", sha256_hex(&cert_der)),
    })
}

/// Writes a new self-signed certificate and key and returns their DER.
fn generate(
    cert_path: &Path,
    key_path: &Path,
    node_name: &str,
    listen: SocketAddr,
) -> io::Result<(Vec<u8>, Vec<u8>)> {
    // The node name and the loopback names are always valid; the listen
    // address joins them unless the listener accepts on every interface.
    let mut names = vec![
        node_name.to_string(),
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ];
    if !listen.ip().is_unspecified() {
        names.push(listen.ip().to_string());
    }

    let mut params = CertificateParams::new(names).map_err(io::Error::other)?;
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, node_name);
    params.distinguished_name = dn;
    params.is_ca = IsCa::NoCa;
    // rcgen's default validity window (1975-4096) is comfortably longer than
    // the ten years the certificate needs, and the certificate is pinned by
    // fingerprint rather than trusted through a CA, so no clock-skew window is
    // required at install time.

    let key_pair = KeyPair::generate().map_err(io::Error::other)?;
    let certificate = params.self_signed(&key_pair).map_err(io::Error::other)?;

    let cert_der = certificate.der().to_vec();
    let key_der = key_pair.serialize_der();
    write_private(key_path, key_pair.serialize_pem().as_bytes())?;
    write_file(cert_path, certificate.pem().as_bytes())?;
    Ok((cert_der, key_der))
}

/// Builds the rustls server configuration: ring provider, TLS 1.2+1.3, HTTP/1.1
/// and HTTP/2 over ALPN (axum-server's hyper always carries HTTP/2).
fn server_config(cert_der: Vec<u8>, key_der: Vec<u8>) -> io::Result<Arc<ServerConfig>> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(io::Error::other)?
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(cert_der)],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_der)),
        )
        .map_err(io::Error::other)?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

fn create_private_dir(path: &Path) -> io::Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn write_file(path: &Path, contents: &[u8]) -> io::Result<()> {
    std::fs::write(path, contents)
}

/// Writes a `0600` file: the key must not be readable by anyone else.
fn write_private(path: &Path, contents: &[u8]) -> io::Result<()> {
    use std::io::Write as _;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(contents)?;
    file.sync_all()
}

/// Extracts the DER body of the first PEM block labelled `label`.
///
/// The crate has no PEM dependency; PEM is a header, base64 and a footer, so
/// the few lines below are all that is needed to read what [`generate`] wrote.
fn decode_pem(bytes: &[u8], label: &str) -> io::Result<Vec<u8>> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "TLS PEM file is not UTF-8"))?;
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let start = text
        .find(&begin)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("missing {begin}")))?;
    let rest = &text[start + begin.len()..];
    let stop = rest
        .find(&end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("missing {end}")))?;
    base64_decode(&rest[..stop])
}

/// Standard-alphabet base64, ignoring whitespace and stopping at padding.
fn base64_decode(input: &str) -> io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let mut acc = 0u32;
    let mut bits = 0u8;
    for byte in input.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => break,
            b' ' | b'\t' | b'\r' | b'\n' => continue,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid base64 in a TLS PEM file",
                ));
            }
        };
        acc = (acc << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "quoro-tls-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn generates_once_then_reloads_the_same_certificate() {
        let dir = temp_dir();
        let listen: SocketAddr = "127.0.0.1:8443".parse().unwrap();
        let first = load_or_generate(&dir, "node-1", listen).unwrap();
        assert!(first.fingerprint.starts_with("sha256:"));
        assert_eq!(first.fingerprint.len(), "sha256:".len() + 64);
        assert!(dir.join("tls/cert.pem").exists());
        assert!(dir.join("tls/key.pem").exists());

        let second = load_or_generate(&dir, "node-1", listen).unwrap();
        assert_eq!(
            first.fingerprint, second.fingerprint,
            "reload, do not regenerate"
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(dir.join("tls/key.pem"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
            let mode = std::fs::metadata(dir.join("tls"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o700);
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn base64_round_trips_a_known_value() {
        assert_eq!(base64_decode("aGVsbG8=").unwrap(), b"hello");
        assert_eq!(base64_decode("aGVs\nbG8=").unwrap(), b"hello");
        assert!(base64_decode("a$b").is_err());
    }
}
