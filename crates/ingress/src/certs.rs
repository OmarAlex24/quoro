//! Node-local certificate storage and the SNI resolver of the public proxy.
//!
//! Layout: `<dir>/<hostname>/cert.pem` (the chain, leaf first) and
//! `<dir>/<hostname>/key.pem` (PKCS#8). The store directory and each host
//! directory are `0700`, both files `0600`, written to a temporary file and
//! renamed so a crash never leaves a torn pair behind the resolver. Nothing here
//! is replicated or reachable through the API (ADR 0024).

use rustls::crypto::ring::sign::any_supported_type;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use std::collections::HashMap;
use std::io::{self, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// A certificate the store holds for one hostname.
#[derive(Clone, Debug)]
pub struct StoredCert {
    /// `notAfter` of the leaf, Unix seconds.
    pub not_after: i64,
    /// `notBefore` of the leaf, Unix seconds.
    pub not_before: i64,
    /// The leaf in DER, for the ACME renewal-info id.
    pub leaf_der: CertificateDer<'static>,
}

struct Entry {
    info: StoredCert,
    key: Arc<CertifiedKey>,
}

/// Certificates on this node, on disk and in memory.
#[derive(Clone)]
pub struct CertStore {
    dir: PathBuf,
    entries: Arc<RwLock<HashMap<String, Entry>>>,
}

impl std::fmt::Debug for CertStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CertStore")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

impl CertStore {
    /// Opens `dir` (creating it `0700`) and loads every stored pair. A pair that
    /// fails to parse is skipped with a warning, so one bad file cannot stop
    /// the proxy; it is replaced by the next issuance.
    ///
    /// # Errors
    ///
    /// The directory cannot be created or read.
    pub fn open(dir: &Path) -> io::Result<Self> {
        create_private_dir(dir)?;
        let store = Self {
            dir: dir.to_path_buf(),
            entries: Arc::new(RwLock::new(HashMap::new())),
        };
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let Some(hostname) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if !crate::route::valid_hostname(&hostname) {
                continue;
            }
            let path = entry.path();
            match (
                std::fs::read(path.join("cert.pem")),
                std::fs::read(path.join("key.pem")),
            ) {
                (Ok(cert), Ok(key)) => match parse_pair(&cert, &key) {
                    Ok(parsed) => {
                        store.insert(&hostname, parsed);
                    }
                    Err(e) => {
                        tracing::warn!(%hostname, error = %e, "skipping unreadable certificate")
                    }
                },
                _ => tracing::warn!(%hostname, "skipping incomplete certificate directory"),
            }
        }
        Ok(store)
    }

    /// Validates and stores a chain and its key for `hostname`, then serves it.
    ///
    /// # Errors
    ///
    /// The PEM does not parse, the key does not match a supported type, or a
    /// write fails. On error the previous certificate stays in force.
    pub fn store(&self, hostname: &str, chain_pem: &str, key_pem: &str) -> io::Result<StoredCert> {
        if !crate::route::valid_hostname(hostname) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid hostname",
            ));
        }
        let parsed = parse_pair(chain_pem.as_bytes(), key_pem.as_bytes())?;
        let host_dir = self.dir.join(hostname);
        create_private_dir(&host_dir)?;
        write_private(&host_dir.join("key.pem"), key_pem.as_bytes())?;
        write_private(&host_dir.join("cert.pem"), chain_pem.as_bytes())?;
        Ok(self.insert(hostname, parsed))
    }

    fn insert(&self, hostname: &str, (info, key): (StoredCert, Arc<CertifiedKey>)) -> StoredCert {
        let mut entries = self
            .entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.insert(
            hostname.to_string(),
            Entry {
                info: info.clone(),
                key,
            },
        );
        info
    }

    /// The certificate held for `hostname`.
    #[must_use]
    pub fn get(&self, hostname: &str) -> Option<StoredCert> {
        let entries = self
            .entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.get(hostname).map(|e| e.info.clone())
    }

    /// The SNI resolver for the TLS listener. It sees certificates stored
    /// after it was created.
    #[must_use]
    pub fn resolver(&self) -> Arc<dyn ResolvesServerCert> {
        Arc::new(Resolver {
            entries: Arc::clone(&self.entries),
        })
    }
}

#[derive(Debug)]
struct Resolver {
    entries: Arc<RwLock<HashMap<String, Entry>>>,
}

impl std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("info", &self.info)
            .finish_non_exhaustive()
    }
}

impl ResolvesServerCert for Resolver {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let name = hello.server_name()?.to_ascii_lowercase();
        let entries = self
            .entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.get(&name).map(|e| Arc::clone(&e.key))
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn parse_pair(chain_pem: &[u8], key_pem: &[u8]) -> io::Result<(StoredCert, Arc<CertifiedKey>)> {
    let chain = CertificateDer::pem_slice_iter(chain_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| invalid(format!("certificate PEM: {e}")))?;
    let leaf = chain
        .first()
        .ok_or_else(|| invalid("no certificate in PEM"))?
        .clone();
    let key_der = PrivateKeyDer::from_pem_slice(key_pem)
        .map_err(|_| invalid("private key PEM does not parse"))?;
    let signing_key =
        any_supported_type(&key_der).map_err(|_| invalid("unsupported private key type"))?;
    let certified = CertifiedKey::new(chain, signing_key);
    certified
        .keys_match()
        .map_err(|_| invalid("private key does not match the certificate"))?;
    let (_, cert) = x509_parser::parse_x509_certificate(&leaf)
        .map_err(|e| invalid(format!("certificate: {e}")))?;
    let validity = cert.validity();
    Ok((
        StoredCert {
            not_after: validity.not_after.timestamp(),
            not_before: validity.not_before.timestamp(),
            leaf_der: leaf,
        },
        Arc::new(certified),
    ))
}

fn create_private_dir(path: &Path) -> io::Result<()> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)?;
    // `recursive` leaves an existing directory's mode alone; enforce it.
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
}

fn write_private(path: &Path, contents: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    let _ = std::fs::remove_file(&tmp);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)?;
    file.write_all(contents)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-tmp")
            .join(format!("{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn self_signed(host: &str) -> (String, String) {
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec![host.to_string()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        (cert.pem(), key.serialize_pem())
    }

    #[test]
    fn stores_with_private_modes_and_reloads() {
        let dir = test_dir("certs-store");
        let store = CertStore::open(&dir).unwrap();
        let (cert, key) = self_signed("app.example.com");
        let info = store.store("app.example.com", &cert, &key).unwrap();
        assert!(info.not_after > info.not_before);
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&dir), 0o700);
        assert_eq!(mode(&dir.join("app.example.com")), 0o700);
        assert_eq!(mode(&dir.join("app.example.com/key.pem")), 0o600);
        assert_eq!(mode(&dir.join("app.example.com/cert.pem")), 0o600);

        let reopened = CertStore::open(&dir).unwrap();
        assert_eq!(
            reopened.get("app.example.com").unwrap().not_after,
            info.not_after
        );
        assert!(reopened.get("other.example.com").is_none());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn refuses_a_mismatched_key_and_keeps_the_old_pair() {
        let dir = test_dir("certs-mismatch");
        let store = CertStore::open(&dir).unwrap();
        let (cert, key) = self_signed("app.example.com");
        let first = store.store("app.example.com", &cert, &key).unwrap();
        let (other_cert, _) = self_signed("app.example.com");
        assert!(store.store("app.example.com", &other_cert, &key).is_err());
        assert_eq!(
            store.get("app.example.com").unwrap().leaf_der,
            first.leaf_der
        );
        assert!(store.store("../escape", &cert, &key).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
