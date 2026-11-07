//! TLS to the primary: a certificate pin, or the host's CA store.
//!
//! The primary's certificate is self-signed (ADR 0054), so a context
//! normally pins its SHA-256 fingerprint, the `sha256:<hex>` value a join
//! command carries, and that pin is the whole trust: names and validity dates
//! are not checked, only the digest of the leaf and the handshake signatures
//! made with its key. This mirrors the node agent's verifier
//! (`quoro_node_agent::tls`), which the client does not link. A primary behind
//! a publicly trusted certificate can use `tls = "system"` instead.
//!
//! A first `quoro login` without a pin uses a third configuration,
//! [`capturing`], that records the presented leaf's digest and then fails the
//! handshake, so nothing is sent on that connection; the user confirms the
//! digest and it becomes the context's pin.
//!
//! Every configuration is built on the `ring` provider explicitly, never on
//! a process default, so the algorithms do not depend on what another crate
//! installed (ADR 0080).

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use sha2::{Digest as _, Sha256};
use std::sync::{Arc, Mutex};

/// Reads a certificate fingerprint: `sha256:<64 hex>`, bare hex or
/// colon-separated hex, in any case, as `quoro join` accepts it.
pub fn parse_fingerprint(text: &str) -> Result<[u8; 32], String> {
    let trimmed = text.trim();
    let hex = match trimmed.split_once(':') {
        Some((prefix, rest)) if prefix.eq_ignore_ascii_case("sha256") => rest,
        _ => trimmed,
    };
    let digits: Vec<u8> = hex
        .bytes()
        .filter(|b| *b != b':' && *b != b'-' && !b.is_ascii_whitespace())
        .collect();
    let invalid = || format!("the certificate fingerprint {trimmed:?} is not 32 bytes of hex");
    if digits.len() != 64 {
        return Err(invalid());
    }
    let mut bytes = [0u8; 32];
    let (pairs, _) = digits.as_chunks::<2>();
    for (byte, pair) in bytes.iter_mut().zip(pairs) {
        let pair = std::str::from_utf8(pair).map_err(|_| invalid())?;
        *byte = u8::from_str_radix(pair, 16).map_err(|_| invalid())?;
    }
    Ok(bytes)
}

/// `sha256:<lowercase hex>`, the form the primary prints.
pub fn show(digest: &[u8]) -> String {
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("sha256:{hex}")
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// A configuration that accepts exactly the certificate whose SHA-256 is
/// `pin`.
pub fn pinned(pin: [u8; 32]) -> Result<ClientConfig, String> {
    let provider = provider();
    Ok(ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("no usable TLS protocol versions: {e}"))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedVerifier { pin, provider }))
        .with_no_client_auth())
}

/// A configuration that verifies the primary against the host's CA store.
pub fn system() -> Result<ClientConfig, String> {
    let found = rustls_native_certs::load_native_certs();
    let mut roots = RootCertStore::empty();
    let (added, _) = roots.add_parsable_certificates(found.certs);
    if added == 0 {
        return Err("no CA certificate could be loaded from the host's store".to_string());
    }
    Ok(ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("no usable TLS protocol versions: {e}"))?
        .with_root_certificates(roots)
        .with_no_client_auth())
}

/// The digest the [`capturing`] configuration saw.
pub type Captured = Arc<Mutex<Option<[u8; 32]>>>;

/// A configuration that records the SHA-256 of the presented leaf in
/// `captured` and refuses every certificate, so the handshake never completes
/// and no request is sent.
pub fn capturing(captured: Captured) -> Result<ClientConfig, String> {
    let provider = provider();
    Ok(ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("no usable TLS protocol versions: {e}"))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(CapturingVerifier { captured, provider }))
        .with_no_client_auth())
}

/// Records the leaf's digest, accepts nothing.
#[derive(Debug)]
struct CapturingVerifier {
    captured: Captured,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for CapturingVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let digest: [u8; 32] = Sha256::digest(end_entity.as_ref()).into();
        if let Ok(mut captured) = self.captured.lock() {
            *captured = Some(digest);
        }
        Err(rustls::Error::General(
            "first contact: the certificate is recorded, not trusted".to_string(),
        ))
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::General("first contact".to_string()))
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::General("first contact".to_string()))
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Accepts one certificate, by digest.
#[derive(Debug)]
struct PinnedVerifier {
    pin: [u8; 32],
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let digest = Sha256::digest(end_entity.as_ref());
        if digest.as_slice() == self.pin {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "the primary's certificate fingerprint is {}, not the pinned {}",
                show(&digest),
                show(&self.pin)
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEX: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";

    #[test]
    fn every_accepted_form_reads_the_same_bytes() {
        let expected = parse_fingerprint(HEX).unwrap();
        assert_eq!(show(&expected), format!("sha256:{HEX}"));
        let colon = HEX
            .as_bytes()
            .chunks(2)
            .map(|pair| std::str::from_utf8(pair).unwrap())
            .collect::<Vec<_>>()
            .join(":");
        for form in [
            format!("sha256:{HEX}"),
            format!("SHA256:{}", HEX.to_ascii_uppercase()),
            colon,
            format!("  {HEX}\n"),
        ] {
            assert_eq!(parse_fingerprint(&form).unwrap(), expected, "{form}");
        }
    }

    #[test]
    fn malformed_fingerprints_are_refused() {
        for bad in ["", "abcd", &"z".repeat(64), &"a".repeat(63), "sha512:00"] {
            assert!(parse_fingerprint(bad).is_err(), "{bad}");
        }
    }
}
