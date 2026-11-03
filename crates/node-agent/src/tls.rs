// SPDX-License-Identifier: AGPL-3.0-only
//! Pinned TLS to the primary (`node-agent-protocol.md` sections 2.2 and 3.1).
//!
//! The join command carries the SHA-256 of the primary's certificate, and that
//! pin *is* the trust: the primary's certificate is usually self-signed, so
//! names and validity dates are not checked, only the digest and the handshake
//! signatures made with the certificate's key. No root store is loaded.
//!
//! The `ClientConfig` is built from the ring provider explicitly
//! (`rustls::crypto::ring::default_provider()`), never from a process default,
//! so the algorithms do not depend on another crate having installed one.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use sha2::{Digest as _, Sha256};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::AgentError;

/// Default port for `https://` and `wss://`.
const DEFAULT_PORT: u16 = 443;

/// A parsed `https://host[:port]` or `wss://host[:port]` endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    /// Host name or IP address, without brackets.
    pub host: String,
    /// TCP port.
    pub port: u16,
}

impl Endpoint {
    /// Parses a base or full URL, ignoring any path, query or fragment.
    ///
    /// # Errors
    ///
    /// [`AgentError::Tls`] when the scheme is not `https`/`wss`, when there is
    /// no host, or when the port is not a number.
    pub fn parse(url: &str) -> Result<Self, AgentError> {
        let rest = url
            .strip_prefix("https://")
            .or_else(|| url.strip_prefix("wss://"))
            .ok_or_else(|| {
                AgentError::Tls(format!(
                    "the URL {url:?} must start with https:// or wss://"
                ))
            })?;
        let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
        if authority.is_empty() {
            return Err(AgentError::Tls(format!("the URL {url:?} has no host")));
        }
        if let Some(rest) = authority.strip_prefix('[') {
            // IPv6 literal: `[::1]` or `[::1]:443`.
            let (host, after) = rest
                .split_once(']')
                .ok_or_else(|| AgentError::Tls(format!("the URL {url:?} has a bad IPv6 host")))?;
            let port = match after.strip_prefix(':') {
                Some(text) => parse_port(text, url)?,
                None => DEFAULT_PORT,
            };
            return Ok(Self {
                host: host.to_owned(),
                port,
            });
        }
        match authority.rsplit_once(':') {
            Some((host, text)) => Ok(Self {
                host: host.to_owned(),
                port: parse_port(text, url)?,
            }),
            None => Ok(Self {
                host: authority.to_owned(),
                port: DEFAULT_PORT,
            }),
        }
    }

    /// The `SocketAddr` for an IP literal, or `None` for a name that must be
    /// resolved by the connector.
    fn literal_address(&self) -> Option<SocketAddr> {
        self.host.parse::<IpAddr>().ok().map(|ip| match ip {
            IpAddr::V4(ip) => SocketAddr::new(IpAddr::V4(ip), self.port),
            IpAddr::V6(ip) => SocketAddr::new(IpAddr::V6(ip), self.port),
        })
    }

    fn server_name(&self) -> Result<ServerName<'static>, AgentError> {
        if let Ok(ip) = self.host.parse::<IpAddr>() {
            return Ok(ServerName::IpAddress(ip.into()));
        }
        ServerName::try_from(self.host.clone()).map_err(|e| {
            AgentError::Tls(format!("{:?} is not a usable server name: {e}", self.host))
        })
    }
}

fn parse_port(text: &str, url: &str) -> Result<u16, AgentError> {
    text.parse::<u16>()
        .map_err(|_| AgentError::Tls(format!("the URL {url:?} has an invalid port {text:?}")))
}

/// Normalizes a certificate fingerprint to lowercase hex without the
/// `sha256:` prefix.
///
/// Accepts `sha256:<64 hex>`, bare hex and colon-separated hex, in any case.
///
/// # Errors
///
/// [`AgentError::Tls`] when the text is not 32 bytes of hex.
pub fn normalize_fingerprint(text: &str) -> Result<String, AgentError> {
    let text = text.trim();
    let text = match text.split_once(':') {
        Some((prefix, rest)) if prefix.eq_ignore_ascii_case("sha256") => rest,
        _ => text,
    };
    let cleaned: String = text
        .chars()
        .filter(|c| *c != ':' && *c != '-' && !c.is_ascii_whitespace())
        .collect();
    if cleaned.len() != 64 || !cleaned.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(AgentError::Tls(format!(
            "the certificate fingerprint {text:?} is not 32 bytes of hex"
        )));
    }
    Ok(cleaned.to_ascii_lowercase())
}

/// Parses a normalized fingerprint into its 32 bytes.
fn fingerprint_bytes(pin: &str) -> Result<[u8; 32], AgentError> {
    let normalized = normalize_fingerprint(pin)?;
    let mut bytes = [0u8; 32];
    let (pairs, _) = normalized.as_bytes().as_chunks::<2>();
    for (index, pair) in pairs.iter().enumerate() {
        let text = std::str::from_utf8(pair)
            .map_err(|_| AgentError::Tls("the certificate fingerprint is not hex".to_owned()))?;
        bytes[index] = u8::from_str_radix(text, 16)
            .map_err(|_| AgentError::Tls("the certificate fingerprint is not hex".to_owned()))?;
    }
    Ok(bytes)
}

/// The lowercase-hex SHA-256 of a DER certificate, without a prefix, which is
/// the form [`normalize_fingerprint`] produces.
#[must_use]
pub fn cert_fingerprint(der: &[u8]) -> String {
    crate::util::hex(&Sha256::digest(der))
}

/// A verifier that accepts exactly one certificate, by digest.
#[derive(Debug)]
struct PinnedCertVerifier {
    pin: [u8; 32],
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let digest = Sha256::digest(end_entity.as_ref());
        // Compare all 32 bytes; `!=` on arrays does that and does not stop at
        // the first difference.
        if digest.as_slice() == self.pin {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "the primary's certificate fingerprint is {}, not the pinned {}",
                crate::util::hex(&digest),
                crate::util::hex(&self.pin)
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

/// The client configuration for a pinned certificate.
///
/// # Errors
///
/// [`AgentError::Tls`] when the fingerprint is malformed or the provider has no
/// usable protocol versions.
pub fn client_config(pin: &str) -> Result<Arc<ClientConfig>, AgentError> {
    let pin = fingerprint_bytes(pin)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| AgentError::Tls(format!("no usable TLS protocol versions: {e}")))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedCertVerifier { pin, provider }))
        .with_no_client_auth();
    Ok(Arc::new(config))
}

/// Opens a TLS stream to `url`, accepting only the certificate whose digest is
/// `pin` (section 2.2).
///
/// # Errors
///
/// [`AgentError::Tls`] for a malformed URL or pin, or [`AgentError::Io`] when
/// the TCP or TLS connection fails.
pub async fn connect(url: &str, pin: &str) -> Result<TlsStream<TcpStream>, AgentError> {
    let endpoint = Endpoint::parse(url)?;
    let config = client_config(pin)?;
    let server_name = endpoint.server_name()?;

    let tcp = match endpoint.literal_address() {
        Some(address) => TcpStream::connect(address).await,
        None => TcpStream::connect((endpoint.host.as_str(), endpoint.port)).await,
    }
    .map_err(|e| AgentError::io("connect to the primary", e))?;

    TlsConnector::from(config)
        .connect(server_name, tcp)
        .await
        .map_err(|e| AgentError::Tls(format!("handshake with the primary failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprints_are_normalized_from_every_accepted_form() {
        let hex = "3f9a1c0e7b52d4a86e1f09c3b7d25e4a90c18f6b2d73e5a41c0b98f7e26d1a35";
        assert_eq!(
            normalize_fingerprint(&format!("sha256:{hex}")).unwrap(),
            hex
        );
        assert_eq!(normalize_fingerprint(hex).unwrap(), hex);
        assert_eq!(
            normalize_fingerprint(&hex.to_ascii_uppercase()).unwrap(),
            hex
        );
        let colon: String = hex
            .as_bytes()
            .chunks(2)
            .map(|pair| std::str::from_utf8(pair).unwrap().to_owned())
            .collect::<Vec<_>>()
            .join(":");
        assert_eq!(normalize_fingerprint(&colon).unwrap(), hex);
        assert_eq!(
            normalize_fingerprint(&format!("SHA256:{colon}")).unwrap(),
            hex
        );
    }

    #[test]
    fn bad_fingerprints_are_refused() {
        assert!(normalize_fingerprint("").is_err());
        assert!(normalize_fingerprint("abcd").is_err());
        assert!(normalize_fingerprint(&"z".repeat(64)).is_err());
        assert!(normalize_fingerprint(&"a".repeat(63)).is_err());
    }

    #[test]
    fn endpoints_default_to_443_and_ignore_the_path() {
        assert_eq!(
            Endpoint::parse("https://quoro.example.com").unwrap(),
            Endpoint {
                host: "quoro.example.com".to_owned(),
                port: 443
            }
        );
        assert_eq!(
            Endpoint::parse("wss://10.0.0.1:8443/api/v1/nodes/session").unwrap(),
            Endpoint {
                host: "10.0.0.1".to_owned(),
                port: 8443
            }
        );
        assert_eq!(
            Endpoint::parse("https://[::1]:9443/").unwrap(),
            Endpoint {
                host: "::1".to_owned(),
                port: 9443
            }
        );
        assert!(Endpoint::parse("http://quoro.example.com").is_err());
        assert!(Endpoint::parse("https://quoro.example.com:not-a-port").is_err());
    }
}
