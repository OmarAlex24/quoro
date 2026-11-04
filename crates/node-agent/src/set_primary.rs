// SPDX-License-Identifier: AGPL-3.0-only
//! `quoro node set-primary <url>` (ADR 0139, layer 1).
//!
//! A node dials the primary URL stored at enrollment. When the primary gets a
//! new address, this is how an operator points an enrolled node at it without
//! re-enrolling. The pin is the trust: the node completes a TLS handshake with
//! the certificate fingerprint it already holds, and only then rewrites
//! `primary_url`. The fingerprint, the cluster key and the credential never
//! change, and a failed handshake changes nothing at all.
//!
//! Verification and the write are separate steps so the CLI can stop the
//! running agent in between: the agent keeps the identity in memory and writes
//! it back on a key rotation or when it learns new addresses, which would
//! overwrite a `primary_url` written underneath it.

use std::path::Path;

use crate::identity::{self, Identity};
use crate::{AgentError, tls};

/// What [`set_primary`] did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetPrimary {
    /// The URL `identity.json` held before.
    pub previous: String,
    /// The URL it holds now (normalized).
    pub primary_url: String,
    /// `false` when the node already dialed that address.
    pub changed: bool,
}

/// Verifies `url` and points the node at it in one call, without stopping a
/// running agent. Prefer [`verify_primary`] and [`write_primary`] in the CLI,
/// which stops the agent between them.
///
/// # Errors
///
/// As [`verify_primary`] and [`write_primary`].
pub async fn set_primary(data_dir: &Path, url: &str) -> Result<SetPrimary, AgentError> {
    let (identity, primary_url) = verify_primary(data_dir, url).await?;
    let previous = identity.primary_url;
    if previous == primary_url {
        return Ok(SetPrimary {
            previous,
            primary_url,
            changed: false,
        });
    }
    let updated = write_primary(data_dir, &primary_url)?;
    Ok(SetPrimary {
        previous,
        primary_url: updated.primary_url,
        changed: true,
    })
}

/// Normalizes `url` and proves that the host at that address holds the pinned
/// primary certificate, by completing a TLS handshake with the pin the node
/// stored at enrollment.
///
/// Returns the identity it read (whose `primary_url` is the old address) and
/// the normalized new URL. Nothing is written.
///
/// # Errors
///
/// [`AgentError::Tls`] when the URL is not `https://`, when the handshake
/// times out or fails, or when the certificate is not the pinned one; the
/// message says that nothing was changed. [`AgentError::Identity`] when the
/// node is not enrolled or `identity.json` is malformed.
pub async fn verify_primary(data_dir: &Path, url: &str) -> Result<(Identity, String), AgentError> {
    let primary_url = normalize(url)?;
    let identity = identity::load_identity(data_dir)?;
    // The handshake alone is the proof; no HTTP request is sent.
    let stream = tls::connect(&primary_url, &identity.fingerprint)
        .await
        .map_err(|error| refusal(&primary_url, error))?;
    drop(stream);
    Ok((identity, primary_url))
}

/// Rewrites only `primary_url` in `identity.json`, through the atomic
/// `0600` write path `signing_key_rotate` uses.
///
/// The file is loaded again here rather than reusing the copy
/// [`verify_primary`] returned, so a concurrent change (another address in the
/// list, for example) is kept. Everything except `primary_url` is preserved.
/// If the address is already the stored one, nothing is written.
///
/// # Errors
///
/// [`AgentError::Identity`] when the node is not enrolled or the file is
/// malformed, [`AgentError::Io`] when it cannot be written.
pub fn write_primary(data_dir: &Path, primary_url: &str) -> Result<Identity, AgentError> {
    let mut identity = identity::load_identity(data_dir)?;
    if identity.primary_url == primary_url {
        return Ok(identity);
    }
    identity.primary_url = primary_url.to_owned();
    identity::save_identity(data_dir, &identity)?;
    Ok(identity)
}

/// Accepts only `https://` and returns `https://<authority>` with no path,
/// query, fragment or trailing slash.
///
/// The host is kept as written, an IPv6 literal keeps its brackets and an
/// explicit port is kept. `wss://` is refused even though the session uses it:
/// it is the same listener, but `primary_url` is an HTTPS base URL.
///
/// # Errors
///
/// [`AgentError::Tls`] when the scheme is wrong, the host is empty or the port
/// is not a number.
fn normalize(url: &str) -> Result<String, AgentError> {
    let rest = url.strip_prefix("https://").ok_or_else(|| {
        AgentError::Tls(format!(
            "the primary URL {url:?} must start with https://; only https:// addresses are accepted (not wss:// or http://)"
        ))
    })?;
    // `Endpoint::parse` validates the authority and the port.
    let endpoint = tls::Endpoint::parse(url)?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    let explicit_port = has_explicit_port(authority);
    let host = if endpoint.host.contains(':') {
        format!("[{}]", endpoint.host)
    } else {
        endpoint.host.clone()
    };
    let mut normalized = format!("https://{host}");
    if explicit_port || endpoint.port != 443 {
        normalized.push(':');
        normalized.push_str(&endpoint.port.to_string());
    }
    Ok(normalized)
}

/// Whether the URL's authority spells out a port, including after an IPv6
/// literal's closing bracket.
fn has_explicit_port(authority: &str) -> bool {
    if let Some(rest) = authority.strip_prefix('[') {
        rest.find(']')
            .is_some_and(|end| rest[end + 1..].starts_with(':'))
    } else {
        authority.contains(':')
    }
}

/// Turns a failed handshake into the operator's message, always saying that
/// the identity was left alone.
fn refusal(url: &str, error: AgentError) -> AgentError {
    let reason = match &error {
        AgentError::Tls(reason) => reason.clone(),
        other => other.to_string(),
    };
    if reason.contains("not the pinned") || reason.contains("fingerprint is") {
        AgentError::Tls(format!(
            "the certificate at {url} is not the pinned primary certificate; nothing was changed"
        ))
    } else {
        AgentError::Tls(format!(
            "could not connect to {url}: {reason}; nothing was changed"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::normalize;

    #[test]
    fn only_https_is_accepted_and_the_path_is_dropped() {
        assert_eq!(
            normalize("https://quoro.example.com:8443/api/v1?x#y").unwrap(),
            "https://quoro.example.com:8443"
        );
        assert_eq!(
            normalize("https://quoro.example.com").unwrap(),
            "https://quoro.example.com"
        );
        assert_eq!(
            normalize("https://[2001:db8::1]:9443/").unwrap(),
            "https://[2001:db8::1]:9443"
        );
        assert_eq!(
            normalize("https://192.0.2.10/").unwrap(),
            "https://192.0.2.10"
        );
        assert!(normalize("wss://quoro.example.com").is_err());
        assert!(normalize("http://quoro.example.com").is_err());
        assert!(normalize("quoro.example.com").is_err());
        assert!(normalize("https://").is_err());
    }
}
