// SPDX-License-Identifier: AGPL-3.0-only
//! The one-time enrollment POST (`node-agent-protocol.md` section 2.3).
//!
//! The request goes over the same pinned TLS stream the session uses, before
//! any credential exists: the join token is the proof. Only the node's public
//! key travels; the private key never leaves the machine.
//!
//! Neither the join token nor the returned credential is ever logged.

use http_body_util::{BodyExt as _, Full};
use hyper::body::Bytes;
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};

use crate::{AgentConfig, AgentError, JoinTarget, NodeFacts, tls};

/// Path of the enrollment endpoint.
pub const ENROLL_PATH: &str = "/api/v1/nodes/enroll";

/// The enroll body (section 2.3).
#[derive(Serialize)]
struct EnrollRequest<'a> {
    join_token: &'a str,
    name: &'a str,
    public_key: &'a str,
    roles: &'a [String],
    capacity: quoro_proto::wire::Capacity,
    kvm: quoro_proto::wire::Kvm,
}

/// The enroll response (section 2.4).
#[derive(Deserialize)]
struct EnrollResponse {
    node_id: String,
    cluster_id: String,
    primary_url: String,
    credential: String,
    cluster_public_key: String,
}

/// What the primary returned, before the identity files are written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Enrollment {
    /// `nod_` id of this node.
    pub node_id: String,
    /// `cls_` id of the cluster.
    pub cluster_id: String,
    /// Base URL for this and later sessions.
    pub primary_url: String,
    /// Node session credential, returned once.
    pub credential: String,
    /// Base64 Ed25519 key that signs desired state.
    pub cluster_public_key: String,
}

/// Posts the node's public key and details to the primary and parses the 201.
///
/// # Errors
///
/// [`AgentError::Tls`] when the pinned connection fails, [`AgentError::Enroll`]
/// for any status other than 201, [`AgentError::Protocol`] when a 201 body is
/// malformed, or [`AgentError::Identity`] when the cluster key is not a valid
/// Ed25519 public key.
pub async fn enroll(
    config: &AgentConfig,
    target: &JoinTarget,
    facts: &NodeFacts,
    public_key: &str,
) -> Result<Enrollment, AgentError> {
    let url = target.url.trim_end_matches('/');
    let stream = tls::connect(url, &target.fingerprint).await?;
    let endpoint = tls::Endpoint::parse(url)?;

    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|e| AgentError::Protocol(format!("HTTP handshake failed: {e}")))?;
    // The connection future must be polled for the request to make progress.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::debug!(error = %e, "enrollment connection ended");
        }
    });

    let body = EnrollRequest {
        join_token: &target.token,
        name: &config.node_name,
        public_key,
        roles: &config.roles,
        capacity: facts.capacity,
        kvm: facts.kvm.clone(),
    };
    let body = serde_json::to_vec(&body)
        .map_err(|e| AgentError::Protocol(format!("could not encode the enroll request: {e}")))?;

    let request = Request::builder()
        .method(Method::POST)
        .uri(ENROLL_PATH)
        .header("host", host_header(&endpoint))
        .header("content-type", "application/json")
        .header("accept", "application/json")
        .body(Full::new(Bytes::from(body)))
        .map_err(|e| AgentError::Protocol(format!("could not build the enroll request: {e}")))?;

    let response = sender
        .send_request(request)
        .await
        .map_err(|e| AgentError::Protocol(format!("enroll request failed: {e}")))?;
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .map_err(|e| AgentError::Protocol(format!("could not read the enroll response: {e}")))?
        .to_bytes();

    if status.as_u16() != 201 {
        return Err(AgentError::Enroll {
            status: status.as_u16(),
            detail: problem_detail(&bytes),
        });
    }

    let parsed: EnrollResponse = serde_json::from_slice(&bytes)
        .map_err(|e| AgentError::Protocol(format!("the enroll response body is malformed: {e}")))?;
    // The primary's signing key decides which desired state this node will
    // accept, so a key that is not a valid Ed25519 point fails enrollment
    // rather than surfacing later.
    quoro_proto::sign::public_key_from_base64(&parsed.cluster_public_key)
        .map_err(|e| AgentError::Identity(format!("the primary sent a bad cluster key: {e}")))?;

    Ok(Enrollment {
        node_id: parsed.node_id,
        cluster_id: parsed.cluster_id,
        primary_url: parsed.primary_url,
        credential: parsed.credential,
        cluster_public_key: parsed.cluster_public_key,
    })
}

/// The `Host` header: the authority of the primary URL.
fn host_header(endpoint: &tls::Endpoint) -> String {
    if endpoint.port == 443 {
        authority(&endpoint.host)
    } else {
        format!("{}:{}", authority(&endpoint.host), endpoint.port)
    }
}

fn authority(host: &str) -> String {
    if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_owned()
    }
}

/// The `detail` or `title` of an `application/problem+json` body, falling back
/// to a short excerpt of whatever arrived.
fn problem_detail(bytes: &[u8]) -> String {
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) {
        for key in ["detail", "title"] {
            if let Some(text) = value.get(key).and_then(serde_json::Value::as_str) {
                return text.to_owned();
            }
        }
    }
    let text = String::from_utf8_lossy(bytes);
    text.chars().take(200).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn problem_details_prefer_detail_then_title() {
        assert_eq!(
            problem_detail(br#"{"title":"Conflict","detail":"the token was used"}"#),
            "the token was used"
        );
        assert_eq!(problem_detail(br#"{"title":"Conflict"}"#), "Conflict");
        assert_eq!(problem_detail(b"not json"), "not json");
    }

    #[test]
    fn host_headers_carry_a_non_default_port() {
        let endpoint = tls::Endpoint::parse("https://quoro.example.com").unwrap();
        assert_eq!(host_header(&endpoint), "quoro.example.com");
        let endpoint = tls::Endpoint::parse("https://10.0.0.1:8443").unwrap();
        assert_eq!(host_header(&endpoint), "10.0.0.1:8443");
        let endpoint = tls::Endpoint::parse("https://[::1]:8443").unwrap();
        assert_eq!(host_header(&endpoint), "[::1]:8443");
    }
}
