//! Cloudflare API client: DNS records for ACME and tunnel hostnames, and the
//! tunnel API itself (ADR 0024, 0055).
//!
//! Every DNS record this module creates carries [`RECORD_TAG`] in its comment.
//! The delete and CNAME methods refuse to touch a record without it, so a
//! record an operator made by hand is never overwritten.
//!
//! The token travels in an `Authorization: Bearer` header and is never logged:
//! debug logs carry the method and the path only.

use std::fmt;
use std::time::Duration;

use reqwest::{Client, Method};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::time::sleep;

use crate::{DnsProvider, ProviderError, RecordId, Token};

pub const API_BASE: &str = "https://api.cloudflare.com/client/v4";
pub const DOH_BASE: &str = "https://cloudflare-dns.com/dns-query";
/// Comment stored on every DNS record Quoro creates; ownership marker.
pub const RECORD_TAG: &str = "managed by quoro";

/// One request may run three times: the first attempt plus two retries.
const MAX_ATTEMPTS: usize = 3;
/// Backoff between retries when the provider sends no `Retry-After`.
const RETRY_DELAYS: [Duration; 2] = [Duration::from_millis(500), Duration::from_secs(1)];
/// Upper bound for a provider-supplied `Retry-After`.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(10);

/// A Cloudflare API client. Cloning shares the connection pool.
#[derive(Clone)]
pub struct Cloudflare {
    http: Client,
    api_base: String,
    doh_base: String,
    token: Token,
}

impl fmt::Debug for Cloudflare {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cloudflare")
            .field("api_base", &self.api_base)
            .field("doh_base", &self.doh_base)
            .field("token", &self.token)
            .finish()
    }
}

impl Cloudflare {
    pub fn new(token: Token) -> Result<Self, ProviderError> {
        Self::with_endpoints(token, API_BASE, DOH_BASE)
    }

    /// Builds a client against other endpoints; the tests use this to point at
    /// a local mock server.
    pub fn with_endpoints(
        token: Token,
        api_base: &str,
        doh_base: &str,
    ) -> Result<Self, ProviderError> {
        // reqwest is built without a default crypto provider; install ring.
        // An error means another caller already installed one.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            // The API never redirects; following one could carry the token
            // somewhere unexpected.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|err| ProviderError::Transport(err.to_string()))?;
        Ok(Self {
            http,
            api_base: api_base.trim_end_matches('/').to_string(),
            doh_base: doh_base.to_string(),
            token,
        })
    }

    /// Checks that the token is active. Account-scoped tokens are rejected by
    /// the user endpoint, so with an account id those refusals (and an
    /// unsuccessful envelope) fall back to the account scoped endpoint.
    pub async fn verify_token(&self, account_id: Option<&str>) -> Result<(), ProviderError> {
        match self
            .api(Method::GET, "/user/tokens/verify", &[], None)
            .await
        {
            Ok(value) if token_is_active(&value) => Ok(()),
            Ok(value) => match account_id {
                Some(account_id) => self.verify_account_token(account_id).await,
                None => Err(inactive_token(&value)),
            },
            Err(ProviderError::Unauthorized(_)) => match account_id {
                Some(account_id) => self.verify_account_token(account_id).await,
                None => Err(ProviderError::Unauthorized(
                    "the user token endpoint refused the credential".to_string(),
                )),
            },
            Err(ProviderError::Api { status, message }) if status == 200 => match account_id {
                Some(account_id) => self.verify_account_token(account_id).await,
                None => Err(ProviderError::Api { status, message }),
            },
            Err(err) => Err(err),
        }
    }

    async fn verify_account_token(&self, account_id: &str) -> Result<(), ProviderError> {
        let value = self
            .api(
                Method::GET,
                &format!("/accounts/{account_id}/tokens/verify"),
                &[],
                None,
            )
            .await?;
        if token_is_active(&value) {
            Ok(())
        } else {
            Err(inactive_token(&value))
        }
    }

    /// DNS operations inside one zone.
    pub fn dns(&self, zone_id: &str) -> CloudflareDns {
        CloudflareDns {
            client: self.clone(),
            zone_id: zone_id.to_string(),
        }
    }

    /// Finds a tunnel by exact name, ignoring deleted ones.
    pub async fn find_tunnel(
        &self,
        account_id: &str,
        name: &str,
    ) -> Result<Option<Tunnel>, ProviderError> {
        let value = self
            .api(
                Method::GET,
                &format!("/accounts/{account_id}/cfd_tunnel"),
                &[("name", name), ("is_deleted", "false")],
                None,
            )
            .await?;
        let tunnels: Vec<Tunnel> = decode(result_of(&value)?)?;
        Ok(tunnels.into_iter().find(|tunnel| tunnel.name == name))
    }

    /// Creates a remotely managed tunnel (the config lives at Cloudflare).
    pub async fn create_tunnel(
        &self,
        account_id: &str,
        name: &str,
    ) -> Result<Tunnel, ProviderError> {
        let body = json!({"name": name, "config_src": "cloudflare"});
        let value = self
            .api(
                Method::POST,
                &format!("/accounts/{account_id}/cfd_tunnel"),
                &[],
                Some(&body),
            )
            .await?;
        decode(result_of(&value)?)
    }

    /// The connector token of a tunnel.
    pub async fn tunnel_token(
        &self,
        account_id: &str,
        tunnel_id: &str,
    ) -> Result<Token, ProviderError> {
        let value = self
            .api(
                Method::GET,
                &format!("/accounts/{account_id}/cfd_tunnel/{tunnel_id}/token"),
                &[],
                None,
            )
            .await?;
        let token = result_of(&value)?
            .as_str()
            .ok_or_else(|| {
                ProviderError::Transport("the tunnel token response is not a string".to_string())
            })?
            .to_string();
        Ok(Token::new(token))
    }

    /// Replaces the ingress rules. Rules are sent sorted by hostname and the
    /// `http_status:404` catch-all is always appended last.
    pub async fn put_tunnel_ingress(
        &self,
        account_id: &str,
        tunnel_id: &str,
        rules: &[IngressRule],
    ) -> Result<(), ProviderError> {
        let mut sorted: Vec<&IngressRule> = rules.iter().collect();
        sorted.sort_by(|a, b| a.hostname.cmp(&b.hostname));
        let mut ingress: Vec<Value> = sorted
            .into_iter()
            .map(|rule| json!({"hostname": rule.hostname, "service": rule.service}))
            .collect();
        ingress.push(json!({"service": "http_status:404"}));
        let body = json!({"config": {"ingress": ingress}});
        self.api(
            Method::PUT,
            &format!("/accounts/{account_id}/cfd_tunnel/{tunnel_id}/configurations"),
            &[],
            Some(&body),
        )
        .await?;
        Ok(())
    }

    /// Deletes a tunnel. Cloudflare refuses (error 1022) while the edge still
    /// lists connections, which it does for a while after `cloudflared` stops,
    /// so the stale connections are cleaned up first and the delete is retried
    /// a few times while the edge catches up.
    pub async fn delete_tunnel(
        &self,
        account_id: &str,
        tunnel_id: &str,
    ) -> Result<(), ProviderError> {
        let path = format!("/accounts/{account_id}/cfd_tunnel/{tunnel_id}");
        let mut attempt: u64 = 0;
        loop {
            match self
                .api(Method::DELETE, &format!("{path}/connections"), &[], None)
                .await
            {
                Ok(_) | Err(ProviderError::NotFound(_)) => {}
                Err(err) => return Err(err),
            }
            match self.api(Method::DELETE, &path, &[], None).await {
                Ok(_) => return Ok(()),
                Err(ProviderError::Api {
                    status: 400,
                    message,
                }) if message.starts_with("1022") && attempt < 5 => {
                    attempt += 1;
                    sleep(Duration::from_secs(5 * attempt)).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Performs one API call against `api_base`, retrying idempotent methods.
    async fn api(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<&Value>,
    ) -> Result<Value, ProviderError> {
        // Ids are interpolated into the path; a stray `/`, `?` or `..` from a
        // provider config or a response must not reach another endpoint.
        if !path.split('/').all(|segment| {
            segment
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        }) || path.contains("//")
        {
            return Err(ProviderError::Transport(format!(
                "refusing a request path with unexpected characters: {path}"
            )));
        }
        self.execute(
            method,
            format!("{}{path}", self.api_base),
            query,
            body,
            None,
            true,
        )
        .await
    }

    /// Performs one HTTP call and maps the Cloudflare envelope and status codes
    /// onto [`ProviderError`]. `GET`/`PUT`/`PATCH`/`DELETE` retry on 429, 5xx
    /// and transport errors; `POST` never retries.
    async fn execute(
        &self,
        method: Method,
        url: String,
        query: &[(&str, &str)],
        body: Option<&Value>,
        accept: Option<&str>,
        authenticated: bool,
    ) -> Result<Value, ProviderError> {
        let retryable = method != Method::POST;
        let mut attempt = 0;
        loop {
            attempt += 1;
            tracing::debug!(%method, path = %url, "cloudflare request");

            let mut request = self.http.request(method.clone(), url.clone());
            // The API token goes only to the API; the DoH resolver gets none.
            if authenticated {
                request = request.bearer_auth(self.token.expose());
            }
            if !query.is_empty() {
                request = request.query(query);
            }
            if let Some(accept) = accept {
                request = request.header(reqwest::header::ACCEPT, accept);
            }
            if let Some(body) = body {
                request = request.json(body);
            }

            let response = match request.send().await {
                Ok(response) => response,
                Err(err) => {
                    if retryable && attempt < MAX_ATTEMPTS {
                        sleep(default_delay(attempt)).await;
                        continue;
                    }
                    return Err(ProviderError::Transport(err.to_string()));
                }
            };

            let status = response.status();
            let retry_after = retry_after(&response);
            let text = response
                .text()
                .await
                .map_err(|err| ProviderError::Transport(err.to_string()))?;
            let value = serde_json::from_str::<Value>(&text).ok();

            if status.is_success() {
                let Some(value) = value else {
                    return Err(ProviderError::Transport(format!(
                        "could not decode the provider response as JSON (HTTP {})",
                        status.as_u16()
                    )));
                };
                if value.get("success").and_then(Value::as_bool) == Some(false) {
                    return Err(ProviderError::Api {
                        status: status.as_u16(),
                        message: error_detail(Some(&value)).unwrap_or_else(|| {
                            format!(
                                "the provider reported failure with HTTP {}",
                                status.as_u16()
                            )
                        }),
                    });
                }
                return Ok(value);
            }

            if retryable && is_retryable_status(status) && attempt < MAX_ATTEMPTS {
                sleep(retry_after.unwrap_or_else(|| default_delay(attempt))).await;
                continue;
            }

            return Err(status_error(status.as_u16(), value.as_ref()));
        }
    }
}

/// DNS records of one zone. See [`DnsProvider`] for the contract.
#[derive(Clone, Debug)]
pub struct CloudflareDns {
    client: Cloudflare,
    zone_id: String,
}

impl CloudflareDns {
    fn records_path(&self) -> String {
        format!("/zones/{}/dns_records", self.zone_id)
    }

    async fn records(&self, name: &str) -> Result<Vec<DnsRecord>, ProviderError> {
        let value = self
            .client
            .api(
                Method::GET,
                &self.records_path(),
                &[("name.exact", name)],
                None,
            )
            .await?;
        decode(result_of(&value)?)
    }

    async fn delete_by_id(&self, id: &str) -> Result<(), ProviderError> {
        let path = format!("{}/{}", self.records_path(), id);
        match self.client.api(Method::DELETE, &path, &[], None).await {
            Ok(_) => Ok(()),
            Err(ProviderError::NotFound(_)) => Ok(()),
            Err(err) => Err(err),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct DnsRecord {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    proxied: Option<bool>,
    #[serde(default)]
    comment: Option<String>,
}

impl DnsRecord {
    fn is_ours(&self) -> bool {
        self.comment.as_deref() == Some(RECORD_TAG)
    }
}

impl DnsProvider for CloudflareDns {
    async fn create_txt(&self, name: &str, value: &str) -> Result<RecordId, ProviderError> {
        // Cloudflare accepts TXT content without surrounding quotes.
        let body = json!({
            "type": "TXT",
            "name": name,
            "content": value,
            "ttl": 60,
            "comment": RECORD_TAG,
        });
        let response = self
            .client
            .api(Method::POST, &self.records_path(), &[], Some(&body))
            .await?;
        let record: DnsRecord = decode(result_of(&response)?)?;
        Ok(RecordId(record.id))
    }

    async fn delete_record(&self, id: &RecordId) -> Result<(), ProviderError> {
        self.delete_by_id(&id.0).await
    }

    async fn upsert_cname(
        &self,
        name: &str,
        target: &str,
        proxied: bool,
    ) -> Result<RecordId, ProviderError> {
        let records = self.records(name).await?;
        match records.as_slice() {
            [] => {
                let body = json!({
                    "type": "CNAME",
                    "name": name,
                    "content": target,
                    "proxied": proxied,
                    "ttl": 1,
                    "comment": RECORD_TAG,
                });
                let response = self
                    .client
                    .api(Method::POST, &self.records_path(), &[], Some(&body))
                    .await?;
                let record: DnsRecord = decode(result_of(&response)?)?;
                Ok(RecordId(record.id))
            }
            [record] if record.kind == "CNAME" && record.is_ours() => {
                let unchanged = record.content.as_deref() == Some(target)
                    && record.proxied.unwrap_or(false) == proxied;
                if unchanged {
                    return Ok(RecordId(record.id.clone()));
                }
                let body = json!({"content": target, "proxied": proxied});
                let path = format!("{}/{}", self.records_path(), record.id);
                let response = self
                    .client
                    .api(Method::PATCH, &path, &[], Some(&body))
                    .await?;
                let updated: DnsRecord = decode(result_of(&response)?)?;
                Ok(RecordId(updated.id))
            }
            other => {
                let kinds: Vec<&str> = other.iter().map(|record| record.kind.as_str()).collect();
                Err(ProviderError::Conflict(format!(
                    "a DNS record for {name} already exists ({}): not created by quoro, leaving it untouched",
                    kinds.join(", ")
                )))
            }
        }
    }

    async fn delete_cname(&self, name: &str, target: &str) -> Result<(), ProviderError> {
        for record in self.records(name).await? {
            if record.kind == "CNAME"
                && record.is_ours()
                && record.content.as_deref() == Some(target)
            {
                self.delete_by_id(&record.id).await?;
            }
        }
        Ok(())
    }

    async fn txt_visible(&self, name: &str, value: &str) -> Result<bool, ProviderError> {
        let response = self
            .client
            .execute(
                Method::GET,
                self.client.doh_base.clone(),
                &[("name", name), ("type", "TXT")],
                None,
                Some("application/dns-json"),
                false,
            )
            .await?;
        let answers = response.get("Answer").and_then(Value::as_array);
        Ok(answers.is_some_and(|answers| {
            answers.iter().any(|answer| {
                answer
                    .get("data")
                    .and_then(Value::as_str)
                    .is_some_and(|data| data == value || data.trim_matches('"') == value)
            })
        }))
    }
}

/// A Cloudflare Tunnel as far as Quoro cares about it.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct Tunnel {
    pub id: String,
    pub name: String,
}

/// One ingress rule. The catch-all is added by
/// [`Cloudflare::put_tunnel_ingress`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngressRule {
    pub hostname: String,
    pub service: String,
}

fn token_is_active(value: &Value) -> bool {
    value
        .get("result")
        .and_then(|result| result.get("status"))
        .and_then(Value::as_str)
        == Some("active")
}

fn inactive_token(value: &Value) -> ProviderError {
    let status = value
        .get("result")
        .and_then(|result| result.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    ProviderError::Unauthorized(format!("the token is not active (status: {status})"))
}

fn result_of(value: &Value) -> Result<Value, ProviderError> {
    value
        .get("result")
        .cloned()
        .ok_or_else(|| ProviderError::Transport("the provider response has no result".to_string()))
}

fn decode<T: DeserializeOwned>(value: Value) -> Result<T, ProviderError> {
    serde_json::from_value(value)
        .map_err(|err| ProviderError::Transport(format!("unexpected provider response: {err}")))
}

fn error_detail(value: Option<&Value>) -> Option<String> {
    let errors = value
        .and_then(|value| value.get("errors"))
        .and_then(Value::as_array)?;
    let mut parts = Vec::new();
    for error in errors {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match error.get("code").and_then(Value::as_i64) {
            Some(code) => parts.push(format!("{code}: {message}")),
            None if !message.is_empty() => parts.push(message.to_string()),
            None => {}
        }
    }
    (!parts.is_empty()).then(|| parts.join("; "))
}

fn status_error(status: u16, value: Option<&Value>) -> ProviderError {
    let message = error_detail(value).unwrap_or_else(|| format!("HTTP {status}"));
    match status {
        401 | 403 => ProviderError::Unauthorized(message),
        404 => ProviderError::NotFound(message),
        409 => ProviderError::Conflict(message),
        _ => ProviderError::Api { status, message },
    }
}

fn is_retryable_status(status: reqwest::StatusCode) -> bool {
    status.as_u16() == 429 || status.is_server_error()
}

fn default_delay(attempt: usize) -> Duration {
    RETRY_DELAYS[(attempt - 1).min(RETRY_DELAYS.len() - 1)]
}

fn retry_after(response: &reqwest::Response) -> Option<Duration> {
    response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|seconds| Duration::from_secs(seconds).min(MAX_RETRY_AFTER))
}
