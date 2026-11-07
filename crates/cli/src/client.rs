//! One session with the primary: the HTTP client for a context and the
//! credential every request carries.
//!
//! A human's token is sent as it is. An agent's key mints an access token
//! (`agent.rs`) and a call answered 401 with a cached token is sent once more
//! with a freshly minted one. A request is kept whole ([`Call`]) so the
//! approval flow can repeat it unchanged.

use crate::agent::Minter;
use crate::context::{Context, Credential, Token, Trust};
use crate::error::{Failure, exit};
use crate::{Environment, tls};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Client, Method, Response};
use std::time::Duration;

/// The TCP connection and the TLS handshake together, so an address that
/// never answers fails in bounded time; the node agent uses the same bound.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// One request, kept whole so it can be sent again unchanged.
#[derive(Clone, Debug)]
pub struct Call {
    pub method: Method,
    /// The path and query, starting with `/`.
    pub path: String,
    pub headers: HeaderMap,
    /// A JSON body.
    pub body: Option<Vec<u8>>,
}

impl Call {
    pub fn get(path: impl Into<String>) -> Self {
        Self {
            method: Method::GET,
            path: path.into(),
            headers: HeaderMap::new(),
            body: None,
        }
    }
}

enum Auth {
    Anonymous,
    Bearer(Token),
    Agent(Box<Minter>),
}

/// A context with its HTTP client and credential.
pub struct Session {
    pub context: Context,
    http: Client,
    auth: Auth,
}

impl Session {
    /// Opens a session on `context`; an agent's mints carry `justification`.
    /// Nothing is sent yet.
    pub fn open(
        mut context: Context,
        environment: &Environment,
        justification: &str,
    ) -> Result<Self, Failure> {
        let http = http_client(context.trust)?;
        let auth = match context.credential.take() {
            None => Auth::Anonymous,
            Some(Credential::Token(token)) => Auth::Bearer(token),
            Some(Credential::AgentKey(path)) => Auth::Agent(Box::new(Minter::new(
                &path,
                &context,
                environment,
                justification,
            )?)),
        };
        Ok(Self {
            context,
            http,
            auth,
        })
    }

    /// Mints a fresh access token now, for an agent; nothing for any other
    /// credential.
    pub async fn mint(&mut self) -> Result<(), Failure> {
        if let Auth::Agent(minter) = &mut self.auth {
            minter.mint(&self.http).await?;
        }
        Ok(())
    }

    /// Sends `call` with the session's credential. An agent whose cached
    /// token is refused with 401 mints once more and sends it again.
    pub async fn send(&mut self, call: &Call) -> Result<Response, Failure> {
        let url = &self.context.url;
        match &mut self.auth {
            Auth::Anonymous => send(&self.http, url, call, None).await,
            Auth::Bearer(token) => send(&self.http, url, call, Some(token.expose())).await,
            Auth::Agent(minter) => {
                let (token, fresh) = minter.token(&self.http).await?;
                let response = send(&self.http, url, call, Some(&token)).await?;
                if response.status().as_u16() != 401 || fresh {
                    return Ok(response);
                }
                minter.discard();
                let token = minter.mint(&self.http).await?;
                send(&self.http, url, call, Some(&token)).await
            }
        }
    }
}

async fn send(
    http: &Client,
    url: &str,
    call: &Call,
    token: Option<&str>,
) -> Result<Response, Failure> {
    let mut headers = call.headers.clone();
    if let Some(token) = token {
        let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| Failure::other("the token is not a valid header value"))?;
        value.set_sensitive(true);
        headers.insert(AUTHORIZATION, value);
    }
    let mut request = http
        .request(call.method.clone(), format!("{url}{}", call.path))
        .headers(headers);
    if let Some(body) = &call.body {
        request = request
            .header(CONTENT_TYPE, "application/json")
            .body(body.clone());
    }
    request.send().await.map_err(|e| unreachable(url, &e))
}

/// The HTTP client for a trust setting.
pub fn http_client(trust: Trust) -> Result<Client, Failure> {
    let config = match trust {
        Trust::Pin(pin) => tls::pinned(pin),
        Trust::System => tls::system(),
    }
    .map_err(Failure::other)?;
    client_with(config)
}

/// The HTTP client on a prepared TLS configuration.
pub fn client_with(config: rustls::ClientConfig) -> Result<Client, Failure> {
    Client::builder()
        .use_preconfigured_tls(config)
        .connect_timeout(CONNECT_TIMEOUT)
        // The API never redirects; a redirect is not followed with the token.
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(concat!("quoro/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|e| Failure::other(format!("cannot build the HTTP client: {e}")))
}

/// The primary's `Quoro-Request-Id`.
pub fn request_id(response: &Response) -> Option<String> {
    response
        .headers()
        .get("quoro-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

/// No response, or one cut short. The innermost cause is the useful one: a
/// pin mismatch is several layers below "error sending request".
pub fn unreachable(url: &str, error: &reqwest::Error) -> Failure {
    let mut cause: &dyn std::error::Error = error;
    while let Some(source) = cause.source() {
        cause = source;
    }
    Failure::new(
        exit::UNREACHABLE,
        format!("cannot reach the primary at {url}: {cause}"),
    )
}

/// The whole body of `response`.
pub async fn read_all(
    response: Response,
    url: &str,
    request_id: Option<String>,
) -> Result<Vec<u8>, Failure> {
    match response.bytes().await {
        Ok(body) => Ok(body.to_vec()),
        Err(e) => Err(Failure {
            request_id,
            ..unreachable(url, &e)
        }),
    }
}

/// A response read whole: its status, request id and body.
pub struct Answer {
    pub status: u16,
    pub request_id: Option<String>,
    pub body: Vec<u8>,
}

impl Answer {
    pub async fn read(response: Response, url: &str) -> Result<Self, Failure> {
        let status = response.status().as_u16();
        let request_id = request_id(&response);
        let body = read_all(response, url, request_id.clone()).await?;
        Ok(Self {
            status,
            request_id,
            body,
        })
    }

    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// The body as JSON, or the failure the answer stands for.
    pub fn json(self) -> Result<serde_json::Value, Failure> {
        if !self.is_success() {
            return Err(self.failure());
        }
        serde_json::from_slice(&self.body).map_err(|_| Failure {
            request_id: self.request_id,
            ..Failure::other("the primary's answer is not JSON")
        })
    }

    pub fn failure(&self) -> Failure {
        Failure::from_response(self.status, &self.body, self.request_id.clone())
    }
}

/// Seconds since the Unix epoch.
pub fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX)
        })
}

/// Reads an RFC 3339 timestamp, such as the primary's
/// `2026-11-06T10:00:00.000Z`, as seconds since the Unix epoch; a fraction is
/// dropped.
pub fn unix_seconds(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    let number = |range: std::ops::Range<usize>| -> Option<i64> {
        let digits = text.get(range)?;
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        digits.parse().ok()
    };
    if bytes.len() < 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !matches!(bytes[10], b'T' | b't' | b' ')
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let (year, month, day) = (number(0..4)?, number(5..7)?, number(8..10)?);
    let (hour, minute, second) = (number(11..13)?, number(14..16)?, number(17..19)?);
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    let mut rest = &text[19..];
    if let Some(fraction) = rest.strip_prefix('.') {
        let digits = fraction.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return None;
        }
        rest = &fraction[digits..];
    }
    let offset = match rest {
        "Z" | "z" => 0,
        _ => {
            let sign = match rest.as_bytes().first()? {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            if rest.len() != 6 || rest.as_bytes()[3] != b':' {
                return None;
            }
            let hours: i64 = rest[1..3].parse().ok()?;
            let minutes: i64 = rest[4..6].parse().ok()?;
            sign * (hours * 3600 + minutes * 60)
        }
    };
    // Days from the civil date (Howard Hinnant's algorithm).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let year_of_era = y - era * 400;
    let month_index = (month + 9) % 12;
    let day_of_year = (153 * month_index + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    Some(days * 86_400 + hour * 3600 + minute * 60 + second - offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_read_as_unix_seconds() {
        assert_eq!(unix_seconds("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            unix_seconds("2026-11-06T10:00:00.000Z"),
            Some(1_793_959_200)
        );
        assert_eq!(unix_seconds("2000-02-29T23:59:59.999Z"), Some(951_868_799));
        assert_eq!(
            unix_seconds("2026-11-06T12:00:00+02:00"),
            Some(1_793_959_200)
        );
        for bad in [
            "",
            "2026-11-06",
            "2026-13-06T10:00:00Z",
            "2026-11-06T10:00:00",
            "x026-11-06T10:00:00Z",
        ] {
            assert_eq!(unix_seconds(bad), None, "{bad}");
        }
    }
}
