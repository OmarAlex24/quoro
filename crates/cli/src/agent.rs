//! An agent's credential: the key file, the access tokens minted from it and
//! their cache.
//!
//! The agent key (`akey_`) authorizes nothing by itself (permission-model.md
//! section 2): it only mints a five-minute access token (`atk_`) at
//! `POST /api/v1/auth/agent-token`, and that token is what every call carries.
//! The key file must be readable by its owner only, the rule
//! `quoro mcp stdio` applies to the same file.
//!
//! A command does not mint a token every time: the last one is kept in
//! `$XDG_RUNTIME_DIR/quoro/` (else `$XDG_CACHE_HOME/quoro/`, else
//! `~/.cache/quoro/`, also on macOS), one file per context, mode `0600` in a
//! `0700` directory. It is reused until 30 seconds before it expires, and only
//! for the same primary and the same key, so pointing `QUORO_AGENT_KEY_FILE`
//! at another agent's key never reuses the first agent's token. When the
//! primary answers 401 to a cached token the client mints once more. Neither
//! the key nor a token is ever printed.

use crate::Environment;
use crate::client::{self, now_unix, unix_seconds};
use crate::context::{self, Context, Token};
use crate::error::Failure;
use crate::operations::CREATE_AGENT_TOKEN;
use reqwest::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderValue};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::path::{Path, PathBuf};

/// The prefix of an agent key; anything else in the key file is refused.
const AGENT_KEY_PREFIX: &str = "akey_";
/// The prefix of the access token the primary mints.
const ACCESS_TOKEN_PREFIX: &str = "atk_";
/// An access token lives five minutes (openapi.yaml, `AccessTokenResponse`);
/// this is assumed when `expires_at` cannot be read.
const TOKEN_LIFETIME_S: i64 = 300;
/// A cached token is replaced this long before it expires, so a command never
/// reaches the primary with a token that lapses on the way.
const REUSE_MARGIN_S: i64 = 30;

/// The `Quoro-Justification` of a mint when the command has no `--reason`;
/// the contract requires one because the caller is an agent
/// (openapi.yaml, `createAgentToken`).
pub fn default_justification(command: &str) -> String {
    format!("quoro {command}: access token for a CLI command")
}

/// Reads the agent key from `path`.
///
/// The file must be a regular file that only its owner can read or write
/// (mode `0600` or stricter), the way SSH treats a private key, and must hold
/// one `akey_...` value. Errors name the path, never the content. The same
/// rule and messages as `quoro mcp stdio` (`crates/quoro/src/mcp_stdio.rs`).
pub fn read_agent_key(path: &Path) -> Result<String, String> {
    let shown = path.display();
    let metadata = std::fs::metadata(path).map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound => format!("the agent key file {shown} does not exist"),
        _ => format!("cannot read the agent key file {shown}: {e}"),
    })?;
    if !metadata.is_file() {
        return Err(format!("the agent key file {shown} is not a regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = metadata.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(format!(
                "the agent key file {shown} is accessible by other users (mode {mode:o}); \
                 restrict it with 'chmod 600 {shown}'"
            ));
        }
    }
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read the agent key file {shown}: {e}"))?;
    let key = text.trim();
    let well_formed = key.len() > AGENT_KEY_PREFIX.len()
        && key.starts_with(AGENT_KEY_PREFIX)
        && key.bytes().all(|b| b.is_ascii_graphic());
    if !well_formed {
        return Err(format!(
            "the agent key file {shown} does not hold an agent key (one line starting with {AGENT_KEY_PREFIX})"
        ));
    }
    Ok(key.to_string())
}

/// Mints access tokens from one agent key and keeps the current one.
pub struct Minter {
    key: Token,
    /// SHA-256 of the key, so a cached token is only reused for it.
    key_digest: String,
    url: String,
    justification: HeaderValue,
    cache: Option<PathBuf>,
    current: Option<Token>,
}

impl Minter {
    /// Reads the key file and the cached token. `justification` is sent with
    /// every mint.
    pub fn new(
        key_file: &Path,
        context: &Context,
        environment: &Environment,
        justification: &str,
    ) -> Result<Self, Failure> {
        let key = read_agent_key(key_file).map_err(Failure::other)?;
        let justification = HeaderValue::from_str(justification.trim())
            .map_err(|_| Failure::usage("--reason must be printable ASCII on one line"))?;
        let mut minter = Self {
            key_digest: hex(&Sha256::digest(key.as_bytes())),
            key: Token::new(key),
            url: context.url.clone(),
            justification,
            cache: cache_path(environment, context),
            current: None,
        };
        minter.current = minter.read_cache();
        Ok(minter)
    }

    /// The token to send and whether it was just minted.
    pub async fn token(&mut self, http: &Client) -> Result<(String, bool), Failure> {
        if let Some(token) = &self.current {
            return Ok((token.expose().to_string(), false));
        }
        self.mint(http).await.map(|token| (token, true))
    }

    /// Forgets the current token, after the primary refused it.
    pub fn discard(&mut self) {
        self.current = None;
        if let Some(path) = &self.cache {
            let _ = std::fs::remove_file(path);
        }
    }

    /// Exchanges the key for a fresh access token and caches it.
    pub async fn mint(&mut self, http: &Client) -> Result<String, Failure> {
        self.current = None;
        let mut authorization = HeaderValue::from_str(&format!("Bearer {}", self.key.expose()))
            .map_err(|_| Failure::other("the agent key is not a valid header value"))?;
        authorization.set_sensitive(true);
        let response = http
            .post(format!("{}{}", self.url, CREATE_AGENT_TOKEN.path))
            .header(AUTHORIZATION, authorization)
            .header("quoro-justification", self.justification.clone())
            .header(ACCEPT, "application/json")
            .send()
            .await
            .map_err(|e| client::unreachable(&self.url, &e))?;
        let request_id = client::request_id(&response);
        let status = response.status();
        let body = client::read_all(response, &self.url, request_id.clone()).await?;
        if status.as_u16() != 200 {
            let mut failure = Failure::from_response(status.as_u16(), &body, request_id);
            failure.message = format!(
                "the primary refused to mint an access token from the agent key: {}",
                failure.message
            );
            return Err(failure);
        }
        let body: Value = serde_json::from_slice(&body)
            .map_err(|_| Failure::other("the agent-token answer is not JSON"))?;
        let token = body["access_token"]
            .as_str()
            .filter(|t| {
                t.starts_with(ACCESS_TOKEN_PREFIX) && t.bytes().all(|b| b.is_ascii_graphic())
            })
            .ok_or_else(|| Failure::other("the agent-token answer has no access_token"))?
            .to_string();
        let expires_at = body["expires_at"]
            .as_str()
            .and_then(unix_seconds)
            .unwrap_or_else(|| now_unix() + TOKEN_LIFETIME_S);
        self.write_cache(&token, expires_at);
        self.current = Some(Token::new(token.clone()));
        Ok(token)
    }

    /// The cached token, when it is for this primary and key, is private, and
    /// has more than [`REUSE_MARGIN_S`] left.
    fn read_cache(&self) -> Option<Token> {
        let path = self.cache.as_ref()?;
        let metadata = std::fs::metadata(path).ok()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 {
                return None;
            }
        }
        let entry: Value = serde_json::from_slice(&std::fs::read(path).ok()?).ok()?;
        let token = entry["access_token"].as_str()?;
        let fresh = entry["url"] == self.url.as_str()
            && entry["key_sha256"] == self.key_digest.as_str()
            && token.starts_with(ACCESS_TOKEN_PREFIX)
            && token.bytes().all(|b| b.is_ascii_graphic())
            && entry["expires_at"].as_i64()? - REUSE_MARGIN_S > now_unix();
        fresh.then(|| Token::new(token.to_string()))
    }

    /// Best effort: a token that cannot be cached is minted again next time.
    fn write_cache(&self, token: &str, expires_at: i64) {
        let Some(path) = &self.cache else { return };
        let Some(directory) = path.parent() else {
            return;
        };
        if context::create_private_dir(directory).is_err() {
            return;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let Ok(metadata) = std::fs::metadata(directory) else {
                return;
            };
            if metadata.permissions().mode() & 0o077 != 0
                && std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))
                    .is_err()
            {
                return;
            }
        }
        let entry = json!({
            "url": self.url,
            "key_sha256": self.key_digest,
            "access_token": token,
            "expires_at": expires_at,
        });
        let _ = context::write_atomic(path, entry.to_string().as_bytes(), 0o600);
    }
}

/// The cache file of `context`, or `None` when no directory is known.
pub(crate) fn cache_path(environment: &Environment, context: &Context) -> Option<PathBuf> {
    let absolute = |name: &str| {
        environment
            .var(name)
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
    };
    let directory = absolute("XDG_RUNTIME_DIR")
        .or_else(|| absolute("XDG_CACHE_HOME"))
        .or_else(|| absolute("HOME").map(|home| home.join(".cache")))?
        .join("quoro");
    let safe = |name: &str| {
        !name.is_empty()
            && !name.starts_with('.')
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    };
    let file = match &context.name {
        Some(name) if safe(name) => format!("{name}.token"),
        Some(name) => format!(
            "context-{}.token",
            &hex(&Sha256::digest(name.as_bytes()))[..16]
        ),
        None => format!(
            "adhoc-{}.token",
            &hex(&Sha256::digest(context.url.as_bytes()))[..16]
        ),
    };
    Some(directory.join(file))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "quoro-cli-agent-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn key_file(dir: &Path, content: &str, mode: u32) -> PathBuf {
        use std::os::unix::fs::PermissionsExt as _;
        let path = dir.join("agent.key");
        std::fs::write(&path, content).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        path
    }

    #[test]
    fn a_private_key_file_is_read_and_trimmed() {
        let dir = scratch("read");
        let path = key_file(&dir, "akey_secret\n", 0o600);
        assert_eq!(read_agent_key(&path).unwrap(), "akey_secret");
    }

    #[test]
    fn a_missing_shared_or_malformed_key_file_is_refused() {
        let dir = scratch("refuse");
        let missing = read_agent_key(&dir.join("absent")).unwrap_err();
        assert!(missing.contains("does not exist"), "{missing}");
        let shared = key_file(&dir, "akey_secret", 0o640);
        let message = read_agent_key(&shared).unwrap_err();
        assert!(
            message.contains("mode 640") && message.contains("chmod 600"),
            "{message}"
        );
        assert!(!message.contains("secret"), "{message}");
        let malformed = key_file(&dir, "tok_secret", 0o600);
        let message = read_agent_key(&malformed).unwrap_err();
        assert!(message.contains("does not hold an agent key"), "{message}");
        assert!(!message.contains("secret"), "{message}");
    }

    #[test]
    fn the_cache_file_is_per_context_and_falls_back_to_the_cache_dir() {
        let context = |name: Option<&str>| Context {
            name: name.map(str::to_string),
            url: "https://primary.example".to_string(),
            trust: context::Trust::System,
            credential: None,
        };
        let runtime = Environment::new(
            [("XDG_RUNTIME_DIR", "/run/user/1000"), ("HOME", "/home/me")],
            false,
        );
        assert_eq!(
            cache_path(&runtime, &context(Some("lab"))).unwrap(),
            Path::new("/run/user/1000/quoro/lab.token")
        );
        let home = Environment::new([("HOME", "/home/me")], false);
        assert_eq!(
            cache_path(&home, &context(Some("lab"))).unwrap(),
            Path::new("/home/me/.cache/quoro/lab.token")
        );
        let odd = cache_path(&home, &context(Some("../x"))).unwrap();
        let odd = odd.display().to_string();
        assert!(odd.starts_with("/home/me/.cache/quoro/context-"), "{odd}");
        let ad_hoc = cache_path(&home, &context(None)).unwrap();
        let ad_hoc = ad_hoc.display().to_string();
        assert!(
            ad_hoc.starts_with("/home/me/.cache/quoro/adhoc-"),
            "{ad_hoc}"
        );
        assert!(cache_path(&Environment::default(), &context(Some("lab"))).is_none());
    }
}
