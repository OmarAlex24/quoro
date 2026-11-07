//! Contexts: which primary the client talks to, how it trusts it, and the
//! credential it presents.
//!
//! `config.toml` names the contexts and the current one:
//!
//! ```toml
//! current = "lab"
//!
//! [contexts.lab]
//! url = "https://primary.example:8443"
//! fingerprint = "sha256:..."   # or: tls = "system"
//! ```
//!
//! `credentials.toml` holds a token per context (`[contexts.lab]
//! token = "tok_..."`). It must be readable by its owner only (mode `0600`),
//! the way SSH treats a private key, and is refused otherwise. Both files live
//! in `$XDG_CONFIG_HOME/quoro/`, or `~/.config/quoro/` (also on macOS).
//!
//! The context is `--context`, else `QUORO_CONTEXT`, else an ad-hoc one from
//! `QUORO_URL` and `QUORO_FINGERPRINT` (for CI), else `current`.
//! `QUORO_TOKEN` overrides the stored token. A token is never printed: errors
//! name files and contexts, never their content.

use crate::error::Failure;
use crate::{Environment, tls};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// A resolved context.
#[derive(Debug)]
pub struct Context {
    /// The base URL, `https://host[:port]`, without a trailing slash.
    pub url: String,
    pub trust: Trust,
    pub token: Option<Token>,
}

/// How the primary's certificate is checked.
#[derive(Debug, PartialEq, Eq)]
pub enum Trust {
    /// The SHA-256 of its leaf certificate.
    Pin([u8; 32]),
    /// The host's CA store.
    System,
}

/// A bearer token. `Debug` never shows it.
pub struct Token(String);

impl Token {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Token(..)")
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    current: Option<String>,
    #[serde(default)]
    contexts: BTreeMap<String, ContextEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextEntry {
    url: String,
    fingerprint: Option<String>,
    tls: Option<String>,
}

#[derive(Default, Deserialize)]
struct CredentialsFile {
    #[serde(default)]
    contexts: BTreeMap<String, CredentialEntry>,
}

#[derive(Deserialize)]
struct CredentialEntry {
    token: String,
}

/// Resolves the context for a command; `flag` is `--context`.
pub fn resolve(flag: Option<&str>, environment: &Environment) -> Result<Context, Failure> {
    let directory = config_dir(environment)?;
    let (name, url, trust) = match flag.or(environment.var("QUORO_CONTEXT")) {
        Some(name) => {
            let (url, trust) = named(&directory, name)?;
            (Some(name.to_string()), url, trust)
        }
        None => match environment.var("QUORO_URL") {
            Some(url) => (None, url.to_string(), ad_hoc_trust(environment)?),
            None => {
                let name = current(&directory)?;
                let (url, trust) = named(&directory, &name)?;
                (Some(name), url, trust)
            }
        },
    };
    let token = match environment.var("QUORO_TOKEN") {
        Some(token) => Some(token_from("QUORO_TOKEN", token)?),
        None => match &name {
            Some(name) => stored_token(&directory.join("credentials.toml"), name)?,
            None => None,
        },
    };
    Ok(Context {
        url: base_url(&url)?,
        trust,
        token,
    })
}

/// `$XDG_CONFIG_HOME/quoro`, or `$HOME/.config/quoro`.
fn config_dir(environment: &Environment) -> Result<PathBuf, Failure> {
    let base = environment
        .var("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .or_else(|| {
            environment
                .var("HOME")
                .map(|home| Path::new(home).join(".config"))
        })
        .ok_or_else(|| Failure::usage("neither XDG_CONFIG_HOME nor HOME is set"))?;
    Ok(base.join("quoro"))
}

fn read_config(directory: &Path) -> Result<(PathBuf, ConfigFile), Failure> {
    let path = directory.join("config.toml");
    let config = match std::fs::read_to_string(&path) {
        Ok(text) => toml::from_str(&text).map_err(|e| {
            Failure::other(format!("cannot read {}: {}", path.display(), e.message()))
        })?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => ConfigFile::default(),
        Err(e) => {
            return Err(Failure::other(format!(
                "cannot read {}: {e}",
                path.display()
            )));
        }
    };
    Ok((path, config))
}

/// The `current` context of `config.toml`.
fn current(directory: &Path) -> Result<String, Failure> {
    let (path, config) = read_config(directory)?;
    config.current.ok_or_else(|| {
        Failure::usage(format!(
            "no context is selected: set `current` in {}, pass --context, or set QUORO_URL and QUORO_FINGERPRINT",
            path.display()
        ))
    })
}

/// The URL and trust of the context `name` in `config.toml`.
fn named(directory: &Path, name: &str) -> Result<(String, Trust), Failure> {
    let (path, mut config) = read_config(directory)?;
    let entry = config.contexts.remove(name).ok_or_else(|| {
        Failure::usage(format!(
            "the context '{name}' is not defined in {}",
            path.display()
        ))
    })?;
    let invalid =
        |what: &str| Failure::usage(format!("the context '{name}' in {} {what}", path.display()));
    let trust = match (entry.fingerprint, entry.tls.as_deref()) {
        (Some(fingerprint), None) => {
            Trust::Pin(tls::parse_fingerprint(&fingerprint).map_err(|e| invalid(&e))?)
        }
        (None, Some("system")) => Trust::System,
        (None, Some(other)) => {
            return Err(invalid(&format!(
                "has tls = {other:?}; the only value is \"system\""
            )));
        }
        (Some(_), Some(_)) => return Err(invalid("has both a fingerprint and tls; keep one")),
        (None, None) => return Err(invalid("needs a fingerprint or tls = \"system\"")),
    };
    Ok((entry.url, trust))
}

/// An ad-hoc context is always pinned: `QUORO_FINGERPRINT` is required.
fn ad_hoc_trust(environment: &Environment) -> Result<Trust, Failure> {
    let fingerprint = environment
        .var("QUORO_FINGERPRINT")
        .ok_or_else(|| Failure::usage("QUORO_URL needs QUORO_FINGERPRINT (sha256:...)"))?;
    tls::parse_fingerprint(fingerprint)
        .map(Trust::Pin)
        .map_err(|e| Failure::usage(format!("QUORO_FINGERPRINT: {e}")))
}

/// Checks the URL and drops a trailing slash, so paths join cleanly.
fn base_url(url: &str) -> Result<String, Failure> {
    let url = url.trim().trim_end_matches('/');
    let rest = url
        .strip_prefix("https://")
        .ok_or_else(|| Failure::usage(format!("the URL {url:?} must start with https://")))?;
    if rest.is_empty() || rest.contains(['/', '?', '#']) {
        return Err(Failure::usage(format!(
            "the URL {url:?} must be https://host[:port], without a path"
        )));
    }
    Ok(url.to_string())
}

/// The token stored for `name`, or `None` when there is no file or no entry.
fn stored_token(path: &Path, name: &str) -> Result<Option<Token>, Failure> {
    let shown = path.display();
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(Failure::other(format!("cannot read {shown}: {e}"))),
    };
    if !metadata.is_file() {
        return Err(Failure::other(format!("{shown} is not a regular file")));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = metadata.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(Failure::other(format!(
                "the credentials file {shown} is accessible by other users (mode {mode:o}); \
                 restrict it with 'chmod 600 {shown}'"
            )));
        }
    }
    let text = std::fs::read_to_string(path)
        .map_err(|e| Failure::other(format!("cannot read {shown}: {e}")))?;
    // The parser's message can quote the file, so only the line is shown.
    let mut file: CredentialsFile = toml::from_str(&text).map_err(|e| {
        let line = e
            .span()
            .map_or(1, |span| text[..span.start].matches('\n').count() + 1);
        Failure::other(format!("cannot parse {shown} (line {line})"))
    })?;
    file.contexts
        .remove(name)
        .map(|entry| token_from(&format!("the token of '{name}' in {shown}"), &entry.token))
        .transpose()
}

/// A token is sent as a header value, so it must be one printable word.
fn token_from(source: &str, text: &str) -> Result<Token, Failure> {
    let token = text.trim();
    if token.is_empty() || !token.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(Failure::other(format!(
            "{source} is not a token (one word of printable ASCII)"
        )));
    }
    Ok(Token(token.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PIN: &str = "sha256:a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";

    /// A fresh `$XDG_CONFIG_HOME` with `config.toml` holding `config`.
    fn home(config: &str) -> (PathBuf, Environment) {
        let base = std::env::temp_dir().join(format!(
            "quoro-cli-context-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(base.join("quoro")).unwrap();
        std::fs::write(base.join("quoro/config.toml"), config).unwrap();
        let environment =
            Environment::new([("XDG_CONFIG_HOME", base.display().to_string())], false);
        (base, environment)
    }

    fn with(environment: &Environment, extra: &[(&str, &str)]) -> Environment {
        let mut vars = environment.vars.clone();
        for (key, value) in extra {
            vars.insert((*key).to_string(), (*value).to_string());
        }
        Environment::new(vars, false)
    }

    fn error(result: Result<Context, Failure>) -> String {
        result.expect_err("refused").message
    }

    #[test]
    fn current_flag_and_variable_pick_the_context_in_that_order() {
        let (_dir, environment) = home(&format!(
            "current = \"a\"\n\
             [contexts.a]\nurl = \"https://a.example:8443/\"\nfingerprint = \"{PIN}\"\n\
             [contexts.b]\nurl = \"https://b.example\"\ntls = \"system\"\n"
        ));
        let a = resolve(None, &environment).unwrap();
        assert_eq!(a.url, "https://a.example:8443");
        assert!(matches!(a.trust, Trust::Pin(_)));
        assert!(a.token.is_none());

        let by_variable = with(&environment, &[("QUORO_CONTEXT", "b")]);
        let b = resolve(None, &by_variable).unwrap();
        assert_eq!(
            (b.url.as_str(), b.trust),
            ("https://b.example", Trust::System)
        );

        let by_flag = resolve(Some("a"), &by_variable).unwrap();
        assert_eq!(by_flag.url, "https://a.example:8443", "--context wins");

        assert!(error(resolve(Some("c"), &environment)).contains("'c' is not defined"));
    }

    #[test]
    fn an_ad_hoc_context_comes_from_the_environment_and_needs_a_pin() {
        let (_dir, environment) = home("");
        assert!(error(resolve(None, &environment)).contains("no context is selected"));

        let url_only = with(&environment, &[("QUORO_URL", "https://ci.example")]);
        assert!(error(resolve(None, &url_only)).contains("QUORO_FINGERPRINT"));

        let ad_hoc = with(
            &url_only,
            &[("QUORO_FINGERPRINT", PIN), ("QUORO_TOKEN", "tok_secret")],
        );
        let context = resolve(None, &ad_hoc).unwrap();
        assert_eq!(context.url, "https://ci.example");
        assert_eq!(context.token.unwrap().expose(), "tok_secret");
    }

    #[test]
    fn a_context_needs_exactly_one_trust_and_an_https_url() {
        let cases = [
            ("url = \"https://x\"\n", "needs a fingerprint"),
            (
                &*format!("url = \"https://x\"\nfingerprint = \"{PIN}\"\ntls = \"system\"\n"),
                "both",
            ),
            ("url = \"https://x\"\ntls = \"none\"\n", "only value"),
            ("url = \"https://x\"\nfingerprint = \"abc\"\n", "32 bytes"),
            (
                &*format!("url = \"http://x\"\nfingerprint = \"{PIN}\"\n"),
                "https://",
            ),
            (
                &*format!("url = \"https://x/api\"\nfingerprint = \"{PIN}\"\n"),
                "without a path",
            ),
            ("url = \"https://x\"\nfingerprnt = \"a\"\n", "unknown field"),
        ];
        for (entry, expected) in cases {
            let (_dir, environment) = home(&format!("[contexts.a]\n{entry}"));
            let message = error(resolve(Some("a"), &environment));
            assert!(message.contains(expected), "{entry}: {message}");
        }
    }

    #[test]
    fn a_token_is_never_shown() {
        assert_eq!(format!("{:?}", Token("tok_secret".into())), "Token(..)");
        let message = token_from("QUORO_TOKEN", "tok secret").unwrap_err().message;
        assert!(!message.contains("secret"), "{message}");
    }
}
