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
//! `credentials.toml` holds the credential of each context: a token
//! (`[contexts.lab] token = "tok_..."`) or, for an agent, the absolute path of
//! its key file (`key_file = "/home/me/.quoro/agent.key"`), never the key
//! itself. It must be readable by its owner only (mode `0600`), the way SSH
//! treats a private key, and is refused otherwise. Both files live in
//! `$XDG_CONFIG_HOME/quoro/`, or `~/.config/quoro/` (also on macOS).
//!
//! The context is `--context`, else `QUORO_CONTEXT`, else an ad-hoc one from
//! `QUORO_URL` and `QUORO_FINGERPRINT` (for CI), else `current`.
//! `QUORO_TOKEN` overrides the stored credential, and `QUORO_AGENT_KEY_FILE`
//! overrides it with an agent key file. A token is never printed: errors name
//! files and contexts, never their content.

use crate::error::Failure;
use crate::{Environment, tls};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// A resolved context.
#[derive(Debug)]
pub struct Context {
    /// Its name in `config.toml`; `None` for an ad-hoc context.
    pub name: Option<String>,
    /// The base URL, `https://host[:port]`, without a trailing slash.
    pub url: String,
    pub trust: Trust,
    pub credential: Option<Credential>,
}

/// What a context authenticates with.
#[derive(Debug)]
pub enum Credential {
    /// A bearer token: a human's `tok_`, or an agent's `atk_` handed in.
    Token(Token),
    /// The file that holds an agent key (`akey_`), which only mints access
    /// tokens (`agent.rs`).
    AgentKey(PathBuf),
}

impl Context {
    /// Whether the caller is an agent: an agent key, or an access token.
    pub fn is_agent(&self) -> bool {
        match &self.credential {
            Some(Credential::AgentKey(_)) => true,
            Some(Credential::Token(token)) => token.expose().starts_with("atk_"),
            None => false,
        }
    }
}

/// How the primary's certificate is checked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Trust {
    /// The SHA-256 of its leaf certificate.
    Pin([u8; 32]),
    /// The host's CA store.
    System,
}

/// A bearer token. `Debug` never shows it.
pub struct Token(String);

impl Token {
    pub(crate) fn new(text: String) -> Self {
        Self(text)
    }

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
pub(crate) struct ConfigFile {
    pub current: Option<String>,
    #[serde(default)]
    pub contexts: BTreeMap<String, ContextEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ContextEntry {
    pub url: String,
    pub fingerprint: Option<String>,
    pub tls: Option<String>,
}

#[derive(Default, Deserialize)]
pub(crate) struct CredentialsFile {
    #[serde(default)]
    pub contexts: BTreeMap<String, CredentialEntry>,
}

#[derive(Default, Deserialize)]
pub(crate) struct CredentialEntry {
    pub token: Option<String>,
    pub key_file: Option<String>,
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
    let credential = if let Some(token) = environment.var("QUORO_TOKEN") {
        Some(Credential::Token(token_from("QUORO_TOKEN", token)?))
    } else if let Some(path) = environment.var("QUORO_AGENT_KEY_FILE") {
        Some(Credential::AgentKey(PathBuf::from(path)))
    } else {
        match &name {
            Some(name) => stored_credential(&directory.join("credentials.toml"), name)?,
            None => None,
        }
    };
    Ok(Context {
        name,
        url: base_url(&url)?,
        trust,
        credential,
    })
}

/// A named context as `config.toml` and `credentials.toml` hold it, without
/// the environment's overrides: what `quoro mcp install` writes into an MCP
/// client's configuration and `quoro mcp stdio --context` reads back.
#[derive(Debug)]
pub struct Stored {
    /// The base URL, `https://host[:port]`.
    pub url: String,
    /// `sha256:<hex>` as written; `None` for `tls = "system"`.
    pub fingerprint: Option<String>,
    /// The agent key file's path; `None` when the context stores a token or
    /// no credential.
    pub key_file: Option<PathBuf>,
    /// Whether the context stores a personal API token.
    pub has_token: bool,
}

/// The context `name` of `directory`'s `config.toml` and `credentials.toml`.
pub fn stored(directory: &Path, name: &str) -> Result<Stored, Failure> {
    let (path, mut config) = read_config(directory)?;
    let entry = config.contexts.remove(name).ok_or_else(|| {
        Failure::usage(format!(
            "the context '{name}' is not defined in {}",
            path.display()
        ))
    })?;
    let fingerprint = entry.fingerprint.clone();
    let (url, _) = entry_trust(&path, name, entry)?;
    let credentials = directory.join("credentials.toml");
    let (key_file, has_token) = match stored_credential(&credentials, name)? {
        Some(Credential::AgentKey(path)) => (Some(path), false),
        Some(Credential::Token(_)) => (None, true),
        None => (None, false),
    };
    Ok(Stored {
        url: base_url(&url)?,
        fingerprint,
        key_file,
        has_token,
    })
}

/// `$XDG_CONFIG_HOME/quoro`, or `$HOME/.config/quoro`.
pub(crate) fn config_dir(environment: &Environment) -> Result<PathBuf, Failure> {
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

pub(crate) fn read_config(directory: &Path) -> Result<(PathBuf, ConfigFile), Failure> {
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
pub(crate) fn current(directory: &Path) -> Result<String, Failure> {
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
    entry_trust(&path, name, entry)
}

/// The URL and trust of one `config.toml` entry.
pub(crate) fn entry_trust(
    path: &Path,
    name: &str,
    entry: ContextEntry,
) -> Result<(String, Trust), Failure> {
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
pub(crate) fn base_url(url: &str) -> Result<String, Failure> {
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

/// The credential stored for `name`, or `None` when there is no file or no
/// entry.
fn stored_credential(path: &Path, name: &str) -> Result<Option<Credential>, Failure> {
    let shown = path.display();
    let Some(mut file) = read_credentials(path)? else {
        return Ok(None);
    };
    let Some(entry) = file.contexts.remove(name) else {
        return Ok(None);
    };
    match (entry.token, entry.key_file) {
        (Some(token), None) => Ok(Some(Credential::Token(token_from(
            &format!("the token of '{name}' in {shown}"),
            &token,
        )?))),
        (None, Some(key_file)) => Ok(Some(Credential::AgentKey(PathBuf::from(key_file)))),
        (None, None) => Ok(None),
        (Some(_), Some(_)) => Err(Failure::other(format!(
            "the context '{name}' in {shown} has both a token and a key_file; keep one"
        ))),
    }
}

/// `credentials.toml`, or `None` when it does not exist. Refused when anyone
/// but its owner can read or write it.
pub(crate) fn read_credentials(path: &Path) -> Result<Option<CredentialsFile>, Failure> {
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
    let file: CredentialsFile = toml::from_str(&text).map_err(|e| {
        let line = e
            .span()
            .map_or(1, |span| text[..span.start].matches('\n').count() + 1);
        Failure::other(format!("cannot parse {shown} (line {line})"))
    })?;
    Ok(Some(file))
}

/// Writes `config.toml` in `directory` (mode `0644`: it holds no secret).
pub(crate) fn write_config(directory: &Path, config: &ConfigFile) -> Result<(), Failure> {
    let mut text = String::new();
    if let Some(current) = &config.current {
        text.push_str(&format!("current = {}\n", toml_string(current)));
    }
    for (name, entry) in &config.contexts {
        text.push_str(&format!("\n[contexts.{}]\n", toml_string(name)));
        text.push_str(&format!("url = {}\n", toml_string(&entry.url)));
        if let Some(fingerprint) = &entry.fingerprint {
            text.push_str(&format!("fingerprint = {}\n", toml_string(fingerprint)));
        }
        if let Some(tls) = &entry.tls {
            text.push_str(&format!("tls = {}\n", toml_string(tls)));
        }
    }
    write_atomic(&directory.join("config.toml"), text.as_bytes(), 0o644)
}

/// Writes `credentials.toml` in `directory`, mode `0600`.
pub(crate) fn write_credentials(directory: &Path, file: &CredentialsFile) -> Result<(), Failure> {
    let mut text = String::new();
    for (name, entry) in &file.contexts {
        if !text.is_empty() {
            text.push('\n');
        }
        text.push_str(&format!("[contexts.{}]\n", toml_string(name)));
        if let Some(token) = &entry.token {
            text.push_str(&format!("token = {}\n", toml_string(token)));
        }
        if let Some(key_file) = &entry.key_file {
            text.push_str(&format!("key_file = {}\n", toml_string(key_file)));
        }
    }
    write_atomic(&directory.join("credentials.toml"), text.as_bytes(), 0o600)
}

/// A TOML basic string.
pub(crate) fn toml_string(text: &str) -> String {
    let mut quoted = String::with_capacity(text.len() + 2);
    quoted.push('"');
    for c in text.chars() {
        match c {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\n' => quoted.push_str("\\n"),
            '\t' => quoted.push_str("\\t"),
            c if c.is_control() => quoted.push_str(&format!("\\u{:04X}", u32::from(c))),
            c => quoted.push(c),
        }
    }
    quoted.push('"');
    quoted
}

/// Creates `directory` and its parents for the owner only (`0700`).
pub(crate) fn create_private_dir(directory: &Path) -> Result<(), Failure> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    builder
        .create(directory)
        .map_err(|e| Failure::other(format!("cannot create {}: {e}", directory.display())))
}

/// Replaces `path` with `bytes` in one rename: the content goes to a
/// temporary file created with `mode` in the same directory, so a reader
/// sees the old file or the new one and a secret is never readable by others,
/// not even for a moment.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8], mode: u32) -> Result<(), Failure> {
    create_private_dir(path.parent().unwrap_or(Path::new(".")))?;
    replace_atomic(path, bytes, mode)
}

/// [`write_atomic`] into a directory that already exists: the temporary file
/// is created with `mode` and then given exactly `mode`, whatever the umask,
/// so a caller that keeps a file's mode keeps it to the bit.
pub(crate) fn replace_atomic(path: &Path, bytes: &[u8], mode: u32) -> Result<(), Failure> {
    use std::io::Write as _;
    let directory = path.parent().unwrap_or(Path::new("."));
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let temporary = directory.join(format!(".{file_name}.{}.tmp", std::process::id()));
    let failed =
        |e: std::io::Error| Failure::other(format!("cannot write {}: {e}", path.display()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(mode);
    }
    #[cfg(not(unix))]
    let _ = mode;
    let _ = std::fs::remove_file(&temporary);
    let result = options
        .open(&temporary)
        .and_then(|mut file| {
            file.write_all(bytes)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                file.set_permissions(std::fs::Permissions::from_mode(mode))?;
            }
            file.sync_all()
        })
        .and_then(|()| std::fs::rename(&temporary, path));
    if let Err(e) = result {
        let _ = std::fs::remove_file(&temporary);
        return Err(failed(e));
    }
    Ok(())
}

/// A token is sent as a header value, so it must be one printable word.
pub(crate) fn token_from(source: &str, text: &str) -> Result<Token, Failure> {
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
        assert!(a.credential.is_none());
        assert_eq!(a.name.as_deref(), Some("a"));

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
        assert!(context.name.is_none() && !context.is_agent());
        let Some(Credential::Token(token)) = context.credential else {
            panic!("a token");
        };
        assert_eq!(token.expose(), "tok_secret");

        let agent = with(
            &url_only,
            &[("QUORO_FINGERPRINT", PIN), ("QUORO_AGENT_KEY_FILE", "/k")],
        );
        let context = resolve(None, &agent).unwrap();
        assert!(context.is_agent());
        assert!(
            matches!(context.credential, Some(Credential::AgentKey(path)) if path == Path::new("/k"))
        );
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
    fn written_files_read_back_and_credentials_stay_private() {
        let (base, environment) = home("");
        let directory = base.join("quoro");
        let odd = "a \"quoted\" \\ name";
        let mut config = ConfigFile {
            current: Some(odd.to_string()),
            ..ConfigFile::default()
        };
        config.contexts.insert(
            odd.to_string(),
            ContextEntry {
                url: "https://a.example:8443".to_string(),
                fingerprint: Some(PIN.to_string()),
                tls: None,
            },
        );
        write_config(&directory, &config).unwrap();
        let mut credentials = CredentialsFile::default();
        credentials.contexts.insert(
            odd.to_string(),
            CredentialEntry {
                token: None,
                key_file: Some("/keys/agent \"1\".key".to_string()),
            },
        );
        write_credentials(&directory, &credentials).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(directory.join("credentials.toml"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        let context = resolve(None, &environment).unwrap();
        assert_eq!(context.name.as_deref(), Some(odd));
        assert_eq!(context.url, "https://a.example:8443");
        assert!(
            matches!(context.credential, Some(Credential::AgentKey(path)) if path == Path::new("/keys/agent \"1\".key"))
        );
    }

    #[test]
    fn a_token_is_never_shown() {
        assert_eq!(format!("{:?}", Token("tok_secret".into())), "Token(..)");
        let message = token_from("QUORO_TOKEN", "tok secret").unwrap_err().message;
        assert!(!message.contains("secret"), "{message}");
    }
}
