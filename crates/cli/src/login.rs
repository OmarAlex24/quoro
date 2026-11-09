//! `quoro login [--url URL] [--fingerprint FP] [--token-stdin]` for a human,
//! `quoro login --agent --key-file PATH [--url URL] [--fingerprint FP]` for an
//! agent.
//!
//! Saves a context (`config.toml`: URL and certificate pin) and its
//! credential (`credentials.toml`, mode `0600`, written atomically) and makes
//! it the current context. The context is `--context`, else `QUORO_CONTEXT`,
//! else the current one, else `default`.
//!
//! A human's credential is a personal API token (`tok_`), created in the
//! panel (Settings → API tokens). It is typed at a prompt with the
//! terminal's echo off, or piped with `--token-stdin`, and never read from the
//! command line, where any local user could see it in the process list.
//! Without a terminal and without `--token-stdin` the login is refused before
//! anything is sent. An agent's access token (`atk_`) or key (`akey_`) given
//! as a human's token is refused the same way, before anything is sent.
//!
//! An agent's credential is the key file's absolute path, never the key. The
//! key file is checked first (owner only, as `quoro mcp stdio` checks it).
//!
//! Nothing is written until the login worked: `getMe` must answer with a user
//! for a token, with the agent for a key (an access token minted from it).
//!
//! Without `--fingerprint`, a new context's pin comes from a first contact: a
//! connection that records the certificate the primary presents and stops at
//! the handshake, so nothing is sent on it. The digest is shown and trusted
//! only when the user confirms it on a terminal; without a terminal the login
//! is refused and names `--fingerprint`. An existing context with the same
//! URL keeps its pin. On a terminal the certificate is confirmed before the
//! token is asked for, so a token is only typed for a trusted primary.

use crate::agent::{self, default_justification};
use crate::cli::{Global, LoginArgs};
use crate::client::{self, Answer, Call, Session};
use crate::context::{
    self, ConfigFile, Context, ContextEntry, Credential, CredentialEntry, Token, Trust,
};
use crate::error::{Failure, exit};
use crate::operations::{GET_HEALTH, GET_ME};
use crate::{Environment, tls};
use serde_json::Value;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// The context a login without `--context` creates when none is current.
const DEFAULT_CONTEXT: &str = "default";
/// The prefix of a personal API token.
const TOKEN_PREFIX: &str = "tok_";
/// Where a human creates and revokes a token in the panel.
pub const TOKEN_HOME: &str = "Settings → API tokens";

pub async fn run(
    global: &Global,
    args: LoginArgs,
    environment: &Environment,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    if args.agent {
        agent_login(global, args, environment, stdout, stderr).await
    } else {
        human_login(global, args, environment, stdout, stderr).await
    }
}

/// A human with a personal API token.
async fn human_login(
    global: &Global,
    args: LoginArgs,
    environment: &Environment,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    // Where the token comes from is settled before anything is sent.
    let piped = if args.token_stdin {
        let bytes = environment
            .read_to_end()
            .map_err(|e| Failure::usage(format!("cannot read the token from stdin: {e}")))?;
        let text = String::from_utf8(bytes)
            .map_err(|_| Failure::usage("the token on stdin is not text"))?;
        Some(personal_token(&text)?)
    } else if environment.stdin_is_terminal {
        None
    } else {
        return Err(Failure::usage(format!(
            "there is no terminal to ask for the token: pipe it with --token-stdin, or run \
             quoro login on a terminal. A token is never read from the command line. Create \
             one in the panel: {TOKEN_HOME}"
        )));
    };

    let mut target = Target::open(global, &args, environment)?;
    let trust = target.trust(&args, environment, stderr).await?;
    let token = match piped {
        Some(token) => token,
        None => {
            let _ = write!(
                stderr,
                "Paste a personal API token (tok_...) for {url}. Create one in the panel: \
                 {TOKEN_HOME} ({url}/settings/tokens).\nToken: ",
                url = target.url
            );
            let _ = stderr.flush();
            let line = environment
                .read_hidden_line()
                .map_err(|e| Failure::usage(format!("cannot read the token: {e}")))?;
            personal_token(&line)?
        }
    };
    let secret = token.expose().to_string();

    let context = Context {
        name: Some(target.name.clone()),
        url: target.url.clone(),
        trust,
        credential: Some(Credential::Token(token)),
    };
    let reason = environment
        .justification(global)
        .unwrap_or_else(|| default_justification("login"));
    let mut session = Session::open(context, environment, &reason)?;
    let me = Answer::read(session.send(&Call::get(GET_ME.path)).await?, &target.url)
        .await?
        .json()
        .map_err(|mut failure| {
            failure.message = format!(
                "the primary refused the token: {}; nothing was written",
                failure.message
            );
            failure
        })?;
    if me["type"] != "user" {
        return Err(Failure::other(
            "the primary did not answer with a user for this token; nothing was written",
        ));
    }

    target.save(
        trust,
        CredentialEntry {
            token: Some(secret),
            key_file: None,
        },
    )?;
    let user = &me["user"];
    let name = &target.name;
    crate::api::write(
        stdout,
        format!(
            "Logged in to {name} ({}) as {} <{}>, role {}; {name} is the current context.\n",
            target.url,
            text(user, "name"),
            text(user, "email"),
            text(user, "role")
        )
        .as_bytes(),
    )
}

/// An agent with its key file.
async fn agent_login(
    global: &Global,
    args: LoginArgs,
    environment: &Environment,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    let Some(key_file) = args.key_file.clone() else {
        return Err(Failure::usage("quoro login --agent needs --key-file PATH"));
    };
    let key_file = std::path::absolute(&key_file).map_err(|e| {
        Failure::usage(format!(
            "cannot resolve the key file path {}: {e}",
            key_file.display()
        ))
    })?;
    let key_file_text = key_file
        .to_str()
        .ok_or_else(|| Failure::usage("the key file path must be UTF-8"))?
        .to_string();
    // The key file is checked before anything is read or sent.
    agent::read_agent_key(&key_file).map_err(Failure::other)?;

    let mut target = Target::open(global, &args, environment)?;
    let trust = target.trust(&args, environment, stderr).await?;
    let context = Context {
        name: Some(target.name.clone()),
        url: target.url.clone(),
        trust,
        credential: Some(Credential::AgentKey(key_file)),
    };
    let reason = environment
        .justification(global)
        .unwrap_or_else(|| default_justification("login"));
    let mut session = Session::open(context, environment, &reason)?;
    // A fresh token, so the login proves the key works now.
    session.mint().await?;
    let me = Answer::read(session.send(&Call::get(GET_ME.path)).await?, &target.url)
        .await?
        .json()?;
    if me["type"] != "agent" {
        return Err(Failure::other(
            "the primary did not answer with an agent for this key; nothing was written",
        ));
    }

    target.save(
        trust,
        CredentialEntry {
            token: None,
            key_file: Some(key_file_text),
        },
    )?;
    let agent = &me["agent"];
    let name = &target.name;
    crate::api::write(
        stdout,
        format!(
            "Logged in to {name} ({}) as agent {} (model {}); {name} is the current context.\n",
            target.url,
            text(agent, "name"),
            text(agent, "model")
        )
        .as_bytes(),
    )
}

fn text(value: &Value, key: &str) -> String {
    value[key].as_str().unwrap_or("?").to_string()
}

/// A personal API token from what the user typed or piped; an agent's
/// credential is refused with the way to use it. Nothing here names the
/// secret.
fn personal_token(text: &str) -> Result<Token, Failure> {
    let token = context::token_from("the token", text).map_err(|failure| Failure {
        code: exit::USAGE,
        ..failure
    })?;
    let secret = token.expose();
    if secret.starts_with("atk_") || secret.starts_with("akey_") {
        return Err(Failure::usage(
            "that is an agent's credential, not a personal API token: an agent logs in with \
             quoro login --agent --key-file PATH; nothing was sent",
        ));
    }
    if !secret.starts_with(TOKEN_PREFIX) || secret.len() == TOKEN_PREFIX.len() {
        return Err(Failure::usage(format!(
            "a personal API token starts with {TOKEN_PREFIX}; create one in the panel: \
             {TOKEN_HOME}. Nothing was sent"
        )));
    }
    Ok(token)
}

/// The context a login writes: its name, URL, and the files it goes to.
struct Target {
    name: String,
    url: String,
    directory: PathBuf,
    config_path: PathBuf,
    config: ConfigFile,
    existing: Option<ContextEntry>,
}

impl Target {
    fn open(global: &Global, args: &LoginArgs, environment: &Environment) -> Result<Self, Failure> {
        let directory = context::config_dir(environment)?;
        let (config_path, mut config) = context::read_config(&directory)?;
        // The credentials file is checked (owner only, parseable) before
        // anything is sent, so a login never fails after the primary said yes.
        context::read_credentials(&directory.join("credentials.toml"))?;
        let name = global
            .context
            .clone()
            .or_else(|| environment.var("QUORO_CONTEXT").map(str::to_string))
            .or_else(|| config.current.clone())
            .unwrap_or_else(|| DEFAULT_CONTEXT.to_string());
        let existing = config.contexts.remove(&name);
        let url = match args
            .url
            .as_deref()
            .or(existing.as_ref().map(|e| e.url.as_str()))
        {
            Some(url) => context::base_url(url)?,
            None => {
                return Err(Failure::usage(format!(
                    "the context '{name}' is new: pass --url https://host[:port]"
                )));
            }
        };
        Ok(Self {
            name,
            url,
            directory,
            config_path,
            config,
            existing,
        })
    }

    /// `--fingerprint`, else the existing context's pin for the same URL,
    /// else a confirmed first contact.
    async fn trust(
        &mut self,
        args: &LoginArgs,
        environment: &Environment,
        stderr: &mut dyn Write,
    ) -> Result<Trust, Failure> {
        if let Some(fingerprint) = &args.fingerprint {
            return Ok(Trust::Pin(
                tls::parse_fingerprint(fingerprint)
                    .map_err(|e| Failure::usage(format!("--fingerprint: {e}")))?,
            ));
        }
        match self.existing.take() {
            Some(entry)
                if context::base_url(&entry.url).ok().as_deref() == Some(self.url.as_str()) =>
            {
                Ok(context::entry_trust(&self.config_path, &self.name, entry)?.1)
            }
            _ => first_contact(&self.url, environment, stderr).await,
        }
    }

    /// Writes the credential, then the context, and makes it current.
    fn save(&mut self, trust: Trust, credential: CredentialEntry) -> Result<(), Failure> {
        let mut credentials = context::read_credentials(&self.directory.join("credentials.toml"))?
            .unwrap_or_default();
        credentials.contexts.insert(self.name.clone(), credential);
        context::write_credentials(&self.directory, &credentials)?;
        let (fingerprint, tls) = match trust {
            Trust::Pin(pin) => (Some(tls::show(&pin)), None),
            Trust::System => (None, Some("system".to_string())),
        };
        self.config.contexts.insert(
            self.name.clone(),
            ContextEntry {
                url: self.url.clone(),
                fingerprint,
                tls,
            },
        );
        self.config.current = Some(self.name.clone());
        context::write_config(&self.directory, &self.config)
    }
}

/// Reads the certificate the primary at `url` presents, without trusting it
/// or sending anything, and asks the user to confirm it.
async fn first_contact(
    url: &str,
    environment: &Environment,
    stderr: &mut dyn Write,
) -> Result<Trust, Failure> {
    let captured: tls::Captured = Arc::new(Mutex::new(None));
    let http = client::client_with(tls::capturing(Arc::clone(&captured)).map_err(Failure::other)?)?;
    let attempt = http.get(format!("{url}{}", GET_HEALTH.path)).send().await;
    let digest = captured.lock().ok().and_then(|digest| *digest);
    let Some(digest) = digest else {
        return Err(match attempt {
            Err(e) => client::unreachable(url, &e),
            Ok(_) => Failure::new(exit::UNREACHABLE, format!("no certificate from {url}")),
        });
    };
    let shown = tls::show(&digest);
    if !environment.stdin_is_terminal {
        return Err(Failure::usage(format!(
            "the primary at {url} presents the certificate {shown}, which is trusted only once \
             confirmed: compare it with the sha256: value a join command carries and pass \
             --fingerprint {shown}"
        )));
    }
    let _ = write!(
        stderr,
        "The primary at {url} presents the certificate\n  {shown}\n\
         Compare it with the sha256: value a join command carries. Trust it? [y/N] "
    );
    let _ = stderr.flush();
    let answer = environment
        .read_line()
        .map_err(|e| Failure::usage(format!("cannot read the answer: {e}")))?;
    if matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
        Ok(Trust::Pin(digest))
    } else {
        Err(Failure::usage(
            "the certificate was not trusted; nothing was written",
        ))
    }
}
