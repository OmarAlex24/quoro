//! `quoro login --agent --key-file PATH [--url URL] [--fingerprint FP]`.
//!
//! Saves a context (`config.toml`: URL and certificate pin) and its
//! credential (`credentials.toml`: the key file's absolute path, never the
//! key) and makes it the current context. The context is `--context`, else
//! `QUORO_CONTEXT`, else the current one, else `default`.
//!
//! Nothing is written until the login worked: the key file is checked (owner
//! only, as `quoro mcp stdio` checks it), an access token is minted from it
//! and `getMe` answers with the agent.
//!
//! Without `--fingerprint`, a new context's pin comes from a first contact: a
//! connection that records the certificate the primary presents and stops at
//! the handshake, so nothing is sent on it. The digest is shown and trusted
//! only when the user confirms it on a terminal; without a terminal the login
//! is refused and names `--fingerprint`. An existing context with the same
//! URL keeps its pin.

use crate::agent::{self, default_justification};
use crate::cli::{Global, LoginArgs};
use crate::client::{self, Answer, Call, Session};
use crate::context::{self, Context, ContextEntry, Credential, CredentialEntry, Trust};
use crate::error::{Failure, exit};
use crate::operations::{GET_HEALTH, GET_ME};
use crate::{Environment, tls};
use std::io::{BufRead as _, Write};
use std::sync::{Arc, Mutex};

/// The context a login without `--context` creates when none is current.
const DEFAULT_CONTEXT: &str = "default";

pub async fn run(
    global: &Global,
    args: LoginArgs,
    environment: &Environment,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    let Some(key_file) = args.key_file.filter(|_| args.agent) else {
        return Err(Failure::usage("quoro login needs --agent --key-file PATH"));
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

    let directory = context::config_dir(environment)?;
    let (config_path, mut config) = context::read_config(&directory)?;
    let mut credentials =
        context::read_credentials(&directory.join("credentials.toml"))?.unwrap_or_default();
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
    let trust = match (&args.fingerprint, existing) {
        (Some(fingerprint), _) => Trust::Pin(
            tls::parse_fingerprint(fingerprint)
                .map_err(|e| Failure::usage(format!("--fingerprint: {e}")))?,
        ),
        (None, Some(entry))
            if context::base_url(&entry.url).ok().as_deref() == Some(url.as_str()) =>
        {
            context::entry_trust(&config_path, &name, entry)?.1
        }
        (None, _) => first_contact(&url, environment, stderr).await?,
    };

    let context = Context {
        name: Some(name.clone()),
        url: url.clone(),
        trust,
        credential: Some(Credential::AgentKey(key_file)),
    };
    let reason = environment
        .justification(global)
        .unwrap_or_else(|| default_justification("login"));
    let mut session = Session::open(context, environment, &reason)?;
    // A fresh token, so the login proves the key works now.
    session.mint().await?;
    let me = Answer::read(session.send(&Call::get(GET_ME.path)).await?, &url)
        .await?
        .json()?;
    if me["type"] != "agent" {
        return Err(Failure::other(
            "the primary did not answer with an agent for this key; nothing was written",
        ));
    }

    let (fingerprint, tls) = match trust {
        Trust::Pin(pin) => (Some(tls::show(&pin)), None),
        Trust::System => (None, Some("system".to_string())),
    };
    credentials.contexts.insert(
        name.clone(),
        CredentialEntry {
            token: None,
            key_file: Some(key_file_text),
        },
    );
    context::write_credentials(&directory, &credentials)?;
    config.contexts.insert(
        name.clone(),
        ContextEntry {
            url: url.clone(),
            fingerprint,
            tls,
        },
    );
    config.current = Some(name.clone());
    context::write_config(&directory, &config)?;

    let agent = &me["agent"];
    let text = |key: &str| agent[key].as_str().unwrap_or("?").to_string();
    crate::api::write(
        stdout,
        format!(
            "Logged in to {name} ({url}) as agent {} (model {}); {name} is the current context.\n",
            text("name"),
            text("model")
        )
        .as_bytes(),
    )
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
    let mut answer = String::new();
    std::io::stdin()
        .lock()
        .read_line(&mut answer)
        .map_err(|e| Failure::usage(format!("cannot read the answer: {e}")))?;
    if matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
        Ok(Trust::Pin(digest))
    } else {
        Err(Failure::usage(
            "the certificate was not trusted; nothing was written",
        ))
    }
}
