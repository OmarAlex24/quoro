//! `quoro mcp install [--client claude|codex|cursor] [--scope user|project]`.
//!
//! Makes an MCP client run the relay, `quoro mcp stdio --context NAME` (the
//! host command in `crates/quoro`), for the context in use, and installs the
//! agent skill (`skill.rs`) where that client looks for skills. The relay
//! runs as an agent, so the context must hold an agent key file
//! (`quoro login --agent`) and pin the primary's certificate.
//!
//! The server entry is named `quoro`, one per configuration file: the skill
//! is written against that name, and installing another context replaces the
//! entry and says what it ran before. Its command is the absolute path of the
//! running binary.
//!
//! Where each client reads its configuration, as its documentation says:
//!
//! - Claude Code (<https://code.claude.com/docs/en/mcp>,
//!   <https://code.claude.com/docs/en/skills>): user scope, the top-level
//!   `mcpServers` of `~/.claude.json`; project scope, `mcpServers` in
//!   `.mcp.json` at the project root. Entries take `type`, `command`, `args`.
//!   Skills: `~/.claude/skills/<name>/` and `.claude/skills/<name>/`.
//! - Codex (<https://developers.openai.com/codex/mcp>, which redirects to
//!   <https://learn.chatgpt.com/docs/extend/mcp?surface=cli>, and
//!   <https://learn.chatgpt.com/docs/build-skills>): a `[mcp_servers.<name>]`
//!   table with `command` and `args` in `~/.codex/config.toml`, or in
//!   `.codex/config.toml` of a trusted project. Skills: `~/.agents/skills/`
//!   and `.agents/skills/`.
//! - Cursor (<https://cursor.com/docs/context/mcp>,
//!   <https://cursor.com/docs/context/skills>): `mcpServers` in
//!   `~/.cursor/mcp.json` or `.cursor/mcp.json`; entries take `type`,
//!   `command`, `args`. Skills: `~/.cursor/skills/` and `.cursor/skills/`.
//!
//! A configuration file is merged, never rewritten: only the `quoro` entry
//! changes. JSON is edited as text, so every other byte (key order, spacing)
//! stays as it was, and the result is parsed back and compared before it is
//! written; TOML is edited the same way, line by line, and checked the same
//! way with the `toml` parser.
//! The file is replaced in one rename and keeps its mode; a new one is `0600`
//! in the home directory and `0644` in a project, which is meant to be
//! committed. The skill directory `<skills>/quoro/` is replaced only when it
//! is a copy this command wrote (its `SKILL.md` names the skill `quoro`).
//! Running the command again changes nothing and says so.

use crate::cli::{Global, McpCommand, McpInstallArgs};
use crate::context::{self, Stored};
use crate::error::Failure;
use crate::{Environment, agent, links, skill};
use serde_json::{Value, json};
use std::io::Write;
use std::path::{Path, PathBuf};

/// The name of the server entry in every client's configuration.
const SERVER: &str = "quoro";

pub fn run(
    global: &Global,
    command: McpCommand,
    environment: &Environment,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    match command {
        McpCommand::Install(args) => install(global, &args, environment, stdout),
    }
}

/// An MCP client and where it keeps its files.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Client {
    Claude,
    Codex,
    Cursor,
}

impl Client {
    fn parse(name: &str) -> Result<Self, Failure> {
        match name {
            "claude" => Ok(Self::Claude),
            "codex" => Ok(Self::Codex),
            "cursor" => Ok(Self::Cursor),
            other => Err(Failure::usage(format!("unknown MCP client '{other}'"))),
        }
    }

    fn title(self) -> &'static str {
        match self {
            Self::Claude => "Claude Code",
            Self::Codex => "Codex",
            Self::Cursor => "Cursor",
        }
    }

    /// The configuration file under `base` (the home directory or the
    /// project folder).
    fn config_file(self, base: &Path, user: bool) -> PathBuf {
        match (self, user) {
            (Self::Claude, true) => base.join(".claude.json"),
            (Self::Claude, false) => base.join(".mcp.json"),
            (Self::Codex, _) => base.join(".codex/config.toml"),
            (Self::Cursor, _) => base.join(".cursor/mcp.json"),
        }
    }

    /// The directory that holds skill directories, under `base`.
    fn skills_dir(self, base: &Path) -> PathBuf {
        match self {
            Self::Claude => base.join(".claude/skills"),
            Self::Codex => base.join(".agents/skills"),
            Self::Cursor => base.join(".cursor/skills"),
        }
    }
}

fn install(
    global: &Global,
    args: &McpInstallArgs,
    environment: &Environment,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let client = Client::parse(&args.client)?;
    let user = args.scope == "user";
    let name = context_name(global, environment)?;
    let stored = context::stored(&context::config_dir(environment)?, &name)?;
    check_agent_context(&name, &stored)?;
    let executable = environment.executable()?;

    let base = if user {
        environment
            .var("HOME")
            .map(PathBuf::from)
            .filter(|home| home.is_absolute())
            .ok_or_else(|| Failure::usage("HOME is not set; it holds the client's files"))?
    } else {
        links::current_dir(environment)?
    };
    let config = resolve_link(&client.config_file(&base, user));
    let skill_dir = client.skills_dir(&base).join(skill::NAME);
    // Refused before anything is written.
    let skill_state = skill_state(&skill_dir)?;

    let command = executable.display().to_string();
    let arguments = ["mcp", "stdio", "--context", name.as_str()];
    let existing = read_existing(&config)?;
    let (text, previous) = match client {
        Client::Codex => toml_upsert(existing.as_deref().unwrap_or(""), &command, &arguments),
        Client::Claude | Client::Cursor => {
            let entry = json!({ "type": "stdio", "command": command, "args": arguments });
            json_upsert(existing.as_deref().unwrap_or(""), &entry)
                .map(|(text, previous)| (text, previous.as_ref().map(describe_entry)))
        }
    }
    .map_err(|e| {
        Failure::other(format!(
            "cannot merge into {}: {e}; nothing was written",
            config.display()
        ))
    })?;

    let scope = if user { "user scope" } else { "project scope" };
    let shown = config.display();
    let invocation = format!("{command} {}", arguments.join(" "));
    if existing.as_deref() == Some(text.as_str()) {
        let _ = writeln!(
            stdout,
            "MCP server '{SERVER}' for {} ({scope}): unchanged in {shown}",
            client.title()
        );
    } else {
        write_config(&config, &text, existing.is_some(), user)?;
        let what = match &previous {
            None => format!("added to {shown}"),
            Some(old) if *old == invocation => format!("updated in {shown}"),
            Some(old) => format!("replaced in {shown}; it ran {old}"),
        };
        let _ = writeln!(
            stdout,
            "MCP server '{SERVER}' for {} ({scope}): {what}",
            client.title()
        );
    }
    let _ = writeln!(stdout, "  runs {invocation}");

    let skill_shown = skill_dir.display();
    match skill_state {
        SkillState::Same => {
            let _ = writeln!(
                stdout,
                "Skill '{}': unchanged in {skill_shown}",
                skill::NAME
            );
        }
        SkillState::Absent | SkillState::Ours => {
            write_skill(&skill_dir)?;
            let verb = if skill_state == SkillState::Absent {
                "installed"
            } else {
                "replaced"
            };
            let _ = writeln!(
                stdout,
                "Skill '{}': {verb} in {skill_shown} ({} files)",
                skill::NAME,
                skill::FILES.len()
            );
        }
    }
    if client == Client::Codex && !user {
        let _ = writeln!(
            stdout,
            "Codex reads .codex/config.toml only in a project you trust."
        );
    }
    let _ = writeln!(
        stdout,
        "Start a new {} session to load them.",
        client.title()
    );
    Ok(())
}

/// The context to install: `--context`, else `QUORO_CONTEXT`, else
/// `current`. An ad-hoc context from `QUORO_URL` has no name a client could
/// run with, so it is refused.
fn context_name(global: &Global, environment: &Environment) -> Result<String, Failure> {
    if let Some(name) = global
        .context
        .as_deref()
        .or(environment.var("QUORO_CONTEXT"))
    {
        return Ok(name.to_string());
    }
    if environment.var("QUORO_URL").is_some() {
        return Err(Failure::usage(
            "QUORO_URL selects an ad-hoc context, which an MCP client cannot run with; \
             save one with quoro login --agent --key-file PATH --url URL and pass --context NAME",
        ));
    }
    context::current(&context::config_dir(environment)?)
}

/// The relay runs as an agent over a pinned connection: the context must
/// store an agent key file, readable by its owner only, and a fingerprint.
fn check_agent_context(name: &str, stored: &Stored) -> Result<(), Failure> {
    let login = format!("quoro login --agent --key-file PATH --context {name}");
    let Some(key_file) = &stored.key_file else {
        let holds = if stored.has_token {
            "holds a personal API token"
        } else {
            "has no credential"
        };
        return Err(Failure::usage(format!(
            "the context '{name}' {holds}; MCP runs as an agent: log in with {login}"
        )));
    };
    if stored.fingerprint.is_none() {
        return Err(Failure::usage(format!(
            "the context '{name}' trusts the system's CA store, and quoro mcp stdio pins the \
             primary's certificate: log in again with {login} --fingerprint sha256:..."
        )));
    }
    agent::read_agent_key(key_file).map_err(Failure::other)?;
    Ok(())
}

/// The file a symbolic link points to, so the link itself is kept.
fn resolve_link(path: &Path) -> PathBuf {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
        }
        _ => path.to_path_buf(),
    }
}

/// The file's text, or `None` when it does not exist.
fn read_existing(path: &Path) -> Result<Option<String>, Failure> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(Failure::other(format!(
            "cannot read {}: {e}",
            path.display()
        ))),
    }
}

/// Replaces `path` with `text`, keeping the mode of the file it replaces.
fn write_config(path: &Path, text: &str, exists: bool, user: bool) -> Result<(), Failure> {
    let mode = if exists {
        file_mode(path)?
    } else if user {
        0o600
    } else {
        0o644
    };
    let directory = path.parent().unwrap_or(Path::new("."));
    if !directory.exists() {
        if user {
            context::create_private_dir(directory)?;
        } else {
            std::fs::create_dir_all(directory).map_err(|e| {
                Failure::other(format!("cannot create {}: {e}", directory.display()))
            })?;
        }
    }
    context::replace_atomic(path, text.as_bytes(), mode)
}

fn file_mode(path: &Path) -> Result<u32, Failure> {
    let metadata = std::fs::metadata(path)
        .map_err(|e| Failure::other(format!("cannot read {}: {e}", path.display())))?;
    #[cfg(unix)]
    let mode = {
        use std::os::unix::fs::PermissionsExt as _;
        metadata.permissions().mode() & 0o7777
    };
    #[cfg(not(unix))]
    let mode = {
        let _ = metadata;
        0o644
    };
    Ok(mode)
}

/// `command args...` of a server entry, to say what a replaced entry ran.
fn describe_entry(entry: &Value) -> String {
    let mut words = vec![entry["command"].as_str().unwrap_or("?").to_string()];
    if let Some(args) = entry["args"].as_array() {
        words.extend(args.iter().map(|a| a.as_str().unwrap_or("?").to_string()));
    }
    words.join(" ")
}

/// What `<skills>/quoro` holds now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SkillState {
    Absent,
    /// A copy this command wrote, but not this binary's.
    Ours,
    /// Exactly this binary's copy.
    Same,
}

fn skill_state(directory: &Path) -> Result<SkillState, Failure> {
    let shown = directory.display();
    let foreign = |why: &str| {
        Failure::other(format!(
            "{shown} {why}, so it is not a copy quoro mcp install wrote; move it away and run \
             the command again; nothing was written"
        ))
    };
    let metadata = match std::fs::symlink_metadata(directory) {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(SkillState::Absent),
        Err(e) => return Err(Failure::other(format!("cannot read {shown}: {e}"))),
    };
    if metadata.file_type().is_symlink() {
        return Err(foreign("is a symbolic link"));
    }
    if !metadata.is_dir() {
        return Err(foreign("is not a directory"));
    }
    let manifest = std::fs::read_to_string(directory.join("SKILL.md")).unwrap_or_default();
    if frontmatter_name(&manifest) != Some(skill::NAME) {
        return Err(foreign("holds no SKILL.md named 'quoro'"));
    }
    let mut present = Vec::new();
    list_files(directory, directory, &mut present)
        .map_err(|e| Failure::other(format!("cannot read {shown}: {e}")))?;
    present.sort();
    let mut expected: Vec<(String, Vec<u8>)> = skill::FILES
        .iter()
        .map(|(path, text)| ((*path).to_string(), text.as_bytes().to_vec()))
        .collect();
    expected.sort();
    Ok(if present == expected {
        SkillState::Same
    } else {
        SkillState::Ours
    })
}

/// The `name` in a `SKILL.md`'s YAML frontmatter.
fn frontmatter_name(text: &str) -> Option<&str> {
    let mut lines = text.lines();
    if lines.next()?.trim_end() != "---" {
        return None;
    }
    lines
        .take_while(|line| line.trim_end() != "---")
        .find_map(|line| line.strip_prefix("name:"))
        .map(|value| value.trim().trim_matches(['"', '\'']))
}

/// Every file under `directory`, as a `/`-separated path relative to `root`
/// and its bytes.
fn list_files(
    root: &Path,
    directory: &Path,
    files: &mut Vec<(String, Vec<u8>)>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            list_files(root, &path, files)?;
        } else {
            let relative = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("/");
            files.push((relative, std::fs::read(&path)?));
        }
    }
    Ok(())
}

/// Writes the embedded skill to `target`, replacing the copy there: the new
/// copy is staged next to it and swapped in with renames.
fn write_skill(target: &Path) -> Result<(), Failure> {
    let parent = target.parent().unwrap_or(Path::new("."));
    let failed =
        |e: std::io::Error| Failure::other(format!("cannot write {}: {e}", target.display()));
    std::fs::create_dir_all(parent).map_err(failed)?;
    let pid = std::process::id();
    let staging = parent.join(format!(".{}.new.{pid}", skill::NAME));
    let old = parent.join(format!(".{}.old.{pid}", skill::NAME));
    let _ = std::fs::remove_dir_all(&staging);
    let staged = skill::FILES.iter().try_for_each(|(path, text)| {
        let file = staging.join(path);
        if let Some(directory) = file.parent() {
            std::fs::create_dir_all(directory)?;
        }
        std::fs::write(&file, text)
    });
    if let Err(e) = staged {
        let _ = std::fs::remove_dir_all(&staging);
        return Err(failed(e));
    }
    let swapped = if target.exists() {
        let _ = std::fs::remove_dir_all(&old);
        std::fs::rename(target, &old).and_then(|()| std::fs::rename(&staging, target))
    } else {
        std::fs::rename(&staging, target)
    };
    let _ = std::fs::remove_dir_all(&staging);
    let _ = std::fs::remove_dir_all(&old);
    swapped.map_err(failed)
}

/// Codex's `config.toml` with `[mcp_servers.quoro]` set to `command` and
/// `args`, and what the replaced entry ran.
///
/// The file is edited as text: the lines of our table (its header, its keys
/// and any `[mcp_servers.quoro.*]` subtable right after it) are replaced, or
/// the table is appended, and every other line stays as it was. The result
/// is parsed back and must equal the old document with that one table set,
/// so a layout this edit cannot follow (dotted keys, an inline table) is
/// refused instead of written.
fn toml_upsert(
    text: &str,
    command: &str,
    args: &[&str],
) -> Result<(String, Option<String>), String> {
    let invalid = |e: toml::de::Error| {
        let line = e
            .span()
            .map_or(1, |span| text[..span.start].matches('\n').count() + 1);
        format!("it is not valid TOML (line {line})")
    };
    let old: toml::Table = toml::from_str(text).map_err(invalid)?;
    let ours = old
        .get("mcp_servers")
        .and_then(|servers| servers.get(SERVER));
    let previous = ours.map(|entry| {
        let mut words = vec![
            entry
                .get("command")
                .and_then(toml::Value::as_str)
                .unwrap_or("?")
                .to_string(),
        ];
        if let Some(args) = entry.get("args").and_then(toml::Value::as_array) {
            words.extend(args.iter().map(|a| a.as_str().unwrap_or("?").to_string()));
        }
        words.join(" ")
    });

    let quoted: Vec<String> = args.iter().map(|a| context::toml_string(a)).collect();
    let block = format!(
        "[mcp_servers.{SERVER}]\ncommand = {}\nargs = [{}]\n",
        context::toml_string(command),
        quoted.join(", ")
    );
    let new_text = match table_lines(text, &["mcp_servers", SERVER]) {
        Some((start, end)) => format!("{}{block}{}", &text[..start], &text[end..]),
        None if text.trim().is_empty() => block,
        None => {
            let separator = if text.ends_with('\n') { "\n" } else { "\n\n" };
            format!("{text}{separator}{block}")
        }
    };

    let mut expected = old.clone();
    let servers = expected
        .entry("mcp_servers")
        .or_insert_with(|| toml::Value::Table(toml::Table::new()))
        .as_table_mut()
        .ok_or("mcp_servers is not a table")?;
    let mut entry = toml::Table::new();
    entry.insert("command".to_string(), toml::Value::from(command));
    entry.insert(
        "args".to_string(),
        toml::Value::Array(args.iter().map(|a| toml::Value::from(*a)).collect()),
    );
    servers.insert(SERVER.to_string(), toml::Value::Table(entry));
    if expected == old {
        // Already this entry, perhaps formatted another way: nothing to do.
        return Ok((text.to_string(), previous));
    }
    let merged: toml::Table = toml::from_str(&new_text)
        .map_err(|_| "the merged file would not be valid TOML; edit it by hand".to_string())?;
    if merged != expected {
        return Err("the merged file would differ in more than the quoro entry".to_string());
    }
    Ok((new_text, previous))
}

/// The byte range of the table `key` in a TOML text: from its header line to
/// the end of its last key line, with any subtables of it that follow; the
/// blank and comment lines before the next table are not part of it.
fn table_lines(text: &str, key: &[&str]) -> Option<(usize, usize)> {
    let mut start = None;
    let mut end = 0;
    let mut offset = 0;
    for line in text.split_inclusive('\n') {
        let line_start = offset;
        offset += line.len();
        let trimmed = line.trim();
        if let Some(header) = header_key(trimmed) {
            let inside = header.len() >= key.len() && header.iter().zip(key).all(|(a, b)| a == b);
            match (start, inside) {
                (None, true) if header.len() == key.len() => {
                    start = Some(line_start);
                    end = offset;
                }
                (Some(_), true) => end = offset,
                (Some(_), false) => break,
                _ => {}
            }
        } else if start.is_some() && !trimmed.is_empty() && !trimmed.starts_with('#') {
            end = offset;
        }
    }
    start.map(|start| (start, end))
}

/// The key of a `[table]` header line, its dotted parts unquoted; `None`
/// for any other line, an array of tables included.
fn header_key(line: &str) -> Option<Vec<String>> {
    let inner = line.strip_prefix('[')?;
    if inner.starts_with('[') {
        return None;
    }
    let mut parts = vec![String::new()];
    let mut chars = inner.chars();
    let mut closed = false;
    while let Some(c) = chars.next() {
        match c {
            ']' => {
                closed = true;
                break;
            }
            '.' => parts.push(String::new()),
            '"' => loop {
                match chars.next()? {
                    '"' => break,
                    '\\' => {
                        let escaped = chars.next()?;
                        parts.last_mut()?.push(escaped);
                    }
                    other => parts.last_mut()?.push(other),
                }
            },
            '\'' => loop {
                match chars.next()? {
                    '\'' => break,
                    other => parts.last_mut()?.push(other),
                }
            },
            c if c.is_whitespace() => {}
            c => parts.last_mut()?.push(c),
        }
    }
    let rest = chars.as_str().trim_start();
    (closed && (rest.is_empty() || rest.starts_with('#'))).then_some(parts)
}

/// A JSON configuration with `mcpServers.quoro` set to `entry`, and the
/// entry it replaced. Only that member's text changes; the result is parsed
/// back and must equal the old document with that one member set.
fn json_upsert(text: &str, entry: &Value) -> Result<(String, Option<Value>), String> {
    if text.trim().is_empty() {
        let document = json!({ "mcpServers": { SERVER: entry } });
        let pretty = serde_json::to_string_pretty(&document).map_err(|e| e.to_string())?;
        return Ok((format!("{pretty}\n"), None));
    }
    let old: Value = serde_json::from_str(text)
        .map_err(|e| format!("it is not valid JSON (line {})", e.line()))?;
    if !old.is_object() {
        return Err("it is not a JSON object".to_string());
    }
    let bytes = text.as_bytes();
    let top = skip_whitespace(bytes, 0);
    let (members, close) = object_members(text, top)?;
    let previous = old
        .get("mcpServers")
        .and_then(|servers| servers.get(SERVER))
        .cloned();
    let new_text = match members.iter().rev().find(|m| m.key == "mcpServers") {
        None => {
            let servers = json!({ SERVER: entry });
            insert_member(text, top, &members, close, "mcpServers", &servers)?
        }
        Some(servers) => {
            if bytes.get(servers.value_start) != Some(&b'{') {
                return Err("mcpServers is not an object".to_string());
            }
            let (inner, inner_close) = object_members(text, servers.value_start)?;
            match inner.iter().rev().find(|m| m.key == SERVER) {
                Some(ours) => {
                    let indent = line_indent(text, ours.key_start);
                    let rendered =
                        render(entry, &indent, multiline(text, servers.value_start, &inner))?;
                    format!(
                        "{}{rendered}{}",
                        &text[..ours.value_start],
                        &text[ours.value_end..]
                    )
                }
                None => insert_member(
                    text,
                    servers.value_start,
                    &inner,
                    inner_close,
                    SERVER,
                    entry,
                )?,
            }
        }
    };
    let mut expected = old.clone();
    let servers = expected
        .as_object_mut()
        .ok_or("it is not a JSON object")?
        .entry("mcpServers")
        .or_insert_with(|| json!({}));
    servers
        .as_object_mut()
        .ok_or("mcpServers is not an object")?
        .insert(SERVER.to_string(), entry.clone());
    if expected == old {
        // Already this entry, perhaps formatted another way: nothing to do.
        return Ok((text.to_string(), previous));
    }
    let merged: Value = serde_json::from_str(&new_text)
        .map_err(|_| "the merged file would not be valid JSON".to_string())?;
    if merged != expected {
        return Err("the merged file would differ in more than the quoro entry".to_string());
    }
    Ok((new_text, previous))
}

/// One member of a JSON object: its decoded key and where its key and value
/// sit in the text.
struct Member {
    key: String,
    key_start: usize,
    value_start: usize,
    value_end: usize,
}

/// The members of the object whose `{` is at `open`, and the index of its
/// `}`. The text is valid JSON (it was parsed first).
fn object_members(text: &str, open: usize) -> Result<(Vec<Member>, usize), String> {
    let bytes = text.as_bytes();
    let malformed = || "it is not valid JSON".to_string();
    let mut members = Vec::new();
    let mut at = skip_whitespace(bytes, open + 1);
    if bytes.get(at) == Some(&b'}') {
        return Ok((members, at));
    }
    loop {
        if bytes.get(at) != Some(&b'"') {
            return Err(malformed());
        }
        let key_end = string_end(bytes, at).ok_or_else(malformed)?;
        let key: String = serde_json::from_str(&text[at..key_end]).map_err(|_| malformed())?;
        let colon = skip_whitespace(bytes, key_end);
        if bytes.get(colon) != Some(&b':') {
            return Err(malformed());
        }
        let value_start = skip_whitespace(bytes, colon + 1);
        let value_end = value_end(bytes, value_start).ok_or_else(malformed)?;
        members.push(Member {
            key,
            key_start: at,
            value_start,
            value_end,
        });
        let next = skip_whitespace(bytes, value_end);
        match bytes.get(next) {
            Some(b',') => at = skip_whitespace(bytes, next + 1),
            Some(b'}') => return Ok((members, next)),
            _ => return Err(malformed()),
        }
    }
}

fn skip_whitespace(bytes: &[u8], mut at: usize) -> usize {
    while bytes.get(at).is_some_and(u8::is_ascii_whitespace) {
        at += 1;
    }
    at
}

/// The index after the string that starts at `at`.
fn string_end(bytes: &[u8], at: usize) -> Option<usize> {
    let mut i = at + 1;
    loop {
        match bytes.get(i)? {
            b'\\' => i += 2,
            b'"' => return Some(i + 1),
            _ => i += 1,
        }
    }
}

/// The index after the value that starts at `at`.
fn value_end(bytes: &[u8], at: usize) -> Option<usize> {
    match bytes.get(at)? {
        b'"' => string_end(bytes, at),
        b'{' | b'[' => {
            let mut depth = 0usize;
            let mut i = at;
            loop {
                match bytes.get(i)? {
                    b'"' => {
                        i = string_end(bytes, i)?;
                        continue;
                    }
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth -= 1;
                        if depth == 0 {
                            return Some(i + 1);
                        }
                    }
                    _ => {}
                }
                i += 1;
            }
        }
        _ => {
            let mut i = at;
            while bytes
                .get(i)
                .is_some_and(|b| !matches!(b, b',' | b'}' | b']') && !b.is_ascii_whitespace())
            {
                i += 1;
            }
            Some(i)
        }
    }
}

/// The spaces that start the line holding `at`.
fn line_indent(text: &str, at: usize) -> String {
    let start = text[..at].rfind('\n').map_or(0, |i| i + 1);
    text[start..]
        .chars()
        .take_while(|c| *c == ' ' || *c == '\t')
        .collect()
}

/// Whether the object at `open` is written over several lines.
fn multiline(text: &str, open: usize, members: &[Member]) -> bool {
    members
        .first()
        .is_none_or(|first| text[open..first.key_start].contains('\n'))
}

/// `value` as JSON: pretty, its lines indented to `indent`, or on one line.
fn render(value: &Value, indent: &str, pretty: bool) -> Result<String, String> {
    if pretty {
        let text = serde_json::to_string_pretty(value).map_err(|e| e.to_string())?;
        Ok(text.replace('\n', &format!("\n{indent}")))
    } else {
        serde_json::to_string(value).map_err(|e| e.to_string())
    }
}

/// Adds the member `key: value` at the end of the object at `open`, whose
/// `}` is at `close`, in the object's own layout.
fn insert_member(
    text: &str,
    open: usize,
    members: &[Member],
    close: usize,
    key: &str,
    value: &Value,
) -> Result<String, String> {
    let quoted = serde_json::to_string(key).map_err(|e| e.to_string())?;
    match members.last() {
        Some(last) => {
            let pretty = multiline(text, open, members);
            let addition = if pretty {
                let indent = line_indent(text, members[0].key_start);
                format!(",\n{indent}{quoted}: {}", render(value, &indent, true)?)
            } else {
                format!(", {quoted}: {}", render(value, "", false)?)
            };
            Ok(format!(
                "{}{addition}{}",
                &text[..last.value_end],
                &text[last.value_end..]
            ))
        }
        None => {
            let outer = line_indent(text, open);
            let indent = format!("{outer}  ");
            Ok(format!(
                "{}\n{indent}{quoted}: {}\n{outer}{}",
                &text[..=open],
                render(value, &indent, true)?,
                &text[close..]
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(context: &str) -> Value {
        json!({
            "type": "stdio",
            "command": "/usr/local/bin/quoro",
            "args": ["mcp", "stdio", "--context", context],
        })
    }

    #[test]
    fn json_is_merged_with_every_other_byte_kept() {
        let (text, previous) = json_upsert("", &entry("lab")).unwrap();
        assert!(previous.is_none());
        assert_eq!(
            serde_json::from_str::<Value>(&text).unwrap(),
            json!({ "mcpServers": { "quoro": entry("lab") } })
        );

        // A file with unrelated keys in an order serde_json would not keep.
        let original =
            "{\n  \"zeta\": 1,\n  \"numStartups\": 3,\n  \"alpha\": {\"x\": [1, \"}\\\"{\"]}\n}\n";
        let (text, _) = json_upsert(original, &entry("lab")).unwrap();
        assert!(text.starts_with("{\n  \"zeta\": 1,\n  \"numStartups\": 3,\n  \"alpha\": {\"x\": [1, \"}\\\"{\"]},\n  \"mcpServers\": {\n    \"quoro\": {\n      \"args\": [\n"), "{text}");
        assert!(text.ends_with("}\n"), "{text}");

        // Another server stays; ours is added after it.
        let original = "{\n    \"mcpServers\": {\n        \"other\": { \"command\": \"x\" }\n    },\n    \"b\": true\n}";
        let (text, _) = json_upsert(original, &entry("lab")).unwrap();
        assert!(
            text.contains(
                "\"other\": { \"command\": \"x\" },\n        \"quoro\": {\n          \"args\""
            ),
            "{text}"
        );
        assert!(text.ends_with("    },\n    \"b\": true\n}"), "{text}");

        // An entry that points elsewhere is replaced, and reported.
        let (again, previous) = json_upsert(&text, &entry("prod")).unwrap();
        assert_eq!(previous, Some(entry("lab")));
        assert!(again.contains("\"prod\""), "{again}");
        assert!(!again.contains("\"lab\""), "{again}");
        // Running it twice changes nothing.
        let (same, _) = json_upsert(&again, &entry("prod")).unwrap();
        assert_eq!(same, again);
        // The same entry in a client's own layout is left as it is.
        let reformatted = "{\"mcpServers\":{\"quoro\":{\"command\":\"/usr/local/bin/quoro\",\"args\":[\"mcp\",\"stdio\",\"--context\",\"lab\"],\"type\":\"stdio\"}}}";
        let (same, _) = json_upsert(reformatted, &entry("lab")).unwrap();
        assert_eq!(same, reformatted);

        // Empty objects, a one-line file, and things that are not ours.
        let (text, _) = json_upsert("{}", &entry("lab")).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&text).unwrap()["mcpServers"]["quoro"],
            entry("lab")
        );
        let (text, _) = json_upsert("{\"mcpServers\": {}}", &entry("lab")).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&text).unwrap()["mcpServers"]["quoro"],
            entry("lab")
        );
        let (text, _) =
            json_upsert("{\"a\": 1, \"mcpServers\": {\"b\": 2}}", &entry("lab")).unwrap();
        assert!(
            text.starts_with("{\"a\": 1, \"mcpServers\": {\"b\": 2, \"quoro\": {\"args\""),
            "{text}"
        );
        assert!(json_upsert("[1]", &entry("lab")).is_err());
        assert!(json_upsert("{\"mcpServers\": []}", &entry("lab")).is_err());
        assert!(json_upsert("{\"a\": ", &entry("lab")).is_err());
    }

    #[test]
    fn toml_keeps_comments_and_replaces_only_its_table() {
        let args = ["mcp", "stdio", "--context", "lab"];
        let (text, previous) = toml_upsert("", "/bin/quoro", &args).unwrap();
        assert!(previous.is_none());
        assert_eq!(
            text,
            "[mcp_servers.quoro]\ncommand = \"/bin/quoro\"\nargs = [\"mcp\", \"stdio\", \"--context\", \"lab\"]\n"
        );
        let original = "# my settings\nmodel = \"o3\" # pinned\n\n[mcp_servers.other]\ncommand = \"x\"\n\n[mcp_servers.quoro]\ncommand = \"/old/quoro\"\nargs = [\"mcp\", \"stdio\"]\n\n[mcp_servers.quoro.env]\nA = \"1\"\n\n[profiles.fast]\nmodel = \"o4\"\n";
        let (text, previous) = toml_upsert(original, "/bin/quoro", &args).unwrap();
        assert_eq!(previous.as_deref(), Some("/old/quoro mcp stdio"));
        assert!(text.starts_with("# my settings\nmodel = \"o3\" # pinned\n\n[mcp_servers.other]\ncommand = \"x\"\n\n[mcp_servers.quoro]\ncommand = \"/bin/quoro\""), "{text}");
        assert!(
            !text.contains("A = \"1\""),
            "the old entry is replaced whole: {text}"
        );
        assert!(text.contains("[profiles.fast]\nmodel = \"o4\""), "{text}");
        let (same, _) = toml_upsert(&text, "/bin/quoro", &args).unwrap();
        assert_eq!(same, text);
        let reformatted = "[mcp_servers.quoro]\nargs = [ 'mcp', 'stdio', '--context', 'lab' ]  # mine\ncommand = '/bin/quoro'\n";
        let (same, _) = toml_upsert(reformatted, "/bin/quoro", &args).unwrap();
        assert_eq!(same, reformatted);
        assert!(toml_upsert("mcp_servers = 1\n", "/bin/quoro", &args).is_err());
        assert!(toml_upsert("[a\n", "/bin/quoro", &args).is_err());

        // A quoted header with a comment is still ours; a file without a
        // final newline gets one.
        let (text, previous) = toml_upsert(
            "a = 1\n[ mcp_servers . \"quoro\" ] # old\ncommand = \"/old\"\n",
            "/bin/quoro",
            &args,
        )
        .unwrap();
        assert_eq!(previous.as_deref(), Some("/old"));
        assert!(text.starts_with("a = 1\n[mcp_servers.quoro]\n"), "{text}");
        let (text, _) = toml_upsert("a = 1", "/bin/quoro", &args).unwrap();
        assert!(text.starts_with("a = 1\n\n[mcp_servers.quoro]\n"), "{text}");
        // Layouts a line edit cannot follow are refused, never written.
        for layout in [
            "mcp_servers.quoro.command = \"/old\"\n",
            "[mcp_servers]\nquoro = { command = \"/old\" }\n",
            "[mcp_servers.quoro]\ncommand = \"/old\"\n[other]\n[mcp_servers.quoro.env]\nA = \"1\"\n",
        ] {
            assert!(
                toml_upsert(layout, "/bin/quoro", &args).is_err(),
                "{layout}"
            );
        }
        assert_eq!(
            header_key("[a.\"b.c\" . 'd'] # x"),
            Some(vec!["a".to_string(), "b.c".to_string(), "d".to_string()])
        );
        assert_eq!(header_key("[[a]]"), None);
        assert_eq!(header_key("a = [1]"), None);
    }

    #[test]
    fn the_skill_frontmatter_name_is_read() {
        assert_eq!(
            frontmatter_name("---\nname: quoro\ndescription: x\n---\n"),
            Some("quoro")
        );
        assert_eq!(
            frontmatter_name("---\ndescription: x\n---\nname: quoro\n"),
            None
        );
        assert_eq!(frontmatter_name("name: quoro\n"), None);
        assert_eq!(frontmatter_name(skill::FILES[0].1), Some(skill::NAME));
    }
}
