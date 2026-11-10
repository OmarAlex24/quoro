//! The agent skill (`skills/quoro/`) against the binary it ships in.
//!
//! - The skill names only commands, flags and MCP tools that exist: every
//!   `quoro ...` command line in its code, every `--flag`, and every name
//!   shaped like an MCP tool (`get_app`) is checked against the command tree
//!   and the "MCP tools" table of `docs/contracts/README.md`.
//! - `references/commands.md` and `references/mcp-tools.md` are generated
//!   from those two sources and must match what the generators write now.
//!   `QUORO_BLESS=1 cargo test -p quoro-cli --test skill` rewrites them.
//! - The copy compiled into the binary (`quoro_cli::skill::FILES`) is exactly
//!   the files of the directory.
//! - The command-line client guide (`docs/guides/cli.md`) is held to the same
//!   name check, and its exit code table to `crates/cli/src/error.rs`.

use clap::{Arg, Command};
use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

const REGENERATE: &str = "QUORO_BLESS=1 cargo test -p quoro-cli --test skill";

/// The files that are generated, and so not read for names.
const GENERATED: [&str; 2] = ["references/commands.md", "references/mcp-tools.md"];

/// Host commands (`crates/quoro`, `quoro help host`) the skill may name,
/// with their flags.
const HOST_COMMANDS: &[(&str, &[&str])] = &[
    (
        "mcp stdio",
        &["context", "url", "fingerprint", "key-file", "justification"],
    ),
    ("audit verify", &["file", "db", "prev-hash"]),
    ("help host", &[]),
    ("version", &[]),
];

/// Other programs a code line may start with (`cargo build --release`); their
/// flags are not the client's, so a line with no `quoro` command in it is not
/// checked for flags.
const OTHER_PROGRAMS: &[&str] = &["cargo", "curl", "gh", "git", "tar"];

/// The first word of a name that is checked as an MCP tool.
const TOOL_VERBS: &[&str] = &[
    "get", "list", "create", "update", "delete", "restart", "redeploy", "resolve", "request",
    "start", "stop", "deploy", "set", "cancel", "rollback", "scale", "read",
];

fn repository() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn skill_dir() -> PathBuf {
    repository().join("skills/quoro")
}

/// The command-line client guide in the documentation.
const GUIDE: &str = "docs/guides/cli.md";

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The subcommands a user sees (clap's generated `help` left out).
fn subcommands(command: &Command) -> impl Iterator<Item = &Command> {
    command
        .get_subcommands()
        .filter(|sub| sub.get_name() != "help")
}

fn is_global(argument: &Arg) -> bool {
    argument.is_global_set()
}

fn is_generated(argument: &Arg) -> bool {
    matches!(argument.get_id().as_str(), "help" | "version")
}

/// The value placeholder of an option, `REF` or `[BUILD]`.
fn value_text(argument: &Arg) -> String {
    let name = argument
        .get_value_names()
        .map(|names| {
            names
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(" ")
        })
        .unwrap_or_else(|| argument.get_id().as_str().to_uppercase());
    let optional = argument
        .get_num_args()
        .is_some_and(|range| range.min_values() == 0);
    if optional { format!("[{name}]") } else { name }
}

/// One bullet of `commands.md`.
fn argument_line(argument: &Arg) -> String {
    let takes_value = argument.get_action().takes_values();
    let mut name = if argument.is_positional() {
        value_text(argument)
    } else {
        let long = format!("--{}", argument.get_long().unwrap_or_default());
        let mut name = match argument.get_short() {
            Some(short) => format!("-{short}, {long}"),
            None => long,
        };
        if takes_value {
            name.push(' ');
            name.push_str(&value_text(argument));
        }
        name
    };
    if argument.is_positional() && !argument.is_required_set() {
        name = format!("[{name}]");
    }
    let mut line = format!(
        "- `{name}`: {}",
        argument
            .get_help()
            .map(ToString::to_string)
            .unwrap_or_default()
    );
    if takes_value {
        let values: Vec<String> = argument
            .get_possible_values()
            .iter()
            .filter(|value| !value.is_hide_set())
            .map(|value| value.get_name().to_string())
            .collect();
        if !values.is_empty() {
            let _ = write!(line, ". One of: {}", values.join(", "));
        }
        let defaults: Vec<String> = argument
            .get_default_values()
            .iter()
            .map(|value| value.to_string_lossy().into_owned())
            .collect();
        if !defaults.is_empty() {
            let _ = write!(line, ". Default: {}", defaults.join(" "));
        }
    }
    if argument.is_required_set() && !argument.is_positional() {
        line.push_str(". Required");
    }
    line.push('\n');
    line
}

fn command_section(path: &str, command: &Command, out: &mut String) {
    let _ = writeln!(out, "## {path}\n");
    if let Some(about) = command.get_about() {
        let _ = writeln!(out, "{about}.\n");
    }
    let arguments: Vec<&Arg> = command
        .get_arguments()
        .filter(|argument| !is_global(argument) && !is_generated(argument))
        .collect();
    let children: Vec<&str> = subcommands(command).map(Command::get_name).collect();
    if !children.is_empty() {
        let _ = writeln!(out, "Subcommands: {}.\n", children.join(", "));
    }
    if !arguments.is_empty() {
        for argument in arguments {
            out.push_str(&argument_line(argument));
        }
        out.push('\n');
    }
    if let Some(after) = command.get_after_help() {
        let _ = writeln!(out, "{after}\n");
    }
    for child in subcommands(command) {
        command_section(&format!("{path} {}", child.get_name()), child, out);
    }
}

/// `references/commands.md`, from the command tree.
fn commands_reference() -> String {
    let tree = quoro_cli::command();
    let mut out = format!(
        "# Commands\n\n\
         Generated from the `quoro` command tree by `crates/cli/tests/skill.rs`; do not edit. \
         Regenerate with `{REGENERATE}`.\n\n\
         Host commands, which run on a cluster machine, are not listed: `quoro help host` prints \
         them.\n\n\
         ## Global flags\n\nEvery command accepts these.\n\n"
    );
    for argument in tree
        .get_arguments()
        .filter(|argument| is_global(argument) && !is_generated(argument))
    {
        out.push_str(&argument_line(argument));
    }
    out.push('\n');
    for command in subcommands(&tree) {
        command_section(&format!("quoro {}", command.get_name()), command, &mut out);
    }
    format!("{}\n", out.trim_end())
}

/// The rows of the README's "MCP tools" table, as written.
fn tool_rows() -> Vec<String> {
    let readme = read(&repository().join("docs/contracts/README.md"));
    let rows: Vec<String> = readme
        .lines()
        .skip_while(|line| line.trim_end() != "### MCP tools")
        .skip(1)
        .take_while(|line| !line.starts_with("### "))
        .filter(|line| line.starts_with("| `"))
        .map(str::to_string)
        .collect();
    assert!(!rows.is_empty(), "the README has an MCP tools table");
    rows
}

fn tool_names() -> BTreeSet<String> {
    tool_rows()
        .iter()
        .map(|row| row.split('`').nth(1).expect("a tool name").to_string())
        .collect()
}

/// `references/mcp-tools.md`, from the README's table.
fn mcp_tools_reference() -> String {
    let mut out = format!(
        "# MCP tools\n\n\
         Generated from the \"MCP tools\" table of `docs/contracts/README.md` by \
         `crates/cli/tests/skill.rs`; do not edit. Regenerate with `{REGENERATE}`.\n\n\
         The quoro MCP server exposes exactly these tools. Each calls the same operation as its \
         endpoint and needs the same scope. Every mutating tool takes a required `justification` \
         string and an optional `incident_id`. No tool decides an approval: `approval:decide` is \
         human-only.\n\n\
         | Tool | Endpoint | Scope |\n| --- | --- | --- |\n"
    );
    for row in tool_rows() {
        out.push_str(&row);
        out.push('\n');
    }
    out
}

#[test]
fn generated_references_are_current() {
    let bless = std::env::var("QUORO_BLESS").is_ok_and(|value| value == "1");
    for (file, expected) in [
        ("references/commands.md", commands_reference()),
        ("references/mcp-tools.md", mcp_tools_reference()),
    ] {
        let path = skill_dir().join(file);
        if bless {
            std::fs::write(&path, &expected).unwrap();
            continue;
        }
        let actual = read(&path);
        assert!(
            actual == expected,
            "skills/quoro/{file} is stale; regenerate it with `{REGENERATE}`"
        );
    }
}

/// The code of a Markdown text: the lines of fenced blocks and the inline
/// code spans.
fn code_fragments(text: &str) -> Vec<String> {
    let mut fragments = Vec::new();
    let mut fenced = false;
    for line in text.lines() {
        if line.trim_start().starts_with("```") {
            fenced = !fenced;
            continue;
        }
        if fenced {
            fragments.push(line.to_string());
        } else {
            fragments.extend(line.split('`').skip(1).step_by(2).map(str::to_string));
        }
    }
    fragments
}

/// Shell words, with quotes removed.
fn words(text: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut started = false;
    for c in text.chars() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some(_), c) => current.push(c),
            (None, '"' | '\'') => {
                quote = Some(c);
                started = true;
            }
            (None, c) if c.is_whitespace() => {
                if started || !current.is_empty() {
                    words.push(std::mem::take(&mut current));
                }
                started = false;
            }
            (None, c) => current.push(c),
        }
    }
    if started || !current.is_empty() {
        words.push(current);
    }
    words
}

/// Every long flag of the tree.
fn every_long(command: &Command, longs: &mut BTreeSet<String>) {
    for argument in command.get_arguments() {
        if let Some(long) = argument.get_long() {
            longs.insert(long.to_string());
        }
    }
    for sub in subcommands(command) {
        every_long(sub, longs);
    }
}

/// The argument of `command` (or a global) named by `flag`, `--long` or
/// `-s`.
fn find_flag<'a>(root: &'a Command, command: &'a Command, flag: &str) -> Option<&'a Arg> {
    let matches = |argument: &&Arg| match flag.strip_prefix("--") {
        Some(long) => argument.get_long() == Some(long),
        None => flag
            .strip_prefix('-')
            .filter(|short| short.chars().count() == 1)
            .and_then(|short| short.chars().next())
            .is_some_and(|short| argument.get_short() == Some(short)),
    };
    command
        .get_arguments()
        .find(matches)
        .or_else(|| root.get_arguments().filter(|a| is_global(a)).find(matches))
}

/// clap's help flag, which every command has.
fn is_help(word: &str) -> bool {
    matches!(word, "--help" | "-h")
}

/// Checks the command line that follows a `quoro` word.
fn check_command_line(root: &Command, rest: &[String]) -> Result<(), String> {
    let line = format!("quoro {}", rest.join(" "));
    let host = HOST_COMMANDS.iter().find(|(name, _)| {
        let name: Vec<&str> = name.split(' ').collect();
        rest.len() >= name.len()
            && rest
                .iter()
                .zip(&name)
                .all(|(word, part)| word.as_str() == *part)
    });
    if let Some((name, flags)) = host {
        for word in &rest[name.split(' ').count()..] {
            if let Some(flag) = word.strip_prefix("--") {
                let flag = flag.split('=').next().unwrap_or_default();
                if !flags.contains(&flag) {
                    return Err(format!("'{line}': the host command has no --{flag}"));
                }
            }
        }
        return Ok(());
    }
    let mut command = root;
    let mut words = rest
        .iter()
        .map(|w| w.trim_start_matches('[').trim_end_matches(']'));
    while let Some(word) = words.next() {
        if word.is_empty() || is_help(word) {
            continue;
        }
        // A placeholder (`quoro <command> --help`) stands for any command.
        if word.starts_with('<') && word.ends_with('>') {
            return Ok(());
        }
        if word.starts_with('-') && word.len() > 1 {
            let flag = word.split('=').next().unwrap_or_default();
            let Some(argument) = find_flag(root, command, flag) else {
                return Err(format!(
                    "'{line}': quoro {} has no {flag}",
                    command.get_name()
                ));
            };
            if argument.get_action().takes_values() && !word.contains('=') {
                // The value, unless the next word is a flag (an optional
                // value, such as `--build`'s).
                if words
                    .clone()
                    .next()
                    .is_some_and(|next| !next.starts_with('-'))
                {
                    words.next();
                }
            }
            continue;
        }
        if subcommands(command).next().is_some() {
            match subcommands(command).find(|sub| sub.get_name() == word) {
                Some(sub) => command = sub,
                None => {
                    return Err(format!(
                        "'{line}': '{word}' is not a subcommand of {}",
                        command.get_name()
                    ));
                }
            }
        }
        // Otherwise a positional value of a leaf command.
    }
    Ok(())
}

/// What in `text` names a command, a flag or a tool that does not exist.
fn unknown_names(
    text: &str,
    root: &Command,
    longs: &BTreeSet<String>,
    tools: &BTreeSet<String>,
) -> Vec<String> {
    let mut problems = Vec::new();
    for fragment in code_fragments(text) {
        let words = words(&fragment);
        let mut command_line = false;
        for (index, word) in words.iter().enumerate() {
            if word == "quoro" && index + 1 < words.len() {
                let end = words[index + 1..]
                    .iter()
                    .position(|w| matches!(w.as_str(), "|" | "||" | "&&" | ";" | ">" | "#"))
                    .map_or(words.len(), |p| index + 1 + p);
                if let Err(problem) = check_command_line(root, &words[index + 1..end]) {
                    problems.push(problem);
                }
                command_line = true;
            }
        }
        let other_program = words
            .first()
            .is_some_and(|first| OTHER_PROGRAMS.contains(&first.as_str()));
        if !command_line && !other_program {
            for word in &words {
                if let Some(flag) = word.strip_prefix("--").filter(|f| !f.is_empty()) {
                    let flag = flag.split('=').next().unwrap_or_default();
                    let host = HOST_COMMANDS.iter().any(|(_, flags)| flags.contains(&flag));
                    if !longs.contains(flag) && !host && flag != "help" {
                        problems.push(format!("`{fragment}`: no command has --{flag}"));
                    }
                }
            }
        }
        let trimmed = fragment.trim();
        let tool_shaped = trimmed.contains('_')
            && trimmed
                .split('_')
                .all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_lowercase()))
            && TOOL_VERBS.contains(&trimmed.split('_').next().unwrap_or_default());
        if tool_shaped && !tools.contains(trimmed) {
            problems.push(format!("`{trimmed}` is not an MCP tool"));
        }
    }
    problems
}

/// The skill's hand-written files.
fn checked_files() -> Vec<(String, String)> {
    quoro_cli::skill::FILES
        .iter()
        .filter(|(path, _)| !GENERATED.contains(path))
        .map(|(path, text)| ((*path).to_string(), (*text).to_string()))
        .collect()
}

#[test]
fn the_skill_names_only_commands_flags_and_tools_that_exist() {
    let root = quoro_cli::command();
    let mut longs = BTreeSet::new();
    every_long(&root, &mut longs);
    let tools = tool_names();
    let mut problems = Vec::new();
    for (path, text) in checked_files() {
        for problem in unknown_names(&text, &root, &longs, &tools) {
            problems.push(format!("skills/quoro/{path}: {problem}"));
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[test]
fn the_cli_guide_names_only_commands_flags_and_tools_that_exist() {
    let root = quoro_cli::command();
    let mut longs = BTreeSet::new();
    every_long(&root, &mut longs);
    let tools = tool_names();
    let text = read(&repository().join(GUIDE));
    let problems: Vec<String> = unknown_names(&text, &root, &longs, &tools)
        .into_iter()
        .map(|problem| format!("{GUIDE}: {problem}"))
        .collect();
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// The guide's exit code table has a row for every exit code of
/// `crates/cli/src/error.rs`, and no other.
#[test]
fn the_cli_guide_lists_every_exit_code() {
    use quoro_cli::exit;
    let text = read(&repository().join(GUIDE));
    let rows: BTreeSet<u8> = text
        .lines()
        .filter_map(|line| line.strip_prefix("| ")?.split(' ').next()?.parse().ok())
        .collect();
    let codes: BTreeSet<u8> = [
        exit::OK,
        exit::OTHER,
        exit::USAGE,
        exit::APPROVAL_REQUIRED,
        exit::FORBIDDEN,
        exit::NOT_FOUND,
        exit::CONFLICT,
        exit::UNREACHABLE,
    ]
    .into();
    assert_eq!(rows, codes, "{GUIDE} has one row per exit code");
}

/// The check itself catches a renamed command, a renamed flag and a tool
/// that does not exist.
#[test]
fn the_name_check_catches_what_does_not_exist() {
    let root = quoro_cli::command();
    let mut longs = BTreeSet::new();
    every_long(&root, &mut longs);
    let tools = tool_names();
    let check = |text: &str| unknown_names(text, &root, &longs, &tools);
    assert_eq!(
        check("Run `quoro app list`, then `get_app`; pass `--reason`."),
        Vec::<String>::new()
    );
    assert_eq!(
        check("```sh\nquoro logs -f --build --app web | head\nquoro mcp stdio --context lab\n```"),
        Vec::<String>::new()
    );
    assert_eq!(
        check("`quoro --context lab vm show web --json`"),
        Vec::<String>::new()
    );
    assert_eq!(
        check(
            "`quoro version`, `quoro app --help`, `quoro <command> --help`, \
             `cargo build --release --locked -p quoro`"
        ),
        Vec::<String>::new()
    );
    for wrong in [
        "`quoro app crete web`",
        "`quoro deploy --imag REF`",
        "`quoro logs -x`",
        "`quoro vm list --image REF`",
        "`quoro mcp stdio --key KEY`",
        "`quoro version --all`",
        "`quoro app --help --imag REF`",
        "`cargo build | quoro deploy --imag REF`",
        "pass `--wiat`",
        "`get_me`",
        "`get_quota`",
    ] {
        assert_eq!(check(wrong).len(), 1, "{wrong} is caught");
    }
}

#[test]
fn the_embedded_skill_is_the_repository_copy() {
    fn walk(root: &Path, directory: &Path, files: &mut BTreeSet<String>) {
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, files);
            } else {
                let relative = path.strip_prefix(root).unwrap();
                files.insert(
                    relative
                        .components()
                        .map(|c| c.as_os_str().to_string_lossy().into_owned())
                        .collect::<Vec<_>>()
                        .join("/"),
                );
            }
        }
    }
    let mut on_disk = BTreeSet::new();
    walk(&skill_dir(), &skill_dir(), &mut on_disk);
    let embedded: BTreeSet<String> = quoro_cli::skill::FILES
        .iter()
        .map(|(path, _)| (*path).to_string())
        .collect();
    assert_eq!(
        on_disk, embedded,
        "crates/cli/src/skill.rs lists exactly the files of skills/quoro/"
    );
    for (path, text) in quoro_cli::skill::FILES {
        assert_eq!(read(&skill_dir().join(path)), *text, "{path}");
    }
}

/// The Agent Skills format: frontmatter with `name` (the directory's name;
/// a-z, 0-9 and single hyphens; at most 64) and `description` (at most 1024
/// characters); the body stays short.
#[test]
fn the_skill_follows_the_agent_skills_format() {
    let text = read(&skill_dir().join("SKILL.md"));
    let mut lines = text.lines();
    assert_eq!(
        lines.next(),
        Some("---"),
        "SKILL.md starts with frontmatter"
    );
    let frontmatter: Vec<&str> = lines.by_ref().take_while(|line| *line != "---").collect();
    let field = |key: &str| {
        frontmatter
            .iter()
            .find_map(|line| line.strip_prefix(&format!("{key}: ")))
            .unwrap_or_else(|| panic!("the frontmatter has {key}"))
    };
    let name = field("name");
    assert_eq!(name, quoro_cli::skill::NAME);
    assert_eq!(skill_dir().file_name().unwrap(), name);
    assert!(name.len() <= 64 && !name.starts_with('-') && !name.ends_with('-'));
    assert!(!name.contains("--"));
    assert!(
        name.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    );
    let description = field("description");
    assert!(
        !description.is_empty() && description.chars().count() <= 1024,
        "{} characters",
        description.chars().count()
    );
    assert!(text.lines().count() < 500, "SKILL.md stays under 500 lines");
}

/// `errors.md` explains every exit code of `crates/cli/src/error.rs` and
/// every problem code and refusal reason of the contract.
#[test]
fn the_error_reference_covers_every_exit_code() {
    use quoro_cli::exit;
    let text = read(&skill_dir().join("references/errors.md"));
    for code in [
        exit::OK,
        exit::OTHER,
        exit::USAGE,
        exit::APPROVAL_REQUIRED,
        exit::FORBIDDEN,
        exit::NOT_FOUND,
        exit::CONFLICT,
        exit::UNREACHABLE,
    ] {
        assert!(
            text.lines()
                .any(|line| line.starts_with(&format!("| {code} |"))),
            "errors.md has a row for exit {code}"
        );
    }
    for code in [
        "unauthenticated",
        "forbidden",
        "approval_required",
        "not_found",
        "conflict",
        "payload_too_large",
        "validation_failed",
        "rate_limited",
        "not_primary",
        "node_unreachable",
        "internal",
        "human_only",
        "agent_disabled",
        "scope_missing",
        "selector_mismatch",
        "grant_expired",
        "grant_revoked",
        "grant_exhausted",
    ] {
        assert!(
            text.contains(&format!("| `{code}` |")),
            "errors.md has a row for {code}"
        );
    }
}
