//! The agent skill, compiled into the binary.
//!
//! `skills/quoro/` at the repository root is the skill in the Agent Skills
//! format (<https://agentskills.io/specification>): `SKILL.md` and its
//! `references/`. `quoro mcp install` writes this copy into an MCP client's
//! skills directory, so the skill always matches the binary that runs the
//! relay. `tests/skill.rs` checks that this list is exactly the files of
//! that directory, and that the skill names only commands, flags and MCP
//! tools that exist.

/// The skill's name: its directory and the `name` of its frontmatter.
pub const NAME: &str = "quoro";

/// Every file of the skill: its path inside the skill directory and its
/// text.
pub const FILES: &[(&str, &str)] = &[
    (
        "SKILL.md",
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../skills/quoro/SKILL.md"
        )),
    ),
    (
        "references/commands.md",
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../skills/quoro/references/commands.md"
        )),
    ),
    (
        "references/errors.md",
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../skills/quoro/references/errors.md"
        )),
    ),
    (
        "references/mcp-tools.md",
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../skills/quoro/references/mcp-tools.md"
        )),
    ),
    (
        "references/recipes.md",
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../skills/quoro/references/recipes.md"
        )),
    ),
];
