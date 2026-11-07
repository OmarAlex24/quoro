//! The surface budget of the client command tree.
//!
//! The client stays small enough to learn and for an agent to hold in its
//! context: a command exists because a task needs it, and everything else is
//! `quoro api`. These limits are the budget.
//!
//! Raising a limit is a pull request of its own that names the task needing
//! it and carries the maintainer's approval; never raise one to make a feature
//! fit.

use clap::Command;

/// Words directly under `quoro` (the generated `help` counts).
const MAX_TOP_LEVEL_WORDS: usize = 25;
/// Commands with no subcommand of their own.
const MAX_LEAF_COMMANDS: usize = 50;
/// Options every command accepts.
const MAX_GLOBAL_FLAGS: usize = 7;
/// Options of one command, positional arguments and globals excluded.
const MAX_OWN_FLAGS: usize = 5;
/// Lines of `quoro --help`.
const MAX_HELP_LINES: usize = 40;

/// The global flags, exactly.
const GLOBAL_FLAGS: [&str; 7] = [
    "context", "project", "app", "json", "yes", "reason", "incident",
];

/// The tree as a user sees it, with clap's generated `help` subcommand.
fn tree() -> Command {
    let mut command = quoro_cli::command();
    command.build();
    command
}

/// The help flag clap adds to every command is not a budgeted option.
fn generated(argument: &clap::Arg) -> bool {
    argument.get_id() == "help"
}

fn every_command(command: &Command) -> Vec<&Command> {
    let mut all = vec![command];
    for sub in command.get_subcommands() {
        all.extend(every_command(sub));
    }
    all
}

#[test]
fn top_level_words_fit() {
    let tree = tree();
    let words: Vec<&str> = tree.get_subcommands().map(Command::get_name).collect();
    assert!(
        words.len() <= MAX_TOP_LEVEL_WORDS,
        "{} top-level words, the budget is {MAX_TOP_LEVEL_WORDS}: {words:?}",
        words.len()
    );
}

#[test]
fn leaf_commands_fit() {
    let tree = tree();
    let leaves = every_command(&tree)
        .into_iter()
        .filter(|command| command.get_subcommands().next().is_none())
        .count();
    assert!(
        leaves <= MAX_LEAF_COMMANDS,
        "{leaves} leaf commands, the budget is {MAX_LEAF_COMMANDS}"
    );
}

#[test]
fn global_flags_are_exactly_the_seven() {
    let tree = tree();
    let globals: Vec<&str> = tree
        .get_arguments()
        .filter(|argument| argument.is_global_set() && !generated(argument))
        .map(|argument| argument.get_id().as_str())
        .collect();
    assert!(globals.len() <= MAX_GLOBAL_FLAGS, "{globals:?}");
    assert_eq!(globals, GLOBAL_FLAGS);
}

#[test]
fn every_command_fits_its_own_flags() {
    let tree = tree();
    for command in every_command(&tree) {
        let own: Vec<&str> = command
            .get_arguments()
            .filter(|argument| {
                !argument.is_positional() && !argument.is_global_set() && !generated(argument)
            })
            .map(|argument| argument.get_id().as_str())
            .collect();
        assert!(
            own.len() <= MAX_OWN_FLAGS,
            "'{}' has {} own flags, the budget is {MAX_OWN_FLAGS}: {own:?}",
            command.get_name(),
            own.len()
        );
    }
}

#[test]
fn top_level_help_fits_and_points_to_the_host_commands() {
    let help = tree().render_help().to_string();
    let lines = help.lines().count();
    assert!(
        lines <= MAX_HELP_LINES,
        "quoro --help is {lines} lines, the budget is {MAX_HELP_LINES}:\n{help}"
    );
    let pointers: Vec<&str> = help
        .lines()
        .filter(|line| line.contains("quoro help host"))
        .collect();
    assert_eq!(pointers.len(), 1, "{help}");
    assert_eq!(help.trim_end().lines().last(), pointers.first().copied());
}
