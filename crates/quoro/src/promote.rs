//! `quoro cluster promote`: makes a machine with the `node` role the primary
//! when the primary is gone (`node-agent-protocol.md` section 7.3, ADR 0182,
//! ADR 0232, ADR 0261, issue #19), built in layers.
//!
//! The command line, the refusals the command makes before it changes
//! anything, step 1 and `--abort`:
//!
//! - [`command`] reads the command line and checks that it runs as root
//!   before it reads anything else. It never re-executes itself as the data
//!   directory's owner (`privilege.rs`): every later step that opens a vote
//!   connection must run as root, because the mesh lets a new connection out
//!   only when one of the machine's own root processes opens it (section
//!   6.9).
//! - [`refusals`] runs every refusal of section 7.3, in the contract's order,
//!   and stops at the first that holds. [`keys`] holds the rule for the keys
//!   in use and opens their seals.
//! - Step 1 prints the log position. `--dry-run` stops after it; step 2's
//!   vote pre-check, after which a dry run stops in the contract, is not
//!   built yet.
//! - `--abort` ends a freeze a stopped promotion left, through
//!   `quoro_node_agent::freeze::abort`, and is refused while a promotion runs
//!   on the host.
//!
//! [`install_keys`] holds the start of step 6: it writes the keys opened
//! from this machine's seals where a primary keeps them, in the form its
//! readers read. Its refusals depend only on the keys and the files, and
//! its pure check is for the command to run among the refusals before any
//! change; it is not wired in yet. [`prepare`] holds step 6's host preparation: the control
//! plane's system user, the hand-over of its data directory, `quoro.toml`,
//! its unit and its start, then pointing this machine's agent at it.
//!
//! The command does not run steps 2 to 6 yet (the term, the freeze, the
//! database build, the primary-status move, the keys and the start): after
//! step 1 it says so and stops, having changed nothing.

mod command;
// The keys of step 6, which the command does not call yet.
#[allow(dead_code, reason = "quoro cluster promote (#19) does not call it yet")]
mod install_keys;
mod keys;
// Step 6's host preparation, which the command does not call yet.
#[allow(dead_code, reason = "quoro cluster promote (#19) does not call it yet")]
pub mod prepare;
mod refusals;
#[cfg(test)]
mod tests;

pub use command::command;
