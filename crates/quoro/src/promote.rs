//! `quoro cluster promote`: makes a machine with the `node` role the primary
//! when the primary is gone (`node-agent-protocol.md` section 7.3, ADR 0182,
//! ADR 0232, ADR 0261, issue #19).
//!
//! - [`command`] reads the command line and checks that it runs as root
//!   before it reads anything else. It never re-executes itself as the data
//!   directory's owner (`privilege.rs`): every step that opens a vote
//!   connection runs as root, because the mesh lets a new connection out
//!   only when one of the machine's own root processes opens it (section
//!   6.9).
//! - [`refusals`] runs every refusal of section 7.3 that reads the host and
//!   the agent's state, in the contract's order, and stops at the first that
//!   holds. [`keys`] holds the rule for the keys in use and opens their
//!   seals. Step 1 then prints the log position.
//! - [`steps`] runs the rest, steps 2 to 6: the refusals that read the whole
//!   followed log and step 6's checks, the vote pre-check (where `--dry-run`
//!   stops), the freeze, the real vote, the wait, the database build, the
//!   primary-status write, the keys, the start and the agent pointed at the
//!   new primary. Every vote connection is opened before step 6 hands the
//!   data directory to the control plane's user. [`host_steps`] holds the
//!   host's effects of those steps.
//! - [`install_keys`] holds the start of step 6: it writes the keys opened
//!   from this machine's seals where a primary keeps them, in the form its
//!   readers read; its pure check runs among the refusals before any change.
//!   [`prepare`] holds step 6's host preparation: the control plane's system
//!   user, the hand-over of its data directory, `quoro.toml`, its unit and
//!   its start, then pointing this machine's agent at it.
//! - `--abort` ends a freeze a stopped promotion left, through
//!   `quoro_node_agent::freeze::abort`, and is refused while a promotion runs
//!   on the host.

mod command;
mod host_steps;
mod install_keys;
mod keys;
pub mod prepare;
mod refusals;
mod steps;
#[cfg(test)]
mod tests;

pub use command::command;
