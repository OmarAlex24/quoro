// SPDX-License-Identifier: AGPL-3.0-only
//! BuildKit progress parsing (ADR 0119).
//!
//! The builder VM's tagged output lines arrive here one at a time. A line is
//! one of:
//!
//! - a builder marker the guest script prints, `quoro-build phase=<phase>`
//!   ([`MARKER`]), which becomes a [`BuildEvent::Phase`];
//! - one BuildKit `--progress=rawjson` object (a `SolveStatus`: `vertexes`,
//!   `statuses`, `logs`, `warnings`), which becomes human log lines in the
//!   style of `--progress=plain` (`#6 [3/4] RUN ...`, `#6 0.187 <log text>`,
//!   `#6 DONE 0.2s`, `#6 ERROR: ...`), and updates what the parser knows
//!   about each vertex;
//! - anything else (BuildKit's final plain error with the Dockerfile excerpt,
//!   shell output), which is kept as a plain line.
//!
//! The guest is untrusted: every input is bounded and parse errors never
//! panic.

use quoro_proto::wire::{FailedStep, LogStream};

use super::BuildEvent;

/// The prefix of a marker line the builder's guest script prints.
pub const MARKER: &str = "quoro-build ";

/// Turns builder output into build events and remembers the failing step.
#[derive(Debug, Default)]
pub struct ProgressParser {}

impl ProgressParser {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Feeds one output line (without its newline) and returns the events it
    /// produced, in order.
    pub fn push(&mut self, _stream: LogStream, _line: &str) -> Vec<BuildEvent> {
        todo!("progress parser")
    }

    /// The step that failed, once BuildKit reported a vertex error: the
    /// vertex's number, its name, its error, the Dockerfile line from the
    /// plain error excerpt when there was one, and an excerpt (the step's last
    /// log lines and the Dockerfile excerpt), bounded by the limits in
    /// [`super`].
    #[must_use]
    pub fn failed_step(&self) -> Option<FailedStep> {
        todo!("progress parser")
    }
}
