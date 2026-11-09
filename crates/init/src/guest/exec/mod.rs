// SPDX-License-Identifier: AGPL-3.0-only
//! The exec channel the web terminal uses (`engine-trait.md` section 6,
//! ADR 0113): one vsock connection is one PTY session inside the app's root.
//!
//! The listener starts after the switch into the app's root, once the user and
//! the network are configured, and before the app itself, when the
//! configuration (v2) names an `exec_port`. It runs for the VM's lifetime in
//! its own threads and never writes to the app's console or its stdout: its
//! only diagnostics go to the serial log through [`diag`](crate::guest::diag).
//!
//! A session is:
//!
//! - one `OPEN` frame (JSON `argv`, optional `user`, `rows`, `cols`) read by
//!   [`session::serve`];
//! - a PTY allocated with `rustix::pty`, its window size set from the open;
//! - `/proc/self/exe --quoro-exec-helper <program> <arg0> <args...>` spawned
//!   with the PTY slave on stdin, stdout and stderr, the app's environment
//!   (`TERM=xterm-256color`), its working directory and the app's user unless
//!   the open names another. The helper calls `setsid`, takes the slave as its
//!   controlling terminal and `exec`s the program, because `pre_exec` is
//!   `unsafe` and the workspace forbids `unsafe`;
//! - two relay threads: host frames to the PTY master, and PTY output to
//!   `OUTPUT` frames with blocking writes (back-pressure, never an unbounded
//!   buffer);
//! - an `EXIT` frame when the program ends, or `ERROR` when the session
//!   cannot start or the host breaks the format.
//!
//! A connection whose first frame is a sync (ADR 0282) is a sync session
//! instead: [`session::serve`] runs `syncfs` on the mount of each volume it
//! names and answers `SYNCED` or `ERROR`, with no program and no PTY. The
//! engine sends it before it snapshots a volume. The syncs run on a thread of
//! their own and the session ends when the host hangs up, so a `syncfs` that
//! never returns does not keep a session slot.
//!
//! PID 1 reaps every child in its own `supervise` loop, so the session cannot
//! call `waitpid`: it registers the helper's pid with [`reaper`] and waits on
//! a channel that [`reaped`] feeds from that loop.

mod helper;
mod listen;
mod reaper;
mod session;

pub(crate) use helper::run_helper_if_requested;
pub(crate) use listen::start;
pub(crate) use reaper::reaped;
