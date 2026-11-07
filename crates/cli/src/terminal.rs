//! The terminal: reading a secret without showing it, and the raw or quiet
//! mode of an interactive session (`session.rs`).
//!
//! The echo is turned off on stdin with rustix's termios calls (no `unsafe`,
//! no new crate), the line is read, and the previous settings are restored
//! whatever the read did. `ECHONL` keeps the newline the user types, so the
//! next line of output starts on a line of its own.

#[cfg(unix)]
pub fn read_hidden_line() -> std::io::Result<String> {
    use rustix::termios::{self, LocalModes, OptionalActions};
    use std::io::BufRead as _;
    let stdin = std::io::stdin();
    let original = termios::tcgetattr(&stdin)?;
    let mut hidden = original.clone();
    hidden.local_modes.remove(LocalModes::ECHO);
    hidden.local_modes.insert(LocalModes::ECHONL);
    termios::tcsetattr(&stdin, OptionalActions::Now, &hidden)?;
    let mut line = String::new();
    let read = stdin.lock().read_line(&mut line);
    let restored = termios::tcsetattr(&stdin, OptionalActions::Now, &original);
    read?;
    restored?;
    Ok(line.trim_end_matches(['\r', '\n']).to_string())
}

/// Without termios the line cannot be hidden, so it is not read at all.
#[cfg(not(unix))]
pub fn read_hidden_line() -> std::io::Result<String> {
    Err(std::io::Error::other(
        "this platform cannot hide what is typed; pass the token with --token-stdin",
    ))
}

/// How an interactive session sets the terminal up while it runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Every key goes to the remote program as it is typed, Ctrl-C
    /// included (`quoro ssh`).
    Raw,
    /// Keys are not echoed and are thrown away when the session ends; Ctrl-C
    /// still interrupts (`quoro vm console`, which sends nothing).
    Quiet,
}

/// The terminal settings a session changed, restored when dropped: on a
/// normal return, an error, a signal the session handles and a panic, since
/// the workspace unwinds.
#[cfg(unix)]
pub struct Guard {
    original: rustix::termios::Termios,
}

#[cfg(unix)]
impl Guard {
    /// Sets stdin's terminal to `mode`.
    pub fn enter(mode: Mode) -> std::io::Result<Self> {
        use rustix::termios::{self, LocalModes, OptionalActions};
        let stdin = std::io::stdin();
        let original = termios::tcgetattr(&stdin)?;
        let mut changed = original.clone();
        match mode {
            Mode::Raw => changed.make_raw(),
            Mode::Quiet => changed.local_modes.remove(LocalModes::ECHO),
        }
        termios::tcsetattr(&stdin, OptionalActions::Now, &changed)?;
        Ok(Self { original })
    }
}

#[cfg(unix)]
impl Drop for Guard {
    fn drop(&mut self) {
        use rustix::termios::{self, OptionalActions, QueueSelector};
        let stdin = std::io::stdin();
        // Keys typed into a console session were never meant for the shell
        // the user returns to.
        let _ = termios::tcflush(&stdin, QueueSelector::IFlush);
        let _ = termios::tcsetattr(&stdin, OptionalActions::Now, &self.original);
    }
}

/// Without termios the terminal is left as it is.
#[cfg(not(unix))]
pub struct Guard;

#[cfg(not(unix))]
impl Guard {
    pub fn enter(_mode: Mode) -> std::io::Result<Self> {
        Ok(Self)
    }
}

/// The size of the terminal on stdout, as rows and columns; `None` when it
/// cannot be read.
#[cfg(unix)]
pub fn size() -> Option<(u16, u16)> {
    let size = rustix::termios::tcgetwinsize(std::io::stdout()).ok()?;
    (size.ws_row > 0 && size.ws_col > 0).then_some((size.ws_row, size.ws_col))
}

#[cfg(not(unix))]
pub fn size() -> Option<(u16, u16)> {
    None
}
