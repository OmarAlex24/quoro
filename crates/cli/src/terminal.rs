//! Reading a secret from the terminal without showing it.
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
