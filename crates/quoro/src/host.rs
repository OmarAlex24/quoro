//! What the host commands that move a control plane on or off a host share:
//! `quoro node rejoin` (`node-agent-protocol.md` section 7.4) and the host
//! preparation of `quoro cluster promote` (section 7.3, step 6). The names of
//! the two units, of the control-plane database and of the agent's store, the
//! lines a command prints, and the host's `systemctl`.

use std::process::Output;

/// The control-plane unit.
pub const UNIT: &str = "quoro.service";
/// The node agent's unit.
pub const NODE_UNIT: &str = "quoro-node.service";
/// The control-plane database inside the data directory.
pub const DATABASE: &str = "quoro.db";
/// The node agent's store inside the node data directory.
pub const AGENT_STORE: &str = "node.db";

/// Runs `systemctl <args>` and returns what it printed and its status.
///
/// # Errors
///
/// A message when `systemctl` could not be run.
pub fn systemctl(args: &[&str]) -> Result<Output, String> {
    std::process::Command::new("systemctl")
        .args(args)
        .output()
        .map_err(|e| format!("cannot run systemctl {}: {e}", args.join(" ")))
}

/// Runs `systemctl <args>` and fails unless it succeeded.
///
/// # Errors
///
/// A message with the status and what `systemctl` printed on stderr.
pub fn systemctl_check(args: &[&str]) -> Result<(), String> {
    let output = systemctl(args)?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "systemctl {}: {}: {}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

/// The lines a command prints on stdout, kept for the tests.
pub struct Out {
    /// Every line, in order.
    pub lines: Vec<String>,
    echo: bool,
}

impl Out {
    /// Prints each line as it comes.
    #[must_use]
    pub fn printing() -> Self {
        Self {
            lines: Vec::new(),
            echo: true,
        }
    }

    /// Keeps the lines without printing them.
    #[cfg(test)]
    pub fn quiet() -> Self {
        Self {
            lines: Vec::new(),
            echo: false,
        }
    }

    /// Prints `line` (unless quiet) and keeps it.
    pub fn say(&mut self, line: impl Into<String>) {
        let line = line.into();
        if self.echo {
            println!("{line}");
        }
        self.lines.push(line);
    }
}
