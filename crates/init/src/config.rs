// SPDX-License-Identifier: AGPL-3.0-only
//! The config disk: what the engine tells the init, and what the init reports
//! back. See `crates/init/README.md` for the byte layout and the grammar.
//!
//! INTERFACE FROZEN BY THE SUB-ORCHESTRATOR: the public items below (names,
//! fields, signatures, constants) are shared by the engine and the init. Do
//! not rename or change them; implement the bodies.

use std::fmt;
use std::net::Ipv4Addr;

/// virtio-blk serial of the read-only root filesystem (the prepared image).
pub const SERIAL_ROOT: &str = "quoro-root";
/// virtio-blk serial of the VM's writable data disk (ext4, overlay upper).
pub const SERIAL_DATA: &str = "quoro-data";
/// virtio-blk serial of the config disk (raw, this module's layout).
pub const SERIAL_CONFIG: &str = "quoro-config";

/// First line of an encoded configuration without an exec listener (M1-M3).
pub const CONFIG_MAGIC: &str = "quoro-init-config v1";
/// First line of an encoded configuration that carries `exec_port` (M4). A
/// configuration is encoded as v2 exactly when [`GuestConfig::exec_port`] is
/// set, so a v1 init never sees a key it does not know.
pub const CONFIG_MAGIC_V2: &str = "quoro-init-config v2";
/// The vsock port the engine asks the init to listen on for exec sessions
/// (`engine-trait.md` section 6, ADR 0113).
pub const EXEC_PORT: u32 = 1024;
/// First line of an encoded status record.
pub const STATUS_MAGIC: &str = "quoro-init-status v1";

/// Bytes reserved for the configuration at offset 0. The encoded text is
/// followed by NUL padding up to this length.
pub const CONFIG_REGION_LEN: u64 = 1 << 20;
/// Offset of the status record on the config disk.
pub const STATUS_OFFSET: u64 = CONFIG_REGION_LEN;
/// Bytes reserved for the status record. All zero means "no status yet".
pub const STATUS_REGION_LEN: u64 = 4096;
/// Total size of the config disk file the engine creates.
pub const CONFIG_DISK_LEN: u64 = CONFIG_REGION_LEN + STATUS_REGION_LEN;

/// The longest `stop_timeout_ms` the grammar accepts: one day.
const MAX_STOP_TIMEOUT_MS: u64 = 86_400_000;
/// Upper bound on the `hostname` value, in characters.
const MAX_HOSTNAME_CHARS: usize = 64;

/// Which guest console carries the app's tagged output lines.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogConsole {
    /// `/dev/hvc0`, the virtio-console (`ConsoleMode::VirtioConsole`).
    Virtio,
    /// `/dev/ttyS0`, the serial port (`ConsoleMode::Serial`).
    Serial,
    /// No console: the output is read and discarded (`ConsoleMode::Off`).
    None,
}

/// The guest's single network interface.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetConfig {
    /// MAC of the virtio-net device; the init finds the interface by it.
    pub mac: [u8; 6],
    pub address: Ipv4Addr,
    /// 0..=32.
    pub prefix_len: u8,
    pub gateway: Ipv4Addr,
}

/// Everything the init needs before it starts the app.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GuestConfig {
    /// The app's argv: the entrypoint followed by the command. Never empty,
    /// and `args[0]` is never empty.
    pub args: Vec<String>,
    /// The app's whole environment, in order. Names are non-empty and
    /// contain neither `=` nor NUL; values contain no NUL.
    pub env: Vec<(String, String)>,
    /// `user`, `uid`, `user:group`, `uid:gid` (any mix), resolved by the init
    /// against the image's `/etc/passwd` and `/etc/group`. `None` is root.
    pub user: Option<String>,
    /// Absolute path; `None` is `/`.
    pub workdir: Option<String>,
    pub hostname: Option<String>,
    pub log_console: LogConsole,
    pub net: Option<NetConfig>,
    /// Written to `/etc/resolv.conf` when non-empty.
    pub dns: Vec<Ipv4Addr>,
    /// After the power button: how long the init waits for the app to exit
    /// after `SIGTERM` before it sends `SIGKILL` and powers off.
    pub stop_timeout_ms: u64,
    /// The vsock port the init listens on for exec sessions (M4, config v2).
    /// `None` encodes a v1 configuration, and the init starts no listener.
    /// Never 0 and never `u32::MAX` (`VMADDR_PORT_ANY`).
    pub exec_port: Option<u32>,
}

/// What the init reports when the guest is about to power off.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppStatus {
    /// The app ran and exited. `code` is its exit status, or `128 + signal`
    /// when it was killed by a signal.
    Exited { code: i32 },
    /// The boot failed before or while starting the app (no config, bad
    /// user, missing entrypoint, unmountable disk...). `code` is non-zero:
    /// 127 when the entrypoint was not found or not executable, 1 otherwise.
    Fatal { code: i32, message: String },
}

/// A malformed configuration or status record, or one that cannot be encoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

impl GuestConfig {
    /// Validates and encodes the configuration. The result is at most
    /// [`CONFIG_REGION_LEN`] bytes and contains no NUL byte.
    ///
    /// # Errors
    ///
    /// Any field that breaks the rules documented on [`GuestConfig`], or an
    /// encoding longer than [`CONFIG_REGION_LEN`].
    pub fn encode(&self) -> Result<Vec<u8>, ConfigError> {
        validate(self)?;

        let mut out = String::new();
        out.push_str(if self.exec_port.is_some() {
            CONFIG_MAGIC_V2
        } else {
            CONFIG_MAGIC
        });
        out.push('\n');
        for arg in &self.args {
            push_line(&mut out, "arg", arg);
        }
        for (name, value) in &self.env {
            let mut pair = String::with_capacity(name.len() + value.len() + 1);
            pair.push_str(name);
            pair.push('=');
            pair.push_str(value);
            push_line(&mut out, "env", &pair);
        }
        if let Some(user) = &self.user {
            push_line(&mut out, "user", user);
        }
        if let Some(workdir) = &self.workdir {
            push_line(&mut out, "workdir", workdir);
        }
        if let Some(hostname) = &self.hostname {
            push_line(&mut out, "hostname", hostname);
        }
        push_line(&mut out, "console", console_name(self.log_console));
        if let Some(net) = &self.net {
            push_line(&mut out, "net.mac", &format_mac(net.mac));
            push_line(
                &mut out,
                "net.address",
                &format!("{}/{}", net.address, net.prefix_len),
            );
            push_line(&mut out, "net.gateway", &net.gateway.to_string());
        }
        for dns in &self.dns {
            push_line(&mut out, "dns", &dns.to_string());
        }
        push_line(
            &mut out,
            "stop_timeout_ms",
            &self.stop_timeout_ms.to_string(),
        );
        if let Some(port) = self.exec_port {
            push_line(&mut out, "exec_port", &port.to_string());
        }

        if out.len() as u64 > CONFIG_REGION_LEN {
            return Err(ConfigError(format!(
                "encoded configuration is {} bytes, more than the {} available",
                out.len(),
                CONFIG_REGION_LEN
            )));
        }
        Ok(out.into_bytes())
    }

    /// Decodes the configuration region: the bytes up to the first NUL (or
    /// the whole slice when there is none).
    ///
    /// # Errors
    ///
    /// A missing or wrong magic line, an unknown key, a missing required key,
    /// a repeated single-valued key, a bad escape or a value that fails the
    /// same validation as [`GuestConfig::encode`].
    pub fn decode(region: &[u8]) -> Result<Self, ConfigError> {
        let text = decode_text(region)?;
        let lines = split_lines(text)?;
        let (magic, body) = lines
            .split_first()
            .ok_or_else(|| ConfigError("configuration is empty".to_string()))?;
        let v2 = if *magic == CONFIG_MAGIC {
            false
        } else if *magic == CONFIG_MAGIC_V2 {
            true
        } else {
            return Err(ConfigError(format!(
                "line 1: expected magic {CONFIG_MAGIC:?} or {CONFIG_MAGIC_V2:?}, got {magic:?}"
            )));
        };

        let mut args: Vec<String> = Vec::new();
        let mut env: Vec<(String, String)> = Vec::new();
        let mut user: Option<(String, usize)> = None;
        let mut workdir: Option<(String, usize)> = None;
        let mut hostname: Option<(String, usize)> = None;
        let mut console: Option<LogConsole> = None;
        let mut net_mac: Option<([u8; 6], usize)> = None;
        let mut net_address: Option<(Ipv4Addr, u8, usize)> = None;
        let mut net_gateway: Option<(Ipv4Addr, usize)> = None;
        let mut dns: Vec<Ipv4Addr> = Vec::new();
        let mut stop_timeout_ms: Option<u64> = None;
        let mut exec_port: Option<u32> = None;

        for (offset, line) in body.iter().enumerate() {
            let line_no = offset + 2;
            if line.is_empty() {
                return Err(line_error(line_no, "blank line".to_string()));
            }
            let (key, raw) = line
                .split_once('=')
                .ok_or_else(|| line_error(line_no, format!("expected key=value, got {line:?}")))?;
            match key {
                "arg" => {
                    let value = unescape(raw, line_no)?;
                    no_nul(&value, "arg", line_no)?;
                    args.push(value);
                }
                "env" => {
                    let pair = unescape(raw, line_no)?;
                    if pair.contains('\0') {
                        return Err(line_error(line_no, "env value contains a NUL".to_string()));
                    }
                    let (name, value) = pair.split_once('=').ok_or_else(|| {
                        line_error(line_no, format!("env is missing '=': {raw:?}"))
                    })?;
                    validate_env_name(name).map_err(|e| line_error(line_no, e))?;
                    env.push((name.to_string(), value.to_string()));
                }
                "user" => {
                    single(&user, line_no, "user")?;
                    let value = unescape(raw, line_no)?;
                    no_nul(&value, "user", line_no)?;
                    validate_user(&value).map_err(|e| line_error(line_no, e))?;
                    user = Some((value, line_no));
                }
                "workdir" => {
                    single(&workdir, line_no, "workdir")?;
                    let value = unescape(raw, line_no)?;
                    no_nul(&value, "workdir", line_no)?;
                    validate_workdir(&value).map_err(|e| line_error(line_no, e))?;
                    workdir = Some((value, line_no));
                }
                "hostname" => {
                    single(&hostname, line_no, "hostname")?;
                    let value = unescape(raw, line_no)?;
                    no_nul(&value, "hostname", line_no)?;
                    validate_hostname(&value).map_err(|e| line_error(line_no, e))?;
                    hostname = Some((value, line_no));
                }
                "console" => {
                    let value = unescape(raw, line_no)?;
                    let parsed = parse_console(&value).map_err(|e| line_error(line_no, e))?;
                    if console.replace(parsed).is_some() {
                        return Err(line_error(line_no, "duplicate console".to_string()));
                    }
                }
                "net.mac" => {
                    if net_mac.is_some() {
                        return Err(line_error(line_no, "duplicate net.mac".to_string()));
                    }
                    let value = unescape(raw, line_no)?;
                    let mac = parse_mac(&value).map_err(|e| line_error(line_no, e))?;
                    net_mac = Some((mac, line_no));
                }
                "net.address" => {
                    if net_address.is_some() {
                        return Err(line_error(line_no, "duplicate net.address".to_string()));
                    }
                    let value = unescape(raw, line_no)?;
                    let (address, prefix_len) =
                        parse_cidr(&value).map_err(|e| line_error(line_no, e))?;
                    net_address = Some((address, prefix_len, line_no));
                }
                "net.gateway" => {
                    if net_gateway.is_some() {
                        return Err(line_error(line_no, "duplicate net.gateway".to_string()));
                    }
                    let value = unescape(raw, line_no)?;
                    let gateway =
                        parse_ipv4(&value, "net.gateway").map_err(|e| line_error(line_no, e))?;
                    net_gateway = Some((gateway, line_no));
                }
                "dns" => {
                    let value = unescape(raw, line_no)?;
                    let ip = parse_ipv4(&value, "dns").map_err(|e| line_error(line_no, e))?;
                    dns.push(ip);
                }
                "stop_timeout_ms" => {
                    if stop_timeout_ms.is_some() {
                        return Err(line_error(line_no, "duplicate stop_timeout_ms".to_string()));
                    }
                    let value = unescape(raw, line_no)?;
                    let parsed = value.parse::<u64>().map_err(|_| {
                        line_error(
                            line_no,
                            format!("stop_timeout_ms is not a number: {value:?}"),
                        )
                    })?;
                    if parsed > MAX_STOP_TIMEOUT_MS {
                        return Err(line_error(
                            line_no,
                            format!(
                                "stop_timeout_ms {parsed} is more than the maximum \
                                 {MAX_STOP_TIMEOUT_MS}"
                            ),
                        ));
                    }
                    stop_timeout_ms = Some(parsed);
                }
                "exec_port" if v2 => {
                    if exec_port.is_some() {
                        return Err(line_error(line_no, "duplicate exec_port".to_string()));
                    }
                    let value = unescape(raw, line_no)?;
                    let parsed = value.parse::<u32>().map_err(|_| {
                        line_error(line_no, format!("exec_port is not a port: {value:?}"))
                    })?;
                    exec_port = Some(parsed);
                }
                other => {
                    return Err(line_error(line_no, format!("unknown key {other:?}")));
                }
            }
        }

        let net = match (net_mac, net_address, net_gateway) {
            (None, None, None) => None,
            (Some((mac, _)), Some((address, prefix_len, _)), Some((gateway, _))) => {
                Some(NetConfig {
                    mac,
                    address,
                    prefix_len,
                    gateway,
                })
            }
            (mac, address, gateway) => {
                let (missing, first_line) = if gateway.is_none() {
                    ("net.gateway", mac.map_or(2, |(_, line)| line))
                } else if address.is_none() {
                    ("net.address", mac.map_or(2, |(_, line)| line))
                } else {
                    ("net.mac", address.map_or(2, |(_, _, line)| line))
                };
                return Err(line_error(
                    first_line,
                    format!("net.* group is incomplete: {missing} is missing"),
                ));
            }
        };

        let console = console.ok_or_else(|| ConfigError("missing console".to_string()))?;
        let stop_timeout_ms =
            stop_timeout_ms.ok_or_else(|| ConfigError("missing stop_timeout_ms".to_string()))?;
        if v2 && exec_port.is_none() {
            return Err(ConfigError("missing exec_port".to_string()));
        }

        let config = GuestConfig {
            args,
            env,
            user: user.map(|(value, _)| value),
            workdir: workdir.map(|(value, _)| value),
            hostname: hostname.map(|(value, _)| value),
            log_console: console,
            net,
            dns,
            stop_timeout_ms,
            exec_port,
        };
        validate(&config)?;
        Ok(config)
    }
}

/// Encodes a status record. The result is at most [`STATUS_REGION_LEN`]
/// bytes; a `Fatal` message is truncated (on a char boundary) to fit.
#[must_use]
pub fn encode_status(status: &AppStatus) -> Vec<u8> {
    let mut out = String::new();
    out.push_str(STATUS_MAGIC);
    out.push('\n');
    match status {
        AppStatus::Exited { code } => {
            out.push_str(&format!("exit={code}\n"));
        }
        AppStatus::Fatal { code, message } => {
            // A NUL in the message would end the record early on the reader's
            // side; the init's own messages never contain one, but the record
            // must stay valid whatever it is handed.
            let cleaned: String = message.chars().filter(|ch| *ch != '\0').collect();
            let prefix = format!("{STATUS_MAGIC}\nexit={code}\nfatal=");
            let suffix = "\n";
            // One byte stays free so the record is always NUL-terminated
            // inside its region.
            let limit = STATUS_REGION_LEN as usize;
            let budget = limit.saturating_sub(1 + prefix.len() + suffix.len());
            let kept = truncate_escaped(&cleaned, budget);
            out.clear();
            out.push_str(&prefix);
            out.push_str(&escape(&kept));
            out.push_str(suffix);
        }
    }
    let mut bytes = out.into_bytes();
    if bytes.len() + 1 > STATUS_REGION_LEN as usize {
        bytes.truncate(STATUS_REGION_LEN as usize - 1);
    }
    bytes.push(0);
    bytes
}

/// Decodes the status region. A region that is all zero, or empty, is
/// `Ok(None)`: the guest has not reported anything.
///
/// # Errors
///
/// A non-zero region without the status magic, or with a malformed record.
pub fn decode_status(region: &[u8]) -> Result<Option<AppStatus>, ConfigError> {
    if region.iter().all(|byte| *byte == 0) {
        return Ok(None);
    }
    let text = decode_text(region)?;
    if text.is_empty() {
        return Ok(None);
    }
    let lines = split_lines(text)?;
    let (magic, body) = lines
        .split_first()
        .ok_or_else(|| ConfigError("status record is empty".to_string()))?;
    if *magic != STATUS_MAGIC {
        return Err(ConfigError(format!(
            "line 1: expected magic {STATUS_MAGIC:?}, got {magic:?}"
        )));
    }

    let mut code: Option<i32> = None;
    let mut message: Option<String> = None;
    for (offset, line) in body.iter().enumerate() {
        let line_no = offset + 2;
        if line.is_empty() {
            return Err(line_error(line_no, "blank line".to_string()));
        }
        let (key, raw) = line
            .split_once('=')
            .ok_or_else(|| line_error(line_no, format!("expected key=value, got {line:?}")))?;
        match key {
            "exit" => {
                if code.is_some() {
                    return Err(line_error(line_no, "duplicate exit".to_string()));
                }
                let value = unescape(raw, line_no)?;
                let parsed = value
                    .parse::<i32>()
                    .map_err(|_| line_error(line_no, format!("exit is not a number: {value:?}")))?;
                code = Some(parsed);
            }
            "fatal" => {
                if message.is_some() {
                    return Err(line_error(line_no, "duplicate fatal".to_string()));
                }
                message = Some(unescape(raw, line_no)?);
            }
            other => return Err(line_error(line_no, format!("unknown key {other:?}"))),
        }
    }

    let code = code.ok_or_else(|| ConfigError("missing exit".to_string()))?;
    Ok(Some(match message {
        Some(message) => AppStatus::Fatal { code, message },
        None => AppStatus::Exited { code },
    }))
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

/// Appends one `key=escaped(value)` line. Arguments are validated first, so
/// the escaped text can never contain a newline of its own.
fn push_line(out: &mut String, key: &str, value: &str) {
    out.push_str(key);
    out.push('=');
    out.push_str(&escape(value));
    out.push('\n');
}

/// Escapes the two bytes the line grammar reserves: backslash and newline.
fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

/// Reverses [`escape`], rejecting any other `\x` sequence.
fn unescape(value: &str, line_no: usize) -> Result<String, ConfigError> {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('\\') => out.push('\\'),
            Some('n') => out.push('\n'),
            Some(other) => {
                return Err(line_error(
                    line_no,
                    format!("unknown escape sequence \\{other}"),
                ));
            }
            None => return Err(line_error(line_no, "trailing lone backslash".to_string())),
        }
    }
    Ok(out)
}

/// The escaped form of `message`, cut so it uses at most `budget` bytes and
/// only whole characters (never half an escape pair).
fn truncate_escaped(message: &str, budget: usize) -> String {
    let mut kept = String::new();
    let mut used = 0;
    for ch in message.chars() {
        let cost = match ch {
            '\\' | '\n' => 2,
            other => other.len_utf8(),
        };
        if used + cost > budget {
            break;
        }
        kept.push(ch);
        used += cost;
    }
    kept
}

fn console_name(console: LogConsole) -> &'static str {
    match console {
        LogConsole::Virtio => "virtio",
        LogConsole::Serial => "serial",
        LogConsole::None => "none",
    }
}

fn parse_console(value: &str) -> Result<LogConsole, String> {
    match value {
        "virtio" => Ok(LogConsole::Virtio),
        "serial" => Ok(LogConsole::Serial),
        "none" => Ok(LogConsole::None),
        other => Err(format!(
            "console must be virtio, serial or none, got {other:?}"
        )),
    }
}

fn format_mac(mac: [u8; 6]) -> String {
    format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
    )
}

fn parse_mac(value: &str) -> Result<[u8; 6], String> {
    let parts: Vec<&str> = value.split(':').collect();
    if parts.len() != 6 {
        return Err(format!(
            "mac must have six colon-separated octets: {value:?}"
        ));
    }
    let mut mac = [0u8; 6];
    for (index, part) in parts.iter().enumerate() {
        if part.len() != 2 {
            return Err(format!("mac octet {part:?} is not two hex digits"));
        }
        let byte = u8::from_str_radix(part, 16)
            .map_err(|_| format!("mac octet {part:?} is not two hex digits"))?;
        mac[index] = byte;
    }
    Ok(mac)
}

fn parse_ipv4(value: &str, key: &str) -> Result<Ipv4Addr, String> {
    value
        .parse::<Ipv4Addr>()
        .map_err(|_| format!("{key} is not an IPv4 address: {value:?}"))
}

fn parse_cidr(value: &str) -> Result<(Ipv4Addr, u8), String> {
    let (address, prefix) = value
        .split_once('/')
        .ok_or_else(|| format!("net.address must be address/prefix: {value:?}"))?;
    let address = parse_ipv4(address, "net.address")?;
    let prefix_len = prefix
        .parse::<u8>()
        .map_err(|_| format!("net.address prefix is not a number: {prefix:?}"))?;
    if prefix_len > 32 {
        return Err(format!("net.address prefix {prefix_len} is more than 32"));
    }
    Ok((address, prefix_len))
}

/// Reads the text of a region: everything up to the first NUL.
fn decode_text(region: &[u8]) -> Result<&str, ConfigError> {
    let cut = region.split(|byte| *byte == 0).next().unwrap_or(&[]);
    std::str::from_utf8(cut).map_err(|error| {
        let valid = &cut[..error.valid_up_to()];
        let line_no = 1 + valid.iter().filter(|byte| **byte == b'\n').count();
        line_error(line_no, "invalid UTF-8".to_string())
    })
}

/// Splits the region text into lines. Every line must end with `\n`; a final
/// unterminated line is an error, and so is a region that is not text at all.
fn split_lines(text: &str) -> Result<Vec<&str>, ConfigError> {
    let mut lines = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        match rest.find('\n') {
            Some(position) => {
                lines.push(&rest[..position]);
                rest = &rest[position + 1..];
            }
            None => {
                return Err(line_error(
                    lines.len() + 1,
                    "line does not end with a newline".to_string(),
                ));
            }
        }
    }
    Ok(lines)
}

fn line_error(line_no: usize, message: String) -> ConfigError {
    ConfigError(format!("line {line_no}: {message}"))
}

/// Rejects a second occurrence of a single-valued key.
fn single<T>(slot: &Option<T>, line_no: usize, key: &str) -> Result<(), ConfigError> {
    if slot.is_some() {
        return Err(line_error(line_no, format!("duplicate {key}")));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Validation shared by encode and decode
// ---------------------------------------------------------------------------

fn validate(config: &GuestConfig) -> Result<(), ConfigError> {
    if config.args.is_empty() {
        return Err(ConfigError(
            "missing arg: at least one is required".to_string(),
        ));
    }
    for (index, arg) in config.args.iter().enumerate() {
        no_nul_field(arg, "arg")?;
        if index == 0 && arg.is_empty() {
            return Err(ConfigError("args[0] is empty".to_string()));
        }
    }
    for (name, value) in &config.env {
        no_nul_field(name, "env name")?;
        no_nul_field(value, "env value")?;
        validate_env_name(name).map_err(|e| ConfigError(format!("env name {name:?}: {e}")))?;
    }
    if let Some(user) = &config.user {
        no_nul_field(user, "user")?;
        validate_user(user).map_err(|e| ConfigError(format!("user {user:?}: {e}")))?;
    }
    if let Some(workdir) = &config.workdir {
        no_nul_field(workdir, "workdir")?;
        validate_workdir(workdir).map_err(|e| ConfigError(format!("workdir {workdir:?}: {e}")))?;
    }
    if let Some(hostname) = &config.hostname {
        no_nul_field(hostname, "hostname")?;
        validate_hostname(hostname)
            .map_err(|e| ConfigError(format!("hostname {hostname:?}: {e}")))?;
    }
    if let Some(net) = &config.net
        && net.prefix_len > 32
    {
        return Err(ConfigError(format!(
            "net prefix length {} is more than 32",
            net.prefix_len
        )));
    }
    if config.stop_timeout_ms > MAX_STOP_TIMEOUT_MS {
        return Err(ConfigError(format!(
            "stop_timeout_ms {} is more than the maximum {MAX_STOP_TIMEOUT_MS}",
            config.stop_timeout_ms
        )));
    }
    if let Some(port) = config.exec_port
        && (port == 0 || port == u32::MAX)
    {
        return Err(ConfigError(format!(
            "exec_port {port} is not a listening vsock port"
        )));
    }
    Ok(())
}

fn no_nul_field(value: &str, field: &str) -> Result<(), ConfigError> {
    if value.contains('\0') {
        return Err(ConfigError(format!("{field} contains a NUL byte")));
    }
    Ok(())
}

fn no_nul(value: &str, field: &str, line_no: usize) -> Result<(), ConfigError> {
    if value.contains('\0') {
        return Err(line_error(line_no, format!("{field} contains a NUL byte")));
    }
    Ok(())
}

fn validate_env_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("name is empty".to_string());
    }
    if name.contains('=') {
        return Err("name contains '='".to_string());
    }
    Ok(())
}

fn validate_user(user: &str) -> Result<(), String> {
    if user.is_empty() {
        return Err("is empty".to_string());
    }
    if user.chars().any(char::is_whitespace) {
        return Err("contains whitespace".to_string());
    }
    let mut parts = user.split(':');
    let left = parts.next().unwrap_or("");
    let right = parts.next();
    if parts.next().is_some() {
        return Err("has more than one ':'".to_string());
    }
    if left.is_empty() {
        return Err("the user side is empty".to_string());
    }
    if right.is_some_and(str::is_empty) {
        return Err("the group side is empty".to_string());
    }
    Ok(())
}

fn validate_workdir(workdir: &str) -> Result<(), String> {
    if !workdir.starts_with('/') {
        return Err("must start with '/'".to_string());
    }
    Ok(())
}

fn validate_hostname(hostname: &str) -> Result<(), String> {
    let chars = hostname.chars().count();
    if chars == 0 || chars > MAX_HOSTNAME_CHARS {
        return Err(format!("must be 1 to {MAX_HOSTNAME_CHARS} characters"));
    }
    if !hostname
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '.' || ch == '-')
    {
        return Err("may only contain A-Z, a-z, 0-9, '.' and '-'".to_string());
    }
    if hostname.starts_with('-')
        || hostname.ends_with('-')
        || hostname.starts_with('.')
        || hostname.ends_with('.')
    {
        return Err("must not start or end with '-' or '.'".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> GuestConfig {
        GuestConfig {
            args: vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                "echo hi".to_string(),
                String::new(),
            ],
            env: vec![
                ("PATH".to_string(), "/usr/bin:/bin".to_string()),
                ("EMPTY".to_string(), String::new()),
            ],
            user: Some("1000:1000".to_string()),
            workdir: Some("/srv/app".to_string()),
            hostname: Some("web-1.example".to_string()),
            log_console: LogConsole::Virtio,
            net: Some(NetConfig {
                mac: [0x02, 0x00, 0x00, 0x00, 0x00, 0x01],
                address: Ipv4Addr::new(10, 42, 1, 10),
                prefix_len: 24,
                gateway: Ipv4Addr::new(10, 42, 1, 1),
            }),
            dns: vec![Ipv4Addr::new(1, 1, 1, 1), Ipv4Addr::new(8, 8, 8, 8)],
            stop_timeout_ms: 10_000,
            exec_port: None,
        }
    }

    fn minimal() -> GuestConfig {
        GuestConfig {
            args: vec!["/bin/true".to_string()],
            env: Vec::new(),
            user: None,
            workdir: None,
            hostname: None,
            log_console: LogConsole::Serial,
            net: None,
            dns: Vec::new(),
            stop_timeout_ms: 5_000,
            exec_port: None,
        }
    }

    fn decode_str(text: &str) -> Result<GuestConfig, ConfigError> {
        GuestConfig::decode(text.as_bytes())
    }

    #[test]
    fn round_trips_a_full_config() {
        let config = sample();
        let encoded = config.encode().expect("sample encodes");
        assert!(encoded.iter().all(|byte| *byte != 0));
        let decoded = GuestConfig::decode(&encoded).expect("encoded config decodes");
        assert_eq!(decoded, config);
    }

    #[test]
    fn round_trips_a_minimal_config() {
        let config = minimal();
        let encoded = config.encode().expect("minimal encodes");
        let text = std::str::from_utf8(&encoded).expect("ascii");
        assert_eq!(
            text,
            format!("{CONFIG_MAGIC}\narg=/bin/true\nconsole=serial\nstop_timeout_ms=5000\n")
        );
        assert_eq!(GuestConfig::decode(&encoded).expect("decodes"), config);
    }

    #[test]
    fn a_config_with_an_exec_port_is_version_2() {
        let config = GuestConfig {
            exec_port: Some(EXEC_PORT),
            ..minimal()
        };
        let encoded = config.encode().expect("v2 encodes");
        let text = std::str::from_utf8(&encoded).expect("ascii");
        assert_eq!(
            text,
            format!(
                "{CONFIG_MAGIC_V2}\narg=/bin/true\nconsole=serial\nstop_timeout_ms=5000\nexec_port=1024\n"
            )
        );
        assert_eq!(GuestConfig::decode(&encoded).expect("decodes"), config);

        let sample_v2 = GuestConfig {
            exec_port: Some(7),
            ..sample()
        };
        let encoded = sample_v2.encode().expect("encodes");
        assert_eq!(GuestConfig::decode(&encoded).expect("decodes"), sample_v2);
    }

    #[test]
    fn exec_port_is_only_valid_in_version_2() {
        let v1 = format!(
            "{CONFIG_MAGIC}\narg=/bin/true\nconsole=serial\nstop_timeout_ms=1\nexec_port=1024\n"
        );
        assert!(
            GuestConfig::decode(v1.as_bytes()).is_err(),
            "v1 has no exec_port"
        );

        let missing =
            format!("{CONFIG_MAGIC_V2}\narg=/bin/true\nconsole=serial\nstop_timeout_ms=1\n");
        assert!(
            GuestConfig::decode(missing.as_bytes()).is_err(),
            "v2 requires it"
        );

        for port in ["0", "4294967295", "-1", "x", "4294967296"] {
            let text = format!(
                "{CONFIG_MAGIC_V2}\narg=/bin/true\nconsole=serial\nstop_timeout_ms=1\nexec_port={port}\n"
            );
            assert!(GuestConfig::decode(text.as_bytes()).is_err(), "port {port}");
        }
        let twice = format!(
            "{CONFIG_MAGIC_V2}\narg=/bin/true\nconsole=serial\nstop_timeout_ms=1\nexec_port=1\nexec_port=2\n"
        );
        assert!(GuestConfig::decode(twice.as_bytes()).is_err());
        for port in [0, u32::MAX] {
            let config = GuestConfig {
                exec_port: Some(port),
                ..minimal()
            };
            assert!(config.encode().is_err(), "port {port} is not encodable");
        }
    }

    #[test]
    fn encodes_fields_in_the_documented_order() {
        let encoded = sample().encode().expect("sample encodes");
        let text = String::from_utf8(encoded).expect("ascii");
        let keys: Vec<&str> = text
            .lines()
            .skip(1)
            .map(|line| line.split_once('=').map_or(line, |(key, _)| key))
            .collect();
        assert_eq!(
            keys,
            vec![
                "arg",
                "arg",
                "arg",
                "arg",
                "env",
                "env",
                "user",
                "workdir",
                "hostname",
                "console",
                "net.mac",
                "net.address",
                "net.gateway",
                "dns",
                "dns",
                "stop_timeout_ms",
            ]
        );
    }

    #[test]
    fn escapes_backslashes_and_newlines_in_values() {
        let mut config = minimal();
        config.args = vec!["/bin/sh".to_string(), "a\\b\nc".to_string()];
        config.env = vec![("WEIRD".to_string(), "x=y\\z\nline2".to_string())];
        let encoded = config.encode().expect("encodes");
        let text = String::from_utf8(encoded).expect("ascii");
        assert!(text.contains("arg=a\\\\b\\nc\n"), "got {text:?}");
        assert!(
            text.contains("env=WEIRD=x=y\\\\z\\nline2\n"),
            "got {text:?}"
        );
        assert_eq!(
            GuestConfig::decode(text.as_bytes()).expect("decodes"),
            config
        );
    }

    #[test]
    fn round_trips_unicode_and_equals_in_env_values() {
        let mut config = minimal();
        config.env = vec![
            ("A".to_string(), "1=2=3".to_string()),
            ("B".to_string(), "héllo wörld ✓".to_string()),
        ];
        config.args = vec!["/bin/echo".to_string(), "✓".to_string()];
        let encoded = config.encode().expect("encodes");
        assert_eq!(GuestConfig::decode(&encoded).expect("decodes"), config);
    }

    #[test]
    fn allows_empty_args_after_the_first() {
        let mut config = minimal();
        config.args = vec!["/bin/true".to_string(), String::new(), String::new()];
        let encoded = config.encode().expect("encodes");
        let decoded = GuestConfig::decode(&encoded).expect("decodes");
        assert_eq!(decoded.args, config.args);
    }

    #[test]
    fn rejects_empty_first_arg() {
        let mut config = minimal();
        config.args = vec![String::new()];
        let error = config.encode().expect_err("empty args[0] must fail");
        assert!(error.0.contains("args[0]"), "got {error}");
    }

    #[test]
    fn rejects_missing_args() {
        let mut config = minimal();
        config.args.clear();
        let error = config.encode().expect_err("no args must fail");
        assert!(error.0.contains("missing arg"), "got {error}");

        let text = format!("{CONFIG_MAGIC}\nconsole=none\nstop_timeout_ms=1\n");
        let error = decode_str(&text).expect_err("no arg line must fail");
        assert!(error.0.contains("missing arg"), "got {error}");
    }

    #[test]
    fn rejects_bad_magic() {
        let error =
            decode_str("quoro-init-config v3\narg=/bin/true\nconsole=none\nstop_timeout_ms=1\n")
                .expect_err("wrong magic must fail");
        assert!(error.0.contains("line 1"), "got {error}");
        let error = GuestConfig::decode(b"").expect_err("empty region must fail");
        assert!(error.0.contains("empty"), "got {error}");
    }

    #[test]
    fn rejects_unknown_keys_with_the_line_number() {
        let text = format!(
            "{CONFIG_MAGIC}\narg=/bin/true\nconsole=none\ncapabilities=all\nstop_timeout_ms=1\n"
        );
        let error = decode_str(&text).expect_err("unknown key must fail");
        assert!(error.0.contains("line 4"), "got {error}");
        assert!(error.0.contains("capabilities"), "got {error}");
    }

    #[test]
    fn rejects_duplicate_single_keys() {
        let base = format!(
            "{CONFIG_MAGIC}\narg=/bin/true\nuser=a\nworkdir=/\nhostname=a\nconsole=none\n\
             stop_timeout_ms=1\nnet.mac=02:00:00:00:00:01\nnet.address=10.0.0.1/24\n\
             net.gateway=10.0.0.2\n"
        );
        let expected_line = base.lines().count() + 1;
        for extra in [
            "user=a",
            "workdir=/",
            "hostname=a",
            "console=none",
            "stop_timeout_ms=1",
            "net.mac=02:00:00:00:00:01",
            "net.address=10.0.0.1/24",
            "net.gateway=10.0.0.2",
        ] {
            let text = format!("{base}{extra}\n");
            let error = decode_str(&text).expect_err("duplicate must fail");
            assert!(error.0.contains("duplicate"), "{extra} gave {error}");
            assert!(
                error.0.contains(&format!("line {expected_line}")),
                "{extra} gave {error}"
            );
        }
    }

    #[test]
    fn rejects_partial_net_group() {
        let text = format!(
            "{CONFIG_MAGIC}\narg=/bin/true\nconsole=none\nnet.mac=02:00:00:00:00:01\n\
             stop_timeout_ms=1\n"
        );
        let error = decode_str(&text).expect_err("partial net group must fail");
        assert!(error.0.contains("line 4"), "got {error}");
        assert!(error.0.contains("incomplete"), "got {error}");

        let text = format!(
            "{CONFIG_MAGIC}\narg=/bin/true\nconsole=none\nnet.mac=02:00:00:00:00:01\n\
             net.address=10.0.0.1/24\nstop_timeout_ms=1\n"
        );
        let error = decode_str(&text).expect_err("partial net group must fail");
        assert!(error.0.contains("net.gateway"), "got {error}");
    }

    #[test]
    fn rejects_missing_required_keys() {
        let text = format!("{CONFIG_MAGIC}\narg=/bin/true\nstop_timeout_ms=1\n");
        let error = decode_str(&text).expect_err("missing console must fail");
        assert!(error.0.contains("missing console"), "got {error}");

        let text = format!("{CONFIG_MAGIC}\narg=/bin/true\nconsole=none\n");
        let error = decode_str(&text).expect_err("missing stop_timeout_ms must fail");
        assert!(error.0.contains("missing stop_timeout_ms"), "got {error}");
    }

    #[test]
    fn rejects_bad_escapes() {
        let text = format!("{CONFIG_MAGIC}\narg=/bin/true\nconsole=none\nstop_timeout_ms=1\n");
        for bad in ["arg=a\\tb", "arg=\\q", "arg=trailing\\"] {
            let text = text.replace("arg=/bin/true", bad);
            let error = decode_str(&text).expect_err("bad escape must fail");
            assert!(error.0.contains("line 2"), "{bad} gave {error}");
        }
    }

    #[test]
    fn rejects_lines_without_newline_or_with_blank_lines() {
        let error = decode_str(&format!("{CONFIG_MAGIC}\narg=/bin/true\nconsole=none"))
            .expect_err("unterminated line must fail");
        assert!(error.0.contains("line 3"), "got {error}");

        let error = decode_str(&format!(
            "{CONFIG_MAGIC}\n\narg=/bin/true\nconsole=none\nstop_timeout_ms=1\n"
        ))
        .expect_err("blank line must fail");
        assert!(error.0.contains("line 2"), "got {error}");
        assert!(error.0.contains("blank"), "got {error}");
    }

    #[test]
    fn rejects_invalid_utf8_with_the_line_number() {
        let mut bytes = format!("{CONFIG_MAGIC}\narg=/bin/true\nconsole=none\n").into_bytes();
        bytes.extend_from_slice(&[0xff, 0xfe]);
        let error = GuestConfig::decode(&bytes).expect_err("invalid UTF-8 must fail");
        assert!(error.0.contains("line 4"), "got {error}");
        assert!(error.0.contains("UTF-8"), "got {error}");
    }

    #[test]
    fn rejects_nul_bytes() {
        let mut bytes = format!("{CONFIG_MAGIC}\narg=/bin/true\nconsole=none\nstop_timeout_ms=1\n")
            .into_bytes();
        bytes.extend_from_slice(b"junk");
        // A NUL cuts the region short, so the text no longer ends in a line.
        bytes.push(0);
        bytes.extend_from_slice(b"more\n");
        let error = GuestConfig::decode(&bytes).expect_err("NUL must fail");
        assert!(error.0.contains("line 5"), "got {error}");
    }

    #[test]
    fn rejects_bad_user_values() {
        for user in ["", "a b", "a:b:c", ":b", "a:"] {
            let text = format!(
                "{CONFIG_MAGIC}\narg=/bin/true\nconsole=none\nuser={user}\nstop_timeout_ms=1\n"
            );
            let error = decode_str(&text).expect_err("bad user must fail");
            assert!(error.0.contains("line 4"), "user {user:?} gave {error}");
        }
    }

    #[test]
    fn rejects_bad_workdir_and_hostname() {
        let text = format!(
            "{CONFIG_MAGIC}\narg=/bin/true\nconsole=none\nworkdir=relative\nstop_timeout_ms=1\n"
        );
        let error = decode_str(&text).expect_err("relative workdir must fail");
        assert!(error.0.contains("line 4"), "got {error}");

        for hostname in ["", "-a", "a-", ".a", "a.", "a b", "a_b", &"a".repeat(65)] {
            let text = format!(
                "{CONFIG_MAGIC}\narg=/bin/true\nconsole=none\nhostname={hostname}\n\
                 stop_timeout_ms=1\n"
            );
            let error = decode_str(&text).expect_err("bad hostname must fail");
            assert!(
                error.0.contains("line 4"),
                "hostname {hostname:?} gave {error}"
            );
        }
        let text = format!(
            "{CONFIG_MAGIC}\narg=/bin/true\nconsole=none\nhostname={}\nstop_timeout_ms=1\n",
            "a".repeat(64)
        );
        decode_str(&text).expect("64-character hostname is allowed");
    }

    #[test]
    fn rejects_bad_console_values() {
        let text = format!("{CONFIG_MAGIC}\narg=/bin/true\nconsole=tty\nstop_timeout_ms=1\n");
        let error = decode_str(&text).expect_err("bad console must fail");
        assert!(error.0.contains("line 3"), "got {error}");
    }

    #[test]
    fn rejects_bad_net_values() {
        let cases = [
            ("net.mac", "02:00:00:00:00", "line 4"),
            ("net.mac", "02:00:00:00:00:zz", "line 4"),
            ("net.address", "10.0.0.1", "line 5"),
            ("net.address", "10.0.0.1/33", "line 5"),
            ("net.address", "10.0.0.999/24", "line 5"),
            ("net.gateway", "10.0.0", "line 6"),
        ];
        for (key, value, line) in cases {
            let text = format!(
                "{CONFIG_MAGIC}\narg=/bin/true\nconsole=none\nnet.mac=02:00:00:00:00:01\n\
                 net.address=10.0.0.1/24\nnet.gateway=10.0.0.2\nstop_timeout_ms=1\n"
            )
            .replace(
                &format!(
                    "{key}={}",
                    match key {
                        "net.mac" => "02:00:00:00:00:01",
                        "net.address" => "10.0.0.1/24",
                        _ => "10.0.0.2",
                    }
                ),
                &format!("{key}={value}"),
            );
            let error = decode_str(&text).expect_err("bad net value must fail");
            assert!(error.0.contains(line), "{key}={value} gave {error}");
        }
    }

    #[test]
    fn rejects_bad_dns_and_accepts_several() {
        let text = format!(
            "{CONFIG_MAGIC}\narg=/bin/true\nconsole=none\ndns=1.1.1.1\ndns=8.8.8.8\n\
             stop_timeout_ms=1\n"
        );
        let config = decode_str(&text).expect("two dns servers decode");
        assert_eq!(
            config.dns,
            vec![Ipv4Addr::new(1, 1, 1, 1), Ipv4Addr::new(8, 8, 8, 8)]
        );

        let text = format!(
            "{CONFIG_MAGIC}\narg=/bin/true\nconsole=none\ndns=1.1.1\ndns=8.8.8.8\n\
             stop_timeout_ms=1\n"
        );
        let error = decode_str(&text).expect_err("bad dns must fail");
        assert!(error.0.contains("line 4"), "got {error}");
    }

    #[test]
    fn rejects_bad_stop_timeout() {
        for value in ["-1", "abc", "86400001"] {
            let text =
                format!("{CONFIG_MAGIC}\narg=/bin/true\nconsole=none\nstop_timeout_ms={value}\n");
            let error = decode_str(&text).expect_err("bad stop timeout must fail");
            assert!(error.0.contains("line 4"), "{value} gave {error}");
        }
        let text =
            format!("{CONFIG_MAGIC}\narg=/bin/true\nconsole=none\nstop_timeout_ms=86400000\n");
        decode_str(&text).expect("the maximum is allowed");
    }

    #[test]
    fn rejects_nul_in_a_value_when_encoding() {
        let mut config = minimal();
        config.args = vec!["/bin/true".to_string(), "a\0b".to_string()];
        let error = config.encode().expect_err("NUL must fail");
        assert!(error.0.contains("NUL"), "got {error}");

        let mut config = minimal();
        config.env = vec![("A".to_string(), "a\0b".to_string())];
        let error = config.encode().expect_err("NUL must fail");
        assert!(error.0.contains("NUL"), "got {error}");
    }

    #[test]
    fn rejects_bad_env_names() {
        for name in ["", "A=B"] {
            let mut config = minimal();
            config.env = vec![(name.to_string(), "v".to_string())];
            let error = config.encode().expect_err("bad env name must fail");
            assert!(error.0.contains("env name"), "{name:?} gave {error}");
        }

        let text = format!(
            "{CONFIG_MAGIC}\narg=/bin/true\nconsole=none\nenv=NOEQUALS\n\
                            stop_timeout_ms=1\n"
        );
        let error = decode_str(&text).expect_err("env without '=' must fail");
        assert!(error.0.contains("line 4"), "got {error}");
    }

    #[test]
    fn decode_stops_at_the_first_nul() {
        let mut bytes = minimal().encode().expect("encodes");
        bytes.push(0);
        bytes.extend_from_slice(b"garbage that is ignored\n");
        assert_eq!(GuestConfig::decode(&bytes).expect("decodes"), minimal());
    }

    #[test]
    fn rejects_an_encoding_longer_than_the_region() {
        let mut config = minimal();
        config.args = vec![
            "/bin/true".to_string(),
            "x".repeat(CONFIG_REGION_LEN as usize),
        ];
        let error = config.encode().expect_err("oversized config must fail");
        assert!(error.0.contains("more than"), "got {error}");
    }

    #[test]
    fn status_round_trips() {
        for status in [
            AppStatus::Exited { code: 0 },
            AppStatus::Exited { code: 3 },
            AppStatus::Exited { code: 128 + 9 },
            AppStatus::Fatal {
                code: 127,
                message: "/bin/nope: not found".to_string(),
            },
            AppStatus::Fatal {
                code: 1,
                message: "line\\slash\nnewline ✓".to_string(),
            },
            AppStatus::Fatal {
                code: 1,
                message: String::new(),
            },
        ] {
            let encoded = encode_status(&status);
            assert!(
                encoded.len() as u64 <= STATUS_REGION_LEN,
                "{} bytes",
                encoded.len()
            );
            assert_eq!(encoded.last().copied(), Some(0), "record is NUL-terminated");
            assert_eq!(
                decode_status(&encoded).expect("decodes"),
                Some(status),
                "round trip"
            );
        }
    }

    #[test]
    fn status_region_of_zeros_is_none() {
        assert_eq!(decode_status(&[]).expect("empty"), None);
        assert_eq!(decode_status(&[0u8; 16]).expect("zeros"), None);
        assert_eq!(
            decode_status(&[0u8; STATUS_REGION_LEN as usize]).expect("zeros"),
            None
        );
    }

    #[test]
    fn truncates_a_long_fatal_message() {
        let status = AppStatus::Fatal {
            code: 1,
            message: "x".repeat(10_000),
        };
        let encoded = encode_status(&status);
        assert!(encoded.len() as u64 <= STATUS_REGION_LEN);
        assert_eq!(encoded.len() as u64, STATUS_REGION_LEN);
        match decode_status(&encoded).expect("truncated record decodes") {
            Some(AppStatus::Fatal { code, message }) => {
                assert_eq!(code, 1);
                assert!(message.len() < 10_000);
                assert!(message.len() > 3_000);
            }
            other => panic!("unexpected status {other:?}"),
        }
    }

    #[test]
    fn truncation_keeps_a_multi_byte_character_whole() {
        let status = AppStatus::Fatal {
            code: 1,
            message: "✓".repeat(5_000),
        };
        let encoded = encode_status(&status);
        assert!(encoded.len() as u64 <= STATUS_REGION_LEN);
        match decode_status(&encoded).expect("truncated record decodes") {
            Some(AppStatus::Fatal { message, .. }) => {
                assert!(message.chars().all(|ch| ch == '✓'), "no broken chars");
            }
            other => panic!("unexpected status {other:?}"),
        }
    }

    #[test]
    fn truncation_never_leaves_a_lone_backslash() {
        // Every character is two escaped bytes, so an odd budget is plausible.
        let status = AppStatus::Fatal {
            code: 1,
            message: "\\".repeat(5_000),
        };
        let encoded = encode_status(&status);
        decode_status(&encoded).expect("escaped backslashes decode");
    }

    #[test]
    fn status_rejects_bad_records() {
        let error = decode_status(b"quoro-init-status v2\nexit=0\n").expect_err("bad magic");
        assert!(error.0.contains("line 1"), "got {error}");

        let error = decode_status(b"quoro-init-status v1\n").expect_err("missing exit");
        assert!(error.0.contains("missing exit"), "got {error}");

        let error = decode_status(b"quoro-init-status v1\nexit=x\n").expect_err("bad exit");
        assert!(error.0.contains("line 2"), "got {error}");

        let error =
            decode_status(b"quoro-init-status v1\nexit=0\nextra=1\n").expect_err("unknown key");
        assert!(error.0.contains("line 3"), "got {error}");
    }
}
