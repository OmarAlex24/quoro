// SPDX-License-Identifier: AGPL-3.0-only
//! The guest side of `docs/contracts/engine-trait.md` section 6, as a library.
//!
//! `quoro-init` is PID 1 in every microVM. The engine and the init agree on
//! two small formats, both defined here so there is exactly one copy:
//!
//! - the **configuration** the engine writes at the start of the config disk
//!   and the init reads before it starts the app ([`GuestConfig`]);
//! - the **status record** the init writes into the same disk when the app
//!   exits or the boot fails, and the engine reads after the VMM has exited
//!   ([`AppStatus`]).
//!
//! - the **exec channel** framing on vsock (M4, [`exec`]): one connection is
//!   one PTY session the engine opens for the web terminal.
//!
//! The guest finds its disks by virtio-blk serial number ([`SERIAL_ROOT`],
//! [`SERIAL_DATA`], [`SERIAL_CONFIG`]), not by probe order.
//!
//! This library has no dependencies and no platform code. The binary in
//! `main.rs` is the Linux-only PID 1.

pub mod config;
pub mod exec;

pub use config::{
    AppStatus, CONFIG_DISK_LEN, CONFIG_MAGIC, CONFIG_MAGIC_V2, CONFIG_REGION_LEN, ConfigError,
    EXEC_PORT, GuestConfig, LogConsole, NetConfig, SERIAL_CONFIG, SERIAL_DATA, SERIAL_ROOT,
    STATUS_MAGIC, STATUS_OFFSET, STATUS_REGION_LEN, decode_status, encode_status,
};
