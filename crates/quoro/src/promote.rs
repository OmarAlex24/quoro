//! `quoro cluster promote` (`node-agent-protocol.md` section 7.3, ADR 0182,
//! ADR 0232, ADR 0261), built in layers that the command calls (#19).
//!
//! Only the host preparation of step 6 is here so far, in [`prepare`]: the
//! control plane's system user, its data and configuration directories,
//! `quoro.toml`, its unit and its start, then pointing this machine's agent
//! at it. The keys, the freeze, the votes and the database are other layers.

pub mod prepare;
