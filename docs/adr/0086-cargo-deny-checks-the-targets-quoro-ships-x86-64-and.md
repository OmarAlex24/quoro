# ADR 0086: cargo-deny checks the targets Quoro ships (x86_64 and aarch64 Linux, gnu and musl), not every target; the ADR 0080 license allowlist is unchanged

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0086
- **Tags:** governance, security
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer chose to limit the check to shipped targets on 2026-11-02. webpki-root-certs (CDLA-Permissive-2.0) enters the graph only for wasm32 through oci-client -> reqwest -> rustls-platform-verifier and never reaches the shipped binary; checking every target failed CI on a crate Quoro does not ship.

## Decision

cargo-deny checks the targets Quoro ships (x86_64 and aarch64 Linux, gnu and musl), not every target; the ADR 0080 license allowlist is unchanged.

deny.toml [graph] targets = x86_64-unknown-linux-gnu, x86_64-unknown-linux-musl, aarch64-unknown-linux-gnu, aarch64-unknown-linux-musl. Adding a shipped target adds it here. Refines D-0080 (scope of the check); the allowlist and the no-exceptions rule stay.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0086 in the project decision ledger. Do not edit by hand; changes go through a new decision.
