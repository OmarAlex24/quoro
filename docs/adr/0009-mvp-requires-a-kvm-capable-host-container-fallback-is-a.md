# ADR 0009: MVP requires a KVM-capable host; container fallback is a later Engine implementation

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0009
- **Tags:** engine, product
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Many VPS do not expose nested virtualization; a container fallback (e.g. youki, Rust OCI runtime) is more work and weaker isolation.

## Decision

MVP requires a KVM-capable host; container fallback is a later Engine implementation.

Supported in MVP: bare metal or VPS with nested virt. Fallback mode deferred.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0009 in the project decision ledger. Do not edit by hand; changes go through a new decision.
