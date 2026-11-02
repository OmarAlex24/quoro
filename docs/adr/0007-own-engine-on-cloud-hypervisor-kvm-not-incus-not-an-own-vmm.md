# ADR 0007: Own engine on Cloud Hypervisor + KVM (not Incus, not an own VMM)

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0007
- **Tags:** engine, architecture
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer wants to own the whole orchestration with minimal, auditable, Rust dependencies. Incus is a full Go daemon (black box). Writing a VMM from scratch (rust-vmm) is years of work and a huge security surface.

## Decision

Own engine on Cloud Hypervisor + KVM (not Incus, not an own VMM).

We depend on KVM (kernel) and Cloud Hypervisor (Rust VMM with REST API; license/details to verify). We write: images, disks/snapshots, networking (bridges, TAP, nftables, IPs), console, lifecycle. Engine sits behind an `Engine` trait so other backends (Firecracker, containers) can be added.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0007 in the project decision ledger. Do not edit by hand; changes go through a new decision.
