# ADR 0063: Q-0005: kernel WireGuard configured by Quoro over netlink; mesh deferred to M5a when cross-node traffic starts

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0063
- **Tags:** networking
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Hosts already need kernel >= 6.8 for Cloud Hypervisor (F-0036) and WireGuard has been in mainline since 5.6 (hypothesis, not re-verified), so the kernel module is always present; writing our own WireGuard is a crypto risk.

## Decision

Q-0005: kernel WireGuard configured by Quoro over netlink; mesh deferred to M5a when cross-node traffic starts.

Options: (a) kernel WireGuard driven by Quoro via netlink (no wg-quick), per-node keypair, peers and allowed-IPs distributed in desired state; (b) userspace boringtun over TUN: portable, slower, one more dependency; (c) own implementation: rejected for security. NAT: two nodes behind NAT cannot reach each other directly; at least one publicly reachable peer (public node or edge) acts as relay hub, or traffic falls back to the dial-out path. The control-plane session does not depend on the mesh (TLS dial-out, D-0011), so M2 (single app node) does not need the mesh; it is first needed for app replicas and replication in M5a. Accepted option: (a), with (b) only as a fallback backend.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0063 in the project decision ledger. Do not edit by hand; changes go through a new decision.
