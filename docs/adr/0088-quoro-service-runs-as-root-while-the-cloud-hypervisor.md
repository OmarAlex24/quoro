# ADR 0088: quoro.service runs as root while the Cloud Hypervisor engine is in-process (M1)

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0088
- **Tags:** installer, engine, security
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The CH engine creates a bridge, TAPs and nftables rules (CAP_NET_ADMIN), writes ip_forward, and converts OCI layers with ownership preserved (gated on euid==0 in ImageStore); the engine suite was only validated as root. User=quoro with no capabilities cannot run it.

## Decision

quoro.service runs as root while the Cloud Hypervisor engine is in-process (M1).

Implemented in the M1 integration PRs: unit without User=, hardened with NoNewPrivileges, ProtectHome, PrivateTmp, ProtectSystem=full and RestartPreventExitStatus=78 (host check refusal); installer no longer creates a quoro user; /var/lib/quoro root 0700; setup-token runs as root. Alternative: User=quoro + AmbientCapabilities (NET_ADMIN, CHOWN, FOWNER, DAC_OVERRIDE, MKNOD) plus a capability-aware ownership check in ImageStore; rejected for M1 because CHOWN+DAC_OVERRIDE is root-equivalent anyway and it is untested. Revisit when the node agent splits from the control plane (M2): an unprivileged API process talking to a privileged node agent.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0088 in the project decision ledger. Do not edit by hand; changes go through a new decision.
