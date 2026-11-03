# ADR 0096: M2 process model: quoro.service is the unprivileged control plane (User=quoro-api, empty capability bounding set) and quoro-node.service is the root node agent with KillMode=process; node data in /var/lib/quoro-node

- **Status:** Accepted
- **Date:** 2026-11-03
- **Ledger id:** D-0096
- **Tags:** security, installer, engine, m2
- **Supersedes:** [ADR 0088](0088-quoro-service-runs-as-root-while-the-cloud-hypervisor.md)
- **Superseded by:** none
- **Related:** none

## Context

D-0088 was M1-only (engine in-process). D-0089 requires the split in M2; this records the concrete shape so D-0088 is marked superseded.

## Decision

M2 process model: quoro.service is the unprivileged control plane (User=quoro-api, empty capability bounding set) and quoro-node.service is the root node agent with KillMode=process; node data in /var/lib/quoro-node.

Implements D-0089 (issue #63). quoro.service: User/Group quoro-api (dedicated system user; 'quoro' is not reused because it is a login user on some hosts), CapabilityBoundingSet= and AmbientCapabilities= empty, NoNewPrivileges, ProtectSystem=strict with ReadWritePaths=/var/lib/quoro, PrivateDevices, RestrictAddressFamilies inet/unix; runs 'quoro serve' with [node] local=false. quoro-node.service: root, 'quoro node', RestartPreventExitStatus=78, KillMode=process so agent restarts never stop VMMs (the engine adopts them), After=quoro.service without Requires (D-0011). Node data dir /var/lib/quoro-node (sibling of /var/lib/quoro, which D-0054 named for data; the control plane cannot read node secrets). /var/lib/quoro owned quoro-api 0700, /etc/quoro/quoro.toml root:quoro-api 0640. The primary's agent enrolls via a local join file the control plane writes until the primary holds a node credential. 'quoro setup-token' run as root drops to the data dir owner. Installer: QUORO_ROLE=primary|node with QUORO_JOIN_URL/TOKEN/FINGERPRINT for nodes. Tests: unit-file assertions and a Linux check that a running quoro serve has zero CapInh/CapPrm/CapEff/CapAmb; lab check of /proc/<MainPID>/status for the installed unit.

_Some wording was changed for the public repository._

## Consequences

- Replaces ADR 0088.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0096 in the project decision ledger. Do not edit by hand; changes go through a new decision.
