# ADR 0011: Nodes are autonomous and dial out

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0011
- **Tags:** architecture, node-agent, reliability
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Reliability is non-negotiable: the data plane must not depend on the panel. Dial-out works behind NAT and lets the control plane live anywhere.

## Decision

Nodes are autonomous and dial out.

Each node agent stores its desired state locally and reconciles it; apps keep running and self-restart if the control plane disappears. Nodes open the connection to the control plane, never the reverse.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0011 in the project decision ledger. Do not edit by hand; changes go through a new decision.
