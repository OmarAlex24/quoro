# ADR 0002: Quoro unifies hypervisor and app deploy in one self-hosted tool, operable by AI agents

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0002
- **Tags:** product
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Today self-hosters stack Proxmox -> Debian VM -> Dokploy/Coolify: two dashboards, two mental models, nothing designed for agents. Harvester+Rancher and Cozystack unify both layers but are Kubernetes-heavy and aimed at hosting providers.

## Decision

Quoro unifies hypervisor and app deploy in one self-hosted tool, operable by AI agents.

One interface for creating/managing VMs and deploying services (push-to-deploy, domains, TLS, logs). MCP and events are first-class from day one. The agent is 'one more operator': it reads the same state as the panel, acts through the same API, and is logged the same way as a human.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0002 in the project decision ledger. Do not edit by hand; changes go through a new decision.
