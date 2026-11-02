# ADR 0033: Milestones M0-M6; HA and backups out of MVP but the MVP architecture prepares them

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0033
- **Tags:** roadmap
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Reliability is non-negotiable, but HA is expensive; scope is the main project risk.

## Decision

Milestones M0-M6; HA and backups out of MVP but the MVP architecture prepares them.

M0 Contracts: repo, CI, cargo workspace, OpenAPI spec, DB schema, event schema, node-agent protocol (incl. self-fencing) + spike 'boot a microVM from an OCI image'. M1 Skeleton: one-command installer, engine create/start/stop/delete, basic panel. M2 Deploy: node agent, deploy an OCI image with domain + TLS, live logs. M3 AI-native: MCP read + restart/redeploy, scoped grants, audit log, event webhooks. M4 Full experience: Git builds (Dockerfile; Railpack/Nixpacks to verify), metrics, web terminal, VM console. M5a: stateless replicas + edge LB + backups to S3/R2 (cold). M5b: witness, leases, fencing, replicated volumes (warm), capacity indicator. M6: automatic failover + hot. M1-M3 is already a self-explaining demo: 'I told an agent the app was down, it restarted it, and it is in the audit log'.

_Some operational details were redacted for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0033 in the project decision ledger. Do not edit by hand; changes go through a new decision.
