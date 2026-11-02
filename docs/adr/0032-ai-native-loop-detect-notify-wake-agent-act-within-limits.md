# ADR 0032: AI-native loop: detect -> notify -> wake agent -> act within limits

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0032
- **Tags:** agents, events
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Events close the loop by waking an agent.

## Decision

AI-native loop: detect -> notify -> wake agent -> act within limits.

Detect: node agent + health checks report state/metrics. Notify: configurable webhooks per event (VM down, build failed, disk full, certificate expiring). Wake: webhook starts an agent (e.g. Claude Agent SDK) with Quoro's MCP and incident context. Act: only what its grant allows; destructive needs human approval.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0032 in the project decision ledger. Do not edit by hand; changes go through a new decision.
