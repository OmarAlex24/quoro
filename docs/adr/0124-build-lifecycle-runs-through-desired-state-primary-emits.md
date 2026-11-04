# ADR 0124: Build lifecycle runs through desired state; primary emits build.* events; Deployment.phase build is never reported

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0124
- **Tags:** contracts, m4
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Keeps one node-control path and a single writer for replicated build records

## Decision

Build lifecycle runs through desired state; primary emits build.* events; Deployment.phase build is never reported.

spec.builds[] with a cancelled flag; nodes report status_report.builds[]; a failed build starts no deployment, so no deployment.failed with phase build. Build secrets travel only in signed desired state, written to the work disk for the build only, never logged.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0124 in the project decision ledger. Do not edit by hand; changes go through a new decision.
