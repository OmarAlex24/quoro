# ADR 0021: N+1 capacity indicator: 'if node A dies, do its apps fit on the others?'

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0021
- **Tags:** reliability, ha-policy, panel
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Users must be told how much space/RAM HA reserves; no self-hosted tool does this well; ideal for an agent to watch.

## Decision

N+1 capacity indicator: 'if node A dies, do its apps fit on the others?'.

Example: 'yes' / 'no, you are missing 6 GB of RAM'.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0021 in the project decision ledger. Do not edit by hand; changes go through a new decision.
