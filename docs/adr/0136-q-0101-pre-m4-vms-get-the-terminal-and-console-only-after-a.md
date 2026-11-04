# ADR 0136: Q-0101: pre-M4 VMs get the terminal and console only after a redeploy; the release notes say so and the panel shows a 'redeploy to enable' hint instead of the action; the node agent never recreates a VM on its own

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0136
- **Tags:** m4, engine, panel
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendation on 2026-11-04. A restart that silently changed a VM's devices would break the rule that a restart keeps the stored spec; a redeploy is the explicit way to change it.

## Decision

Q-0101: pre-M4 VMs get the terminal and console only after a redeploy; the release notes say so and the panel shows a 'redeploy to enable' hint instead of the action; the node agent never recreates a VM on its own.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0136 in the project decision ledger. Do not edit by hand; changes go through a new decision.
