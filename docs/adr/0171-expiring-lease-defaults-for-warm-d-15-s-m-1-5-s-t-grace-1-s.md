# ADR 0171: Expiring lease defaults for warm: D = 15 s (M = 1.5 s, T_grace 1 s, T_force 0.3 s); before a planned primary restart the primary answers renewals with a longer duration so warm apps survive upgrades

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0171
- **Tags:** m5b, reliability, node-agent
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Expiring leases end when the authority cannot be reached (D-0047). The primary restarts on every upgrade (moving the test lab's primary by a restore had the API down 28 s), so with D = 15 s every upgrade would fence every warm app. Node agent restarts are already covered by D-0072; primary restarts are not.

## Decision

Expiring lease defaults for warm: D = 15 s (M = 1.5 s, T_grace 1 s, T_force 0.3 s); before a planned primary restart the primary answers renewals with a longer duration so warm apps survive upgrades.

Defaults from the node-agent-protocol 8.4 worked example: D 15000 ms, renewal every D/3, M = max(1000, D/10), T_grace 1000 ms, T_force 300 ms; a lab spike checks clock drift and forced-stop time against them. Planned restart: the installer's upgrade path (and an operator command, quoro cluster maintenance) first asks the primary to enter a maintenance window; for the next renewal of each expiring lease the primary answers with duration_ms = W (default 180 s, at most 600 s) and records the longer gate (T_grant + W + 2M). This is inside the contract: a grant that answers a renewal may carry any duration at or above the 3000 ms floor. An unplanned primary outage longer than D still fences warm apps by design; the HA view says so, and M6's election removes it. The maintainer accepted this on 2026-11-06.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0171 in the project decision ledger. Do not edit by hand; changes go through a new decision.
