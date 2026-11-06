# ADR 0177: Planned switchover: POST /apps/{app_id}/switchover {node_id} runs now; it pauses the source, ships the final snapshot, revokes the source lease with an acknowledgement, grants the target and moves the route; choosing a low-traffic window is the caller's in M5b

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0177
- **Tags:** m5b, control-plane, contracts, reliability
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0022 step 6 describes a switchover that loses no data: lag near zero, a low-traffic window, a short write freeze, final sync, lease move, traffic shift, drain. Scheduling windows (user-chosen or agent-proposed) is a separate feature.

## Decision

Planned switchover: POST /apps/{app_id}/switchover {node_id} runs now; it pauses the source, ships the final snapshot, revokes the source lease with an acknowledgement, grants the target and moves the route; choosing a low-traffic window is the caller's in M5b.

Refused with conflict unless the warm copy is in_sync and lag is below a threshold (default 2 intervals). Steps: vm.pause on the source (the freeze), final snapshot and ship, stop the source VM, lease_revoke with reason moved and wait for command_result, grant the target a higher token, start the target, the edge switches the backend, connections to the old backend drain (edge 6.8). If any step before the revoke fails, the source resumes and nothing moved. Same scope as failover (app:failover). Events app.switched_over. Scheduling a window is deferred. The maintainer accepted this on 2026-11-06.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0177 in the project decision ledger. Do not edit by hand; changes go through a new decision.
