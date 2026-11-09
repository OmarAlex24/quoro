# ADR 0305: Section 8.8 Restarts: after a reboot a voter restarts its promise and horizon from the longest window it may have granted, not only the last S and L

- **Status:** Accepted
- **Date:** 2026-11-09
- **Ledger id:** D-0305
- **Tags:** m5b, contract, voters, leases
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0198](0198-the-candidate-waits-for-each-voter-s-lease-horizon-which.md)

## Context

The literal rule (restart from now with the stored S and L) uses the last answered request's values. If L dropped (a maintenance window W=180 s ended, so L went back to 15 s) and the voter stored the shorter L but the primary never got that answer, the primary still counts the earlier support with L=W and may have granted up to W. After a reboot the voter would then restart its horizon at now+S+15+2M_L (~33 s) while a grant made under the W support may stay before its gate for up to ~231 s. A candidate counting lease_wait_ms from that horizon could start an app the old primary's holder still runs.

## Decision

Section 8.8 Restarts: after a reboot a voter restarts its promise and horizon from the longest window it may have granted, not only the last S and L.

The maintainer chose this text for node-agent-protocol.md section 8.8 Restarts: after a reboot the voter starts both again from its first start, the promise until now + max(S, stored promise - stored v_recv) and the horizon until now + max(S + L + 2 M_L, stored horizon - stored v_recv), with the stored S and L, so neither is shorter than the longest window it may still owe. The voter also stores the last v_recv (section 12 node_meta). #685 implements this reading already (it errs late).

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0198](0198-the-candidate-waits-for-each-voter-s-lease-horizon-which.md): where a voter's promise and lease horizon start again after a reboot: from its first start, each for the longer of the rule's span and what was left of it at the last grant. The rest of [ADR 0198](0198-the-candidate-waits-for-each-voter-s-lease-horizon-which.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0305 in the project decision ledger. Do not edit by hand; changes go through a new decision.
