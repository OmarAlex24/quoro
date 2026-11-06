# ADR 0197: Voter support is a primary-initiated support_request / support_grant pair on the session, S = 15 s renewed every S/3

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0197
- **Tags:** m5b, contracts, leases
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0169 fixes support windows but not their messages, default, persistence, how the primary's own host counts, or what happens below three voters or without a majority.

## Decision

Voter support is a primary-initiated support_request / support_grant pair on the session, S = 15 s renewed every S/3.

New session messages: support_request (control plane to voter: term, primary_node_id, support_ms, max_lease_ms, voters_version), every support_ms/3 after desired_state; support_grant (voter to control plane, re = request: the same fields). S default 15000 ms (= D), floor 3000, M_S = max(1000, S/10). The primary measures support from its send time (valid until p_send + S - M_S); the voter promises from its receipt (v_recv + S) and persists before it answers. The primary's own host counts only through the support its own node agent grants over its own session. Majority = more than half of all voters of the version, connected or not; with fewer than three voters the majority of existing voters is still required and ha_ready is false. Without a majority for the duration it grants, the primary leaves renewals unanswered (no new error code); local leases need no support. Voters stop answering when spec.votes leaves their desired state.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0197 in the project decision ledger. Do not edit by hand; changes go through a new decision.
