# ADR 0199: stale_term is a fatal session error that every machine sends to a primary with a lower term, and it does not stop redialing

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0199
- **Tags:** m5b, contracts, leases
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0169 adds stale_term but not who sends it, whether it is fatal, how a machine learns a higher term, or what a primary does on receiving it.

## Decision

stale_term is a fatal session error that every machine sends to a primary with a lower term, and it does not stop redialing.

Every machine stores the highest term it has seen (welcome, desired_state, support_request, a vote it granted) in node_meta. A welcome, desired_state or support_request with a lower term is answered with error stale_term, fatal: the machine ends the session and keeps redialing through primary_url and primary_urls (second exception to 'fatal stops redialing', next to unsupported_version), running its stored desired state meanwhile. A higher term in welcome or desired_state is adopted (the pinned certificate authenticates the session; a promoted primary keeps it, D-0181). At a term it voted in, a voter supports only the candidate it voted for. A primary that receives stale_term stops granting and renewing expiring leases at once; refusing writes is the promotion contract's (D-0182). No term field is added to hello.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0199 in the project decision ledger. Do not edit by hand; changes go through a new decision.
