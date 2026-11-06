# ADR 0203: The primary keeps the gate as the latest T_grant + d + 2M per holder, survives its own restart through leases.duration_ms, and writes nothing for renewals

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0203
- **Tags:** m5b, contracts, leases
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The gate as a primary obligation needs bookkeeping that survives a primary restart and a shorter grant after a longer one, and renewals every 5 s per warm lease would flood the hash-chained change log if written.

## Decision

The primary keeps the gate as the latest T_grant + d + 2M per holder, survives its own restart through leases.duration_ms, and writes nothing for renewals.

Gate = the latest T_grant + duration_ms + 2M over every grant to the current holder, so a shorter grant after a longer one never brings it forward; renewals by the current holder are not gated. No monotonic time survives a primary restart: at start every expiring lease is treated as granted at that moment for leases.duration_ms, which holds the longest duration the primary may have granted (raised before a longer grant, lowered only once that grant's gate passed). Renewals change no state and append no change-log entry; Lease.renewed_at, expires_at and the new Lease.gate_at are the primary's in-memory view (null after a restart until the next renewal). expires_at is redefined as the last grant's time + duration_ms. App.lease carries the full Lease, optional like App.ha_cost (absent when the app has no lease row or on an older primary). The voters' support, not an external arbiter, keeps a second primary from granting at the same time in M5b.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0203 in the project decision ledger. Do not edit by hand; changes go through a new decision.
