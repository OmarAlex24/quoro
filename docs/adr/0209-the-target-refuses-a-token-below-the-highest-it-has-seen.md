# ADR 0209: The target refuses a token below the highest it has seen, and reports the refusal

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0209
- **Tags:** m5b, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

ADR 0173 says the stream carries the lease token; the exact comparison is open.

## Decision

The target refuses a token below the highest it has seen, and reports the refusal.

The target accepts a stream only when its fencing_token is at least max(token in its spec.volumes[].warm, highest token it has accepted for that volume); otherwise it answers stale_token and records rejected_token and rejected_at, reported in status_report.warm. A source that lost its lease while cut off therefore cannot overwrite the copy a failover started from.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0209 in the project decision ledger. Do not edit by hand; changes go through a new decision.
