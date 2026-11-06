# ADR 0187: Signing-key rotation (#76, D-0154) and the automatic rebuild on a critical finding (D-0155) come last in M5b and move to M6 by a new decision if the milestone slips

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0187
- **Tags:** m5b, roadmap
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Neither is on the M5b DoD. Promotion needs key custody, not rotation; the node already handles signing_key_rotate, so #76 is a primary-side task. The automatic rebuild needs #78's health-gated restarts and a 2048 MiB builder VM, which does not fit on the test lab's primary next to the demo apps and a failed-over warm app.

## Decision

Signing-key rotation (#76, D-0154) and the automatic rebuild on a critical finding (D-0155) come last in M5b and move to M6 by a new decision if the milestone slips.

Order: #76 is the last part of the promotion work; D-0155 follows #78 in the lease work. When the lease or warm-replication work reaches its failover part, whatever of the two has not started is proposed for M6 through a new decision. The maintainer accepted this on 2026-11-06.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0187 in the project decision ledger. Do not edit by hand; changes go through a new decision.
