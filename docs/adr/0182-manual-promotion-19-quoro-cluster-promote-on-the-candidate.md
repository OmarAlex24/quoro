# ADR 0182: Manual promotion (#19): quoro cluster promote on the candidate, refused while the old primary may still hold expiring leases unless the operator acknowledges the fence; a returning old primary demotes itself and rejoins with quoro node rejoin

- **Status:** Accepted; refined by [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md)
- **Date:** 2026-11-06
- **Ledger id:** D-0182
- **Tags:** m5b, control-plane, reliability, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md)

## Context

D-0067: promotion fences the old primary's leases first and refuses while it may still hold expiring leases unless an operator acknowledges the fence. When the old primary is gone its API is gone too, so promotion is a CLI step on the candidate, like quoro restore.

## Decision

Manual promotion (#19): quoro cluster promote on the candidate, refused while the old primary may still hold expiring leases unless the operator acknowledges the fence; a returning old primary demotes itself and rejoins with quoro node rejoin.

quoro cluster promote [--ack-fence] on a role node machine, as root: checks its log is current to its last ack and shows the lag, opens its custody seals, collects term t+1 from a majority of voters (ADR 0169; under its fallback option B only the acknowledgement path exists), waits for the old primary's gate unless --ack-fence (recorded in the audit log with the operator's statement), replays the log into a fresh database, writes cluster.promoted, starts quoro.service and serves at term t+1. Nodes and the edge find it through welcome.primary_urls and the unchanged pin; they refuse desired_state with a lower term (stale_term). An old primary that sees a higher term stops writing (503 not_primary) and waits; quoro node rejoin quarantines its divergent log tail (section 7) and makes it a follower. After the promotion, warm apps whose leases expired are granted again only to their last holder (newest data), never moved. The maintainer accepted this on 2026-11-06.

_Some wording was changed for the public repository._

## Consequences

- [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md) refines the candidate builds from the log it froze, takes its term from the vote answers, and gains --accept-loss. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0182 in the project decision ledger. Do not edit by hand; changes go through a new decision.
