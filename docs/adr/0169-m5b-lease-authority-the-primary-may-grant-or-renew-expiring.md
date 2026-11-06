# ADR 0169: M5b lease authority: the primary may grant or renew expiring leases only while a majority of voters supports it; promotion takes the next term from a majority

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0169
- **Tags:** m5b, reliability, witness
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

With the primary as the only lease authority the witness adds no safety: a primary that is cut off and is also a node keeps renewing its own node's expiring leases, so the DoD's 'refused while the old primary may still hold expiring leases' can never be decided and every promotion needs a fence acknowledgement. Tying the primary's right to grant and renew to a majority of voters makes it decidable and gives the third vote a real job (T-LEASE-08, T-AVL-03).

## Decision

M5b lease authority: the primary may grant or renew expiring leases only while a majority of voters supports it; promotion takes the next term from a majority.

The maintainer accepted option A on 2026-11-06: every voter (a machine with role node or witness) grants the primary a support window S over its existing session and promises not to support another candidate for a higher term until S has passed since its last contact with that primary (monotonic clocks, margins as in node-agent-protocol 8.4). The primary grants or renews expiring leases only while it holds support from a majority (counting its own host when it is a voter); without a majority its renewals stop, its own node's leases go uncertain and then expired, and they fence. Manual promotion (CLI on the candidate) asks the voters for term t+1 over the mesh; a voter agrees only after S has run out for the old primary; with a majority the candidate waits until the old primary's last possible grant has passed its gate (S + D + 2M) or the operator acknowledges the fence, then starts at term t+1. Nodes and voters refuse a lower term (new protocol error code stale_term). No automatic trigger and no log-commit quorum: both are M6 (D-0016). Option B, kept as the fallback: the witness only counts votes and the HA view reports three votes; lease authority stays the primary alone; promotion always needs the operator's fence acknowledgement whenever the old primary's node may hold an expiring lease. Cheap, meets the DoD literally, but the witness is decorative. Option C, not chosen: an external arbiter (R2 conditional write or Durable Object); deferred, needs Cloudflare setup and still needs tokens. If the lease work slips in M5b, a new decision can fall back to option B.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0169 in the project decision ledger. Do not edit by hand; changes go through a new decision.
