# ADR 0018: The edge/LB can be the third voter (role witness+edge of the same binary)

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0018
- **Tags:** reliability, witness, ingress
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

It sits in a third location, sees both nodes, and decides where traffic goes, so its health view is what users experience. The maintainer's idea: 2 VPS + 1 small LB VPS.

## Decision

The edge/LB can be the third voter (role witness+edge of the same binary).

Caveat: a single edge is a traffic SPOF. Mitigations: Cloudflare as the traffic edge with the small VPS only as witness, or two edges with DNS failover.

_Some wording was changed for the public repository._ _Some operational details were redacted for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0018 in the project decision ledger. Do not edit by hand; changes go through a new decision.
