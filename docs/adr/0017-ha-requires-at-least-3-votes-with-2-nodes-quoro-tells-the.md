# ADR 0017: HA requires at least 3 votes; with 2 nodes Quoro tells the user and offers a witness

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0017
- **Tags:** reliability, witness
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Majority voting needs an odd number of voters; the witness only breaks ties and stores no data.

## Decision

HA requires at least 3 votes; with 2 nodes Quoro tells the user and offers a witness.

Witness options: (1) lightweight free daemon on any machine, must be in a different failure domain than the nodes (a home PC next to node A fails together with it); (2) optional external lease arbiter: Cloudflare Durable Object (strong consistency) or S3/R2 conditional writes (to verify). The self-hosted witness is free and first-class.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0017 in the project decision ledger. Do not edit by hand; changes go through a new decision.
