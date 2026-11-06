# ADR 0248: Warm replication: a root_mismatch answer makes the source's base a lost base; the source deletes it and the next cycle ships a full copy

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0248
- **Tags:** m5b, warm, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0208](0208-the-target-applies-a-stream-into-a-staging-clone-checks-a.md)

## Context

A root_mismatch means the target's copy does not match the source's base, so keeping the base would fail every later incremental cycle the same way. The reading was accepted on 2026-11-06; the live volume is untouched and the cost is one full copy.

## Decision

Warm replication: a root_mismatch answer makes the source's base a lost base; the source deletes it and the next cycle ships a full copy.

Section 6.13: 'a failed cycle keeps the base' holds for every refusal except root_mismatch. On root_mismatch the source treats its base as lost (like a base lost to a crash): it deletes the base and next, and the next cycle ships every allocated range as a full copy. Section 6.13 states it; the sender in #448 already does this.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0208](0208-the-target-applies-a-stream-into-a-staging-clone-checks-a.md): what the source does with its base when the target refuses with root_mismatch. The rest of [ADR 0208](0208-the-target-applies-a-stream-into-a-staging-clone-checks-a.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0248 in the project decision ledger. Do not edit by hand; changes go through a new decision.
