# ADR 0325: The fencing-token ceiling applies to every operation that raises a token: a raise at the ceiling writes nothing and answers 409

- **Status:** Accepted
- **Date:** 2026-11-10
- **Ledger id:** D-0325
- **Tags:** m5b, leases, fencing, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0321](0321-four-lease-and-voter-readings-every-waiting-grant-is-marked.md)

## Context

ADR 0321 says what happens at the token ceiling for one operation, the acknowledgement of a failed fence. Three other operations raise a token (declaring a node lost, removing a node, and the fence acknowledgement given by flag) and the contract said nothing for them. The ceiling is 2^53 - 1 and is not reached in practice, but an undefined path should not stay in the contract. The maintainer chose on 2026-11-10 to apply one rule to all of them.

## Decision

The fencing-token ceiling applies to every operation that raises a token: a raise at the ceiling writes nothing and answers 409.

Every operation that would raise a lease's fencing token past the highest token the change log carries is refused the same way: it writes nothing and answers 409 conflict with the reason that names the exhausted token. The contract gets one sentence that says so, in a standalone contract PR. The check in the three other code paths is done with the token-cap work of issue 767. If that would make issue 767 late for the M5b demo, the code for the three paths moves to an issue after M5b; the rule itself stands either way.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0321](0321-four-lease-and-voter-readings-every-waiting-grant-is-marked.md): which operations the token cap refuses: every operation that would raise a token past it, not only a fence acknowledgement. The rest of [ADR 0321](0321-four-lease-and-voter-readings-every-waiting-grant-is-marked.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0325 in the project decision ledger. Do not edit by hand; changes go through a new decision.
