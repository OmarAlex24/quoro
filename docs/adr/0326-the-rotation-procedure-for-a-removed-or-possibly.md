# ADR 0326: The rotation procedure for a removed or possibly compromised node-role machine includes user passwords

- **Status:** Accepted
- **Date:** 2026-11-10
- **Ledger id:** D-0326
- **Tags:** m5b, security, contracts, custody
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0320](0320-say-that-every-node-role-machine-can-read-the-provider.md)

## Context

ADR 0320 gives the list of what to rotate when a node-role machine is removed or may be compromised. User password hashes travel in the change log (ADR 0317), so such a machine holds them. They are Argon2id hashes and cannot be read, but a weak password can be guessed offline. The list did not name them. The maintainer chose on 2026-11-10 to add them to the procedure as text. Not sending the hashes to the nodes was not taken: after a failover nobody could log in until passwords were reset.

## Decision

The rotation procedure for a removed or possibly compromised node-role machine includes user passwords.

The rotation procedure gains one line: users change their passwords. Personal tokens are named in the same line if their hashes travel in the change log too. This is procedure text only, with no code change in M5b.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0320](0320-say-that-every-node-role-machine-can-read-the-provider.md): what the rotation procedure covers: users change their passwords too. The rest of [ADR 0320](0320-say-that-every-node-role-machine-can-read-the-provider.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0326 in the project decision ledger. Do not edit by hand; changes go through a new decision.
