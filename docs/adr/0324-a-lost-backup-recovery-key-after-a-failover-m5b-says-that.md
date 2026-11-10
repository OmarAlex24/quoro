# ADR 0324: A lost backup recovery key after a failover: M5b says that re-keying is not served yet, and replacing the backup repository comes after M5b

- **Status:** Accepted
- **Date:** 2026-11-10
- **Ledger id:** D-0324
- **Tags:** m5b, backups, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0322](0322-how-a-promoted-primary-gets-the-backup-repository-key-back.md)

## Context

After a failover the new primary gets the backup repository key back through the restore operation, which needs the owner's recovery key (ADR 0322). The contract text said that an owner who has lost the recovery key re-keys the repository, but no operation does that: a second backup repository is refused and none can be deleted. The maintainer chose on 2026-11-10 to state the limit in M5b and plan the fix after it. Adding the operation in M5b, or allowing the repository to be deleted, were the options not taken.

## Decision

A lost backup recovery key after a failover: M5b says that re-keying is not served yet, and replacing the backup repository comes after M5b.

The contract says plainly that re-keying a backup repository is not served yet. Until it is, an owner who has lost the recovery key cannot take new backups after a failover, and backups already taken stay readable only with that key. An issue plans an owner-only operation that replaces the backup repository: it starts a new repository with a new key and keeps the old one read-only. That operation is designed and decided after M5b, with its own contract PR.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0322](0322-how-a-promoted-primary-gets-the-backup-repository-key-back.md): what an owner who has lost the recovery key can do after a promotion: re-keying the repository is not served yet, and replacing the repository is planned after M5b. The rest of [ADR 0322](0322-how-a-promoted-primary-gets-the-backup-repository-key-back.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0324 in the project decision ledger. Do not edit by hand; changes go through a new decision.
