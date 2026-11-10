# ADR 0322: How a promoted primary gets the backup repository key back, and what the API shows until then

- **Status:** Accepted
- **Date:** 2026-11-10
- **Ledger id:** D-0322
- **Tags:** m5b, backups, promotion, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0147](0147-backup-repository-format-and-keys-fastcdc-2020-chunks-256k.md)

## Context

The repository key stays out of the change log, so a promoted primary rebuilds the backup_repositories row with an empty repo_key_enc (contract PR #795/#796 proposes that row and what is refused meanwhile). The contract says the key comes back from the owner's recovery key or the repository is re-keyed, but names no mechanism. Until one exists, a promoted cluster takes no backup, restores no volume and runs no garbage collection, and rotateBackupRecoveryKey (implemented) has no defined answer, because its response list has no 409. Choosing the mechanism adds API or CLI surface that takes a secret, so it needs the maintainer.

## Decision

How a promoted primary gets the backup repository key back, and what the API shows until then.

Three questions, each with options.

(1) How the key comes back.
(A) A new API operation, human-only and owner-only: POST /api/v1/backup-repositories/{id}/repository-key. It takes the recovery key, unwraps repo_key from the newest config object it opens (backup-format.md section 4), checks the fingerprint, and writes repo_key_enc on this primary only, never to the change log. Audited with the recovery key redacted (a new operation row, scope backup:manage). It works from the panel and the CLI, and only while the primary serves.
(B) A host command run as root on the primary, quoro backup recover-key, reading the recovery key from stdin. It needs no new API surface or redaction rule, but it needs shell access, and agents cannot run it.
(C) Re-key only: a new repository with a new repo_key. Old backups stay readable only with the recovery key, offline. Volume restores of older backups are lost through the API.
The maintainer chose (A) on 2026-11-10; re-keying (option C) stays the operator's path when the recovery key is lost. (A) keeps recovery in the audited API that a promoted cluster already serves.

(2) What rotateBackupRecoveryKey answers while the key is absent. Options: 409 conflict, which adds a response and needs a small Rust change for the openapi diff; or the operation first requires the key back. The maintainer chose that rotateBackupRecoveryKey answers 409 while the key is absent.

(3) Whether BackupRepository shows the absence. Options: a boolean field such as repository_key_present (an additive schema field with a Rust change); or nothing, with the gap seen only through backup.failed events with the error 'repository key absent'. The maintainer chose the field, so the panel can ask the owner for the recovery key.

The upgrade backup also fails while the key is absent (backup-format.md section 7.2 step 4), so an upgrade before recovery needs --skip-upgrade-backup. With (A), the operator starts once with that flag, then recovers the key.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0147](0147-backup-repository-format-and-keys-fastcdc-2020-chunks-256k.md): where the repository key lives after a promotion: never in the change log, and restored on a promoted primary from the recovery key. The rest of [ADR 0147](0147-backup-repository-format-and-keys-fastcdc-2020-chunks-256k.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0322 in the project decision ledger. Do not edit by hand; changes go through a new decision.
