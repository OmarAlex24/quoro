# ADR 0147: Backup repository format and keys: FastCDC 2020 chunks (256K/1M/4M), keyed BLAKE3 ids, zstd + XChaCha20-Poly1305, signed manifests; a repo key wrapped by the cluster DEK and by an owner recovery key shown once; the volume's node uploads directly with a credential sent in signed desired state

- **Status:** Accepted; refined by [ADR 0322](0322-how-a-promoted-primary-gets-the-backup-repository-key-back.md)
- **Date:** 2026-11-04
- **Ledger id:** D-0147
- **Tags:** m5a, contracts, sync, security
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0322](0322-how-a-promoted-primary-gets-the-backup-repository-key-back.md)

## Context

D-0023/D-0069 make the engine ours; a backup encrypted only under the cluster DEK is unreadable exactly when the primary is lost. Spike: m5a-backups-and-chunks.md section 6.

## Decision

Backup repository format and keys: FastCDC 2020 chunks (256K/1M/4M), keyed BLAKE3 ids, zstd + XChaCha20-Poly1305, signed manifests; a repo key wrapped by the cluster DEK and by an owner recovery key shown once; the volume's node uploads directly with a credential sent in signed desired state.

Contract B: backups resource (POST /apps/{app_id}/backups, GET list/one, POST /backups/{id}/restore {node_id}), scopes backup:read, backup:write, backup:restore (destructive, JIT); cold policy schedule {interval_h default 24, keep default 7}; tables backups and backup_repositories (provider_id, wrapped keys); events backup.succeeded/failed, restore.succeeded/failed; protocol spec.backups[] jobs for the node holding the volume, credential handled like tunnel tokens; restore creates a new volume on the target node, then moves placement; retention with mark-and-sweep GC on the primary with a grace period; backup and audit credentials are separate entries. Crates: fastcdc, blake3, zstd, aws-sigv4 over reqwest (all ADR 0080 allowed); rustic_core rejected (restic format, contradicts own engine). Recommendation: accept, including the recovery key.

## Consequences

- [ADR 0322](0322-how-a-promoted-primary-gets-the-backup-repository-key-back.md) refines where the repository key lives after a promotion: never in the change log, and restored on a promoted primary from the recovery key. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0147 in the project decision ledger. Do not edit by hand; changes go through a new decision.
