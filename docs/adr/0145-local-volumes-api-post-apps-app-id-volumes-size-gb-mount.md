# ADR 0145: Local volumes API: POST /apps/{app_id}/volumes {size_gb, mount_path}, list, DELETE /volumes/{id} (destructive, JIT); the first volume makes the app stateful, pins it to the volume's node with a local lease; init config v3 mounts by serial; the host never mounts a volume a guest wrote

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0145
- **Tags:** m5a, contracts, engine
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0052 requires local volumes before the cold-backup DoD; protocol already reserves spec.vms[].volumes and stateful VMs need lease_id/fencing_token. Spike: m5a-backups-and-chunks.md section 4.

## Decision

Local volumes API: POST /apps/{app_id}/volumes {size_gb, mount_path}, list, DELETE /volumes/{id} (destructive, JIT); the first volume makes the app stateful, pins it to the volume's node with a local lease; init config v3 mounts by serial; the host never mounts a volume a guest wrote.

Contract B: openapi volume endpoints and Volume schema leave 'shape only'; scopes: reuse app:write for create, volume deletion is destructive (permission-model 4.1, JIT); engine: VolumeAttach implemented by the CH backend as sparse raw ext4 files created by the engine (like WorkDisk) with serial quoro-vol-`<n>`; quoro-init config v3 lists serial->mount_path; the primary grants a `local` lease (D-0047) when placing a stateful app; replicas stays 1. Backups read and restore write the file as bytes, never mounting it (the P-0100 rule). Recommendation: accept.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0145 in the project decision ledger. Do not edit by hand; changes go through a new decision.
