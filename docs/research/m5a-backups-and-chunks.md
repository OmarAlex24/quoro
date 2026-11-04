# M5a spike: local volumes, the chunk engine, cold backups and the off-site audit copy

- **Question:** what do local persistent volumes (ADR 0052), the content-addressed chunk engine (ADR 0023, ADR 0069), client-side encrypted backups to S3-compatible storage through the provider interface (ADR 0025, ADR 0051), consistent SQLite snapshots, restore, and the object-locked off-site audit copy (ADR 0031, ADR 0053) need? Which crates can be reused under the license policy (ADR 0080), and what must be written? This note also answers the research task the roadmap requires before M5a: object lock per provider (R2, S3, B2, MinIO).
- **Researched:** 2026-11-04. Desk research against `main` and the providers' primary documentation, plus read-only facts from lab-1 and lab-2. **No prototype ran.** Provider behaviour below is quoted from documentation, not exercised against a bucket.
- **Status:** design proposal for the M5a contract PR. The object-lock findings contradict the literal wording of ADR 0053, so they are raised for a superseding decision.

## Short answer

1. **Object lock is not one feature across providers, and ADR 0053's "compliance mode" exists only on S3-API providers.** AWS S3 and Backblaze B2 have S3 Object Lock with compliance mode. **Cloudflare R2 implements none of the S3 object-lock API.** It has its own prefix-scoped "bucket lock rules", set through the Cloudflare API, that refuse overwrite and delete with `409`, and anyone who may edit the bucket's settings can remove them. On S3 a plain `PUT` to a locked key *succeeds* by creating a new version, and a plain `DELETE` *succeeds* by adding a delete marker: only the locked version itself is immutable. MinIO's community edition was archived on 2026-04-25. The DoD's "an overwrite or delete is refused" therefore needs a per-provider definition. See [section 1](#1-object-lock-per-provider-the-pre-m5a-research-task).
2. **The chunk engine is ours, built on small permissive crates.** FastCDC chunking (`fastcdc`, MIT), keyed BLAKE3 chunk ids (`blake3`, Apache-2.0 option), zstd compression (`zstd`, BSD-3-Clause), XChaCha20-Poly1305 (`chacha20poly1305`, already a dependency) and a thin S3 client (SigV4 through `aws-sigv4` over the existing `reqwest`). Reusing a whole backup repository library (`rustic_core`) would contradict ADR 0023 and ADR 0069 and import restic's format, so it is not recommended.
3. **Backups need a key that survives the primary.** Chunks encrypted with the cluster data key (ADR 0050) are unreadable if the primary and its key are lost, and losing the primary is when a restore matters. Recommendation: a separate backup repository key, wrapped by the cluster key for daily use and also by a recovery key shown once to the owner.
4. **Snapshots: reflink where the filesystem has it, a short pause where it does not.** ADR 0062 chose reflink snapshots for M5a. Both lab VMs have an ext4 root, and ext4 has no reflink. Recommendation: probe `FICLONE` on the volume directory. Use reflink when it works (XFS, btrfs). Otherwise pause the VM through Cloud Hypervisor's `vm.pause`, copy the sparse volume file, and resume. The panel shows the pause length. **Hypothesis** until the volumes epic measures it.
5. **SQLite: `VACUUM INTO`.** It produces a transactionally consistent, minimal copy with deleted content purged, and it is not a write operation, so it does not fail on a held write lock (sqlite.org). The backup API is cheaper in CPU and incremental, but it copies freed pages, and those can still hold rotated secrets.

## Findings

### 1. Object lock per provider (the pre-M5a research task)

| Provider | Mechanism | Modes | Overwrite of a protected key | Delete of a protected key | Enable on existing bucket | Label |
| --- | --- | --- | --- | --- | --- | --- |
| AWS S3 | S3 Object Lock on object versions; needs versioning | Compliance (no one, root included, can shorten or remove) and governance (bypass with `s3:BypassGovernanceRetention`) | `PUT` succeeds and creates a **new version**; the locked version stays | Plain `DELETE` returns `200` and adds a **delete marker**; `DELETE` with a version id returns `403` | Yes (per AWS docs; not re-checked here) | Confirmed (AWS docs) |
| Backblaze B2 | S3-compatible Object Lock and native API | Compliance and governance; compliance retention "cannot be removed by any user, but … can be extended" | Not stated on the page read | Changes and deletes before the date "will fail" | Yes; it cannot be disabled afterwards | Confirmed (B2 docs); version semantics **Hypothesis** |
| Cloudflare R2 | "Bucket lock rules" per prefix, set through the dashboard, `wrangler` or the Cloudflare REST API. **No S3 object-lock API:** `PutObjectLockConfiguration`, the `x-amz-object-lock-*` headers and bucket versioning are all listed as unimplemented | Age, date or indefinite retention; no compliance or governance distinction; rules can be removed by editing the bucket's configuration | Refused: `409 ObjectLockedByBucketPolicy` | Refused: `409`; in `DeleteObjects`, a per-key error | Yes; rules are bucket configuration | Confirmed (R2 docs) |
| MinIO (community) | S3 Object Lock | Compliance and governance | As S3 | As S3 | Bucket created `--with-lock` | Repository archived 2026-04-25 (GitHub API); lock behaviour **Hypothesis** |
| RustFS 1.0.4 (Apache-2.0) | Documents S3 Object Lock and versioning | Compliance, governance, legal hold | As S3 (documented) | As S3 (documented) | n/a | **Hypothesis**, untested |

What this means for the audit copy (`audit-log-format.md` §8):

- **Never overwrite, on every provider.** The exporter writes every segment and head object with `If-None-Match: *`. AWS returns `412` when a current version exists (AWS conditional writes docs), and R2 supports the same header (R2 S3 API docs). So Quoro itself never creates a second version, and its own retries cannot cause trouble.
- **What "refused" can mean.** On S3 and B2, "an overwrite or delete is refused" is literally true only for operations on the locked *version*. An attacker with a put credential can still hide a segment behind a delete marker or a newer version, and the verifier must read by version id, or list versions, to see past that. On R2 an overwrite or delete of a key under a lock rule is refused outright. But the lock is bucket configuration, so a stolen Cloudflare token that may edit R2 settings can remove the rule. On S3 compliance mode even the root user cannot do that.
- **How Quoro sets the lock.** On S3 and B2: a default retention on the bucket, or `x-amz-object-lock-mode: COMPLIANCE` plus a retain-until date per object. AWS has required an integrity header (`Content-MD5` or an `x-amz-checksum-*`) on such a `PUT` (**Hypothesis**, verify in the epic). On R2 Quoro cannot set the lock through the S3 API at all. Either the operator adds a rule on the audit prefix, or Quoro adds it through the Cloudflare account credential of a provider entry (ADR 0051).
- **The probe** (ADR 0053) becomes: write a probe object under the audit prefix, try to overwrite it and delete it with the exporter's own credential, then read it back. On R2, expect `409`. On S3 and B2, expect the version to survive and its retention to read back as `COMPLIANCE`. Report the strength the store gave: `compliance` (S3, B2) or `bucket_rule` (R2). Report `none`, and never "locked", when the probe fails.

### 2. Lab facts (read-only, 2026-11-04)

| Fact | lab-1 | lab-2 |
| --- | --- | --- |
| Root filesystem | ext4 (`discard`), 30 GB, 5% used | ext4 (`discard`), 30 GB, 27% used |
| Reflink on the volume path | No: ext4 has no reflink | No |
| Kernel modules available | `btrfs`, `xfs`, `dm-thin-pool`, `loop` (all `=m`) | same |
| Memory | 2 GB | 4 GB |

No Quoro node writes volumes yet: `VmSpec.volumes` is rejected by the Cloud Hypervisor engine with an error that says volumes ship in M5a, citing ADR 0052 (`crates/engine/src/ch/engine.rs`).

### 3. Crates (crates.io metadata, 2026-11-04)

| Need | Crate | Version, license | ADR 0080 | Recommendation |
| --- | --- | --- | --- | --- |
| Content-defined chunking | `fastcdc` | 5.0.0, MIT; optional `tokio` and `futures` features only | Allowed | Use the 2020 algorithm |
| Chunk id (keyed hash) | `blake3` | 1.8.7, `CC0-1.0 OR Apache-2.0 OR Apache-2.0 WITH LLVM-exception` | Allowed through the Apache-2.0 option | Use keyed mode |
| Compression | `zstd` (C library through `zstd-sys`) | 0.14.3, BSD-3-Clause | Allowed | Use; `ruzstd` (MIT, pure Rust) is the fallback if a C build is unwanted |
| AEAD | `chacha20poly1305` | 0.11.0, Apache-2.0 OR MIT, already in the workspace | Allowed | Reuse XChaCha20-Poly1305 (ADR 0050) |
| S3 request signing | `aws-sigv4` | 1.6.4, Apache-2.0 | Allowed | Use with the workspace's `reqwest` |
| Full S3 SDK | `aws-sdk-s3` | 1.157.0, Apache-2.0 | Allowed | Not recommended: a large dependency tree for a handful of calls |
| Generic object store | `object_store` | 0.14.2, MIT/Apache-2.0 | Allowed | Fallback; object-lock headers are **not known** to be exposed (**Hypothesis**) |
| Multi-backend storage | `opendal` | 0.59.5, Apache-2.0 | Allowed | Not needed for S3-compatible only |
| SQLite | `sqlx` (sqlite) | already in the workspace | Allowed | Run `VACUUM INTO` through the existing pool |
| Whole backup repository | `rustic_core` | 0.13.0, Apache-2.0 OR MIT | Allowed | Rejected: ADR 0023 and 0069 make the engine ours, and its format is restic's |

## Design (Hypothesis until built and tested on the lab)

### 4. Local volumes (ADR 0052)

- **Shape.** A volume is a sparse raw file in the node's data directory, created by the engine as an empty ext4 filesystem of `size_gb`. It is the same path the M4 work disk uses (`WorkDisk`, engine-trait §3.6). It is attached as a virtio-blk disk with serial `quoro-vol-<n>` and mounted by `quoro-init` at `mount_path`, which needs one more key in the init config (v3: a list of serial and mount path pairs).
- **The host never mounts a volume after a guest has written it.** This is the rule M4 adopted for builder disks: a guest-written filesystem is untrusted input to the host kernel. Backups read the file as bytes, and restore writes bytes. Nothing is mounted.
- **API.** `POST /api/v1/apps/{app_id}/volumes {size_gb, mount_path}`, `GET` to list, and `DELETE /api/v1/volumes/{volume_id}`, which is destructive and so goes through JIT approval (permission-model §9). The first volume makes the app `stateful` and pins it to the volume's node. A stateful app keeps `replicas = 1`.
- **Lease.** A stateful VM needs a `lease_id` and a `fencing_token` (protocol §5.11). For `none` and `cold` the lease is `local`: granted by the primary at placement, it never expires on control-plane loss (ADR 0047). M5a needs only that grant. Expiring leases and fencing stay in M5b.
- **Redeploy.** The volume belongs to the app, not to the instance (engine-trait §3.9), so a redeploy attaches the same file to the new VM. This is DoD line 3.

### 5. Snapshot primitive (ADR 0062 on hosts without reflink)

| Host filesystem under the node's data dir | Snapshot | Guest impact | Label |
| --- | --- | --- | --- |
| XFS (reflink on by default in current `mkfs.xfs`) or btrfs | `ioctl(FICLONE)` of the volume file into a snapshot file, then a chunk scan of the snapshot | None; crash-consistent at the instant of the clone | **Hypothesis** (standard Linux behaviour, not run here) |
| ext4 (both lab VMs, and Debian's default) | `vm.pause` through the Cloud Hypervisor API, sparse copy of the file, `vm.resume`, then a chunk scan of the copy | Paused for the copy: about the used size divided by disk throughput | **Hypothesis**; measure in the volumes epic |

The probe runs once per node at start-up and reports `snapshot: reflink | pause_copy` with the node's capabilities. A crash-consistent image of an ext4 volume replays its journal on restore, which is the same guarantee as a power cut. Databases that need more than that use their native dumps (ADR 0023). A future guest-side freeze (`fsfreeze` through the exec channel, ADR 0113) can shorten the pause. It is not needed for `cold`.

### 6. Chunk engine and repository format

- **Chunking.** FastCDC 2020 with min/avg/max of 256 KiB / 1 MiB / 4 MiB for volumes. A full scan of the snapshot on every backup (ADR 0062) keeps the design simple. Only chunks the repository lacks are uploaded.
- **Chunk id.** `BLAKE3-keyed(id_key, plaintext)`. A plain content hash would let anyone who can read the bucket test whether a known file is in a backup. A keyed id does not leak that, and dedup still works within one repository.
- **Chunk object.** `zstd(level 3)`, then XChaCha20-Poly1305 with a random 24-byte nonce under `data_key`, with the chunk id as associated data. Key: `<prefix>/<cluster_id>/chunks/<first 2 hex>/<id>`. Written with `If-None-Match: *`, so a duplicate upload is a harmless `412`.
- **Manifest.** One encrypted object per backup: backup id, source (volume or control-plane database), the ordered chunk ids with offsets, logical size, snapshot method, Quoro version and time. It is signed with the cluster signing key, so a restore can tell a manifest Quoro wrote from one an attacker planted.
- **Keys.** A random `repo_key` per repository, from which `id_key` and `data_key` are derived with keyed BLAKE3 derivation. `repo_key` is stored wrapped twice: under the cluster data key, for unattended backups, and under a recovery key the owner sees once at setup and must keep. Restoring onto a new cluster asks for the recovery key. Without it, a backup outlives the primary only in name.
- **Who uploads.** The node that holds the volume reads its snapshot and uploads directly, so app data never passes through the primary. The bucket credential reaches that node only while it has a backup job. It travels inside the signed desired state and is stored encrypted with the node-local key, as tunnel tokens are (ADR 0094). The audit-copy credential is a separate, put-only provider entry and never goes to a node (architecture §11).
- **Retention and garbage collection.** Keep the last N backups per volume (`cold` default: daily, keep 7). After a manifest is deleted, a mark-and-sweep run on the primary lists the manifests, computes the live chunk set and deletes the unreferenced chunks. Deleting needs a credential that may delete, so the backup credential and the audit credential must be different entries.
- **Restore.** A restore creates a new volume on the target node, writes the chunks back in order into a sparse file (zero chunks stay holes), verifies the manifest's digests, and then moves the app's placement to that node and deploys it. The old volume is kept until an operator deletes it. This is DoD line 4: back up on lab-1, restore on lab-2.

### 7. Control-plane database and catalog templates (ADR 0069)

- The primary runs `VACUUM INTO '<tmp>/state-<seq>.db'` on a schedule (default every 6 hours, and before an upgrade). It feeds the file through the same chunk engine as a source of kind `control_plane`, then deletes the temporary file. `VACUUM INTO` needs an empty or missing target and must not run inside an open transaction on the same connection (sqlite.org).
- A restore of the control plane is a CLI step on a fresh host (`quoro restore --from <provider> --recovery-key …`). M5a needs it to work once on the lab, per the threat model's M5a check. A panel flow for it is later.
- A catalog template with SQLite data is just an app with a volume, so it is covered by section 4 to section 6. A Litestream backend remains optional and later (ADR 0069).

### 8. Off-site audit copy (ADR 0031, ADR 0053)

The segment format, keys and resume rules in `audit-log-format.md` §8 stand. The changes are the probe and the strength reporting from section 1, plus `If-None-Match: *` on every put. Detection of tail truncation compares the local chain's head with the latest exported `head-<seq>.json`. A local `last_seq` below an exported one, or a hash mismatch at an exported seq, raises `audit.chain_broken`.

### 9. HA policy cost (ADR 0020, ADR 0068)

For `none`: nothing reserved. For `cold`: the bucket storage (the logical size of the last backup plus the unique chunks of the kept ones, read from the manifests) and an estimated restore time (bytes divided by a measured or configured download rate). Both are computed on the primary from data it already holds. The panel shows them before and after a policy switch, which is DoD line 5.

## Open risks and unknowns

- **R2's lock is weaker than compliance mode.** It is removable by whoever can edit the bucket. The panel must say so, and the maintainer must accept that the lab demo shows `bucket_rule` strength.
- **The pause-and-copy fallback** stops the app for the length of the copy. On a large volume on slow disk, that can be minutes. The cost estimate must show it, and XFS for the node's data directory should be documented as the recommended setup.
- **Key loss.** A lost recovery key plus a lost primary means unreadable backups. That is by design, and the setup flow must make it impossible to miss.
- **Garbage collection racing an upload.** A sweep must not delete a chunk that an in-flight backup is about to reference. Mitigation: a grace period on chunk age before deletion. **Hypothesis**, needs a test.
- **Provider quirks not exercised:** multipart upload for chunks larger than one `PUT` (not needed with a 4 MiB maximum), B2's version semantics, R2 lock rules set through the API by Quoro itself, and request rate limits on listing during the sweep.
- **Testing without a cloud bucket.** CI needs an S3-compatible server. MinIO's community edition is archived. RustFS (Apache-2.0) documents object lock but was not tried. **Hypothesis.**

## Sources

- AWS S3 Object Lock: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html (accessed 2026-11-04)
- AWS S3 conditional writes: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html (accessed 2026-11-04)
- Cloudflare R2 bucket locks: https://developers.cloudflare.com/r2/buckets/bucket-locks/ (accessed 2026-11-04)
- Cloudflare R2 S3 API compatibility: https://developers.cloudflare.com/r2/api/s3/api/ (accessed 2026-11-04)
- Backblaze B2 Object Lock: https://www.backblaze.com/docs/cloud-storage-object-lock (accessed 2026-11-04)
- MinIO repository (archived): https://github.com/minio/minio (GitHub API, accessed 2026-11-04)
- RustFS repository and releases: https://github.com/rustfs/rustfs (GitHub API, accessed 2026-11-04)
- SQLite `VACUUM INTO`: https://www.sqlite.org/lang_vacuum.html (accessed 2026-11-04)
- Crates: https://crates.io/crates/fastcdc, https://crates.io/crates/blake3, https://crates.io/crates/zstd, https://crates.io/crates/ruzstd, https://crates.io/crates/aws-sigv4, https://crates.io/crates/aws-sdk-s3, https://crates.io/crates/object_store, https://crates.io/crates/opendal, https://crates.io/crates/rustic_core (accessed 2026-11-04)
- Contracts: `docs/contracts/audit-log-format.md` §8, `docs/contracts/engine-trait.md` §3.6, §3.16, `docs/contracts/node-agent-protocol.md` §5.11, `docs/contracts/db-schema.sql` (`volumes`, `audit_exports`)
- Previous research: `docs/research/r2-conditional-writes.md`, `docs/research/cloud-hypervisor.md` (snapshots hold no disk contents)
