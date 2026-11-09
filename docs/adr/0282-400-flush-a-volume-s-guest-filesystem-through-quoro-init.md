# ADR 0282: #400: flush a volume's guest filesystem through quoro-init (syncfs over the exec port) before every volume snapshot and before a switchover freeze, with a crash-consistent fallback

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0282
- **Tags:** m5b, engine, init, backup, volumes, warm, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

A host-side volume snapshot misses data the guest wrote without fsync in about the last 35 s (ext4 delayed allocation and writeback); measured 0 bytes 8 s after a write, as on the lab. It affects cold backups on both methods, warm cycles, and a planned switchover's final snapshot, where the paused guest's dirty pages are lost on both nodes. Fixing it changes the exec framing (engine-trait section 6), section 3.18's only-caller rule, section 3.16's sequencing and the snapshot descriptor, so it needs a decision; contract changes are standalone PRs.

## Decision

#400: flush a volume's guest filesystem through quoro-init (syncfs over the exec port) before every volume snapshot and before a switchover freeze, with a crash-consistent fallback.

### Problem (#400)

Every volume snapshot is taken on the host: `FICLONE` of the volume file for `Reflink`, a copy of a paused VM's volume file for `PauseCopy` (engine-trait section 3.16). Data the app wrote without `fsync` sits in the guest's page cache, not in the file. With ext4's defaults (`data=ordered`, delayed allocation, `commit=5`) and the kernel's writeback defaults (`dirty_expire_centisecs` 3000, `dirty_writeback_centisecs` 500), such data reaches the file up to about 35 s later. On the lab, a file written 8 s before a backup came back with 0 bytes.

The same gap covers every path that snapshots a volume:

- **Cold backup** (section 6.7), on both methods. A `PauseCopy` pause freezes the vCPUs with the dirty pages still in guest RAM, so the paused copy misses them too.
- **Warm cycle** (section 6.13). Each cycle ships a snapshot. Unflushed data reaches the target one or more cycles late, and is lost on a failover.
- **Planned switchover** (`switchover_freeze`, section 5.17, ADR 0177). The source pauses the VM and then takes the final snapshot, so the guest's dirty pages are not in it. The `lease_revoke` that follows stops the VM gracefully, but a graceful stop never resumes a VM that `pause` froze (section 3.8, ADR 0270). The stop is then forced, and those pages are lost on both nodes. Section 9's claim that "the old source's volume holds the same data" stays true. But a planned switchover drops writes the app saw complete (`write()` returned, no `fsync`) in the last ~35 s before the freeze. For a planned move, nobody expects power-cut consistency.

Data that was `fsync`ed is in every snapshot. Apps that rely on `fsync` lose nothing they committed, for example Postgres, or SQLite in its default mode. Apps that write files and never `fsync` are exposed: uploads, generated assets, many frameworks' file stores.

### Measurement

The setup:

- a privileged Linux container (kernel 7.0), with an ext4 image on a loop device, made with the engine's `mkfs.ext4` options and mounted `nosuid,nodev` like the init's volume mount;
- the image copied from outside the filesystem while the writer is idle, which holds the same bytes as a clone;
- the journal replayed (`e2fsck`), as a restore does;
- that kernel's writeback sysctls: expire 30 s, writeback interval 15 s. No sysctl was changed.

| Write, no `fsync` | Mount options | Before the copy | Copy holds |
| --- | --- | --- | --- |
| New 1 MiB file | defaults | wait 8 s | **0 bytes** (the #400 result) |
| New 1 MiB file | defaults | `syncfs` on the mount, then copy at once | the new data |
| New 1 MiB file | defaults | `FIFREEZE`, then copy at once | the new data |
| New 1 MiB file | `nodelalloc` | wait 8 s | the new data |
| In-place overwrite of 1 MiB already on disk | `nodelalloc` | wait 8 s | **the old data** |
| In-place overwrite of 1 MiB already on disk | defaults | wait 8 s | **the old data** |
| New 1 MiB file | defaults | wait 40 s | the new data |

This was not a microVM and not a reflink filesystem. The lab saw the same 0-byte result on a real VM.

### Options

What each option does to the bounds the snapshot has today:

- `Reflink`'s zero pause;
- `PauseCopy`'s pause;
- the warm `interval_s`: 30 s by default, 5 s at least;
- the switchover freeze.

#### A. Flush the volume's filesystem in the guest before the snapshot (`syncfs` through quoro-init over vsock)

**How it works:**

1. If a VM that attaches the volume is running and not paused, the engine asks its init, over the vsock port the init already listens on, to `syncfs` that volume's mount.
2. It waits at most a short fixed bound (proposed 2 s), then clones (`Reflink`) or pauses and copies (`PauseCopy`).
3. If there is no answer within the bound, an error, an old init, or no vsock device, the engine takes the snapshot anyway. That snapshot is crash-consistent, as today.

**What it costs and covers:**

- **Bounds:**
  - the guest keeps running during the flush, so `Reflink`'s pause stays zero;
  - the flush runs before the `PauseCopy` pause, so that pause does not grow;
  - a warm cycle gains at most the bound, which fits the 5 s minimum interval.
- **Residual window:** writes between the `syncfs` return and the clone, in milliseconds. They are crash-consistent as today.
- **Paths:**
  - cold backup and warm cycles: covered;
  - switchover: covered if the source flushes right before the `pause` (below), with the same millisecond window.
- **Writeback load:** the flush forces writeback the kernel would do within ~35 s anyway. At the 30 s default interval the load barely changes. At 5 s, writeback is more frequent, which means smaller and more frequent writes.
- **Old init:** an M4 or M5a init refuses any first frame other than `open`. It answers `131` error ("the first frame must be open, got type 5", in `crates/init/src/guest/exec/session.rs`) and closes. The engine reads that as "unsupported" at once, without waiting out the bound. A VM gets the new init at its next start, as with every init change.
- **Init code:** `rustix::fs::syncfs` is a safe call in a feature the init already enables. No new dependency, and no `unsafe`.

**This is a contract change:**

- section 6's exec framing gains a session kind;
- section 3.18 says the web terminal is exec's only caller;
- section 3.16 says a guest-side freeze "is not part of M5a";
- the snapshot descriptor should record whether the flush happened.

#### A′. Freeze the filesystem (`FIFREEZE`/`FITHAW`) around the clone

Same path as A. The init freezes the mount, the host clones, and the init thaws.

**Gain over A:**

- no residual window;
- a clean journal in the snapshot.

**Costs:**

- every writer to the volume blocks for the clone;
- the init needs an auto-thaw for when the host never thaws (engine restart, vsock drop), or the app hangs on its next write;
- rustix exposes `ioctl` only as `unsafe` and has no freeze wrapper, so the init would need a new dependency or an `unsafe` exception, and the workspace forbids `unsafe`.

**Fit:** for `PauseCopy`, a freeze for the whole copy (62 to 98 s for 5 GiB, ADR 0156) only adds a second freeze to the pause. It is worth revisiting for the switchover's final snapshot only.

#### B. Document the window

Section 3.16, the backup docs and the release notes would say that data written without `fsync` in about the last 35 s may be missing from a backup, from a warm copy and from a switchover's final snapshot, while data that was `fsync`ed is always in them.

**Fit and limits:**

- it fits the current contract, needs no code, and changes no bound;
- it fixes nothing, and a planned switchover still drops writes the app saw complete.

#### C. Change the init's volume mount options

Section 6 item 10 fixes no mount options, so this fits the contract text. It changes every app's write path, though, so it is a product decision.

**Measured:**

- `nodelalloc` covers new files within one journal commit (5 s), but not in-place overwrites. Those stay up to ~35 s, the pattern of a database that rewrites its pages.
- `data=journal` would cover both within 5 s, but it writes all data twice.
- the `sync` mount option makes every write synchronous.

**Limits:**

- every variant leaves a window of at least one commit;
- it costs every app all the time, not only at a snapshot;
- it does nothing for the switchover's paused guest.

### Recommendation

**A now, with B for what A cannot reach.** The contract change set is below. It would be one standalone contract PR, then the implementation PRs.

1. **engine-trait section 6, exec framing.** Host-to-guest frame `5` sync is valid only as a session's first frame, in place of `1` open. Its payload is JSON `{"serials":["quoro-vol-0", ...]}`, and the guest answers:
   - `132` synced, with an empty payload, after `syncfs` on each named volume's mount;
   - `131` error, followed by a close.

   The session then ends. A sync session starts no program and allocates no PTY. The exec listener never logs it to the app's console. Reusing the exec port needs no configuration version bump. The fallback is how an older init already answers a first frame that is not `open`: `131`, then a close. A separate port would mean `quoro-init-config v4`, and a VM must restart for the new init either way.
2. **Section 3.18.** "The web terminal is its only caller" becomes: the web terminal is the only caller of `open`, and the engine's own volume flush the only caller of `sync`. Neither the API nor MCP exposes `sync`. A terminal user cannot send it, because the engine frames their input as `2`.
3. **Section 3.16, sequencing.**
   - Before the `FICLONE` or the `PauseCopy` pause, the engine sends a sync session to the init of the running VM that attaches the volume, and waits at most 2 s.
   - It skips the flush for a VM that `pause` froze (section 3.22), since that guest cannot answer.
   - The crash-consistency clause stays as the fallback guarantee. A snapshot whose flush succeeded also holds every write the guest completed before the flush.
   - "A guest-side freeze ... is not part of M5a" is replaced by this.
4. **Section 3.22 and protocol section 5.17.**
   - A new engine method, `flush_volume(volume_id, within) -> GuestFlush`, with a default implementation that returns `Unsupported`.
   - `switchover_freeze` calls it before `pause`, then pauses and takes the final snapshot.
   - The app keeps serving during the flush, and the freeze does not lengthen.
5. **Snapshot descriptor and status.**
   - `VolumeSnapshot` gains `guest_flush: GuestFlush`, one of `Flushed`, `NoRunningVm`, `Paused`, `Unsupported`, `TimedOut` or `Failed`. It is additive, in M5b.
   - `status_report.backups` gains `guest_flush`, so an operator can see which backups are only crash-consistent.
   - The backup manifest format is unchanged.
6. **B, in the same release.** The docs and release notes describe the ~35 s window for VMs still on an older init and for a snapshot whose flush timed out or failed. They also say that apps needing more than "everything completed before the snapshot" still use their own dump (ADR 0023).

**Not now:**

- A′, which needs a dependency or an `unsafe` exception and a thaw watchdog;
- C, which taxes every write and still leaves overwrites at ~35 s.

**Open question for the maintainer:** should a failed or timed-out flush stay a successful but marked snapshot (recommended: a backup is still better than none), or should it fail the backup?

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0282 in the project decision ledger. Do not edit by hand; changes go through a new decision.
