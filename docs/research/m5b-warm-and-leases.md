# M5b spikes: warm diff, lease timing, change-log size and the lab cut

- **Question:** do the M5b plan's defaults hold on the lab before the contract PRs fix them? Specifically: can two reflink snapshots be diffed through their extent maps without missing a range (ADR 0172), what interval that allows (ADR 0173), whether the expiring-lease timings and their drift assumption hold (ADR 0171, node-agent-protocol §8.4), how big the change log is and how long a promotion replay takes (ADR 0179), and whether the nftables cut works as the lease tests need (ADR 0185).
- **Measured:** 2026-11-06 on the three-machine lab running v0.2.0: lab-2 is the primary and a node, lab-1 is a node with the XFS volume pool, lab-3 is the edge. Each lab machine is a VM with 2 vCPUs on an Intel i7-10700T host and a virtual disk. Only tools already on the machines were used (python3, `xfs_io`, `nft`, `systemd-run`, `sha256sum`, `dd`, `ping`, curl). Nothing was built on the lab.
- **Status:** measurements. Where a number is extrapolated or a tool was missing, the text says so and labels it **Hypothesis**.

## Short answer

| Spike | Result | Decides |
| --- | --- | --- |
| S1 warm diff | Over 50 cycles at each size, the FIEMAP diff of two consecutive clones found exactly the 4 KiB blocks that were written and missed none. After the 50 cycles the target copy's sha256 matched the source snapshot. The diff took 18–20 ms (median) and 45 ms at most. The clone itself took 0.14–0.18 s (median) and up to 2 s once the file had about 9,700 extents. A full hashed scan takes about 3 s per GiB, limited by the disk. 5 GiB did not fit in the space budget; 1 and 1.5 GiB were measured. | ADR 0172 holds as written. ADR 0173: keep the **30 s** interval default and the 5 s minimum. |
| S2 lease timing | Clock rate differences between the machines: at most 4.2 ppm on `CLOCK_MONOTONIC`. That is at most 0.063 ms over a 15 s lease, against M = 1,500 ms. Session-path RTT p99 is 0.75 ms (TCP connect). A 256 MiB VM stops gracefully in 57–78 ms and is forced off in 2–28 ms, so the time budget T_grace + T_force = 1.3 s ≤ M has a margin of more than 10×. A primary restart brings the API back in 0.2 s and the nodes in about 1 s, but one run of three took **26 s** for one node (#359). | ADR 0171: D = 15 s, M = 1.5 s, T_grace 1 s and T_force 0.3 s stand. The §8.4 drift **Hypothesis** is confirmed on this lab. The maintenance window is needed, and #359 must be fixed first. |
| S3 cut dry run | The nftables table cut lab-1 off in one step. The primary marked lab-1 disconnected 14.2 s later, and the mesh stopped. lab-1 kept its internet, and the operator's SSH session kept working. The dead-man timer removed the table 100.4 s after the cut. A node-agent restart during the cut rebuilt only the agent's own tables. | ADR 0185: the method works. The script is kept for E1. |
| S4 change-log size | `log_seq` 7,262 after 3.1 days (about 2,300 entries a day); 6.8 MB of a 10.2 MB database. Verifying the whole chain takes 0.17 s, and a stand-in replay into a fresh database takes 0.04 s. | ADR 0179: replay at promotion takes seconds. No snapshot or compaction is needed in M5b. |

## S1: warm diff on the XFS volume pool

**Setup.** The test ran on lab-1's loop-mounted XFS pool (`reflink=1`), in a scratch directory next to the volumes; no app volume was touched. Each run did the following:
1. Fill a scratch file ("live") with random data.
2. Clone it with `FICLONE`, as cold backups do (snapshot 0).
3. Clone snapshot 0 again as the "target" copy, the way the receiver's staging copy starts.
4. Repeat 50 cycles. In each cycle:
   - About 100 random writes go to "live": 60% 4 KiB pages, 30% odd sizes up to 64 KiB, 10% 64 KiB blocks.
   - Every 10th cycle also writes a 16 MiB burst, and cycle 25 writes nothing.
   - Clone "live" into snapshot k.
   - Read the FIEMAP extent lists of snapshots k−1 and k. Every logical range whose physical block is not the same in both is a changed range.
   - Copy those ranges from snapshot k into the target, then delete snapshot k−1.
5. After cycle 50, compare the sha256 of the target and of snapshot 50.

**Sizes.** The pool had 11.5 GB free, so the 20% budget was 2.3 GB.
- A 1 GiB run peaked at 2.06 GB of pool space.
- A 1.75 GiB run passed 2.3 GB while the file was still being filled, and the space guard stopped it.
- 1.5 GiB fit (peak 2.13 GB). That run used a 4 KiB CoW extent-size hint, which made no measurable difference to the timings.
- **5 GiB was not run.** Its numbers below are extrapolated.

Peak pool use was about 2× the file size. That is the file, its clone and the copy-on-write blocks of the overwrites, plus XFS's speculative preallocation (**Hypothesis** for the split between these).

| | 1 GiB | 1.5 GiB |
| --- | --- | --- |
| Diff (two FIEMAP reads + compare), median / p90 / max | 19.5 / 35.1 / 38.4 ms | 18.4 / 32.3 / 45.0 ms |
| of which FIEMAP reads, median | 12.6 ms | 11.6 ms |
| Clone (`FICLONE`), median / p90 / max | 182 / 602 / 2,073 ms | 137 / 351 / 1,366 ms |
| Apply changed ranges to the target, median | 4.2 ms (≈1.9 MB) | 4.4 ms |
| Extents in the snapshot, cycle 1 → cycle 50 | 202 → 9,698 | 206 → 9,165 |
| Bytes found ÷ written blocks (4 KiB-rounded), every cycle | 1.000 | 1.000 |
| Bytes found ÷ bytes written (not rounded), median | 1.06 | 1.06 |
| Blocks written but not found, all cycles | 0 | 0 |
| Idle cycle | 0 ranges | 0 ranges |
| sha256 of target = sha256 of snapshot 50 | yes | yes |
| Full scan, cold cache: sha256 / O_DIRECT read only | 3.04 s / 2.76 s | 3.92 s / 2.91 s |

What the numbers mean:
- **Precision and correctness.** On XFS, FIEMAP reports changes at block granularity. The diff found every written block and nothing else. The only overhead is rounding a partial write up to its 4 KiB blocks, which is about 6% for this mix. After 50 applied cycles the target was identical to the source snapshot, so no range was missed.
- **Cost grows with fragmentation, not with size.** Diff time follows the extent count, about 4 µs per extent. Each overwrite splits an extent, so a busy volume fragments over time. The clone, not the diff, dominates the cost of a cycle, and it reached 2 s at about 9,700 extents.
  - The engine should report the extent count of a warm volume.
  - Periodic defragmentation of the base is a later concern, not a blocker for M5b.
- **Full scan fallback.** The scan reads at about 350–550 MB/s from this virtual disk. Python's sha256 hashes at about 460 MB/s on one core, so the disk and the hash are similar bottlenecks here. BLAKE3 was **not** measured: neither `b3sum` nor a Python `blake3` module is installed, and nothing may be built on the lab, so sha256 stood in. BLAKE3 is several times faster than sha256 on one core (**Hypothesis**, from upstream benchmarks), so a BLAKE3 scan would be bound by the disk alone. Extrapolated to 5 GiB: about 14–20 s for a cold scan (**Hypothesis**, linear in size).
- **Interval.** A 30 s cycle costs well under a second of work: clone, FIEMAP and apply of about 2 MB. The 5 s minimum is also feasible. The cost of one interval depends on the changed bytes and the extent count, not on the interval itself.

**Decides:**
- ADR 0172: keep the reflink snapshots, the FIEMAP comparison, and the full scan as fallback, as written.
- ADR 0173: keep the 30 s interval default and the 5 s minimum. Add the volume's extent count to what the source node reports.

## S2: lease timing

### Clock drift

**Method.** The Mac served as a common reference. It kept one SSH session open per machine. Every 5 s for one hour it sent each machine a burst of 7 requests, and each machine answered with `CLOCK_MONOTONIC`, `CLOCK_BOOTTIME` and `CLOCK_MONOTONIC_RAW`.
- The answer with the shortest round trip was kept. It was placed at the midpoint of that round trip on the Mac's monotonic clock.
- Each machine's clock rate against the Mac is the slope of a least-squares fit. The rate difference between two machines cancels the Mac's own rate.
- Error of one sample: at most half its round trip (35.5 ms median). The fit's residual standard deviation was 1.4–3.8 ms. The slope's one-sigma error is in the table.

| Clock | lab-1 − lab-2 | lab-1 − lab-3 | lab-2 − lab-3 |
| --- | --- | --- | --- |
| `CLOCK_MONOTONIC` (= `CLOCK_BOOTTIME`) | −1.05 ± 0.10 ppm | −4.21 ± 0.16 ppm | −3.17 ± 0.14 ppm |
| … per hour | −3.8 ms | −15.2 ms | −11.4 ms |
| … per 15 s lease | −0.016 ms | −0.063 ms | −0.048 ms |
| `CLOCK_MONOTONIC_RAW` | −0.04 ± 0.09 ppm | −0.01 ± 0.09 ppm | +0.02 ± 0.07 ppm |

720 samples per machine over 60.0 minutes. Each machine's rate against the Mac: `CLOCK_MONOTONIC` +1.2, +2.3 and +5.5 ppm; `CLOCK_MONOTONIC_RAW` −8.5 ppm on all three.

Observations:
- All three machines run `systemd-timesyncd` and use `kvm-clock`.
- `CLOCK_BOOTTIME` equals `CLOCK_MONOTONIC` here, because nothing was suspended.
- `CLOCK_MONOTONIC` follows the NTP-slewed rate. `CLOCK_MONOTONIC_RAW` shows the host's underlying oscillator: the three VMs share one host, so their raw rates nearly agree.

§8.4 needs δ < M, where δ is the drift plus the offset over one lease lifetime. Over a 15 s lease the measured drift is 0.063 ms, against M = 1,500 ms. The **Hypothesis** holds by more than three orders of magnitude on this lab. Machines on separate hosts are not covered here: they can differ more, but still in ppm, which is microseconds per lease.

### Session RTT, lab-1 to lab-2

The node agent does not expose its session RTT, so lab-1 measured the path the session uses: the primary's address from lab-1's identity, port 8443.
- TCP connect time, once a second for an hour. A connect returns on the SYN-ACK, so it measures one network round trip.
- ICMP echo to the same address, once a second.

The minutes of the S3 cut are excluded.

| Probe | n | Median | p90 | p99 | p99.9 | Max |
| --- | --- | --- | --- | --- | --- | --- |
| TCP connect to the session port | 3,489 | 0.42 ms | 0.52 ms | **0.75 ms** | 2.0 ms | 53 ms |
| ICMP echo | 3,408 | 0.26 ms | 0.34 ms | 0.40 ms | 0.46 ms | 2.8 ms |

No connect failed outside the cut. The single 53 ms outlier is far below anything that matters for a 5 s renewal interval.

### Stop time of a 256 MiB Cloud Hypervisor VM

These were measured on a throwaway app (`traefik/whoami`, 256 MiB, 1 vCPU) on lab-1, created and deleted for the test. Times run from the request until the VMM process has exited.

| Stop | How | n | Time |
| --- | --- | --- | --- |
| Graceful, through the agent | `POST /apps/{id}/stop`, timed on the node from the engine's stop marker | 5 | 59–74 ms |
| Graceful, direct | `vm.power-button` on the VMM API | 3 | 57–78 ms |
| Forced, first step | `vmm.shutdown` on the VMM API | 2 | 22–28 ms |
| Forced, last step | `SIGKILL` to the VMM's exact pid | 3 | 2–20 ms |

- T_grace = 1,000 ms and T_force = 300 ms leave a margin of more than 10× for this app. The graceful time is the app's own shutdown: whoami exits at once. A slow app is cut off at T_grace, and the forced path stays under 30 ms.
- T_grace + T_force = 1.3 s ≤ M = 1.5 s holds.
- **Side observation.** After a forced stop, the agent's crash-restart path brought the VM back with an exponential backoff: 7–22 s after the first stops, then 38, 70 and 134 s. A self-fenced VM must not go through that path, which E1 already plans for.

### Primary restart

`systemctl restart quoro` on lab-2, three runs about 90 s apart.

| Run | `systemctl` returned | API `/api/v1/health` 200 | lab-1 session | lab-2's own agent | lab-3 (edge) |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.04 s | 0.22 s | 1.36 s | 1.38 s | 0.57 s |
| 2 | 0.03 s | 0.20 s | 1.04 s | 2.54 s | **26.02 s** |
| 3 | 0.03 s | 0.20 s | 0.98 s | 1.40 s | 0.92 s |

What happened in run 2:
- The edge's first redial came while the listener was still down, and the connection was refused.
- The agent then tried the primary's other advertised addresses (a bridge address, a mesh address and an mDNS name). None of them answers from the edge, and each one waited for the 10 s TLS connect timeout.
- That is filed as **#359**.

The 28 s quoted in the plan was a restore on a new host, not a restart.

What this means for leases:
- A held lease fences at `t_send + 13.5 s`, and renewals go every 5 s. So a node must be back within about 8 s of a primary restart, or it fences.
- A 1 s restart is safe without a maintenance window. The 26 s case is not, so it needs the window or the #359 fix. With the window answering renewals with W = 180 s before a planned restart, both cases survive.
- An unplanned restart with #359 can still fence warm apps on an unlucky node, so #359 should be fixed before E1's lab tests.

**Decides (ADR 0171):**
- Keep D = 15 s, renewal every D/3, M = max(1,000 ms, D/10), T_grace 1,000 ms and T_force 300 ms. The §8.4 drift **Hypothesis** is confirmed on the lab.
- Keep the maintenance window, W = 180 s. Fix #359 so that an unplanned restart reconnects within a second or two.

## S3: cut dry run

**Script.** `lab/lab-cut.sh` in the maintainers' ops repository, kept for E1. It runs from the operator's machine over SSH:
- It asks each peer for the source address of its default route. Lab addresses come from DHCP, so nothing is hardcoded.
- It refuses to run if the operator's own SSH session arrives from a peer address.
- It first arms a transient systemd timer (`systemd-run --on-active=`, 100 s by default, at most 120 s) that deletes the table.
- It then adds `table inet quoro_lab_cut`: input, output and forward chains at priority −300 that drop packets to and from the peers' addresses and accept everything else.
- `--check` validates the ruleset with `nft -c` and changes nothing. `--end` removes the cut early, and `--status` reports it.

The dry run took one lab-1 cut from lab-2 and lab-3, with monitors on all three machines and on the edge's public hostname. Times are counted from the moment the table was added.

| Check | Result |
| --- | --- |
| Table added; the operator's SSH session through the management host | yes; SSH kept working throughout |
| lab-1's agent notices | +9.1 s: "no frame from the primary within the session timeout; reconnecting" |
| Primary marks lab-1 `disconnected` | **+14.2 s** (within the 15 s the plan expects) |
| WireGuard to lab-3 | `quoro-wg0` received bytes stopped at the cut and resumed 1.8 s after the table was removed |
| lab-1's internet (HTTPS to Cloudflare, twice a second) | 187 of 187 answered 200. lab-1 runs no tunnel connector, because it has no tunnel mapping, so internet egress stands in for "the tunnel stays up". |
| Node-agent restart at +60 s, during the cut | The agent rebuilt `ip`/`bridge quoro_quoro0` and `inet quoro_mesh`, as it also does every 60 s. `nft monitor` shows no event on `quoro_lab_cut` until the timer. |
| Dead-man timer | Deleted the table at **+100.4 s**. The timer and its service then deactivated, with nothing left behind. |
| lab-1 reconnects | 12.3 s after the table was removed. The agent was inside the 10 s timeouts of #359. |
| Edge, one request a second to the replicated demo app | 5 requests timed out (3 s client limit) between +4.6 and +25.7 s, until the edge's health checks took lab-1's replica out. lab-2's replica served the rest. lab-1's replica served again 28 s after the table was removed. |
| After | lab-1 connected with applied = desired, edge split 5/5, demos 200 |

Two notes for E1:
- `ping` to the mesh hub's address fails even without a cut, because the mesh table's input chain drops it. Watch the interface counters instead.
- Restarting the node agent while it is cut off took 17.8 s, because the stop waited for a dial in progress. That is harmless for the VMs (`KillMode=process`), but it lengthens agent-restart tests.

**Decides (ADR 0185):** the method works as described. Use one table, armed after its dead-man timer. The agent leaves it alone, the management path and the internet stay up, and the timer always removes it. The ADR's 15-minute timer is longer than these tests need: with D = 15 s, the fence completes within 15 s of the last renewal. The script defaults to 100 s and refuses more than 120 s.

## S4: change-log size and promotion replay

The data came from the primary's `GET /api/v1/cluster` and an offline copy of its database in a root-only temporary directory on lab-2. The copy was deleted afterwards, and the live database was never opened.

| | Value |
| --- | --- |
| `log_seq` | 7,262, over 3.1 days of lab use (about 2,300 entries a day) |
| Database size | 10.2 MB |
| `change_log` table | 6.8 MB, plus 0.7 MB of indexes |
| Payload bytes | 4.35 MB; largest payload 4,097 B |
| Entries by kind | vulnerability findings 3,442 (47%); node updates 1,047; events 946; audit records 629; builds 351; the rest are app, VM, deployment and backup changes |
| Entities alive at the head | 3,752 |
| Verify the whole chain (`prev_hash` and SHA-256 of each entry, in Python) | 0.17 s, no mismatch |
| `quoro audit verify --db` on the copy (629 audit records, Rust) | 0.03 s |
| Replay into a fresh SQLite database (stand-in schema: one upsert per entry, in order) | 0.03–0.04 s |

About the replay figure:
- The real replay (ADR 0179) does not exist yet, so the replay was approximated with a stand-in schema. The figure is a lower bound: the real schema adds indexes and triggers. A factor of 10 would still keep it under a second (**Hypothesis**).
- It was measured on lab-2. lab-1, the promotion candidate, runs on the same CPU model. The database was not copied to lab-1.

At this rate, a year of comparable use is about 840,000 entries and roughly 800 MB of log. That means about 20 s to verify and some tens of seconds to replay (**Hypothesis**, linear).

The largest single source is vulnerability findings. ADR 0179 already treats the vulnerability *cache* as local, but findings themselves are replicated state.

**Decides (ADR 0179):** promotion replay takes seconds today and stays well under a minute for a year of this rate. M5b needs no log snapshot or compaction. The lag that matters is the follow lag, not the replay.

## Open risks and unknowns

- **Different hosts.** All three lab VMs share one physical host, so drift between machines on separate hosts is not measured. Expect a few ppm more (**Hypothesis**).
- **Larger volumes.** 5 GiB volumes and long-lived fragmentation are not measured. FIEMAP and clone cost grow with the extent count. A busy volume over weeks may need its base defragmented or re-cloned.
- **Pool space.** The pool needs about 2× the volume size at peak while a warm volume changes, from copy-on-write and preallocation. Pool growth (ADR 0156) should budget for it.
- **BLAKE3 scan speed.** It is assumed, not measured.
- **Reconnect delay.** #359 can delay a reconnect past the lease margin after a primary restart.

## Sources

- Lab measurements on 2026-11-06 (UTC). The scripts and raw logs are kept in the maintainers' ops repository and scratch space; the cut script is `lab/lab-cut.sh` there.
- `docs/contracts/node-agent-protocol.md` §8.4 (timing, the drift Hypothesis) and §8.5 (fencing).
- `docs/contracts/engine-trait.md` §3.8 (stop) and §3.16 (volume snapshots).
- Linux `FS_IOC_FIEMAP` and `FICLONE`: `Documentation/filesystems/fiemap.rst` and ioctl_ficlone(2), kernel.org and man7.org, accessed 2026-11-06.
- `crates/node-agent/src/tls.rs` (`CONNECT_TIMEOUT`), `crates/node-agent/src/session.rs` (redial), `crates/node-agent/src/mesh.rs` and `crates/engine/src/ch/net.rs` (the agent's own nftables tables), `crates/control-plane/src/changelog.rs` (entry hash).
