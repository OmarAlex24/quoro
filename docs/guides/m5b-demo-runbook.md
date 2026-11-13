# M5b demo runbook

This runbook runs the demo that closes M5b, Witness, leases and warm replication, on the three-VM test lab. It follows the [M5b section of the roadmap](../product/roadmap.md#m5b--witness-leases-and-warm-replication): its Definition of Done (13 boxes) and its demo script (steps 1 to 6). For each step it gives the exact command or panel action, the expected result, the evidence to keep, the Definition of Done box the step proves, and what to do if the step goes wrong.

The readers are the person who runs the demo and the maintainer, who approves it. Every operation named here is in the contracts: [`openapi.yaml`](https://github.com/OmarAlex24/quoro/blob/main/docs/contracts/openapi.yaml) and the [node-agent protocol](../contracts/node-agent-protocol.md). Some are not served on `main` yet. The [table below](#what-main-does-not-serve-yet) lists them with their issues, and each step repeats the warning where it applies. Anything the contracts leave unspecified is listed under [Open points](#open-points), not filled in here.

## Contents

- [Definition of Done map](#definition-of-done-map)
- [What `main` does not serve yet](#what-main-does-not-serve-yet)
- [Preconditions](#preconditions)
- [Conventions](#conventions)
- [Pre-flight checklist](#pre-flight-checklist)
- [Before the demo: the fence tests](#before-the-demo-the-fence-tests)
- [Step 1: two votes, then the witness](#step-1-two-votes-then-the-witness)
- [Step 2: a warm app on lab-1](#step-2-a-warm-app-on-lab-1)
- [Step 3: cut lab-1 off, refuse, then fail over](#step-3-cut-lab-1-off-refuse-then-fail-over)
- [Step 4: lab-1 returns](#step-4-lab-1-returns)
- [Step 5: planned switchover and the N+1 indicator](#step-5-planned-switchover-and-the-n1-indicator)
- [Step 6: promotion](#step-6-promotion)
- [Cleanup](#cleanup)
- [Decided points](#decided-points)
- [Open points](#open-points)

## Definition of Done map

| # | Definition of Done box (roadmap, M5b) | Proved in |
| --- | --- | --- |
| 1 | Roles: lab-2 is the primary and a node, lab-1 a node hosting the stateful app, lab-3 the edge that gains the witness role | [Pre-flight](#pre-flight-checklist), [step 1](#step-1-two-votes-then-the-witness) |
| 2 | With lab-1 and lab-2 as the only voters, Quoro reports that HA needs a third vote and offers a witness | [Step 1a](#1a-two-voters) |
| 3 | lab-3 gains the witness role without enrolling again; three votes; lab-3 stores no application data | [Step 1b](#1b-lab-3-gains-the-witness-role) |
| 4 | A stateful app runs `warm` on lab-1 with its data shipped to lab-2; the panel shows the last shipped snapshot | [Step 2](#step-2-a-warm-app-on-lab-1) |
| 5 | lab-1 is cut off by a firewall rule on lab-1 that a timer removes; its agent keeps running; it self-fences its stateful apps | [Step 3a](#3a-cut-lab-1-off) |
| 6 | While lab-1 still holds the lease, Quoro refuses to start the warm copy on lab-2 | [Step 3b](#3b-refused-while-lab-1-may-hold-the-lease) |
| 7 | After the lease is lost, an operator starts the warm copy on lab-2 and it holds the last shipped data | [Step 3c](#3c-fail-over-to-lab-2) |
| 8 | lab-1 comes back, starts nothing stateful, becomes the warm copy following lab-2, and its divergent writes are a quarantined copy recorded in the audit log | [Step 4](#step-4-lab-1-returns) |
| 9 | A known record, then a planned switchover back to lab-1 that runs at once; the app serves from lab-1 and the record is there | [Step 5a](#5a-planned-switchover-to-lab-1) |
| 10 | The N+1 indicator answers "if lab-1 dies, do its apps fit?" | [Step 5b](#5b-the-n1-indicator) |
| 11 | An operator promotes a replica node; refused while the old primary may still hold expiring leases unless the fence is acknowledged | [Step 6](#step-6-promotion) |
| 12 | The concurrent-fence and mid-fence crash tests pass | [Before the demo](#before-the-demo-the-fence-tests) |
| 13 | A held expiring lease survives an agent restart within the same host boot | [Step 2d](#2d-a-held-lease-survives-an-agent-restart) |

## What `main` does not serve yet

The runbook is written against the contracts. On the day of the demo, check each line of this table against `main` and against `quoro version` on the labs. A step whose operation is not served cannot run yet.

| Needed for | Operation or behaviour | Issue |
| --- | --- | --- |
| Steps 2, 3 | `expiring` leases on the primary: the conversion grant and the renewals | [#555](https://github.com/OmarAlex24/quoro/issues/555) |
| Step 3b | `quoro cluster maintenance` (a maintenance window), to widen the refusal's window | [#764](https://github.com/OmarAlex24/quoro/issues/764) |
| Step 4 | A `deleted` entry that reaches the returning source's volume before its hand-over must not remove it | [#557](https://github.com/OmarAlex24/quoro/issues/557) |
| Step 5 | `POST /api/v1/apps/{app_id}/switchover` (`switchoverApp`) | [#735](https://github.com/OmarAlex24/quoro/issues/735) |
| Step 5 | A health check must not restart a VM that a switchover freeze paused | [#630](https://github.com/OmarAlex24/quoro/issues/630) |
| Step 5 | `GET /api/v1/cluster/capacity` (`getClusterCapacity`, the N+1 indicator) | [#717](https://github.com/OmarAlex24/quoro/issues/717) |
| Step 6 | `quoro cluster promote` | [#19](https://github.com/OmarAlex24/quoro/issues/19) |
| Step 6 | The old primary demotes itself on a newer term, and `quoro node rejoin` | [#728](https://github.com/OmarAlex24/quoro/issues/728) |

These are on `main`:

- `failoverApp` ([#726](https://github.com/OmarAlex24/quoro/pull/726)). A failover whose grant waits for replication answers `202` with `grant_pending` ([#742](https://github.com/OmarAlex24/quoro/pull/742), ADR 0292).
- `Cluster.votes` with `ha_ready`, and voter support on the primary.
- `updateNodeRoles` and a witness-only machine ([#744](https://github.com/OmarAlex24/quoro/pull/744)).
- The panel's HA view ([#745](https://github.com/OmarAlex24/quoro/pull/745)).
- `declareNodeLost` ([#759](https://github.com/OmarAlex24/quoro/pull/759)).
- Custody seals, so that `Node.custody` reads `sealed` ([#743](https://github.com/OmarAlex24/quoro/pull/743)).
- Change-log follow, with `Node.log_seq` and `Node.log_lag`.
- The warm copy's progress in `App.warm`.
- The returning source's hand-over and its quarantined copies, on the node ([#763](https://github.com/OmarAlex24/quoro/pull/763)) and on the primary ([#738](https://github.com/OmarAlex24/quoro/pull/738)).
- `restoreBackupRepositoryKey`, and the refusals of backup work while a promoted primary does not hold the repository key ([#797](https://github.com/OmarAlex24/quoro/issues/797)).

The panel on `main` has no switchover action. Until it does, step 5's panel action has an API call that does the same thing, and the API answer is the evidence.

## Preconditions

- **Labs and roles.** Three Debian 13 VMs with nested KVM, as described in the [roadmap](../product/roadmap.md#the-lab):
  - lab-2 is the control-plane primary and a node;
  - lab-1 is a node, with no control plane of its own;
  - lab-3 is enrolled as an edge only, with the tunnel and the mesh hub it had in M5a.
- **One build on every lab.** All three labs run the same `quoro` binary. Run `quoro version` on each and keep the output; the three must match. Keep the previous binary on each lab for rollback.
- **The warm policy.** Until the last warm PR merges, only a test-lab build made with the dev-only `dev-warm-policy` feature accepts `ha_policy: warm` ([ADR 0223](../adr/0223-expiring-leases-in-dev-builds-before-the-warm-policy-ships.md), [ADR 0277](../adr/0277-warm-turns-on-at-the-end-of-its-chain-until-then-updateapp.md)). Check `quoro version`: such a build says so in its output. If the warm chain has merged by the demo, the release build is enough and the version shows no dev feature.
- **lab-1's volume pool.** lab-1 and lab-2 snapshot volumes with `reflink`: a warm node must ([`WarmCopy.node_id`](https://github.com/OmarAlex24/quoro/blob/main/docs/contracts/openapi.yaml)). The API reports no snapshot method per node, so the check is the answer to `updateApp` in [step 2b](#2b-turn-on-warm): without `reflink` it refuses `warm` with `validation_failed`.
- **A human token.** `$TOKEN` is a human's API token (`tok_...`) with the owner or operator role. `updateNodeRoles`, `declareNodeLost`, `ackNodeFence` and `deleteVolumeQuarantine` are human-only, and an agent is refused with `forbidden`. The end of step 6 calls `restoreBackupRepositoryKey`, which is owner-only: it needs the owner's token and the owner's backup recovery key. API tokens are replicated, so the same token works after the promotion in step 6. Panel sessions are not replicated, so the panel asks for a new login after a promotion.
- **The demo app.** A stateful HTTP app with one volume, 256 MiB of memory, that:
  - writes a record to its volume when asked over HTTP, and
  - serves the records it holds, with the instance that answered.

  It is deployed from an image: an app with a Git `source` cannot be `warm` in M5b. Its domain is served through lab-3's tunnel. The image is listed under [Decided points](#decided-points).
- **The cut method** of [step 3](#3a-cut-lab-1-off) is ready on the operator's machine and was rehearsed. The operator reaches lab-1 over SSH from an address that is not lab-2's or lab-3's.
- **No Git build during the demo.** A builder VM takes 2048 MiB. On lab-2 it does not fit next to the demo apps and the warm app after its failover.

## Conventions

Every API call goes to the primary. Until step 6, the primary is lab-2.

```sh
export QUORO="https://<lab-2 address>:8443"     # the primary's base URL
export TOKEN="tok_..."                           # a human token, never echoed
# QUORO_TLS holds the curl options that trust the primary's pinned certificate.
# A promotion keeps the same certificate. Whether the same options work against
# lab-1's address depends on how they check the name (see Open points).
q() { curl -sS $QUORO_TLS -H "Authorization: Bearer $TOKEN" "$@"; }

# Ids, read once.
LAB1=$(q "$QUORO/api/v1/nodes" | jq -r '.items[] | select(.name=="lab-1") | .id')
LAB2=$(q "$QUORO/api/v1/nodes" | jq -r '.items[] | select(.name=="lab-2") | .id')
LAB3=$(q "$QUORO/api/v1/nodes" | jq -r '.items[] | select(.name=="lab-3") | .id')
```

The demo app's id is `$APP` and its volume's id is `$VOL` once step 2 creates them. Write every time in UTC. For each step, save the raw API answers, the commands as they were typed, and the screenshots. Never save a token, a password or a key in the evidence. Before you share any evidence, remove lab addresses from it.

## Pre-flight checklist

Run it on the day of the demo. Stop at the first line that fails.

| Check | How | Expected |
| --- | --- | --- |
| Versions | `quoro version` on lab-1, lab-2 and lab-3 | Identical. If warm still needs it, the dev feature is shown |
| Units | `systemctl is-active quoro quoro-node` on each lab | lab-2: both `active`. lab-1: `quoro-node` active, `quoro` `inactive` (no such unit). lab-3: `quoro-node` active |
| No control plane on lab-1 | On lab-1: `test ! -e /etc/systemd/system/quoro.service && test ! -e /var/lib/quoro/quoro.db && echo clean` | `clean`. `quoro cluster promote` refuses on a host that has a control-plane database or unit ([ADR 0232](../adr/0232-quoro-cluster-promote-refuses-on-a-host-that-already-has-a.md)) |
| Host storage and load | `df -h /var/lib/quoro-node` (the node agent's directory, with its volumes and engine state) and `uptime` on each lab | Enough room for one more copy of the demo volume on lab-1 and lab-2. No load left over from an earlier run |
| Nodes | `q "$QUORO/api/v1/nodes"` | Three machines `connected`. lab-1 and lab-2 have `roles: ["node"]` and lab-3 `["edge"]`. `applied_rev` equals `desired_rev`. `fence_failure` is null everywhere. lab-1's and lab-2's `log_lag` is 0 or close to it |
| lab-2's memory | `allocated.memory_mb` and `capacity.memory_mb` of lab-2 in the same answer | Room for the demo app (256 MiB) once the warm copy starts there, within lab-2's VM budget (about 3410 MiB on the lab). If it does not fit, stop an app that the demo does not need |
| Custody (step 6) | `custody` of lab-1 and lab-2 in the same answer | `sealed` on both. Without it, promotion is refused ([#727](https://github.com/OmarAlex24/quoro/issues/727)) |
| Cluster | `q "$QUORO/api/v1/cluster" \| jq '{primary_node_id, term, votes}'` | `primary_node_id` is lab-2. `votes.count` is 2. Keep the `term` |
| Apps present | `q "$QUORO/api/v1/apps"` | The apps of earlier milestones are there and running. The demo app does not exist yet, or exists at `ha_policy: none` |
| Edge and domain | Request the demo domain several times | `200` |
| No cut left over | On lab-1: `nft list table inet quoro_lab_cut` and `systemctl list-units --all 'quoro-lab-cut-deadman*'` | No such table, and no unit listed, not even a failed one |

Evidence: the outputs above, and a screenshot of the panel's Nodes page.

## Before the demo: the fence tests

**Proves:** box 12, together with the self-fence of [step 3a](#3a-cut-lab-1-off) on the lab.

The tests that [ADR 0075](../adr/0075-concurrent-fences-and-a-mid-fence-crash-test-are-mandatory.md) requires are in `crates/node-agent/tests/local_leases.rs`: `fences_of_different_leases_run_concurrently` and `a_crash_mid_fence_fences_again_on_restart`. The test for box 13 is `a_held_lease_survives_an_agent_restart_on_the_same_boot`, in `crates/node-agent/tests/expiring_leases.rs`.

```sh
cargo test --locked -p quoro-node-agent --test local_leases -- \
  fences_of_different_leases_run_concurrently a_crash_mid_fence_fences_again_on_restart
cargo test --locked -p quoro-node-agent --test expiring_leases -- \
  a_held_lease_survives_an_agent_restart_on_the_same_boot
```

Run them on a Linux host, from the commit the demo build was made from. The lab run that goes with them is step 3a with a second `warm` app ([decided points](#decided-points)). Do not run them on a lab VM: the lab VMs are too small for a build.

**Evidence:** the commit, the test output, and the link to the CI run of that commit.

## Step 1: two votes, then the witness

### 1a. Two voters

**Proves:** box 2, and box 1 for the roles.

- **Panel:** open the HA view ([#688](https://github.com/OmarAlex24/quoro/issues/688)).
- **API:** `q "$QUORO/api/v1/cluster" | jq .votes`

**Expected:**

- `voters` lists lab-1 and lab-2 only. An edge-only machine does not vote ([ADR 0170](../adr/0170-witness-role-any-enrolled-machine-can-add-the-witness-role.md)).
- `count: 2`, `required: 3`, `ha_ready: false`.
- The HA view says that HA needs a third vote. It offers a witness: a join token with the role `witness` for a new machine, or the roles operation for an enrolled one ([ADR 0017](../adr/0017-ha-requires-at-least-3-votes-with-2-nodes-quoro-tells-the.md)).

**Evidence:** the JSON, and a screenshot of the HA view.

### 1b. lab-3 gains the witness role

**Proves:** box 3.

- **Panel:** take the witness offer for lab-3 in the HA view.
- **API:**

  ```sh
  q -X POST "$QUORO/api/v1/nodes/$LAB3/roles" \
    -H 'Content-Type: application/json' -d '{"add":["witness"]}'
  ```

**Expected:**

- `200` with lab-3's `roles` as `edge` and `witness`. lab-3 is not enrolled again ([ADR 0018](../adr/0018-the-edge-lb-can-be-the-third-voter-role-witness-edge-of-the.md)).
- Within a few seconds, `GET /api/v1/cluster` shows `votes.count: 3` with lab-3 among the voters. `ha_ready` turns `true` once a majority supports the primary.
- lab-3 holds no application data:
  - no volume of any app names lab-3 (`GET /api/v1/apps/{app_id}/volumes` for each app);
  - `GET /api/v1/nodes/$LAB3` shows `allocated.memory_mb` and `allocated.disk_gb` at 0, and no VM runs there;
  - on lab-3, `pgrep -c cloud-hypervisor` prints `0`.
- The audit log has an `updateNodeRoles` record: `q "$QUORO/api/v1/audit?target_id=$LAB3&order=desc&limit=5"`.

**Evidence:** the role answer, the votes before and after, the volume lists, the audit record, and a screenshot of the HA view with three votes.

**If it goes wrong:**

- A `409 conflict` means the previous voter-set change has not reached a majority yet ([protocol section 8.10](../contracts/node-agent-protocol.md#810-voter-set-changes-m5b)). Wait and repeat the call.
- If `ha_ready` stays `false`, check that lab-3's agent is connected and has stored the new desired state.
- To undo the step: `{"remove":["witness"]}`. It leaves fewer than three voters, so it is allowed only while no `expiring` lease row is unrevoked or still behind its gate. A revoked row whose gate has passed does not block it ([protocol section 8.10](../contracts/node-agent-protocol.md#810-voter-set-changes-m5b), [ADR 0321](../adr/0321-four-lease-and-voter-readings-every-waiting-grant-is-marked.md)). Never pass `force` in the demo.

## Step 2: a warm app on lab-1

**Proves:** box 4, and box 13 in [2d](#2d-a-held-lease-survives-an-agent-restart).

### 2a. Create the app with its volume on lab-1

Skip this part if the demo app already exists.

```sh
q -X POST "$QUORO/api/v1/apps" -H 'Content-Type: application/json' -d '{
  "project_id": "<project id>", "name": "warm-demo", "image": "docker.io/sigoden/dufs:v0.45.0",
  "port": 5000, "resources": {"vcpus": 1, "memory_mb": 256, "disk_gb": 1},
  "placement": {"node_ids": ["'"$LAB1"'"]}}'
q -X POST "$QUORO/api/v1/apps/$APP/volumes" -H 'Content-Type: application/json' \
  -d '{"size_gb": 1, "mount_path": "/data"}'
q -X PUT "$QUORO/api/v1/apps/$APP/env" -H 'Content-Type: application/json' \
  -d '{"items": [{"name": "DUFS_SERVE_PATH", "value": "/data"}, {"name": "DUFS_ALLOW_UPLOAD", "value": "true"}]}'
q -X POST "$QUORO/api/v1/apps/$APP/deployments" -H 'Content-Type: application/json' \
  -d '{"image": "docker.io/sigoden/dufs:v0.45.0"}'
```

Take `APP` from the `id` of the first answer and `VOL` from the `id` of the second, for example `APP=$(... | jq -r .id)`. Attach the demo domain to the app, as in M5a.

### 2b. Turn on `warm`

- **Panel:** the app's Storage tab, high-availability policy `warm`. The panel shows what each policy costs.
- **API:**

  ```sh
  q -X PATCH "$QUORO/api/v1/apps/$APP" -H 'Content-Type: application/json' \
    -d '{"ha_policy":"warm","warm":{"node_id":"'"$LAB2"'"}}'
  ```

  Pinning lab-2 makes the warm node the one the demo needs. Without the pin, the scheduler picks among the eligible nodes, and on this lab lab-2 is the only one.

**Expected:**

- `200`. `App.warm` has `node_id` lab-2 and `interval_s: 30`. `state` is `syncing`, then `in_sync` once lab-2 holds a first complete copy.
- `App.lease` shows `holder_node_id` lab-1. `mode` turns `expiring` after lab-1's first renewal following the change, with `duration_ms: 15000` and `unsettled: false` ([protocol section 8.4](../contracts/node-agent-protocol.md#84-expiring-mode-timing), [ADR 0171](../adr/0171-expiring-lease-defaults-for-warm-d-15-s-m-1-5-s-t-grace-1-s.md)).

### 2c. Write a known record and watch it ship

1. Write record **A** through the demo domain. Note the time.
2. Poll `q "$QUORO/api/v1/apps/$APP" | jq '{warm, lease}'` until the shipped copy's snapshot was taken after record A: the time of the reading minus `warm.lag_s` is later than the time of record A, with `state: in_sync`. `last_shipped_at` alone is not enough, because it is when lab-2 completed the copy, and the snapshot may have been taken before A was written. Waiting until `last_snapshot_seq` has advanced twice after A also works.

- **Panel:** the app's last shipped snapshot on lab-2 ([#839](https://github.com/OmarAlex24/quoro/pull/839)).

**Expected:** `last_snapshot_seq` grows by one about every 30 s. `lag_s` stays at or below 60 s.

**Evidence:**

- the `PATCH` answer;
- three readings of `App.warm`, at least 30 s apart;
- the time of record A and the first `last_shipped_at` after it;
- a screenshot of the panel showing the last shipped snapshot.

### 2d. A held lease survives an agent restart

**Proves:** box 13 ([ADR 0072](../adr/0072-lease-timing-a-node-renews-immediately-after-a-first-grant.md)).

1. On lab-1: `systemctl restart quoro-node`. Note the time.
2. Watch the app's VM on lab-1. The Cloud Hypervisor process of the app keeps the same pid: `pgrep -af cloud-hypervisor`.
3. Read the events: `q "$QUORO/api/v1/events?type=node.fenced&subject_id=$LAB1"`. A fence event's subject is the node, not the app.

**Expected:**

- the VM keeps running and its pid does not change;
- no `node.fenced` event;
- the lease's `fencing_token` does not change;
- the domain keeps answering `200`, apart from requests in flight;
- lab-1 is `connected` again within a few seconds.

**Evidence:** the pid before and after, the lab-1 journal around the restart (`journalctl -u quoro-node --since ...`), the events, and the lease before and after.

**If it goes wrong:** if the lease fenced, the app stops on lab-1, and the primary gives the lease back to lab-1 at a higher token when the node reports it `expired` ([ADR 0281](../adr/0281-the-primary-regrants-a-lease-from-hello-only-when-the-node.md)). Record it as a failure of box 13. Wait for the app to run again and `warm.state` to be `in_sync` before step 3.

## Step 3: cut lab-1 off, refuse, then fail over

**Proves:** boxes 5, 6 and 7.

**Pre-flight for this step.** Read `q "$QUORO/api/v1/apps/$APP" | jq '{lease, warm}'` and `q "$QUORO/api/v1/cluster" | jq .votes`. Go on only when all of these hold:

- `lease.mode` is `expiring` and `lease.unsettled` is `false`. A failover of a marked row answers `202` with `grant_pending`, and its grant waits until the row's newest write is replicated (ADR 0292), which this step does not show;
- `warm.state` is `in_sync`, and record A has shipped (step 2c);
- `votes.ha_ready` is `true`. Without lab-3's vote, lab-2 alone is not a majority once lab-1 is cut off, and it cannot grant the lease.

### The cut method

The cut is one nftables table on lab-1, and a timer removes it. Nothing else on the lab changes. A script in the lab tooling does it, outside this repository:

1. It reads lab-2's and lab-3's current addresses from the peers themselves, because the lab addresses come from DHCP.
2. It refuses if the operator's SSH session to lab-1 comes from one of those addresses, because the cut would cut the operator too.
3. Before it adds the table, it arms a dead man's switch: a transient systemd timer on lab-1 that deletes the table after 100 s by default, and after 120 s at most. The table goes even if the operator's session or the script dies.
4. It adds the table `inet quoro_lab_cut`, with input, output and forward chains at priority -300. They drop packets from and to the peer addresses and accept everything else. If adding the table fails, the timer is stopped and nothing is left.

The management path to lab-1, its internet access and its local VMs are untouched. The node agent manages only its own tables and never this one. The same thing by hand, on lab-1 as root:

```sh
systemctl reset-failed quoro-lab-cut-deadman.service quoro-lab-cut-deadman.timer 2>/dev/null
systemd-run --collect --unit=quoro-lab-cut-deadman --on-active=100s \
  --timer-property=AccuracySec=1s /bin/sh -c 'nft delete table inet quoro_lab_cut || true' &&
systemctl is-active --quiet quoro-lab-cut-deadman.timer &&
nft -f - <<'EOF'
table inet quoro_lab_cut {
  set peers4 { type ipv4_addr; elements = { <lab-2 address>, <lab-3 address> } }
  chain input   { type filter hook input   priority -300; policy accept; ip saddr @peers4 drop; }
  chain output  { type filter hook output  priority -300; policy accept; ip daddr @peers4 drop; }
  chain forward { type filter hook forward priority -300; policy accept; ip saddr @peers4 drop; ip daddr @peers4 drop; }
}
EOF
```

The `&&` chain adds the table only once the timer is armed. The by-hand form skips the script's check that the operator's SSH session does not come from a peer address, so check that yourself. Cut every IPv6 address of the peers too, link-local and unique local ones included, with a `peers6` set and the same rules for `ip6`. Or check before the cut that lab-1's session to the primary runs over IPv4: `welcome.primary_urls` can name a `.local` host, which may resolve to an IPv6 address that an IPv4-only cut leaves open, and then lab-1 never fences. To end the cut early: `nft delete table inet quoro_lab_cut`, then `systemctl stop quoro-lab-cut-deadman.timer`. Rehearse the cut once before the demo.

### 3a. Cut lab-1 off

**Proves:** box 5 ([ADR 0019](../adr/0019-fencing-is-mandatory-and-lease-based-node-agent-self-fences.md)).

1. Start two watchers, one line per second, each with a UTC timestamp:
   - on the operator's machine, `q "$QUORO/api/v1/nodes/$LAB1" | jq -c '{status, last_seen_at}'` together with `q "$QUORO/api/v1/apps/$APP" | jq -c .lease`;
   - on lab-1 over SSH, `journalctl -fu quoro-node`.
2. Arm the cut with the timer at 100 s. Note the time: `t0`.
3. Right after the cut, from lab-1's host, write record **B** to the app's VM at its private address, which the app's `instances[].ip` shows. The domain and the web terminal both pass through the cut, so neither of them can write it. Record B is a write after the last shipped snapshot that the cut keeps from shipping, because the replication stream runs between lab-1 and lab-2. It is what step 4 finds quarantined. Do it within about 5 s of `t0`, before the fence. That lab-1's host can reach its own VM's address is how the lab behaves today; the contracts do not state it (see Open points). Check it in the rehearsal.

**Expected** (lease defaults: `D` = 15 s, renewal every 5 s, margin 1.5 s):

- lab-1's agent keeps running. Its renewals go unanswered, and the lease turns `uncertain`, then `expired`.
- About 8.5 to 15 s after `t0`, lab-1 fences: the app's VM stops on lab-1, and the journal shows the fence.
- The primary marks lab-1 `disconnected` about 15 s after its last frame from lab-1.
- The `node.fenced` event reaches the primary when lab-1 returns.

**Evidence:** `t0`, the time of record B, both watcher logs, the time of the fence in lab-1's journal, and `pgrep -af cloud-hypervisor` on lab-1 after the fence.

### 3b. Refused while lab-1 may hold the lease

**Proves:** box 6.

As soon as the watcher shows lab-1 `disconnected`, start the warm copy on lab-2:

```sh
q -X POST "$QUORO/api/v1/apps/$APP/failover" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" -d '{"node_id":"'"$LAB2"'"}'
```

**Expected:** `409 conflict` with `reason: lease_held`: lab-1 is not connected, and the lease's gate (`App.lease.gate_at`, the last grant plus `D + 2M`) has not passed. Keep the answer and `gate_at`. The refusal is in the audit log with `result: refused`.

Do not call `failoverApp` before the primary shows lab-1 `disconnected`. While the primary still sees lab-1 as connected, it first revokes the lease over the session and waits for the answer. That is the connected-source path, not this refusal.

**The window is short, and some runs have none.** The primary marks lab-1 `disconnected` 15 s after its last frame of any kind: a heartbeat, a status report or a log acknowledgement. The gate passes 18 s after the last renewal it answered. The window is therefore 3 s minus the time between that renewal and lab-1's last frame. Heartbeats and renewals run on separate 5 s cycles, so the window ranges from about -2 s to 3 s. When it is negative, no call is ever refused. Fire the call from a loop that polls `status` every 200 ms. If the call lands after `gate_at`, it is not refused: it starts the warm copy, which is step 3c, and box 6 is not shown in this run. Budget for repeated runs: repeat the demo from step 2 for box 6 after [cleanup](#cleanup). The demo avoids this with a maintenance window (below).

**The demo widens the window with a maintenance window.** Before arming the cut, on lab-2 as root: `quoro cluster maintenance --duration 30`. With `W` = 30 s, lab-1 fences about 27 s after its last answered renewal and the gate passes 36 s after it, so the refusal shows for roughly 16 to 21 s on every run ([protocol section 8.11](../contracts/node-agent-protocol.md#811-maintenance-window-m5b)). The command is not on `main` yet ([#764](https://github.com/OmarAlex24/quoro/issues/764)). Until it is, use repeated runs as above.

### 3c. Fail over to lab-2

**Proves:** box 7 ([ADR 0175](../adr/0175-manual-failover-of-a-warm-app-post-apps-app-id-failover.md)).

Once `gate_at` has passed, send the call again, with a new `Idempotency-Key`. An operator does it here with a human token. An agent can do it only with an approval for `{type: app, id}` with `node_id` in `params`; otherwise it is refused with `approval_required` ([ADR 0030](../adr/0030-grants-scope-resource-selector-ttl-optional-max-uses.md)).

**Expected:**

- `202` with the app. `lease.holder_node_id` is lab-2, with a higher `fencing_token` and `mode: expiring`. `grant_pending` turns `false` once lab-2 holds the lease.
- `App.warm.node_id` is now lab-1, the old source.
- An `app.failed_over` event; the audit log has the `failoverApp` record and a `lease.grant` record.
- The app runs on lab-2, and the edge moves its backend there. The domain answers `200` and serves record A, but not record B.

**Evidence:**

- the `202` answer, and the app as read afterwards;
- the event, and the two audit records (`q "$QUORO/api/v1/audit?target_id=$APP&order=desc&limit=10"`);
- the domain's answer, with record A present, record B absent, and the instance that answered;
- the time between `t0` and the first `200` from lab-2.

**If it goes wrong:**

- `no_support`:
  - check `votes.ha_ready` and lab-3's connection;
  - also check `lease.unsettled`: for a marked row the answer is `202` with `grant_pending`, and the grant waits for replication (ADR 0292).
- `no_copy`: lab-2 holds no complete copy. Step 2c did not complete. End the cut, wait for `in_sync`, and start step 3 again.
- `fence_failed`: lab-1 reported a failed fence. End the cut. Check on lab-1 that the VM stopped. Then acknowledge the fence with `POST /api/v1/nodes/$LAB1/fence-ack`, using the `lease_ids` of `fence_failure`. Never acknowledge without checking.
- A lease that stays `lease_held` after its gate passes. A `local` or marked lease moves only once the node is declared lost: `POST /api/v1/nodes/$LAB1/declare-lost` ([ADR 0183](../adr/0183-add-post-api-v1-nodes-node-id-declare-lost-human-only.md), [#734](https://github.com/OmarAlex24/quoro/issues/734)). It is human-only and audited. Use it only while the cut holds and lab-1's VM has stopped.
  It has two side effects. First, it raises the token of every lease lab-1 holds, including the `local` leases of other stateful apps on lab-1. When lab-1 returns, those leases are revoked with `declared_lost`, and those apps stay stopped until an operator acts. Second, for a marked row the failover's grant waits until lab-1 is removed, or returns and acknowledges the row's newest write ([protocol section 8.4](../contracts/node-agent-protocol.md#84-expiring-mode-timing)), so the failover does not complete while the cut holds. Run it only with the maintainer's OK.
- The timer removes the cut before the failover commits. lab-1 reconnects with its lease `expired`, and the primary gives the lease back to lab-1 at a higher token ([ADR 0281](../adr/0281-the-primary-regrants-a-lease-from-hello-only-when-the-node.md)). The app starts again on lab-1. Nothing was lost. Wait for `warm.state: in_sync` and start step 3 again.

End the cut when step 3c is done, or let the timer end it. Do not leave it until the timer if step 4 is ready.

## Step 4: lab-1 returns

**Proves:** box 8 ([ADR 0022](../adr/0022-failback-is-never-automatic-or-immediate-returning-node.md), [ADR 0176](../adr/0176-returning-source-after-a-failover-it-keeps-its-divergent.md)).

1. End the cut: `nft delete table inet quoro_lab_cut` and `systemctl stop quoro-lab-cut-deadman.timer` on lab-1, or wait for the timer. Note the time: `t1`.
2. Watch lab-1 reconnect, and read the app and its volume:

   ```sh
   q "$QUORO/api/v1/apps/$APP" | jq '{lease, warm, instances}'
   q "$QUORO/api/v1/apps/$APP/volumes" | jq '.items[] | {id, node_id, quarantines}'
   q "$QUORO/api/v1/audit?target_id=$VOL&order=desc&limit=5"
   q "$QUORO/api/v1/events?type=volume.quarantined"
   ```

**Expected** ([protocol section 9](../contracts/node-agent-protocol.md#9-node-return)):

- lab-1 is `connected` again. The primary first revokes the lease lab-1 still names, and lab-1 starts nothing stateful. The app's VM on lab-1 is deleted, because the desired state lists it as `standby`, and no Cloud Hypervisor process of the app runs there.
- Its live volume holds record B, which no snapshot it kept has. lab-1 therefore keeps the volume as a quarantined copy (`qrn_...`) with its size and SHA-256. The volume's `quarantines` lists it.
- The audit log has a `volume.quarantine` record, and the event `volume.quarantined` was emitted.
- `App.warm` names lab-1 as the warm node, and its `state` goes `syncing`, then `in_sync`. lab-2 ships from the copy the failover started from, so the resync is incremental when lab-1 still holds that snapshot.
- The app keeps serving from lab-2, because failback is never automatic.

**Evidence:**

- `t1` and lab-1's journal from `t1`;
- the volume with its `quarantines`;
- the `volume.quarantine` audit record and the event;
- `App.warm` until it reads `in_sync`;
- `pgrep -af cloud-hypervisor` on lab-1.

**If it goes wrong:** if no quarantined copy appears, check whether record B was written after the cut, and whether the copy is still being compared (its SHA-256 comes later). The copy stays on lab-1's disk in every case. Nothing in this step deletes it. Do not delete it during the demo.

## Step 5: planned switchover and the N+1 indicator

### 5a. Planned switchover to lab-1

**Proves:** box 9 ([ADR 0177](../adr/0177-planned-switchover-post-apps-app-id-switchover-node-id-runs.md), [#735](https://github.com/OmarAlex24/quoro/issues/735)).

1. Write record **C** through the domain. Note the time.
2. Check that `App.warm.state` is `in_sync` and that lab-1 and lab-2 are both connected.
3. Start the switchover:
   - **Panel:** the switchover action on the app (not on `main` yet).
   - **API:**

     ```sh
     q -X POST "$QUORO/api/v1/apps/$APP/switchover" -H 'Content-Type: application/json' \
       -H "Idempotency-Key: $(uuidgen)" -d '{"node_id":"'"$LAB1"'"}'
     ```

4. Request the domain once a second throughout.

**Expected** ([protocol section 6.13](../contracts/node-agent-protocol.md#613-warm-replication)):

- `202` at once. The switchover runs immediately, with no low-traffic window.
- lab-2 flushes and pauses the app, ships a final snapshot, stops the app and gives up the lease. lab-1 gets the lease with a higher token and starts the app, and the edge moves its backend.
- The domain does not answer for a few seconds, then answers `200` from lab-1, with records A and C present.
- An `app.switched_over` event. `App.lease.holder_node_id` is lab-1. `App.warm.node_id` is lab-2, and no new quarantined copy appears.

**Evidence:** the `202`, the probe log with the gap, the event, the app before and after, and the domain's answer with records A and C and the instance that answered.

**If it goes wrong:**

- `not_in_sync`: wait for `in_sync` and send the call again.
- `app.switchover_failed`: the app resumed on lab-2 and nothing moved. Keep the event and repeat.
- If the app stays paused on lab-2 past a few seconds, check whether its health check restarted it ([#630](https://github.com/OmarAlex24/quoro/issues/630)).

### 5b. The N+1 indicator

**Proves:** box 10 ([ADR 0021](../adr/0021-n-1-capacity-indicator-if-node-a-dies-do-its-apps-fit-on.md), [ADR 0178](../adr/0178-n-1-indicator-get-api-v1-cluster-capacity-lost-node-id.md), [#717](https://github.com/OmarAlex24/quoro/issues/717)).

- **Panel:** the HA view, or the capacity sentence above the nodes, with lab-1 chosen as the lost node.
- **API:** `q "$QUORO/api/v1/cluster/capacity?lost_node_id=$LAB1"`

**Expected:**

- On this lab, most likely `fits: true`. The demo app would start on its warm node, lab-2, with `action: failover`. A stateless app that keeps a replica elsewhere needs nothing.
- Otherwise `fits: false`, with `missing` naming the CPUs, memory or disk that are short.

Both answers satisfy box 10. The "no" branch needs an app on lab-1 too big for lab-2's free memory. Showing it is optional, and it means a temporary app that cleanup deletes.

**Evidence:** the JSON answer and the panel screenshot.

## Step 6: promotion

**Proves:** box 11 ([ADR 0067](../adr/0067-manual-promotion-of-a-replica-node-to-primary-ships-in-m5b.md), [ADR 0182](../adr/0182-manual-promotion-19-quoro-cluster-promote-on-the-candidate.md), [protocol section 7.3](../contracts/node-agent-protocol.md#73-promotion), [#19](https://github.com/OmarAlex24/quoro/issues/19)).

**Pre-flight for this step:**

- `GET /api/v1/nodes`: lab-1's `custody` is `sealed`, its `log_lag` is 0 and its status is `connected`.
- `GET /api/v1/cluster`: `votes.count` is 3, `ha_ready` is `true`. Keep the `term`.
- On lab-1, no control-plane unit or database (see the pre-flight checklist).
- Each command below runs as root on the host named.

### 6a. Refused while lab-2's control plane runs

On lab-1:

```sh
quoro cluster promote --dry-run
quoro cluster promote
```

**Expected:**

- `--dry-run` shows the log position and the vote pre-check, and changes nothing.
- The real command asks every voter with `check` first. The voters still support lab-2, so they answer `refused` with `support_held`. The command prints each voter's answer and stops before any real vote.
- No voter moved and no term changed: `GET /api/v1/cluster` shows the same `term` and `primary_node_id`.

**Evidence:** both command outputs, and the cluster before and after.

### 6b. Stop lab-2's control plane, then promote lab-1

1. On lab-2: `systemctl stop quoro`. Its node agent keeps running: `systemctl is-active quoro-node` prints `active`. The `quoro-node` unit orders itself after `quoro` but does not require it. Note the time: `t2`.
2. Wait at least 15 s, the voters' support window `S`, or the `retry_after_ms` that a `support_held` answer carries. A vote before that is refused with `support_held` again, which is safe: run the command again.
3. On lab-1: `quoro cluster promote`.

**Expected:**

- A majority answers `granted`. The command then waits until the old primary's grants have passed their gates: the latest answer's receipt time plus its `lease_wait_ms`, at most about 33 s after lab-2's last support request ([protocol section 8.9](../contracts/node-agent-protocol.md#89-promotion-votes-m5b)). It prints the time left.
- It builds the database from lab-1's followed log, moves the primary status to lab-1 at the new term, records `cluster.promoted`, and starts the control plane on lab-1.
- The nodes and the edge reach lab-1 through the addresses the old primary advertised. `GET /api/v1/cluster` against lab-1 shows `primary_node_id` lab-1 and a `term` one higher. lab-1, lab-2 and lab-3 are `connected`.
- **The demo app stops for a while.** Nothing answers lab-1's renewals once lab-2's control plane stops. lab-1 fences the demo app about 13.5 to 15 s after `t2`: an outage of the primary longer than `D` fences `warm` apps by design, until M6's election ([protocol section 8.11](../contracts/node-agent-protocol.md#811-maintenance-window-m5b)). The promotion's wait ends only after every holder has fenced. lab-1's control plane then gives the lease back to its last holder, lab-1, at a higher token: once it has run for 18 s (`duration_ms + 2M`) and holds majority support ([protocol section 7.3](../contracts/node-agent-protocol.md#73-promotion), [ADR 0281](../adr/0281-the-primary-regrants-a-lease-from-hello-only-when-the-node.md)). The app then starts again on lab-1 from a cold start.

Switch `$QUORO` to lab-1's address for every call from here.

**`--ack-fence` instead of the wait.** `quoro cluster promote --ack-fence --statement "<text>"` skips the wait for the old primary's grants ([ADR 0236](../adr/0236-ack-fence-needs-a-written-statement-skips-only-the-voters.md)). It does not skip the refusal while the voters still support lab-2, and it does not skip any lease's own gate. Its statement says that the old primary's host is down and stays down. In this demo that is not true, because lab-2's agent keeps running and lab-2 comes back. The flag also declares lab-2 lost in the same write, so every lease lab-2 holds is revoked with a new token, and lab-2's stateful apps stay stopped until an operator acts. The demo therefore takes the wait. Use the flag only if the maintainer asks for it, and record that choice.

**Evidence:**

- `t2` and `systemctl is-active` on lab-2;
- the command output with its wait;
- `GET /api/v1/cluster` from lab-1;
- the `cluster.promoted` audit record (`q "$QUORO/api/v1/audit?order=desc&limit=5"`), whose `params` carry `from_node_id`, `to_node_id`, `term`, `log_seq`, `ack_fence` and `accept_loss`;
- `GET /api/v1/nodes` showing every machine connected to lab-1;
- the `node.fenced` event for lab-1, the lease's `fencing_token` before and after, and how long the domain did not answer;
- the domain answering `200` again after the lease came back to lab-1.

**If it goes wrong:**

- Before any real vote, the command changes nothing. Start lab-2's control plane again with `systemctl start quoro` and stop. If more than about 13 s have passed since `t2`, lab-1 has already fenced the demo app; lab-2 gives the lease back to lab-1 in the same way once it runs again.
- If the command was interrupted after it froze lab-1's agent, run `quoro cluster promote --abort` on lab-1. If it stopped after it wrote the new database, a rerun is refused because lab-1 now has a control-plane database: move that database aside first with `quoro node rejoin --force --statement "<why>"`. lab-1 was never demoted, so rejoin needs `--force` here, and it records the statement ([protocol sections 7.3 and 7.4](../contracts/node-agent-protocol.md#73-promotion)).
- After the real votes, go forward only. Run `quoro cluster promote` again at the same or a higher term, and do not start lab-2's control plane. A primary that sees a higher term demotes itself ([ADR 0230](../adr/0230-a-primary-that-learns-of-a-higher-term-demotes-itself-with.md)).
- If a node does not find lab-1, point it there with `quoro node set-primary <lab-1 URL>`. The certificate pin does not change.

### 6c. The end of step 6: promote lab-2 back

The roadmap's step 6 ends by promoting lab-2 back. The demo uses option A. Option B stays here as the fallback if the second promotion cannot run.

**Option A: promote lab-2 back (the roadmap's script).**

1. On lab-2: `quoro node rejoin` ([#728](https://github.com/OmarAlex24/quoro/issues/728)). lab-2's control plane was stopped by hand in 6b and never learned of lab-1's term, but its agent has seen it, so lab-2 counts as demoted and no `--force` is needed. It stops and disables lab-2's control-plane unit, then moves its data directory and the unit file into a copy named with the term and the time. Nothing is deleted. lab-2's agent finds lab-1. Its followed log differs from lab-1's, so it quarantines that log and follows again from `seq` 1 ([protocol section 7.4](../contracts/node-agent-protocol.md#74-the-old-primary)). Wait until lab-2's `log_lag` is 0 and its `custody` is `sealed`.
2. On lab-2: `quoro cluster promote`. Expected: refused with `support_held`, because lab-1's control plane runs. lab-2 has neither a control-plane database nor a unit any more, so section 7.3 does not refuse it for them. With its `log_lag` at 0 and its `custody` `sealed` (step 1), `support_held` is the expected refusal.
3. On lab-1: `systemctl stop quoro`. Its agent keeps running.
4. Wait at least 15 s, then on lab-2: `quoro cluster promote`. Expected: it waits as in 6b, then lab-2 is the primary at the next term. As in 6b, lab-1 fences the demo app once its control plane stops, and lab-2 gives the lease back to lab-1 once it holds majority support.
5. On lab-1: `quoro node rejoin`. Its control plane was stopped by hand in step 3 and never learned of the newer term, but its agent has seen lab-2's higher term, so lab-1 counts as demoted and no `--force` is needed. If it refuses, lab-1's agent has not reached lab-2 yet: check that lab-1 is `connected` and run it again. Do not use `--force`. lab-1 is a node again.
6. Check: `GET /api/v1/cluster` against lab-2 shows `primary_node_id` lab-2. Every machine is connected, and the demo app runs.
7. Restore the backup repository key on lab-2, with `$QUORO` pointing at lab-2 again. The repository key never travels in the change log, so a promoted primary rebuilds the backup repository without it. Until the owner gives the recovery key back, lab-2 takes no backup, restores none and runs no garbage collection, and each scheduled backup fails with `repository key absent` ([ADR 0322](../adr/0322-how-a-promoted-primary-gets-the-backup-repository-key-back.md), [protocol section 7.3](../contracts/node-agent-protocol.md#73-promotion) step 4). The call is owner-only: use the owner's token, and type the recovery key in rather than putting it on a command line.

   ```sh
   REPO=$(q "$QUORO/api/v1/backup-repositories" | jq -r '.items[0].id')
   q "$QUORO/api/v1/backup-repositories/$REPO" | jq '{recovery_key_fingerprint, repository_key_present}'
   read -rs QRK    # the owner's recovery key, qrk1-..., not echoed
   printf '%s' "$QRK" | jq -Rs '{recovery_key: .}' |
     q -X POST "$QUORO/api/v1/backup-repositories/$REPO/repository-key" \
       -H 'Content-Type: application/json' --data-binary @- | jq '{id, repository_key_present}'
   unset QRK
   ```

   Expected: before the call, `repository_key_present: false`. The call answers `200` with `repository_key_present: true`. lab-2 checks the key's fingerprint against `recovery_key_fingerprint`, unwraps the repository key from the newest config object in the bucket, and writes it on lab-2 only ([backup format section 4](../contracts/backup-format.md#4-config-object)). The audit log has the call with `params` `{"recovery_key": "[redacted]"}`. A `422 validation_failed` means the key is not the repository's current recovery key, or the bucket cannot be read; check the fingerprint the owner holds and the bucket. A `409 conflict` means lab-2 already holds the key. If `GET /api/v1/backup-repositories` lists no repository, skip this step.

This is the state the demo started from, with a higher term and two quarantined control-plane directories, one on each host.

The key is also absent on lab-1 while lab-1 is the primary, from 6b until lab-2 is promoted back in step 4. The demo restores it only at the end, on lab-2: a restored key stays on the primary that received it, so a key restored on lab-1 would be absent again on lab-2 after the promotion back. Scheduled backups that fall due while lab-1 is the primary fail with `repository key absent`; that is expected.

**Option B: leave lab-1 as the primary.**

1. On lab-2: `quoro node rejoin`, as in option A step 1.
2. Restore the backup repository key on lab-1, as in option A step 7, with `$QUORO` pointing at lab-1.
3. Stop there. lab-1 is the primary and a node, and lab-2 is a node.

Option B is shorter and does not run a second promotion. Every later lab session then uses lab-1's address. Option A proves promotion twice. It also leaves the lab as every other runbook expects it.

**Evidence:** the outputs of each command, the backup repository before and after the key is restored, the audit record of the restore, and `GET /api/v1/cluster` and `GET /api/v1/nodes` at the end. Never save the recovery key.

## Cleanup

In this order:

1. **Check the cut.** On lab-1, `nft list table inet quoro_lab_cut` fails, and `systemctl list-timers quoro-lab-cut-deadman.timer` lists nothing.
2. **The demo app.** Set it back to `ha_policy: none` before anything in the voter set changes. `deleteApp` and `deleteVolume` are refused with `has_quarantines` while a quarantined copy exists, so delete the app, if at all, only after the next item. Its `expiring` lease blocks removing a vote.
3. **The quarantined copy.** First save its record: id, node, size and SHA-256. Then decide whether to keep it. Deleting it is human-only and cannot be undone: `q -X DELETE "$QUORO/api/v1/volumes/$VOL/quarantines/<qrn id>"` ([#732](https://github.com/OmarAlex24/quoro/issues/732)).
4. **The witness role.** Keep it: M6 needs three votes. Remove it only if the maintainer asks for that: `{"remove":["witness"]}` once no `expiring` lease row is unrevoked or still behind its gate, and no lease has `local_pending` true: after an app leaves `warm`, its holder keeps renewing until it takes the `local` grant, and the guard does not count that lease. Never use `force`.
5. **Quarantined control-plane directories** from step 6. They stay on the hosts until the maintainer decides. Record where they are.
6. **Final state.** `GET /api/v1/nodes`, `GET /api/v1/cluster` and `GET /api/v1/apps`, compared with the pre-flight readings. Request every demo domain once.

## Decided points

The maintainer approved these choices on 2026-11-10:

- **Step 3b** uses a 30 s maintenance window, so the refusal of box 6 shows on every run ([#764](https://github.com/OmarAlex24/quoro/issues/764)). Until the command is on `main`, repeated runs.
- **Box 12's lab run** is step 3a with a second `warm` app on lab-1, so that two `expiring` fences run at once on the same node. The second app is not failed over; it comes back on lab-1 when the cut ends.
- **Step 6 ends with option A:** promote lab-2 back, as the roadmap says.
- **`quoro node rejoin`** moves the control-plane unit file into the moved-aside copy together with the data directory, so a host that rejoined can be promoted again. It refuses on a machine that was never demoted unless `--force --statement "<why>"` is given. A machine counts as demoted when its database records the demotion, or when its agent has seen a higher term than its database ([protocol section 7.4](../contracts/node-agent-protocol.md#74-the-old-primary)).
- **The demo app** is `docker.io/sigoden/dufs:v0.45.0` (index digest `sha256:9b45a406b019921c7d2a512784a8652fc23a83c5598bf0ddc81d3c130c623f31`), port 5000, with its volume at `/data` and the variables `DUFS_SERVE_PATH=/data` and `DUFS_ALLOW_UPLOAD=true`. A record is written with `PUT /<name>` (answers `201`) and read with `GET /<name>`.

## Open points

- **The panel.** The panel on `main` shows the warm copy's last shipped snapshot (box 4, [#562](https://github.com/OmarAlex24/quoro/issues/562)) but not the switchover action (step 5, no issue yet). Until it ships, the API answers are the evidence for step 5.
- **Reaching the VM from its host.** Step 3a writes record B from lab-1's host to the app's VM at its private address. The lab allows it today, but the contracts do not state that the node's firewall admits that path.
- **TLS after the promotion.** The promoted primary keeps the old primary's certificate. Curl options that pin the certificate and do not check the host name work against lab-1. Options that check the name against the certificate fail if the certificate does not name lab-1's address. Which names the certificate carries is not stated here.
