# ADR 0144: Mesh in M5a: kernel WireGuard star with the edge as hub (node-to-node peers in M5b), driven by netlink-packet-wireguard + genetlink; one /24 per node from 10.42.0.0/16 stored in nodes.subnet (#77), existing non-first nodes renumbered on redeploy

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0144
- **Tags:** m5a, contracts, networking
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0063 fixes kernel WireGuard over netlink; only edge-to-instance traffic crosses nodes in M5a; every node has the same guest subnet today so the edge cannot route to two nodes. wireguard-control is LGPL (F-0245).

## Decision

Mesh in M5a: kernel WireGuard star with the edge as hub (node-to-node peers in M5b), driven by netlink-packet-wireguard + genetlink; one /24 per node from 10.42.0.0/16 stored in nodes.subnet (#77), existing non-first nodes renumbered on redeploy.

Contract A: hello carries the mesh public key (private key generated and kept on the machine); desired_state.spec.mesh {listen_port default 51820, address, peers[{node_id, public_key, endpoint?, allowed_ips, persistent_keepalive_s?}]}; nodes.subnet column, assigned at enrolment, never reused while a VM from it is recorded; the first node keeps its current subnet, other nodes get a new /24 and their VMs move on next deploy (release notes, like D-0136). nftables: from the WireGuard interface only the edge's mesh address may reach the backend guest address:port pairs. Recommendation: accept the star and the crates; full mesh with M5b warm replication.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0144 in the project decision ledger. Do not edit by hand; changes go through a new decision.
