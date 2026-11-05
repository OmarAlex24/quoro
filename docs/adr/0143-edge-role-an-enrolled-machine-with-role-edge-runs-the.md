# ADR 0143: Edge role: an enrolled machine with role edge runs the embedded proxy from a new desired_state spec.edge (routes with remote backends), does its own active health checks, keeps serving without the primary; Cloudflare LB is a later LoadBalancerProvider

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0143
- **Tags:** m5a, contracts, ingress, edge
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The milestone demo wants an edge health check that removes a node's replica and adds it back; only an own edge does that; Cloudflare LB is paid and sees one endpoint per tunnel. Spike section 3 and 5.

## Decision

Edge role: an enrolled machine with role edge runs the embedded proxy from a new desired_state spec.edge (routes with remote backends), does its own active health checks, keeps serving without the primary; Cloudflare LB is a later LoadBalancerProvider.

Contract A: node-agent-protocol: edge-only machines enrol with roles [edge], need no KVM; desired_state.spec.edge {routes[{hostname, domain_id, app_id, tls_mode, health_check, backends[{vm_id,node_id,address,port}]}], tunnel?}; status_report gains edge backend health; health: probe health_check.path every interval_s, out after `failures` misses, in after the same number of passes, plus passive removal on refused/reset; round robin, retry once on connect failure for GET/HEAD only. A domain on an app with replicas>1 is served by the edge (DNS or tunnel rule points at the edge's tunnel or address). app.degraded when a replica is out of rotation. Recommendation: accept; LoadBalancerProvider (Cloudflare LB) deferred until two edges are wanted.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0143 in the project decision ledger. Do not edit by hand; changes go through a new decision.
