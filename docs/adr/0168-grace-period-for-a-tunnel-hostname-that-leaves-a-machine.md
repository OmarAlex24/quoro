# ADR 0168: Grace period for a tunnel hostname that leaves a machine: the machine keeps serving it for about 120 s (a contract change for M5b)

- **Status:** Accepted
- **Date:** 2026-11-05
- **Ledger id:** D-0168
- **Tags:** m5a, contracts, ingress, edge
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Issue #344: when an app's replicas go 1->2 the node drops the hostname from its proxy at once (+0.4 s) while Cloudflare keeps sending some requests to its tunnel until the CNAME change propagates; the lab saw 404s until +70 s. PR #348 removes the delete+create of the CNAME (one PATCH now) but cannot keep the route on the node: node-agent-protocol 5.11 says a node gets routes only for apps with one replica, and 6.2 step 6 applies exactly spec.routes. Keeping the hostname only in the losing tunnel's Cloudflare ingress does not help because the node proxy answers 404 for it.

## Decision

Grace period for a tunnel hostname that leaves a machine: the machine keeps serving it for about 120 s (a contract change for M5b).

The maintainer deferred this to M5b on 2026-11-05, as a standalone contract PR amending node-agent-protocol 5.11 spec.routes and 6.2 step 6 (and 6.8 for the edge side of the reverse move): option A, the primary keeps a departing tunnel route in the losing machine's spec.routes (with backends that still run: for 1->2 the node's own VM stays, for 2->1 the edge keeps its route with the remaining backend) and in its tunnel ingress for a grace window after the CNAME was repointed, marked with a new optional route field draining_until; option B, the node keeps serving a tunnel route that left spec.routes for the grace window while its VM is still in spec.vms. Option A is the recommended design, since the primary already knows when it repointed the CNAME and the node stays a pure applier of desired state. A lab measurement after PR #348 saw the last 404 at up to about 114 s after the change, so the window should be about 120 s. Until the change ships, v0.2.0 documents the remaining 404 tail as a known issue.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0168 in the project decision ledger. Do not edit by hand; changes go through a new decision.
