# ADR 0194: draining_until is an RFC 3339 field on tunnel routes only (spec.routes and spec.edge.routes), window 120 s after the CNAME repoint; the machine never acts on the time and a machine with no running backend gets no draining route

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0194
- **Tags:** m5b, contracts, ingress, edge
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0168 chose option A (primary keeps the departed route, marked draining_until) with a window of about 120 s; the first M5b contract change must fix the field's form, where it appears and who acts on the time.

## Decision

draining_until is an RFC 3339 field on tunnel routes only (spec.routes and spec.edge.routes), window 120 s after the CNAME repoint; the machine never acts on the time and a machine with no running backend gets no draining route.

Field: optional string, RFC 3339 like every protocol timestamp, set only on tunnel routes, in spec.routes (1->2: the node keeps its own VM as backend) and spec.edge.routes (2->1: the edge keeps the remaining instance). The primary also keeps the hostname in the losing tunnel's ingress rules for the window and drops both in the first desired_state issued after draining_until. Nodes and the edge serve a draining route exactly like any other and never act on the time (pure appliers, D-0168); an older node ignores the field and serves the route. The vm_ids rule 'a node gets routes only for apps with one replica' gains this one exception. Window: 120 s after the CNAME was repointed.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0194 in the project decision ledger. Do not edit by hand; changes go through a new decision.
