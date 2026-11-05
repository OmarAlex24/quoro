# ADR 0148: Control-plane backup: VACUUM INTO on the primary every 6 h and before upgrades, fed through the same chunk engine; restore is a CLI step on a fresh host with the recovery key (quoro restore)

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0148
- **Tags:** m5a, contracts, sync
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0069 allows backup API or VACUUM INTO; VACUUM INTO gives a consistent, minimal copy with deleted content (old secrets) purged and is not a write operation; the threat model's M5a check restores a primary snapshot.

## Decision

Control-plane backup: VACUUM INTO on the primary every 6 h and before upgrades, fed through the same chunk engine; restore is a CLI step on a fresh host with the recovery key (quoro restore).

Contract B: source kind control_plane in manifests; schedule settable by the owner; CLI restore documented, no panel flow in M5a. Backup API rejected as default because it copies freed pages. Recommendation: accept.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0148 in the project decision ledger. Do not edit by hand; changes go through a new decision.
