# ADR 0119: Build log transport: tagged serial-console lines out of the builder VM, kept on the node, streamed to the panel over SSE via a new stream_open kind build_logs

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0119
- **Tags:** builds, m4, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Spike: buildctl output on stdout is already tagged by quoro-init on the serial console like app logs; --progress=rawjson yields the failing vertex (name, error, timing) as JSON; init status region carries the exit code. No vsock or guest agent needed.

## Decision

Build log transport: tagged serial-console lines out of the builder VM, kept on the node, streamed to the panel over SSE via a new stream_open kind build_logs.

Node parses rawjson into BuildEvents and keeps plain lines as the human log next to the build's work dir (ADR 0014). Control plane: GET /api/v1/builds/{id}/logs (paginated, finished builds) and GET /api/v1/builds/{id}/logs/stream (SSE, ADR 0057) backed by node-agent stream_open kind build_logs {build_id, tail}. Replicated: only build status and failed_step (vertex, name, error, Dockerfile line, a few excerpt lines), so the panel shows the failure when the node is down. vsock stays a later option.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0119 in the project decision ledger. Do not edit by hand; changes go through a new decision.
