# ADR 0116: M4 VM console: read-only; serial in socket mode with the node agent as its only client from vm.create and a node-side ring per VM; viewers fan out from the node

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0116
- **Tags:** m4, console, engine, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

CH v54 socket mode keeps a 1 MiB ring until a client attaches (consumed on replay) and a new connection silently takes over from the current one, so viewers must never connect directly. Today's file= serial.log is truncated at every VMM start and unbounded. Nothing in the guest reads ttyS0, so input is useless.

## Decision

M4 VM console: read-only; serial in socket mode with the node agent as its only client from vm.create and a node-side ring per VM; viewers fan out from the node.

Engine-trait change: VmSpec carries both the app-log virtio-console and a serial socket (ConsoleMode is exclusive today); Engine::console reads the node ring plus live. Ring: e.g. two 512 KiB segments in the VM dir, survives VMM restarts (keeps the previous boot). CH's own 1 MiB ring covers node-agent restarts (KillMode=process). Keep loglevel=4 (init diagnostics are the boot output; serial costs ~30 KB/s of guest CPU on the lab). Node protocol: stream_open kinds terminal {vm_id, rows, cols, argv?, user?} and console {vm_id, tail_bytes?}; stream_data.data base64 <=64 KiB; new stream_input {stream_id, seq, data?, resize?, eof?} (refused on console); new stream_ack {stream_id, acked_bytes} with a 256 KiB window so a busy terminal cannot starve heartbeats; stream_end exit_code. Panel WS: binary frames = bytes, text frames = JSON control (resize, exit, closed reason, error). Minimal fallback: tail today's serial.log read-only. Evidence: docs/research/m4-terminal-and-console.md (PR #155, issue #151).

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0116 in the project decision ledger. Do not edit by hand; changes go through a new decision.
