# M4 web terminal and VM console

- **Question:** M4 spike (issue #151): how the web terminal reaches "the app's context", how the microVM's serial console is streamed to the panel, and what the M4 contract pull request must add (ADR 0031, ADR 0056, ADR 0057, ADR 0089, ADR 0096, ADR 0112)
- **Researched:** 2026-11-04, on lab-2 (Cloud Hypervisor v54.0, the pinned guest kernel 7.2.8, nested KVM), with a throwaway guest program and host relay kept outside the repository
- **Status:** answered; the recommendations need maintainer decisions before the contract pull request

## Short answer

1. **Web terminal: exec through Quoro's own init over virtio-vsock.** Every microVM gets a `--vsock` device whose host end is a UNIX socket in the node's root-only data directory. `quoro-init` listens on one vsock port and, per connection, starts a PTY session inside the app's root filesystem with the app's environment. The node agent (root) is the only process that connects; the control plane (unprivileged) only relays bytes over the node session it already has. On lab-2 a prototype of exactly that ran `uname -a` in 21 ms, carried 56 MB/s, resized the PTY, served three sessions at once and stopped the guest cleanly when the reader stalled. A shell on the node, SSH into the guest, or a getty on the serial port are all worse (section 1.3).
2. **VM console: read-only in M4, the serial port in `socket` mode, with the node agent as its permanent single client and a node-side ring buffer.** Today the serial port is a file that Cloud Hypervisor truncates at every VMM start and lets grow without bound. Cloud Hypervisor's socket mode keeps a 1 MiB ring while no client is attached and replays it to the next client, but it serves one client and a new connection silently takes over from the old one, so neither the panel nor a second agent task may ever connect to it directly. Nothing in the guest reads the serial port, so an interactive console would have nothing to talk to.
3. **The contract pull request** adds two WebSocket session endpoints, two scopes (`app:terminal`, human-only in M4; `vm:console`), an open and a close audit record per session with a node-local transcript referenced by hash, two `stream_open` kinds, a control-plane-to-node `stream_input` message with resize, a credit-based `stream_ack`, and an engine change so one VM can have both the app-log console and a serial socket (section 3).

Every measured number below comes from nested virtualization on the lab and is an order of magnitude, not a benchmark.

## Method

- A scratch microVM started directly with Cloud Hypervisor v54.0 as an unprivileged user in the `kvm` group: no TAP, no disks, the pinned guest kernel, the same kernel command line the engine uses (`console=ttyS0 loglevel=4 panic=-1 rdinit=/init`), 1 vCPU, 256 MiB, `--serial socket=` or `--serial file=`, `--console off`, `--vsock cid=3,socket=`.
- A throwaway static PID 1 (C, musl) standing in for `quoro-init`: mounts, diagnostics on `ttyS0`, a "tick" line every second (to see what happens to serial output while no client is attached), a reader on `ttyS0` (to test input), and a vsock listener that runs a PTY session per connection. Frames are `[u8 type][u32 length][payload]`: open (rows, cols, argv), stdin data, resize, close; output data, exit status, error. A static BusyBox supplied the shell.
- A host relay in Python standing in for the node agent: Cloud Hypervisor's hybrid vsock handshake, then the frames above.
- Cloud Hypervisor v54.0 source read for the console behaviour (`serial_buffer/src/lib.rs`, `serial_buffer/src/socket.rs`, `vmm/src/serial_manager.rs`, `vmm/src/console_devices.rs`, `docs/vsock.md`).
- Read-only checks of the running node: the VM directory layout, the serial log of the demo app's VM (a file read, never its sockets), and whether the demo app still answered afterwards.

## Findings

### What Quoro does today

| Item | Today | Evidence | Label |
| --- | --- | --- | --- |
| App output | virtio-console in `socket` mode under the node data dir's `consoles/`, read by the node agent as tagged `out`/`err` lines (the M2 live-log path) | `crates/node-agent/src/reconcile.rs`, `crates/engine/src/ch/vmconfig.rs`, a running VM's `spec.json` on lab-2 | Confirmed |
| Serial port | `file` mode, `serial.log` in the VM directory, carrying kernel messages and `quoro-init:` diagnostics | `vmconfig.rs` (`Off`/`VirtioConsole` both map serial to `File`) and lab-2 | Confirmed |
| Console modes | `ConsoleMode` is one of `Off`, `VirtioConsole { socket }`, `Serial { socket }`, so a VM has either the app-log socket or a serial socket, never both; `Engine::console` requires `Serial` and `Engine::output` requires `VirtioConsole` | `docs/contracts/engine-trait.md` section 3, `crates/engine/src/ch/engine.rs` | Confirmed |
| Boot output volume | With `loglevel=4` a whole boot is 599 bytes: three kernel warnings and nine `quoro-init:` lines ending in `app started` | the demo VM's `serial.log` on lab-2 | Confirmed |
| Guest input on the serial port | Nothing in the guest reads `ttyS0`: `quoro-init` only writes diagnostics there and starts no getty | `crates/init/README.md` "Console protocol", `crates/init/src/guest/` | Confirmed |
| vsock | No VM has a vsock device; the pinned guest kernel has virtio-vsock built in (`virtio_vsock_init`, `vsock_init` initcalls in the image) | lab-2, strings of the pinned `vmlinux` | Confirmed |
| Node protocol | `stream_open` kinds `app_logs` and `deployment_logs`; `console` and `terminal` are reserved for M4; `stream_data.data` (base64) is reserved for byte streams; there is no control-plane-to-node data message; text frames only, 1 MiB per message, 32 streams per session | `docs/contracts/node-agent-protocol.md` sections 3.2, 5.7, 5.15 | Confirmed |
| Panel auth on a WebSocket | The panel authenticates with the `quoro_session` cookie (`HttpOnly; SameSite=Strict`); browsers cannot set `Authorization` on a WebSocket; axum's `ws` feature is already enabled for the node session | `crates/control-plane/src/auth/mod.rs`, `api/auth.rs`, `Cargo.toml` | Confirmed |

### 1. Web terminal

#### 1.1 What "the app's context" is

There are no containers or namespaces inside a Quoro microVM (ADR 0008): `quoro-init` mounts the overlay root, switches into it with `chroot`, sets the hostname, network and user, and starts the app. Anything `quoro-init` starts after the switch therefore runs in the app's filesystem, network, hostname and kernel, which is the whole of the app's context. A terminal needs no `nsenter`-style entry; it needs a process that the init starts on request, with the app's environment and, by default, the app's user.

#### 1.2 Measurements of exec over vsock (option a)

| Check | Result | Label |
| --- | --- | --- |
| Hybrid vsock handshake (`CONNECT 1024\n` on the VMM's UNIX socket, answer `OK <port>`) | 1.3 ms | Confirmed (lab) |
| Guest listener ready | the guest bound and listened on vsock port 1024 on its first try, 0.1 s after boot | Confirmed (lab) |
| First shell prompt after the open frame | 5.1 ms | Confirmed (lab) |
| `uname -a` round trip (command echo to prompt, including the relay's polling) | 21 ms; the output names the guest kernel, so it ran in the guest | Confirmed (lab) |
| PTY | `tty` printed `/dev/pts/0`; `stty size` printed `24 80`, and `50 132` after a resize frame (`TIOCSWINSZ`) | Confirmed (lab) |
| Exit status | `exit 7` arrived as wait status 1792 (exit code 7) | Confirmed (lab) |
| Close | a close frame sent `SIGHUP` to the session's process group; the shell ended by signal 1 | Confirmed (lab) |
| Concurrency | three sessions at once, three shells, three exit codes, 1.0 s total for a `sleep 1` each | Confirmed (lab) |
| Throughput | 67 MB of `dd` output through the PTY and vsock in 1.19 s, about 56 MB/s | Confirmed (lab) |
| Back-pressure | with `yes` running and the host not reading for 10 s, Cloud Hypervisor's RSS stayed at 83.6 MiB (+60 KiB); reading resumed at the same rate. vsock's credit flow control stops the guest, so a stalled reader costs no host memory in the VMM | Confirmed (lab) |
| Missing shell | asking for `/bin/bash` in an image that has none returns `exec /bin/bash: No such file or directory` and exit 127 | Confirmed (lab) |
| Port without listener | `CONNECT 2048` is closed by the VMM with no answer | Confirmed (lab) |
| Host kernel module | Cloud Hypervisor held no `/dev/vhost-*` file descriptor; its vsock device is implemented in the VMM, and the host side is the UNIX socket | Confirmed (lab: `/proc/<pid>/fd`) |
| Guest-to-host direction | a guest can only reach the host if a process listens on `<socket>_<port>`; the node never creates one, so the guest cannot open connections to the host | Confirmed (Cloud Hypervisor `docs/vsock.md`); the node rule is a recommendation |
| The same CID for every VM | each VMM has its own UNIX socket, so a fixed guest CID per VM does not collide on the host | Hypothesis (true for the hybrid backend measured here; would not hold with vhost-vsock) |

#### 1.3 Options compared

| Option | How | Verdict |
| --- | --- | --- |
| **(a) exec through `quoro-init` over vsock** | The node agent connects to the VM's vsock socket; the init spawns a PTY session in the app's root | **Recommended.** Runs inside the guest (threat model T-LOG-06), many sessions, resize, exit codes, back-pressure, no network exposure, no new host privilege, works for every image that has the requested program |
| (b1) a shell on the node | `nsenter` or a root shell on the host | Rejected. There is no namespace to enter (a microVM is not a container), and the result is a root shell on the host, which is the opposite of "in the app's context" and of T-LOG-06 |
| (b2) mount the VM's disks on the host | loop-mount the root and data images | Rejected. Files only, no processes; mounting a live ext4 that the guest also writes corrupts it |
| (b3) SSH into the guest | sshd in the image, keys pushed in | Rejected. Needs sshd and keys in every image, listens on the VM's network, where every VM of the same project can reach it (ADR 0084) |
| (c1) a getty on the serial port or a second console | the init starts a shell on `ttyS0` or `hvc0` | Rejected. One client, and a new client silently takes over (section 2); no resize signalling; one shell shared by every user who opens it, with its history and working directory; about 30 KB/s on the lab; `ttyS0` also carries kernel and init output, `hvc0` carries the tagged app logs |
| (c2) an in-guest agent over the VM's network | the init or a sidecar listens on TCP | Rejected. Same exposure as (b3) |

#### 1.4 What `quoro-init` needs

- A vsock listener on one fixed port, started after the switch into the app's root and before the app, kept for the VM's lifetime, never accepting anything but connections from the host (vsock delivers only host connections to a guest listener).
- Per connection: read an open request (argv, optional user, rows, cols), allocate a PTY, start the program as a session leader with the PTY as its controlling terminal, the app's environment (`TERM` set), the app's working directory and the app's user unless the request names another, relay bytes and resize until the program exits, send the exit status, and on a close from the host send `SIGHUP` to the session's process group.
- **The workspace forbids `unsafe`.** `std::os::unix::process::CommandExt::pre_exec`, the usual place to call `setsid` and take a controlling terminal between fork and exec, is an `unsafe fn`. A safe path is for the init to re-execute itself as a small session helper (`/proc/self/exe` with a helper argument): the helper calls `setsid` and opens the PTY's secondary side, which becomes its controlling terminal, moves it onto stdin, stdout and stderr, and then replaces itself with the requested program through the safe `CommandExt::exec`. rustix provides `setsid`, the PTY calls (`openpt`, `grantpt`, `unlockpt`, `ptsname`) and `tcsetwinsize`. rustix 1.1.6 knows `AddressFamily::VSOCK` but has no vsock socket-address type, and its `SocketAddrArg` trait is `unsafe` to implement, so the listener cannot be written with rustix alone under the workspace's `forbid(unsafe_code)` (Confirmed, rustix 1.1.6 source). The `vsock` crate (0.5.5, Apache-2.0, on the ADR 0080 allowlist) provides a safe `VsockListener`; it is the smallest addition, and keeps the `unsafe` inside a dependency the way rustix already does.
- A versioned config change: the init's config grammar refuses unknown keys, so an `exec` line (or a port) is a new `quoro-init-config` version, and the engine and the init must ship together. The initramfs is rebuilt from the installed binary, so a VM gets the new init on its next start; running VMs keep the old one until they restart.
- The engine adds `--vsock cid=<n>,socket=<vm dir>/vsock.sock` at create. Cloud Hypervisor creates that socket at boot, not at create (measured), in the node's root-only data directory.
- **Images without a shell** (distroless, `scratch`): the session can run any program the image has, and fails with a clear "not found" otherwise. A Quoro-supplied static toolbox (for example on a read-only extra disk mounted at a hidden path) would cover those images, but it ships a third-party binary inside every VM with its own license and update duty (BusyBox is GPL-2.0, outside the ADR 0080 allowlist, which covers Rust dependencies), so it is left for after M4.

#### 1.5 Security of the terminal

- **Who opens it.** A new scope `app:terminal`. A shell inside the app reads every environment variable and secret the app holds, can change the app's files and processes, and its effect cannot be expressed as one audited API call. M4 makes it human-only and part of the `owner` and `operator` bundles, not `viewer`. Agents keep acting through scoped API calls; a later, non-interactive `app:exec` (one argv, captured output, recorded in full) reachable only through an approval grant (ADR 0030) fits agents better than an interactive PTY, and is a separate decision.
- **Privilege split (ADR 0089, ADR 0096).** The control plane never touches a VM socket: it authorizes, audits, and relays bytes between the panel's WebSocket and the node session it already holds. The node agent, already root and already the only process that reads VM sockets, connects to the vsock socket. No new privilege appears in the API process, and the vsock socket lives in the node's `0700` root-owned data directory.
- **The guest is untrusted.** The app (often root in its own guest) can kill or impersonate the session, read what the human types and print anything. The node therefore bounds every frame, never parses guest output as anything but bytes, and the panel's xterm.js must disable features that act on the browser (clipboard writes through OSC 52, link handlers that open URLs without a click) and render the session as a terminal, never as HTML (threat model T-LOG-03).
- **A long-lived WebSocket is one request.** The permission model checks a grant or role on every request (T-GRT-04). A session must therefore be re-checked while it is open: the primary closes it when the identity is disabled, the role changes, the session cookie or token expires, or (when agents get access) the grant is revoked, and on an idle timeout (proposal: 15 minutes without input) and a hard maximum (proposal: 8 hours).
- **Cross-site WebSocket hijacking.** A browser sends cookies on a WebSocket handshake from any origin. `SameSite=Strict` already blocks cross-site use, but the handshake must also check `Origin` against the panel's own origin, and the recommended open flow (section 3.1) adds a single-use ticket so the upgrade request itself carries no ambient authority.

#### 1.6 Audit of a session

The audit log records one record per call (`audit-log-format.md` section 3), and a terminal session is a call that lasts minutes. What can be recorded:

| Option | What | Trade-off |
| --- | --- | --- |
| Open and close only | the open as a normal record (who, on whose behalf, target VM, rows, cols, argv, result) and a close record (duration, bytes each way, exit code, close reason) | Cheap, says nothing about what was done |
| "Commands" | parse the input stream into commands | Not reliable: a PTY carries keystrokes, line editing, history recall, pasted text and full-screen programs; a parser gives a false sense of completeness |
| **Open and close, plus a node-local transcript referenced by hash** | the node records the session as a timed byte transcript (input and output, asciicast-style) to a size-capped file; the close record carries its SHA-256 and size | **Recommended.** The hash-chained close record makes later tampering with the transcript evident (ADR 0031); the transcript stays on the node like other telemetry (ADR 0014); the audit log does not grow with session size |
| Transcript in the audit log or on the primary | ship the bytes to the primary | Moves bulk session data off the node against ADR 0014 and puts secrets typed or printed in a session into the replicated, exported audit log |

Either transcript option records secrets that appear in the session (a printed environment, a pasted password). That is the purpose of an audit trail of a root-equivalent session, and it means transcripts need the same access control as the audit log (`audit:read`) and a retention limit.

### 2. VM console

#### 2.1 Cloud Hypervisor's serial back-ends (v54.0)

| Mode | Behaviour | Evidence | Label |
| --- | --- | --- | --- |
| `file=<path>` | `File::create` at every VMM start, so the previous boot's output is lost on restart; no size limit while the VM runs; no input | `vmm/src/console_devices.rs`; lab: a second VMM on the same path left 7 lines of the 318 written by the first | Confirmed |
| `socket=<path>` | stream UNIX socket, created at `vm.create` (before boot) | lab | Confirmed |
| `socket=` with no client | output goes to a ring of `MAX_BUFFER_SIZE = 1 << 20` bytes that keeps the newest bytes; the ring is replayed to the next client and emptied by the replay | `serial_buffer/src/lib.rs`; lab: a client connecting 6 s after boot received the whole boot, and a client returning after a 12 s gap received the lines written during the gap and nothing older | Confirmed |
| `socket=` with a slow client | bytes the client cannot take are put in the same 1 MiB ring; the guest is never slowed and the oldest bytes are dropped | `serial_buffer/src/lib.rs` | Confirmed |
| A second client | accepted; the VMM shuts down the current client's connection and serves the new one (last connection wins, silently) | `vmm/src/serial_manager.rs`; lab: client A got end-of-file the moment client B connected, and B received the next line | Confirmed |
| Input | bytes from the client reach the guest's `ttyS0` line discipline (the guest echoed them) | lab | Confirmed |
| Speed | about 30 KB/s from the guest on nested virtualization, the same with and without a client (each byte is a port write the VMM emulates) | lab: 200 KB in 6.3 s of guest system time | Confirmed (lab), slower than bare metal is a Hypothesis |
| `pty`, `tty`, `tcp=` | also available; `tcp=` is loopback-only and single-client | `docs/research/cloud-hypervisor.md` | Confirmed |

The CPU cost of the serial port is on the boot path: the kernel writes console messages through the emulated UART, so a `loglevel=7` boot (318 lines, 21 KB on the lab) would add roughly 0.7 s of boot time at the measured rate (**Hypothesis**, derived from the measured rate, not timed directly). With today's `loglevel=4` a boot writes about 600 bytes.

#### 2.2 Recommendation for the console

- **Serial in `socket` mode, the node agent as its only client from `vm.create`.** The agent connects as soon as the VM is created (the socket exists before boot), so it sees the whole boot. It appends what it reads to a node-local, size-capped ring per VM (proposal: two 512 KiB segments in the VM directory), which survives VMM restarts and therefore keeps the previous boot's output, the one an operator needs after a crash loop. While the agent restarts (`KillMode=process` keeps VMMs running, ADR 0096), Cloud Hypervisor's own 1 MiB ring covers the gap and is replayed on reconnect.
- **Viewers never connect to the socket.** A second connection would silently take the console away from the agent. A console session is a `stream_open` of kind `console` on the node session: the node replays the ring (a requested tail, default all of it) and then follows live output, fanning out to any number of viewers.
- **Read-only in M4.** Nothing in the guest reads `ttyS0`, so input would go nowhere; the interactive shell is the web terminal. If a later engine ships guests with a serial getty, input becomes a separate, human-only scope.
- **Keep `loglevel=4`.** The DoD asks for "the guest's boot output": the `quoro-init:` lines (root ready, hostname, network, user, `app started`, and any `FATAL`) plus kernel warnings are that output and are what an operator needs. A higher level costs boot time on the serial port (section 2.1).
- **The engine contract changes.** `VmSpec` must carry the app-log virtio-console and the serial socket at the same time (for example `log_console` and `serial` as two fields), and `Engine::console` becomes a read of the node's ring plus live output instead of a direct socket connection. This is an engine-trait change and goes into the contract pull request.
- **Minimal alternative.** If the engine change does not fit M4, the node agent can tail today's `serial.log` read-only. It needs no engine change, but it keeps the unbounded file and loses the previous boot on every restart.

#### 2.3 Who opens the console

A new scope `vm:console`: read the serial console of a VM. The output is kernel and init diagnostics (the init prints counts, never environment values), which is closer to logs than to a shell, so `vm:console` is grantable to agents (an agent diagnosing a boot that fails is the loop of ADR 0032) and part of the `owner` and `operator` bundles. Adding it to `viewer` would change the role bundles; M4 can leave it out. The open is one audit record; a close record with duration and byte count matches the terminal's shape.

### 3. Recommendations for the M4 contract pull request

#### 3.1 OpenAPI

| Operation | Scope | Notes |
| --- | --- | --- |
| `POST /api/v1/apps/{app_id}/terminal-sessions` | `app:terminal` (human-only in M4) | Body `{ "rows", "cols", "command"?: [argv], "user"? }`. Picks the app's running VM (or `vm_id` in the body when there are several). Returns `201 { "id", "ticket", "expires_at", "websocket_path" }`; the ticket is single use and expires in 30 s; this call is the audited open |
| `POST /api/v1/vms/{vm_id}/console-sessions` | `vm:console` | Body `{ "tail_bytes"? }`. Same answer shape |
| `GET /api/v1/interactive-sessions/{session_id}/ws?ticket=` | the ticket | WebSocket upgrade (ADR 0057). `Origin` must match the panel's origin for cookie-authenticated tickets. A spent, expired or foreign ticket is `401` before the upgrade |
| `DELETE /api/v1/interactive-sessions/{session_id}` | the opener, or `owner` | Ends a session; the close record names who ended it |

Session ids need a new prefix in the identifier table (proposal: `trm_` for both terminal and console sessions). The paths say `interactive-sessions` rather than `sessions` so they are not confused with the panel's login sessions (the `sessions` table). Tickets can live in the primary's memory: they are single use and short, and the primary is the only process that serves the upgrade.

**WebSocket framing (panel to primary).** Binary frames carry raw terminal bytes in both directions, so xterm.js writes them as they come. Text frames carry JSON control messages: client to server `{"type":"resize","rows":R,"cols":C}`; server to client `{"type":"exit","code":N}`, `{"type":"closed","reason":"idle_timeout"|"max_duration"|"revoked"|"ended_by_user"|"node_disconnected"|"vm_stopped"}` and `{"type":"error","code":...}`, then a normal close. A console session accepts no binary frames from the client (read-only).

#### 3.2 Scopes (`permission-model.md`)

- `app:terminal`: open an interactive terminal in an app's VM. Human-only in M4; in the `owner` and `operator` bundles.
- `vm:console`: read a VM's serial console. Grantable; in the `owner` and `operator` bundles.

#### 3.3 Audit (`audit-log-format.md`)

- The open is a normal record for the `POST`, with `target_type` `vm` (or `app`), `params` `{rows, cols, command, user}` and the session id in `result_detail` or a new optional field `session_id`.
- A new recorded action, the **session end**, written by the primary when the session closes for any reason, with the same actor, `on_behalf_of` and channel as the open, `operation` `session.end`, and `params` `{session_id, kind: "terminal"|"console", duration_ms, bytes_in, bytes_out, exit_code?, close_reason, transcript_sha256?, transcript_bytes?}`. It is the only way to know a session ended and how.
- Transcripts stay on the node (section 1.6); retention and how `audit:read` holders fetch them are decisions for the contract pull request.

#### 3.4 Node-agent protocol (`node-agent-protocol.md`)

- `stream_open` kind `terminal`, params `{vm_id, rows, cols, argv?, user?}`; kind `console`, params `{vm_id, tail_bytes?}`.
- `stream_data` for both kinds uses the reserved `data` field (base64), at most 64 KiB of decoded bytes per message, coalesced over a few milliseconds so one keystroke echo is not one message.
- A new message **`stream_input`**, control plane to node: `{stream_id, seq, data?, resize?: {rows, cols}, eof?}`. A node answers `bad_message` for `stream_input` on a `console` stream.
- A new message **`stream_ack`**, control plane to node: `{stream_id, acked_bytes}`. The node sends at most a fixed window (proposal: 256 KiB) of unacknowledged output per stream, then stops reading the vsock socket; the measured vsock back-pressure then stops the guest program. Without it, one busy terminal can fill the node session's queue and delay heartbeats and lease messages on the same WebSocket.
- `stream_end` gains an optional `exit_code` for `terminal`.

#### 3.5 Engine trait and guest init

- `VmSpec` carries a vsock device and both consoles (section 2.2); `Capabilities` gains `exec: bool`.
- A new `Engine::exec(id, ExecRequest) -> ExecSession` (a byte stream plus resize and an exit future), implemented over the vsock socket; `Engine::console` reads the node's ring.
- `quoro-init-config` v2 with the exec listener settings (section 1.4).

## Implications for Quoro

- ADR 0056 (consoles are text-only, xterm.js over serial or virtio-console, "or a vsock in-guest agent") already allows both mechanisms recommended here; no ADR has to be superseded.
- ADR 0057 (WebSockets for interactive sessions) holds; the node session needs `stream_input` and `stream_ack` because today it only carries data from node to primary.
- ADR 0089 and ADR 0096 hold: no new privilege in the control plane.
- ADR 0014 holds: console bytes and transcripts stay on the node; only the audit records travel.
- The engine-trait statement that `console` uses `--serial socket=` and answers `Conflict` on a second reader is accurate for the socket, but the socket's takeover behaviour (a second connection displaces the first) makes a direct per-viewer connection unsafe; the node must own the socket.

## Open risks

- **A new guest dependency.** The init's vsock listener needs the `vsock` crate or an equivalent (section 1.4), the init's second dependency after rustix; it must build static for musl and pass cargo-deny (ADR 0080, ADR 0086).
- **Agent restarts during a session.** A node agent restart drops every open terminal (the vsock connection belongs to the agent process). Sessions are not resumable in M4; the close record says `node_disconnected`.
- **Data lost before the agent connects.** Cloud Hypervisor's ring is 1 MiB and drops the oldest bytes; a guest that floods `ttyS0` while the agent is down loses its earliest output.
- **A guest that floods the serial port** costs host CPU in the VMM (one emulated port write per byte). The node-side ring caps disk use, not CPU.
- **Transcripts hold secrets.** Retention, access and export of transcripts need decisions; until then a cap and `audit:read` access are the minimum.
- **Old guests.** VMs started before the new init have no vsock device and no listener; the terminal answers "not available until the app restarts".
- **Images without a shell** get a terminal that can only run programs the image ships (section 1.4).
- **Nested virtualization numbers.** Every timing here is from nested KVM on the lab and only says "fast enough"; it is not a benchmark.

## Sources

- Cloud Hypervisor v54.0, `serial_buffer/src/lib.rs` (`MAX_BUFFER_SIZE`, ring behaviour): https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/v54.0/serial_buffer/src/lib.rs (accessed 2026-11-04)
- Cloud Hypervisor v54.0, `serial_buffer/src/socket.rs` (one client, buffering until a client connects): https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/v54.0/serial_buffer/src/socket.rs (accessed 2026-11-04)
- Cloud Hypervisor v54.0, `vmm/src/serial_manager.rs` (a new connection shuts down the current client): https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/v54.0/vmm/src/serial_manager.rs (accessed 2026-11-04)
- Cloud Hypervisor v54.0, `vmm/src/console_devices.rs` (`File::create` for `file=`): https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/v54.0/vmm/src/console_devices.rs (accessed 2026-11-04)
- Cloud Hypervisor v54.0, `docs/vsock.md` (hybrid vsock, `CONNECT <port>`, `<socket>_<port>` for guest-initiated connections): https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/v54.0/docs/vsock.md (accessed 2026-11-04)
- rustix 1.1.6 source, `src/net/types.rs` (`AddressFamily::VSOCK`) and `src/net/addr.rs` (`pub unsafe trait SocketAddrArg`): https://crates.io/crates/rustix/1.1.6 (accessed 2026-11-04)
- `vsock` crate 0.5.5, Apache-2.0: https://crates.io/crates/vsock (accessed 2026-11-04)
- Rust standard library, `CommandExt::pre_exec` is `unsafe`, `CommandExt::exec` is safe: https://doc.rust-lang.org/std/os/unix/process/trait.CommandExt.html (accessed 2026-11-04)
- Quoro: `docs/research/cloud-hypervisor.md`, `docs/contracts/engine-trait.md`, `docs/contracts/node-agent-protocol.md`, `docs/contracts/permission-model.md`, `docs/contracts/audit-log-format.md`, `docs/architecture/threat-model.md`, `crates/init/README.md`, `crates/engine/src/ch/`, `crates/node-agent/src/`, `crates/control-plane/src/`
- Lab measurements on lab-2, 2026-11-04 (scratch microVM, removed afterwards)
