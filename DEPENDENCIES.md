# Dependencies

Every dependency of the Rust workspace is listed here with the reason it is
needed. A pull request that adds a dependency adds its row in the same change;
a dependency without a reason here is a review failure.

Rules:

- Prefer the standard library. Add a crate only when writing the code by hand
  would be more surface to review than the crate itself.
- Enable only the features that are used, with `default-features = false`
  where the crate allows it.
- Test-only needs go in `[dev-dependencies]` and are listed separately.
- `Cargo.lock` is committed and every build and test in CI runs with `--locked`.
- A new dependency must pass `cargo deny check`: its license is on the ADR 0080
  allowlist and it comes from crates.io. The check covers the Linux targets
  Quoro ships (listed in `deny.toml`), not targets it never builds for.

## Runtime dependencies

| Crate | Used by | Features | Why |
| --- | --- | --- | --- |
| `tokio` | `crates/engine` | `io-util`, `sync`, `time` (no default features) | The engine trait is async; `io-util` gives the `AsyncRead`/`AsyncWrite` bounds the contract's console stream uses and the fake's in-memory console pipe, `sync` the fake's event and output channels, `time` the fake's injected stop delays |
| `tokio` | `crates/node-agent` | `sync`, `time`, `macros` (no default features) | The lease agent uses channels for the session and the stop flag (`sync`), the paused monotonic clock every lease timestamp is taken from (`time`), and `tokio::select!` in the agent loop (`macros`) |
| `futures-core` | `crates/engine` | no default features | `EventStream` and `LineStream` in `docs/contracts/engine-trait.md` section 2 are `futures_core::Stream` trait objects |
| `serde` | `crates/types`, `crates/control-plane` | `derive`, `std` | Ids, scopes, request and response bodies are (de)serialized as JSON |
| `getrandom` | `crates/types`, `crates/control-plane` | no default features | The random part of ULID ids, session secrets, setup tokens and password salts come straight from the OS generator; no userspace RNG crate is needed |
| `axum` | `crates/control-plane` | `http1`, `json`, `query`, `tokio` | The HTTP API server and router of D-0056 |
| `tokio` | `crates/control-plane` | `sync`, `time`, `rt`, `macros` | The single-writer gate (`sync`), timers, and the runtime axum runs on |
| `sqlx` | `crates/control-plane` | `sqlite`, `runtime-tokio` | SQLite access for the change log and state tables (D-0015); runtime queries only, no compile-time macros, so the build needs no database |
| `serde_json` | `crates/control-plane` | `std` | JSON bodies, change-log payloads and audit params; the RFC 8785 canonical form comes from `quoro-proto` (`jcs.rs`) |
| `utoipa` | `crates/control-plane` | `macros` | Handler annotations that generate the OpenAPI document diffed against the frozen `openapi.yaml` (D-0048) |
| `sha2` | `crates/control-plane` | no default features | SHA-256 for the change-log and audit hash chains and for hashing session and setup secrets |
| `argon2` | `crates/control-plane` | `alloc`, `password-hash` | Argon2id password hashes in `users.password_hash`, as `permission-model.md` and `openapi.yaml` require |
| `time` | `crates/control-plane` | `std` | UTC calendar fields for the fixed-width RFC 3339 timestamps stored and compared as strings |
| `tracing` | `crates/control-plane` | `std` | Server-side logging of internal errors and failed logins without putting them in responses |
| `chacha20poly1305` | `crates/control-plane` | `alloc`, `zeroize` (no default features) | XChaCha20-Poly1305 AEAD for the `*_enc` columns under the cluster data-encryption key (ADR 0050), starting with `providers.secret_enc` (ADR 0051); RustCrypto, pure Rust. Nonces come from `getrandom`, so its `getrandom` feature stays off |
| `zeroize` | `crates/control-plane` | `alloc` | Wipes the data-encryption key and decrypted provider secrets from memory when they are dropped |
| `tokio` | `crates/quoro` | `rt-multi-thread`, `macros`, `net`, `signal`, `sync`, `time` (no default features) | The binary's runtime: the listener, SIGTERM/Ctrl-C shutdown (`signal`), the driver channel and its loop |
| `axum` | `crates/quoro` | `http1`, `tokio` | Turns the control-plane router into the service the TLS listener serves |
| `axum-server` | `crates/quoro` | `tls-rustls-no-provider` (no default features) | The HTTPS listener of D-0054 (one TLS port, 8443) with graceful shutdown; the no-provider feature keeps aws-lc-rs out of the build (D-0080) |
| `rustls` | `crates/quoro` | `ring`, `std`, `tls12` (no default features) | TLS for that listener with the `ring` provider D-0080 allows |
| `rcgen` | `crates/quoro` | `ring`, `pem` (no default features) | Generates the self-signed primary certificate at first start (D-0054), with the same provider |
| `toml` | `crates/quoro` | `parse` (no default features) | Reads `/etc/quoro/quoro.toml` (D-0054) |
| `serde` | `crates/quoro` | `derive`, `std` | Deserializes that file into `Config` |
| `tracing` | `crates/quoro` | `std` | Structured start, driver and error logs |
| `tracing-subscriber` | `crates/quoro` | `fmt`, `std` (no default features, so no ANSI colours) | Writes those logs as plain text to stderr, which systemd forwards to journald |
| `futures-core` | `crates/quoro` | no default features | Polls the engine's `EventStream` with `std::future::poll_fn` instead of pulling in `futures` |
| `rust-embed` | `crates/quoro` | `deterministic-timestamps` (no default features) | Compiles `panel/dist` into the binary so one file serves the panel (D-0056); `allow_missing` lets a Rust-only build compile, and deterministic timestamps keep release builds reproducible (ADR 0027). It brings a second `sha2` (0.11, for its own file hashes) next to the 0.10 the control plane uses; every 8.x release does |
| `sqlx` | `crates/quoro` | `sqlite`, `runtime-tokio` | `quoro setup-token` checks for an existing owner with one query on the store's pool |
| `serde`, `serde_json` | `crates/proto` | `derive`, `std` / `std` | Every node-agent message is one JSON text frame (`node-agent-protocol.md` section 3.2); the RFC 8785 canonical form (`jcs.rs`, moved here from the control plane) is computed over `serde_json::Value` |
| `ed25519-dalek` | `crates/proto` | `std`, `fast`, `zeroize` (no default features) | The cluster signing key signs `desired_state` and `signing_key_rotate`; nodes verify against the pinned public key (section 5.11, ADR 0050). Pure Rust, BSD-3-Clause; keys come from 32-byte seeds read from the OS generator, so no `rand` feature |
| `base64` | `crates/proto`, `crates/control-plane`, `crates/node-agent` | `std` (no default features) | Public keys, signatures and node credentials are standard base64 on the wire (section 2) |
| `quoro-proto` (workspace crate) | `crates/control-plane` | none | The session's wire types, the signature helpers and the canonical JSON the change log hashes |
| `axum` | `crates/control-plane` | adds `ws` | Serves `GET /api/v1/nodes/session` as a WebSocket (section 3.1); brings `tokio-tungstenite`, the same version the node agent dials with |
| `tokio` | `crates/node-agent` | adds `net`, `rt`, `fs`, `io-util` | The dial-out TCP connection (`net`), spawned session and reconciler tasks (`rt`), the key files (`fs`) and the TLS stream (`io-util`) |
| `rustls`, `tokio-rustls` | `crates/node-agent` | `ring`, `std`, `tls12` / `ring`, `tls12` (no default features) | TLS to the primary with a verifier that accepts only the certificate whose SHA-256 matches the join command's fingerprint (section 2.2); no root store, so no `webpki-roots` |
| `tokio-tungstenite` | `crates/node-agent` | `handshake` (no default features) | The WebSocket client over that pinned TLS stream (section 3.1); its own TLS features are off because they would bring a root store instead of the pin |
| `futures-util` | `crates/node-agent` | `sink`, `std` (no default features) | `StreamExt` and `SinkExt` on the WebSocket; already in the tree through axum |
| `hyper`, `hyper-util`, `http-body-util` | `crates/node-agent` | `client`, `http1` / `tokio` / none | The one enrollment POST (section 2.3) over the same pinned TLS stream; all three are already in the tree through axum |
| `sqlx` | `crates/node-agent` | `sqlite`, `runtime-tokio` | The local node store (section 12): signed desired state, applied state and the event buffer survive agent restarts in SQLite, like the control plane's state |
| `serde`, `serde_json` | `crates/node-agent` | `derive`, `std` / `std` | Rows of that store and the node identity file |
| `sha2` | `crates/node-agent` | no default features | SHA-256 of the primary's certificate for the pin |
| `getrandom` | `crates/node-agent` | no default features | The node's Ed25519 seed, the agent `boot_id` and message ids come from the OS generator |
| `tracing` | `crates/node-agent` | `std` | Session, reconcile and redial logs |
| `quoro-node-agent` (workspace crate) | `crates/quoro` | none | `quoro join`, `quoro node` and the primary's own local node agent |
| `quoro-init` (workspace crate) | `crates/engine` (Linux) | none | The config-disk and status-record formats shared with the guest init, so the engine and the init use one copy of the encoder and decoder |
| `tokio` | `crates/engine` (Linux) | adds `rt`, `fs`, `net`, `process`, `macros` | Image conversion runs on `spawn_blocking` (`rt`); blobs stream to files (`fs`); the Cloud Hypervisor API is HTTP over a UNIX socket and VMM exits are watched through a pidfd (`net`); VMM processes are spawned and reaped (`process`); `select!` in the per-VM supervisor (`macros`) |
| `oci-client` | `crates/engine` (Linux) | `rustls-tls` only | OCI distribution protocol for `prepare_image`: anonymous token auth, manifest and index fetch, blob streaming (D-0061, validated by the M0 spike). Writing a registry client by hand is more code to review than this crate. Pulls `reqwest`, `hyper`, `rustls` with `aws-lc-rs`; every crate in that tree is under an allowlisted license (D-0080) |
| `sha2` | `crates/engine` (Linux) | no default features | The engine verifies every manifest, config, compressed layer and uncompressed layer (`diff_ids`) digest itself instead of trusting the client, and checks the pinned Cloud Hypervisor and kernel sha256 at start (F-0206). Same major as `oci-client` uses, so no duplicate |
| `tar` | `crates/engine` (Linux) | no default features | Reads OCI layer tarballs (GNU and PAX headers, long names, xattrs). Entries are applied by the engine's own in-root code, never with `tar::Archive::unpack` |
| `flate2` | `crates/engine` (Linux) | `rust_backend` only | gzip-compressed layers, the common case; pure-Rust `miniz_oxide` backend, no C zlib |
| `serde`, `serde_json` | `crates/engine` (Linux) | `std`, `derive` / `std` | OCI image config and index JSON, the cached image metadata, Cloud Hypervisor's JSON API, and the per-VM state files the engine reads back after an agent restart |
| `rustix` | `crates/engine` (Linux) | `std`, `fs`, `process` | Safe wrappers for `openat2(RESOLVE_IN_ROOT)` and the `*at` calls that apply layers inside the rootfs (engine-trait.md section 3.4), and for `pidfd_open` and `kill` in VMM supervision, so the workspace keeps `unsafe_code = "forbid"` |
| `rustix` | `crates/init` (Linux) | `std`, `fs`, `mount`, `net`, `process`, `system`, `termios` | The guest PID 1 needs mount, move-mount, chroot, reboot, rtnetlink sockets, `waitpid` on any child and `tcsetattr` (raw output on the app console), none of which `std` offers. `rustix` keeps the init free of `unsafe`; the M0 spike used raw `libc` instead |
| `reqwest` | `crates/providers` | `json`, `rustls-tls-native-roots-no-provider` (no default features) | HTTPS client for the Cloudflare API and DNS-over-HTTPS propagation checks (ADR 0025). Native roots read the host's CA store, which keeps `webpki-roots` (CDLA-Permissive-2.0, outside ADR 0080) out; no-provider leaves the crypto to `rustls` with `ring`. Redirects are disabled so the API token cannot follow one |
| `rustls` | `crates/providers`, `crates/ingress` | `ring`, `std`, `tls12` (no default features) | The `ring` provider for the provider client and for the ingress TLS listener, built with an explicit provider because the workspace also enables aws-lc-rs through `oci-client` |
| `serde`, `serde_json` | `crates/providers`, `crates/ingress` | `derive`, `std` / `std` | Cloudflare API envelopes and the tunnel ingress configuration |
| `tokio` | `crates/providers` | `time` (no default features) | Backoff between retries of idempotent API calls |
| `tracing` | `crates/providers`, `crates/ingress` | `std` | Method-and-path debug logs (never a token), certificate and connector logs |
| `zeroize` | `crates/providers` | `alloc` | Wipes API and tunnel tokens from memory when they are dropped |
| `quoro-providers` (workspace crate) | `crates/ingress` | none | `DnsProvider` for DNS-01 and the Cloudflare tunnel API |
| `tokio` | `crates/ingress` | `rt`, `net`, `sync`, `time`, `macros`, `process`, `fs`, `io-util` (no default features) | Listeners and connection tasks, the route-table swap and event channel, renewal timers, the `cloudflared` child process and its log pipes |
| `hyper` | `crates/ingress` | `http1`, `server`, `client` (no default features) | The embedded reverse proxy (ADR 0059): HTTP/1.1 server for clients, client to the VM backends, upgrades for WebSocket. Already in the tree through `reqwest` |
| `hyper-util` | `crates/ingress` | `tokio`, `server`, `server-auto`, `client-legacy`, `http1` (no default features) | Tokio I/O adapters, the timer for the header-read timeout, and the pooled backend client |
| `http-body-util`, `bytes` | `crates/ingress` | none / `std` | Streaming request and response bodies through the proxy without buffering |
| `tokio-rustls` | `crates/ingress` | `ring`, `tls12` (no default features) | TLS termination on 443 with the SNI resolver over node-local certificates |
| `instant-acme` | `crates/ingress` | `ring`, `hyper-rustls`, `time`, `x509-parser` (no default features) | ACME client for DNS-01 issuance and ARI renewal (RFC 9773), chosen by the spike for issue #65: maintained by rustls maintainers, Apache-2.0, `ring` without aws-lc-rs. Its `ring` feature also pulls `rcgen` 0.14 |
| `rcgen` | `crates/ingress` | `ring`, `pem` (no default features) | A fresh P-256 key and CSR per issuance. Same major `instant-acme` uses; `crates/quoro` still uses 0.13 for the listener certificate, a duplicate the bans check reports as a warning |
| `x509-parser` | `crates/ingress` | no default features | Reads `notBefore`/`notAfter` of stored certificates for the renewal policy; same major `instant-acme` uses |
| `time` | `crates/ingress` | `std` (no default features) | Unix timestamps and RFC 3339 rendering of certificate expiry in events |

## Test-only dependencies

| Crate | Used by | Features | Why |
| --- | --- | --- | --- |
| `tokio` | `crates/engine` | `macros`, `rt`, `test-util`, `io-util`, `sync`, `time` | `#[tokio::test]` needs the macro and a runtime; `test-util` pauses the clock so the injected stop sleeps are deterministic |
| `tokio` | `crates/node-agent` | `macros`, `rt`, `test-util`, `sync`, `time` | `#[tokio::test]` needs the macro and a current-thread runtime; `test-util` pauses the clock so lease timing and the injected stop delays are exact. The engine's `async fn` futures are not `Send`, so the tests drive the agent and the in-process primary in one task instead of spawning |
| `tokio` | `crates/control-plane` | `macros`, `rt-multi-thread`, `net` | `#[tokio::test]` and a TCP listener for the HTTP integration tests |
| `axum` | `crates/control-plane` | as above | The test harness serves the router with `axum::serve` on an ephemeral port |
| `reqwest` | `crates/control-plane` | `json`, `cookies` (no TLS) | Integration tests drive the API over real HTTP with a cookie jar, as the panel does |
| `serde_norway` | `crates/control-plane` | no default features | Parses the frozen `openapi.yaml` in the D-0048 diff test; the maintained `serde_yaml` fork that utoipa itself offers |
| `rustls` | `crates/control-plane` | `ring`, `std` | In a workspace build reqwest carries the binary's no-provider rustls feature, so the tests install the `ring` provider before building a client |
| `reqwest` | `crates/quoro` | `json`, `cookies`, `rustls-tls-manual-roots-no-provider` | The end-to-end test drives the built binary over real HTTPS with a cookie jar; manual roots because it trusts only the self-signed certificate, which also keeps `webpki-roots` (CDLA-Permissive-2.0, outside D-0080) out of the lock file |
| `rustls` | `crates/quoro` | `ring`, `std` | The provider that end-to-end test installs for reqwest |
| `serde_json` | `crates/quoro` | `std` | Reads the API's JSON bodies in that test |
| `tokio` | `crates/quoro` | `process`, `io-util`, `time` and the runtime features | The test spawns the binary, reads its stderr for the start lines and waits with timeouts |
| `tokio` | `crates/providers`, `crates/ingress` | `macros`, `rt-multi-thread`, `net`, `test-util`, `signal` | `#[tokio::test]` and local mock servers; `signal` for the lab example's Ctrl-C |
| `axum` | `crates/providers` | `http1`, `json`, `query`, `tokio` | A local mock of the Cloudflare API for the client tests |
| `rcgen` | `crates/ingress` | `ring`, `pem` | Self-signed certificates for the TLS listener tests |
| `quoro-control-plane` (workspace crate) | `crates/ingress` (example only) | none | The lab example reads a provider credential from the node's database through the in-process accessor, never over HTTP |
| `tracing-subscriber` | `crates/ingress` (example only) | `fmt`, `std` | Logs of the lab example |

## Tooling outside Cargo

Host programs the engine runs (packaged by the installer, not linked): `mkfs.ext4` from e2fsprogs >= 1.43 (`-d`) for root and data disks, `ip` from iproute2 for the bridge and TAP devices, `nft` from nftables for egress NAT and isolation, and the pinned `cloud-hypervisor-static` binary and guest kernel listed in `crates/engine/pins.txt`.

| Tool | Where | Why |
| --- | --- | --- |
| `actions/checkout`, pinned by commit SHA | `.github/workflows/ci.yml`, `.github/workflows/release.yml` | Checks out the repository in CI and in the release workflow |
| `pyyaml`, `jsonschema`, `openapi-spec-validator`, pinned versions | `.github/scripts/requirements.txt` | Validate `openapi.yaml` against OpenAPI 3.1 and `events.schema.json` against JSON Schema 2020-12 in CI (`docs/contracts/README.md`, "Validation"); `scripts/check_contracts.py` stays standard-library only |
| `cargo-deny` 0.20.2, pinned by version and sha256 | `.github/workflows/ci.yml`, `deny.toml` | Enforces the ADR 0080 license allowlist, RustSec advisories, the duplicate-version report and the crates.io-only source policy |
| `actions/attest-build-provenance` v4.2.2, pinned by commit SHA | `.github/workflows/release.yml` | Signs the release tarball keylessly: SLSA build provenance through Sigstore, bound to the release workflow and the tag (ADR 0027); no signing key to hold or leak |
| `ubuntu:24.04` container image, pinned by linux/amd64 manifest digest | `packaging/container/Containerfile` | Base of the release build image (ADR 0090): CI release builds and `packaging/reproduce-release.sh` run in the same image, so the host's compilers never reach the release bytes |
| Ubuntu packages from `snapshot.ubuntu.com` at `20261102T000000Z`, each at an exact version: `binutils`, `ca-certificates`, `curl`, `gcc`, `gcc-13`, `git`, `libc6-dev`, `musl`, `musl-dev`, `musl-tools`, `python3`, `xz-utils` | `packaging/container/Containerfile` | `musl-gcc` (GCC 13.3) compiles the C dependencies (ring, aws-lc, SQLite) of the static musl build; git, curl, python3 and xz serve the build scripts; the snapshot fixes the dependency closure. Rust code is linked with the toolchain's `rust-lld` |
| `rustup-init` 1.29.2, pinned by version and sha256 | `packaging/container/Containerfile` | Installs the Rust toolchain pinned in `rust-toolchain.toml` and the musl target into the release build image |
| Node.js, pinned by version and sha256 in `packaging/node.txt` | `packaging/fetch-node.sh`, `packaging/container/Containerfile` | Builds the web panel that `rust-embed` compiles into the `quoro` binary, so it is a release build input (ADR 0027); installed into the release build image |
| podman or docker (host) | `packaging/build-in-container.sh`, `packaging/reproduce-release.sh`, `.github/workflows/release.yml`, `.github/workflows/ci.yml` | Builds and runs the release build image; podman is preferred, the GitHub-hosted runner uses its preinstalled docker. The engine does not reach the release bytes |
| `gh` (preinstalled on the GitHub-hosted runner) | `.github/workflows/release.yml` | Self-checks the attestation with `packaging/verify-release.sh` and creates the draft release |
| GitHub CLI `gh` 2.103.3, pinned by version and sha256 | `installer/install.sh` | Downloaded to a temporary directory by the installer to verify the release attestation before anything is installed, then deleted; `cosign` was not used because its binary is ten times larger |
| `cloudflared` 2026.10.7, pinned by version and sha256 in `crates/ingress/pins.txt` | Cloudflare Tunnel backend (`crates/ingress`) | A separate Apache-2.0 process the tunnel backend runs and supervises (ADR 0024, 0055); it gets the tunnel run token only through its environment. Packaging it with the installer is installer work |
