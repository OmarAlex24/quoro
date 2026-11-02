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
  allowlist and it comes from crates.io.

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
| `serde_json` | `crates/control-plane` | `std` | JSON bodies, change-log payloads and audit params; the RFC 8785 canonical form is computed in-crate (`jcs.rs`) over `serde_json::Value` |
| `utoipa` | `crates/control-plane` | `macros` | Handler annotations that generate the OpenAPI document diffed against the frozen `openapi.yaml` (D-0048) |
| `sha2` | `crates/control-plane` | no default features | SHA-256 for the change-log and audit hash chains and for hashing session and setup secrets |
| `argon2` | `crates/control-plane` | `alloc`, `password-hash` | Argon2id password hashes in `users.password_hash`, as `permission-model.md` and `openapi.yaml` require |
| `time` | `crates/control-plane` | `std` | UTC calendar fields for the fixed-width RFC 3339 timestamps stored and compared as strings |
| `tracing` | `crates/control-plane` | `std` | Server-side logging of internal errors and failed logins without putting them in responses |
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
| `quoro-init` (workspace crate) | `crates/engine` (Linux) | none | The config-disk and status-record formats shared with the guest init, so the engine and the init use one copy of the encoder and decoder |
| `tokio` | `crates/engine` (Linux) | adds `rt`, `fs`, `net`, `process`, `macros` | Image conversion runs on `spawn_blocking` (`rt`); blobs stream to files (`fs`); the Cloud Hypervisor API is HTTP over a UNIX socket and VMM exits are watched through a pidfd (`net`); VMM processes are spawned and reaped (`process`); `select!` in the per-VM supervisor (`macros`) |
| `oci-client` | `crates/engine` (Linux) | `rustls-tls` only | OCI distribution protocol for `prepare_image`: anonymous token auth, manifest and index fetch, blob streaming (D-0061, validated by the M0 spike). Writing a registry client by hand is more code to review than this crate. Pulls `reqwest`, `hyper`, `rustls` with `aws-lc-rs`; every crate in that tree is under an allowlisted license (D-0080) |
| `sha2` | `crates/engine` (Linux) | no default features | The engine verifies every manifest, config, compressed layer and uncompressed layer (`diff_ids`) digest itself instead of trusting the client, and checks the pinned Cloud Hypervisor and kernel sha256 at start (F-0206). Same major as `oci-client` uses, so no duplicate |
| `tar` | `crates/engine` (Linux) | no default features | Reads OCI layer tarballs (GNU and PAX headers, long names, xattrs). Entries are applied by the engine's own in-root code, never with `tar::Archive::unpack` |
| `flate2` | `crates/engine` (Linux) | `rust_backend` only | gzip-compressed layers, the common case; pure-Rust `miniz_oxide` backend, no C zlib |
| `serde`, `serde_json` | `crates/engine` (Linux) | `std`, `derive` / `std` | OCI image config and index JSON, the cached image metadata, Cloud Hypervisor's JSON API, and the per-VM state files the engine reads back after an agent restart |
| `rustix` | `crates/engine` (Linux) | `std`, `fs`, `process` | Safe wrappers for `openat2(RESOLVE_IN_ROOT)` and the `*at` calls that apply layers inside the rootfs (engine-trait.md section 3.4), and for `pidfd_open` and `kill` in VMM supervision, so the workspace keeps `unsafe_code = "forbid"` |
| `rustix` | `crates/init` (Linux) | `std`, `fs`, `mount`, `net`, `process`, `system`, `termios` | The guest PID 1 needs mount, move-mount, chroot, reboot, rtnetlink sockets, `waitpid` on any child and `tcsetattr` (raw output on the app console), none of which `std` offers. `rustix` keeps the init free of `unsafe`; the M0 spike used raw `libc` instead |

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

## Tooling outside Cargo

Host programs the engine runs (packaged by the installer, not linked): `mkfs.ext4` from e2fsprogs >= 1.43 (`-d`) for root and data disks, `ip` from iproute2 for the bridge and TAP devices, `nft` from nftables for egress NAT and isolation, and the pinned `cloud-hypervisor-static` binary and guest kernel listed in `crates/engine/pins.txt`.

| Tool | Where | Why |
| --- | --- | --- |
| `actions/checkout`, pinned by commit SHA | `.github/workflows/ci.yml`, `.github/workflows/release.yml` | Checks out the repository in CI and in the release workflow |
| `pyyaml`, `jsonschema`, `openapi-spec-validator`, pinned versions | `.github/scripts/requirements.txt` | Validate `openapi.yaml` against OpenAPI 3.1 and `events.schema.json` against JSON Schema 2020-12 in CI (`docs/contracts/README.md`, "Validation"); `scripts/check_contracts.py` stays standard-library only |
| `cargo-deny` 0.20.2, pinned by version and sha256 | `.github/workflows/ci.yml`, `deny.toml` | Enforces the ADR 0080 license allowlist, RustSec advisories, the duplicate-version report and the crates.io-only source policy |
| `actions/attest-build-provenance` v4.2.2, pinned by commit SHA | `.github/workflows/release.yml` | Signs the release tarball keylessly: SLSA build provenance through Sigstore, bound to the release workflow and the tag (ADR 0027); no signing key to hold or leak |
| `musl-tools` (Ubuntu package, not pinned) | `.github/workflows/release.yml` | `musl-gcc` for C dependencies of the static musl build; Rust code is linked with the toolchain's `rust-lld`, so the host linker stays out of the build |
| `gh` (preinstalled on the GitHub-hosted runner) | `.github/workflows/release.yml` | Self-checks the attestation with `packaging/verify-release.sh` and creates the draft release |
| GitHub CLI `gh` 2.103.3, pinned by version and sha256 | `installer/install.sh` | Downloaded to a temporary directory by the installer to verify the release attestation before anything is installed, then deleted; `cosign` was not used because its binary is ten times larger |
