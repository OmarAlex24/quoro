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

## Runtime dependencies

| Crate | Used by | Features | Why |
| --- | --- | --- | --- |
| `tokio` | `crates/engine` | `io-util`, `sync`, `time` (no default features) | The engine trait is async; `io-util` gives the `AsyncRead`/`AsyncWrite` bounds the contract's console stream uses and the fake's in-memory console pipe, `sync` the fake's event and output channels, `time` the fake's injected stop delays |
| `tokio` | `crates/node-agent` | `sync`, `time`, `macros` (no default features) | The lease agent uses channels for the session and the stop flag (`sync`), the paused monotonic clock every lease timestamp is taken from (`time`), and `tokio::select!` in the agent loop (`macros`) |
| `futures-core` | `crates/engine` | no default features | `EventStream` and `LineStream` in `docs/contracts/engine-trait.md` section 2 are `futures_core::Stream` trait objects |

## Test-only dependencies

| Crate | Used by | Features | Why |
| --- | --- | --- | --- |
| `tokio` | `crates/engine` | `macros`, `rt`, `test-util`, `io-util`, `sync`, `time` | `#[tokio::test]` needs the macro and a runtime; `test-util` pauses the clock so the injected stop sleeps are deterministic |
| `tokio` | `crates/node-agent` | `macros`, `rt`, `test-util`, `sync`, `time` | `#[tokio::test]` needs the macro and a current-thread runtime; `test-util` pauses the clock so lease timing and the injected stop delays are exact. The engine's `async fn` futures are not `Send`, so the tests drive the agent and the in-process primary in one task instead of spawning |

## Tooling outside Cargo

| Tool | Where | Why |
| --- | --- | --- |
| `actions/checkout`, pinned by commit SHA | `.github/workflows/ci.yml` | Checks out the repository in CI; the only third-party action |
| `pyyaml`, `jsonschema`, `openapi-spec-validator`, pinned versions | `.github/scripts/requirements.txt` | Validate `openapi.yaml` against OpenAPI 3.1 and `events.schema.json` against JSON Schema 2020-12 in CI (`docs/contracts/README.md`, "Validation"); `scripts/check_contracts.py` stays standard-library only |
