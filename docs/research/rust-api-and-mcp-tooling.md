# rmcp maturity, Litestream replication to R2, and a utoipa -> TypeScript client pipeline

- **Question:** Q-0014 VERIFY: rmcp maturity; Litestream replication to R2; utoipa + orval/openapi-ts pipeline
- **Researched:** 2026-11-02 by agent:ds-research-4
- **Status:** answered

## Short answer

rmcp is the official SDK and is past 1.0 (3.8.0, 2026-10-26), but it has shipped three semver-major versions in five months (1.0 in March, 2.0 in June, 3.0 in July 2026), so pinning an exact minor version and budgeting for upgrades is mandatory. It implements MCP `2026-07-28` by default and still understands `2025-03-26` through `2025-11-25`; it ships stdio and Streamable HTTP (server as a Tower service mountable on Axum), and deliberately does **not** ship the legacy 2024-11-05 HTTP+SSE transport. OAuth 2.1/PKCE support is documented for the client side; no server-side auth middleware is documented.

Litestream is still 0.5.x/beta (0.5.17, 2026-08-31), actively maintained, Apache-2.0, and has an official Cloudflare R2 section in its S3-compatible guide. Replication is asynchronous with a 1s default sync (RPO up to ~1s), point-in-time restore via TXID or timestamp, and one Litestream process per database. The 0.5 LTX rewrite removed client-side (age) encryption; only S3 SSE-C/SSE-KMS remain — a direct conflict with Quoro's encrypted-backup expectation that must be resolved.

utoipa 6.0.0 emits OpenAPI 3.1.0 by default (3.2 opt-in) and is Apache-2.0, but is effectively single-maintainer (646 of the top commits) with 256 open issues and only two releases in 12 months. orval 8.46.0 (MIT) and @hey-api/openapi-ts 0.99.0 (MIT) both consume 3.1 and both generate TanStack Query hooks, Zod schemas and fetch clients; openapi-ts is still pre-1.0 with documented breaking changes per minor. One confirmed utoipa->orval break exists today: utoipa 6 omits empty response `description`, which makes orval reject the 3.1.0 document (utoipa issue #1636, fix open in #1649).

## Findings

### rmcp (official Rust MCP SDK)

**Version and cadence**

- Latest published version is **3.8.0, released 2026-10-26**; the crate has 71 published versions since 2025-03-16. **Confirmed.** Source: https://crates.io/api/v1/crates/rmcp (accessed 2026-11-02)
- rmcp is **past 1.0**: 1.0.0 was published **2026-03-03**, 2.0.0 on **2026-06-29**, 3.0.0 on **2026-07-28**. **Confirmed.** Source: https://crates.io/api/v1/crates/rmcp (accessed 2026-11-02)
- **47 crate versions** were published in the last 12 months (created_at after 2025-11-02), roughly four per month. Counting per-crate tags (rmcp + rmcp-macros) the GitHub releases feed shows **94 releases** in the same window. **Confirmed.** Sources: https://crates.io/api/v1/crates/rmcp and https://api.github.com/repos/modelcontextprotocol/rust-sdk/releases (accessed 2026-11-02)
- Three semver-major bumps in five months. 2.0.0 is explicitly breaking (`[**breaking**] align model types with MCP 2025-11-25 spec`, `add Audio variant to PromptMessageContent`) with migration guide discussion #926. 3.0.0 carries a long "Breaking changes" section (sessionless Streamable HTTP, removal of `initialize`, MRTR result enums, `stateful_mode` rename, MSRV 1.88) and migration guide discussion #969. **Confirmed.** Sources: https://api.github.com/repos/modelcontextprotocol/rust-sdk/releases/tags/rmcp-v2.0.0, https://api.github.com/repos/modelcontextprotocol/rust-sdk/releases/tags/rmcp-v3.0.0, https://crates.io/api/v1/crates/rmcp (accessed 2026-11-02)
- The in-repo changelog only goes back to 3.x, so the number of breaking changes before 3.0 cannot be counted from it; the 2.0/3.0 release bodies are the reliable record. **Confirmed** (about the changelog's coverage). Source: https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/crates/rmcp/CHANGELOG.md (accessed 2026-11-02)

**Status, licence, spec revisions**

- It is the official SDK: repository lives under the `modelcontextprotocol` GitHub org and the README calls it "An official Rust Model Context Protocol SDK implementation with tokio async runtime". **Confirmed.** Source: https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/README.md (accessed 2026-11-02)
- Licence: the LICENSE file documents a **transition from MIT to Apache-2.0**; contributions whose authors have not consented remain MIT. GitHub reports the licence as "Other"/NOASSERTION, so downstream users must read the LICENSE rather than trusting an SPDX tag. **Confirmed.** Sources: https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/LICENSE and https://api.github.com/repos/modelcontextprotocol/rust-sdk (accessed 2026-11-02)
- Spec revisions understood: the `ProtocolVersion` constants in `crates/rmcp/src/model.rs` are `2024-11-05`, `2025-03-26`, `2025-06-18`, `2025-11-25`, `2026-07-28`, with `LATEST = V_2026_07_28` and `KNOWN_VERSIONS` listing all five. **Confirmed.** Source: https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/crates/rmcp/src/model.rs (accessed 2026-11-02)
- The README states it "implements the stable MCP `2026-07-28` specification while remaining fully compatible with the `2025-11-25` release and earlier versions". **Confirmed.** Source: https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/README.md (accessed 2026-11-02)

**Transports**

- Supported transports, per the README table: stdio (`transport-io`), client child process (`transport-child-process`), Streamable HTTP server (`transport-streamable-http-server`), Streamable HTTP client (`transport-streamable-http-client-reqwest`), and worker/in-process (`transport-worker`). **Confirmed.** Source: https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/README.md (accessed 2026-11-02)
- Streamable HTTP server is "a Tower service you can mount on any `axum`/`hyper` router"; `rmcp::transport::StreamableHttpService` is the type. This is the direct Axum integration path for Quoro. **Confirmed.** Source: https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/README.md (accessed 2026-11-02)
- SSE as a standalone transport is **not** provided. SSE framing is an implementation detail of Streamable HTTP (`client-side-sse` feature). The legacy two-endpoint HTTP+SSE transport of revision `2024-11-05` is a "deliberate non-goal" and "rmcp will not add the legacy transport back". **Confirmed.** Source: https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/README.md (accessed 2026-11-02)

**Auth and capability surface**

- OAuth 2.1 support with PKCE (S256), RFC 8707 resource binding, RFC 9728 protected-resource metadata, RFC 8414 + OIDC AS metadata, RFC 7591 dynamic client registration, CIMD, scope upgrade on 403, and automatic token refresh. Enabled by the `auth` cargo feature. **Confirmed.** Source: https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/docs/OAUTH_SUPPORT.md (accessed 2026-11-02)
- The OAuth document describes the **client** side (obtaining and refreshing tokens, then calling an MCP server). It documents no server-side authorization middleware for a Rust MCP server. Whether a server can rely on rmcp for inbound token validation is therefore unclear. **Hypothesis** (absence of documentation is not proof of absence). Source: https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/docs/OAUTH_SUPPORT.md (accessed 2026-11-02)
- Capabilities advertised in the README table of contents and examples: tools, resources (incl. subscriptions), prompts, sampling, elicitation, roots, logging, completions, notifications, tasks (long-running tool invocations), response caching. **Confirmed.** Source: https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/README.md (accessed 2026-11-02)
- Tool macros exist: `#[tool]`, `#[tool_router]`, `#[tool_handler]`, plus `#[tool_router(server_handler)]` for tools-only servers; implemented in the `rmcp-macros` crate. JSON Schema is generated with `schemars` (draft 2020-12). **Confirmed.** Source: https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/README.md (accessed 2026-11-02)

**Maturity signals**

| Signal | Value | Label | Source |
|---|---|---|---|
| GitHub stars | 4,085 | Confirmed | https://api.github.com/repos/modelcontextprotocol/rust-sdk (accessed 2026-11-02) |
| Open issues (GitHub count, includes PRs) | 70 | Confirmed | same |
| Forks | 694 | Confirmed | same |
| Contributors | 223 | Confirmed | https://api.github.com/repos/modelcontextprotocol/rust-sdk/contributors?per_page=1 (accessed 2026-11-02) |
| crates.io downloads (total) | 43,612,793 | Confirmed | https://crates.io/api/v1/crates/rmcp (accessed 2026-11-02) |
| crates.io downloads (recent 90d) | 24,877,320 | Confirmed | same |
| First release | 2025-03-16 | Confirmed | same |
| Last commit | 2026-11-01 | Confirmed | https://api.github.com/repos/modelcontextprotocol/rust-sdk (accessed 2026-11-02) |

- Notable users listed by the project: Block's `goose`, `apollo-mcp-server` (Apollo GraphQL), `rustfs-mcp`, `containerd-mcp-server`, `nvim-mcp`, `stakpak-agent`, `hyper-mcp`. **Confirmed** (as project self-reporting). Source: https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/README.md (accessed 2026-11-02)
- Maturity judgement: downloads and named users indicate real production adoption, but the release history shows the API is still tracking a fast-moving spec with breaking releases every few months. **Hypothesis** (inference from download counts, release cadence and changelog, not a stated fact). Sources: as above (accessed 2026-11-02)

### Litestream (SQLite replication)

**Version, licence, maintenance**

- Latest release is **v0.5.17, published 2026-08-31**. The project is still 0.x and its README carries a "status-beta" badge. **Confirmed.** Sources: https://api.github.com/repos/benbjohnson/litestream/releases and https://raw.githubusercontent.com/benbjohnson/litestream/main/README.md (accessed 2026-11-02)
- Licence is **Apache-2.0**. **Confirmed.** Source: https://api.github.com/repos/benbjohnson/litestream (accessed 2026-11-02)
- Maintained by Ben Johnson (repo owner); the README thanks Fly.io for support and testing credits. Recent commits: 2026-10-31 (`fix(s3): add opt-in PutObject checksums`), 2026-10-30 (`fix(s3): retry throttling with bounded backoff`). 43 contributors, 14,564 stars, 149 open issues, not archived. **Confirmed.** Sources: https://api.github.com/repos/benbjohnson/litestream/commits and https://api.github.com/repos/benbjohnson/litestream (accessed 2026-11-02)
- CONTRIBUTING says large external feature contributions are "generally not accepted" and major features are implemented internally. **Confirmed.** Source: https://raw.githubusercontent.com/benbjohnson/litestream/main/CONTRIBUTING.md (accessed 2026-11-02)
- Release cadence is irregular: multiple 0.5.x patches through 2026 (0.5.9 Feb, 0.5.10 Mar, 0.5.11 Apr, 0.5.12 Jun, 0.5.13–0.5.14 Jul, 0.5.15 Jul, 0.5.16 Aug, 0.5.17 Aug), then no release for ~2 months while commits continued. **Confirmed.** Source: https://api.github.com/repos/benbjohnson/litestream/releases?per_page=10 (accessed 2026-11-02)

**Big recent changes (0.5.x)**

- v0.5.0 (2025-09-30) replaced the storage layer with **LTX** (`Replace storage layer with LTX #645`), removed generations (#636), **removed multiple-replica support** (#647), added multi-level compaction (#650) and multi-level restore (#652), added a **VFS MVP** (#721) and an **MCP server integration** (#655), and renamed the `wal` command to `ltx` (#671). **Confirmed.** Source: https://api.github.com/repos/benbjohnson/litestream/releases/tags/v0.5.0 (accessed 2026-11-02)
- Read replicas were **not** permanently removed: they return as an optional **VFS extension**, which is read-only, requires CGO, and ships as pre-built binaries or a `-tags vfs` build. **Confirmed.** Sources: https://litestream.io/guides/vfs/ and https://litestream.io/how-it-works/ (accessed 2026-11-02)
- **Client-side (age) encryption was removed** in the LTX rewrite. Since v0.5.1 a non-empty `age:` block makes Litestream exit with `age encryption is not currently supported`; v0.5.0 silently ignored it. Server-side S3 encryption via **SSE-C / SSE-KMS** was added in PR #902 (merged 2025-12-30) and documented as v0.5.6. **Confirmed.** Sources: https://litestream.io/reference/config/ and https://api.github.com/repos/benbjohnson/litestream/pulls/902 (accessed 2026-11-02)

**Cloudflare R2 support**

- R2 has an official section in the "Replicating to S3-Compatible Services" guide. Minimal config: `type: s3`, `bucket`, `path`, `endpoint: ${CF_ACCOUNT_ID}.r2.cloudflarestorage.com`, `region: auto` (or `us-east-1` for compatibility), `access-key-id`, `secret-access-key`. R2 supports both path-style and virtual-hosted addressing, so `force-path-style` is not required. **Confirmed.** Source: https://litestream.io/guides/s3-compatible/ (accessed 2026-11-02)
- Since v0.5.0 the provider is auto-detected from the endpoint and provider defaults applied; for R2 the docs note (v0.5.8) that Litestream "automatically sets concurrency=2 for R2 endpoints to avoid R2's strict concurrent upload limits". A YAML-path detection gap for R2 was fixed in #1098 (merged 2026-02-11). **Confirmed.** Sources: https://litestream.io/guides/s3-compatible/ and https://api.github.com/repos/benbjohnson/litestream/issues/1098 (accessed 2026-11-02)
- Known R2-specific issue: HTTP 429 `Reduce your request rate` was not retried by the S3 client; fixed by `fix(s3): retry throttling with bounded backoff` (#1609), closed 2026-10-30, i.e. not in the 0.5.17 release from 2026-08-31. **Confirmed.** Source: https://api.github.com/repos/benbjohnson/litestream/issues/1603 (accessed 2026-11-02)
- Earlier R2 upload failures were caused by AWS SDK v2 `aws-chunked` content encoding; fixed in v0.5.4 for all custom endpoints (`InvalidContentEncoding`, `MalformedTrailerError`). **Confirmed.** Sources: https://litestream.io/guides/s3-compatible/ and https://api.github.com/repos/benbjohnson/litestream/issues/948 (accessed 2026-11-02)
- Open (not R2-specific but relevant to cost/rate): #1171 "High PUT volume despite zero SQLite writes and long sync-interval" (2026-02-27) and #1387 VFS page-fetch amplification. **Confirmed** (as open issues). Source: https://api.github.com/search/issues?q=repo:benbjohnson/litestream+R2 (accessed 2026-11-02)

**Replication semantics**

- Replication is **asynchronous**: "By default, Litestream will replicate new changes to an S3 replica every second", and a crash can lose the writes in that window. Synchronous replication is on the roadmap but not implemented. **Confirmed.** Source: https://litestream.io/tips/ (accessed 2026-11-02)
- **RPO** is therefore the sync interval (default 1s, configurable via `sync-interval`); graceful shutdown attempts a final sync of outstanding WAL changes. **Confirmed.** Sources: https://litestream.io/tips/ and https://litestream.io/reference/config/ (accessed 2026-11-02)
- **WAL mode is required**; Litestream sets it automatically if absent, and recommends `PRAGMA busy_timeout = 5000`, and `PRAGMA wal_autocheckpoint = 0` under high write load. **Confirmed.** Source: https://litestream.io/tips/ (accessed 2026-11-02)
- Litestream takes over checkpointing by holding a long-running read transaction; it creates a `_litestream_lock` table in the source database, which changes the schema and page count (restored DBs also contain it). **Confirmed.** Source: https://raw.githubusercontent.com/benbjohnson/litestream/main/README.md (accessed 2026-11-02)
- **Restore**: `litestream restore` fetches the newest snapshot not overshooting the target and replays LTX files in TXID order. Targets are `-txid` (inclusive) or `-timestamp` (exclusive), i.e. **point-in-time restore**. Restore granularity is bounded by LTX file boundaries (roughly one per sync while L0 is retained). **Confirmed.** Source: https://litestream.io/how-it-works/ (accessed 2026-11-02)
- **Single writer**: "Ensure only one process writes to the database at a time" and "only one Litestream instance should replicate a given database". Multiple replicators into the same bucket/path can make restore impossible. **Confirmed.** Source: https://litestream.io/guides/docker/ (accessed 2026-11-02)
- **Process or library**: production use is the CLI sidecar; the Go library exists but its API "is not considered stable", requires `modernc.org/sqlite`, and is explicitly not recommended where API stability matters. There is no Rust embedding path. **Confirmed.** Source: https://litestream.io/guides/go-library/ (accessed 2026-11-02)

**Relevance to D-0023 / D-0015**

- D-0023 already names Litestream as the SQLite exception in the catalog ("databases use their native replication orchestrated by Quoro ... Litestream for SQLite"), and D-0015 notes Litestream as a candidate for backing up the control plane. Both are consistent with what Litestream actually does, with two caveats: it is a **sidecar process per database**, not an embedded library, and its 0.5.x backups are **not client-side encrypted** unless the target supports SSE-C/SSE-KMS. **Confirmed** for the mechanics; caveat relevance is a judgement. Sources: as above (accessed 2026-11-02)

### OpenAPI pipeline: utoipa -> TypeScript

**utoipa**

- Latest is **6.0.0, released 2026-09-22**; Apache-2.0; 59.4M downloads. **Confirmed.** Sources: https://crates.io/api/v1/crates/utoipa and https://api.github.com/repos/juhaku/utoipa (accessed 2026-11-02)
- Emits **OpenAPI 3.1** ("Features: OpenAPI 3.1"). 6.0.0 added a `Version32` type for OpenAPI 3.2, but the default document still reports `openapi: 3.1.0` (confirmed by open issue #1636). **Confirmed.** Sources: https://raw.githubusercontent.com/juhaku/utoipa/master/README.md, https://raw.githubusercontent.com/juhaku/utoipa/master/utoipa/CHANGELOG.md, https://api.github.com/repos/juhaku/utoipa/issues/1636 (accessed 2026-11-02)
- Axum integration: `utoipa-axum` **0.3.0** (2026-09-22), which depends on `axum ^0.8.4` and `utoipa ^6.0.0`; the `axum_extras` feature parses path/query params and request/response bodies. Axum's latest is 0.8.9, so versions line up. **Confirmed.** Sources: https://crates.io/api/v1/crates/utoipa-axum/0.3.0/dependencies and https://crates.io/api/v1/crates/axum (accessed 2026-11-02)
- Maintenance: last commit 2026-10-20; two releases in the last 12 months (5.5.0 on 2026-05-04 and 6.0.0 on 2026-09-22); 4,141 stars, 256 open issues. Contributor concentration is extreme: `juhaku` 646 commits, next contributor 29. No "seeking maintainers"/"bus factor" issue was found. **Confirmed** for the numbers; **Hypothesis** that the project is a single-maintainer bus-factor risk. Sources: https://api.github.com/repos/juhaku/utoipa, https://api.github.com/repos/juhaku/utoipa/contributors?per_page=3, https://crates.io/api/v1/crates/utoipa (accessed 2026-11-02)
- Alternatives, one line each: **aide 0.15.1** stable (0.16.0-alpha.4 is the newest published, 2026-04-14), Apache/MIT, Axum-native OpenAPI generation. **poem-openapi 5.1.16**, last released 2025-07-28 (~15 months old), tied to the poem framework. **Confirmed.** Sources: https://crates.io/api/v1/crates/aide and https://crates.io/api/v1/crates/poem-openapi (accessed 2026-11-02)

**orval**

- Latest **8.46.0, published 2026-10-30**, MIT, Node >= 22.18, 69 stable releases in the last 12 months, 3.62M downloads/week, 6,582 stars, 25 open issues. 8.0.0 was a major release on 2026-01-14. **Confirmed.** Sources: https://registry.npmjs.org/orval, https://api.npmjs.org/downloads/point/last-week/orval, https://api.github.com/repos/orval-labs/orval (accessed 2026-11-02)
- OpenAPI 3.1: Orval internally upgrades input to OpenAPI 3.1 ("before the internal OpenAPI 2.0 → 3.1 upgrade"), and its test fixtures were rewritten to OAS 3.1 in PRs #4149/#4153 (2026-09-18/19), with downstream OAS 3.0 handling removed (#4155). **Confirmed.** Sources: https://orval.dev/docs/reference/configuration/input and https://api.github.com/search/issues?q=repo:orval-labs/orval+%22OpenAPI+3.1%22 (accessed 2026-11-02)
- Generates React Query (TanStack Query) hooks, Zod schemas, native fetch clients, Axios, SWR, Vue/Svelte/Solid/Angular query, MSW mocks, and has an MCP-server target. **Confirmed.** Source: https://raw.githubusercontent.com/orval-labs/orval/master/README.md (accessed 2026-11-02)

**@hey-api/openapi-ts**

- Latest stable **0.99.0, published 2026-06-22** (dist-tag `latest`); `next` is a 2026-09-30 prerelease. MIT, Node >= 22.18, 50 stable releases in the last 12 months, 748 stable versions total, 6.88M downloads/week, 5,551 stars, 659 open issues. **Confirmed.** Sources: https://registry.npmjs.org/@hey-api%2fopenapi-ts, https://api.npmjs.org/downloads/point/last-week/@hey-api/openapi-ts, https://api.github.com/repos/hey-api/hey-api (accessed 2026-11-02)
- OpenAPI 3.1: docs state "Hey API supports all valid OpenAPI versions and file formats", and the config example accepts an inline `openapi: '3.1.1'` document. **Confirmed.** Source: https://heyapi.dev/docs/openapi/typescript/configuration/input (accessed 2026-11-02)
- Ships TanStack Query, Zod, Valibot, Fetch, Axios, Ky and other plugins. **Confirmed.** Source: https://heyapi.dev/docs/openapi/typescript/get-started (accessed 2026-11-02)
- Stability: pre-1.0 and explicit that "that pace sometimes comes with breaking changes", documented per minor version (e.g. v0.99.0 plugin dedupe and `plugin.symbols` -> `plugin.imports`, v0.96.0 dropped Node 20, v0.91.0 dropped CJS). **Confirmed.** Source: https://heyapi.dev/docs/openapi/typescript/migrating (accessed 2026-11-02)

**Known utoipa output vs generator incompatibilities**

- **Confirmed break, orval:** utoipa 6.0.0 stopped serialising empty response `description` while still declaring `openapi: 3.1.0`; OpenAPI 3.1 requires `description` on a Response Object, and "strict clients such as orval reject it". The fix is open as PR #1649 ("Always serialize the description of a response"). Until merged, the workaround is a doc comment/`description` on every response or pinning utoipa 5.x. **Confirmed.** Sources: https://api.github.com/repos/juhaku/utoipa/issues/1636 and https://api.github.com/repos/juhaku/utoipa/issues/1649 (accessed 2026-11-02)
- **Nullable `Option<T>` shape:** utoipa emits `Option<T>` as `oneOf: [null, T]` (or a `type: [T, "null"]` array) rather than a `nullable` flag; tracked in open issue #1260, which reports downstream generators choking on the `null` branch. utoipa-gen 6.x has partial mitigations ("emit title/default instead of oneOf when Option is non-nullable", "avoid degenerate oneOf"). **Confirmed** for the emitted shape and issue state; **Hypothesis** that orval/openapi-ts mishandle it today (no direct repro found). Sources: https://api.github.com/repos/juhaku/utoipa/issues/1260 and https://raw.githubusercontent.com/juhaku/utoipa/master/utoipa-gen/CHANGELOG.md (accessed 2026-11-02)
- **orval 3.1 null-union bugs** (independent of utoipa): `aliasCombinedTypes: true` produced duplicate self-referencing types for `type: [object, 'null']` (#4291, closed 2026-10-12), duplicate nullable component aliases (#4292, closed), and nullable `date-time` query params written as `anyOf` were sent as `Date.toString()` (#4417, fixed 2026-11-01). **Confirmed.** Source: https://api.github.com/search/issues?q=repo:orval-labs/orval+%22OpenAPI+3.1%22 (accessed 2026-11-02)
- **openapi-ts + `allOf` composition:** open issue #4470 (2026-10-13) reports the TanStack Query plugin skipping infinite-query options when pagination arrives through `allOf` — relevant because utoipa can emit `allOf` for flattened/composed schemas, but the issue is not utoipa-specific. **Confirmed** as an open issue; **Hypothesis** that it affects utoipa-generated specs. Source: https://api.github.com/search/issues?q=utoipa+repo:hey-api/hey-api (accessed 2026-11-02)

**orval vs openapi-ts (sourced facts)**

| Fact | orval | @hey-api/openapi-ts |
|---|---|---|
| Version / date | 8.46.0 / 2026-10-30 **Confirmed** [6] | 0.99.0 / 2026-06-22 **Confirmed** [7] |
| Licence | MIT **Confirmed** [6] | MIT **Confirmed** [8] |
| OpenAPI 3.1 | Yes, input upgraded to 3.1 internally **Confirmed** [9] | Yes, "all valid OpenAPI versions" **Confirmed** [10] |
| TanStack Query hooks | React/Vue/Svelte/Solid/Angular Query targets **Confirmed** [11] | TanStack Query plugin **Confirmed** [12] |
| Zod | `@orval/zod` client **Confirmed** [11] | Zod validator plugin **Confirmed** [12] |
| Fetch client | Native fetch client **Confirmed** [11] | Fetch API client **Confirmed** [12] |
| Node requirement | >= 22.18 **Confirmed** [6] | >= 22.18 **Confirmed** [8] |
| Stable releases, last 12 mo | 69 **Confirmed** [6] | 50 **Confirmed** [7] |
| npm downloads/week | 3.62M **Confirmed** [13] | 6.88M **Confirmed** [14] |
| Open GitHub issues | 25 **Confirmed** [15] | 659 **Confirmed** [16] |
| Semver status | 8.x, major bumps rare (8.0.0 2026-01-14) **Confirmed** [6] | pre-1.0, breaking changes per minor **Confirmed** [17] |
| Last release age | 3 days | ~4.5 months (stable channel) |

## Versions checked

| Component | Version | Release date | Source |
|---|---|---|---|
| rmcp | 3.8.0 | 2026-10-26 | https://crates.io/api/v1/crates/rmcp |
| utoipa | 6.0.0 | 2026-09-22 | https://crates.io/api/v1/crates/utoipa |
| utoipa-axum | 0.3.0 | 2026-09-22 | https://crates.io/api/v1/crates/utoipa-axum |
| aide | 0.15.1 (0.16.0-alpha.4 latest) | 2026-04-14 (alpha) | https://crates.io/api/v1/crates/aide |
| poem-openapi | 5.1.16 | 2025-07-28 | https://crates.io/api/v1/crates/poem-openapi |
| axum | 0.8.9 | 2026-04-14 | https://crates.io/api/v1/crates/axum |
| Litestream | 0.5.17 | 2026-08-31 | https://api.github.com/repos/benbjohnson/litestream/releases |
| orval | 8.46.0 | 2026-10-30 | https://registry.npmjs.org/orval |
| @hey-api/openapi-ts | 0.99.0 | 2026-06-22 | https://registry.npmjs.org/@hey-api%2fopenapi-ts |

## Implications for Quoro

- **D-0028 (MCP: rmcp).** The choice is sound on capability: official org, 1.0+, Axum-mountable Streamable HTTP, OAuth 2.1 client, all five spec revisions including `2025-11-25` and `2026-07-28`. The cost is churn: three major versions in five months. Quoro should pin `rmcp` to an exact minor (not a caret range), keep the MCP surface behind a thin internal trait, and treat spec upgrades as scheduled work. Because rmcp deliberately omits the legacy 2024-11-05 HTTP+SSE transport, any agent client Quoro must support that only speaks that revision cannot connect directly — Streamable HTTP is the only HTTP option.
- **D-0028/Axum.** `StreamableHttpService` being a Tower service means MCP can share the same Axum router, ports, tracing and middleware as the REST API. That is a real reduction in surface, consistent with "minimal dependencies".
- **D-0028 (OpenAPI via utoipa).** utoipa 6 is the only mature code-first option for Axum (`utoipa-axum` tracks axum 0.8.x, which is current). The risk is maintenance concentration (one contributor, 256 open issues, two releases a year) and the confirmed orval break on missing response `description`. Quoro can mitigate today by requiring a `description` on every response in the API contract and by pinning utoipa exactly.
- **D-0028 (orval vs openapi-ts).** Both satisfy the stated need (TanStack Query + Zod + fetch). orval is 8.x, low open-issue count, released days ago, and normalises to 3.1; openapi-ts has more downloads but is pre-1.0 with documented per-minor breaking changes and 659 open issues. Evidence favours orval on stability, openapi-ts on plugin breadth and download share. No decision is implied here.
- **D-0023 / D-0015 (Litestream).** The mechanics match the decisions: async WAL shipping, ~1s RPO, PITR by timestamp or TXID, one sidecar per database, official R2 target. But two premises need correcting: (a) Litestream is a **separate process**, not something Quoro can embed in a Rust binary, so "orchestrated by Quoro" means supervising a sidecar and its config; (b) in 0.5.x it provides **no client-side encryption** — age was removed and is hard-rejected. D-0023's whole volume path is "content-addressed chunks (dedup, encrypted)", so shipping SQLite backups in plaintext to R2 would be inconsistent with that decision. Options are SSE-C/SSE-KMS where the provider supports it, Quoro-side encryption of the SQLite dump/file, or using Quoro's own chunk engine for SQLite too. That is a decision for the maintainer, not this research.
- **D-0023 / D-0025 (R2).** R2 is a first-class documented Litestream target with `region: auto` and no force-path-style requirement, and Litestream already throttles concurrency to 2 for R2. The 429 retry fix landed after 0.5.17, so Quoro should not ship 0.5.17 as-is for R2 under load without testing, or should build from a commit including #1609.
- **D-0015 (control-plane backup).** Litestream can back up the control-plane SQLite file, but it adds a `_litestream_lock` table to the source schema and requires WAL mode plus a single writer. If Quoro's state store is an append-only change log with its own chunk replication, Litestream may be redundant for the control plane; the evidence does not decide this.

## Open risks and unknowns

- rmcp 3.x moves with the MCP spec; `2026-07-28` removes protocol sessions and `initialize`, so any Quoro design assuming stateful sessions or `Mcp-Session-Id` will not survive an rmcp upgrade.
- No primary source found for **server-side** OAuth/token validation in rmcp; if Quoro's MCP endpoint must authenticate inbound agents, that middleware may have to be written on the Axum side. Unverified.
- rmcp licence is in an MIT -> Apache-2.0 transition (GitHub reports "Other"). Legal review needed before distribution, especially with D-0004's AGPL core.
- utoipa has a bus factor of roughly one and 256 open issues; no maintainer-succession statement found. A future stall would strand the API contract.
- The utoipa -> orval break (#1636/#1649) is unresolved as of 2026-11-02; whether the fix lands in 6.0.x or 6.1 is unknown.
- No direct repro was found for utoipa `Option<T>` `oneOf [null, T]` failing in orval or openapi-ts; this needs a small fixture test before committing to the pipeline.
- Litestream 0.5.x has no client-side encryption (age removed). R2 does list SSE-C (`x-amz-server-side-encryption-customer-*`) as supported on PutObject, GetObject, HeadObject, CreateMultipartUpload and CopyObject, and lists SSE/SSE-KMS headers as unsupported. **Confirmed.** Source: https://developers.cloudflare.com/r2/api/s3/api/ (accessed 2026-11-02, reviewer agent:so-research). Whether Litestream's SSE-C path works end to end against R2 is untested. SSE-C still means R2 holds plaintext transiently and the key travels with every request; it is not equivalent to client-side encryption.
- Litestream is beta/0.x with no 1.0 date published; the 0.5 LTX format is incompatible with 0.3 (`generations` removed), so restore tooling must match the version that wrote the replica.
- The R2 429 backoff fix (#1609) is not in 0.5.17; the next release date is unknown.
- hey-api's stable channel is ~4.5 months old while `next` prereleases continue; how quickly 1.0 lands is unknown.
- Download counts and star counts are point-in-time and were read on 2026-11-02.

## Sources

1. https://crates.io/api/v1/crates/rmcp (accessed 2026-11-02)
2. https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/README.md (accessed 2026-11-02)
3. https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/LICENSE (accessed 2026-11-02)
4. https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/crates/rmcp/src/model.rs (accessed 2026-11-02)
5. https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/docs/OAUTH_SUPPORT.md (accessed 2026-11-02)
6. https://registry.npmjs.org/orval (accessed 2026-11-02)
7. https://registry.npmjs.org/@hey-api%2fopenapi-ts (accessed 2026-11-02)
8. https://api.github.com/repos/hey-api/hey-api (accessed 2026-11-02)
9. https://orval.dev/docs/reference/configuration/input (accessed 2026-11-02)
10. https://heyapi.dev/docs/openapi/typescript/configuration/input (accessed 2026-11-02)
11. https://raw.githubusercontent.com/orval-labs/orval/master/README.md (accessed 2026-11-02)
12. https://heyapi.dev/docs/openapi/typescript/get-started (accessed 2026-11-02)
13. https://api.npmjs.org/downloads/point/last-week/orval (accessed 2026-11-02)
14. https://api.npmjs.org/downloads/point/last-week/@hey-api/openapi-ts (accessed 2026-11-02)
15. https://api.github.com/repos/orval-labs/orval (accessed 2026-11-02)
16. https://api.github.com/repos/modelcontextprotocol/rust-sdk (accessed 2026-11-02)
17. https://heyapi.dev/docs/openapi/typescript/migrating (accessed 2026-11-02)
18. https://api.github.com/repos/modelcontextprotocol/rust-sdk/releases (accessed 2026-11-02)
19. https://api.github.com/repos/modelcontextprotocol/rust-sdk/releases/tags/rmcp-v2.0.0 (accessed 2026-11-02)
20. https://api.github.com/repos/modelcontextprotocol/rust-sdk/releases/tags/rmcp-v3.0.0 (accessed 2026-11-02)
21. https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/crates/rmcp/CHANGELOG.md (accessed 2026-11-02)
22. https://api.github.com/repos/modelcontextprotocol/rust-sdk/contributors?per_page=1 (accessed 2026-11-02)
23. https://api.github.com/repos/benbjohnson/litestream (accessed 2026-11-02)
24. https://api.github.com/repos/benbjohnson/litestream/releases (accessed 2026-11-02)
25. https://api.github.com/repos/benbjohnson/litestream/releases/tags/v0.5.0 (accessed 2026-11-02)
26. https://raw.githubusercontent.com/benbjohnson/litestream/main/README.md (accessed 2026-11-02)
27. https://raw.githubusercontent.com/benbjohnson/litestream/main/CONTRIBUTING.md (accessed 2026-11-02)
28. https://litestream.io/guides/s3-compatible/ (accessed 2026-11-02)
29. https://litestream.io/how-it-works/ (accessed 2026-11-02)
30. https://litestream.io/tips/ (accessed 2026-11-02)
31. https://litestream.io/guides/docker/ (accessed 2026-11-02)
32. https://litestream.io/guides/go-library/ (accessed 2026-11-02)
33. https://litestream.io/guides/vfs/ (accessed 2026-11-02)
34. https://litestream.io/reference/config/ (accessed 2026-11-02)
35. https://api.github.com/repos/benbjohnson/litestream/issues/1098 (accessed 2026-11-02)
36. https://api.github.com/repos/benbjohnson/litestream/issues/1603 (accessed 2026-11-02)
37. https://api.github.com/repos/benbjohnson/litestream/issues/948 (accessed 2026-11-02)
38. https://api.github.com/repos/benbjohnson/litestream/pulls/902 (accessed 2026-11-02)
39. https://api.github.com/repos/benbjohnson/litestream/commits (accessed 2026-11-02)
40. https://crates.io/api/v1/crates/utoipa (accessed 2026-11-02)
41. https://raw.githubusercontent.com/juhaku/utoipa/master/README.md (accessed 2026-11-02)
42. https://raw.githubusercontent.com/juhaku/utoipa/master/utoipa/CHANGELOG.md (accessed 2026-11-02)
43. https://raw.githubusercontent.com/juhaku/utoipa/master/utoipa-gen/CHANGELOG.md (accessed 2026-11-02)
44. https://api.github.com/repos/juhaku/utoipa (accessed 2026-11-02)
45. https://api.github.com/repos/juhaku/utoipa/contributors?per_page=3 (accessed 2026-11-02)
46. https://api.github.com/repos/juhaku/utoipa/issues/1260 (accessed 2026-11-02)
47. https://api.github.com/repos/juhaku/utoipa/issues/1636 (accessed 2026-11-02)
48. https://api.github.com/repos/juhaku/utoipa/issues/1649 (accessed 2026-11-02)
49. https://crates.io/api/v1/crates/utoipa-axum/0.3.0/dependencies (accessed 2026-11-02)
50. https://crates.io/api/v1/crates/axum (accessed 2026-11-02)
51. https://crates.io/api/v1/crates/aide (accessed 2026-11-02)
52. https://crates.io/api/v1/crates/poem-openapi (accessed 2026-11-02)
53. https://api.github.com/search/issues?q=repo:orval-labs/orval+%22OpenAPI+3.1%22 (accessed 2026-11-02)
54. https://api.github.com/search/issues?q=utoipa+repo:hey-api/hey-api (accessed 2026-11-02)
55. https://api.github.com/search/issues?q=repo:benbjohnson/litestream+R2 (accessed 2026-11-02)
