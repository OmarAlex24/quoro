# Quoro panel

The operator's panel for Quoro: **Setup**, **Login**, **Apps** (list, deploy
from an image or a Git source, inspect, operate, build, live logs),
**Virtual machines** (list, create, start, stop, delete, with the state visible
after each step), and the agency screens:
**Agents** and their keys, **Grants**, **Approvals**, the **Audit log** with
chain verification, and **Webhooks** with their deliveries. React 19 + TypeScript on
Vite, generated from the frozen contract in `../docs/contracts/openapi.yaml`.

Routes: `/` is the apps list, `/apps/new` the deploy work order, `/apps/$appId`
one app with its source, builds, deployment, domains, environment, instances and
log tape, `/apps/$appId/builds/$buildId` one build with its live log,
`/apps/$appId/terminal` a web terminal into the app's guest, `/vms` the virtual
machines, `/vms/$vmId/console` the read-only serial console of one VM,
`/agents` and `/agents/$agentId`, `/grants`
(`?agent_id=&status=&kind=`), `/approvals`, `/audit`
(`?actor_id=&target_id=&result=&incident_id=&seq=`), `/webhooks` and
`/webhooks/$webhookId`, plus `/login` and `/setup`.

## Commands

Node 22, npm 10.

```sh
npm ci            # install exactly what package-lock.json pins
npm run generate  # orval: regenerate src/api/generated/ from the frozen spec
npm run dev       # real mode: proxies /api to https://127.0.0.1:8443
npm run dev:mock  # mock mode: stateful in-browser MSW, no server needed
npm run typecheck # generate + tsc --noEmit (strict)
npm test          # vitest: the pure logic in src/lib and the mock's audit chain
npm run build     # generate + typecheck + vite build -> dist/
```

`build`, `dev`, `dev:mock` and `typecheck` run `generate` first; the generated
client is never hand-edited and is git-ignored.

## Modes

- **Real** (`npm run dev`): Vite serves the panel on `http://localhost:5173` and
  proxies `/api` to `https://127.0.0.1:8443` with `secure: false` (the engine's
  self-signed certificate). Requests are same-origin, so the session cookie is
  sent with `credentials: 'same-origin'`.
- **Mock** (`npm run dev:mock`, or `vite --mode mock`): the MSW service worker
  in `mock-public/` is served and `src/mocks/` answers every request from an
  in-memory database. The mock is stateful: setup creates the owner, login
  starts a session, and it seeds `lab-3`, a **Blog** project with one running
  `blog` app and a **Docs** project with one running `docs-site` app that builds
  from `https://github.com/example/docs-site.git`. The Git app comes with one
  succeeded and one failed build (the failed one is newest, with the BuildKit
  excerpt), a built image, a `trigger: build` deployment and an instance.
  Projects are created from the deploy screen or the New VM dialog. A created VM
  goes `creating` → `starting` → `running` over about 1.6 s; a deployment goes
  `pending` → `preparing` (pull, rootfs) → `starting` (boot) → `succeeded` over
  about 3 s. Start and stop converge in 1.5 s; delete removes the VM immediately.
  `/api/v1/apps/<id>/logs/stream` answers `text/event-stream` and emits a fresh
  nginx-style access line every second.
  A build goes `queued` → `preparing` → `building` → `exporting` → `importing` →
  `succeeded` over about 4 s while
  `/api/v1/builds/<id>/logs/stream` replays BuildKit-style lines (`#5 [3/4] RUN
  npm ci`, `#5 DONE 2.4s`, …) with `seq` ids and a `vertex`, sends `status` on
  every move and closes with `end`. A successful build sets the app image to
  `build.quoro.invalid/<app_id>:<build_id>` and starts a `trigger: build`
  deployment. A repository URL containing `fail` stops in `building` at
  `[3/4] RUN ls /does-not-exist` with `error`, `dockerfile_line: 3` and a real
  BuildKit trailer in `excerpt`; the log ends with the same ERROR line.
  The chosen password is the one you set up with. Any non-empty setup token
  works in mock mode.

To see each screen in mock mode: set up, log in, then open `/` (apps rack),
`/apps/new` (deploy), click `blog` for `/apps/<id>`, `docs-site` for its source
and builds, or `/vms` for the VM rack. The seeded app's tape starts printing
lines on the app screen. For a live build, open `/apps/new`, choose **Git
repository**, use `https://github.com/example/docs-site.git` and submit; the
build screen follows it to a deployment. To see a failure, use a URL containing
`fail` (for example `https://github.com/example/fail-demo.git`). To see a
cancellation, start a build and press **Cancel build** while it runs. The seeded
`docs-site` failed build shows the failing-step panel without starting anything.
The app plate's **Terminal** action opens `/apps/<id>/terminal`; each VM bay's
**Console** action opens `/vms/<id>/console`. Both answer over a mock
WebSocket: the terminal prints a banner and a `/ # ` prompt, echoes typing, and
answers `uname -a`, `help` and `exit` (`exit` sends `exit` then `closed` and
closes 1000); any other command prints `sh: <cmd>: not found`. The console
replays a kernel boot log and `quoro-init` lines, then prints one line every
few seconds and refuses input with an `error` control frame.

The agency screens are seeded on first use after setup: two agents
(`on-call-restarter`, enabled; `weekly-reporter`, disabled), a standard, an
incident and an expired grant, an open incident for `blog`, one pending and one
denied approval, two webhooks with deliveries, and an audit log that already
holds the M3 demo entries (the agent's `restart_app` through MCP, its refused
`delete_app`, its approval request). The mock's audit log is a real SHA-256
hash chain, recomputed by verify. Three console helpers exist in mock mode only:

- `quoroMock.tamperAudit(3)` edits entry 3 without rehashing it; the next
  verify reports `Chain broken at seq 3` (`hash_mismatch`).
- `quoroMock.fileMockApproval()` files a new pending `app:delete` request from
  `on-call-restarter`.
- `quoroMock.setRole('viewer')` (or `operator`, `owner`) changes the owner's
  role; log out and in again to see the panel as that role.

The production build contains no MSW code: `src/mocks/` is only reached through
`import.meta.env.MODE === 'mock'` in `src/main.tsx`, and `mock-public/` is the
`publicDir` only in mock mode. `grep -r msw dist` prints nothing.

## What the panel respects

- Every colour comes from the tokens in `src/styles.css`, in a dark and a light
  set. The theme follows `prefers-color-scheme`; `data-theme="dark"` or
  `"light"` on `<html>` overrides it. Tailwind sees only the tokens: its
  default palette is dropped, so `bg-gray-500` or `text-white` do not exist.
- Archivo sets every word; Martian Mono sets only code values (ids, keys,
  scopes, images, hashes, log lines), never a label. Both are self-hosted
  (`font-src 'self'`).
- The dither kit (`src/lib/dither.ts`, `src/lib/dither-draw.ts`,
  `DitherCanvas`) draws an ordered 8x8 Bayer dither at a device pixel ratio of
  at least 2, so each cell lands on whole pixels, and redraws when the theme
  changes. Two-tone fills use a second matrix (Bayer with its row and column
  bits reversed) so the second tone spreads evenly. An agent avatar is a
  mirrored 5x5 dither seeded by the agent id, in one of three colour pairs; a
  person is round with initials; the system is the Quoro mark.
- One microVM per app; the list is a rack of bays, each with a lamp whose
  **outer ring is the desired state and whose core is the observed state**. The
  lamp is still when they agree, pulses slowly while a VM converges (not under
  `prefers-reduced-motion`), and is fault-red with no pulse when crashed. The
  caption says it plainly: "Starting, will be running".
- The VM list refreshes every 1 s while any VM is converging and every 10 s
  otherwise, and is invalidated after each action.
- The apps rack refreshes every 2 s while any app deploys and every 10 s
  otherwise. Each bay's lamp maps `app.status`: running → run, deploying →
  shift pulsing, degraded → shift, down → fault, stopped → idle.
- Deploying is a checked sequence, each call awaited: `createApp` →
  `replaceAppEnv` (only with variables) → `createAppDomain` (only with a
  hostname) → `createDeployment`. Each mutating call sends a fresh
  `Idempotency-Key` (`crypto.randomUUID()`). If a later step fails the app is
  already real, so the plate offers "Open the app" rather than creating it
  twice.
- The work order chooses a source first: an **Image** (the sequence above) or a
  **Git repository**. A Git app is created with `source` and no `image`, then
  gets the same environment and domain, then its first `startBuild` instead of a
  deployment, and the panel opens its build. The source form takes an `https://`
  repository URL, ref (`main`), Dockerfile path (`Dockerfile`), context (`.`),
  an optional target stage, `--build-arg` rows and build secrets picked from the
  environment variable names entered on the same form. The checks live in
  `src/lib/source-form.ts`.
- A Git app's screen shows its source (repository, ref, Dockerfile, context,
  last built commit) and its builds newest first, and a "Build now" button with
  an optional one-build ref override. The button is disabled, with the reason in
  words, while a build is not terminal or the role cannot deploy (`app:deploy`).
  `createDeployment` answers `conflict` for a Git app, so the panel never calls
  it for one.
- The build view is a phase rail (`queued → preparing → building → exporting →
  importing → succeeded`) plus the log. The log is one `EventSource` on
  `/api/v1/builds/<id>/logs/stream?tail=2000` with credentials: `log` events
  carry `BuildLogLine` with its `vertex`, `status` events move the rail, `error`
  events are a `Problem` said in plain words, and `end` closes the source. The
  browser resumes with `Last-Event-ID` by itself. A finished build replays its
  log and ends. When the stream cannot open (the node answers 504) the panel
  reads the same log through `getBuildLogs` pages and says the full log stays on
  the node; the replicated failing step is shown either way.
- A failed build shows the phase, the BuildKit step (`[3/4] RUN …`), its error,
  the Dockerfile line and the excerpt with the failing line (`>>>` or
  `dockerfile_line`) at full contrast; "Jump to step" scrolls the log to the
  first line of that `vertex`. A successful build links to the deployment it
  started. A non-terminal build offers "Cancel build" (`cancelBuild`).
- A built image is named `build.quoro.invalid/<app_id>:<build_id>`, so the panel
  says "built image" and the build id and never shows it as a pullable
  registry reference.
- The app screen shows the newest deployment as four rail stations (pull,
  rootfs, boot, health) derived from `status` + `phase`, and polls
  `getDeployment` every 1 s while it is pending, preparing or starting. There
  is no deployment SSE stream in the contract. A 504 from the progress-lines
  call says the node is unreachable.
- The app screen's **Storage and HA** section, between Environment and
  Instances, offers a stateless app "Add a volume" (1–1024 GiB and an absolute
  mount path, `createVolume`) and, for a stateful app, its volume's size, mount
  path, node, state and allocated bytes; the four HA policies (warm and hot say
  "not in this release", cold says "needs a volume" or "needs a backup
  repository") with the `ha_cost` table (reserved vCPU, memory and disk,
  storage, restore estimate as `m:ss` or `h:mm:ss`, pause per backup, the
  current row highlighted, all of it labelled estimates); the backup schedule
  while the policy is `cold`; and the backups newest first, with "Back up now"
  and its 409 in the screen's `ProblemBanner`. A policy or schedule change calls
  `updateApp` and refetches the app so `ha_cost` updates. The audit screen's
  **Off-site copy** block reads `listAuditExports`, says the strength in the
  words audit-log-format.md section 8.2 fixes, and shows "Probe again" to an
  owner only. Mock mode seeds `blog` with a 10 GiB volume on `lab-3` that the
  node snapshots with `reflink`, a `lab-backups` repository, two succeeded
  backups and one failed one, and an audit export whose strength is
  `bucket_rule`; the mock computes `ha_cost` and the cold row moves from the
  volume's allocated bytes to the kept backups' uploaded bytes when the policy
  switches, and "Back up now" adds a backup that goes queued → running →
  succeeded over about 2 s.
- A terminal or console session opens with one POST and then a WebSocket
  carrying the single-use ticket (30 s, never shown or stored). Guest bytes go
  only into xterm.js; the clipboard (OSC 52) and web-links addons are not
  loaded, so the untrusted guest cannot write the clipboard or open links. The
  console is `disableStdin` and only a terminal sends keystrokes and debounced
  `resize` controls.
- Live logs use one `EventSource` on  `/api/v1/apps/<id>/logs/stream?tail=200` with credentials, `log` events
  (`LogEntry`), `error` events whose data is a `Problem`, and `end`. The tape
  keeps at most 2000 lines, follows the newest line unless the operator scrolls
  up, pauses on request, and closes the source on unmount.
- `403 forbidden` renders its `reason` in words ("Your role can't create VMs
  (scope_missing: vm:create)"), `422 validation_failed` shows `detail` next to
  the form, `409` from setup means setup is already done, and a `401` mid-session
  drops the identity and says "Your session ended. Log in again."
- A `viewer` sees the list but no create/start/stop/delete controls. The server
  still decides; the panel only hides what the role cannot do.
- Create VM sends a fresh `Idempotency-Key` (`crypto.randomUUID()`) per submit.
- Every request goes through `src/api/fetcher.ts`, which turns a non-2xx
  `application/problem+json` body into an `ApiProblem` (status, code, reason,
  detail, scope).

- Approve and Deny render only for a human `owner` or `operator`
  (`canDecideApprovals` in `src/lib/capabilities.ts`); an agent session or a
  viewer sees the queue read-only. The same rule hides the agent, key, grant
  and webhook write controls (`agent:write`, `grant:write`, `webhook:write` are
  human-only).
- An agent key and a webhook signing secret are shown once, from the create
  call's result only (`SecretOnce`): never written to the query cache, the
  URL, browser storage or a notice, and dropped when the plate closes.
- The grant and webhook-wake scope pickers never offer a human-only or
  destructive scope (`src/lib/scopes.ts`, permission-model.md sections 4.1 and
  4.2). An incident picked for a new grant travels as the `Quoro-Incident`
  header, not a body field.
- The audit log is read in ascending `seq` with cursor paging, as the API
  serves it. Verify shows a clean pass, or the exact `broken_at_seq` with its
  `reason`, draws that link broken on the chain rail and opens the entry. An
  entry shows every field of audit-log-format.md section 2, "Not recorded" for
  absent ones; user ids resolve to names only for the owner (the user list
  needs `user:write`), and to "you" for the signed-in user.

## Dependencies, each with a reason

Runtime:

| Package | Version | Why |
| --- | --- | --- |
| `react` | 19.3.1 | The UI runtime. |
| `react-dom` | 19.3.1 | Mounts the panel in the browser. |
| `@tanstack/react-router` | 1.170.46 | Code-based routes (`/`, `/apps/new`, `/apps/$appId`, `/vms`, `/login`, `/setup`), no file-route codegen. |
| `@tanstack/react-query` | 5.105.2 | Server state, the 1 s / 10 s refresh, and invalidation after actions. |
| `@fontsource-variable/archivo` | 5.3.0 | Self-hosted Archivo (weight and width axes); no CDN, no external request. |
| `@fontsource-variable/martian-mono` | 5.3.0 | Self-hosted Martian Mono (OFL-1.1, weight and width axes) for code values only; no CDN. |
| `@radix-ui/react-dialog` | 1.3.1 | Accessible New VM, edit-variables and delete-app dialogs. |
| `@radix-ui/react-select` | 2.4.1 | Accessible project and node pickers inside that dialog. |
| `@radix-ui/react-slot` | 1.5.0 | `Button asChild`, the shadcn/ui pattern. |
| `class-variance-authority` | 0.7.1 | Button variants (primary, default, ghost, danger, danger-solid) and sizes in one place. |
| `clsx` | 2.1.1 | Conditional class names. |
| `tailwind-merge` | 3.7.0 | `cn()` lets a caller override a component's utilities. |
| `lucide-react` | 1.66.0 | The icons the panel uses: select chevron, check, dialog close, the copy button and the kind glyphs (Git, image, template, VM). |
| `@xterm/xterm` | 6.0.0 | Renders the web terminal and the read-only console; guest bytes never touch the DOM as HTML. |
| `@xterm/addon-fit` | 0.11.0 | Fits the terminal grid to its bezel and reports the new size for the `resize` control. |

Development:

| Package | Version | Why |
| --- | --- | --- |
| `vite` | 8.3.5 | Dev server and production build. |
| `@vitejs/plugin-react` | 6.2.1 | React fast refresh and JSX transform. |
| `tailwindcss` | 4.3.3 | Utility CSS and the `@theme` design tokens. |
| `@tailwindcss/vite` | 4.3.3 | Tailwind v4 as a Vite plugin, no PostCSS config. |
| `typescript` | 5.9.3 | Strict type checking; the generated client must type-check. |
| `orval` | 8.46.0 | Generates the typed client and MSW handlers from the frozen OpenAPI spec. |
| `msw` | 3.0.3 | The stateful browser mock behind `dev:mock`; never in the build. |
| `@faker-js/faker` | 10.6.0 | Peer of orval's generated mock data (fallback handlers). |
| `vitest` | 5.1.0 | Runs the pure-logic unit tests (lamp, app status, deploy order, log buffer, dither, theme). |
| `@types/react` | 19.3.0 | React types. |
| `@types/react-dom` | 19.3.0 | React DOM types. |

## Layout

```
src/
  api/            fetcher.ts (the mutator), client.ts (query client), auth-store.ts
  api/generated/  orval output (git-ignored, regenerated by `npm run generate`)
  components/     ui/ (button, tag, dialog, field, input, textarea, select, banner),
                  lamp, avatar, glyph-tile, dither-canvas,
                  vm-bay, app-bay, deploy-rail, build-rail, build-failure,
                  build-log, build-view, log-tape, tone-lamp, app-domains,
                  app-env-dialog, secret-once, copy-button, scope-picker,
                  selector-editor, identity-name, session-room, xterm-view,
                  agent-*, grant-*, approval-*, audit-*, webhook-*
  lib/            lamp.ts, app-status.ts, build-status.ts, build-log.ts,
                  source-form.ts, deploy-flow.ts, log-buffer.ts, problem.ts,
                  slug.ts, capabilities.ts, scopes.ts, interactive.ts,
                  agency-format.ts, agents.ts, grants.ts, approvals.ts, audit.ts,
                  webhooks.ts, dither.ts, dither-draw.ts, use-theme.ts
                  (each with a test where pure)
  mocks/          db.ts, builds.ts, http.ts, agency.ts, handlers.ts,
                  handlers-builds.ts, handlers-<area>.ts, browser.ts (mock only)
  screens/        apps-screen.tsx, deploy-screen.tsx, app-screen.tsx,
                  build-screen.tsx, vms-screen.tsx, terminal-screen.tsx,
                  console-screen.tsx, agents-screen.tsx, agent-screen.tsx,
                  grants-screen.tsx, approvals-screen.tsx, audit-screen.tsx,
                  webhooks-screen.tsx, webhook-screen.tsx, login-screen.tsx,
                  setup-screen.tsx
mock-public/      mockServiceWorker.js, served only in mock mode
```

`src/lib/lamp.ts` is pure: it maps `desired_status` / `observed_status` to the
ring and core tones, the pulse, the caption and the list's convergence check.
`src/lib/app-status.ts` maps `app.status` to the app lamp, derives the four
deployment stations, names certificate states, picks the default node and
formats relative change times. `src/lib/build-status.ts` does the same for a
build: the phase rail, the lamp, the commit, the duration and the "built image"
label. `src/lib/build-log.ts` parses the build stream's frames, orders lines by
`seq` across a resume and finds the failing line in an excerpt.
`src/lib/source-form.ts` checks the Git source and derives the app name from the
repository. `src/lib/deploy-flow.ts` holds the deploy step order (an image ends
in a deployment, a Git source in its first build) and the app-name derivation.
`src/lib/log-buffer.ts` holds the 2000-line cap, SSE frame writer/parser used by
the mock, local time formatting and the `Problem`-from-SSE conversion. Each has
a vitest file next to it.
