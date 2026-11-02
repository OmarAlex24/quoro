# Quoro panel

The operator's panel for Quoro: **Setup**, **Login** and **Virtual machines**
(list, create, start, stop, delete, with the state visible after each step).
React 19 + TypeScript on Vite, generated from the frozen contract in
`../docs/contracts/openapi.yaml`.

## Commands

Node 22, npm 10.

```sh
npm ci            # install exactly what package-lock.json pins
npm run generate  # orval: regenerate src/api/generated/ from the frozen spec
npm run dev       # real mode: proxies /api to https://127.0.0.1:8443
npm run dev:mock  # mock mode: stateful in-browser MSW, no server needed
npm run typecheck # generate + tsc --noEmit (strict)
npm test          # vitest: the lamp state logic
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
  starts a session, `lab-3` is the only seeded thing, projects are created from
  the New VM dialog, and a created VM goes `creating` → `starting` → `running`
  over about 1.6 s (start and stop converge in 1.5 s; delete removes the VM
  immediately). The chosen password is the one you set up with. Any non-empty
  setup token works in mock mode.

The production build contains no MSW code: `src/mocks/` is only reached through
`import.meta.env.MODE === 'mock'` in `src/main.tsx`, and `mock-public/` is the
`publicDir` only in mock mode. `grep -r msw dist` prints nothing.

## What the panel respects

- One microVM per app; the list is a rack of bays, each with a lamp whose
  **outer ring is the desired state and whose core is the observed state**. The
  lamp is still when they agree, pulses slowly while a VM converges (not under
  `prefers-reduced-motion`), and is fault-red with no pulse when crashed. The
  caption says it plainly: "Starting, will be running".
- The VM list refreshes every 1 s while any VM is converging and every 10 s
  otherwise, and is invalidated after each action.
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

## Dependencies, each with a reason

Runtime:

| Package | Version | Why |
| --- | --- | --- |
| `react` | 19.3.1 | The UI runtime. |
| `react-dom` | 19.3.1 | Mounts the panel in the browser. |
| `@tanstack/react-router` | 1.170.46 | Code-based routes (`/`, `/login`, `/setup`), no file-route codegen. |
| `@tanstack/react-query` | 5.105.2 | Server state, the 1 s / 10 s refresh, and invalidation after actions. |
| `@fontsource-variable/archivo` | 5.3.0 | Self-hosted Archivo (weight and width axes); no CDN, no external request. |
| `@radix-ui/react-dialog` | 1.3.1 | Accessible New VM dialog and the in-place delete confirmation container. |
| `@radix-ui/react-select` | 2.4.1 | Accessible project and node pickers inside that dialog. |
| `@radix-ui/react-slot` | 1.5.0 | `Button asChild`, the shadcn/ui pattern. |
| `class-variance-authority` | 0.7.1 | Button variants (primary, secondary, danger, quiet) in one place. |
| `clsx` | 2.1.1 | Conditional class names. |
| `tailwind-merge` | 3.7.0 | `cn()` lets a caller override a component's utilities. |
| `lucide-react` | 1.66.0 | The two icons the panel uses: select chevron, check, dialog close. |

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
| `vitest` | 5.1.0 | Runs the lamp state unit test. |
| `@types/react` | 19.3.0 | React types. |
| `@types/react-dom` | 19.3.0 | React DOM types. |

## Layout

```
src/
  api/            fetcher.ts (the mutator), client.ts (query client), auth-store.ts
  api/generated/  orval output (git-ignored, regenerated by `npm run generate`)
  components/     ui/ (button, dialog, field, input, select, banner), lamp, vm-bay, ...
  lib/            lamp.ts (desired/observed logic + test), problem.ts, slug.ts
  mocks/          db.ts, handlers.ts, browser.ts (mock mode only)
  screens/        vms-screen.tsx, login-screen.tsx, setup-screen.tsx
mock-public/      mockServiceWorker.js, served only in mock mode
```

`src/lib/lamp.ts` is pure: it maps `desired_status` / `observed_status` to the
ring and core tones, the pulse, the caption and the list's convergence check.
`src/lib/lamp.test.ts` covers agreement, every converging step, crash and
unknown.
