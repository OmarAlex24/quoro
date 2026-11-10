// SPDX-License-Identifier: AGPL-3.0-only
import { setupServer, type SetupServer } from 'msw/node';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type {
  App,
  AppDetail,
  AppList,
  Build,
  Deployment,
  DeploymentList,
  DeploymentLogList,
  IncidentList,
} from '../api/generated/model';

/*
 * The seed's in-progress states, read through the handlers as the panel
 * reads them: blog down with its open incident, and Atlas's web in a redeploy
 * that walks its phases and starts again once it succeeded. Every test loads
 * a fresh mock (a new seed) with the clock faked, signs the owner in, and
 * reads at chosen moments of the cycle.
 */

const BASE = 'http://panel.test/api/v1';
const SIGN_IN = Date.UTC(2026, 10, 10, 12, 0, 0);

type DbModule = typeof import('./db');
type AgencyModule = typeof import('./agency');

let server: SetupServer | null = null;

/**
 * `db.schedule` runs its timers on the browser's `window`, so the test's
 * global stands in for it, but only once the handlers are loaded: msw's
 * WebSocket link reads a `window` as a browser's.
 */
const pointWindowAtGlobal = (on: boolean) => {
  const global = globalThis as unknown as { window?: typeof globalThis };
  if (on) global.window = globalThis;
  else delete global.window;
};

afterEach(() => {
  server?.close();
  server = null;
  vi.useRealTimers();
});

/**
 * A fresh mock: the seed is made, the owner set up and signed in, and the
 * first read starts the cycle at `SIGN_IN`. `at(seconds)` moves the clock to
 * that many seconds after the cycle's first redeploy started.
 */
const freshMock = async () => {
  vi.resetModules();
  // Only the clock and the timers the mock schedules with: msw answers through the others.
  vi.useFakeTimers({ now: SIGN_IN - 60_000, toFake: ['Date', 'setTimeout', 'clearTimeout'] });
  pointWindowAtGlobal(false);
  const mock: DbModule = await import('./db');
  const agencyModule: AgencyModule = await import('./agency');
  const { handlers } = await import('./handlers');
  pointWindowAtGlobal(true);
  server = setupServer(...handlers);
  server.listen();

  const { db } = mock;
  db.owner = {
    id: mock.mockId('usr'),
    email: 'owner@example.com',
    name: 'Owner',
    role: 'owner',
    disabled: false,
    created_at: mock.now(),
  };
  db.users = [db.owner];
  db.setupDone = true;
  db.sessionUserId = db.owner.id;
  vi.setSystemTime(SIGN_IN);

  const read = async <T>(path: string): Promise<T> => {
    const response = await fetch(`${BASE}${path}`);
    expect(response.status).toBe(200);
    return (await response.json()) as T;
  };
  const post = (path: string, body?: unknown) =>
    fetch(`${BASE}${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  const appNamed = async (name: string, project: string): Promise<AppDetail> => {
    const projectId = db.projects.find((item) => item.slug === project)?.id;
    const { items } = await read<AppList>('/apps');
    const app = items.find((item: App) => item.name === name && item.project_id === projectId);
    if (!app) throw new Error(`the mock no longer seeds ${project}/${name}`);
    return read<AppDetail>(`/apps/${app.id}`);
  };
  const web = await appNamed('web', 'atlas');
  const at = (seconds: number) => vi.setSystemTime(SIGN_IN + seconds * 1_000);
  const deploymentsOf = async (appId = web.id): Promise<Deployment[]> =>
    (await read<DeploymentList>(`/apps/${appId}/deployments`)).items;
  const logsOf = async (deploymentId: string) =>
    (await read<DeploymentLogList>(`/deployments/${deploymentId}/logs`)).items;
  const cycleIds = () => db.deployCycle?.deploymentIds ?? [];
  const events = (type: string) => agencyModule.agency().events.filter((event) => event.type === type);
  const redeployRecords = () =>
    agencyModule.agency().audit.filter((record) => record.operation === 'POST /api/v1/apps/{app_id}/redeploy');

  return { ...mock, ...agencyModule, read, post, appNamed, web, at, deploymentsOf, logsOf, cycleIds, events, redeployRecords };
};

const LIVE = ['pending', 'preparing', 'starting'];

describe('blog in the seed', () => {
  it('is down, as its open incident says', async () => {
    const { appNamed, read } = await freshMock();
    const blog = await appNamed('blog', 'blog');
    expect(blog.status).toBe('down');

    const { items } = await read<IncidentList>('/incidents?status=open');
    expect(items.find((incident) => incident.subject.id === blog.id)?.title).toBe('blog is down');
  });

  it('has no instance running: both crashed after the restart, and neither is in the edge rotation', async () => {
    const { appNamed } = await freshMock();
    const blog = await appNamed('blog', 'blog');
    expect(blog.restart_generation).toBe(1);
    expect(blog.instances.map((vm) => [vm.name, vm.observed_status, vm.last_exit_code])).toEqual([
      ['blog-1', 'crashed', 1],
      ['blog-2', 'crashed', 1],
    ]);
    expect(blog.instances.every((vm) => vm.desired_status === 'running' && vm.restart_count === 1)).toBe(true);
    const backends = blog.edge_backends ?? [];
    expect(backends).toHaveLength(2);
    expect(backends.some((backend) => backend.in_rotation)).toBe(false);
    expect(backends.every((backend) => backend.reason === 'connect_failed')).toBe(true);
    expect(backends.every((backend) => backend.last_error?.includes('connection refused'))).toBe(true);
  });

  it("opened its incident from the edge's report that no instance was left in rotation", async () => {
    const { appNamed, events } = await freshMock();
    const blog = await appNamed('blog', 'blog');
    const [down] = events('app.down');
    expect(down?.subject.id).toBe(blog.id);
    expect(down?.data).toMatchObject({ check: 'edge_health', node_id: blog.edge_backends?.[0]?.edge_node_id });
  });
});

describe("Atlas's web in the seed", () => {
  it('starts a redeploy by the owner when they sign in, with the last good deployment still current', async () => {
    const { at, appNamed, deploymentsOf, logsOf, db, read, web } = await freshMock();
    at(10);
    const detail = await read<AppDetail>(`/apps/${web.id}`);
    expect(detail.status).toBe('deploying');

    const [live, previous] = await deploymentsOf();
    expect(live).toMatchObject({
      status: 'preparing',
      phase: 'pull',
      trigger: 'redeploy',
      created_by_type: 'user',
      created_by_id: db.owner?.id,
      created_at: new Date(SIGN_IN).toISOString(),
      finished_at: null,
      image: web.image,
    });
    expect(previous?.status).toBe('succeeded');
    expect(detail.current_deployment_id).toBe(previous?.id);
    expect((await logsOf(live!.id)).map((line) => line.phase)).toEqual(['pull']);
    expect((await appNamed('api', 'atlas')).status).toBe('running');
  });

  it('starts nothing before the owner signs in', async () => {
    vi.resetModules();
    vi.useFakeTimers({ now: SIGN_IN });
    const { db } = await import('./db');
    const { tickDeployCycle } = await import('./agency');
    tickDeployCycle();
    expect(db.deployCycle?.deploymentIds).toEqual([]);
    expect(db.apps.find((app) => app.id === db.deployCycle?.appId)?.status).toBe('running');
  });

  it('walks pull, rootfs, boot and health forward, booting one instance at a time', async () => {
    const { at, read, deploymentsOf, logsOf, web, cycleIds } = await freshMock();
    const seen: Array<[string, string | null | undefined, string]> = [];
    for (const seconds of [10, 30, 50, 75, 90, 120, 140, 170]) {
      at(seconds);
      const detail = await read<AppDetail>(`/apps/${web.id}`);
      const [deployment] = await deploymentsOf();
      expect(detail.status).toBe('deploying');
      seen.push([
        deployment?.status ?? '',
        deployment?.phase,
        detail.instances.map((vm) => `${vm.name}:${vm.observed_status}`).join(' '),
      ]);
    }
    expect(seen).toEqual([
      ['preparing', 'pull', 'web-1:running web-2:running'],
      ['preparing', 'pull', 'web-1:running web-2:running'],
      ['preparing', 'rootfs', 'web-1:running web-2:running'],
      ['preparing', 'rootfs', 'web-1:running web-2:running'],
      ['starting', 'boot', 'web-1:starting web-2:running'],
      ['starting', 'boot', 'web-1:running web-2:starting'],
      ['starting', 'health', 'web-1:running web-2:running'],
      ['starting', 'health', 'web-1:running web-2:running'],
    ]);
    const lines = await logsOf(cycleIds()[0]!);
    expect(lines.map((line) => [line.phase, line.message.split(' ')[0]])).toEqual([
      ['pull', 'Pulling'],
      ['rootfs', 'Preparing'],
      ['boot', 'Booting'],
      ['boot', 'Booting'],
      ['health', 'Waiting'],
    ]);
    expect(lines[2]?.message).toBe('Booting web-1 on lab-3');
    expect(lines[3]?.message).toBe('Booting web-2 on lab-4');
    const times = lines.map((line) => Date.parse(line.time));
    expect(times).toEqual([...times].sort((left, right) => left - right));
  });

  it('succeeds after three minutes, becomes current, and the next redeploy starts after the gap', async () => {
    const { at, read, deploymentsOf, logsOf, web, cycleIds, CYCLE_SECONDS, CYCLE_GAP_S } = await freshMock();
    const first = cycleIds()[0];

    at(CYCLE_SECONDS + 1);
    const done = await read<AppDetail>(`/apps/${web.id}`);
    expect(done.status).toBe('running');
    expect(done.current_deployment_id).toBe(first);
    expect(done.instances.every((vm) => vm.observed_status === 'running')).toBe(true);
    const succeeded = (await deploymentsOf())[0];
    expect(succeeded).toMatchObject({ id: first, status: 'succeeded', phase: 'health' });
    expect(Date.parse(succeeded!.finished_at ?? '') - SIGN_IN).toBe(CYCLE_SECONDS * 1_000);
    expect(succeeded?.image_digest).toMatch(/^sha256:/);
    expect((await logsOf(first!)).at(-1)?.message).toBe('Health check passed; the app is running');

    at(CYCLE_SECONDS + CYCLE_GAP_S + 3);
    const again = await read<AppDetail>(`/apps/${web.id}`);
    expect(again.status).toBe('deploying');
    expect(again.current_deployment_id).toBe(first);
    const [next, previous] = await deploymentsOf();
    expect(next?.id).not.toBe(first);
    expect(next).toMatchObject({ status: 'preparing', phase: 'pull', trigger: 'redeploy' });
    expect(previous?.id).toBe(first);
  });

  it('puts each redeploy in the audit log and the events, in order, and the chain verifies', async () => {
    const { at, read, web, cycleIds, events, redeployRecords, db, agency, verifyAuditChain, CYCLE_SECONDS, CYCLE_GAP_S } =
      await freshMock();
    at(CYCLE_SECONDS + CYCLE_GAP_S + 3);
    await read<AppDetail>(`/apps/${web.id}`);

    const ids = cycleIds();
    expect(ids).toHaveLength(2);
    expect(redeployRecords()).toHaveLength(2);
    expect(redeployRecords().every((record) => record.actor_id === db.owner?.id && record.scope === 'app:redeploy')).toBe(
      true,
    );
    expect(events('deployment.started').map((event) => event.data.deployment_id)).toEqual(ids);
    const [succeeded] = events('deployment.succeeded');
    expect(succeeded?.data.deployment_id).toBe(ids[0]);
    expect(succeeded?.source).toEqual({ kind: 'node', node_id: db.extraNodes[0]?.id });

    // `time` never goes back as `seq` rises, in the audit log or the events.
    for (const log of [agency().audit, agency().events]) {
      const times = log.map((item) => item.time);
      expect(times).toEqual([...times].sort());
    }
    expect(verifyAuditChain().ok).toBe(true);
  });

  it('after a long pause starts only the redeploy that would run now', async () => {
    const { at, read, deploymentsOf, web, cycleIds, events, verifyAuditChain, CYCLE_SECONDS, CYCLE_GAP_S } =
      await freshMock();
    await read<AppDetail>(`/apps/${web.id}`);
    const period = CYCLE_SECONDS + CYCLE_GAP_S;
    at(19 * period + 40);
    await read<AppDetail>(`/apps/${web.id}`);
    expect(cycleIds()).toHaveLength(2);
    const [live] = await deploymentsOf();
    expect(live?.id).toBe(cycleIds()[1]);
    expect(live).toMatchObject({ status: 'preparing', phase: 'rootfs' });
    expect(Date.parse(live!.created_at) - SIGN_IN).toBe(19 * period * 1_000);
    expect(events('deployment.succeeded')).toHaveLength(1);
    expect(verifyAuditChain().ok).toBe(true);
  });
});

describe('a person takes web over from the cycle', () => {
  /** A fresh mock `seconds` into the gap after the first redeploy succeeded. */
  const gap = async (seconds: number) => {
    const mock = await freshMock();
    mock.at(mock.CYCLE_SECONDS + 1);
    await mock.read<AppDetail>(`/apps/${mock.web.id}`);
    expect((await mock.deploymentsOf())[0]?.status).toBe('succeeded');
    mock.at(mock.CYCLE_SECONDS + seconds);
    return mock;
  };

  /**
   * Let the person's action finish (the mock's own timers run within a few
   * seconds), then read now and then, far past where the next redeploy would
   * have started.
   */
  const later = async (mock: Awaited<ReturnType<typeof freshMock>>) => {
    await vi.advanceTimersByTimeAsync(5_000);
    for (const seconds of [200, 260, 400, 700]) {
      vi.setSystemTime(SIGN_IN + seconds * 1_000);
      await mock.read<AppList>('/apps');
    }
  };

  it('a redeploy 3 s into the gap shows Deploying, succeeds, and stays current; no other starts', async () => {
    const mock = await gap(3);
    const response = await mock.post(`/apps/${mock.web.id}/redeploy`);
    expect(response.status).toBe(202);
    const theirs = (await response.json()) as Deployment;

    expect((await mock.read<AppDetail>(`/apps/${mock.web.id}`)).status).toBe('deploying');
    await vi.advanceTimersByTimeAsync(1_000);
    expect((await mock.read<AppDetail>(`/apps/${mock.web.id}`)).status).toBe('deploying');
    await later(mock);

    const detail = await mock.read<AppDetail>(`/apps/${mock.web.id}`);
    expect(detail.status).toBe('running');
    expect(detail.current_deployment_id).toBe(theirs.id);
    expect((await mock.deploymentsOf())[0]).toMatchObject({ id: theirs.id, status: 'succeeded' });
    expect(mock.cycleIds()).toHaveLength(1);
    expect(mock.db.deployCycle?.stopped).toBe(true);
  });

  it('a build 1 s into the gap deploys its image, which becomes current; no other starts', async () => {
    const mock = await gap(1);
    const response = await mock.post(`/apps/${mock.web.id}/builds`, { ref: 'main' });
    expect(response.status).toBe(202);
    const build = (await response.json()) as Build;

    const deployed = () => mock.db.builds.find((item) => item.id === build.id)?.deployment_id;
    for (let step = 0; step < 100 && !deployed(); step += 1) await vi.advanceTimersByTimeAsync(100);
    const deploymentId = deployed();
    expect(deploymentId).toBeTruthy();
    expect((await mock.read<AppDetail>(`/apps/${mock.web.id}`)).status).toBe('deploying');
    await later(mock);

    const detail = await mock.read<AppDetail>(`/apps/${mock.web.id}`);
    expect(detail.status).toBe('running');
    expect(detail.current_deployment_id).toBe(deploymentId);
    expect(mock.cycleIds()).toHaveLength(1);
  });

  it('a build mid-rollout: its deployment supersedes the rollout, and no other starts', async () => {
    const mock = await freshMock();
    mock.at(60);
    const response = await mock.post(`/apps/${mock.web.id}/builds`, { ref: 'main' });
    expect(response.status).toBe(202);
    await mock.read<AppList>('/apps');
    // The build's deployment supersedes the rollout once it starts.
    await later(mock);
    const list = await mock.deploymentsOf();
    expect(list.find((item) => item.id === mock.cycleIds()[0])?.status).toBe('superseded');
    expect(list[0]?.trigger).toBe('build');
    expect((await mock.read<AppDetail>(`/apps/${mock.web.id}`)).current_deployment_id).toBe(list[0]?.id);
    expect(mock.cycleIds()).toHaveLength(1);
  });

  it('a stop in the gap keeps the app stopped', async () => {
    const mock = await gap(3);
    expect((await mock.post(`/apps/${mock.web.id}/stop`)).status).toBe(202);
    await later(mock);
    const detail = await mock.read<AppDetail>(`/apps/${mock.web.id}`);
    expect(detail.status).toBe('stopped');
    expect(detail.instances.every((vm) => vm.observed_status === 'stopped')).toBe(true);
    expect(mock.cycleIds()).toHaveLength(1);
  });

  it('a stop mid-rollout fails the rollout at its phase', async () => {
    const mock = await freshMock();
    mock.at(60);
    expect((await mock.post(`/apps/${mock.web.id}/stop`)).status).toBe(202);
    await later(mock);
    const [deployment] = await mock.deploymentsOf();
    expect(deployment).toMatchObject({ status: 'failed', phase: 'rootfs' });
    expect(mock.events('deployment.failed')).toHaveLength(1);
    expect((await mock.read<AppDetail>(`/apps/${mock.web.id}`)).status).toBe('stopped');
  });

  it('a delete in the gap removes the app and the cycle with it', async () => {
    const mock = await gap(3);
    const response = await fetch(`${BASE}/apps/${mock.web.id}`, { method: 'DELETE' });
    expect(response.status).toBe(202);
    await later(mock);
    expect((await fetch(`${BASE}/apps/${mock.web.id}`)).status).toBe(404);
    expect(mock.db.deployments.some((item) => item.app_id === mock.web.id)).toBe(false);
    expect(mock.db.deployCycle?.stopped).toBe(true);
  });

  it('stopping one instance mid-rollout leaves it stopped and fails the rollout', async () => {
    const mock = await freshMock();
    mock.at(100);
    const vm = (await mock.read<AppDetail>(`/apps/${mock.web.id}`)).instances[0]!;
    expect((await mock.post(`/vms/${vm.id}/stop`)).status).toBe(200);
    await later(mock);
    const detail = await mock.read<AppDetail>(`/apps/${mock.web.id}`);
    const stopped = detail.instances.find((item) => item.id === vm.id);
    expect(stopped).toMatchObject({ desired_status: 'stopped', observed_status: 'stopped' });
    expect((await mock.deploymentsOf())[0]?.status).toBe('failed');
    expect(mock.cycleIds()).toHaveLength(1);
  });

  it("a restart mid-rollout keeps the restart's own instance states", async () => {
    const mock = await freshMock();
    mock.at(60);
    expect((await mock.post(`/apps/${mock.web.id}/restart`)).status).toBe(202);
    const restarted = await mock.read<AppDetail>(`/apps/${mock.web.id}`);
    expect(restarted.instances.every((vm) => vm.observed_status === 'starting')).toBe(true);
    expect((await mock.deploymentsOf())[0]?.status).toBe('failed');
    await later(mock);
    const detail = await mock.read<AppDetail>(`/apps/${mock.web.id}`);
    expect(detail.status).toBe('running');
    expect(detail.instances.every((vm) => vm.observed_status === 'running')).toBe(true);
    expect(mock.cycleIds()).toHaveLength(1);
  });
});
