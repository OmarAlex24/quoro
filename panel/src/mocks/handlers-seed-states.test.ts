// SPDX-License-Identifier: AGPL-3.0-only
import { setupServer } from 'msw/node';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import type {
  App,
  AppDetail,
  AppList,
  Deployment,
  DeploymentList,
  DeploymentLogList,
  IncidentList,
} from '../api/generated/model';
import { agency, verifyAuditChain } from './agency';
import { CYCLE_GAP_S, CYCLE_SECONDS, db, mockId, now } from './db';
import { handlers } from './handlers';

/*
 * The seed's in-progress states, read through the handlers as the panel
 * reads them: blog down with its open incident, and Atlas's web in a redeploy
 * that walks its phases and starts again once it succeeded. The clock is
 * faked, so each read lands at a chosen moment of the cycle.
 */

const server = setupServer(...handlers);
const BASE = 'http://panel.test/api/v1';

/** When the seeded cycle's first deployment was created. */
let cycleStart = 0;

beforeAll(() => {
  (globalThis as unknown as { window: typeof globalThis }).window = globalThis;
  const first = db.deployCycle?.deploymentIds[0];
  cycleStart = Date.parse(db.deployments.find((item) => item.id === first)?.created_at ?? '');
  vi.useFakeTimers({ now: cycleStart + 14_000 });
  server.listen();
  db.owner = {
    id: mockId('usr'),
    email: 'owner@example.com',
    name: 'Owner',
    role: 'owner',
    disabled: false,
    created_at: now(),
  };
  db.sessionUserId = db.owner.id;
});

afterAll(() => {
  server.close();
  vi.useRealTimers();
});

const at = (seconds: number) => vi.setSystemTime(cycleStart + seconds * 1_000);

const read = async <T>(path: string): Promise<T> => {
  const response = await fetch(`${BASE}${path}`);
  expect(response.status).toBe(200);
  return (await response.json()) as T;
};

const appNamed = async (name: string, project: string): Promise<AppDetail> => {
  const projectId = db.projects.find((item) => item.slug === project)?.id;
  const { items } = await read<AppList>('/apps');
  const app = items.find((item: App) => item.name === name && item.project_id === projectId);
  if (!app) throw new Error(`the mock no longer seeds ${project}/${name}`);
  return read<AppDetail>(`/apps/${app.id}`);
};

const deploymentsOf = async (appId: string): Promise<Deployment[]> =>
  (await read<DeploymentList>(`/apps/${appId}/deployments`)).items;

const logsOf = async (deploymentId: string) =>
  (await read<DeploymentLogList>(`/deployments/${deploymentId}/logs`)).items;

const cycleRecords = () =>
  agency().audit.filter((record) => record.operation === 'POST /api/v1/apps/{app_id}/redeploy');

const eventsOf = (type: string) => agency().events.filter((event) => event.type === type);

describe('blog in the seed', () => {
  it('is down, as its open incident says', async () => {
    const blog = await appNamed('blog', 'blog');
    expect(blog.status).toBe('down');

    const { items } = await read<IncidentList>('/incidents?status=open');
    expect(items.find((incident) => incident.subject.id === blog.id)?.title).toBe('blog is down');
  });

  it('has no instance left in the edge rotation, one crashed after the restart', async () => {
    const blog = await appNamed('blog', 'blog');
    expect(blog.restart_generation).toBe(1);
    expect(blog.instances.map((vm) => [vm.name, vm.observed_status])).toEqual([
      ['blog-1', 'crashed'],
      ['blog-2', 'running'],
    ]);
    expect(blog.instances.every((vm) => vm.desired_status === 'running' && vm.restart_count === 1)).toBe(true);
    expect(blog.instances.find((vm) => vm.name === 'blog-1')?.last_exit_code).toBe(1);
    const backends = blog.edge_backends ?? [];
    expect(backends).toHaveLength(2);
    expect(backends.some((backend) => backend.in_rotation)).toBe(false);
    expect(backends.every((backend) => backend.reason === 'health_check' && backend.last_error)).toBe(true);
  });
});

describe("Atlas's web in the seed", () => {
  it('opens mid-redeploy, pulling, by the owner, with the last good deployment still current', async () => {
    at(14);
    const web = await appNamed('web', 'atlas');
    expect(web.status).toBe('deploying');

    const [live, previous] = await deploymentsOf(web.id);
    expect(live).toMatchObject({
      status: 'preparing',
      phase: 'pull',
      trigger: 'redeploy',
      created_by_type: 'user',
      created_by_id: db.owner?.id,
      finished_at: null,
      image: web.image,
    });
    expect(previous?.status).toBe('succeeded');
    expect(web.current_deployment_id).toBe(previous?.id);
    expect((await logsOf(live!.id)).map((line) => line.phase)).toEqual(['pull']);
  });

  it('walks pull, rootfs, boot and health forward, and its instances boot during boot', async () => {
    const web = await appNamed('web', 'atlas');
    const id = db.deployCycle?.deploymentIds[0];
    const seen: Array<[string, string | null | undefined]> = [];
    for (const seconds of [10, 25, 40, 50, 80, 95, 110]) {
      at(seconds);
      const detail = await read<AppDetail>(`/apps/${web.id}`);
      const deployment = (await deploymentsOf(web.id)).find((item) => item.id === id);
      seen.push([deployment?.status ?? '', deployment?.phase]);
      expect(detail.status).toBe('deploying');
      const booting = deployment?.phase === 'boot';
      expect(detail.instances.every((vm) => vm.observed_status === (booting ? 'starting' : 'running'))).toBe(true);
    }
    expect(seen).toEqual([
      ['preparing', 'pull'],
      ['preparing', 'rootfs'],
      ['preparing', 'rootfs'],
      ['starting', 'boot'],
      ['starting', 'boot'],
      ['starting', 'health'],
      ['starting', 'health'],
    ]);
    const lines = await logsOf(id!);
    expect(lines.map((line) => line.phase)).toEqual(['pull', 'rootfs', 'boot', 'health']);
    const times = lines.map((line) => Date.parse(line.time));
    expect(times).toEqual([...times].sort((left, right) => left - right));
  });

  it('succeeds after about two minutes, becomes current, and the next redeploy starts after the gap', async () => {
    const web = await appNamed('web', 'atlas');
    const first = db.deployCycle?.deploymentIds[0];

    at(CYCLE_SECONDS + 1);
    const done = await read<AppDetail>(`/apps/${web.id}`);
    expect(done.status).toBe('running');
    expect(done.current_deployment_id).toBe(first);
    const succeeded = (await deploymentsOf(web.id))[0];
    expect(succeeded).toMatchObject({ id: first, status: 'succeeded', phase: 'health' });
    expect(Date.parse(succeeded!.finished_at ?? '') - cycleStart).toBe(CYCLE_SECONDS * 1_000);
    expect(succeeded?.image_digest).toMatch(/^sha256:/);
    expect((await logsOf(first!)).at(-1)?.message).toBe('Health check passed; the app is running');

    at(CYCLE_SECONDS + CYCLE_GAP_S + 3);
    const again = await read<AppDetail>(`/apps/${web.id}`);
    expect(again.status).toBe('deploying');
    expect(again.current_deployment_id).toBe(first);
    const [next, previous] = await deploymentsOf(web.id);
    expect(next?.id).not.toBe(first);
    expect(next).toMatchObject({ status: 'preparing', phase: 'pull', trigger: 'redeploy' });
    expect(previous?.id).toBe(first);
  });

  it('puts each redeploy in the audit log and the events, and the chain still verifies', () => {
    const ids = db.deployCycle?.deploymentIds ?? [];
    expect(ids).toHaveLength(2);
    expect(cycleRecords().map((record) => record.time)).toEqual(
      ids.map((id) => db.deployments.find((item) => item.id === id)?.created_at),
    );
    expect(cycleRecords().every((record) => record.actor_id === db.owner?.id && record.scope === 'app:redeploy')).toBe(
      true,
    );
    expect(eventsOf('deployment.started').map((event) => event.data.deployment_id)).toEqual(ids);
    expect(eventsOf('deployment.succeeded').map((event) => event.data.deployment_id)).toEqual([ids[0]]);
    expect(verifyAuditChain().ok).toBe(true);
  });

  it('after a long pause starts only the redeploy that would run now', async () => {
    const web = await appNamed('web', 'atlas');
    at(3_600);
    await read<AppDetail>(`/apps/${web.id}`);
    const ids = db.deployCycle?.deploymentIds ?? [];
    expect(ids).toHaveLength(3);
    const [live] = await deploymentsOf(web.id);
    expect(live?.id).toBe(ids[2]);
    expect(live?.status === 'pending' || live?.status === 'preparing' || live?.status === 'starting').toBe(true);
    expect(3_600_000 - (Date.parse(live!.created_at) - cycleStart)).toBeLessThan((CYCLE_SECONDS + CYCLE_GAP_S) * 1_000);
    expect(eventsOf('deployment.succeeded')).toHaveLength(2);
    expect(verifyAuditChain().ok).toBe(true);
  });

  it('lets go when a person redeploys: theirs supersedes it and no other starts', async () => {
    const web = await appNamed('web', 'atlas');
    const response = await fetch(`${BASE}/apps/${web.id}/redeploy`, { method: 'POST' });
    expect(response.status).toBe(202);
    const theirs = (await response.json()) as Deployment;

    at(3_600 + 10 * (CYCLE_SECONDS + CYCLE_GAP_S));
    await vi.runAllTimersAsync();
    const list = await deploymentsOf(web.id);
    expect(list[0]?.id).toBe(theirs.id);
    expect(list.find((item) => item.id === db.deployCycle?.deploymentIds[2])?.status).toBe('superseded');
    expect(db.deployCycle?.stopped).toBe(true);
    expect(db.deployCycle?.deploymentIds).toHaveLength(3);
  });
});
