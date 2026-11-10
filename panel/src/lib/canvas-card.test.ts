// SPDX-License-Identifier: AGPL-3.0-only
import { describe, expect, it } from 'vitest';
import type { App, Backup, Build, Deployment, Vm, VmMetricSample, Volume } from '../api/generated/model';
import {
  appFootRows,
  cardFigures,
  cpuFigure,
  deployAge,
  deployProgress,
  footText,
  justDeployed,
  memoryFigure,
  memoryShare,
  nodeChips,
  vmFootRows,
} from './canvas-card';

const MIB = 1024 * 1024;
const NOW = Date.parse('2026-11-10T12:00:00Z');
const at = (secondsAgo: number) => new Date(NOW - secondsAgo * 1_000).toISOString();

const sample = (secondsAgo: number, cpu: number, rssMib: number, vcpus = 1, memMib = 512): VmMetricSample => ({
  time: at(secondsAgo),
  cpu_usage: cpu,
  vcpus,
  memory_rss_bytes: rssMib * MIB,
  memory_bytes: memMib * MIB,
});

describe('cardFigures', () => {
  it('is null while no instance has a sample, so the card shows a dash', () => {
    expect(cardFigures([])).toBeNull();
    expect(cardFigures([{ samples: [] }, { samples: [] }])).toBeNull();
  });

  it('weights each instance by its vCPUs and sums the newest memory of each', () => {
    const figures = cardFigures([
      { samples: [sample(10, 0.1, 180), sample(5, 0.2, 190)] },
      { samples: [sample(10, 0.4, 200, 3, 1024), sample(5, 0.6, 210, 3, 1024)] },
    ])!;
    // (0.2 * 1 + 0.6 * 3) / 4
    expect(figures.cpu).toBeCloseTo(0.5);
    expect(figures.memoryBytes).toBe(400 * MIB);
    expect(figures.memoryTotalBytes).toBe(1536 * MIB);
    expect(figures.cpuTrace.map((point) => Math.round(point.percent))).toEqual([33, 50]);
  });

  it('prints the figures as the card does', () => {
    expect(cpuFigure(0.214)).toBe('21%');
    expect(memoryFigure(386 * MIB)).toBe('386 MiB');
    expect(memoryFigure(1536 * MIB)).toBe('1.5 GiB');
    expect(memoryShare({ memoryBytes: 256 * MIB, memoryTotalBytes: 512 * MIB })).toBe(0.5);
    expect(memoryShare({ memoryBytes: 1, memoryTotalBytes: 0 })).toBe(0);
  });
});

const vm = (node: string, observed: Vm['observed_status']): Pick<Vm, 'node_id' | 'desired_status' | 'observed_status'> => ({
  node_id: node,
  desired_status: 'running',
  observed_status: observed,
});

describe('nodeChips', () => {
  it('names each node once, sorted, with its worst instance and no deleted one', () => {
    const chips = nodeChips(
      [vm('n4', 'running'), vm('n3', 'running'), vm('n3', 'crashed'), vm('n9', 'deleted')],
      { n3: 'lab-3', n4: 'lab-4', n9: 'lab-9' },
    );
    expect(chips.map((chip) => [chip.name, chip.tone])).toEqual([
      ['lab-3', 'fault'],
      ['lab-4', 'run'],
    ]);
  });

  it('leaves out a node the list does not name', () => {
    expect(nodeChips([vm('n1', 'running')], {})).toEqual([]);
  });
});

const deployment = (patch: Partial<Deployment>): Deployment => ({
  id: 'dep_1',
  app_id: 'app_1',
  image: 'ghcr.io/example/web:1',
  status: 'succeeded',
  phase: null,
  trigger: 'redeploy',
  created_by_type: 'user',
  created_by_id: 'usr_1',
  created_at: at(2_500),
  finished_at: at(2_400),
  ...patch,
});

describe('deployAge', () => {
  it('says how long the app has run on its current deployment', () => {
    const app = { status: 'running' as const, current_deployment_id: 'dep_1' };
    expect(deployAge(app, [deployment({})], NOW)).toBe('for 40 min');
  });

  it('says when a down app was deployed, and just now right after a deploy', () => {
    expect(deployAge({ status: 'down', current_deployment_id: 'dep_1' }, [deployment({})], NOW)).toBe('deployed 40 min ago');
    expect(
      deployAge({ status: 'running', current_deployment_id: 'dep_1' }, [deployment({ finished_at: at(5) })], NOW),
    ).toBe('just now');
  });

  it('falls back to the newest succeeded deployment, and says nothing without one', () => {
    const live = deployment({ id: 'dep_2', status: 'preparing', finished_at: null, created_at: at(20) });
    expect(deployAge({ status: 'deploying', current_deployment_id: null }, [live, deployment({})], NOW)).toBe('for 40 min');
    expect(deployAge({ status: 'deploying', current_deployment_id: null }, [live], NOW)).toBeNull();
  });
});

describe('deployProgress', () => {
  it('marks the phases before the current one done and the rest waiting', () => {
    const progress = deployProgress(
      deployment({ status: 'preparing', phase: 'rootfs', created_at: at(42), finished_at: null }),
      2,
      NOW,
    )!;
    expect(progress.word).toBe('Deploying · rootfs');
    expect(progress.sentence).toBe('Preparing the root disk');
    expect(progress.clock).toBe('0:42');
    expect(progress.steps.map((step) => step.state)).toEqual(['done', 'live', 'pending', 'pending']);
  });

  it('waits on every phase before the first one, and boots one or several microVMs', () => {
    const queued = deployProgress(deployment({ status: 'pending', phase: null, finished_at: null }), 1, NOW)!;
    expect(queued.word).toBe('Deploying · queued');
    expect(queued.steps.every((step) => step.state === 'pending')).toBe(true);
    const boot = (replicas: number) =>
      deployProgress(deployment({ status: 'starting', phase: 'boot', finished_at: null }), replicas, NOW)!.sentence;
    expect(boot(1)).toBe('Booting the microVM');
    expect(boot(2)).toBe('Booting the microVMs');
  });

  it('is null once the deployment has ended', () => {
    expect(deployProgress(deployment({ status: 'succeeded' }), 1, NOW)).toBeNull();
    expect(deployProgress(deployment({ status: 'failed', phase: 'boot' }), 1, NOW)).toBeNull();
    expect(deployProgress(undefined, 1, NOW)).toBeNull();
  });
});

describe('justDeployed', () => {
  it('confirms only a deployment seen rolling out that has now succeeded', () => {
    expect(justDeployed('dep_1', [deployment({ status: 'succeeded' })])).toBe(true);
    expect(justDeployed('dep_1', [deployment({ status: 'failed' })])).toBe(false);
    expect(justDeployed('dep_1', [deployment({ status: 'superseded' })])).toBe(false);
    expect(justDeployed(null, [deployment({ status: 'succeeded' })])).toBe(false);
  });
});

/* ---------- the footer ---------- */

const app = (patch: Partial<App>): App =>
  ({
    id: 'app_1',
    project_id: 'prj_1',
    name: 'worker',
    stateful: false,
    ha_policy: 'none',
    warm: null,
    backup_schedule: { interval_h: 6, keep: 7 },
    replicas: 1,
    image: 'ghcr.io/example/atlas-worker:1.8',
    port: 8080,
    status: 'running',
    current_deployment_id: null,
    ...patch,
  }) as App;

const gitSource = { kind: 'git', repo_url: 'https://github.com/example/web.git', ref: 'main' } as App['source'];
const build = (patch: Partial<Build>): Build =>
  ({ id: 'bld_1', status: 'succeeded', commit: '7d41e0a9f', source: { ref: 'main' }, ...patch }) as Build;
const volume = { state: 'ready', mount_path: '/var/lib/postgresql', size_gb: 20 } as Volume;
const backup = { status: 'succeeded', created_at: at(3_600), finished_at: at(3_600) } as Backup;
const names = { n3: 'lab-3', n4: 'lab-4' };
const rows = (input: Partial<Parameters<typeof appFootRows>[0]>) =>
  appFootRows({
    app: app({}),
    builds: undefined,
    volumes: undefined,
    backups: undefined,
    nodeIds: ['n4'],
    nodeNames: names,
    now: NOW,
    ...input,
  }).map((row) => [row.key, footText(row)]);

describe('appFootRows', () => {
  it('names the image an app runs when there is nothing else to say', () => {
    expect(rows({})).toEqual([['image', 'ghcr.io/example/atlas-worker:1.8']]);
  });

  it('keeps the nodes for a Git app with no build on the page', () => {
    expect(rows({ app: app({ source: gitSource }), builds: [], nodeIds: ['n4', 'n3'] })).toEqual([
      ['nodes', 'lab-3 · lab-4'],
    ]);
  });

  it('sets the commit first, with its sha as code', () => {
    const foot = appFootRows({
      app: app({ source: gitSource }),
      builds: [build({})],
      volumes: undefined,
      backups: undefined,
      nodeIds: [],
      nodeNames: names,
    });
    expect(foot.map((row) => row.key)).toEqual(['commit']);
    expect(foot[0]!.parts).toEqual([{ text: '7d41e0a', code: true }, { text: ' on main' }]);
  });

  it('says a failed build in red instead of the commit', () => {
    const foot = appFootRows({
      app: app({ source: gitSource }),
      builds: [build({ status: 'failed' })],
      volumes: undefined,
      backups: undefined,
      nodeIds: [],
      nodeNames: names,
    });
    expect(foot.map((row) => [row.key, footText(row), row.fault])).toEqual([['build', 'Build of 7d41e0a failed', true]]);
  });

  it('lets the volume and the next backup win over the image', () => {
    const cold = app({ stateful: true, ha_policy: 'cold' });
    expect(rows({ app: cold, volumes: [volume], backups: [backup] })).toEqual([
      ['volume', '/var/lib/postgresql · 20 GiB'],
      ['backup', 'Backup in 5 h'],
    ]);
  });

  it('lets the warm copy win over the image, in red when it failed', () => {
    const warm = app({
      ha_policy: 'warm',
      warm: { state: 'failed', node_id: 'n4', lag_s: null, error: 'no space left on device' } as App['warm'],
    });
    const foot = appFootRows({
      app: warm,
      builds: undefined,
      volumes: undefined,
      backups: undefined,
      nodeIds: ['n3'],
      nodeNames: names,
    });
    expect(foot.map((row) => [row.key, footText(row), row.fault])).toEqual([
      ['warm', 'Warm copy failed: no space left on device', true],
    ]);
  });

  it('lets a warm copy in sync win over the image too', () => {
    const warm = app({
      ha_policy: 'warm',
      warm: { state: 'in_sync', node_id: 'n4', lag_s: 4, error: null } as App['warm'],
    });
    expect(rows({ app: warm }).map(([key]) => key)).toEqual(['warm']);
  });
});

describe('vmFootRows', () => {
  it('says the node and the address, the address as code', () => {
    const foot = vmFootRows({ node_id: 'n3', ip: '10.0.1.40' }, names);
    expect(foot.map((row) => [row.key, footText(row)])).toEqual([['host', 'lab-3 · 10.0.1.40']]);
    expect(foot[0]!.parts[1]).toEqual({ text: '10.0.1.40', code: true });
  });

  it('keeps the node alone until the VM has an address', () => {
    expect(vmFootRows({ node_id: 'n3', ip: null }, names).map(footText)).toEqual(['lab-3']);
    expect(vmFootRows({ node_id: 'gone', ip: null }, names)).toEqual([]);
  });
});
