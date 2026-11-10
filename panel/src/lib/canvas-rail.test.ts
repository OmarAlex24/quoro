// SPDX-License-Identifier: AGPL-3.0-only
import { describe, expect, it } from 'vitest';
import type { App, Build, Deployment, Domain, Grant, Node, Vm } from '../api/generated/model';
import {
  certPhrase,
  deployBarWidth,
  deployRows,
  deployWhen,
  domainRows,
  placement,
  projectGrants,
  projectSummary,
  spanWords,
} from './canvas-rail';

const NOW = Date.parse('2026-11-10T12:00:00Z');
const at = (secondsAgo: number) => new Date(NOW - secondsAgo * 1_000).toISOString();
const ahead = (seconds: number) => new Date(NOW + seconds * 1_000).toISOString();

const vm = (id: string, node: string, app: string | null, vcpus = 1, observed: Vm['observed_status'] = 'running'): Vm =>
  ({
    id,
    name: id,
    node_id: node,
    app_id: app,
    vcpus,
    desired_status: 'running',
    observed_status: observed,
  }) as Vm;

const app = (id: string, name: string, patch: Partial<App> = {}): App =>
  ({ id, name, source: null, status: 'running', image: `docker.io/library/${name}:1`, ...patch }) as App;

const node = (id: string, name: string, vcpus: number): Node =>
  ({ id, name, status: 'connected', capacity: { cpus: vcpus, memory_mb: 0, disk_gb: 0 } }) as unknown as Node;

describe('projectSummary', () => {
  it('counts apps by status and standalone VMs by what their node reports', () => {
    const summary = projectSummary(
      [app('a1', 'web', { status: 'deploying' }), app('a2', 'api'), app('a3', 'blog', { status: 'down' })],
      [vm('w1', 'n3', 'a1'), vm('w2', 'n4', 'a1'), vm('p1', 'n4', 'a2'), vm('ci', 'n3', null, 4), vm('old', 'n9', 'a2', 1, 'deleted')],
    );
    expect(summary.states).toEqual([
      { tone: 'run', words: '2 running' },
      { tone: 'shift', words: '1 deploying' },
      { tone: 'fault', words: '1 down' },
    ]);
    expect(summary.instances).toBe('4 instances on 2 nodes');
  });

  it('says nothing about instances when there are none', () => {
    expect(projectSummary([], []).instances).toBeNull();
    expect(projectSummary([], [vm('ci', 'n3', null)]).instances).toBe('1 instance on 1 node');
  });
});

describe('placement', () => {
  const apps = [app('a1', 'web', { source: { kind: 'git' } as App['source'], status: 'deploying' }), app('a2', 'blog', { status: 'down' })];
  const nodes = [node('n3', 'lab-3', 12), node('n4', 'lab-4', 8)];
  const vms = [
    vm('web-2', 'n4', 'a1'),
    vm('web-1', 'n3', 'a1'),
    vm('ci-runner', 'n3', null, 4),
    vm('blog-1', 'n4', 'a2', 1, 'crashed'),
    vm('gone', 'n4', 'a2', 1, 'deleted'),
  ];

  it('groups the instances by node, sorted by name, with what they reserve there', () => {
    const groups = placement(vms, apps, nodes, 'Atlas');
    expect(groups.map((group) => [group.name, group.share, group.cells.map((cell) => cell.label)])).toEqual([
      ['lab-3', '5 of 12 vCPU to Atlas', ['web-1', 'ci-runner']],
      ['lab-4', '2 of 8 vCPU to Atlas', ['web-2', 'blog-1']],
    ]);
  });

  it('sizes a cell by its vCPUs and keys it to its card', () => {
    const [lab3] = placement(vms, apps, nodes, 'Atlas');
    expect(lab3!.cells.map((cell) => [cell.units, cell.kind, cell.service])).toEqual([
      [1, 'git', 'app:a1'],
      [4, 'vm', 'vm:ci-runner'],
    ]);
  });

  it('marks the cells of an app that deploys or is down', () => {
    const [lab3, lab4] = placement(vms, apps, nodes, 'Atlas');
    expect(lab3!.cells[0]!.tone).toBe('shift');
    expect(lab4!.cells[1]!.tone).toBe('fault');
  });

  it('keeps an unlisted node by its id', () => {
    const [group] = placement([vm('x', 'nod_X', null, 2)], [], [], 'Atlas');
    expect(group).toMatchObject({ name: 'nod_X', share: '2 vCPU to Atlas', tone: 'unknown' });
  });
});

const deployment = (id: string, appId: string, patch: Partial<Deployment>): Deployment => ({
  id,
  app_id: appId,
  image: 'ghcr.io/example/atlas-worker:1.8',
  status: 'succeeded',
  phase: null,
  trigger: 'api',
  created_by_type: 'system',
  created_by_id: 'system',
  created_at: at(86_400),
  finished_at: at(86_366),
  ...patch,
});

describe('deployRows', () => {
  const web = app('a1', 'web', { source: { kind: 'git', ref: 'main' } as App['source'] });
  const worker = app('a2', 'worker');
  const builds: Record<string, Build[]> = {
    a1: [
      {
        id: 'b1',
        app_id: 'a1',
        status: 'succeeded',
        commit: '7d41e0a77',
        source: { ref: 'main' },
        deployment_id: 'd1',
        created_at: at(2_507),
        started_at: at(2_503),
        finished_at: at(2_440),
      } as Build,
    ],
  };
  const deployments: Record<string, Deployment[]> = {
    a1: [
      deployment('d2', 'a1', { trigger: 'redeploy', status: 'preparing', phase: 'pull', created_by_type: 'user', created_by_id: 'usr_1', created_at: at(10), finished_at: null }),
      deployment('d1', 'a1', { trigger: 'build', created_at: at(2_440), finished_at: at(2_400) }),
    ],
    a2: [deployment('d3', 'a2', {})],
  };

  it('lists every app’s runs newest first, a build with its sha and a redeploy with who started it', () => {
    const rows = deployRows([web, worker], builds, deployments, NOW);
    expect(rows.map((row) => [row.name, row.title, row.actor?.id ?? null, row.seconds])).toEqual([
      ['web', { text: 'Redeploy' }, 'usr_1', null],
      ['web', { text: 'Build ', code: '7d41e0a' }, null, 107],
      ['worker', { text: 'atlas-worker:1.8' }, null, 34],
    ]);
  });

  it('keeps the newest few and leaves out an app whose deployments are not read', () => {
    expect(deployRows([web, worker], builds, { a2: deployments.a2! }, NOW).map((row) => row.name)).toEqual(['worker']);
    expect(deployRows([web, worker], builds, deployments, NOW, 1)).toHaveLength(1);
  });

  it('says when and how long, and sizes the bar against the longest run', () => {
    expect(spanWords(34)).toBe('34 s');
    expect(spanWords(107)).toBe('1 m 47 s');
    expect(spanWords(3_720)).toBe('1 h 02 m');
    expect(deployWhen({ start: NOW - 2_507_000, seconds: 107 }, NOW)).toBe('42 min ago · 1 m 47 s');
    expect(deployWhen({ start: NOW - 10_000, seconds: null }, NOW)).toBe('just now');
    expect(deployBarWidth(107, 107)).toBe(54);
    expect(deployBarWidth(34, 107)).toBe(17);
    expect(deployBarWidth(1, 107)).toBe(6);
    expect(deployBarWidth(null, 107)).toBe(6);
  });
});

const grant = (patch: Partial<Grant>): Grant =>
  ({
    id: 'grt_1',
    agent_id: 'agt_1',
    kind: 'standard',
    scopes: ['app:read', 'app:restart'],
    selector: { projects: ['atlas'] },
    status: 'active',
    created_at: at(86_400),
    expires_at: ahead(6 * 86_400),
    ...patch,
  }) as Grant;

describe('projectGrants', () => {
  const apps = [app('a1', 'web'), app('a2', 'api')];

  it('keeps grants on the project, on every resource, or on its apps', () => {
    const rows = projectGrants(
      [
        grant({ id: 'g1', agent_id: 'agt_1' }),
        grant({ id: 'g2', agent_id: 'agt_2', selector: { all: true }, expires_at: ahead(3_600) }),
        grant({ id: 'g3', agent_id: 'agt_3', selector: { apps: ['a2', 'a1'] } }),
      ],
      { slug: 'atlas' },
      apps,
      NOW,
    );
    expect(rows.map((row) => [row.agentId, row.phrase, row.where])).toEqual([
      ['agt_2', 'read and restart apps', 'here'],
      ['agt_1', 'read and restart apps', 'here'],
      ['agt_3', 'read and restart apps', 'on api, web'],
    ]);
  });

  it('leaves out other projects, other apps, nodes, expired grants and scopes that do nothing here', () => {
    const rows = projectGrants(
      [
        grant({ selector: { projects: ['blog'] } }),
        grant({ selector: { apps: ['app_elsewhere'] } }),
        grant({ selector: { nodes: ['nod_1'] } }),
        grant({ status: 'revoked' }),
        grant({ expires_at: at(1) }),
        grant({ scopes: ['audit:read'] }),
      ],
      { slug: 'atlas' },
      apps,
      NOW,
    );
    expect(rows).toEqual([]);
  });

  it('gives one row per agent, its incident grant first, and counts the rest', () => {
    const rows = projectGrants(
      [
        grant({ id: 'g1' }),
        grant({ id: 'g2', kind: 'incident', selector: { apps: ['a1'] }, expires_at: ahead(2_700) }),
      ],
      { slug: 'atlas' },
      apps,
      NOW,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ more: 1, where: 'on web' });
    expect(rows[0]!.grant.id).toBe('g2');
  });
});

const domain = (patch: Partial<Domain>): Domain =>
  ({
    id: 'dom_1',
    hostname: 'atlas.example.com',
    tls_mode: 'tunnel',
    cert_status: 'not_applicable',
    cert_expires_at: null,
    serving_status: 'serving',
    ...patch,
  }) as Domain;

describe('domains', () => {
  it('says whether a machine serves the hostname and who holds its certificate', () => {
    expect(domainRows([domain({})], NOW)).toEqual([
      { id: 'dom_1', hostname: 'atlas.example.com', tone: 'run', words: 'serving · TLS by Cloudflare Tunnel' },
    ]);
    expect(domainRows([domain({ serving_status: 'unserved' })], NOW)[0]).toMatchObject({
      tone: 'fault',
      words: 'not served · TLS by Cloudflare Tunnel',
    });
  });

  it('says when an ACME certificate expires', () => {
    const acme = (patch: Partial<Domain>) => certPhrase(domain({ tls_mode: 'acme', ...patch }), NOW);
    expect(acme({ cert_status: 'issued', cert_expires_at: ahead(62 * 86_400 + 600) })).toBe('certificate expires in 62 d');
    expect(acme({ cert_status: 'issued', cert_expires_at: ahead(5 * 3_600) })).toBe('certificate expires in 5 h');
    expect(acme({ cert_status: 'issued', cert_expires_at: at(60) })).toBe('certificate expired');
    expect(acme({ cert_status: 'pending' })).toBe('certificate pending');
    expect(acme({ cert_status: 'failed' })).toBe('certificate failed');
  });

  it('orders the domains by hostname', () => {
    const rows = domainRows([domain({ id: 'b', hostname: 'www.example.com' }), domain({ id: 'a', hostname: 'api.example.com' })], NOW);
    expect(rows.map((row) => row.hostname)).toEqual(['api.example.com', 'www.example.com']);
  });
});
