// SPDX-License-Identifier: AGPL-3.0-only
import { describe, expect, it } from 'vitest';
import type { App, ClusterCapacity, Event, Node, Vm } from '../api/generated/model';
import { n1Text } from './node-capacity';
import {
  changeLogWords,
  clusterFacts,
  groupedCount,
  mergeEvents,
  nodeEventRow,
  seenWords,
  simulateOnCards,
  simulationSentence,
  vmCells,
} from './nodes-page';

const lab3 = { id: 'nod_3', name: 'lab-3', allocated: { cpus: 8, memory_mb: 10_240, disk_gb: 48 } };
const lab4 = { id: 'nod_4', name: 'lab-4', allocated: { cpus: 5, memory_mb: 3_072, disk_gb: 10 } };
const nodes = [lab3, lab4];

const app = (id: string, name: string, extra: Partial<App> = {}): App =>
  ({
    id,
    name,
    source: null,
    status: 'running',
    resources: { vcpus: 1, memory_mb: 512, disk_gb: 2 },
    ...extra,
  }) as App;

const git = { kind: 'git', repo_url: 'https://example.com/r.git', ref: 'main' } as unknown as App['source'];
const apps = [
  app('app_web', 'web', { source: git, status: 'deploying' }),
  app('app_blog', 'blog', { status: 'down' }),
  app('app_docs', 'docs-site', { source: git }),
  app('app_pg', 'postgres', { resources: { vcpus: 2, memory_mb: 1_024, disk_gb: 20 } }),
];

const vm = (name: string, nodeId: string, appId: string | null, extra: Partial<Vm> = {}): Vm =>
  ({
    id: `vm_${name}`,
    name,
    node_id: nodeId,
    app_id: appId,
    vcpus: 1,
    observed_status: 'running',
    desired_status: 'running',
    ...extra,
  }) as Vm;

describe('clusterFacts', () => {
  const cluster = {
    primary_node_id: 'nod_3',
    term: 1,
    version: '0.3.0',
    votes: {
      voters: [
        { node_id: 'nod_3', roles: ['node'], connected: true },
        { node_id: 'nod_4', roles: ['node', 'edge'], connected: true },
      ],
      count: 2,
      required: 3,
      ha_ready: false,
    },
  } as Parameters<typeof clusterFacts>[0];

  it('names the primary and says how many voters are connected', () => {
    expect(clusterFacts(cluster, nodes)).toEqual([
      { label: 'Primary', value: 'lab-3' },
      { label: 'Term', value: '1' },
      { label: 'Voters', value: '2 of 2', spoken: '2 of 2 voters connected' },
      { label: 'Version', value: '0.3.0' },
    ]);
  });

  it('counts a voter that is not connected and leaves out a primary it cannot name', () => {
    const facts = clusterFacts(
      { ...cluster, primary_node_id: 'nod_gone', votes: { ...cluster.votes, voters: [cluster.votes.voters[0]!, { ...cluster.votes.voters[1]!, connected: false }] } },
      nodes,
    );
    expect(facts.map((fact) => fact.label)).toEqual(['Term', 'Voters', 'Version']);
    expect(facts[1]!.value).toBe('1 of 2');
  });
});

describe('a node card', () => {
  it('says when the node was last seen', () => {
    const now = Date.parse('2026-11-10T12:00:00Z');
    expect(seenWords('2026-11-10T11:59:58Z', now)).toBe('seen 2 s ago');
    expect(seenWords('2026-11-10T12:00:00Z', now)).toBe('seen just now');
    expect(seenWords('2026-11-10T11:57:00Z', now)).toBe('seen 3 min ago');
    expect(seenWords('2026-11-10T09:00:00Z', now)).toBe('seen 3 h ago');
    expect(seenWords(null, now)).toBeNull();
  });

  it('says where the node stands on the change log', () => {
    expect(changeLogWords(0)).toEqual({ words: 'Change log in sync', tone: 'run' });
    expect(changeLogWords(3)).toEqual({ words: 'Change log 3 entries behind', tone: 'shift' });
    expect(changeLogWords(null)).toBeNull();
    expect(changeLogWords(undefined)).toBeNull();
  });

  it('groups thousands', () => {
    expect(groupedCount(37)).toBe('37');
    expect(groupedCount(1482)).toBe('1 482');
  });
});

describe('vmCells', () => {
  const vms = [
    vm('web-1', 'nod_3', 'app_web'),
    vm('blog-1', 'nod_3', 'app_blog', { observed_status: 'crashed' }),
    vm('docs-site-1', 'nod_3', 'app_docs'),
    vm('ci-runner', 'nod_3', null, { vcpus: 4 }),
    vm('web-2', 'nod_4', 'app_web'),
    vm('old-1', 'nod_3', 'app_docs', { observed_status: 'deleted' }),
  ];

  it('groups the VMs by node, one cell each, as wide as its vCPUs and coloured by kind', () => {
    const cells = vmCells('nod_3', vms, apps);
    expect(cells.map((cell) => [cell.label, cell.kind, cell.units])).toEqual([
      ['web-1', 'git', 1],
      ['blog-1', 'image', 1],
      ['docs-site-1', 'git', 1],
      ['ci-runner', 'vm', 4],
    ]);
    expect(vmCells('nod_4', vms, apps).map((cell) => cell.label)).toEqual(['web-2']);
    expect(vmCells('nod_edge', vms, apps)).toEqual([]);
  });

  it('marks a crashed VM or a down app as a fault and a deploying app as a shift', () => {
    const cells = vmCells('nod_3', vms, apps);
    expect(cells.map((cell) => cell.tone)).toEqual(['shift', 'fault', null, null]);
    expect(cells[1]!.title).toBe('blog-1 · instance of blog · 1 vCPU · crashed · app down');
    expect(cells[3]!.title).toBe('ci-runner · standalone VM · 4 vCPU · running');
  });
});

describe('the simulated loss of a node', () => {
  const answer: ClusterCapacity = {
    lost_node_id: 'nod_3',
    fits: true,
    missing: { cpus: 0, memory_mb: 0, disk_gb: 0 },
    computed_at: '2026-11-10T12:00:00Z',
    apps: [
      { app_id: 'app_web', policy: 'none', action: 'keep', node_id: 'nod_4', reason: null },
      { app_id: 'app_blog', policy: 'none', action: 'restore', node_id: 'nod_4', reason: null },
      { app_id: 'app_docs', policy: 'none', action: 'start', node_id: 'nod_4', reason: null },
    ],
  };

  it('dims the lost node and puts the apps that move on the node that takes them, as ghosts', () => {
    const cards = simulateOnCards(nodes, answer, apps);
    expect(cards.nod_3).toEqual({ lost: true, ghosts: [], reserved: null });
    expect(cards.nod_4!.lost).toBe(false);
    expect(cards.nod_4!.ghosts.map((cell) => [cell.label, cell.kind, cell.units, cell.ghost])).toEqual([
      ['blog restore', 'image', 1, true],
      ['docs-site', 'git', 1, true],
    ]);
    expect(cards.nod_4!.ghosts[0]!.title).toBe('blog would be restored here from its latest backup');
  });

  it('raises the receiving node to what would be reserved there', () => {
    expect(simulateOnCards(nodes, answer, apps).nod_4!.reserved).toEqual({ cpus: 7, memory_mb: 4_096, disk_gb: 14 });
  });

  it('adds no disk for a warm failover, and projects nothing when an arriving app is unknown', () => {
    const failover: ClusterCapacity = {
      ...answer,
      apps: [{ app_id: 'app_pg', policy: 'warm', action: 'failover', node_id: 'nod_4', reason: null }],
    };
    expect(simulateOnCards(nodes, failover, apps).nod_4!.reserved).toEqual({ cpus: 7, memory_mb: 4_096, disk_gb: 10 });
    const unknown: ClusterCapacity = {
      ...answer,
      apps: [...answer.apps, { app_id: 'app_gone', policy: 'none', action: 'start', node_id: 'nod_4', reason: null }],
    };
    const card = simulateOnCards(nodes, unknown, apps).nod_4!;
    expect(card.reserved).toBeNull();
    expect(card.ghosts).toHaveLength(2);
    expect(simulateOnCards(nodes, null, apps)).toEqual({});
  });

  it('says what happens to each app, in placement order', () => {
    expect(n1Text({ segments: simulationSentence({ lost: lab3, answer, nodes, apps }) })).toBe(
      'Without lab-3: blog is restored on lab-4 from its latest backup and docs-site starts there. web keeps running on its other replica.',
    );
  });

  it('says which apps get no node, and when none has to move', () => {
    const short: ClusterCapacity = {
      ...answer,
      lost_node_id: 'nod_4',
      fits: false,
      apps: [
        { app_id: 'app_web', policy: 'none', action: 'keep', node_id: 'nod_3', reason: null },
        { app_id: 'app_blog', policy: 'none', action: 'keep', node_id: 'nod_3', reason: null },
        { app_id: 'app_pg', policy: 'none', action: 'restore', node_id: null, reason: 'no_capacity' },
        { app_id: 'app_docs', policy: 'none', action: 'restore', node_id: null, reason: 'no_backup' },
      ],
    };
    expect(n1Text({ segments: simulationSentence({ lost: lab4, answer: short, nodes, apps }) })).toBe(
      'Without lab-4: postgres finds no node with room and docs-site has no backup to restore. web and blog keep running on their other replicas.',
    );
    const crowded: ClusterCapacity = {
      ...short,
      apps: [
        { app_id: 'app_pg', policy: 'none', action: 'restore', node_id: null, reason: 'no_capacity' },
        { app_id: 'app_web', policy: 'none', action: 'start', node_id: null, reason: 'no_capacity' },
      ],
    };
    expect(n1Text({ segments: simulationSentence({ lost: lab4, answer: crowded, nodes, apps }) })).toBe(
      'Without lab-4: postgres and web find no node with room.',
    );
    expect(n1Text({ segments: simulationSentence({ lost: lab4, answer: { ...answer, apps: [] }, nodes, apps }) })).toBe(
      'Without lab-4: no app has to move.',
    );
  });
});

describe('node events', () => {
  const event = (seq: number, type: Event['type'], data: Record<string, unknown>): Event =>
    ({
      id: `evt_${seq}`,
      seq,
      type,
      time: '2026-11-10T12:00:00Z',
      source: { kind: 'control_plane' },
      subject: { type: 'node', id: String(data.node_id ?? 'nod_x') },
      data,
    }) as Event;

  it('keeps each event once, newest first, up to the limit', () => {
    const held = [event(5, 'node.connected', {}), event(3, 'node.enrolled', {})];
    const read = [event(5, 'node.connected', {}), event(6, 'node.disconnected', {}), event(7, 'node.connected', {})];
    expect(mergeEvents(held, read, 3).map((item) => item.seq)).toEqual([7, 6, 5]);
    expect(mergeEvents([], [], 3)).toEqual([]);
  });

  it('reads each node event type as a sentence with a lamp', () => {
    const text = (item: Event) => {
      const row = nodeEventRow(item, nodes);
      return `${row.tone}: ${n1Text(row)}`;
    };
    expect(text(event(1, 'node.enrolled', { node_id: 'nod_4', name: 'lab-4', roles: ['node'] }))).toBe(
      'run: lab-4 enrolled with a join token',
    );
    expect(text(event(2, 'node.connected', { node_id: 'nod_4' }))).toBe('run: lab-4 connected');
    expect(text(event(3, 'node.disconnected', { node_id: 'nod_4', reason: 'heartbeat_timeout' }))).toBe(
      'shift: lab-4 missed its heartbeats',
    );
    expect(text(event(4, 'node.disconnected', { node_id: 'nod_4', reason: 'session_closed' }))).toBe(
      'shift: lab-4 lost its session',
    );
    expect(text(event(5, 'node.fenced', { node_id: 'nod_3', app_ids: ['app_pg'] }))).toBe('shift: lab-3 fenced 1 stateful app');
    expect(text(event(6, 'node.fence_failed', { node_id: 'nod_3' }))).toBe('fault: lab-3 could not confirm a fence stopped its VMs');
    expect(text(event(7, 'node.disk_low', { node_id: 'nod_3', free_gb: 4 }))).toBe('fault: lab-3 is low on disk: 4 GiB free');
  });

  it('names a node it does not list from the payload, never by id', () => {
    expect(n1Text(nodeEventRow(event(1, 'node.enrolled', { node_id: 'nod_9', name: 'lab-9' }), nodes))).toBe(
      'lab-9 enrolled with a join token',
    );
    expect(n1Text(nodeEventRow(event(2, 'node.connected', { node_id: 'nod_9' }), nodes))).toBe('A node connected');
  });
});
