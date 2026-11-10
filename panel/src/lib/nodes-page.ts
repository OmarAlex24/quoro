// SPDX-License-Identifier: AGPL-3.0-only
import type {
  App,
  Cluster,
  ClusterCapacity,
  ClusterCapacityApp,
  Event,
  EventType,
  Node,
  NodeCapacity,
  Vm,
} from '../api/generated/model';
import type { LampTone } from './lamp';
import type { Segment } from './node-capacity';
import { appKind } from './services';

/*
 * The Nodes page as pure data from the contract: the cluster band's facts
 * (`getCluster`), each node's VMs as a strip of cells (`listVms` grouped by
 * `node_id`), the simulated loss of a node drawn on the cards
 * (`getClusterCapacity`), and the node events (`listEvents` by type).
 */

/* ---------- the cluster band ---------- */

export interface ClusterFact {
  label: string;
  value: string;
  /** What a screen reader hears for the fact, when the value alone says too little. */
  spoken?: string;
}

/**
 * Primary, term, voters and version, from `GET /cluster`. Voters says how
 * many of the voting machines are connected. The primary is named when it is
 * listed; otherwise the fact is left out rather than shown as an id.
 */
export const clusterFacts = (
  cluster: Pick<Cluster, 'primary_node_id' | 'term' | 'votes' | 'version'>,
  nodes: ReadonlyArray<Pick<Node, 'id' | 'name'>>,
): ClusterFact[] => {
  const primary = nodes.find((node) => node.id === cluster.primary_node_id);
  const connected = cluster.votes.voters.filter((voter) => voter.connected).length;
  const voters = cluster.votes.voters.length;
  return [
    ...(primary ? [{ label: 'Primary', value: primary.name }] : []),
    { label: 'Term', value: String(cluster.term) },
    {
      label: 'Voters',
      value: `${connected} of ${voters}`,
      spoken: `${connected} of ${voters} ${voters === 1 ? 'voter' : 'voters'} connected`,
    },
    { label: 'Version', value: cluster.version },
  ];
};

/* ---------- a node's card ---------- */

/** `seen 2 s ago`, `seen 3 min ago`; null without a time. */
export const seenWords = (lastSeen: string | null | undefined, now: number): string | null => {
  if (!lastSeen) return null;
  const then = Date.parse(lastSeen);
  if (Number.isNaN(then)) return null;
  const seconds = Math.max(0, Math.floor((now - then) / 1_000));
  if (seconds < 1) return 'seen just now';
  if (seconds < 60) return `seen ${seconds} s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `seen ${minutes} min ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `seen ${hours} h ago`;
  return `seen ${Math.floor(hours / 24)} d ago`;
};

/** `Change log in sync`, `Change log 3 entries behind`; null for a machine that follows no log. */
export const changeLogWords = (lag: Node['log_lag']): { words: string; tone: LampTone } | null => {
  if (lag === null || lag === undefined) return null;
  if (lag === 0) return { words: 'Change log in sync', tone: 'run' };
  return { words: `Change log ${lag} ${lag === 1 ? 'entry' : 'entries'} behind`, tone: 'shift' };
};

/** `1 482`: a count with a thin space between thousands, as the proposal sets it. */
export const groupedCount = (value: number): string =>
  String(value).replace(/\B(?=(\d{3})+(?!\d))/g, ' ');

export type CellKind = 'git' | 'image' | 'vm';

/** One VM in a node's strip, or an app that would arrive there in a simulated loss. */
export interface VmCell {
  key: string;
  /** The words in the cell: the VM's name, or the arriving app's. */
  label: string;
  kind: CellKind;
  /** The cell's width in vCPUs. */
  units: number;
  /** A state worth a lamp: the VM is not running, or its app is down or deploying. Null when all is well. */
  tone: Extract<LampTone, 'shift' | 'fault'> | null;
  /** The cell's description: what it is and what state it is in. */
  title: string;
  ghost: boolean;
}

const VM_STATE_WORDS: Partial<Record<Vm['observed_status'], string>> = {
  creating: 'creating',
  starting: 'starting',
  stopping: 'stopping',
  stopped: 'stopped',
  crashed: 'crashed',
  unknown: 'state unknown',
};

/**
 * The VMs placed on a node, oldest first, one cell each: as wide as its
 * vCPUs, coloured by what it is (a Git app's instance, an image app's, or a
 * standalone VM) and marked when it or its app is not well. A deleted VM is
 * not on the node.
 */
export const vmCells = (
  nodeId: string,
  vms: readonly Vm[],
  apps: ReadonlyArray<Pick<App, 'id' | 'name' | 'source' | 'status'>>,
): VmCell[] =>
  vms
    .filter((vm) => vm.node_id === nodeId && vm.observed_status !== 'deleted')
    .map((vm) => {
      const app = vm.app_id === null ? undefined : apps.find((candidate) => candidate.id === vm.app_id);
      const vmState = VM_STATE_WORDS[vm.observed_status];
      const faulty = vm.observed_status === 'crashed' || app?.status === 'down';
      const busy =
        vm.observed_status === 'creating' ||
        vm.observed_status === 'starting' ||
        vm.observed_status === 'stopping' ||
        vm.observed_status === 'unknown' ||
        app?.status === 'deploying' ||
        app?.status === 'degraded';
      const appState =
        app?.status === 'down' ? 'app down' : app?.status === 'deploying' ? 'app deploying' : app?.status === 'degraded' ? 'app degraded' : null;
      const parts = [
        vm.name,
        app ? `instance of ${app.name}` : 'standalone VM',
        `${vm.vcpus} vCPU`,
        vmState ?? 'running',
        ...(appState ? [appState] : []),
      ];
      return {
        key: vm.id,
        label: vm.name,
        kind: app ? appKind(app) : 'vm',
        units: Math.max(1, vm.vcpus),
        tone: faulty ? 'fault' : busy ? 'shift' : null,
        title: parts.join(' · '),
        ghost: false,
      };
    });

/* ---------- the simulated loss of a node ---------- */

const MOVE_WORDS: Record<Exclude<ClusterCapacityApp['action'], 'keep'>, { cell: string; title: string }> = {
  start: { cell: '', title: 'would start here' },
  failover: { cell: ' failover', title: 'would start here from its warm copy' },
  restore: { cell: ' restore', title: 'would be restored here from its latest backup' },
};

export interface SimulatedNode {
  /** This is the node whose loss is simulated. */
  lost: boolean;
  /** The apps that would arrive here, as ghost cells, in placement order. */
  ghosts: VmCell[];
  /**
   * What would be reserved here once they arrived: `allocated` plus each
   * arriving app's resources (a warm failover adds no disk: the warm node
   * holds it already). Null when nothing arrives, or when an arriving app is
   * not in the app list, so its size is unknown: then nothing is projected.
   */
  reserved: NodeCapacity | null;
}

const moving = (app: ClusterCapacityApp): app is ClusterCapacityApp & { node_id: string; action: 'start' | 'failover' | 'restore' } =>
  app.node_id !== null && app.action !== 'keep';

/** The order moves are told and drawn in: what starts, then warm copies, then restores. */
const MOVE_ORDER = { start: 0, failover: 1, restore: 2 } as const;
const moves = (answer: ClusterCapacity) =>
  answer.apps.filter(moving).sort((a, b) => MOVE_ORDER[a.action] - MOVE_ORDER[b.action]);

/**
 * The primary's answer for a lost node, per card: the lost node dims, and
 * every node an app would move to shows it as a ghost cell and the
 * reservation it would reach. Empty without an answer.
 */
export const simulateOnCards = (
  nodes: ReadonlyArray<Pick<Node, 'id' | 'allocated'>>,
  answer: ClusterCapacity | null | undefined,
  apps: ReadonlyArray<Pick<App, 'id' | 'name' | 'source' | 'resources'>>,
): Record<string, SimulatedNode> => {
  if (!answer) return {};
  const result: Record<string, SimulatedNode> = {};
  for (const node of nodes) {
    const arriving = moves(answer).filter((app) => app.node_id === node.id);
    const known = arriving.map((entry) => ({ entry, app: apps.find((app) => app.id === entry.app_id) }));
    const ghosts: VmCell[] = known.flatMap(({ entry, app }) => {
      if (!app) return [];
      const words = MOVE_WORDS[entry.action];
      return [
        {
          key: `ghost:${app.id}`,
          label: `${app.name}${words.cell}`,
          kind: appKind(app),
          units: Math.max(1, app.resources.vcpus),
          tone: null,
          title: `${app.name} ${words.title}`,
          ghost: true,
        },
      ];
    });
    const allKnown = known.every(({ app }) => app !== undefined);
    const reserved =
      arriving.length > 0 && allKnown
        ? known.reduce<NodeCapacity>(
            (sum, { entry, app }) => ({
              cpus: sum.cpus + app!.resources.vcpus,
              memory_mb: sum.memory_mb + app!.resources.memory_mb,
              disk_gb: sum.disk_gb + (entry.action === 'failover' ? 0 : app!.resources.disk_gb),
            }),
            { ...node.allocated },
          )
        : null;
    result[node.id] = { lost: node.id === answer.lost_node_id, ghosts, reserved };
  }
  return result;
};

const listWords = (items: readonly string[]): string =>
  items.length <= 1 ? (items[0] ?? '') : `${items.slice(0, -1).join(', ')} and ${items[items.length - 1]}`;

/**
 * The band's sentence while a loss is simulated: what happens to each app.
 * `Without lab-3: docs-site starts on lab-4 and blog is restored there from
 * its latest backup. web and api keep one replica each.` The kept apps say
 * "one replica" only when the VM list shows each has exactly one instance
 * left off the lost node; otherwise "their other replicas". An app or node
 * missing from the lists is named `an app` or `another node`, never by id.
 */
export const simulationSentence = ({
  lost,
  answer,
  nodes,
  apps,
  vms = [],
}: {
  lost: Pick<Node, 'name'>;
  answer: ClusterCapacity;
  nodes: ReadonlyArray<Pick<Node, 'id' | 'name'>>;
  apps: ReadonlyArray<Pick<App, 'id' | 'name'>>;
  vms?: ReadonlyArray<Pick<Vm, 'app_id' | 'node_id' | 'observed_status'>>;
}): Segment[] => {
  const appName = (id: string) => apps.find((app) => app.id === id)?.name ?? 'an app';
  const nodeName = (id: string) => nodes.find((node) => node.id === id)?.name ?? 'another node';

  const clauses: Segment[][] = [];
  let lastTarget: string | null = null;
  for (const entry of moves(answer)) {
    const where = entry.node_id === lastTarget ? 'there' : `on ${nodeName(entry.node_id)}`;
    lastTarget = entry.node_id;
    const name = { strong: appName(entry.app_id) };
    if (entry.action === 'start') clauses.push([name, ` starts ${where}`]);
    else if (entry.action === 'failover') clauses.push([name, `’s warm copy starts ${where}`]);
    else clauses.push([name, ` is restored ${where} from its latest backup`]);
  }
  // The apps that get no node, grouped by why: `postgres and worker find no node with room`.
  const stranded = answer.apps.filter((app) => app.node_id === null);
  const reasons = [
    { reason: 'no_capacity', one: ' finds no node with room', many: ' find no node with room' },
    { reason: 'no_backup', one: ' has no backup to restore', many: ' have no backup to restore' },
    { reason: 'no_warm_copy', one: ' has no complete warm copy', many: ' have no complete warm copy' },
  ] as const;
  for (const { reason, one, many } of reasons) {
    const names = stranded
      .filter((app) => (app.reason ?? 'no_capacity') === reason)
      .map((app) => appName(app.app_id));
    if (names.length === 0) continue;
    const named: Segment[] = [];
    names.forEach((item, index) => {
      if (index > 0) named.push(index === names.length - 1 ? ' and ' : ', ');
      named.push({ strong: item });
    });
    clauses.push([...named, names.length === 1 ? one : many]);
  }

  const segments: Segment[] = ['Without ', { strong: lost.name }, ': '];
  if (clauses.length === 0) segments.push('no app has to move');
  clauses.forEach((clause, index) => {
    if (index > 0) segments.push(index === clauses.length - 1 ? ' and ' : ', ');
    segments.push(...clause);
  });
  segments.push('.');

  const kept = answer.apps.filter((app) => app.action === 'keep' && app.node_id !== null);
  if (kept.length > 0) {
    const one = kept.length === 1;
    const left = (appId: string) =>
      vms.filter((vm) => vm.app_id === appId && vm.node_id !== answer.lost_node_id && vm.observed_status !== 'deleted')
        .length;
    const single = kept.every((app) => left(app.app_id) === 1);
    const names = listWords(kept.map((app) => appName(app.app_id)));
    segments.push(
      single
        ? ` ${names} ${one ? 'keeps one replica' : 'keep one replica each'}.`
        : one
          ? ` ${names} keeps running on its other ${left(kept[0]!.app_id) > 1 ? 'replicas' : 'replica'}.`
          : ` ${names} keep running on their other replicas.`,
    );
  }
  return segments;
};

/* ---------- node events ---------- */

/** The node event types the contract lists (openapi.yaml `EventType`). */
export const NODE_EVENT_TYPES: EventType[] = [
  'node.enrolled',
  'node.connected',
  'node.disconnected',
  'node.fenced',
  'node.fence_failed',
  'node.disk_low',
];

/** Rows the Node events card shows. */
export const NODE_EVENT_ROWS = 5;

/**
 * Fold a newly read page into the events already held: each `seq` once,
 * newest first, at most `keep`. The log is read in ascending `seq`, so the
 * page after the last held one is what is new.
 */
export const mergeEvents = (held: readonly Event[], read: readonly Event[], keep: number): Event[] => {
  const bySeq = new Map<number, Event>();
  for (const event of [...held, ...read]) bySeq.set(event.seq, event);
  return [...bySeq.values()].sort((left, right) => right.seq - left.seq).slice(0, keep);
};

export interface NodeEventRow {
  seq: number;
  time: string;
  tone: LampTone;
  segments: Segment[];
}

const field = (data: Event['data'], name: string): unknown => (data as Record<string, unknown>)[name];
const plural = (count: number, one: string, many = `${one}s`) => `${count} ${count === 1 ? one : many}`;

/**
 * One node event as a sentence with a lamp: `lab-4 enrolled with a join
 * token`, `lab-4 missed its heartbeats`. The node is named from the list,
 * then from the event's own payload; never by id.
 */
export const nodeEventRow = (event: Event, nodes: ReadonlyArray<Pick<Node, 'id' | 'name'>>): NodeEventRow => {
  const nodeId = (field(event.data, 'node_id') as string | undefined) ?? event.subject.id;
  const listed = nodes.find((node) => node.id === nodeId)?.name;
  const named = field(event.data, 'name');
  const name = { strong: listed ?? (typeof named === 'string' ? named : 'A node') };
  const row = (tone: LampTone, ...rest: Segment[]): NodeEventRow => ({
    seq: event.seq,
    time: event.time,
    tone,
    segments: [name, ...rest],
  });
  switch (event.type) {
    case 'node.enrolled':
      return row('run', ' enrolled with a join token');
    case 'node.connected':
      return row('run', ' connected');
    case 'node.disconnected':
      return field(event.data, 'reason') === 'heartbeat_timeout'
        ? row('shift', ' missed its heartbeats')
        : row('shift', ' lost its session');
    case 'node.fenced': {
      const apps = field(event.data, 'app_ids');
      const count = Array.isArray(apps) ? apps.length : 0;
      return row('shift', count > 0 ? ` fenced ${plural(count, 'stateful app')}` : ' fenced its stateful apps');
    }
    case 'node.fence_failed':
      return row('fault', ' could not confirm a fence stopped its VMs');
    case 'node.disk_low': {
      const free = field(event.data, 'free_gb');
      return row('fault', typeof free === 'number' ? ` is low on disk: ${free} GiB free` : ' is low on disk');
    }
    default:
      return row('idle', ` ${event.type}`);
  }
};
