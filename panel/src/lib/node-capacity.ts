// SPDX-License-Identifier: AGPL-3.0-only
import type {
  App,
  ClusterCapacity,
  ClusterCapacityApp,
  Node,
  Vm,
} from '../api/generated/model';
import type { MeterTone } from './dither-draw';

/*
 * The node cards and the N+1 strip, as pure data from the contract: a node's
 * `capacity` and the part `allocated` to its VMs (openapi.yaml `NodeCapacity`),
 * the VMs placed on it, and the primary's answer to "if this node is lost, do
 * its apps fit on the others?" (`getClusterCapacity`, ADR 0021, ADR 0178).
 * Nothing here guesses: the verdict is the primary's, and a number is only
 * said when the fields it comes from are there.
 */

/* ---------- meters ---------- */

/** A meter at or past this share is drawn in the fault colour. */
export const METER_FAULT_AT = 0.9;

/** How full `used` is of `total`, 0 and up (above 1 when overcommitted); null without a total. */
export const capacityShare = (used: number, total: number): number | null =>
  Number.isFinite(used) && Number.isFinite(total) && total > 0 ? Math.max(0, used / total) : null;

/** The tone a meter takes: the fault colour once it is nearly full. */
export const meterTone = (share: number | null): MeterTone =>
  share !== null && share >= METER_FAULT_AT ? 'fault' : 'accent';

/** Mebibytes in gibibytes, as few digits as say it: `0.5`, `3.3`, `24`. */
export const gibWords = (mb: number): string => {
  const gib = mb / 1024;
  if (gib >= 10) return `${Math.round(gib)}`;
  const tenths = Math.round(gib * 10) / 10;
  return Number.isInteger(tenths) ? `${tenths}` : tenths.toFixed(1);
};

const plural = (count: number, one: string, many = `${one}s`): string =>
  `${count} ${count === 1 ? one : many}`;

export type MeterKey = 'cpu' | 'memory' | 'disk';

export interface NodeMeter {
  key: MeterKey;
  label: string;
  /** Reserved over capacity; null when the node reports no capacity. */
  share: number | null;
  /** The reading beside the label: `8 of 12 reserved`, or `2 CPUs reserved` without a capacity. */
  words: string;
  /**
   * Said in place of the meter when the node reports no capacity (a missing
   * or zero figure): a share of nothing is unknown, not empty.
   */
  unknown: string | null;
  /** The meter's spoken name. */
  spoken: string;
}

/**
 * A node's CPU, memory and disk as reserved against capacity: what the VMs
 * placed on it were given (`allocated`), out of what it has (`capacity`).
 * A capacity of 0 or none is not drawn as a meter: the words say what is
 * reserved and that the node reports no capacity.
 */
export const nodeMeters = (node: Pick<Node, 'capacity' | 'allocated'>): NodeMeter[] => {
  const { capacity, allocated } = node;
  const meter = (
    key: MeterKey,
    label: string,
    noun: string,
    used: number,
    total: number,
    words: { of: string; alone: string },
  ): NodeMeter => {
    const share = capacityShare(used, total);
    if (share === null) {
      return {
        key,
        label,
        share,
        words: words.alone,
        unknown: `The node reports no ${noun} capacity`,
        spoken: `${label}: ${words.alone}, no capacity reported`,
      };
    }
    return {
      key,
      label,
      share,
      words: words.of,
      unknown: null,
      spoken: `${label}: ${Math.round(share * 100)}% reserved, ${words.of}`,
    };
  };
  return [
    meter('cpu', 'CPU', 'CPU', allocated.cpus, capacity.cpus, {
      of: `${allocated.cpus} of ${capacity.cpus} reserved`,
      alone: `${plural(allocated.cpus, 'CPU')} reserved`,
    }),
    meter('memory', 'Memory', 'memory', allocated.memory_mb, capacity.memory_mb, {
      of: `${gibWords(allocated.memory_mb)} of ${gibWords(capacity.memory_mb)} GiB reserved`,
      alone: `${gibWords(allocated.memory_mb)} GiB reserved`,
    }),
    meter('disk', 'Disk', 'disk', allocated.disk_gb, capacity.disk_gb, {
      of: `${allocated.disk_gb} of ${capacity.disk_gb} GiB reserved`,
      alone: `${allocated.disk_gb} GiB reserved`,
    }),
  ];
};

/* ---------- what a node runs ---------- */

export interface NodeLoad {
  /** VMs placed on the node, app instances and standalone ones. */
  vms: number;
  /** Apps with an instance on the node. */
  apps: number;
}

/** How many VMs a node holds, and how many apps they are instances of. */
export const nodeLoad = (
  nodeId: string,
  vms: ReadonlyArray<Pick<Vm, 'node_id' | 'app_id' | 'observed_status'>>,
): NodeLoad => {
  const here = vms.filter((vm) => vm.node_id === nodeId && vm.observed_status !== 'deleted');
  const apps = new Set(here.map((vm) => vm.app_id).filter((id): id is string => id !== null));
  return { vms: here.length, apps: apps.size };
};

/** `4 VMs · 3 apps`, `1 VM`, `No VMs`. */
export const loadWords = ({ vms, apps }: NodeLoad): string => {
  if (vms === 0) return 'No VMs';
  return apps === 0 ? plural(vms, 'VM') : `${plural(vms, 'VM')} · ${plural(apps, 'app')}`;
};

/**
 * The words tagged on a node: `primary` or `replica` when its control-plane
 * status says so (null says neither), then its roles.
 */
export const nodeTags = (node: Pick<Node, 'cp_status' | 'roles'>): string[] => [
  ...(node.cp_status ? [node.cp_status] : []),
  ...node.roles,
];

/* ---------- the N+1 strip ---------- */

/**
 * The nodes whose loss the indicator can simulate: enrolled, not removed, with
 * the `node` role. The primary places apps only on nodes with that role and
 * answers 404 for a removed one.
 */
export const lossCandidates = <N extends Pick<Node, 'status' | 'roles'>>(nodes: readonly N[]): N[] =>
  nodes.filter((node) => node.status !== 'removed' && node.roles.includes('node'));

/**
 * The cluster's biggest node: of the candidates, the one carrying the most,
 * by reserved memory, then vCPUs, then disk; a tie goes to the name that sorts
 * first. Losing it is the worst case the strip answers first.
 */
export const biggestNode = <N extends Pick<Node, 'status' | 'roles' | 'allocated' | 'name'>>(
  nodes: readonly N[],
): N | undefined =>
  lossCandidates(nodes)
    .slice()
    .sort(
      (left, right) =>
        right.allocated.memory_mb - left.allocated.memory_mb ||
        right.allocated.cpus - left.allocated.cpus ||
        right.allocated.disk_gb - left.allocated.disk_gb ||
        left.name.localeCompare(right.name),
    )[0];

/** A run of the strip's sentence: plain text, or a node name set in bold. */
export type Segment = string | { strong: string };

export interface N1Readout {
  /** `run` when everything fits, `shift` when it fits but an app has nothing to start from, `fault` when it does not fit. */
  tone: 'run' | 'shift' | 'fault';
  segments: Segment[];
}

/** The sentence as plain text, for tests and for a spoken label. */
export const n1Text = (readout: Pick<N1Readout, 'segments'>): string =>
  readout.segments.map((segment) => (typeof segment === 'string' ? segment : segment.strong)).join('');

const apps = (count: number) => plural(count, 'app');

/** `a`, `a and b`, `a, b and c`. */
const listWords = (items: readonly string[]): string =>
  items.length <= 1 ? (items[0] ?? '') : `${items.slice(0, -1).join(', ')} and ${items[items.length - 1]}`;

/** The resources missing for the apps left without a node: `2 vCPUs and 1.5 GiB of memory`. */
export const shortfallWords = (missing: ClusterCapacity['missing']): string | null => {
  const parts = [
    missing.cpus > 0 ? plural(missing.cpus, 'vCPU') : null,
    missing.memory_mb > 0 ? `${gibWords(missing.memory_mb)} GiB of memory` : null,
    missing.disk_gb > 0 ? `${missing.disk_gb} GiB of disk` : null,
  ].filter((part): part is string => part !== null);
  return parts.length === 0 ? null : listWords(parts);
};

/** What the apps that cannot start anywhere lack: a backup, a warm copy, or either. */
const unrecoverableWords = (stuck: readonly ClusterCapacityApp[]): string => {
  const verb = stuck.length === 1 ? 'has' : 'have';
  if (stuck.every((app) => app.reason === 'no_backup')) return `${apps(stuck.length)} ${verb} no backup to restore`;
  if (stuck.every((app) => app.reason === 'no_warm_copy')) return `${apps(stuck.length)} ${verb} no complete warm copy`;
  return `${apps(stuck.length)} ${verb} no copy to start from`;
};

/**
 * The N+1 strip in one sentence. With a single node there is nothing to fail
 * over to, and that is said without asking the primary. Otherwise the verdict
 * is the primary's `ClusterCapacity` for `lost`: which apps keep running,
 * which move and where, and what is missing when they do not fit. The share
 * of memory a receiving node would use is its reserved memory plus the memory
 * of the apps that move there, over its capacity; it is left out when an app
 * that moves is not in `apps`. Null when there is no answer for `lost` yet,
 * or no node to ask about: the strip then shows nothing rather than a guess.
 */
export function n1Readout({
  lost,
  nodes,
  answer,
  apps: knownApps,
}: {
  lost: Pick<Node, 'id' | 'name' | 'status' | 'roles'>;
  nodes: ReadonlyArray<Pick<Node, 'id' | 'name' | 'status' | 'roles' | 'allocated' | 'capacity'>>;
  answer: ClusterCapacity | null | undefined;
  apps: ReadonlyArray<Pick<App, 'id' | 'resources'>>;
}): N1Readout | null {
  const others = lossCandidates(nodes).filter((node) => node.id !== lost.id);
  const name = { strong: lost.name };

  if (others.length === 0) {
    return {
      tone: 'fault',
      segments: [name, ' is the only node: if it goes down, no other node can take its apps.'],
    };
  }
  if (!answer || answer.lost_node_id !== lost.id) return null;

  const stranded = answer.apps.filter((app) => app.reason === 'no_capacity');
  const stuck = answer.apps.filter((app) => app.reason === 'no_backup' || app.reason === 'no_warm_copy');
  const moving = answer.apps.filter((app) => app.node_id !== null && app.action !== 'keep');
  const kept = answer.apps.filter((app) => app.node_id !== null && app.action === 'keep');

  if (!answer.fits) {
    const short = shortfallWords(answer.missing);
    const who = stranded.length > 0 ? apps(stranded.length) : 'some of its apps';
    return {
      tone: 'fault',
      segments: ['Losing ', name, ` would leave ${who} without room${short ? `: ${short} short` : ''}.`],
    };
  }

  const tail = stuck.length > 0 ? `, but ${unrecoverableWords(stuck)}` : '';
  const tone = stuck.length > 0 ? 'shift' : 'run';

  if (moving.length === 0) {
    return {
      tone,
      segments:
        kept.length > 0
          ? [
              'If ',
              name,
              ` goes down, its ${apps(kept.length)} ${kept.length === 1 ? 'keeps' : 'keep'} running on ${
                kept.length === 1 ? 'its other replica' : 'their other replicas'
              }${tail}.`,
            ]
          : ['If ', name, ` goes down, no app has to move${tail}.`],
    };
  }

  const one = moving.length === 1;
  const who =
    moving.length + stuck.length === answer.apps.length
      ? `its ${apps(moving.length)} ${one ? 'fits' : 'fit'}`
      : `the ${apps(moving.length)} that ${one ? 'needs a new node fits' : 'need a new node fit'}`;

  // Where they go, in the order the primary placed them.
  const targetIds = [...new Set(moving.map((app) => app.node_id as string))];
  const targets = targetIds.map((id) => nodes.find((node) => node.id === id));
  if (targets.some((node) => node === undefined)) {
    return { tone, segments: ['If ', name, ` goes down, ${who} on the other nodes${tail}.`] };
  }
  const receiving = targets as NonNullable<(typeof targets)[number]>[];

  // Each receiving node's memory after the move, when every moving app is known.
  const sizes = new Map(knownApps.map((app) => [app.id, app.resources.memory_mb]));
  const known = moving.every((app) => sizes.has(app.app_id));
  const fullness = receiving.map((node) => {
    const added = moving
      .filter((app) => app.node_id === node.id)
      .reduce((sum, app) => sum + (sizes.get(app.app_id) ?? 0), 0);
    return { node, share: capacityShare(node.allocated.memory_mb + added, node.capacity.memory_mb) };
  });

  const segments: Segment[] = ['If ', name, ` goes down, ${who} on `];
  receiving.forEach((node, index) => {
    if (index > 0) segments.push(index === receiving.length - 1 ? ' and ' : ', ');
    segments.push({ strong: node.name });
  });

  if (known && fullness.every((entry) => entry.share !== null)) {
    if (fullness.length === 1) {
      segments.push(`, using ${Math.round(fullness[0]!.share! * 100)}% of its memory`);
    } else {
      const fullest = fullness.reduce((top, entry) => (entry.share! > top.share! ? entry : top));
      segments.push(', with ', { strong: fullest.node.name }, ` the fullest at ${Math.round(fullest.share! * 100)}% of its memory`);
    }
  }
  segments.push(`${tail}.`);
  return { tone, segments };
}
