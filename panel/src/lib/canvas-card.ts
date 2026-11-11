// SPDX-License-Identifier: AGPL-3.0-only
import type { App, Backup, Build, Deployment, Vm, VmMetricSample, Volume } from '../api/generated/model';
import { relativeTime } from './app-status';
import { appCpuSeries } from './board';
import { DEPLOY_TAPE_STEPS, formatClock, isDeploymentLive, parseTime, type DeployTapeStep } from './deploy-tape';
import { lampState, type LampTone } from './lamp';
import { commitLine, lastBuildFailed, memoryWords, nextBackupLine, nodesLine, volumeLine } from './services';
import { warmCardLine } from './warm-words';

/*
 * A service card on the canvas as pure data: its CPU and memory figures from
 * the app's metrics, the nodes its instances run on, how long ago it was last
 * deployed, a redeploy's progress from the deployment record, and the footer
 * rows. The card component only lays these out.
 */

const MIB = 1024 * 1024;

/* ---------- CPU and memory ---------- */

export interface CardFigures {
  /** The busy share of all the app's vCPUs at the newest sample, each instance weighted by its vCPUs. */
  cpu: number;
  /** The app's CPU over the window, in percent, oldest first: the trace under the figure. */
  cpuTrace: Array<{ time: string; percent: number }>;
  /** Resident memory of every instance's VMM at its newest sample. */
  memoryBytes: number;
  /** The memory every instance is configured with, summed. */
  memoryTotalBytes: number;
}

/**
 * What a card says about an app's CPU and memory, from `GET /apps/{id}/metrics`
 * (`AppMetrics.instances`). Each instance's newest sample counts once. Null
 * while no instance has a sample: the card then shows a dash, not 0 %.
 */
export const cardFigures = (
  instances: ReadonlyArray<{ samples: readonly VmMetricSample[] }>,
): CardFigures | null => {
  const newest = instances
    .map((instance) => instance.samples[instance.samples.length - 1])
    .filter((sample): sample is VmMetricSample => sample !== undefined);
  if (newest.length === 0) return null;
  const series = appCpuSeries(instances);
  const last = series[series.length - 1];
  let busy = 0;
  let vcpus = 0;
  for (const sample of newest) {
    busy += sample.cpu_usage * sample.vcpus;
    vcpus += sample.vcpus;
  }
  return {
    cpu: last ? last.cpu_usage : vcpus > 0 ? busy / vcpus : 0,
    cpuTrace: series.map((point) => ({ time: point.time, percent: point.cpu_usage * 100 })),
    memoryBytes: newest.reduce((total, sample) => total + sample.memory_rss_bytes, 0),
    memoryTotalBytes: newest.reduce((total, sample) => total + sample.memory_bytes, 0),
  };
};

/** `21%`, rounded to a whole percent as the card prints it. */
export const cpuFigure = (share: number): string => `${Math.round(Math.max(0, share) * 100)}%`;

/** `386 MiB`, `1.2 GiB`: resident memory in the unit the card prints. */
export const memoryFigure = (bytes: number): string => memoryWords(Math.round(Math.max(0, bytes) / MIB));

/** How full the memory meter is: resident over configured, 0..1. */
export const memoryShare = (figures: Pick<CardFigures, 'memoryBytes' | 'memoryTotalBytes'>): number =>
  figures.memoryTotalBytes > 0 ? Math.min(1, figures.memoryBytes / figures.memoryTotalBytes) : 0;

/* ---------- the nodes of its instances ---------- */

export interface NodeChip {
  nodeId: string;
  name: string;
  /** The worst state among the app's instances on this node. */
  tone: LampTone;
  /** What a screen reader hears: `lab-3, running`. */
  spoken: string;
}

const TONE_RANK: Record<LampTone, number> = { run: 0, idle: 1, unknown: 2, shift: 3, fault: 4 };

/**
 * One chip per node an app's instances run on, sorted by name, each with the
 * lamp of the worst instance there. A deleted VM is not on a node; a node the
 * list does not name is left out.
 */
export const nodeChips = (
  vms: ReadonlyArray<Pick<Vm, 'node_id' | 'desired_status' | 'observed_status'>>,
  names: Readonly<Record<string, string>>,
): NodeChip[] => {
  const byNode = new Map<string, { tone: LampTone; text: string }>();
  for (const vm of vms) {
    if (vm.observed_status === 'deleted') continue;
    const lamp = lampState(vm);
    const seen = byNode.get(vm.node_id);
    if (!seen || TONE_RANK[lamp.coreTone] > TONE_RANK[seen.tone]) {
      byNode.set(vm.node_id, { tone: lamp.coreTone, text: lamp.text });
    }
  }
  return [...byNode.entries()]
    .flatMap(([nodeId, { tone, text }]) => {
      const name = names[nodeId];
      return name ? [{ nodeId, name, tone, spoken: `${name}, ${text.toLowerCase()}` }] : [];
    })
    .sort((left, right) => left.name.localeCompare(right.name));
};

/* ---------- the age of the last deploy ---------- */

/**
 * The deployment the app runs: its `current_deployment_id` when the page
 * holds it, otherwise the newest that succeeded.
 */
export const currentDeployment = (
  app: Pick<App, 'current_deployment_id'>,
  deployments: readonly Deployment[],
): Deployment | undefined =>
  deployments.find((deployment) => deployment.id === app.current_deployment_id) ??
  deployments.find((deployment) => deployment.status === 'succeeded');

/**
 * How long ago the app was last deployed: `for 40 min` while it runs on that
 * deployment, `deployed 3 h ago` when it is down, `just now` right after.
 * Null without a succeeded deployment on the page.
 */
export const deployAge = (
  app: Pick<App, 'status' | 'current_deployment_id'>,
  deployments: readonly Deployment[],
  now: number,
): string | null => {
  const current = currentDeployment(app, deployments);
  if (!current) return null;
  const at = current.finished_at ?? current.created_at;
  const relative = relativeTime(at, now);
  if (relative === 'unknown time') return null;
  if (relative === 'just now') return 'just now';
  return app.status === 'down' || app.status === 'stopped'
    ? `deployed ${relative}`
    : `for ${relative.replace(/ ago$/, '')}`;
};

/* ---------- a redeploy's progress ---------- */

export type TapeState = 'done' | 'live' | 'pending';

export interface DeployProgress {
  deploymentId: string;
  /** `Deploying · rootfs`, or `Deploying · queued` before the first phase. */
  word: string;
  /** What the phase does: `Preparing the root disk`. */
  sentence: string;
  /** `0:42` since the deployment started. */
  clock: string;
  steps: ReadonlyArray<{ step: DeployTapeStep; state: TapeState }>;
}

const PHASE_SENTENCES: Record<DeployTapeStep, (replicas: number) => string> = {
  pull: () => 'Pulling the image',
  rootfs: () => 'Preparing the root disk',
  boot: (replicas) => (replicas > 1 ? 'Booting the microVMs' : 'Booting the microVM'),
  health: () => 'Waiting for the health check',
};

/**
 * Where a deployment that rolls out stands, from its record alone
 * (`Deployment.status`, `phase`, `created_at`): the phases before the current
 * one are done, the current one runs, the rest wait. Null once it has ended.
 */
export const deployProgress = (
  deployment: Pick<Deployment, 'id' | 'status' | 'phase' | 'created_at'> | undefined,
  replicas: number,
  now: number,
): DeployProgress | null => {
  if (!deployment || !isDeploymentLive(deployment.status)) return null;
  const phase = (DEPLOY_TAPE_STEPS as readonly string[]).includes(deployment.phase ?? '')
    ? (deployment.phase as DeployTapeStep)
    : null;
  const at = phase === null ? -1 : DEPLOY_TAPE_STEPS.indexOf(phase);
  const started = parseTime(deployment.created_at);
  return {
    deploymentId: deployment.id,
    word: `Deploying · ${phase ?? 'queued'}`,
    sentence: phase === null ? 'Queued, waiting for the node' : PHASE_SENTENCES[phase](replicas),
    clock: formatClock(started === null ? 0 : (now - started) / 1_000),
    steps: DEPLOY_TAPE_STEPS.map((step, index) => ({
      step,
      state: index < at ? 'done' : index === at ? 'live' : 'pending',
    })),
  };
};

/**
 * True when the deployment the card last saw rolling out has now succeeded:
 * the moment for the one short confirmation. A deployment that failed, was
 * superseded, or was never seen live gives none, and neither does a first load.
 */
export const justDeployed = (
  watched: string | null,
  deployments: ReadonlyArray<Pick<Deployment, 'id' | 'status'>>,
): boolean =>
  watched !== null && deployments.some((deployment) => deployment.id === watched && deployment.status === 'succeeded');

/* ---------- the footer rows ---------- */

export type FootKey = 'build' | 'commit' | 'volume' | 'backup' | 'warm' | 'nodes' | 'image' | 'host';

export interface FootPart {
  text: string;
  /** Set in Martian Mono: a sha, an image reference, an IP. */
  code?: boolean;
}

export interface FootRow {
  key: FootKey;
  parts: FootPart[];
  /**
   * Said in the fault colour. Only the text: a failed build is also why the
   * card glows red, but a failed warm copy is not, since the lamp and the
   * aura stay the app's status (`appFailed` reads neither row).
   */
  fault?: boolean;
}

/** The row's words as one string, as a test or a title reads them. */
export const footText = (row: FootRow): string => row.parts.map((part) => part.text).join('');

const plain = (text: string): FootPart[] => [{ text }];

/** `7d41e0a on main` with the sha as code. */
const commitParts = (line: string): FootPart[] => {
  const space = line.indexOf(' ');
  return space < 0 ? [{ text: line, code: true }] : [{ text: line.slice(0, space), code: true }, { text: line.slice(space) }];
};

/**
 * An app card's footer rows, in order: the build (a failed one in red, or the
 * commit it runs), its volume, its next backup, its warm copy. With none of
 * those, one row says what it runs: the image reference for an app run from
 * an image, or the nodes for a Git app with no build on the page.
 * `volumes` and `backups` are undefined until read.
 */
export const appFootRows = (input: {
  app: App;
  builds: readonly Build[] | undefined;
  volumes: readonly Volume[] | undefined;
  backups: readonly Backup[] | undefined;
  nodeIds: readonly string[];
  nodeNames: Readonly<Record<string, string>>;
  now?: number;
}): FootRow[] => {
  const { app, builds, nodeIds, nodeNames } = input;
  const foot: FootRow[] = [];
  const newest = builds?.[0];
  if (builds && newest && lastBuildFailed(builds)) {
    const text = newest.commit ? `Build of ${newest.commit.slice(0, 7)} failed` : 'Last build failed';
    foot.push({ key: 'build', parts: plain(text), fault: true });
  } else {
    const commit = builds ? commitLine(builds) : null;
    if (commit) foot.push({ key: 'commit', parts: commitParts(commit) });
  }
  const volume = app.stateful ? volumeLine(input.volumes ?? []) : null;
  if (volume) foot.push({ key: 'volume', parts: plain(volume) });
  const backup = input.backups ? nextBackupLine(app, input.backups, input.now) : null;
  if (backup) foot.push({ key: 'backup', parts: plain(backup) });
  // A warm app's copy, where a cold app's next backup goes; the full error is
  // in the Storage tab.
  if (app.warm) {
    foot.push({ key: 'warm', parts: plain(warmCardLine(app.warm, nodeNames)), fault: app.warm.state === 'failed' });
  }
  if (foot.length === 0) {
    if (app.source == null) {
      foot.push({ key: 'image', parts: [{ text: app.image, code: true }] });
    } else {
      const nodes = nodesLine(nodeIds, nodeNames);
      if (nodes) foot.push({ key: 'nodes', parts: plain(nodes) });
    }
  }
  return foot;
};

/**
 * A standalone VM's footer row: its node and its address, `lab-3 · 10.0.1.40`.
 * Without an address (`Vm.ip` is null until the VM has one) it is the node alone.
 */
export const vmFootRows = (
  vm: Pick<Vm, 'node_id' | 'ip'>,
  nodeNames: Readonly<Record<string, string>>,
): FootRow[] => {
  const node = nodesLine([vm.node_id], nodeNames);
  if (vm.ip) {
    return [{ key: 'host', parts: [...(node ? [{ text: `${node} · ` }] : []), { text: vm.ip, code: true }] }];
  }
  return node ? [{ key: 'nodes', parts: plain(node) }] : [];
};
