// SPDX-License-Identifier: AGPL-3.0-only
import type {
  App,
  Approval,
  AuditRecord,
  Build,
  Deployment,
  Incident,
  NodeMetricSample,
  Project,
  Vm,
  VmMetricSample,
} from '../api/generated/model';
import { recordSentence, type RecordContext, type Segment } from './activity';
import { BUILD_FAILED_PHASE_LABELS } from './build-status';
import type { LampTone } from './lamp';
import { projectSummary, type ProjectSummaryInput } from './projects';

/*
 * The projects board as pure data: the cluster's load summed from each
 * node's own samples, an app's CPU from its instances, what a project tile
 * says in its header and at its foot, which project a pending approval
 * belongs to, and how the tiles stack into columns. Everything comes from
 * what the contract serves; nothing is guessed.
 */

/* ---------- series ---------- */

/** The nodes and the VM agents sample every 5 s. */
export const SAMPLE_INTERVAL_MS = 5_000;

/** One moment on a shared clock: the newest sample of each series at or before it. */
export interface AlignedBucket<T> {
  /** The start of the bucket, in milliseconds. */
  time: number;
  /** One sample per series, in the order the series were given. */
  samples: T[];
}

/**
 * Lines several series up on one clock. Samples fall into buckets of
 * `intervalMs`; a bucket is emitted only when every series has a sample in it
 * or in one of the `carry` buckets before it, so a sum never drops a node
 * that simply reported a moment later than the others. A series whose newest
 * sample is older than `carry` buckets behind the newest of all has stopped
 * reporting and is left out, so one silent node does not freeze the rest.
 */
export const alignSeries = <T extends { time: string }>(
  series: ReadonlyArray<readonly T[]>,
  intervalMs: number = SAMPLE_INTERVAL_MS,
  carry = 2,
): AlignedBucket<T>[] => {
  const keyed = series
    .map((samples) => {
      const byBucket = new Map<number, T>();
      for (const sample of samples) {
        const time = Date.parse(sample.time);
        if (!Number.isFinite(time)) continue;
        const bucket = Math.floor(time / intervalMs);
        const held = byBucket.get(bucket);
        if (!held || Date.parse(held.time) <= time) byBucket.set(bucket, sample);
      }
      return byBucket;
    })
    .filter((byBucket) => byBucket.size > 0);
  if (keyed.length === 0) return [];

  const newestOf = (byBucket: Map<number, T>) => Math.max(...byBucket.keys());
  const newest = Math.max(...keyed.map(newestOf));
  const active = keyed.filter((byBucket) => newest - newestOf(byBucket) <= carry);
  const buckets = [...new Set(active.flatMap((byBucket) => [...byBucket.keys()]))].sort((a, b) => a - b);

  const held: Array<{ bucket: number; sample: T } | undefined> = active.map(() => undefined);
  const out: AlignedBucket<T>[] = [];
  for (const bucket of buckets) {
    active.forEach((byBucket, index) => {
      const sample = byBucket.get(bucket);
      if (sample) held[index] = { bucket, sample };
    });
    if (held.every((entry) => entry !== undefined && bucket - entry.bucket <= carry)) {
      out.push({ time: bucket * intervalMs, samples: held.map((entry) => entry!.sample) });
    }
  }
  return out;
};

/** The whole cluster at one moment, summed from its nodes. */
export interface ClusterSample {
  /** ISO time of the bucket, so the chart reads it like any sample. */
  time: string;
  /** Busy share of every host CPU in the cluster, 0..1, each node weighted by its CPU count. */
  cpu_usage: number;
  cpus: number;
  memory_used_bytes: number;
  memory_total_bytes: number;
}

/** The cluster's CPU and memory over the window, from each node's own samples. */
export const sumNodeSeries = (series: ReadonlyArray<readonly NodeMetricSample[]>): ClusterSample[] =>
  alignSeries(series).map(({ time, samples }) => {
    let busy = 0;
    let cpus = 0;
    let used = 0;
    let total = 0;
    for (const sample of samples) {
      busy += sample.cpu_usage * sample.cpu_count;
      cpus += sample.cpu_count;
      used += sample.memory_used_bytes;
      total += sample.memory_total_bytes;
    }
    return {
      time: new Date(time).toISOString(),
      cpu_usage: cpus > 0 ? busy / cpus : 0,
      cpus,
      memory_used_bytes: used,
      memory_total_bytes: total,
    };
  });

/**
 * An app's CPU over the window: the busy share of all its instances' vCPUs,
 * each instance weighted by its vCPU count.
 */
export const appCpuSeries = (
  instances: ReadonlyArray<{ samples: readonly VmMetricSample[] }>,
): Array<{ time: string; cpu_usage: number }> =>
  alignSeries(instances.map((instance) => instance.samples)).map(({ time, samples }) => {
    let busy = 0;
    let vcpus = 0;
    for (const sample of samples) {
      busy += sample.cpu_usage * sample.vcpus;
      vcpus += sample.vcpus;
    }
    return { time: new Date(time).toISOString(), cpu_usage: vcpus > 0 ? busy / vcpus : 0 };
  });

/**
 * The top of the cluster band's hidden axis, in percent: half again the
 * window's peak in round tens, between 20 and 100, so the area fills the band
 * without the scale ever reading as more than it is (the number says it).
 */
export const bandAxisMax = (peakPercent: number): number => {
  const peak = Number.isFinite(peakPercent) ? Math.max(0, peakPercent) : 0;
  return Math.min(100, Math.max(20, Math.ceil((peak * 1.5) / 10) * 10));
};

/* ---------- the band's facts ---------- */

export interface BandFacts {
  running: number;
  apps: number;
  vms: number;
}

/** Apps running out of all apps, and every VM the cluster holds (app instances and standalone). */
export const bandFacts = (
  apps: ReadonlyArray<Pick<App, 'status'>>,
  vms: ReadonlyArray<Pick<Vm, 'observed_status'>>,
): BandFacts => ({
  running: apps.filter((app) => app.status === 'running').length,
  apps: apps.length,
  vms: vms.filter((vm) => vm.observed_status !== 'deleted').length,
});

/** Bytes as GiB with one decimal, as the band says them: `19.9`. */
export const gibOne = (bytes: number): string =>
  (Number.isFinite(bytes) ? bytes / 1024 ** 3 : 0).toFixed(1);

/** Bytes as whole GiB for a total: `32`; one decimal below 10. */
export const gibTotal = (bytes: number): string => {
  const gib = Number.isFinite(bytes) ? bytes / 1024 ** 3 : 0;
  return gib >= 10 ? `${Math.round(gib)}` : `${Math.round(gib * 10) / 10}`;
};

/* ---------- a tile's header ---------- */

/** What a tile's header says, and how its glyph field and edge are toned. */
export interface TileStatus {
  readonly tone: LampTone;
  readonly pulse: boolean;
  readonly words: string;
  /** The glyph field's tone: amber for an open incident, red for a failed build. */
  readonly field: 'accent' | 'shift' | 'fault';
  /** An open incident draws the tile's amber edge. */
  readonly warn: boolean;
}

/**
 * The tile's state in a few words, the most urgent first: an open incident,
 * then the last build failing, then something deploying, then the counts
 * (`5 running`, `4 running · 1 stopped`). The lamp's tone follows the words.
 */
export const tileStatus = (input: ProjectSummaryInput): TileStatus => {
  if (input.openIncidents > 0) {
    return {
      tone: 'shift',
      pulse: false,
      words: input.openIncidents === 1 ? 'Open incident' : `${input.openIncidents} open incidents`,
      field: 'shift',
      warn: true,
    };
  }
  if (input.lastBuildFailed) {
    return { tone: 'fault', pulse: false, words: 'Last build failed', field: 'fault', warn: false };
  }
  if (input.apps.some((app) => app.status === 'deploying')) {
    return { tone: 'shift', pulse: true, words: 'Deploying', field: 'accent', warn: false };
  }
  const summary = projectSummary(input);
  return { tone: summary.tone, pulse: summary.pulse, words: summary.text, field: 'accent', warn: false };
};

/* ---------- a tile's foot ---------- */

/** A run of the foot's line: plain words, or a code value (a commit, an image) in the code face. */
export interface FootSegment {
  text: string;
  code?: boolean;
}

export interface TileFoot {
  kind: 'incident' | 'failed' | 'deployed';
  segments: FootSegment[];
  /** When it happened, for the time at the right. */
  at: string;
}

export interface TileFootInput {
  /** The project's open incidents. */
  incidents: ReadonlyArray<Pick<Incident, 'title' | 'opened_at'>>;
  /** The project's apps, each with its recent builds (newest first) and deployments, when read. */
  apps: ReadonlyArray<{
    app: Pick<App, 'name'>;
    builds?: ReadonlyArray<Pick<Build, 'status' | 'commit' | 'failed_step' | 'failed_phase' | 'error' | 'deployment_id' | 'created_at' | 'finished_at'>>;
    deployments?: ReadonlyArray<Pick<Deployment, 'id' | 'status' | 'image' | 'created_at' | 'finished_at'>>;
  }>;
}

const timeOf = (iso: string | null | undefined): number => {
  const at = iso ? Date.parse(iso) : Number.NaN;
  return Number.isFinite(at) ? at : Number.NEGATIVE_INFINITY;
};

const newestFirst = <T>(items: readonly T[], at: (item: T) => string | null | undefined): T[] =>
  [...items].sort((left, right) => timeOf(at(right)) - timeOf(at(left)));

/**
 * What the tile's foot says, the most urgent first: the newest open incident
 * by its title; else the newest failed build of the project that is still its
 * app's newest, at the exact step (`Build of 9a8b7c6 failed at [3/4] RUN ls`);
 * else the newest finished deployment (`web deployed from 7d41e0a`, the
 * commit its build built, or the image it ran). Null when nothing is known.
 */
export const tileFoot = (input: TileFootInput): TileFoot | null => {
  const incident = newestFirst(input.incidents, (item) => item.opened_at)[0];
  if (incident) {
    return { kind: 'incident', segments: [{ text: incident.title }], at: incident.opened_at };
  }

  const failures = input.apps.flatMap(({ builds }) => {
    const newest = builds?.[0];
    return newest && newest.status === 'failed' ? [newest] : [];
  });
  const failed = newestFirst(failures, (build) => build.finished_at ?? build.created_at)[0];
  if (failed) {
    const where = failed.failed_step
      ? ` failed at ${failed.failed_step.name}`
      : failed.failed_phase
        ? ` failed ${BUILD_FAILED_PHASE_LABELS[failed.failed_phase]}`
        : ' failed';
    const segments: FootSegment[] = failed.commit
      ? [{ text: 'Build of ' }, { text: failed.commit.slice(0, 7), code: true }, { text: where }]
      : [{ text: `Build${where}` }];
    return { kind: 'failed', segments, at: failed.finished_at ?? failed.created_at };
  }

  const deployed = newestFirst(
    input.apps.flatMap(({ app, builds, deployments }) =>
      (deployments ?? [])
        .filter((deployment) => deployment.status === 'succeeded')
        .map((deployment) => ({ app, builds, deployment })),
    ),
    (entry) => entry.deployment.finished_at ?? entry.deployment.created_at,
  )[0];
  if (deployed) {
    const { app, builds, deployment } = deployed;
    const build = builds?.find((candidate) => candidate.deployment_id === deployment.id);
    const from = build?.commit
      ? build.commit.slice(0, 7)
      : deployment.image.startsWith('build.quoro.invalid/')
        ? null
        : deployment.image;
    return {
      kind: 'deployed',
      segments: from
        ? [{ text: `${app.name} deployed from ` }, { text: from, code: true }]
        : [{ text: `${app.name} deployed` }],
      at: deployment.finished_at ?? deployment.created_at,
    };
  }
  return null;
};

/** How long ago, without "ago", for an incident that is still open: `16 min`, `2 h`. */
export const openFor = (relative: string): string => relative.replace(/ ago$/, '');

/* ---------- approvals ---------- */

/**
 * The project a pending approval acts in: an app's or a VM's project, or the
 * project itself. Undefined for any other target, or one this page does not list.
 */
export const approvalProjectId = (
  approval: Pick<Approval, 'target'>,
  lists: {
    apps: ReadonlyArray<Pick<App, 'id' | 'project_id'>>;
    vms: ReadonlyArray<Pick<Vm, 'id' | 'project_id'>>;
    projects: ReadonlyArray<Pick<Project, 'id'>>;
  },
): string | undefined => {
  const { type, id } = approval.target;
  if (type === 'app') return lists.apps.find((app) => app.id === id)?.project_id;
  if (type === 'vm') return lists.vms.find((vm) => vm.id === id)?.project_id;
  if (type === 'project') return lists.projects.some((project) => project.id === id) ? id : undefined;
  return undefined;
};

/* ---------- columns ---------- */

/**
 * Stacks tiles into columns the way a person would: each tile goes under the
 * column that is shortest so far, by `weight` (about its height), ties to the
 * left. Read column by column, the order stays the given one within a column.
 */
export const splitColumns = <T>(items: readonly T[], weight: (item: T) => number, columns = 2): T[][] => {
  const out: T[][] = Array.from({ length: Math.max(1, columns) }, () => []);
  const heights = out.map(() => 0);
  for (const item of items) {
    let target = 0;
    for (let index = 1; index < heights.length; index += 1) {
      if (heights[index]! < heights[target]!) target = index;
    }
    out[target]!.push(item);
    heights[target]! += weight(item);
  }
  return out;
};

/* ---------- recent activity ---------- */

/**
 * A record in the rail's few words: who and what, without the channel the
 * Activity page adds, and "· refused" or "· failed" when it did not go through.
 */
export const railSentence = (record: AuditRecord, ctx: RecordContext = {}): Segment[] => {
  const words = recordSentence(record, ctx).filter((segment) => !segment.faint);
  if (record.result === 'refused') return [...words, { text: ' · refused' }];
  if (record.result === 'error') return [...words, { text: ' · failed' }];
  return words;
};
