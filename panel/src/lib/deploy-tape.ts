// SPDX-License-Identifier: AGPL-3.0-only
/**
 * The deploy tape as pure data: where the time of a deployment went, phase by
 * phase, and what the app's earlier deployments say it usually takes. Nothing
 * here touches React or the DOM, so every rule is tested on its own.
 *
 * Every time comes from the contract, joined as it sanctions:
 * - A Git app's run starts with its `Build`: `created_at` to `started_at` is
 *   the queue, `started_at` to `finished_at` (or now) is the build.
 *   `Build.deployment_id` names the deployment the build started.
 * - A deployment's phases come from its progress lines
 *   (`DeploymentLogEntry.time` and `.phase`): a phase starts at its first
 *   line and ends at the next phase's first line, or at
 *   `Deployment.finished_at`, or now while it runs. `build` is never reported
 *   as a deployment phase (ADR 0119), so a line that says so is ignored.
 * - BuildKit steps come from the build's log lines (`BuildLogLine.vertex` and
 *   `.time`): a step runs from its first line to its last.
 */
import type {
  Build,
  BuildLogLine,
  Deployment,
  DeploymentLogEntry,
} from '../api/generated/model';
import { BUILD_FAILED_PHASE_LABELS, isBuildTerminal } from './build-status';

/* ---------- steps ---------- */

/** The steps of a run, in order: the build's two, then the deployment's four. */
export const TAPE_STEPS = ['queue', 'build', 'pull', 'rootfs', 'boot', 'health'] as const;
export type TapeStep = (typeof TAPE_STEPS)[number];

export const BUILD_TAPE_STEPS = ['queue', 'build'] as const;
export const DEPLOY_TAPE_STEPS = ['pull', 'rootfs', 'boot', 'health'] as const;
export type DeployTapeStep = (typeof DEPLOY_TAPE_STEPS)[number];

export const TAPE_STEP_LABELS: Record<TapeStep, string> = {
  queue: 'Queue',
  build: 'Build',
  pull: 'Pull',
  rootfs: 'Rootfs',
  boot: 'Boot',
  health: 'Health',
};

/**
 * - `done`: finished, with its span.
 * - `live`: running now; its span ends now.
 * - `failed`: where the run broke.
 * - `stopped`: the run was cancelled or superseded while in it.
 * - `pending`: a live run has not reached it yet.
 * - `skipped`: a finished run has no record of it (never reached, or no line).
 */
export type StepState = 'done' | 'live' | 'failed' | 'stopped' | 'pending' | 'skipped';

export interface StepSpan {
  readonly step: TapeStep;
  readonly state: StepState;
  /** Epoch milliseconds, null without a record. */
  readonly start: number | null;
  readonly end: number | null;
  /** The span in seconds; null without a record. */
  readonly seconds: number | null;
}

/** Epoch milliseconds of an RFC 3339 time, or null when it does not parse. */
export const parseTime = (iso: string | null | undefined): number | null => {
  if (iso === null || iso === undefined || iso === '') return null;
  const value = Date.parse(iso);
  return Number.isNaN(value) ? null : value;
};

const span = (
  step: TapeStep,
  state: StepState,
  start: number | null,
  end: number | null,
): StepSpan => ({
  step,
  state,
  start,
  end: start === null ? null : end,
  seconds: start === null || end === null ? null : Math.max(0, (end - start) / 1_000),
});

const isDeployStep = (phase: unknown): phase is DeployTapeStep =>
  typeof phase === 'string' && (DEPLOY_TAPE_STEPS as readonly string[]).includes(phase);

const LIVE_DEPLOYMENT: ReadonlySet<Deployment['status']> = new Set([
  'pending',
  'preparing',
  'starting',
]);

export const isDeploymentLive = (status: Deployment['status']): boolean =>
  LIVE_DEPLOYMENT.has(status);

/** The build's two steps: the queue, then the build itself. */
export function buildSpans(
  build: Pick<Build, 'status' | 'created_at' | 'started_at' | 'finished_at'>,
  now: number,
): StepSpan[] {
  const created = parseTime(build.created_at);
  const started = parseTime(build.started_at);
  const finished = parseTime(build.finished_at);
  const live = !isBuildTerminal(build.status);
  const broke: StepState =
    build.status === 'failed' ? 'failed' : build.status === 'cancelled' ? 'stopped' : 'done';

  // Without `started_at` the build never left the queue: it waits there, or
  // ended there (failed or cancelled before the node took it).
  const queue =
    started !== null
      ? span('queue', 'done', created, started)
      : live
        ? span('queue', 'live', created, now)
        : broke === 'done'
          ? span('queue', 'skipped', null, null)
          : span('queue', broke, created, finished ?? created);

  const work =
    started === null
      ? span('build', live ? 'pending' : 'skipped', null, null)
      : live
        ? span('build', 'live', started, now)
        : span('build', broke, started, finished);

  return [queue, work];
}

/**
 * The deployment's four steps from its progress lines. A step without a line
 * has no span: before the current one it is `skipped`, after it `pending`
 * while the deployment runs.
 */
export function deploymentSpans(
  deployment: Pick<Deployment, 'status' | 'phase' | 'finished_at'>,
  entries: readonly DeploymentLogEntry[] | undefined,
  now: number,
): StepSpan[] {
  const first = new Map<DeployTapeStep, number>();
  const last = new Map<DeployTapeStep, number>();
  for (const entry of entries ?? []) {
    if (!isDeployStep(entry.phase)) continue;
    const time = parseTime(entry.time);
    if (time === null) continue;
    const seenFirst = first.get(entry.phase);
    if (seenFirst === undefined || time < seenFirst) first.set(entry.phase, time);
    const seenLast = last.get(entry.phase);
    if (seenLast === undefined || time > seenLast) last.set(entry.phase, time);
  }
  const seen = DEPLOY_TAPE_STEPS.filter((step) => first.has(step));
  const lastSeen = seen[seen.length - 1] ?? null;
  const live = isDeploymentLive(deployment.status);
  const finished = parseTime(deployment.finished_at);

  // Where the deployment is, broke, or was stopped.
  let marker: DeployTapeStep | null = null;
  let markerState: StepState = 'done';
  if (live) {
    marker = lastSeen;
    markerState = 'live';
  } else if (deployment.status === 'failed') {
    marker = isDeployStep(deployment.phase) ? deployment.phase : lastSeen;
    markerState = 'failed';
  } else if (deployment.status === 'superseded') {
    marker = lastSeen;
    markerState = 'stopped';
  }
  const markerIndex = marker === null ? -1 : DEPLOY_TAPE_STEPS.indexOf(marker);

  return DEPLOY_TAPE_STEPS.map((step, index) => {
    const start = first.get(step) ?? null;
    let end: number | null = null;
    if (start !== null) {
      const next = seen.find((other) => DEPLOY_TAPE_STEPS.indexOf(other) > index);
      end =
        next !== undefined
          ? (first.get(next) ?? null)
          : live
            ? now
            : (finished ?? last.get(step) ?? start);
    }

    let state: StepState;
    if (deployment.status === 'succeeded') {
      state = start === null ? 'skipped' : 'done';
    } else if (live && markerIndex < 0) {
      state = 'pending';
    } else if (index < markerIndex) {
      state = start === null ? 'skipped' : 'done';
    } else if (index === markerIndex) {
      state = markerState;
    } else {
      state = live ? 'pending' : start === null ? 'skipped' : 'done';
    }
    return span(step, state, start, end);
  });
}

/* ---------- runs: a build and the deployment it started, or a deployment alone ---------- */

export interface DeployRun {
  readonly key: string;
  readonly build: Build | null;
  readonly deployment: Deployment | null;
}

const runStart = (run: DeployRun): number =>
  parseTime(run.build?.created_at ?? run.deployment?.created_at) ?? 0;

/**
 * Join builds to the deployments they started (`Build.deployment_id`), newest
 * run first. A deployment no listed build started (an image deploy, a
 * redeploy, a rollback, or a build older than the page) is a run of its own.
 */
export function joinRuns(builds: readonly Build[], deployments: readonly Deployment[]): DeployRun[] {
  const byId = new Map(deployments.map((deployment) => [deployment.id, deployment]));
  const claimed = new Set<string>();
  const runs: DeployRun[] = builds.map((build) => {
    const deployment =
      build.deployment_id !== null ? (byId.get(build.deployment_id) ?? null) : null;
    if (deployment) claimed.add(deployment.id);
    return { key: `build:${build.id}`, build, deployment };
  });
  for (const deployment of deployments) {
    if (!claimed.has(deployment.id)) {
      runs.push({ key: `deployment:${deployment.id}`, build: null, deployment });
    }
  }
  return runs
    .map((run, index) => ({ run, index }))
    .sort((left, right) => runStart(right.run) - runStart(left.run) || left.index - right.index)
    .map(({ run }) => run);
}

export type RunState = 'live' | 'succeeded' | 'failed' | 'cancelled' | 'superseded';

export interface RunTimeline {
  readonly run: DeployRun;
  /** A run with a build has six steps; a deployment alone has four. */
  readonly kind: 'git' | 'image';
  readonly state: RunState;
  readonly steps: readonly StepSpan[];
  /** Epoch milliseconds the run started. */
  readonly start: number;
  /** Epoch milliseconds it ended; null while live. */
  readonly end: number | null;
  /** Seconds from the start to the end, or to now while live. */
  readonly elapsed: number;
  /** Seconds the recorded steps add up to: where the time went. */
  readonly recorded: number;
}

const blank = (state: StepState): StepSpan[] =>
  DEPLOY_TAPE_STEPS.map((step) => span(step, state, null, null));

/** Every step of one run, in order, with its state and span. */
export function runTimeline(
  run: DeployRun,
  entries: readonly DeploymentLogEntry[] | undefined,
  now: number,
): RunTimeline {
  const build = run.build;
  const deployment = run.deployment;
  const kind = build ? 'git' : 'image';

  const steps: StepSpan[] = [];
  let state: RunState;
  if (build) steps.push(...buildSpans(build, now));

  if (build && build.status !== 'succeeded') {
    const buildLive = !isBuildTerminal(build.status);
    steps.push(...blank(buildLive ? 'pending' : 'skipped'));
    state = buildLive ? 'live' : build.status === 'failed' ? 'failed' : 'cancelled';
  } else if (!deployment && build?.deployment_id == null) {
    // The build succeeded and the deployment it starts is not named yet.
    steps.push(...blank('pending'));
    state = 'live';
  } else if (!deployment) {
    // The build's deployment is older than the page read: the build is all
    // there is to draw, and nothing is guessed about how the rollout went.
    steps.push(...blank('skipped'));
    state = 'succeeded';
  } else {
    steps.push(...deploymentSpans(deployment, entries, now));
    state = isDeploymentLive(deployment.status)
      ? 'live'
      : deployment.status === 'succeeded'
        ? 'succeeded'
        : deployment.status === 'failed'
          ? 'failed'
          : 'superseded';
  }

  const start = runStart(run);
  const stepEnds = steps.map((step) => step.end ?? 0);
  const end =
    state === 'live'
      ? null
      : (parseTime(deployment?.finished_at) ??
        parseTime(build?.finished_at) ??
        Math.max(start, ...stepEnds));
  const elapsed = Math.max(0, ((end ?? now) - start) / 1_000);
  const recorded = steps.reduce((total, step) => total + (step.seconds ?? 0), 0);
  return { run, kind, state, steps, start, end, elapsed, recorded };
}

/** The step a live run is in: the running one, or the first still to come. */
export const currentStep = (timeline: RunTimeline): StepSpan | null =>
  timeline.steps.find((step) => step.state === 'live') ??
  timeline.steps.find((step) => step.state === 'pending') ??
  null;

/* ---------- the estimate ---------- */

const median = (values: readonly number[]): number => {
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1
    ? sorted[middle]!
    : (sorted[middle - 1]! + sorted[middle]!) / 2;
};

export interface Estimate {
  /** The median seconds of each step, over the runs that recorded it. */
  readonly steps: Partial<Record<TapeStep, number>>;
  /** The median seconds of a whole run. */
  readonly total: number;
  /** How many earlier runs it comes from. */
  readonly samples: number;
}

/**
 * What a run of this kind usually takes, from the app's previous succeeded
 * runs of the same kind (newest first, at most `limit`): the median of each
 * step and of the whole. No such run, no estimate: the screen then shows
 * none, rather than a made-up number.
 */
export function estimateRun(
  history: readonly RunTimeline[],
  kind: RunTimeline['kind'],
  limit = 5,
): Estimate | null {
  const done = history
    .filter((timeline) => timeline.state === 'succeeded' && timeline.kind === kind)
    .slice(0, limit);
  if (done.length === 0) return null;
  const steps: Partial<Record<TapeStep, number>> = {};
  for (const step of TAPE_STEPS) {
    const values = done.flatMap((timeline) => {
      const found = timeline.steps.find((candidate) => candidate.step === step);
      return found && found.state === 'done' && found.seconds !== null ? [found.seconds] : [];
    });
    if (values.length > 0) steps[step] = median(values);
  }
  return { steps, total: median(done.map((timeline) => timeline.elapsed)), samples: done.length };
}

/**
 * Seconds a live run still has to go by the estimate: what the running step
 * usually takes beyond what it has taken, plus every step still to come.
 * Null without an estimate or for a run that is over.
 */
export function remainingSeconds(timeline: RunTimeline, estimate: Estimate | null): number | null {
  if (!estimate || timeline.state !== 'live') return null;
  let rest = 0;
  for (const step of timeline.steps) {
    const typical = estimate.steps[step.step];
    if (typical === undefined) continue;
    if (step.state === 'live') rest += Math.max(0, typical - (step.seconds ?? 0));
    else if (step.state === 'pending') rest += typical;
  }
  return rest;
}

/* ---------- the mini tape of a history row ---------- */

export type MiniTone = 'done' | 'live' | 'failed' | 'stopped' | 'rest';

export interface MiniSegment {
  readonly tone: MiniTone;
  /** Seconds, used as the flex weight. */
  readonly grow: number;
}

/** The one time scale every row of a history shares: its longest recorded run. */
export const miniTapeScale = (timelines: readonly RunTimeline[]): number =>
  timelines.reduce((longest, timeline) => Math.max(longest, timeline.recorded), 0);

/**
 * One row's mini tape on the shared scale: a segment per recorded step, red
 * where it broke, and the rest of the scale left grey.
 */
export function miniTape(timeline: RunTimeline, scale: number): MiniSegment[] {
  const segments: MiniSegment[] = timeline.steps.flatMap((step) => {
    if (step.seconds === null || step.seconds <= 0) return [];
    const tone: MiniTone =
      step.state === 'failed'
        ? 'failed'
        : step.state === 'stopped'
          ? 'stopped'
          : step.state === 'live'
            ? 'live'
            : 'done';
    return [{ tone, grow: step.seconds }];
  });
  const used = segments.reduce((total, segment) => total + segment.grow, 0);
  const rest = Math.max(scale, used) - used;
  if (rest > 0) segments.push({ tone: 'rest', grow: rest });
  return segments;
}

/* ---------- BuildKit steps ---------- */

export interface BuildkitStep {
  readonly vertex: number;
  /** BuildKit's name for the step, for example `[3/4] RUN npm ci`. */
  readonly name: string;
  readonly start: number;
  readonly end: number;
  readonly seconds: number;
  readonly cached: boolean;
  readonly state: 'done' | 'live' | 'failed';
  /** The step's share of the build, 0..1. */
  readonly share: number;
}

/** A Dockerfile step names its place, `[2/4]` or `[builder 2/4]`; internal ones do not. */
const STEP_LINE = /^#\d+\s+(\[(?:[\w.-]+\s+)?\d+\/\d+\]\s.*)$/;
const CACHED_LINE = /^#\d+\s+CACHED\s*$/;

/**
 * The Dockerfile steps of a build from its log: one per `vertex` that names a
 * `[n/m]` step, from its first line to its last (to now for the step still
 * running), the failing one from `failed_step`, and its share of the build.
 */
export function buildkitSteps(
  lines: readonly BuildLogLine[],
  build: Pick<Build, 'status' | 'started_at' | 'finished_at' | 'failed_step'>,
  now: number,
): BuildkitStep[] {
  const groups = new Map<number, BuildLogLine[]>();
  for (const line of lines) {
    if (line.vertex === null || line.vertex === undefined) continue;
    const group = groups.get(line.vertex);
    if (group) group.push(line);
    else groups.set(line.vertex, [line]);
  }

  const live = !isBuildTerminal(build.status);
  const found: Array<Omit<BuildkitStep, 'state' | 'share'> & { failed: boolean }> = [];
  for (const [vertex, group] of groups) {
    const named = group.map((line) => STEP_LINE.exec(line.line)).find((match) => match !== null);
    if (!named) continue;
    const times = group.map((line) => parseTime(line.time)).filter((time) => time !== null);
    if (times.length === 0) continue;
    const start = Math.min(...times);
    const end = Math.max(...times);
    found.push({
      vertex,
      name: named[1]!.trim(),
      start,
      end,
      seconds: Math.max(0, (end - start) / 1_000),
      cached: group.some((line) => CACHED_LINE.test(line.line)),
      failed: build.failed_step?.vertex === vertex,
    });
  }
  found.sort((left, right) => left.start - right.start || left.vertex - right.vertex);

  const lastIndex = found.length - 1;
  const steps = found.map((step, index) => {
    const running = live && index === lastIndex;
    const end = running ? Math.max(step.end, now) : step.end;
    return {
      ...step,
      end,
      seconds: Math.max(0, (end - step.start) / 1_000),
      state: step.failed ? ('failed' as const) : running ? ('live' as const) : ('done' as const),
    };
  });

  const started = parseTime(build.started_at);
  const finished = parseTime(build.finished_at) ?? (live ? now : null);
  const buildSeconds =
    started !== null && finished !== null ? Math.max(0, (finished - started) / 1_000) : 0;
  const sum = steps.reduce((total, step) => total + step.seconds, 0);
  const whole = Math.max(buildSeconds, sum);
  return steps.map(({ failed: _failed, ...step }) => ({
    ...step,
    share: whole > 0 ? Math.min(1, step.seconds / whole) : 0,
  }));
}

/* ---------- words and numbers ---------- */

const pad2 = (value: number): string => String(value).padStart(2, '0');

/** A span as the stepper writes it: `0.4s`, `6s`, `1m 08s`, `1h 02m`. */
export const formatSpan = (seconds: number): string => {
  if (seconds < 1) return `${(Math.round(seconds * 10) / 10).toFixed(1)}s`;
  const whole = Math.round(seconds);
  if (whole < 60) return `${whole}s`;
  const minutes = Math.floor(whole / 60);
  if (minutes < 60) return `${minutes}m ${pad2(whole % 60)}s`;
  return `${Math.floor(minutes / 60)}h ${pad2(minutes % 60)}m`;
};

/** A running clock: `0:09`, `1:41`, `1:02:03`. */
export const formatClock = (seconds: number): string => {
  const whole = Math.max(0, Math.floor(seconds));
  const hours = Math.floor(whole / 3_600);
  const minutes = Math.floor((whole % 3_600) / 60);
  const rest = whole % 60;
  return hours > 0 ? `${hours}:${pad2(minutes)}:${pad2(rest)}` : `${minutes}:${pad2(rest)}`;
};

const BUILD_WORDS: Partial<Record<Build['status'], string>> = {
  preparing: 'Preparing the source',
  building: 'Building the image',
  exporting: 'Exporting the image',
  importing: 'Importing the root disk',
};

/** The run in one phrase: what it is doing now, or how it ended. */
export function runSentence(timeline: RunTimeline, replicas = 1): string {
  const { run, state } = timeline;
  if (state === 'succeeded') return 'Deployed';
  if (state === 'cancelled') return 'Build cancelled';
  if (state === 'superseded') return 'Superseded by a newer deployment';
  if (state === 'failed') {
    if (run.build && run.build.status === 'failed') {
      const phase = run.build.failed_phase;
      return phase === null ? 'The build failed' : `Failed while ${BUILD_FAILED_PHASE_LABELS[phase]}`;
    }
    const broke = timeline.steps.find((step) => step.state === 'failed');
    return broke ? `Failed at ${TAPE_STEP_LABELS[broke.step].toLowerCase()}` : 'The deployment failed';
  }
  const step = currentStep(timeline);
  if (step === null || step.state === 'pending') {
    return timeline.kind === 'git' ? 'Starting the deployment' : 'Queued, waiting for the node';
  }
  switch (step.step) {
    case 'queue':
      return 'Waiting for the node';
    case 'build':
      return (run.build && BUILD_WORDS[run.build.status]) ?? 'Building the image';
    case 'pull':
      return 'Pulling the image';
    case 'rootfs':
      return 'Preparing the root disk';
    case 'boot':
      return replicas > 1 ? 'Booting the microVMs' : 'Booting the microVM';
    case 'health':
      return 'Waiting for the health check';
  }
}

export type RunTagTone = 'shift' | 'run' | 'fault' | 'plain';

/** The tag a run wears in the list: what it is, in one word. */
export function runTag(
  timeline: RunTimeline,
  currentDeploymentId: string | null | undefined,
): { word: string; tone: RunTagTone } {
  switch (timeline.state) {
    case 'live':
      return timeline.run.build && !isBuildTerminal(timeline.run.build.status)
        ? { word: 'Building', tone: 'shift' }
        : { word: 'Deploying', tone: 'shift' };
    case 'succeeded':
      return (timeline.run.deployment?.id ?? timeline.run.build?.deployment_id) === currentDeploymentId
        ? { word: 'Active', tone: 'run' }
        : { word: 'Removed', tone: 'plain' };
    case 'failed':
      return { word: 'Failed', tone: 'fault' };
    case 'cancelled':
      return { word: 'Cancelled', tone: 'plain' };
    case 'superseded':
      return { word: 'Superseded', tone: 'plain' };
  }
}
