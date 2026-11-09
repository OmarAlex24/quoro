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
  /**
   * The build succeeded and names its deployment, but the deployments were
   * read before it started: the run is still rolling out, not settled.
   */
  readonly awaiting: boolean;
}

const runStart = (run: DeployRun): number =>
  parseTime(run.build?.created_at ?? run.deployment?.created_at) ?? 0;

const buildBroke = (build: Build): boolean => build.status === 'failed' || build.status === 'cancelled';

/**
 * Join builds to the deployments they started (`Build.deployment_id`), newest
 * run first. A deployment no listed build started (an image deploy, a
 * redeploy, a rollback, or a build older than the page) is a run of its own.
 *
 * The two lists are read on their own and land at different moments, so the
 * join lines them up:
 * - The builds read can be ahead: a build that just succeeded names a
 *   deployment the deployments read does not have yet. If the build ended
 *   after the oldest listed deployment started, its deployment would be on the
 *   page, so the page predates it and the run is `awaiting`. Otherwise it fell
 *   off the page. `deployments` is undefined until it has been read.
 * - The deployments read can be ahead: the newest build's own deployment is
 *   listed while the build still reads as running. An app has one unfinished
 *   build at a time (`startBuild` refuses another), so a deployment a build
 *   started after the newest build was created is that build's.
 */
export function joinRuns(
  builds: readonly Build[],
  deployments: readonly Deployment[] | undefined,
): DeployRun[] {
  const listed = deployments ?? [];
  const byId = new Map(listed.map((deployment) => [deployment.id, deployment]));
  const oldestListed = listed.reduce<number | null>((oldest, deployment) => {
    const created = parseTime(deployment.created_at);
    return created === null ? oldest : oldest === null ? created : Math.min(oldest, created);
  }, null);
  const claimed = new Set<string>();
  const newest = builds[0];
  const unnamed =
    newest && newest.deployment_id === null && !buildBroke(newest)
      ? listed.find(
          (deployment) =>
            deployment.trigger === 'build' &&
            !builds.some((build) => build.deployment_id === deployment.id) &&
            (parseTime(deployment.created_at) ?? 0) >= (parseTime(newest.created_at) ?? Infinity),
        )
      : undefined;

  const runs: DeployRun[] = builds.map((build) => {
    let deployment =
      build.deployment_id !== null ? (byId.get(build.deployment_id) ?? null) : null;
    if (build === newest && unnamed) deployment = unnamed;
    if (deployment) claimed.add(deployment.id);
    const ended = parseTime(build.finished_at) ?? parseTime(build.created_at) ?? 0;
    const awaiting =
      deployments !== undefined &&
      deployment === null &&
      build.status === 'succeeded' &&
      build.deployment_id !== null &&
      (oldestListed === null || ended >= oldestListed);
    return { key: `build:${build.id}`, build, deployment, awaiting };
  });
  for (const deployment of listed) {
    if (!claimed.has(deployment.id)) {
      runs.push({ key: `deployment:${deployment.id}`, build: null, deployment, awaiting: false });
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
  } else if (!deployment && (build?.deployment_id == null || run.awaiting)) {
    // The build succeeded and the deployment it starts is not named yet, or
    // not in the deployments read yet.
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

/** True while a run still moves: its build runs, or its deployment is to come or rolls out. */
export function isRunLive(run: DeployRun | undefined): boolean {
  if (!run) return false;
  const { build, deployment } = run;
  if (build && build.status !== 'succeeded') return !isBuildTerminal(build.status);
  if (!deployment) return build !== null && (build.deployment_id === null || run.awaiting);
  return isDeploymentLive(deployment.status);
}

export type RunPhase = 'building' | 'deploying';

/**
 * What a live run is doing, as its tag in the tab says it (`runTag`):
 * `building` while its build runs, `deploying` while its deployment is to
 * come or rolls out. Null once the run settled, and with no run.
 */
export function runPhase(run: DeployRun | undefined): RunPhase | null {
  if (!run || !isRunLive(run)) return null;
  return run.build && !isBuildTerminal(run.build.status) ? 'building' : 'deploying';
}

/** When a run's deployment started; a build's end stands in for one off the page. */
const deployedAt = (run: DeployRun): number =>
  parseTime(run.deployment?.created_at ?? run.build?.finished_at) ?? runStart(run);

/**
 * The deployment the tab calls Active, from the two lists alone: the run
 * whose deployment started last among those that read as deployed. The
 * server makes a deployment the app's `current_deployment_id` in the same
 * write that marks it succeeded, and a newer deployment supersedes every
 * older one still rolling out, so the newest succeeded deployment is the
 * current one. Reading it from the lists, not from the app read, keeps one
 * row Active through a deploy: the old one until the new run itself reads as
 * deployed. Null when no listed run is deployed.
 */
export function activeDeploymentId(timelines: readonly RunTimeline[]): string | null {
  let active: RunTimeline | null = null;
  for (const timeline of timelines) {
    if (timeline.state !== 'succeeded') continue;
    if (active === null || deployedAt(timeline.run) > deployedAt(active.run)) active = timeline;
  }
  return active ? (active.run.deployment?.id ?? active.run.build?.deployment_id ?? null) : null;
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

/* ---------- the node's own lines ---------- */

/** A line of the node's own is listed with the build steps once it took this long. */
export const NODE_LINE_MIN_SECONDS = 1;

const BUILDKIT_LINE = /^\s*#\d+\s/;

export interface NodeLineSpan {
  readonly seq: number;
  /** The node's words, as logged: `preparing the builder image docker.io/moby/buildkit:v0.34.2`. */
  readonly line: string;
  readonly start: number;
  readonly end: number;
  readonly seconds: number;
  readonly state: 'done' | 'live';
  /** The span's share of the build, 0..1. */
  readonly share: number;
}

/**
 * The time the Dockerfile steps do not cover, said in the node's own words.
 * The build record has no per-phase times, but the node logs what it does
 * outside BuildKit as lines with no step (`vertex` null): fetching the
 * source, pulling the builder image, starting the builder VM. Each such line
 * runs until the next line of any kind, or until the build ended (now while
 * it runs). The first build on a node spends most of its time here. Lines
 * that took under a second are left out, except the one a live build is in.
 * No phase is made up: a stretch no line opens (BuildKit's own export, the
 * import after it) stays unlabelled.
 */
export function nodeLineSpans(
  lines: readonly BuildLogLine[],
  build: Pick<Build, 'status' | 'started_at' | 'finished_at'>,
  now: number,
): NodeLineSpan[] {
  const timed = lines
    .flatMap((line) => {
      const time = parseTime(line.time);
      return time === null ? [] : [{ line, time }];
    })
    .sort((left, right) => left.time - right.time || left.line.seq - right.line.seq);
  const live = !isBuildTerminal(build.status);
  const started = parseTime(build.started_at);
  const finished = parseTime(build.finished_at) ?? (live ? now : null);
  const buildSeconds =
    started !== null && finished !== null ? Math.max(0, (finished - started) / 1_000) : 0;

  const spans: NodeLineSpan[] = [];
  timed.forEach(({ line, time }, index) => {
    if (line.vertex !== null && line.vertex !== undefined) return;
    // BuildKit's own text (`#7 exporting layers`) that came without its step is not the node's.
    if (BUILDKIT_LINE.test(line.line)) return;
    const next = timed[index + 1];
    const running = next === undefined && live;
    const end = next !== undefined ? next.time : running ? now : Math.max(time, finished ?? time);
    const seconds = Math.max(0, (end - time) / 1_000);
    if (seconds < NODE_LINE_MIN_SECONDS && !running) return;
    spans.push({
      seq: line.seq,
      line: line.line.trim(),
      start: time,
      end,
      seconds,
      state: running ? 'live' : 'done',
      share: buildSeconds > 0 ? Math.min(1, seconds / buildSeconds) : 0,
    });
  });
  return spans;
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

/**
 * The tag a run wears in the list: what it is, in one word. `activeId` is the
 * deployment the tab reads as current (`activeDeploymentId`).
 */
export function runTag(
  timeline: RunTimeline,
  activeId: string | null | undefined,
): { word: string; tone: RunTagTone } {
  switch (timeline.state) {
    case 'live':
      return timeline.run.build && !isBuildTerminal(timeline.run.build.status)
        ? { word: 'Building', tone: 'shift' }
        : { word: 'Deploying', tone: 'shift' };
    case 'succeeded':
      return (timeline.run.deployment?.id ?? timeline.run.build?.deployment_id) === activeId
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
