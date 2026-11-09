// SPDX-License-Identifier: AGPL-3.0-only
import { describe, expect, it } from 'vitest';
import type {
  Build,
  BuildLogLine,
  Deployment,
  DeploymentLogEntry,
} from '../api/generated/model';
import {
  activeDeploymentId,
  buildkitSteps,
  buildSpans,
  currentStep,
  deploymentSpans,
  estimateRun,
  formatClock,
  formatSpan,
  isRunLive,
  joinRuns,
  miniTape,
  miniTapeScale,
  nodeLineSpans,
  remainingSeconds,
  runSentence,
  runTag,
  runTimeline,
  type RunTimeline,
} from './deploy-tape';

const T0 = Date.parse('2026-11-06T17:00:00Z');
const at = (seconds: number): string => new Date(T0 + seconds * 1_000).toISOString();
const ms = (seconds: number): number => T0 + seconds * 1_000;

const deployment = (overrides: Partial<Deployment> = {}): Deployment => ({
  id: 'dep_A',
  app_id: 'app_A',
  image: 'ghcr.io/example/api:1',
  image_digest: null,
  status: 'succeeded',
  phase: 'health',
  error: null,
  trigger: 'api',
  created_by_type: 'user',
  created_by_id: 'usr_A',
  created_at: at(0),
  finished_at: at(40),
  ...overrides,
});

const build = (overrides: Partial<Build> = {}): Build => ({
  id: 'bld_A',
  app_id: 'app_A',
  node_id: 'nod_A',
  source: {
    kind: 'git',
    repo_url: 'https://github.com/example/api.git',
    ref: 'main',
    dockerfile_path: 'Dockerfile',
    context_dir: '.',
    target: null,
    build_args: [],
    secrets: [],
  },
  status: 'succeeded',
  commit: '4f2a9c1e8b7d6c5a4f3e2d1c0b9a8f7e6d5c4b3a',
  image: 'build.quoro.invalid/app_A:bld_A',
  image_digest: null,
  failed_phase: null,
  error: null,
  failed_step: null,
  deployment_id: 'dep_A',
  trigger: 'api',
  refresh_base: false,
  created_by_type: 'user',
  created_by_id: 'usr_A',
  created_at: at(0),
  started_at: at(6),
  finished_at: at(74),
  ...overrides,
});

const entry = (seconds: number, phase: DeploymentLogEntry['phase'], message = '…'): DeploymentLogEntry => ({
  time: at(seconds),
  phase,
  message,
});

/** Pull 6 s, rootfs 12 s, boot 20 s, health 10 s, from second 80 to 128. */
const fullEntries = [
  entry(80, 'pull'),
  entry(82, 'pull'),
  entry(86, 'rootfs'),
  entry(98, 'boot'),
  entry(118, 'health'),
];

describe('deploymentSpans', () => {
  it('times each phase from its first line to the next phase, and the last to finished_at', () => {
    const spans = deploymentSpans(
      deployment({ created_at: at(78), finished_at: at(128) }),
      fullEntries,
      ms(500),
    );
    expect(spans.map((span) => [span.step, span.state, span.seconds])).toEqual([
      ['pull', 'done', 6],
      ['rootfs', 'done', 12],
      ['boot', 'done', 20],
      ['health', 'done', 10],
    ]);
  });

  it('runs the current phase to now and leaves the rest pending', () => {
    const spans = deploymentSpans(
      deployment({ status: 'starting', phase: 'boot', finished_at: null }),
      [entry(0, 'pull'), entry(6, 'rootfs'), entry(18, 'boot')],
      ms(27),
    );
    expect(spans.map((span) => [span.step, span.state, span.seconds])).toEqual([
      ['pull', 'done', 6],
      ['rootfs', 'done', 12],
      ['boot', 'live', 9],
      ['health', 'pending', null],
    ]);
  });

  it('marks the failing phase red and the later ones as never reached', () => {
    const spans = deploymentSpans(
      deployment({ status: 'failed', phase: 'boot', error: 'exit 1', finished_at: at(30) }),
      [entry(0, 'pull'), entry(6, 'rootfs'), entry(18, 'boot')],
      ms(500),
    );
    expect(spans.map((span) => [span.step, span.state, span.seconds])).toEqual([
      ['pull', 'done', 6],
      ['rootfs', 'done', 12],
      ['boot', 'failed', 12],
      ['health', 'skipped', null],
    ]);
  });

  it('gives a phase without lines no span, and ignores lines that claim a build phase', () => {
    const spans = deploymentSpans(
      deployment({ finished_at: at(20) }),
      [entry(0, 'build'), entry(2, 'pull'), entry(12, 'health')],
      ms(500),
    );
    expect(spans.map((span) => [span.step, span.state, span.seconds])).toEqual([
      ['pull', 'done', 10],
      ['rootfs', 'skipped', null],
      ['boot', 'skipped', null],
      ['health', 'done', 8],
    ]);
  });

  it('keeps a queued deployment with no lines pending throughout', () => {
    const spans = deploymentSpans(
      deployment({ status: 'pending', phase: null, finished_at: null }),
      [],
      ms(3),
    );
    expect(spans.every((span) => span.state === 'pending' && span.seconds === null)).toBe(true);
  });

  it('drops a line whose time does not parse', () => {
    const spans = deploymentSpans(
      deployment({ finished_at: at(10) }),
      [{ time: 'not a time', phase: 'pull', message: '' }, entry(4, 'health')],
      ms(500),
    );
    expect(spans[0]?.state).toBe('skipped');
    expect(spans[3]?.seconds).toBe(6);
  });
});

describe('buildSpans', () => {
  it('splits a build into its queue and its work', () => {
    expect(buildSpans(build(), ms(500)).map((span) => [span.step, span.state, span.seconds])).toEqual([
      ['queue', 'done', 6],
      ['build', 'done', 68],
    ]);
  });

  it('runs a queued build in its queue and a building one to now', () => {
    const queued = buildSpans(
      build({ status: 'queued', started_at: null, finished_at: null }),
      ms(4),
    );
    expect(queued.map((span) => [span.state, span.seconds])).toEqual([
      ['live', 4],
      ['pending', null],
    ]);
    const building = buildSpans(build({ status: 'building', finished_at: null }), ms(30));
    expect(building.map((span) => [span.state, span.seconds])).toEqual([
      ['done', 6],
      ['live', 24],
    ]);
  });

  it('marks a failed build red in its build step and a cancelled one as stopped', () => {
    const failed = buildSpans(
      build({ status: 'failed', failed_phase: 'building', finished_at: at(40) }),
      ms(500),
    );
    expect(failed[1]).toMatchObject({ state: 'failed', seconds: 34 });
    const cancelled = buildSpans(build({ status: 'cancelled', finished_at: at(20) }), ms(500));
    expect(cancelled[1]).toMatchObject({ state: 'stopped', seconds: 14 });
  });
});

describe('joinRuns', () => {
  it('joins a build to the deployment it started and keeps other deployments apart, newest first', () => {
    const fromBuild = deployment({ id: 'dep_B', trigger: 'build', created_at: at(80) });
    const redeploy = deployment({ id: 'dep_R', trigger: 'redeploy', created_at: at(300) });
    const orphan = deployment({ id: 'dep_O', trigger: 'build', created_at: at(-900) });
    const runs = joinRuns(
      [build({ id: 'bld_B', deployment_id: 'dep_B' }), build({ id: 'bld_F', status: 'failed', deployment_id: null, created_at: at(-100) })],
      [redeploy, fromBuild, orphan],
    );
    expect(runs.map((run) => [run.build?.id ?? null, run.deployment?.id ?? null])).toEqual([
      [null, 'dep_R'],
      ['bld_B', 'dep_B'],
      ['bld_F', null],
      [null, 'dep_O'],
    ]);
  });

  it('keeps a build whose deployment is not in the page, without one', () => {
    const runs = joinRuns([build({ deployment_id: 'dep_gone' })], []);
    expect(runs).toHaveLength(1);
    expect(runs[0]?.deployment).toBeNull();
  });

  it('draws a build whose deployment fell off the page as settled, not as deploying', () => {
    // The page starts after the build ended: its deployment is older than the page.
    const runs = joinRuns(
      [build({ deployment_id: 'dep_gone' })],
      [deployment({ id: 'dep_N', trigger: 'redeploy', created_at: at(900), finished_at: at(905) })],
    );
    const run = runs.find((candidate) => candidate.build !== null)!;
    expect(run.awaiting).toBe(false);
    const timeline = runTimeline(run, undefined, ms(9_000));
    expect(timeline.state).toBe('succeeded');
    expect(timeline.end).not.toBeNull();
    expect(timeline.steps.slice(2).map((step) => step.state)).toEqual(['skipped', 'skipped', 'skipped', 'skipped']);
    expect(runTag(timeline, 'dep_gone')).toEqual({ word: 'Active', tone: 'run' });
    expect(runTag(timeline, 'dep_other').word).toBe('Removed');
  });

  it('waits on the deployment a build just started before it is named', () => {
    const [run] = joinRuns([build({ deployment_id: null })], []);
    expect(runTimeline(run!, undefined, ms(9_000)).state).toBe('live');
  });

  it('waits on a deployment the build names but the deployments read does not have yet', () => {
    const older = deployment({ id: 'dep_O', trigger: 'build', created_at: at(-300), finished_at: at(-296) });
    const [run] = joinRuns([build({ deployment_id: 'dep_new', finished_at: at(74) })], [older]);
    expect(run!.awaiting).toBe(true);
    const timeline = runTimeline(run!, undefined, ms(75));
    expect(timeline.state).toBe('live');
    expect(timeline.steps.slice(2).every((step) => step.state === 'pending')).toBe(true);
    expect(runSentence(timeline)).toBe('Starting the deployment');
    expect(runTag(timeline, 'dep_O')).toEqual({ word: 'Deploying', tone: 'shift' });
    expect(isRunLive(run)).toBe(true);
    // An empty read predates every deployment; an unread one says nothing.
    expect(joinRuns([build({ deployment_id: 'dep_new' })], [])[0]!.awaiting).toBe(true);
    expect(joinRuns([build({ deployment_id: 'dep_new' })], undefined)[0]!.awaiting).toBe(false);
  });

  it('gives a still-running build the deployment the deployments read already lists for it', () => {
    const running = build({ id: 'bld_N', status: 'exporting', finished_at: null, deployment_id: null, created_at: at(100) });
    const earlier = build({ id: 'bld_P', deployment_id: 'dep_P', created_at: at(0) });
    const runs = joinRuns(
      [running, earlier],
      [
        deployment({ id: 'dep_N', trigger: 'build', status: 'succeeded', created_at: at(160), finished_at: at(162) }),
        deployment({ id: 'dep_P', trigger: 'build', created_at: at(76), finished_at: at(80) }),
      ],
    );
    expect(runs.map((run) => [run.build?.id ?? null, run.deployment?.id ?? null])).toEqual([
      ['bld_N', 'dep_N'],
      ['bld_P', 'dep_P'],
    ]);
    // The build still reads as running, so the run does too, whatever its deployment says.
    expect(runTimeline(runs[0]!, undefined, ms(163)).state).toBe('live');
    // A deployment the newest build cannot have started stays a run of its own.
    const apart = joinRuns(
      [running],
      [deployment({ id: 'dep_X', trigger: 'build', created_at: at(50), finished_at: at(55) })],
    );
    expect(apart.map((run) => [run.build?.id ?? null, run.deployment?.id ?? null])).toEqual([
      ['bld_N', null],
      [null, 'dep_X'],
    ]);
  });
});

describe('activeDeploymentId', () => {
  const timelines = (builds: Build[], deployments: Deployment[] | undefined): RunTimeline[] =>
    joinRuns(builds, deployments).map((run) => runTimeline(run, undefined, ms(10_000)));

  it('is the deployment that started last among the runs that read as deployed', () => {
    const list = timelines(
      [],
      [
        deployment({ id: 'dep_F', status: 'failed', phase: 'boot', created_at: at(200), finished_at: at(204) }),
        deployment({ id: 'dep_B', created_at: at(100), finished_at: at(104) }),
        deployment({ id: 'dep_A', created_at: at(0), finished_at: at(4) }),
      ],
    );
    expect(activeDeploymentId(list)).toBe('dep_B');
    expect(activeDeploymentId([])).toBeNull();
  });

  it('follows the deployment, not the run order, when a redeploy ran during a build', () => {
    // The build started first and its deployment last, so it is current.
    const list = timelines(
      [build({ id: 'bld_A', deployment_id: 'dep_B', created_at: at(0), finished_at: at(60) })],
      [
        deployment({ id: 'dep_B', trigger: 'build', created_at: at(61), finished_at: at(64) }),
        deployment({ id: 'dep_R', trigger: 'redeploy', created_at: at(20), finished_at: at(24) }),
      ],
    );
    expect(list.map((timeline) => timeline.run.deployment?.id)).toEqual(['dep_R', 'dep_B']);
    expect(activeDeploymentId(list)).toBe('dep_B');
  });
});

/**
 * A Git deploy on a single-node cluster, read as the sheet read it: the builds
 * and the deployments land at different moments, and the app read (not used
 * here) at yet another. At every step exactly one row is Active, and it moves
 * to the new run only once that run reads as deployed.
 */
describe('the Deployments tab through a deploy', () => {
  const previous = build({ id: 'bld_P', deployment_id: 'dep_P', created_at: at(0), started_at: at(1), finished_at: at(6) });
  const previousDeployment = deployment({ id: 'dep_P', trigger: 'build', created_at: at(6), finished_at: at(8) });
  const older = deployment({ id: 'dep_O', trigger: 'api', created_at: at(-600), finished_at: at(-596) });
  const next = (overrides: Partial<Build>) =>
    build({ id: 'bld_N', created_at: at(300), started_at: at(301), finished_at: null, deployment_id: null, ...overrides });
  const newDeployment = (overrides: Partial<Deployment>) =>
    deployment({ id: 'dep_N', trigger: 'build', created_at: at(311), finished_at: null, ...overrides });

  const tab = (builds: Build[], deployments: Deployment[], now: number) => {
    const list = joinRuns(builds, deployments).map((run) => runTimeline(run, undefined, now));
    const activeId = activeDeploymentId(list);
    return {
      top: `${runTag(list[0]!, activeId).word} | ${runSentence(list[0]!)}`,
      tags: list.map((timeline) => `${timeline.run.key}: ${runTag(timeline, activeId).word}`),
      active: list.filter((timeline) => runTag(timeline, activeId).word === 'Active').length,
    };
  };

  const steps = [
    {
      what: 'the build is queued',
      builds: [next({ status: 'queued', started_at: null }), previous],
      deployments: [previousDeployment, older],
      top: 'Building | Waiting for the node',
      active: 'build:bld_P',
    },
    {
      what: 'the build exports the image',
      builds: [next({ status: 'exporting' }), previous],
      deployments: [previousDeployment, older],
      top: 'Building | Exporting the image',
      active: 'build:bld_P',
    },
    {
      what: 'the deployments read lists the new deployment before the builds read ends the build',
      builds: [next({ status: 'exporting' }), previous],
      deployments: [newDeployment({ status: 'succeeded', finished_at: at(313) }), previousDeployment, older],
      top: 'Building | Exporting the image',
      active: 'build:bld_P',
    },
    {
      what: 'the builds read names a deployment the deployments read does not have yet',
      builds: [next({ status: 'succeeded', finished_at: at(310), deployment_id: 'dep_N' }), previous],
      deployments: [previousDeployment, older],
      top: 'Deploying | Starting the deployment',
      active: 'build:bld_P',
    },
    {
      what: 'the new deployment rolls out',
      builds: [next({ status: 'succeeded', finished_at: at(310), deployment_id: 'dep_N' }), previous],
      deployments: [newDeployment({ status: 'starting', phase: 'boot' }), previousDeployment, older],
      top: 'Deploying | Starting the deployment',
      active: 'build:bld_P',
    },
    {
      what: 'the new deployment is current',
      builds: [next({ status: 'succeeded', finished_at: at(310), deployment_id: 'dep_N' }), previous],
      deployments: [newDeployment({ status: 'succeeded', finished_at: at(313) }), previousDeployment, older],
      top: 'Active | Deployed',
      active: 'build:bld_N',
    },
  ];

  for (const step of steps) {
    it(`keeps one row Active when ${step.what}`, () => {
      const view = tab(step.builds, step.deployments, ms(312));
      expect(view.top).toBe(step.top);
      expect(view.active).toBe(1);
      expect(view.tags).toContain(`${step.active}: Active`);
    });
  }

  it('keeps one row Active through an image redeploy', () => {
    const current = deployment({ id: 'dep_A', created_at: at(0), finished_at: at(4) });
    const rolling = deployment({ id: 'dep_R', trigger: 'redeploy', status: 'preparing', phase: 'rootfs', created_at: at(100), finished_at: null });
    const during = tab([], [rolling, current], ms(102));
    expect(during.tags).toEqual(['deployment:dep_R: Deploying', 'deployment:dep_A: Active']);
    const after = tab([], [{ ...rolling, status: 'succeeded', phase: 'health', finished_at: at(104) }, current], ms(105));
    expect(after.tags).toEqual(['deployment:dep_R: Active', 'deployment:dep_A: Removed']);
  });
});

describe('runTimeline', () => {
  it('puts a Git run on six steps, from the queue to the health check', () => {
    const run = joinRuns([build()], [deployment({ trigger: 'build', created_at: at(76), finished_at: at(128) })])[0]!;
    const timeline = runTimeline(run, fullEntries, ms(500));
    expect(timeline.kind).toBe('git');
    expect(timeline.state).toBe('succeeded');
    expect(timeline.steps.map((step) => step.seconds)).toEqual([6, 68, 6, 12, 20, 10]);
    expect(timeline.elapsed).toBe(128);
    expect(timeline.recorded).toBe(122);
  });

  it('waits on the deployment steps while the build runs', () => {
    const run = joinRuns([build({ status: 'building', finished_at: null, deployment_id: null })], [])[0]!;
    const timeline = runTimeline(run, undefined, ms(42));
    expect(timeline.state).toBe('live');
    expect(timeline.end).toBeNull();
    expect(timeline.elapsed).toBe(42);
    expect(currentStep(timeline)?.step).toBe('build');
    expect(timeline.steps.slice(2).every((step) => step.state === 'pending')).toBe(true);
    expect(runSentence(timeline)).toBe('Building the image');
  });

  it('ends a failed build at the build, red there, with no deployment steps', () => {
    const run = joinRuns(
      [build({ status: 'failed', failed_phase: 'building', finished_at: at(39), deployment_id: null })],
      [],
    )[0]!;
    const timeline = runTimeline(run, undefined, ms(500));
    expect(timeline.state).toBe('failed');
    expect(timeline.steps.map((step) => step.state)).toEqual([
      'done',
      'failed',
      'skipped',
      'skipped',
      'skipped',
      'skipped',
    ]);
    expect(runSentence(timeline)).toBe('Failed while building the image');
  });

  it('reads a live deployment as the phase it is in', () => {
    const run = joinRuns(
      [],
      [deployment({ status: 'starting', phase: 'boot', finished_at: null })],
    )[0]!;
    const timeline = runTimeline(run, [entry(0, 'pull'), entry(6, 'rootfs'), entry(18, 'boot')], ms(27));
    expect(timeline.kind).toBe('image');
    expect(runSentence(timeline, 2)).toBe('Booting the microVMs');
    expect(runSentence(timeline, 1)).toBe('Booting the microVM');
  });
});

const succeededGit = (total: number, buildSeconds: number): RunTimeline => {
  const start = 0;
  const run = joinRuns(
    [build({ started_at: at(start + 6), finished_at: at(start + 6 + buildSeconds) })],
    [deployment({ trigger: 'build', finished_at: at(total) })],
  )[0]!;
  const pull = 6 + buildSeconds;
  return runTimeline(
    run,
    [entry(pull, 'pull'), entry(pull + 6, 'rootfs'), entry(pull + 18, 'boot'), entry(total - 10, 'health')],
    ms(10_000),
  );
};

describe('estimateRun', () => {
  it('has nothing to say without history', () => {
    expect(estimateRun([], 'git')).toBeNull();
  });

  it('has nothing to say when no earlier run of that kind succeeded', () => {
    const failed = runTimeline(
      joinRuns([build({ status: 'failed', failed_phase: 'building', deployment_id: null })], [])[0]!,
      undefined,
      ms(500),
    );
    expect(estimateRun([failed], 'git')).toBeNull();
    expect(estimateRun([succeededGit(120, 60)], 'image')).toBeNull();
  });

  it('takes the median of each step and of the whole over the last succeeded runs', () => {
    const history = [succeededGit(120, 60), succeededGit(130, 70), succeededGit(200, 140)];
    const estimate = estimateRun(history, 'git');
    expect(estimate).not.toBeNull();
    expect(estimate!.samples).toBe(3);
    expect(estimate!.total).toBe(130);
    expect(estimate!.steps.build).toBe(70);
    expect(estimate!.steps.queue).toBe(6);
    expect(estimate!.steps.health).toBe(10);
  });

  it('looks at no more than the given number of runs', () => {
    const history = [succeededGit(100, 40), succeededGit(110, 50), succeededGit(500, 400)];
    expect(estimateRun(history, 'git', 2)!.total).toBe(105);
  });

  it('says how much a live run has left', () => {
    const estimate = estimateRun([succeededGit(120, 60)], 'git')!;
    const live = runTimeline(
      joinRuns([build({ status: 'building', finished_at: null, deployment_id: null })], [])[0]!,
      undefined,
      ms(36),
    );
    // build: 60 typical, 30 taken; then pull 6, rootfs 12, boot (120-10)-(66+18)=26, health 10.
    expect(remainingSeconds(live, estimate)).toBe(30 + 6 + 12 + 26 + 10);
    expect(remainingSeconds(live, null)).toBeNull();
  });
});

describe('the mini tape', () => {
  it('draws every row on the scale of the longest run', () => {
    const long = succeededGit(120, 60);
    const short = runTimeline(
      joinRuns([], [deployment({ trigger: 'redeploy', finished_at: at(19) })])[0]!,
      [entry(0, 'pull'), entry(3, 'rootfs'), entry(7, 'boot'), entry(15, 'health')],
      ms(500),
    );
    const scale = miniTapeScale([long, short]);
    expect(scale).toBe(long.recorded);
    const longTape = miniTape(long, scale);
    expect(longTape.some((segment) => segment.tone === 'rest')).toBe(false);
    const shortTape = miniTape(short, scale);
    const rest = shortTape.find((segment) => segment.tone === 'rest');
    expect(rest?.grow).toBe(scale - 19);
    const total = shortTape.reduce((sum, segment) => sum + segment.grow, 0);
    expect(total).toBe(scale);
  });

  it('turns red where a run broke', () => {
    const failed = runTimeline(
      joinRuns([build({ status: 'failed', failed_phase: 'building', finished_at: at(39), deployment_id: null })], [])[0]!,
      undefined,
      ms(500),
    );
    expect(miniTape(failed, 100).map((segment) => segment.tone)).toEqual(['done', 'failed', 'rest']);
  });

  it('has a zero scale for an empty list', () => {
    expect(miniTapeScale([])).toBe(0);
  });
});

const line = (seq: number, seconds: number, vertex: number | null, text: string): BuildLogLine => ({
  seq,
  time: at(seconds),
  stream: 'stdout',
  vertex,
  line: text,
});

describe('buildkitSteps', () => {
  const lines = [
    line(1, 6, 1, '#1 [internal] load build definition from Dockerfile'),
    line(2, 7, 3, '#3 [1/4] FROM docker.io/library/node:22-alpine'),
    line(3, 7, 3, '#3 CACHED'),
    line(4, 7, 4, '#4 [2/4] COPY package*.json ./'),
    line(5, 7.4, 4, '#4 DONE 0.4s'),
    line(6, 8, 5, '#5 [3/4] RUN npm ci'),
    line(7, 56, 5, '#5 DONE 48.0s'),
    line(8, 57, 6, '#6 [4/4] RUN npm run build'),
    line(9, 71, 6, '#6 DONE 14.0s'),
    line(10, 72, null, '#7 exporting to image'),
  ];

  it('keeps the Dockerfile steps, timed from their first line to their last', () => {
    const steps = buildkitSteps(lines, build({ started_at: at(6), finished_at: at(74) }), ms(500));
    expect(steps.map((step) => [step.name, step.seconds, step.cached, step.state])).toEqual([
      ['[1/4] FROM docker.io/library/node:22-alpine', 0, true, 'done'],
      ['[2/4] COPY package*.json ./', 0.4, false, 'done'],
      ['[3/4] RUN npm ci', 48, false, 'done'],
      ['[4/4] RUN npm run build', 14, false, 'done'],
    ]);
    expect(steps[2]!.share).toBeCloseTo(48 / 68, 5);
  });

  it('runs the newest step of a live build to now', () => {
    const steps = buildkitSteps(lines.slice(0, 6), build({ status: 'building', started_at: at(6), finished_at: null }), ms(20));
    expect(steps[steps.length - 1]).toMatchObject({ name: '[3/4] RUN npm ci', state: 'live', seconds: 12 });
  });

  it('marks the failing step from the build record', () => {
    const steps = buildkitSteps(
      lines.slice(0, 7),
      build({
        status: 'failed',
        failed_phase: 'building',
        started_at: at(6),
        finished_at: at(57),
        failed_step: { vertex: 5, name: '[3/4] RUN npm ci', error: 'exit code: 1', dockerfile_line: 3, excerpt: [] },
      }),
      ms(500),
    );
    expect(steps.find((step) => step.vertex === 5)?.state).toBe('failed');
  });

  it('names a step in a named stage', () => {
    const steps = buildkitSteps(
      [line(1, 0, 8, '#8 [builder 2/5] RUN cargo build --release'), line(2, 3, 8, '#8 DONE 3.0s')],
      build({ started_at: at(0), finished_at: at(3) }),
      ms(500),
    );
    expect(steps[0]?.name).toBe('[builder 2/5] RUN cargo build --release');
    expect(steps[0]?.share).toBe(1);
  });
});

describe('nodeLineSpans', () => {
  // Issue 638: the first build of a Git app on a single-node cluster, from its
  // real log (times as the node logged them; the repo and base image renamed).
  // The deploy tape said Build took 50 s while the Dockerfile steps added up
  // to about 13 s: the rest was the node pulling the builder image and
  // starting the builder VM, which it logs as lines with no step.
  const real = (seq: number, time: string, vertex: number | null, text: string): BuildLogLine => ({
    seq,
    time: `2026-11-09T${time}Z`,
    stream: 'stdout',
    vertex,
    line: text,
  });
  const firstBuild = build({
    created_at: '2026-11-09T14:40:34.954Z',
    started_at: '2026-11-09T14:40:35.007414069Z',
    finished_at: '2026-11-09T14:41:25.401998515Z',
  });
  const log = [
    real(1, '14:40:35.022929370', null, 'fetching https://github.com/example/site main'),
    real(2, '14:40:35.523831851', null, 'resolved cf78b51189054e9b5c612bffdb572248f17e5d89'),
    real(3, '14:40:35.624274693', null, 'writing the source disk'),
    real(4, '14:40:35.625033077', null, 'preparing the builder image docker.io/moby/buildkit:v0.34.2'),
    real(5, '14:41:02.056489957', null, 'starting the builder VM'),
    real(6, '14:41:04.728956471', 1, '#1 [internal] load build definition from Dockerfile'),
    real(20, '14:41:06.974337423', 6, '#6 [1/2] FROM docker.io/library/nginx:alpine'),
    real(73, '14:41:18.604199874', 6, '#6 extracting sha256:237d424ec589 done'),
    real(74, '14:41:18.617160019', 4, '#4 [2/2] COPY index.html /usr/share/nginx/html/index.html'),
    real(75, '14:41:19.288419225', 4, '#4 DONE 0.7s'),
    real(84, '14:41:20.670683888', 7, '#7 DONE 1.4s'),
    real(86, '14:41:25.418568775', null, 'build succeeded: sha256:e7f77d290384'),
  ];

  it('names the time the steps leave out in the node’s own words', () => {
    const spans = nodeLineSpans(log, firstBuild, Date.parse('2026-11-09T15:00:00Z'));
    expect(spans.map((span) => [span.seq, span.line, span.state])).toEqual([
      [4, 'preparing the builder image docker.io/moby/buildkit:v0.34.2', 'done'],
      [5, 'starting the builder VM', 'done'],
    ]);
    expect(spans[0]!.seconds).toBeCloseTo(26.43, 2);
    expect(spans[1]!.seconds).toBeCloseTo(2.67, 2);
    expect(spans[0]!.share).toBeCloseTo(26.43 / 50.39, 2);
    // With the steps, they account for the build; no phase is invented.
    const steps = buildkitSteps(log, firstBuild, Date.parse('2026-11-09T15:00:00Z'));
    const covered = [...spans, ...steps].reduce((total, item) => total + item.seconds, 0);
    expect(covered).toBeLessThanOrEqual(50.4);
    expect(covered).toBeGreaterThan(40);
  });

  it('runs the line a live build is in to now, even under a second', () => {
    const live = build({ status: 'preparing', started_at: '2026-11-09T14:40:35.007414069Z', finished_at: null });
    const spans = nodeLineSpans(log.slice(0, 4), live, Date.parse('2026-11-09T14:40:36.125Z'));
    expect(spans).toHaveLength(1);
    expect(spans[0]).toMatchObject({ seq: 4, state: 'live' });
    expect(spans[0]!.seconds).toBeCloseTo(0.5, 1);
  });

  it('skips the node’s quick lines and the steps’ own lines', () => {
    const spans = nodeLineSpans(log, firstBuild, Date.parse('2026-11-09T15:00:00Z'));
    expect(spans.some((span) => span.seq === 1 || span.seq === 3 || span.seq === 86)).toBe(false);
    expect(nodeLineSpans([], firstBuild, Date.parse('2026-11-09T15:00:00Z'))).toEqual([]);
  });

  it('leaves BuildKit’s text to BuildKit even when it came without a step', () => {
    const export_ = [
      real(1, '14:41:19.329000000', null, '#7 exporting to oci image format'),
      real(2, '14:41:25.418568775', null, 'build succeeded: sha256:e7f77d290384'),
    ];
    expect(nodeLineSpans(export_, firstBuild, Date.parse('2026-11-09T15:00:00Z'))).toEqual([]);
  });
});

describe('formats and words', () => {
  it('writes spans and clocks the way the stepper does', () => {
    expect(formatSpan(0.4)).toBe('0.4s');
    expect(formatSpan(6)).toBe('6s');
    expect(formatSpan(68)).toBe('1m 08s');
    expect(formatSpan(59.6)).toBe('1m 00s');
    expect(formatSpan(3_725)).toBe('1h 02m');
    expect(formatClock(9.8)).toBe('0:09');
    expect(formatClock(101)).toBe('1:41');
    expect(formatClock(3_723)).toBe('1:02:03');
  });

  it('tags the active run, a replaced one and a failure', () => {
    const done = runTimeline(joinRuns([], [deployment()])[0]!, fullEntries, ms(500));
    expect(runTag(done, 'dep_A')).toEqual({ word: 'Active', tone: 'run' });
    expect(runTag(done, 'dep_other')).toEqual({ word: 'Removed', tone: 'plain' });
    const failed = runTimeline(
      joinRuns([], [deployment({ status: 'failed', phase: 'pull' })])[0]!,
      [entry(0, 'pull')],
      ms(500),
    );
    expect(runTag(failed, null)).toEqual({ word: 'Failed', tone: 'fault' });
    expect(runSentence(failed)).toBe('Failed at pull');
  });
});
