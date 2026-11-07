// SPDX-License-Identifier: AGPL-3.0-only
/**
 * Build state in plain words: the phase rail, the lamp and the small facts
 * (commit, duration, built image) the panel shows next to a build. Pure, so
 * the rail and the labels are tested without a query client.
 */
import type {
  Build,
  BuildFailedPhase,
  BuildStatus,
} from '../api/generated/model';
import type { StationState } from './app-status';
import type { LampTone } from './lamp';

/** The real order of a build, from the queue to the image being ready. */
export const BUILD_PHASES = [
  'queued',
  'preparing',
  'building',
  'exporting',
  'importing',
  'succeeded',
] as const;

export type BuildPhase = (typeof BUILD_PHASES)[number];

export const BUILD_PHASE_LABELS: Record<BuildPhase, string> = {
  queued: 'Queued',
  preparing: 'Prepare source',
  building: 'Build',
  exporting: 'Export image',
  importing: 'Import rootfs',
  succeeded: 'Image ready',
};

/** The phase words of a failure, matching the contract's `failed_phase`. */
export const BUILD_FAILED_PHASE_LABELS: Record<
  Exclude<BuildFailedPhase, null>,
  string
> = {
  preparing: 'preparing the source',
  building: 'building the image',
  exporting: 'exporting the image',
  importing: 'importing the image',
};

const TERMINAL: ReadonlySet<BuildStatus> = new Set([
  'succeeded',
  'failed',
  'cancelled',
]);

/** True once the build cannot move any more. */
export const isBuildTerminal = (status: BuildStatus): boolean =>
  TERMINAL.has(status);

/** True while the node is still working on the build. */
export const isBuildLive = (status: BuildStatus): boolean =>
  !isBuildTerminal(status);

export interface BuildLampState {
  readonly tone: LampTone;
  readonly pulse: boolean;
  readonly statusText: string;
  readonly caption: string;
}

const buildLamp: Record<BuildStatus, BuildLampState> = {
  queued: {
    tone: 'shift',
    pulse: true,
    statusText: 'Queued',
    caption: 'Queued, waiting for the node',
  },
  preparing: {
    tone: 'shift',
    pulse: true,
    statusText: 'Preparing',
    caption: 'Booting the builder and checking out the source',
  },
  building: {
    tone: 'shift',
    pulse: true,
    statusText: 'Building',
    caption: 'Building the image from the Dockerfile',
  },
  exporting: {
    tone: 'shift',
    pulse: true,
    statusText: 'Exporting',
    caption: 'Writing the built image',
  },
  importing: {
    tone: 'shift',
    pulse: true,
    statusText: 'Importing',
    caption: 'Turning the built image into a root disk',
  },
  succeeded: {
    tone: 'run',
    pulse: false,
    statusText: 'Succeeded',
    caption: 'The image is built; its deployment starts next',
  },
  failed: {
    tone: 'fault',
    pulse: false,
    statusText: 'Failed',
    caption: 'The build failed; the step is shown below',
  },
  cancelled: {
    tone: 'idle',
    pulse: false,
    statusText: 'Cancelled',
    caption: 'Cancelled before the build finished',
  },
};

export const buildLampState = (status: BuildStatus): BuildLampState =>
  buildLamp[status];

export interface BuildStation {
  readonly phase: BuildPhase;
  readonly state: StationState;
}

export interface BuildRail {
  readonly stations: ReadonlyArray<BuildStation>;
  /** A rail-wide note, for example a cancellation. */
  readonly caption: string | null;
  /** True while the build is still moving. */
  readonly live: boolean;
}

const notReached = (): BuildStation[] =>
  BUILD_PHASES.map((phase) => ({ phase, state: 'not-reached' as const }));

const failedIndex = (phase: BuildFailedPhase): number => {
  if (phase === null) return -1;
  return BUILD_PHASES.indexOf(phase);
};

/**
 * Derive the phase rail from `status` (and `failed_phase` when it failed). The
 * order is the real sequence, so the stations carry no step numbers.
 */
export const buildRail = (
  build: Pick<Build, 'status' | 'failed_phase'>,
): BuildRail => {
  if (build.status === 'succeeded') {
    return {
      stations: BUILD_PHASES.map((phase) => ({ phase, state: 'done' as const })),
      caption: null,
      live: false,
    };
  }

  if (build.status === 'failed') {
    const failedAt = failedIndex(build.failed_phase);
    return {
      stations:
        failedAt < 0
          ? notReached()
          : BUILD_PHASES.map((phase, index) => ({
              phase,
              state:
                index < failedAt
                  ? 'done'
                  : index === failedAt
                    ? 'failed'
                    : 'not-reached',
            })),
      caption:
        failedAt < 0 ? 'The build failed before it named a phase.' : null,
      live: false,
    };
  }

  if (build.status === 'cancelled') {
    return {
      stations: notReached(),
      caption: 'Cancelled before the build finished.',
      live: false,
    };
  }

  const current = BUILD_PHASES.indexOf(build.status);
  return {
    stations: BUILD_PHASES.map((phase, index) => ({
      phase,
      state:
        index < current
          ? 'done'
          : index === current
            ? 'current'
            : 'not-reached',
    })),
    caption: build.status === 'queued' ? 'Waiting for the node' : null,
    live: true,
  };
};

/** A build id short enough for a plate: the prefix plus a few characters. */
export const shortBuildId = (id: string): string => `${id.slice(0, 10)}…`;

/** The short form of a resolved commit, or a dash before the checkout. */
export const shortCommit = (commit: string | null): string =>
  commit === null ? '—' : commit.slice(0, 7);

/** Build time, from the node starting it (or the queue) to the terminal state. */
export const buildDurationSeconds = (
  build: Pick<Build, 'created_at' | 'started_at' | 'finished_at'>,
  nowMs: number = Date.now(),
): number => {
  const start = Date.parse(build.started_at ?? build.created_at);
  const end = build.finished_at === null ? nowMs : Date.parse(build.finished_at);
  if (Number.isNaN(start) || Number.isNaN(end)) return 0;
  return Math.max(0, (end - start) / 1_000);
};

/** A duration in plain words: `4.2 s`, `38 s`, `2 min 5 s`. */
export const formatBuildDuration = (seconds: number): string => {
  if (seconds < 10) return `${seconds.toFixed(1)} s`;
  if (seconds < 60) return `${Math.round(seconds)} s`;
  const minutes = Math.floor(seconds / 60);
  const rest = Math.round(seconds % 60);
  return rest === 0 ? `${minutes} min` : `${minutes} min ${rest} s`;
};

export const buildDurationLabel = (
  build: Pick<Build, 'created_at' | 'started_at' | 'finished_at'>,
  nowMs: number = Date.now(),
): string => formatBuildDuration(buildDurationSeconds(build, nowMs));

/**
 * The built image is a local reference no node ever pulls. When `image` is one
 * of those, say "built image" and the build id instead of the registry name;
 * return null for an image given by reference.
 */
export const builtImageLabel = (image: string): string | null => {
  const buildId = builtImageBuildId(image);
  if (buildId === null) return null;
  return buildId === '' ? 'built image' : `built image ${buildId}`;
};

/**
 * The short build id inside a built image reference, so a view can set the
 * words in the text face and only the id in the code face. An empty string
 * when the reference has no tag; null for an image given by reference.
 */
export const builtImageBuildId = (image: string): string | null => {
  const prefix = 'build.quoro.invalid/';
  if (!image.startsWith(prefix)) return null;
  const tag = image.slice(image.lastIndexOf(':') + 1);
  return tag === '' ? '' : shortBuildId(tag);
};

/** The commit of the newest successful build, for the source plate. */
export const lastBuiltCommit = (
  builds: ReadonlyArray<Pick<Build, 'status' | 'commit'>>,
): string | null =>
  builds.find((build) => build.status === 'succeeded' && build.commit !== null)
    ?.commit ?? null;
