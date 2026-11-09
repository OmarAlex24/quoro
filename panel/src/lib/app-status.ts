import type {
  App,
  AppDetail,
  AppStatus,
  Build,
  DomainCertStatus,
  DomainServingStatus,
  DomainTlsMode,
  Node,
} from '../api/generated/model';
import { isBuildLive } from './build-status';
import type { LampTone } from './lamp';

/** Everything the app lamp and its caption need, from the aggregated status. */
export interface AppLampState {
  readonly tone: LampTone;
  readonly pulse: boolean;
  readonly statusText: string;
  readonly caption: string;
}

const appLamp: Record<AppStatus, AppLampState> = {
  running: { tone: 'run', pulse: false, statusText: 'Running', caption: 'Running' },
  deploying: {
    tone: 'shift',
    pulse: true,
    statusText: 'Deploying',
    caption: 'Deploying',
  },
  degraded: {
    tone: 'shift',
    pulse: false,
    statusText: 'Degraded',
    caption: 'Degraded: an instance reports trouble',
  },
  down: {
    tone: 'fault',
    pulse: false,
    statusText: 'Down',
    caption: "Down: the app's VM crashed or its health check fails",
  },
  stopped: { tone: 'idle', pulse: false, statusText: 'Stopped', caption: 'Stopped' },
};

export const appLampState = (status: AppStatus): AppLampState => appLamp[status];

const building: AppLampState = {
  tone: 'shift',
  pulse: true,
  statusText: 'Building',
  caption: 'Building a new image from the Git source',
};

/** True while the newest of an app's builds (newest first) is still running. */
export const newestBuildLive = (
  builds: ReadonlyArray<Pick<Build, 'status'>> | null | undefined,
): boolean => {
  const newest = builds?.[0];
  return newest !== undefined && isBuildLive(newest.status);
};

/**
 * The lamp the canvas card and the sheet both show, from the same two reads:
 * the app's status and its newest build. `AppStatus` has no build state, so a
 * Git app keeps its own status (`stopped` on a first build) until the build's
 * deployment starts; while its newest build runs, the panel says Building.
 */
export const serviceLampState = (
  status: AppStatus,
  builds?: ReadonlyArray<Pick<Build, 'status'>> | null,
): AppLampState => (status !== 'deploying' && newestBuildLive(builds) ? building : appLamp[status]);

/**
 * The apps list refreshes every 2 s while anything deploys or builds and every
 * 10 s otherwise. Pure so the refresh rule can be tested without a query client.
 */
export const appsPollingInterval = (
  apps: ReadonlyArray<Pick<App, 'status'>>,
  anyBuildLive = false,
): number => (anyBuildLive || apps.some((app) => app.status === 'deploying') ? 2_000 : 10_000);

/**
 * The canvas lists apps and the sheet reads the open one on its own, and the
 * two reads land at different moments. The canvas draws the open app from the
 * sheet's read, so its card and the sheet's header always say the same thing.
 */
export const withOpenApp = <T extends App>(apps: readonly T[], open: AppDetail | undefined): T[] => {
  if (!open) return [...apps];
  const { instances: _instances, domains: _domains, edge_backends: _edge, ...fields } = open;
  return apps.map((app) => (app.id === open.id ? { ...app, ...fields } : app));
};

/** The four real phases of a rollout, in order. Build (M4) is not a station. */
export const DEPLOY_PHASES = ['pull', 'rootfs', 'boot', 'health'] as const;
export type DeployPhase = (typeof DEPLOY_PHASES)[number];

export const DEPLOY_PHASE_LABELS: Record<DeployPhase, string> = {
  pull: 'Pull image',
  rootfs: 'Prepare rootfs',
  boot: 'Boot',
  health: 'Health check',
};

export type StationState = 'done' | 'current' | 'failed' | 'not-reached';

export const stationTone = (state: StationState): LampTone => {
  switch (state) {
    case 'done':
      return 'run';
    case 'current':
      return 'shift';
    case 'failed':
      return 'fault';
    case 'not-reached':
      return 'idle';
  }
};

export const certTone = (status: DomainCertStatus): LampTone => {
  switch (status) {
    case 'issued':
      return 'run';
    case 'pending':
    case 'renewing':
      return 'shift';
    case 'failed':
      return 'fault';
    case 'not_applicable':
      return 'idle';
  }
};

export const certWords: Record<DomainCertStatus, string> = {
  pending: 'Pending',
  issued: 'Issued',
  renewing: 'Renewing',
  failed: 'Failed',
  not_applicable: 'Not applicable',
};

/** Whether a machine serves the hostname (ADR 0167). */
export const servingTone = (status: DomainServingStatus): LampTone => {
  switch (status) {
    case 'serving':
      return 'run';
    case 'unserved':
      return 'fault';
    case 'pending':
      return 'idle';
  }
};

export const servingWords: Record<DomainServingStatus, string> = {
  serving: 'Serving',
  unserved: 'Not served',
  pending: 'Waiting for an instance',
};

export const tlsWords: Record<DomainTlsMode, string> = {
  acme: 'ACME certificate',
  tunnel: 'Cloudflare Tunnel',
};

export const shortDeploymentId = (id: string): string => `${id.slice(0, 10)}…`;

/** A short, plain relative time for an environment variable's last change. */
export const relativeTime = (iso: string, now: number = Date.now()): string => {
  const then = Date.parse(iso);
  if (Number.isNaN(then)) return 'unknown time';
  const seconds = Math.max(0, Math.round((now - then) / 1_000));
  if (seconds < 45) return 'just now';
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes} min ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours} h ago`;
  const days = Math.round(hours / 24);
  return `${days} d ago`;
};

/**
 * Preselect the first connected node that is not the primary; otherwise the
 * first connected node; otherwise whatever is listed.
 */
export const pickDefaultNode = (
  nodes: ReadonlyArray<Pick<Node, 'id' | 'status' | 'cp_status'>>,
): string => {
  const connected = nodes.filter((node) => node.status === 'connected');
  const preferred =
    connected.find((node) => node.cp_status !== 'primary') ?? connected[0] ?? nodes[0];
  return preferred?.id ?? '';
};
