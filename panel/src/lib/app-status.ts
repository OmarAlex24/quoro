import type {
  App,
  AppStatus,
  Deployment,
  DeploymentPhase,
  DomainCertStatus,
  DomainTlsMode,
  Node,
} from '../api/generated/model';
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

/**
 * The apps list refreshes every 2 s while anything deploys and every 10 s
 * otherwise. Pure so the refresh rule can be tested without a query client.
 */
export const appsPollingInterval = (
  apps: ReadonlyArray<Pick<App, 'status'>>,
): number => (apps.some((app) => app.status === 'deploying') ? 2_000 : 10_000);

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

export interface RailStation {
  readonly phase: DeployPhase;
  readonly state: StationState;
}

export interface DeploymentRail {
  readonly stations: ReadonlyArray<RailStation>;
  /** A rail-wide note, e.g. when a deployment was superseded. */
  readonly caption: string | null;
  /** True while the deployment is still moving, so the caller polls every 1 s. */
  readonly live: boolean;
}

const phaseIndex = (phase: DeploymentPhase | null | undefined): number => {
  if (phase === null || phase === undefined || phase === 'build') return -1;
  const index = DEPLOY_PHASES.indexOf(phase);
  return index;
};

const notReached = (): RailStation[] =>
  DEPLOY_PHASES.map((phase) => ({ phase, state: 'not-reached' as const }));

/**
 * Derive the four stations from `status` + `phase`. The order is the real
 * sequence, so the stations are never numbered.
 */
export const deploymentRail = (
  deployment: Pick<Deployment, 'status' | 'phase'>,
): DeploymentRail => {
  const live =
    deployment.status === 'pending' ||
    deployment.status === 'preparing' ||
    deployment.status === 'starting';

  if (deployment.status === 'succeeded') {
    return {
      stations: DEPLOY_PHASES.map((phase) => ({ phase, state: 'done' as const })),
      caption: null,
      live: false,
    };
  }

  if (deployment.status === 'superseded') {
    return {
      stations: notReached(),
      caption: 'Superseded by a newer deployment',
      live: false,
    };
  }

  if (deployment.status === 'failed') {
    const failedAt = phaseIndex(deployment.phase);
    return {
      stations: DEPLOY_PHASES.map((phase, index) => ({
        phase,
        state:
          failedAt < 0
            ? 'not-reached'
            : index < failedAt
              ? 'done'
              : index === failedAt
                ? 'failed'
                : 'not-reached',
      })),
      caption: null,
      live: false,
    };
  }

  // pending, preparing or starting: the current phase is the station lit amber.
  let current = -1;
  if (deployment.status === 'preparing') {
    const index = phaseIndex(deployment.phase);
    current = index >= 0 ? index : 0;
  } else if (deployment.status === 'starting') {
    const index = phaseIndex(deployment.phase);
    current = index >= 0 ? index : 2;
  }

  return {
    stations: DEPLOY_PHASES.map((phase, index) => ({
      phase,
      state:
        current < 0
          ? 'not-reached'
          : index < current
            ? 'done'
            : index === current
              ? 'current'
              : 'not-reached',
    })),
    caption: deployment.status === 'pending' ? 'Queued, waiting for the node' : null,
    live,
  };
};

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
