import type {
  App,
  AppEnvVar,
  AppHaCost,
  AppSource,
  AuditExport,
  Backup,
  BackupRepository,
  Build,
  BuildLogLine,
  Deployment,
  DeploymentLogEntry,
  Domain,
  EdgeBackend,
  HaPolicyCost,
  Node,
  Project,
  Provider,
  ProviderConfig,
  User,
  Vm,
  Volume,
  WarmCopy,
} from '../api/generated/model';
import { isBuildLive } from '../lib/build-status';
import { warmCopyAt } from './warm-clock';

/** Crockford base32, the alphabet the contract allows after the id prefix. */
const ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

const encode = (value: number, length: number): string => {
  let rest = Math.max(0, Math.floor(value));
  let out = '';
  for (let index = 0; index < length; index += 1) {
    out = ALPHABET[rest % 32] + out;
    rest = Math.floor(rest / 32);
  }
  return out;
};

let sequence = 0;

/** A fixed start for the time part of mock ids. */
const MOCK_EPOCH = Date.UTC(2026, 0, 1);

/**
 * A contract-shaped id: `<prefix>_` plus 26 Crockford characters. Ids come
 * from a counter, not the clock, so the seeded data has the same ids after
 * every reload: what the browser keeps by id (a canvas layout) still applies.
 */
export const mockId = (prefix: string): string => {
  sequence += 1;
  const time = encode(MOCK_EPOCH + sequence, 10);
  const randomness = encode(sequence * 7919 + 104_729, 16);
  return `${prefix}_${time}${randomness}`;
};

export const now = (): string => new Date().toISOString();

export interface MockDb {
  setupDone: boolean;
  owner: User | null;
  /** Every human identity: the owner created at setup plus those invited. */
  users: User[];
  /** The password chosen at setup; the mock compares against it. */
  password: string | null;
  /** Id of the signed-in user, or null. The panel's cookie, simulated. */
  sessionUserId: string | null;
  node: Node;
  /** Further nodes of the mock cluster; `node` stays the primary. */
  extraNodes: Node[];
  projects: Project[];
  vms: Vm[];
  apps: App[];
  deployments: Deployment[];
  deploymentLogs: Record<string, DeploymentLogEntry[]>;
  builds: Build[];
  buildLogs: Record<string, BuildLogLine[]>;
  domains: Domain[];
  appEnv: Record<string, AppEnvVar[]>;
  /** The edge's view of an app's instances, keyed by app id. */
  edgeBackends: Record<string, EdgeBackend[]>;
  volumes: Volume[];
  backups: Backup[];
  backupRepositories: BackupRepository[];
  auditExports: AuditExport[];
  /** Provider entries, without their secrets: the mock keeps only `has_secret`. */
  providers: Provider[];
  /**
   * The repository config objects the buckets hold, one per bucket and prefix
   * (`repositoryObjectKey`). Creating a repository where one already sits
   * answers `conflict`, as the server's `put_new` probe does.
   */
  repositoryObjects: string[];
  /** The redeploy mock mode keeps in progress (`advanceDeployCycle`); null when none. */
  deployCycle: DeployCycle | null;
}

/**
 * A deploy that mock mode keeps in progress, so the deploying states show in
 * demos: one redeploy of an app after another, each walking its phases over
 * three minutes, the next starting a few seconds after the last succeeded.
 */
export interface DeployCycle {
  appId: string;
  /** Every deployment the cycle started, oldest first; empty until the owner first signs in. */
  deploymentIds: string[];
  /**
   * True once a person took the app over: a deploy or build of theirs, a
   * stop, a restart, a delete, or an action on one of its instances.
   */
  stopped: boolean;
  /** The app's `restart_generation` after the cycle's own newest redeploy bumped it. */
  generation: number;
  /** The app's instances when the newest redeploy started. */
  vmIds: string[];
}

export const db: MockDb = {
  setupDone: false,
  owner: null,
  users: [],
  password: null,
  sessionUserId: null,
  node: {
    id: mockId('nod'),
    name: 'lab-3',
    roles: ['node'],
    cp_status: 'primary',
    status: 'connected',
    public_key: 'c2VlZC1sYWItMy1wdWJsaWMta2V5',
    address: '10.0.0.3',
    behind_nat: false,
    labels: { room: 'lab', rack: 'r1' },
    // 16 CPUs: the Harbor project's warm apps run here too, and lab-3 is
    // not overcommitted.
    capacity: { cpus: 16, memory_mb: 24_576, disk_gb: 200 },
    // The nodes handler reports what the VMs placed here add up to.
    allocated: { cpus: 0, memory_mb: 0, disk_gb: 0 },
    kvm: { available: true, kernel: '6.12.0-lab', nested: false },
    engine_version: '0.1.0',
    agent_version: '0.1.0',
    desired_rev: 1,
    applied_rev: 1,
    log_seq: 1,
    // Has the `node` role and holds the mock cluster's newest entry.
    log_lag: 0,
    fence_failure: null,
    tunnel: null,
    last_seen_at: now(),
    lost_at: null,
    created_at: now(),
  },
  extraNodes: [],
  projects: [],
  vms: [],
  apps: [],
  deployments: [],
  deploymentLogs: {},
  builds: [],
  buildLogs: {},
  domains: [],
  appEnv: {},
  edgeBackends: {},
  volumes: [],
  backups: [],
  backupRepositories: [],
  auditExports: [],
  providers: [],
  repositoryObjects: [],
  deployCycle: null,
};

/** Where a repository's config object sits: the provider's endpoint and bucket, and the prefix. */
export const repositoryObjectKey = (config: ProviderConfig, prefix: string): string =>
  `${String(config.endpoint ?? '')}|${String(config.bucket ?? '')}|${prefix}`;

const timers = new Set<number>();

/**
 * How much slower than its default the mock runs what it simulates: builds,
 * rollouts, convergence. `localStorage['quoro.mock.pace']`, 1 to 60, read on
 * every call, so a demo can watch a deploy at a believable speed. Tests run
 * without storage and keep the default.
 */
export const mockPace = (): number => {
  try {
    const raw = Number(globalThis.localStorage?.getItem('quoro.mock.pace') ?? '');
    return Number.isFinite(raw) && raw >= 1 ? Math.min(60, raw) : 1;
  } catch {
    return 1;
  }
};

/** Run `fn` later, so a test or a reset can see it as pending. */
export const schedule = (fn: () => void, delay: number): void => {
  const id = window.setTimeout(() => {
    timers.delete(id);
    fn();
  }, delay * mockPace());
  timers.add(id);
};

/** Newest first by `created_at`, the order the contract lists deployments and builds in. */
export const newestFirst = <T extends { created_at: string }>(items: readonly T[]): T[] =>
  items
    .slice()
    .reverse()
    .sort((left, right) => Date.parse(right.created_at) - Date.parse(left.created_at));

/**
 * Simulated convergence: the node reports each step, then settles on the
 * desired state. Deleted VMs simply drop out of the lookup and stop moving.
 */
export const converge = (
  vmId: string,
  steps: ReadonlyArray<readonly [number, Vm['observed_status']]>,
): void => {
  for (const [delay, status] of steps) {
    schedule(() => {
      const vm = db.vms.find((candidate) => candidate.id === vmId);
      if (!vm) return;
      vm.observed_status = status;
      vm.updated_at = now();
    }, delay);
  }
};

export const findApp = (appId: string): App | undefined =>
  db.apps.find((candidate) => candidate.id === appId);

/** When each seeded warm copy's progress was measured, by app id (`warmCopyAt`). */
const warmMeasuredAt = new Map<string, number>();

/**
 * An app as a read answers it at `now`: its warm copy's progress moved on
 * with the clock, as the primary would report it (`warmCopyAt`, a
 * simulation). The stored app is left as seeded.
 */
export const appAt = <T extends App>(app: T, now: number = Date.now()): T => {
  const measuredAt = warmMeasuredAt.get(app.id);
  if (app.warm === null || app.warm === undefined || measuredAt === undefined) return app;
  return { ...app, warm: warmCopyAt(app.warm, measuredAt, now) };
};

export const findDeployment = (deploymentId: string): Deployment | undefined =>
  db.deployments.find((candidate) => candidate.id === deploymentId);

const GIB = 1024 ** 3;
/** Rates the contract names as defaults before a measurement exists (ADR 0147). */
const DEFAULT_DOWNLOAD_RATE = 50 * 1024 ** 2;

/** The one volume M5a allows an app to have. */
const activeVolume = (appId: string): Volume | undefined =>
  db.volumes.find(
    (volume) =>
      volume.app_id === appId &&
      (volume.state === 'creating' ||
        volume.state === 'ready' ||
        volume.state === 'restoring'),
  );

/**
 * The mock's `ha_cost` (openapi.yaml `AppHaCost`): one price per policy from
 * the volume, its node's snapshot method and the kept backups. Switching to
 * `cold` moves its storage from the sparse file's estimate to the bytes the
 * kept backups actually uploaded, so the table changes.
 */
export const mockHaCost = (app: App): AppHaCost => {
  const volume = activeVolume(app.id);
  const repository = db.backupRepositories[0];
  const kept = db.backups
    .filter(
      (backup) =>
        backup.app_id === app.id && backup.status === 'succeeded' && !backup.pruned_at,
    )
    .sort((left, right) => Date.parse(right.created_at) - Date.parse(left.created_at))
    .slice(0, app.backup_schedule.keep);

  const storedBytes = kept.reduce((total, backup) => total + (backup.uploaded_bytes ?? 0), 0);
  const coldStorage =
    app.ha_policy === 'cold' && kept.length > 0
      ? storedBytes
      : (volume?.allocated_bytes ?? null);
  const coldRestore =
    coldStorage === null ? null : Math.round(coldStorage / DEFAULT_DOWNLOAD_RATE);
  const coldPause = volume === undefined ? null : 0;

  const none: HaPolicyCost = {
    policy: 'none',
    available: true,
    reserved: { vcpus: 0, memory_mb: 0, disk_gb: 0 },
    storage_bytes: 0,
    restore_estimate_s: null,
    pause_estimate_ms: null,
  };
  const cold: HaPolicyCost = {
    policy: 'cold',
    available: volume !== undefined && repository !== undefined,
    unavailable_reason:
      volume === undefined ? 'stateless' : repository === undefined ? 'no_repository' : null,
    reserved: { vcpus: 0, memory_mb: 0, disk_gb: 0 },
    storage_bytes: volume === undefined ? null : coldStorage,
    restore_estimate_s: volume === undefined ? null : coldRestore,
    pause_estimate_ms: coldPause,
  };
  const hot: HaPolicyCost = {
    policy: 'hot',
    available: false,
    unavailable_reason: 'not_in_this_release',
    reserved: {
      vcpus: app.resources.vcpus,
      memory_mb: app.resources.memory_mb,
      disk_gb: app.resources.disk_gb + (volume?.size_gb ?? 0),
    },
    storage_bytes: null,
    restore_estimate_s: null,
    pause_estimate_ms: null,
  };
  // The mock's `updateApp` refuses `warm`, so only the seeded warm apps price
  // it as the M5b primary does: disk only on the warm node, the copy's bytes,
  // the 10 s start estimate before one was observed, and no pause (reflink).
  // With no eligible node it is `no_warm_node`.
  const warm: HaPolicyCost =
    app.warm === null || app.warm === undefined
      ? { ...hot, policy: 'warm' }
      : {
          policy: 'warm',
          available: app.warm.node_id !== null,
          unavailable_reason: app.warm.node_id === null ? 'no_warm_node' : null,
          reserved: { vcpus: 0, memory_mb: 0, disk_gb: app.resources.disk_gb + (volume?.size_gb ?? 0) },
          storage_bytes: volume?.allocated_bytes ?? null,
          restore_estimate_s: app.warm.node_id === null ? null : 10,
          pause_estimate_ms: 0,
        };

  return {
    snapshot_method: volume === undefined ? null : 'reflink',
    policies: [none, cold, warm, hot],
    computed_at: now(),
  };
};

const appInstances = (appId: string): Vm[] =>
  db.vms.filter((vm) => vm.app_id === appId);

/**
 * Simulated rollout: pending, pull, rootfs, boot, then succeeded over ~3 s.
 * Every step appends a progress line and moves the app to `deploying` until it
 * lands on `running`.
 */
export const runDeployment = (
  appId: string,
  image: string,
  trigger: Deployment['trigger'],
): Deployment => {
  for (const older of db.deployments) {
    if (
      older.app_id === appId &&
      (older.status === 'pending' ||
        older.status === 'preparing' ||
        older.status === 'starting')
    ) {
      older.status = 'superseded';
      older.finished_at = now();
    }
  }

  const app = findApp(appId);
  const deployment: Deployment = {
    id: mockId('dep'),
    app_id: appId,
    image,
    image_digest: null,
    status: 'pending',
    phase: null,
    error: null,
    trigger,
    created_by_type: 'user',
    created_by_id: db.sessionUserId ?? 'system',
    created_at: now(),
    finished_at: null,
  };
  db.deployments.push(deployment);
  db.deploymentLogs[deployment.id] = [
    {
      time: now(),
      phase: 'pull',
      message: `Queued ${image} on ${db.node.name}`,
    },
  ];

  if (app) {
    app.status = 'deploying';
    app.image = image;
    app.current_deployment_id = deployment.id;
    app.updated_at = now();
  }

  let instance = appInstances(appId)[0];
  if (!instance && app) {
    instance = {
      id: mockId('vm'),
      project_id: app.project_id,
      app_id: appId,
      node_id: app.placement.node_ids[0] ?? db.node.id,
      name: `${app.name}-1`,
      vcpus: app.resources.vcpus,
      memory_mb: app.resources.memory_mb,
      disk_gb: app.resources.disk_gb,
      image,
      desired_status: 'running',
      observed_status: 'creating',
      ip: null,
      restart_generation: app.restart_generation,
      restart_count: 0,
      last_exit_code: null,
      created_at: now(),
      updated_at: now(),
    };
    db.vms.push(instance);
  }

  const steps: ReadonlyArray<{
    delay: number;
    status: Deployment['status'];
    phase: Deployment['phase'];
    message: string;
    vm?: Vm['observed_status'];
  }> = [
    {
      delay: 500,
      status: 'preparing',
      phase: 'pull',
      message: `Pulling ${image}`,
      vm: 'creating',
    },
    {
      delay: 1_100,
      status: 'preparing',
      phase: 'rootfs',
      message: 'Preparing the root filesystem',
    },
    {
      delay: 1_800,
      status: 'starting',
      phase: 'boot',
      message: 'Booting the microVM',
      vm: 'starting',
    },
    {
      delay: 2_600,
      status: 'starting',
      phase: 'health',
      message: `Waiting for the health check on port ${app?.port ?? 80}`,
    },
    {
      delay: 3_000,
      status: 'succeeded',
      phase: 'health',
      message: 'Health check passed; the app is running',
      vm: 'running',
    },
  ];

  for (const step of steps) {
    schedule(() => {
      const current = findDeployment(deployment.id);
      if (!current) return;
      current.status = step.status;
      current.phase = step.phase;
      current.finished_at = step.status === 'succeeded' ? now() : null;
      db.deploymentLogs[current.id] = [
        ...(db.deploymentLogs[current.id] ?? []),
        { time: now(), phase: step.phase ?? 'pull', message: step.message },
      ];

      if (step.status === 'succeeded' && app) {
        app.status = 'running';
        app.updated_at = now();
      } else if (app) {
        app.status = 'deploying';
        app.updated_at = now();
      }

      const vm = instance
        ? db.vms.find((candidate) => candidate.id === instance.id)
        : undefined;
      if (vm && step.vm) {
        vm.observed_status = step.vm;
        vm.image = image;
        if (step.vm === 'running') vm.ip = '10.0.0.21';
        vm.updated_at = now();
      }
    }, step.delay);
  }

  return deployment;
};

/** Who the seeded records name before setup creates the owner; setup hands them to the owner. */
export const SEED_OWNER_ID = 'usr_01JC9Z6Q5W7X8Y9Z0A1B2C3D4E';

/** Seconds a cycle's deployment stays `pending` before it pulls. */
const CYCLE_QUEUE_S = 2;

/**
 * A cycle's phases: their status, how long each takes and what the
 * deployment's log says when it begins. Slower than the seeded history's
 * rollouts, so a demo can watch each phase: three minutes in all.
 */
export const CYCLE_PHASES: ReadonlyArray<{
  phase: NonNullable<Deployment['phase']>;
  status: Deployment['status'];
  seconds: number;
  message: (image: string, port: number) => string;
}> = [
  { phase: 'pull', status: 'preparing', seconds: 35, message: (image) => `Pulling ${image}` },
  { phase: 'rootfs', status: 'preparing', seconds: 45, message: () => 'Preparing the root filesystem' },
  { phase: 'boot', status: 'starting', seconds: 50, message: () => 'Booting the microVM' },
  {
    phase: 'health',
    status: 'starting',
    seconds: 48,
    message: (_image, port) => `Waiting for the health check on port ${port}`,
  },
];

/** Seconds from a cycle's deployment being created to it succeeding. */
export const CYCLE_SECONDS =
  CYCLE_QUEUE_S + CYCLE_PHASES.reduce((total, step) => total + step.seconds, 0);

/** Seconds between one cycle's success and the next redeploy. */
export const CYCLE_GAP_S = 10;

const isDeploymentLive = (status: Deployment['status']): boolean =>
  status === 'pending' || status === 'preparing' || status === 'starting';

const setIfChanged = <T extends object, K extends keyof T>(item: T, key: K, value: T[K]): boolean => {
  if (item[key] === value) return false;
  item[key] = value;
  return true;
};

const nodeName = (nodeId: string): string =>
  [db.node, ...db.extraNodes].find((node) => node.id === nodeId)?.name ?? nodeId;

/** The app's instances meant to run, in the order the rollout takes them. */
const rolloutInstances = (appId: string): Vm[] =>
  appInstances(appId).filter((vm) => vm.desired_status === 'running');

/**
 * Write what a cycle's deployment, its app and the app's instances look like
 * at `nowMs`, from the deployment's `created_at` alone: the same moment
 * always gives the same records, and phases only move forward. The old
 * instances keep serving while the image is pulled and the root filesystem
 * prepared. During `boot` the rollout takes one node at a time (ADR 0142):
 * each instance boots in its own share of the phase while the others serve.
 * Only instances meant to run are touched.
 */
const applyCycleAt = (deployment: Deployment, app: App, nowMs: number): void => {
  const start = Date.parse(deployment.created_at);
  const elapsed = (nowMs - start) / 1_000;
  const at = (seconds: number): string => iso(start + seconds * 1_000);
  const instances = rolloutInstances(app.id);

  let status: Deployment['status'] = 'pending';
  let phase: Deployment['phase'] = null;
  const logs: DeploymentLogEntry[] = [];
  /** Each instance's share of `boot`, in seconds from the deployment's start. */
  const slots = new Map<string, readonly [number, number]>();
  let clock = CYCLE_QUEUE_S;
  for (const step of CYCLE_PHASES) {
    if (step.phase === 'boot' && instances.length > 0) {
      const share = step.seconds / instances.length;
      instances.forEach((vm, index) => {
        const from = clock + index * share;
        slots.set(vm.id, [from, from + share]);
        if (elapsed >= from) {
          logs.push({ time: at(from), phase: 'boot', message: `Booting ${vm.name} on ${nodeName(vm.node_id)}` });
        }
      });
    } else if (elapsed >= clock) {
      logs.push({ time: at(clock), phase: step.phase, message: step.message(deployment.image, app.port) });
    }
    if (elapsed < clock) break;
    status = step.status;
    phase = step.phase;
    clock += step.seconds;
  }
  const done = elapsed >= CYCLE_SECONDS;
  if (done) {
    logs.push({ time: at(CYCLE_SECONDS), phase: 'health', message: 'Health check passed; the app is running' });
    status = 'succeeded';
    // A redeploy runs the image the app already runs, so its digest is the current one's.
    if (deployment.image_digest === null && app.current_deployment_id !== deployment.id) {
      deployment.image_digest =
        (app.current_deployment_id ? findDeployment(app.current_deployment_id)?.image_digest : null) ?? null;
    }
  }

  deployment.status = status;
  deployment.phase = phase;
  deployment.finished_at = done ? at(CYCLE_SECONDS) : null;
  if ((db.deploymentLogs[deployment.id]?.length ?? -1) !== logs.length) {
    db.deploymentLogs[deployment.id] = logs;
  }

  const appChanged = [
    setIfChanged(app, 'status', done ? 'running' : 'deploying'),
    // The server makes it the app's current deployment in the write that marks it succeeded.
    done ? setIfChanged(app, 'current_deployment_id', deployment.id) : false,
  ].some(Boolean);
  if (appChanged) app.updated_at = now();

  for (const vm of instances) {
    const [from, to] = slots.get(vm.id) ?? [Number.POSITIVE_INFINITY, Number.POSITIVE_INFINITY];
    const booted = elapsed >= from;
    const changed = [
      setIfChanged(vm, 'observed_status', booted && elapsed < to && !done ? 'starting' : 'running'),
      booted ? setIfChanged(vm, 'restart_generation', app.restart_generation) : false,
      booted ? setIfChanged(vm, 'image', deployment.image) : false,
    ].some(Boolean);
    if (changed) vm.updated_at = now();
  }
};

/** Start the cycle's next redeploy at `startMs`: `pending`, by the owner, of the image the app runs. */
const startCycleDeployment = (cycle: DeployCycle, app: App, owner: User, startMs: number): Deployment => {
  const deployment: Deployment = {
    id: mockId('dep'),
    app_id: app.id,
    image: app.image,
    image_digest: null,
    status: 'pending',
    phase: null,
    error: null,
    trigger: 'redeploy',
    created_by_type: 'user',
    created_by_id: owner.id,
    created_at: iso(startMs),
    finished_at: null,
  };
  db.deployments.push(deployment);
  db.deploymentLogs[deployment.id] = [];
  cycle.deploymentIds.push(deployment.id);
  // A redeploy bumps the counter (openapi.yaml, `App.restart_generation`).
  app.restart_generation += 1;
  app.updated_at = iso(startMs);
  cycle.generation = app.restart_generation;
  cycle.vmIds = appInstances(app.id).map((vm) => vm.id);
  applyCycleAt(deployment, app, startMs);
  return deployment;
};

/**
 * Why a person's action took the app over from the cycle, as the error its
 * deployment ends with if it was still rolling out; null while nobody did.
 * A newer deployment supersedes it instead (`runDeployment`).
 */
const takeover = (cycle: DeployCycle, app: App, deployment: Deployment): string | null => {
  const newest = newestFirst(db.deployments.filter((item) => item.app_id === app.id))[0];
  if (newest?.id !== deployment.id) return 'A newer deployment of the app started.';
  if (app.status === 'stopped') return 'The app was stopped before the deployment finished.';
  if (app.restart_generation !== cycle.generation) return 'The app was restarted before the deployment finished.';
  const instances = appInstances(app.id);
  const same =
    instances.length === cycle.vmIds.length && instances.every((vm) => cycle.vmIds.includes(vm.id));
  if (!same || instances.some((vm) => vm.desired_status !== 'running')) {
    return 'An instance was stopped or removed before the deployment finished.';
  }
  return null;
};

/**
 * Bring the deploy cycle up to `nowMs`. The mock handlers call it before
 * they answer, so every read sees the cycle where the clock says it is,
 * without timers. Its first redeploy starts when the owner first signs in,
 * so it is theirs. Once a deployment succeeded and the gap passed, the next
 * one starts; after a long pause only the current one is started, never
 * every one that was missed.
 *
 * Before it writes anything, it checks whether a person took the app over:
 * a deployment of theirs, a stop, a restart, or an action on one of its
 * instances. Then it lets go for good, and a rollout of its own still under
 * way ends as failed (or was already superseded). A build of theirs lets the
 * rollout under way finish, as the server does, but no new one starts.
 */
export const advanceDeployCycle = (nowMs: number = Date.now()): void => {
  const cycle = db.deployCycle;
  if (!cycle || cycle.stopped) return;
  if (cycle.deploymentIds.length === 0) {
    const app = findApp(cycle.appId);
    if (!app) {
      cycle.stopped = true;
      return;
    }
    if (!db.owner || db.sessionUserId !== db.owner.id) return;
    startCycleDeployment(cycle, app, db.owner, nowMs);
    return;
  }
  // Two passes at most: finish the current deployment, then start the next.
  for (let pass = 0; pass < 2; pass += 1) {
    const app = findApp(cycle.appId);
    const lastId = cycle.deploymentIds[cycle.deploymentIds.length - 1];
    const deployment = lastId ? findDeployment(lastId) : undefined;
    if (!app || !deployment || !db.owner || deployment.status === 'superseded' || deployment.status === 'failed') {
      cycle.stopped = true;
      return;
    }
    const reason = takeover(cycle, app, deployment);
    if (reason !== null) {
      if (isDeploymentLive(deployment.status)) {
        deployment.status = 'failed';
        deployment.error = reason;
        deployment.finished_at = iso(nowMs);
      }
      cycle.stopped = true;
      return;
    }

    applyCycleAt(deployment, app, nowMs);
    if (deployment.status !== 'succeeded') return;

    if (db.builds.some((build) => build.app_id === app.id && isBuildLive(build.status))) {
      cycle.stopped = true;
      return;
    }
    const nextAt = Date.parse(deployment.created_at) + (CYCLE_SECONDS + CYCLE_GAP_S) * 1_000;
    if (nowMs < nextAt) return;
    const periodMs = (CYCLE_SECONDS + CYCLE_GAP_S) * 1_000;
    const startMs = nowMs - nextAt >= periodMs ? nowMs - ((nowMs - nextAt) % periodMs) : nextAt;
    startCycleDeployment(cycle, app, db.owner, startMs);
  }
};

/**
 * Atlas's web is the app mock mode keeps redeploying. Its first redeploy
 * starts when the owner first signs in (`advanceDeployCycle`). Seeded last,
 * so every id seeded before keeps its value.
 */
const seedDeployCycle = (): void => {
  const atlas = db.projects.find((project) => project.slug === 'atlas');
  const web = db.apps.find((app) => app.project_id === atlas?.id && app.name === 'web');
  if (!web) return;
  db.deployCycle = { appId: web.id, deploymentIds: [], stopped: false, generation: web.restart_generation, vmIds: [] };
};

/** One project, one running app and its deployment, domain and variables. */
const seed = (): void => {
  const createdAt = now();
  const hoursAgo = (hours: number): string =>
    new Date(Date.parse(createdAt) - hours * 3_600_000).toISOString();
  const secondsAfter = (time: string, seconds: number): string =>
    new Date(Date.parse(time) + seconds * 1_000).toISOString();
  const project: Project = {
    id: mockId('prj'),
    slug: 'blog',
    name: 'Blog',
    created_at: createdAt,
    updated_at: createdAt,
  };
  db.projects.push(project);

  const appId = mockId('app');
  const deploymentId = mockId('dep');
  const instanceId = mockId('vm');
  const replicaInstanceId = mockId('vm');

  const replicaNode: Node = {
    id: mockId('nod'),
    name: 'lab-4',
    roles: ['node', 'edge'],
    // Not the primary; `replica` is reserved for read serving (M6).
    cp_status: null,
    status: 'connected',
    public_key: 'c2VlZC1sYWItNC1wdWJsaWMta2V5',
    address: '10.0.0.4',
    behind_nat: false,
    labels: { room: 'lab', rack: 'r2' },
    // 12 CPUs: losing lab-3 still fits, the Harbor apps with a complete
    // warm copy included (their copies are here).
    capacity: { cpus: 12, memory_mb: 8_192, disk_gb: 60 },
    allocated: { cpus: 0, memory_mb: 0, disk_gb: 0 },
    kvm: { available: true, kernel: '6.12.0-lab', nested: false },
    engine_version: '0.1.0',
    agent_version: '0.1.0',
    desired_rev: 1,
    applied_rev: 1,
    log_seq: 1,
    // Has the `node` role and holds the mock cluster's newest entry.
    log_lag: 0,
    fence_failure: null,
    tunnel: null,
    last_seen_at: createdAt,
    lost_at: null,
    created_at: createdAt,
  };
  db.extraNodes.push(replicaNode);

  const app: App = {
    id: appId,
    project_id: project.id,
    name: 'blog',
    stateful: true,
    ha_policy: 'none',
    warm: null,
    backup_schedule: { interval_h: 24, keep: 7 },
    replicas: 2,
    image: 'docker.io/library/nginx:alpine',
    port: 80,
    health_check: { path: '/', interval_s: 10, timeout_s: 2, failures: 3 },
    resources: { vcpus: 1, memory_mb: 512, disk_gb: 2 },
    placement: { node_ids: [db.node.id, replicaNode.id] },
    // Down, as its open incident says ("blog is down"): both instances failed
    // their health checks and left the edge's rotation, and after the restart
    // 12 minutes ago both crashed. No instance runs.
    status: 'down',
    current_deployment_id: deploymentId,
    restart_generation: 1,
    created_at: createdAt,
    updated_at: createdAt,
  };
  db.apps.push(app);

  db.deployments.push({
    id: deploymentId,
    app_id: appId,
    image: app.image,
    image_digest: 'sha256:6f8d0b7d7e5d4f0c9a1e2d3c4b5a69788796a5b4c3d2e1f0a9b8c7d6e5f4a3b2',
    status: 'succeeded',
    phase: 'health',
    error: null,
    trigger: 'api',
    created_by_type: 'system',
    created_by_id: 'system',
    created_at: hoursAgo(2),
    finished_at: secondsAfter(hoursAgo(2), 34),
  });
  db.deploymentLogs[deploymentId] = [
    { time: secondsAfter(hoursAgo(2), 0.2), phase: 'pull', message: `Pulling ${app.image}` },
    { time: secondsAfter(hoursAgo(2), 4), phase: 'rootfs', message: 'Preparing the root filesystem' },
    { time: secondsAfter(hoursAgo(2), 11), phase: 'boot', message: 'Booting the microVM' },
    { time: secondsAfter(hoursAgo(2), 27), phase: 'health', message: 'Waiting for the health check on port 80' },
    { time: secondsAfter(hoursAgo(2), 34), phase: 'health', message: 'Health check passed; the app is running' },
  ];

  db.vms.push({
    id: instanceId,
    project_id: project.id,
    app_id: appId,
    node_id: db.node.id,
    name: 'blog-1',
    vcpus: 1,
    memory_mb: 512,
    disk_gb: 2,
    image: app.image,
    desired_status: 'running',
    // Restarted with the app, then its process exited.
    observed_status: 'crashed',
    ip: '10.0.0.21',
    restart_generation: 1,
    restart_count: 1,
    last_exit_code: 1,
    created_at: createdAt,
    updated_at: createdAt,
  });

  db.vms.push({
    id: replicaInstanceId,
    project_id: project.id,
    app_id: appId,
    node_id: replicaNode.id,
    name: 'blog-2',
    vcpus: 1,
    memory_mb: 512,
    disk_gb: 2,
    image: app.image,
    desired_status: 'running',
    // Restarted with the app, then its process exited too.
    observed_status: 'crashed',
    ip: '10.0.0.31',
    restart_generation: 1,
    restart_count: 1,
    last_exit_code: 1,
    created_at: createdAt,
    updated_at: createdAt,
  });

  // blog-2 left the rotation first (degraded), then blog-1 (down: the
  // `app.down` that opened the incident 15 minutes ago), both after failed
  // health checks. Since the restart neither accepts a connection: both crashed.
  db.edgeBackends[appId] = [
    {
      edge_node_id: replicaNode.id,
      vm_id: instanceId,
      node_id: db.node.id,
      in_rotation: false,
      since: hoursAgo(15 / 60),
      reason: 'connect_failed',
      last_error: 'dial tcp 10.0.0.21:80: connect: connection refused',
    },
    {
      edge_node_id: replicaNode.id,
      vm_id: replicaInstanceId,
      node_id: replicaNode.id,
      in_rotation: false,
      since: hoursAgo(19 / 60),
      reason: 'connect_failed',
      last_error: 'dial tcp 10.0.0.31:80: connect: connection refused',
    },
  ];

  db.domains.push({
    id: mockId('dom'),
    app_id: appId,
    hostname: 'blog.example.com',
    tls_mode: 'tunnel',
    cert_status: 'not_applicable',
    cert_expires_at: null,
    serving_status: 'serving',
    last_error: null,
    created_at: createdAt,
  });

  db.appEnv[appId] = [
    { app_id: appId, name: 'SITE_NAME', updated_at: createdAt },
    { app_id: appId, name: 'LOG_LEVEL', updated_at: createdAt },
  ];

  // Storage: a 10 GiB volume on lab-3, a backup repository and the backups it
  // already holds. The reflink snapshot method is the one the lab node reports.
  const volume: Volume = {
    id: mockId('vol'),
    app_id: appId,
    node_id: db.node.id,
    size_gb: 10,
    mount_path: '/data',
    state: 'ready',
    allocated_bytes: Math.round(3.4 * GIB),
    restored_from_backup_id: null,
    error: null,
    quarantines: [],
    created_at: createdAt,
    updated_at: createdAt,
  };
  db.volumes.push(volume);

  const repository: BackupRepository = {
    id: mockId('brp'),
    name: 'lab-backups',
    provider_id: mockId('prv'),
    prefix: 'quoro-backups',
    recovery_key_fingerprint: '9f2c4e6a1b8d3057',
    recovery_key_rotated_at: null,
    repository_key_present: true,
    control_plane_schedule: { interval_h: 6, keep: 7 },
    created_by: SEED_OWNER_ID,
    created_at: createdAt,
  };
  db.backupRepositories.push(repository);

  db.backups.push(
    {
      id: mockId('bkp'),
      repository_id: repository.id,
      source_kind: 'volume',
      app_id: appId,
      volume_id: volume.id,
      node_id: db.node.id,
      status: 'failed',
      trigger: 'schedule',
      snapshot_method: null,
      pause_ms: null,
      logical_bytes: null,
      uploaded_bytes: null,
      chunk_count: null,
      error: 'the node could not read the volume snapshot',
      created_at: hoursAgo(0.5),
      started_at: hoursAgo(0.5),
      finished_at: hoursAgo(0.45),
      pruned_at: null,
    },
    {
      id: mockId('bkp'),
      repository_id: repository.id,
      source_kind: 'volume',
      app_id: appId,
      volume_id: volume.id,
      node_id: db.node.id,
      status: 'succeeded',
      trigger: 'api',
      snapshot_method: 'reflink',
      pause_ms: 0,
      logical_bytes: Math.round(3.4 * GIB),
      uploaded_bytes: Math.round(0.18 * GIB),
      chunk_count: 41,
      error: null,
      created_at: hoursAgo(1),
      started_at: hoursAgo(1),
      finished_at: hoursAgo(0.98),
      pruned_at: null,
    },
    {
      id: mockId('bkp'),
      repository_id: repository.id,
      source_kind: 'volume',
      app_id: appId,
      volume_id: volume.id,
      node_id: db.node.id,
      status: 'succeeded',
      trigger: 'schedule',
      snapshot_method: 'reflink',
      pause_ms: 0,
      logical_bytes: Math.round(3.4 * GIB),
      uploaded_bytes: Math.round(2.1 * GIB),
      chunk_count: 540,
      error: null,
      created_at: hoursAgo(6),
      started_at: hoursAgo(6),
      finished_at: hoursAgo(5.9),
      pruned_at: null,
    },
  );

  // The off-site copy: an R2 bucket rule, the strength the probe observed.
  db.auditExports.push({
    id: mockId('aex'),
    provider_id: mockId('prv'),
    prefix: 'quoro-audit',
    lock_strength: 'bucket_rule',
    probed_at: hoursAgo(1),
    probe_detail: 'Overwrite refused with 409; delete refused with 409.',
    last_exported_seq: 428,
    last_exported_hash:
      '6f8d0b7d7e5d4f0c9a1e2d3c4b5a69788796a5b4c3d2e1f0a9b8c7d6e5f4a3b2',
    last_error: null,
    updated_at: hoursAgo(1),
  });

  seedGitApp(createdAt);
  seedAtlas(createdAt, repository.id);
  seedSettings();
  seedEdge(createdAt);
  seedWarm(createdAt);
  applyRepositoryStart();
  seedDeployCycle();
};

/**
 * The demo's starting state for the HA view (ADR 0170): `lab-3` and `lab-4`
 * vote, and `lab-edge` is an enrolled edge-only machine without a vote, the
 * one that can become the third voter (ADR 0018). Seeded after everything
 * else, so every id seeded before keeps its value and `extraNodes[0]` stays
 * `lab-4`.
 */
const seedEdge = (createdAt: string): void => {
  db.extraNodes.push({
    id: mockId('nod'),
    name: 'lab-edge',
    roles: ['edge'],
    cp_status: null,
    status: 'connected',
    public_key: 'c2VlZC1sYWItZWRnZS1wdWJsaWMta2V5',
    address: '10.0.0.5',
    behind_nat: false,
    labels: { room: 'lab', rack: 'r3' },
    capacity: { cpus: 2, memory_mb: 2_048, disk_gb: 20 },
    allocated: { cpus: 0, memory_mb: 0, disk_gb: 0 },
    // An edge needs no KVM: it runs the node agent and the proxy, no engine.
    kvm: { available: false, kernel: '6.12.0-lab', nested: false },
    agent_version: '0.1.0',
    desired_rev: 1,
    applied_rev: 1,
    log_seq: 1,
    // No `node` role, so it follows no change log.
    log_lag: null,
    fence_failure: null,
    tunnel: null,
    last_seen_at: createdAt,
    lost_at: null,
    created_at: createdAt,
  });
};

/**
 * Mock mode only: `localStorage['quoro.mock.repository']` starts the cluster
 * without its backup repository, so the create form can be tried. `none`: no
 * repository was ever made. `lost`: the repository and its backups are gone
 * from the database, but its bucket still holds the config object, so
 * creating one on `lab-r2-backups` under `quoro-backups` answers `conflict`.
 * Anything else, or no storage (the tests), keeps the seeded repository.
 */
const applyRepositoryStart = (): void => {
  let start: string | null = null;
  try {
    start = globalThis.localStorage?.getItem('quoro.mock.repository') ?? null;
  } catch {
    start = null;
  }
  if (start !== 'none' && start !== 'lost') return;
  const gone = new Set(db.backupRepositories.map((repository) => repository.id));
  db.backupRepositories = [];
  db.backups = db.backups.filter((backup) => !gone.has(backup.repository_id));
  if (start === 'none') db.repositoryObjects = [];
};

const seedBuildLog = (
  buildId: string,
  lines: ReadonlyArray<{ vertex: number | null; line: string; stream?: BuildLogLine['stream'] }>,
): void => {
  db.buildLogs[buildId] = lines.map((entry, index) => ({
    seq: index + 1,
    time: now(),
    stream: entry.stream ?? 'stdout',
    vertex: entry.vertex,
    line: entry.line,
  }));
};

/** One Dockerfile step of a seeded build: its BuildKit name and how long it ran. */
interface SeedStep {
  name: string;
  seconds: number;
  cached?: boolean;
}

/**
 * One past run of a seeded app: a build of `commit` that starts a deployment,
 * or a deployment alone (`trigger` other than `build`). Times are seconds;
 * `ago` is how long before the seed the run started. A `fail` stops the build
 * in that step with that exit code, and nothing is deployed.
 */
interface SeedRun {
  ago: number;
  trigger?: Deployment['trigger'];
  commit?: string;
  queue?: number;
  steps?: readonly SeedStep[];
  fail?: { step: number; code: number };
  /** Pull, rootfs, boot and health. */
  deploy?: readonly [number, number, number, number];
}

const NODE_STEPS = (npmCi: number, npmBuild: number): SeedStep[] => [
  { name: '[1/4] FROM docker.io/library/node:22-alpine', seconds: 0, cached: true },
  { name: '[2/4] COPY package*.json ./', seconds: 0.4 },
  { name: '[3/4] RUN npm ci', seconds: npmCi },
  { name: '[4/4] RUN npm run build', seconds: npmBuild },
];

const iso = (ms: number): string => new Date(ms).toISOString();

/**
 * Seed an app's history, oldest run first, with the times the panel's deploy
 * tape reads: each build's `created_at`, `started_at`, `finished_at` and its
 * BuildKit lines (`vertex`, `time`); each deployment's progress lines
 * (`time`, `phase`) and `finished_at`. The newest deployment that succeeded
 * becomes the app's current one, with its image.
 */
const seedRuns = (app: App, runs: readonly SeedRun[], seededAt: number): void => {
  let image = app.image;
  for (const run of runs) {
    const startedAt = seededAt - run.ago * 1_000;
    let deployAt = startedAt;
    let build: Build | null = null;

    if (run.commit !== undefined && app.source) {
      const buildId = mockId('bld');
      const queued = startedAt;
      const started = queued + (run.queue ?? 5) * 1_000;
      const lines: BuildLogLine[] = [];
      let clock = started;
      const push = (vertex: number | null, line: string, stream: BuildLogLine['stream'] = 'stdout') =>
        lines.push({ seq: lines.length + 1, time: iso(clock), stream, vertex, line });

      push(1, '#1 [internal] load build definition from Dockerfile');
      clock += 300;
      push(1, '#1 transferring dockerfile: 412B done');
      clock += 1_200;
      push(2, '#2 [internal] load metadata for docker.io/library/node:22-alpine');
      clock += 900;
      push(2, '#2 DONE 0.9s');

      let failedStep: Build['failed_step'] = null;
      const steps = run.steps ?? [];
      for (const [index, step] of steps.entries()) {
        const vertex = index + 3;
        push(vertex, `#${vertex} ${step.name}`);
        if (step.cached) {
          push(vertex, `#${vertex} CACHED`);
          continue;
        }
        clock += step.seconds * 1_000;
        if (run.fail?.step === index) {
          const command = step.name.replace(/^\[[^\]]*\]\s*RUN\s*/, '');
          const error = `process "/bin/sh -c ${command}" did not complete successfully: exit code: ${run.fail.code}`;
          push(vertex, `#${vertex} ${step.seconds.toFixed(2)} npm error code ERESOLVE`, 'stderr');
          push(vertex, `#${vertex} ERROR: ${error}`, 'stderr');
          const dockerfile = steps.map((item, at) => {
            const instruction = item.name.replace(/^\[[^\]]*\]\s*/, '');
            return `${String(at + 1).padStart(4)} | ${at === index ? '>>> ' : ''}${instruction}`;
          });
          failedStep = {
            vertex,
            name: step.name,
            error,
            dockerfile_line: index + 1,
            excerpt: [
              `Dockerfile:${index + 1}`,
              '--------------------',
              ...dockerfile.slice(0, index + 1),
              '--------------------',
              `ERROR: failed to solve: ${error}`,
            ],
          };
          break;
        }
        push(vertex, `#${vertex} DONE ${step.seconds.toFixed(1)}s`);
      }

      if (!failedStep) {
        clock += 400;
        push(null, `#${steps.length + 3} exporting to oci image format`);
        clock += 1_800;
        push(null, `#${steps.length + 3} writing image to the local store`);
        clock += 400;
        push(null, `#${steps.length + 3} DONE 2.6s`);
      }

      const builtImage = `build.quoro.invalid/${app.id}:${buildId}`;
      build = {
        id: buildId,
        app_id: app.id,
        node_id: app.placement.node_ids[0] ?? db.node.id,
        source: app.source,
        status: failedStep ? 'failed' : 'succeeded',
        commit: run.commit,
        image: failedStep ? null : builtImage,
        image_digest: failedStep
          ? null
          : 'sha256:0c3e5a7b9d1f2a4c6e8b0d2f4a6c8e0b2d4f6a8c0e2b4d6f8a0c2e4b6d8f0a2c',
        failed_phase: failedStep ? 'building' : null,
        error: failedStep ? failedStep.error : null,
        failed_step: failedStep,
        deployment_id: null,
        trigger: 'api',
        refresh_base: false,
        created_by_type: 'system',
        created_by_id: 'system',
        created_at: iso(queued),
        started_at: iso(started),
        finished_at: iso(clock),
      };
      db.builds.push(build);
      db.buildLogs[buildId] = lines;
      if (failedStep) continue;
      image = builtImage;
      deployAt = clock + 800;
    }

    const [pull, rootfs, boot, health] = run.deploy ?? [5, 9, 14, 6];
    const deploymentId = mockId('dep');
    let clock = deployAt + 200;
    const logs: DeploymentLogEntry[] = [];
    logs.push({ time: iso(clock), phase: 'pull', message: `Pulling ${image}` });
    clock += pull * 1_000;
    logs.push({ time: iso(clock), phase: 'rootfs', message: 'Preparing the root filesystem' });
    clock += rootfs * 1_000;
    logs.push({ time: iso(clock), phase: 'boot', message: 'Booting the microVM' });
    clock += boot * 1_000;
    logs.push({
      time: iso(clock),
      phase: 'health',
      message: `Waiting for the health check on port ${app.port}`,
    });
    clock += health * 1_000;
    logs.push({ time: iso(clock), phase: 'health', message: 'Health check passed; the app is running' });

    db.deployments.push({
      id: deploymentId,
      app_id: app.id,
      image,
      image_digest: 'sha256:0c3e5a7b9d1f2a4c6e8b0d2f4a6c8e0b2d4f6a8c0e2b4d6f8a0c2e4b6d8f0a2c',
      status: 'succeeded',
      phase: 'health',
      error: null,
      trigger: run.trigger ?? (build ? 'build' : 'api'),
      created_by_type: 'system',
      created_by_id: 'system',
      created_at: iso(deployAt),
      finished_at: iso(clock),
    });
    db.deploymentLogs[deploymentId] = logs;
    if (build) build.deployment_id = deploymentId;
    app.current_deployment_id = deploymentId;
    app.image = image;
  }
};

/** A second app that builds from a public repository, with both outcomes. */
const seedGitApp = (createdAt: string): void => {
  const project: Project = {
    id: mockId('prj'),
    slug: 'docs',
    name: 'Docs',
    created_at: createdAt,
    updated_at: createdAt,
  };
  db.projects.push(project);

  const appId = mockId('app');
  const succeededBuildId = mockId('bld');
  const failedBuildId = mockId('bld');
  const deploymentId = mockId('dep');
  const instanceId = mockId('vm');

  const source: AppSource = {
    kind: 'git',
    repo_url: 'https://github.com/example/docs-site.git',
    ref: 'main',
    dockerfile_path: 'Dockerfile',
    context_dir: '.',
    target: null,
    build_args: [{ name: 'NODE_ENV', value: 'production' }],
    secrets: ['NPM_TOKEN'],
  };
  const builtImage = `build.quoro.invalid/${appId}:${succeededBuildId}`;
  const successCommit = '4f2c9a1b2c3d4e5f60718293a4b5c6d7e8f90123';
  const failedCommit = '9a8b7c6d5e4f3021a9b8c7d6e5f40312a1b2c3d4';
  const successFinishedAt = new Date(Date.parse(createdAt) + 4_250).toISOString();
  const failedFinishedAt = new Date(Date.parse(createdAt) + 2_400).toISOString();
  const failedError =
    'process "/bin/sh -c ls /does-not-exist" did not complete successfully: exit code: 2';

  db.apps.push({
    id: appId,
    project_id: project.id,
    name: 'docs-site',
    stateful: false,
    ha_policy: 'none',
    warm: null,
    backup_schedule: { interval_h: 24, keep: 7 },
    replicas: 1,
    image: builtImage,
    source,
    port: 3000,
    health_check: { path: '/', interval_s: 10, timeout_s: 2, failures: 3 },
    resources: { vcpus: 1, memory_mb: 512, disk_gb: 2 },
    // Any node: losing lab-3 can start it again on lab-4 (the N+1 strip).
    placement: { node_ids: [] },
    status: 'running',
    current_deployment_id: deploymentId,
    restart_generation: 0,
    created_at: createdAt,
    updated_at: createdAt,
  });

  db.deployments.push({
    id: deploymentId,
    app_id: appId,
    image: builtImage,
    image_digest:
      'sha256:2a7d1c4e8f0b3d6a9c2e5f8b1d4a7c0e3f6b9d2a5c8e1f4b7d0a3c6e9f2b5d8a',
    status: 'succeeded',
    phase: 'health',
    error: null,
    trigger: 'build',
    created_by_type: 'system',
    created_by_id: 'system',
    created_at: createdAt,
    finished_at: createdAt,
  });
  db.deploymentLogs[deploymentId] = [
    { time: createdAt, phase: 'pull', message: `Pulling ${builtImage}` },
    { time: createdAt, phase: 'rootfs', message: 'Preparing the root filesystem' },
    { time: createdAt, phase: 'boot', message: 'Booting the microVM' },
    { time: createdAt, phase: 'health', message: 'Health check passed; the app is running' },
  ];

  db.vms.push({
    id: instanceId,
    project_id: project.id,
    app_id: appId,
    node_id: db.node.id,
    name: 'docs-site-1',
    vcpus: 1,
    memory_mb: 512,
    disk_gb: 2,
    image: builtImage,
    desired_status: 'running',
    observed_status: 'running',
    ip: '10.0.0.22',
    restart_generation: 0,
    restart_count: 0,
    last_exit_code: null,
    created_at: createdAt,
    updated_at: createdAt,
  });

  db.appEnv[appId] = [
    { app_id: appId, name: 'NPM_TOKEN', updated_at: createdAt },
    { app_id: appId, name: 'NODE_ENV', updated_at: createdAt },
  ];

  db.builds.push({
    id: succeededBuildId,
    app_id: appId,
    node_id: db.node.id,
    source,
    status: 'succeeded',
    commit: successCommit,
    image: builtImage,
    image_digest:
      'sha256:2a7d1c4e8f0b3d6a9c2e5f8b1d4a7c0e3f6b9d2a5c8e1f4b7d0a3c6e9f2b5d8a',
    failed_phase: null,
    error: null,
    failed_step: null,
    deployment_id: deploymentId,
    trigger: 'api',
    refresh_base: false,
    created_by_type: 'system',
    created_by_id: 'system',
    created_at: createdAt,
    started_at: createdAt,
    finished_at: successFinishedAt,
  });
  seedBuildLog(succeededBuildId, [
    { vertex: 1, line: '#1 [internal] load build definition from Dockerfile' },
    { vertex: 1, line: '#1 transferring dockerfile: 412B done' },
    { vertex: 2, line: '#2 [internal] load metadata for docker.io/library/node:22-alpine' },
    { vertex: 3, line: '#3 [1/4] FROM docker.io/library/node:22-alpine' },
    { vertex: 3, line: '#3 DONE 0.0s' },
    { vertex: 4, line: '#4 [2/4] COPY package.json package-lock.json ./' },
    { vertex: 4, line: '#4 DONE 0.1s' },
    { vertex: 5, line: '#5 [3/4] RUN npm ci' },
    { vertex: 5, line: '#5 DONE 2.4s' },
    { vertex: 6, line: '#6 [4/4] RUN npm run build' },
    { vertex: 6, line: '#6 DONE 1.8s' },
    { vertex: null, line: '#7 exporting to oci image format' },
    { vertex: null, line: '#7 exporting layers' },
    { vertex: null, line: '#7 writing image sha256:2a7d1c4e8f0b3d6a9c2e5f8b1d4a7c0e3f6b9d2a5c8e1f4b7d0a3c6e9f2b5d8a' },
    { vertex: null, line: '#7 DONE 0.4s' },
  ]);

  db.builds.push({
    id: failedBuildId,
    app_id: appId,
    node_id: db.node.id,
    source,
    status: 'failed',
    commit: failedCommit,
    image: null,
    image_digest: null,
    failed_phase: 'building',
    error: failedError,
    failed_step: {
      vertex: 5,
      name: '[3/4] RUN ls /does-not-exist',
      error: failedError,
      dockerfile_line: 3,
      excerpt: [
        'Dockerfile:3',
        '--------------------',
        '   1 | FROM node:22-alpine',
        '   2 |     RUN echo "step one ok"',
        '   3 | >>> RUN ls /does-not-exist',
        '--------------------',
        `ERROR: failed to solve: ${failedError}`,
      ],
    },
    deployment_id: null,
    trigger: 'api',
    refresh_base: false,
    created_by_type: 'system',
    created_by_id: 'system',
    created_at: createdAt,
    started_at: createdAt,
    finished_at: failedFinishedAt,
  });
  seedBuildLog(failedBuildId, [
    { vertex: 1, line: '#1 [internal] load build definition from Dockerfile' },
    { vertex: 1, line: '#1 transferring dockerfile: 412B done' },
    { vertex: 2, line: '#2 [internal] load metadata for docker.io/library/node:22-alpine' },
    { vertex: 3, line: '#3 [1/4] FROM docker.io/library/node:22-alpine' },
    { vertex: 3, line: '#3 DONE 0.0s' },
    { vertex: 4, line: '#4 [2/4] COPY package.json package-lock.json ./' },
    { vertex: 4, line: '#4 DONE 0.1s' },
    { vertex: 5, line: '#5 [3/4] RUN ls /does-not-exist' },
    {
      vertex: 5,
      stream: 'stderr',
      line: '#5 0.221 ls: /does-not-exist: No such file or directory',
    },
    {
      vertex: 5,
      stream: 'stderr',
      line: `#5 ERROR: ${failedError}`,
    },
  ]);
};

/**
 * Atlas: a project with several services, as the canvas reference draws it.
 * Two Git apps (web with a domain, api on a private port), an image worker,
 * a stateful postgres under the cold policy with a volume and a recent
 * backup, and a standalone VM. It is listed first.
 */
const seedAtlas = (createdAt: string, repositoryId: string): void => {
  const minutesAgo = (minutes: number): string =>
    new Date(Date.parse(createdAt) - minutes * 60_000).toISOString();
  const project: Project = {
    id: mockId('prj'),
    slug: 'atlas',
    name: 'Atlas',
    created_at: createdAt,
    updated_at: createdAt,
  };
  db.projects.unshift(project);
  const lab4 = db.extraNodes[0]?.id ?? db.node.id;

  const addApp = (input: {
    name: string;
    image: string;
    port: number;
    replicas?: number;
    source?: AppSource;
    stateful?: boolean;
    cold?: boolean;
    vcpus?: number;
    memoryMb?: number;
    /** The app's history, oldest first; one image deploy 40 minutes ago by default. */
    runs?: readonly SeedRun[];
  }): App => {
    const appId = mockId('app');
    const replicas = input.replicas ?? 1;
    const nodes = replicas > 1 ? [db.node.id, lab4] : [lab4];
    const resources = {
      vcpus: input.vcpus ?? 1,
      memory_mb: input.memoryMb ?? 512,
      disk_gb: 2,
    };
    const app: App = {
      id: appId,
      project_id: project.id,
      name: input.name,
      stateful: input.stateful ?? false,
      ha_policy: input.cold ? 'cold' : 'none',
      warm: null,
      backup_schedule: { interval_h: 6, keep: 7 },
      replicas,
      image: input.image,
      ...(input.source ? { source: input.source } : {}),
      port: input.port,
      health_check: { path: '/', interval_s: 10, timeout_s: 2, failures: 3 },
      resources,
      placement: { node_ids: nodes },
      status: 'running',
      current_deployment_id: null,
      restart_generation: 0,
      created_at: createdAt,
      updated_at: createdAt,
    };
    db.apps.push(app);
    seedRuns(app, input.runs ?? [{ ago: 2_400, deploy: [5, 9, 14, 6] }], Date.parse(createdAt));
    nodes.forEach((nodeId, index) => {
      db.vms.push({
        id: mockId('vm'),
        project_id: project.id,
        app_id: appId,
        node_id: nodeId,
        name: `${input.name}-${index + 1}`,
        ...resources,
        image: app.image,
        desired_status: 'running',
        observed_status: 'running',
        ip: `10.0.1.${db.vms.length + 10}`,
        restart_generation: 0,
        restart_count: 0,
        last_exit_code: null,
        created_at: createdAt,
        updated_at: createdAt,
      });
    });
    db.appEnv[appId] = [];
    return app;
  };

  const gitSource = (repo: string): AppSource => ({
    kind: 'git',
    repo_url: `https://github.com/example/${repo}.git`,
    ref: 'main',
    dockerfile_path: 'Dockerfile',
    context_dir: '.',
    target: null,
    build_args: [],
    secrets: [],
  });

  const webSource = gitSource('atlas-web');
  const web = addApp({
    name: 'web',
    image: 'build.quoro.invalid/atlas-web',
    port: 3000,
    replicas: 2,
    source: webSource,
    runs: [
      {
        ago: 2_400,
        commit: '7d41e0ab9c2f3e4d5a6b7c8d9e0f1a2b3c4d5e6f',
        queue: 4,
        steps: NODE_STEPS(36, 22),
        deploy: [5, 10, 16, 8],
      },
    ],
  });
  db.domains.push({
    id: mockId('dom'),
    app_id: web.id,
    hostname: 'atlas.example.com',
    tls_mode: 'tunnel',
    cert_status: 'not_applicable',
    cert_expires_at: null,
    serving_status: 'serving',
    last_error: null,
    created_at: createdAt,
  });

  // The api's history, as the deploy reference draws it: three good builds,
  // one that broke in `npm ci`, and a redeploy in between.
  const api = addApp({
    name: 'api',
    image: 'build.quoro.invalid/atlas-api',
    port: 3000,
    replicas: 2,
    source: { ...gitSource('atlas-api'), secrets: ['SESSION_SECRET'] },
    runs: [
      {
        ago: 266_400,
        commit: '2d8f3b0c4e6a8c0e2b4d6f8a0c2e4b6d8f0a2c4e',
        queue: 5,
        steps: NODE_STEPS(45, 13),
        deploy: [5, 13, 19, 11],
      },
      {
        ago: 180_000,
        commit: '9b1e07d4c2a6e8f0b3d5a7c9e1f3b5d7a9c1e3f5',
        queue: 7,
        steps: NODE_STEPS(52, 15),
        deploy: [6, 11, 22, 9],
      },
      {
        ago: 93_600,
        commit: '1c44a2e9f7d5b3a1c9e7f5d3b1a9c7e5f3d1b9a7',
        queue: 5,
        steps: NODE_STEPS(31, 0),
        fail: { step: 2, code: 1 },
      },
      { ago: 18_000, trigger: 'redeploy', deploy: [3, 4, 8, 4] },
      {
        ago: 10_800,
        commit: '4f2a9c1e8b7d6c5a4f3e2d1c0b9a8f7e6d5c4b3a',
        queue: 6,
        steps: NODE_STEPS(48, 14),
        deploy: [6, 12, 20, 10],
      },
    ],
  });
  // Names and when each was set; the values never come back.
  const seededAt = Date.parse(now());
  db.appEnv[api.id] = [
    { app_id: api.id, name: 'DATABASE_URL', updated_at: iso(seededAt - 266_400_000) },
    { app_id: api.id, name: 'REDIS_URL', updated_at: iso(seededAt - 266_400_000) },
    { app_id: api.id, name: 'SESSION_SECRET', updated_at: iso(seededAt - 180_000_000) },
    { app_id: api.id, name: 'LOG_LEVEL', updated_at: iso(seededAt - 20_000) },
  ];

  addApp({ name: 'worker', image: 'ghcr.io/example/atlas-worker:1.8', port: 8080 });

  const postgres = addApp({
    name: 'postgres',
    image: 'docker.io/library/postgres:17',
    port: 5432,
    stateful: true,
    cold: true,
    memoryMb: 1024,
  });
  const volumeId = mockId('vol');
  db.volumes.push({
    id: volumeId,
    app_id: postgres.id,
    node_id: lab4,
    size_gb: 20,
    mount_path: '/var/lib/postgresql',
    state: 'ready',
    allocated_bytes: Math.round(4.2 * GIB),
    restored_from_backup_id: null,
    error: null,
    quarantines: [],
    created_at: createdAt,
    updated_at: createdAt,
  });
  db.backups.push({
    id: mockId('bkp'),
    repository_id: repositoryId,
    source_kind: 'volume',
    app_id: postgres.id,
    volume_id: volumeId,
    node_id: lab4,
    status: 'succeeded',
    trigger: 'schedule',
    snapshot_method: 'reflink',
    pause_ms: 0,
    logical_bytes: Math.round(4.2 * GIB),
    uploaded_bytes: Math.round(0.6 * GIB),
    chunk_count: 160,
    error: null,
    created_at: minutesAgo(227),
    started_at: minutesAgo(227),
    finished_at: minutesAgo(226),
    pruned_at: null,
  });
  // The run before it failed, and the one before that succeeded: the
  // Storage tab's list as the reference draws it.
  db.backups.push(
    {
      id: mockId('bkp'),
      repository_id: repositoryId,
      source_kind: 'volume',
      app_id: postgres.id,
      volume_id: volumeId,
      node_id: lab4,
      status: 'failed',
      trigger: 'schedule',
      snapshot_method: null,
      pause_ms: null,
      logical_bytes: null,
      uploaded_bytes: null,
      chunk_count: null,
      error: 'the node could not read the volume snapshot',
      created_at: minutesAgo(587),
      started_at: minutesAgo(587),
      finished_at: minutesAgo(586),
      pruned_at: null,
    },
    {
      id: mockId('bkp'),
      repository_id: repositoryId,
      source_kind: 'volume',
      app_id: postgres.id,
      volume_id: volumeId,
      node_id: lab4,
      status: 'succeeded',
      trigger: 'api',
      snapshot_method: 'reflink',
      pause_ms: 0,
      logical_bytes: Math.round(4.1 * GIB),
      uploaded_bytes: Math.round(2.1 * GIB),
      chunk_count: 540,
      error: null,
      created_at: minutesAgo(947),
      started_at: minutesAgo(947),
      finished_at: minutesAgo(945),
      pruned_at: null,
    },
  );

  db.vms.push({
    id: mockId('vm'),
    project_id: project.id,
    app_id: null,
    node_id: db.node.id,
    name: 'ci-runner',
    vcpus: 4,
    memory_mb: 8192,
    disk_gb: 40,
    image: 'docker.io/library/ubuntu:24.04',
    desired_status: 'running',
    observed_status: 'running',
    ip: '10.0.1.40',
    restart_generation: 0,
    restart_count: 0,
    last_exit_code: null,
    created_at: createdAt,
    updated_at: createdAt,
  });
};

/**
 * Harbor: one `warm` app in each state the Storage tab and the canvas card
 * show (openapi.yaml `WarmCopy`), in a project of its own so Atlas, the
 * canvas reference, stays as it is. Each app is stateful with a ready volume
 * on lab-3 and, but for one, its warm copy on lab-4. The progress and the
 * errors are what the primary answers (`warm.rs`, `derive`): a copy with no
 * snapshot has no shipped time and no lag, a failed copy keeps the progress
 * the nodes last reported, and the extent count is the source's, with or
 * without a copy. Seeded last, so every id seeded before keeps its value.
 */
const seedWarm = (createdAt: string): void => {
  const seededAt = Date.parse(createdAt);
  const secondsAgo = (seconds: number): string => iso(seededAt - seconds * 1_000);
  const lab3 = db.node.id;
  const lab4 = db.extraNodes.find((node) => node.name === 'lab-4')?.id ?? db.node.id;
  const project: Project = {
    id: mockId('prj'),
    slug: 'harbor',
    name: 'Harbor',
    created_at: createdAt,
    updated_at: createdAt,
  };
  db.projects.push(project);

  const addWarmApp = (input: {
    name: string;
    image: string;
    port: number;
    sizeGb: number;
    allocatedGib: number;
    mountPath: string;
    warm: (volumeId: string) => WarmCopy;
  }): App => {
    const appId = mockId('app');
    const volumeId = mockId('vol');
    const resources = { vcpus: 1, memory_mb: 512, disk_gb: 2 };
    const app: App = {
      id: appId,
      project_id: project.id,
      name: input.name,
      stateful: true,
      ha_policy: 'warm',
      warm: input.warm(volumeId),
      backup_schedule: { interval_h: 24, keep: 7 },
      replicas: 1,
      image: input.image,
      port: input.port,
      health_check: { path: '/', interval_s: 10, timeout_s: 2, failures: 3 },
      resources,
      placement: { node_ids: [lab3] },
      status: 'running',
      current_deployment_id: null,
      restart_generation: 0,
      created_at: createdAt,
      updated_at: createdAt,
    };
    db.apps.push(app);
    warmMeasuredAt.set(appId, seededAt);
    seedRuns(app, [{ ago: 7_200, deploy: [4, 8, 12, 6] }], seededAt);
    db.volumes.push({
      id: volumeId,
      app_id: appId,
      node_id: lab3,
      size_gb: input.sizeGb,
      mount_path: input.mountPath,
      state: 'ready',
      allocated_bytes: Math.round(input.allocatedGib * GIB),
      restored_from_backup_id: null,
      error: null,
      quarantines: [],
      created_at: createdAt,
      updated_at: createdAt,
    });
    db.vms.push({
      id: mockId('vm'),
      project_id: project.id,
      app_id: appId,
      node_id: lab3,
      name: `${input.name}-1`,
      ...resources,
      image: app.image,
      desired_status: 'running',
      observed_status: 'running',
      ip: `10.0.1.${db.vms.length + 10}`,
      restart_generation: 0,
      restart_count: 0,
      last_exit_code: null,
      created_at: createdAt,
      updated_at: createdAt,
    });
    db.appEnv[appId] = [];
    return app;
  };

  const postgres = 'docker.io/library/postgres:17';

  // A first full copy to lab-4: no snapshot is complete there yet, but the
  // source already reported its newest snapshot's extents.
  addWarmApp({
    name: 'inventory',
    image: postgres,
    port: 5432,
    sizeGb: 40,
    allocatedGib: 11.6,
    mountPath: '/var/lib/postgresql',
    warm: () => ({
      node_id: lab4,
      node_pinned: false,
      interval_s: 30,
      state: 'syncing',
      last_snapshot_seq: null,
      last_shipped_at: null,
      lag_s: null,
      extent_count: 640,
      error: null,
    }),
  });

  // In sync: the copy is at most twice the interval behind.
  addWarmApp({
    name: 'orders',
    image: postgres,
    port: 5432,
    sizeGb: 20,
    allocatedGib: 4.8,
    mountPath: '/var/lib/postgresql',
    warm: () => ({
      node_id: lab4,
      node_pinned: true,
      interval_s: 30,
      state: 'in_sync',
      last_snapshot_seq: 4182,
      last_shipped_at: secondsAgo(7),
      lag_s: 12,
      extent_count: 312,
      error: null,
    }),
  });

  // Lagging: a fragmented volume ships slowly, and the lag is far above
  // twice the 60 s interval.
  addWarmApp({
    name: 'analytics',
    image: 'docker.io/clickhouse/clickhouse-server:24.8',
    port: 8123,
    sizeGb: 120,
    allocatedGib: 63,
    mountPath: '/var/lib/clickhouse',
    warm: () => ({
      node_id: lab4,
      node_pinned: false,
      interval_s: 60,
      state: 'lagging',
      last_snapshot_seq: 977,
      last_shipped_at: secondsAgo(381),
      lag_s: 412,
      extent_count: 18_940,
      error: null,
    }),
  });

  // Failed on the warm node's own error, over several lines, after copies
  // that went through: the progress the nodes last reported stays.
  addWarmApp({
    name: 'media',
    image: 'docker.io/minio/minio:latest',
    port: 9000,
    sizeGb: 200,
    allocatedGib: 148,
    mountPath: '/data',
    warm: (volumeId) => ({
      node_id: lab4,
      node_pinned: false,
      interval_s: 5,
      state: 'failed',
      last_snapshot_seq: 2210,
      last_shipped_at: secondsAgo(1_836),
      lag_s: 1_840,
      extent_count: 2_048,
      error: [
        'warm node: receiving snapshot 2211 failed: no space left on device (os error 28)',
        `while writing warm/${volumeId}/snapshots/2211.partial`,
        'the copy resumes from snapshot 2210 once the disk has room',
      ].join('\n'),
    }),
  });

  // Failed with no warm node, in the primary's words. The mock does not
  // model why no node is eligible; its `ha_cost` says `no_warm_node`.
  addWarmApp({
    name: 'search',
    image: 'docker.io/getmeili/meilisearch:v1.11',
    port: 7700,
    sizeGb: 30,
    allocatedGib: 6.2,
    mountPath: '/meili_data',
    warm: () => ({
      node_id: null,
      node_pinned: false,
      interval_s: 30,
      state: 'failed',
      last_snapshot_seq: null,
      last_shipped_at: null,
      lag_s: null,
      extent_count: null,
      error: 'no node is eligible as the warm node',
    }),
  });

  // After a primary restart, before either node reports again: every
  // progress field is null and, with both nodes connected, it is syncing.
  addWarmApp({
    name: 'billing',
    image: postgres,
    port: 5432,
    sizeGb: 20,
    allocatedGib: 2.4,
    mountPath: '/var/lib/postgresql',
    warm: () => ({
      node_id: lab4,
      node_pinned: false,
      interval_s: 30,
      state: 'syncing',
      last_snapshot_seq: null,
      last_shipped_at: null,
      lag_s: null,
      extent_count: null,
      error: null,
    }),
  });
};

/**
 * What Settings shows: the provider entries behind the backup repository and
 * the off-site audit copy (their ids are the ones seeded above), a tunnel and
 * a DNS provider that nothing holds yet, and the control-plane database's own
 * backups. Seeded last, so every id seeded before keeps its value.
 */
const seedSettings = (): void => {
  const hoursAgo = (hours: number): string =>
    new Date(Date.now() - hours * 3_600_000).toISOString();
  const createdBy = db.backupRepositories[0]?.created_by ?? SEED_OWNER_ID;
  const repository = db.backupRepositories[0];
  const auditExport = db.auditExports[0];

  if (repository) {
    db.providers.push({
      id: repository.provider_id,
      kind: 'object_store',
      name: 'lab-r2-backups',
      config: {
        access_key_id: '6d0b3f8e2a9c4517',
        bucket: 'quoro-lab-backups',
        endpoint: 'https://4f1c2b7e9a3d.r2.cloudflarestorage.com',
        region: 'auto',
      },
      has_secret: true,
      created_by: createdBy,
      created_at: hoursAgo(24 * 40),
      rotated_at: hoursAgo(24 * 9),
    });
    const backupsProvider = db.providers[db.providers.length - 1];
    if (backupsProvider) {
      db.repositoryObjects.push(repositoryObjectKey(backupsProvider.config, repository.prefix));
    }
  }
  if (auditExport) {
    db.providers.push({
      id: auditExport.provider_id,
      kind: 'object_store',
      name: 'lab-r2-audit',
      config: {
        access_key_id: 'c27e9a0f5b3d8e16',
        bucket: 'quoro-lab-audit',
        endpoint: 'https://4f1c2b7e9a3d.r2.cloudflarestorage.com',
        region: 'auto',
      },
      has_secret: true,
      created_by: createdBy,
      created_at: hoursAgo(24 * 40),
      rotated_at: null,
    });
  }
  db.providers.push(
    {
      id: mockId('prv'),
      kind: 'tunnel',
      name: 'cloudflare-tunnel',
      config: { account_id: '8b4e1f0c2d6a4e9b' },
      has_secret: true,
      created_by: createdBy,
      created_at: hoursAgo(24 * 40),
      rotated_at: null,
    },
    {
      id: mockId('prv'),
      kind: 'dns',
      name: 'cloudflare-dns',
      config: { zone_id: '2a7d9c41e0b3f856', zone: 'lab.quoro.dev' },
      has_secret: true,
      created_by: createdBy,
      created_at: hoursAgo(24 * 40),
      rotated_at: null,
    },
  );

  if (!repository) return;
  const controlPlane = (
    hours: number,
    trigger: Backup['trigger'],
    logicalMb: number,
    uploadedMb: number,
  ): Backup => ({
    id: mockId('bkp'),
    repository_id: repository.id,
    source_kind: 'control_plane',
    app_id: null,
    volume_id: null,
    node_id: db.node.id,
    status: 'succeeded',
    trigger,
    snapshot_method: null,
    pause_ms: null,
    logical_bytes: logicalMb * 1024 ** 2,
    uploaded_bytes: uploadedMb * 1024 ** 2,
    chunk_count: Math.max(1, Math.round(logicalMb / 4)),
    error: null,
    created_at: hoursAgo(hours),
    started_at: hoursAgo(hours),
    finished_at: hoursAgo(hours - 0.01),
    pruned_at: null,
  });
  db.backups.push(
    controlPlane(2, 'schedule', 46, 3),
    controlPlane(8, 'schedule', 46, 2),
    controlPlane(14, 'schedule', 45, 2),
    controlPlane(19, 'upgrade', 45, 6),
    controlPlane(20, 'schedule', 44, 44),
  );
};

seed();
