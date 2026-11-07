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
  User,
  Vm,
  Volume,
} from '../api/generated/model';

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
    capacity: { cpus: 16, memory_mb: 65_536, disk_gb: 2_000 },
    allocated: { cpus: 0, memory_mb: 0, disk_gb: 0 },
    kvm: { available: true, kernel: '6.12.0-lab', nested: false },
    engine_version: '0.1.0',
    agent_version: '0.1.0',
    desired_rev: 1,
    applied_rev: 1,
    log_seq: 1,
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
};

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
  const warm: HaPolicyCost = {
    policy: 'warm',
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
  const hot: HaPolicyCost = { ...warm, policy: 'hot' };

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
    cp_status: 'replica',
    status: 'connected',
    public_key: 'c2VlZC1sYWItNC1wdWJsaWMta2V5',
    address: '10.0.0.4',
    behind_nat: false,
    labels: { room: 'lab', rack: 'r2' },
    capacity: { cpus: 8, memory_mb: 32_768, disk_gb: 1_000 },
    allocated: { cpus: 1, memory_mb: 512, disk_gb: 2 },
    kvm: { available: true, kernel: '6.12.0-lab', nested: false },
    engine_version: '0.1.0',
    agent_version: '0.1.0',
    desired_rev: 1,
    applied_rev: 1,
    log_seq: 1,
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
    backup_schedule: { interval_h: 24, keep: 7 },
    replicas: 2,
    image: 'docker.io/library/nginx:alpine',
    port: 80,
    health_check: { path: '/', interval_s: 10, timeout_s: 2, failures: 3 },
    resources: { vcpus: 1, memory_mb: 512, disk_gb: 2 },
    placement: { node_ids: [db.node.id, replicaNode.id] },
    status: 'running',
    current_deployment_id: deploymentId,
    restart_generation: 0,
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
    observed_status: 'running',
    ip: '10.0.0.21',
    restart_generation: 0,
    restart_count: 0,
    last_exit_code: null,
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
    observed_status: 'running',
    ip: '10.0.0.31',
    restart_generation: 0,
    restart_count: 0,
    last_exit_code: null,
    created_at: createdAt,
    updated_at: createdAt,
  });

  db.edgeBackends[appId] = [
    {
      edge_node_id: replicaNode.id,
      vm_id: instanceId,
      node_id: db.node.id,
      in_rotation: true,
      since: createdAt,
      reason: null,
      last_error: null,
    },
    {
      edge_node_id: replicaNode.id,
      vm_id: replicaInstanceId,
      node_id: replicaNode.id,
      in_rotation: false,
      since: createdAt,
      reason: 'health_check',
      last_error: 'GET http://10.0.0.31:80/: request canceled after 2s',
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
    control_plane_schedule: { interval_h: 6, keep: 7 },
    created_by: 'usr_01JC9Z6Q5W7X8Y9Z0A1B2C3D4E',
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
    backup_schedule: { interval_h: 24, keep: 7 },
    replicas: 1,
    image: builtImage,
    source,
    port: 3000,
    health_check: { path: '/', interval_s: 10, timeout_s: 2, failures: 3 },
    resources: { vcpus: 1, memory_mb: 512, disk_gb: 2 },
    placement: { node_ids: [db.node.id] },
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

seed();
