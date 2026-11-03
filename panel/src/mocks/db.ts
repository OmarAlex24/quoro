import type {
  App,
  AppEnvVar,
  Deployment,
  DeploymentLogEntry,
  Domain,
  Node,
  Project,
  User,
  Vm,
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

/** A contract-shaped id: `<prefix>_` plus 26 Crockford characters. */
export const mockId = (prefix: string): string => {
  sequence += 1;
  const time = encode(Date.now(), 10);
  const randomness = encode(
    Math.floor(Math.random() * 1_000_000) + sequence * 7919,
    16,
  );
  return `${prefix}_${time}${randomness}`;
};

export const now = (): string => new Date().toISOString();

export interface MockDb {
  setupDone: boolean;
  owner: User | null;
  /** The password chosen at setup; the mock compares against it. */
  password: string | null;
  /** Id of the signed-in user, or null. The panel's cookie, simulated. */
  sessionUserId: string | null;
  node: Node;
  projects: Project[];
  vms: Vm[];
  apps: App[];
  deployments: Deployment[];
  deploymentLogs: Record<string, DeploymentLogEntry[]>;
  domains: Domain[];
  appEnv: Record<string, AppEnvVar[]>;
}

export const db: MockDb = {
  setupDone: false,
  owner: null,
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
    created_at: now(),
  },
  projects: [],
  vms: [],
  apps: [],
  deployments: [],
  deploymentLogs: {},
  domains: [],
  appEnv: {},
};

const timers = new Set<number>();

/** Run `fn` later, so a test or a reset can see it as pending. */
export const schedule = (fn: () => void, delay: number): void => {
  const id = window.setTimeout(() => {
    timers.delete(id);
    fn();
  }, delay);
  timers.add(id);
};

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
      delay: 1_300,
      status: 'preparing',
      phase: 'rootfs',
      message: 'Preparing the root filesystem',
    },
    {
      delay: 2_100,
      status: 'starting',
      phase: 'boot',
      message: 'Booting the microVM',
      vm: 'starting',
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

  const app: App = {
    id: appId,
    project_id: project.id,
    name: 'blog',
    stateful: false,
    ha_policy: 'none',
    image: 'docker.io/library/nginx:alpine',
    port: 80,
    health_check: { path: '/', interval_s: 10, timeout_s: 2, failures: 3 },
    resources: { vcpus: 1, memory_mb: 512, disk_gb: 2 },
    placement: { node_ids: [db.node.id] },
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
    created_at: createdAt,
    finished_at: createdAt,
  });
  db.deploymentLogs[deploymentId] = [
    { time: createdAt, phase: 'pull', message: `Pulling ${app.image}` },
    { time: createdAt, phase: 'rootfs', message: 'Preparing the root filesystem' },
    { time: createdAt, phase: 'boot', message: 'Booting the microVM' },
    { time: createdAt, phase: 'health', message: 'Health check passed; the app is running' },
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

  db.domains.push({
    id: mockId('dom'),
    app_id: appId,
    hostname: 'blog.example.com',
    tls_mode: 'tunnel',
    cert_status: 'not_applicable',
    cert_expires_at: null,
    created_at: createdAt,
  });

  db.appEnv[appId] = [
    { app_id: appId, name: 'SITE_NAME', updated_at: createdAt },
    { app_id: appId, name: 'LOG_LEVEL', updated_at: createdAt },
  ];
};

seed();
