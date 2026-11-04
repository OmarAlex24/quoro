import type {
  App,
  AppEnvVar,
  AppSource,
  Build,
  BuildLogLine,
  Deployment,
  DeploymentLogEntry,
  Domain,
  EdgeBackend,
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
    created_at: createdAt,
  };
  db.extraNodes.push(replicaNode);

  const app: App = {
    id: appId,
    project_id: project.id,
    name: 'blog',
    stateful: false,
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
    created_at: createdAt,
  });

  db.appEnv[appId] = [
    { app_id: appId, name: 'SITE_NAME', updated_at: createdAt },
    { app_id: appId, name: 'LOG_LEVEL', updated_at: createdAt },
  ];

  seedGitApp(createdAt);
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

seed();
