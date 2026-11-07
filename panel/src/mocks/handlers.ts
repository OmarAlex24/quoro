import { HttpResponse, http } from 'msw';
import type {
  App,
  AppDetail,
  AppEnvList,
  AppEnvReplaceRequest,
  AppEnvVar,
  AppList,
  AuditExportList,
  Backup,
  BackupList,
  BackupRepositoryList,
  ChangePasswordRequest,
  Cluster,
  CreateAppRequest,
  CreateDeploymentRequest,
  CreateDomainRequest,
  CreateProjectRequest,
  CreateVolumeRequest,
  CreateVmRequest,
  Deployment,
  DeploymentList,
  DeploymentLogList,
  Domain,
  DomainList,
  LoginRequest,
  MeResponse,
  NodeList,
  Problem,
  ProblemCode,
  Project,
  ProjectList,
  SetupRequest,
  UpdateAppRequest,
  User,
  Vm,
  VmList,
  Volume,
  VolumeList,
} from '../api/generated/model';
import { getHealthMock } from '../api/generated/health/health.msw';
import { getAuthMock } from '../api/generated/auth/auth.msw';
import { getNodesMock } from '../api/generated/nodes/nodes.msw';
import { getProjectsMock } from '../api/generated/projects/projects.msw';
import { getVmsMock } from '../api/generated/vms/vms.msw';
import { getAppsMock } from '../api/generated/apps/apps.msw';
import { getDeploymentsMock } from '../api/generated/deployments/deployments.msw';
import { getDomainsMock } from '../api/generated/domains/domains.msw';
import { getLogsMock } from '../api/generated/logs/logs.msw';
import { getMetricsMock } from '../api/generated/metrics/metrics.msw';
import { getUsersMock } from '../api/generated/users/users.msw';
import { getAgentsMock } from '../api/generated/agents/agents.msw';
import { getGrantsMock } from '../api/generated/grants/grants.msw';
import { getApprovalsMock } from '../api/generated/approvals/approvals.msw';
import { getIncidentsMock } from '../api/generated/incidents/incidents.msw';
import { getAuditMock } from '../api/generated/audit/audit.msw';
import { getEventsMock } from '../api/generated/events/events.msw';
import { getWebhooksMock } from '../api/generated/webhooks/webhooks.msw';
import { formatSseFrame } from '../lib/log-buffer';
import { SLUG_PATTERN } from '../lib/slug';
import {
  converge,
  db,
  findApp,
  findDeployment,
  mockHaCost,
  mockId,
  now,
  runDeployment,
  schedule,
} from './db';
import {
  conflict,
  isIntAtLeast,
  json,
  notFound,
  page,
  problem,
  readJson,
  signedIn,
  unauthorized,
  validationFailed,
  writeRefusal,
} from './http';
import { agentsHandlers } from './handlers-agents';
import { approvalsHandlers } from './handlers-approvals';
import { auditHandlers } from './handlers-audit';
import { buildsHandlers } from './handlers-builds';
import { grantsHandlers } from './handlers-grants';
import { incidentsHandlers } from './handlers-incidents';
import { metricsHandlers } from './handlers-metrics';
import { tokensHandlers } from './handlers-tokens';
import { usersHandlers } from './handlers-users';
import { interactiveHandlers } from './handlers-interactive';
import { vulnerabilitiesHandlers } from './handlers-vulnerabilities';
import { webhooksHandlers } from './handlers-webhooks';

/** The mock cluster's id; one installation per page load. */
const CLUSTER_ID = mockId('cls');

const findVm = (vmId: string): Vm | undefined =>
  db.vms.find((candidate) => candidate.id === vmId);

const findDomain = (domainId: string): Domain | undefined =>
  db.domains.find((candidate) => candidate.id === domainId);

const APP_NAME_PATTERN = /^[a-z0-9-]{1,40}$/;
const ENV_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
const TLS_MODES = new Set(['acme', 'tunnel']);

const appDetail = (app: App): AppDetail => ({
  ...app,
  ha_cost: mockHaCost(app),
  instances: db.vms.filter((vm) => vm.app_id === app.id),
  domains: db.domains.filter((domain) => domain.app_id === app.id),
  edge_backends: db.edgeBackends[app.id] ?? [],
});

/**
 * Stateful handlers first: setup, session, projects and VMs answer from
 * `db`, with convergence simulated over ~1.5 s. The orval-generated handlers
 * stay as the fallback for anything the panel does not drive.
 */
export const handlers = [
  http.get('*/api/v1/health', () =>
    json({ status: 'ok' }),
  ),

  http.post('*/api/v1/setup', async ({ request }) => {
    if (db.setupDone) {
      return problem(409, 'conflict', {
        detail: 'Setup is already done. Log in instead.',
      });
    }

    const body = await readJson<SetupRequest>(request);
    if (!body) {
      return problem(422, 'validation_failed', {
        detail: 'The form could not be read. Try again.',
      });
    }

    const token = request.headers.get('Quoro-Setup-Token')?.trim() ?? '';
    const email = (body.email ?? '').trim();
    const name = (body.name ?? '').trim();
    const password = body.password ?? '';

    if (token === '') {
      return problem(422, 'validation_failed', {
        detail: 'The setup token is required.',
      });
    }
    if (!email.includes('@')) {
      return problem(422, 'validation_failed', {
        detail: 'Enter a valid email address.',
      });
    }
    if (name === '') {
      return problem(422, 'validation_failed', {
        detail: 'Enter the owner name.',
      });
    }
    if (password.length < 12) {
      return problem(422, 'validation_failed', {
        detail: 'Password must be at least 12 characters.',
      });
    }

    const owner: User = {
      id: mockId('usr'),
      email,
      name,
      role: 'owner',
      disabled: false,
      created_at: now(),
    };
    db.owner = owner;
    db.users = [owner];
    db.password = password;
    db.setupDone = true;
    db.sessionUserId = null;

    return json(owner, { status: 201 });
  }),

  http.post('*/api/v1/auth/login', async ({ request }) => {
    const body = await readJson<LoginRequest>(request);
    if (!body || !db.owner || !db.password) {
      return problem(401, 'unauthenticated', {
        detail: 'Email or password is incorrect.',
      });
    }
    if (
      body.email?.trim().toLowerCase() !== db.owner.email.toLowerCase() ||
      body.password !== db.password
    ) {
      return problem(401, 'unauthenticated', {
        detail: 'Email or password is incorrect.',
      });
    }
    db.sessionUserId = db.owner.id;
    const me: MeResponse = { type: 'user', user: db.owner };
    return json(me);
  }),

  http.post('*/api/v1/auth/logout', () => {
    db.sessionUserId = null;
    return new HttpResponse(null, { status: 204 });
  }),

  http.get('*/api/v1/auth/me', () => {
    const user = signedIn();
    if (!user) return unauthorized();
    const me: MeResponse = { type: 'user', user };
    return json(me);
  }),

  http.post('*/api/v1/auth/password', async ({ request }) => {
    if (!signedIn()) return unauthorized();

    const body = await readJson<ChangePasswordRequest>(request);
    if (!body) {
      return validationFailed('The form could not be read. Try again.');
    }
    if (!db.password || body.current_password !== db.password) {
      return validationFailed('the current password is wrong');
    }
    const next = body.new_password ?? '';
    if (next.length < 12) {
      return validationFailed('the new password must be at least 12 characters');
    }
    if (next === body.current_password) {
      return validationFailed(
        'the new password must be different from the current password',
      );
    }

    db.password = next;
    return new HttpResponse(null, { status: 204 });
  }),

  http.get('*/api/v1/cluster', () => {
    if (!signedIn()) return unauthorized();
    const machines = [db.node, ...db.extraNodes].filter((node) => node.status !== 'removed');
    const voters = machines
      .filter((node) => node.roles.some((role) => role === 'node' || role === 'witness'))
      .map((node) => ({
        node_id: node.id,
        roles: node.roles,
        connected: node.status === 'connected',
      }));
    const cluster: Cluster = {
      id: CLUSTER_ID,
      name: 'lab cluster',
      version: db.node.engine_version ?? '0.1.0',
      primary_node_id: db.node.id,
      log_seq: db.node.log_seq,
      log_head_hash: '9f2c4e1a7b3d5c8e0f6a2b4d6e8c0a1f3b5d7e9c1a3f5b7d9e1c3a5f7b9d1e3c',
      term: 1,
      votes: { voters, count: voters.length, required: 3, ha_ready: false },
      created_at: db.owner?.created_at ?? now(),
    };
    return json(cluster);
  }),

  http.get('*/api/v1/nodes', () => {
    if (!signedIn()) return unauthorized();
    const list: NodeList = {
      items: [db.node, ...db.extraNodes],
      next_cursor: null,
    };
    return json(list);
  }),

  http.get('*/api/v1/projects', () => {
    if (!signedIn()) return unauthorized();
    const list: ProjectList = { items: db.projects, next_cursor: null };
    return json(list);
  }),

  http.post('*/api/v1/projects', async ({ request }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role === 'viewer') return writeRefusal('project:write');

    const body = await readJson<CreateProjectRequest>(request);
    if (!body) {
      return problem(422, 'validation_failed', {
        detail: 'The form could not be read. Try again.',
      });
    }
    const name = (body.name ?? '').trim();
    const slug = body.slug ?? '';
    if (name === '') {
      return problem(422, 'validation_failed', {
        detail: 'Project name is required.',
      });
    }
    if (!SLUG_PATTERN.test(slug)) {
      return problem(422, 'validation_failed', {
        detail: 'Slug must be 1-40 lowercase letters, digits or dashes.',
      });
    }
    if (db.projects.some((project) => project.slug === slug)) {
      return problem(409, 'conflict', {
        detail: `A project with slug "${slug}" already exists.`,
      });
    }

    const project: Project = {
      id: mockId('prj'),
      slug,
      name,
      created_at: now(),
      updated_at: now(),
    };
    db.projects.push(project);
    return json(project, { status: 201 });
  }),

  http.get('*/api/v1/vms', ({ request }) => {
    if (!signedIn()) return unauthorized();
    const url = new URL(request.url);
    const projectId = url.searchParams.get('project_id');
    const nodeId = url.searchParams.get('node_id');
    const items = db.vms.filter(
      (vm) =>
        (projectId === null || vm.project_id === projectId) &&
        (nodeId === null || vm.node_id === nodeId),
    );
    const list: VmList = { items, next_cursor: null };
    return json(list);
  }),

  http.post('*/api/v1/vms', async ({ request }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role === 'viewer') return writeRefusal('vm:create');

    const body = await readJson<CreateVmRequest>(request);
    if (!body) {
      return problem(422, 'validation_failed', {
        detail: 'The form could not be read. Try again.',
      });
    }

    const name = (body.name ?? '').trim();
    if (name === '' || name.length > 200) {
      return problem(422, 'validation_failed', {
        detail: 'VM name must be 1-200 characters.',
      });
    }
    if (!isIntAtLeast(body.vcpus, 1)) {
      return problem(422, 'validation_failed', {
        detail: 'vCPUs must be a whole number of at least 1.',
      });
    }
    if (!isIntAtLeast(body.memory_mb, 1)) {
      return problem(422, 'validation_failed', {
        detail: 'Memory must be a whole number of at least 1 MiB.',
      });
    }
    if (!isIntAtLeast(body.disk_gb, 1)) {
      return problem(422, 'validation_failed', {
        detail: 'Disk must be a whole number of at least 1 GiB.',
      });
    }
    if (!(body.image ?? '').trim()) {
      return problem(422, 'validation_failed', {
        detail: 'An image reference is required.',
      });
    }
    if (!db.projects.some((project) => project.id === body.project_id)) {
      return problem(422, 'validation_failed', {
        detail: 'That project does not exist. Choose another one.',
      });
    }
    if (body.node_id !== db.node.id) {
      return problem(422, 'validation_failed', {
        detail: 'That node is not available. Choose another one.',
      });
    }

    const createdAt = now();
    const vm: Vm = {
      id: mockId('vm'),
      project_id: body.project_id,
      app_id: null,
      node_id: body.node_id,
      name,
      vcpus: body.vcpus,
      memory_mb: body.memory_mb,
      disk_gb: body.disk_gb,
      image: body.image.trim(),
      desired_status: 'running',
      observed_status: 'creating',
      ip: null,
      restart_generation: 0,
      restart_count: 0,
      last_exit_code: null,
      created_at: createdAt,
      updated_at: createdAt,
    };
    db.vms.push(vm);
    db.node.allocated = {
      cpus: db.node.allocated.cpus + vm.vcpus,
      memory_mb: db.node.allocated.memory_mb + vm.memory_mb,
      disk_gb: db.node.allocated.disk_gb + vm.disk_gb,
    };
    converge(vm.id, [
      [700, 'starting'],
      [1600, 'running'],
    ]);

    return json(vm, { status: 201 });
  }),

  http.post('*/api/v1/vms/:vmId/start', ({ params }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role === 'viewer') return writeRefusal('vm:start');

    const vm = findVm(String(params.vmId));
    if (!vm) {
      return problem(404, 'not_found', {
        detail: 'That VM no longer exists.',
      });
    }
    vm.desired_status = 'running';
    vm.updated_at = now();
    if (vm.observed_status !== 'running') {
      vm.observed_status = 'starting';
      converge(vm.id, [[1500, 'running']]);
    }
    return json(vm);
  }),

  http.post('*/api/v1/vms/:vmId/stop', ({ params }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role === 'viewer') return writeRefusal('vm:stop');

    const vm = findVm(String(params.vmId));
    if (!vm) {
      return problem(404, 'not_found', {
        detail: 'That VM no longer exists.',
      });
    }
    vm.desired_status = 'stopped';
    vm.updated_at = now();
    if (vm.observed_status !== 'stopped') {
      vm.observed_status = 'stopping';
      converge(vm.id, [[1500, 'stopped']]);
    }
    return json(vm);
  }),

  http.delete('*/api/v1/vms/:vmId', ({ params }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role === 'viewer') return writeRefusal('vm:delete');

    const index = db.vms.findIndex((vm) => vm.id === String(params.vmId));
    if (index === -1) {
      return problem(404, 'not_found', {
        detail: 'That VM no longer exists.',
      });
    }
    const [vm] = db.vms.splice(index, 1);
    if (!vm) {
      return problem(404, 'not_found', {
        detail: 'That VM no longer exists.',
      });
    }
    db.node.allocated = {
      cpus: Math.max(0, db.node.allocated.cpus - vm.vcpus),
      memory_mb: Math.max(0, db.node.allocated.memory_mb - vm.memory_mb),
      disk_gb: Math.max(0, db.node.allocated.disk_gb - vm.disk_gb),
    };
    return json(
      { ...vm, observed_status: 'deleted', updated_at: now() },
      { status: 202 },
    );
  }),

  http.get('*/api/v1/apps', ({ request }) => {
    if (!signedIn()) return unauthorized();
    const url = new URL(request.url);
    const projectId = url.searchParams.get('project_id');
    const items = db.apps.filter(
      (app) => projectId === null || app.project_id === projectId,
    );
    const list: AppList = { items, next_cursor: null };
    return json(list);
  }),

  http.post('*/api/v1/apps', async ({ request }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role === 'viewer') return writeRefusal('app:create');

    const body = await readJson<CreateAppRequest>(request);
    if (!body) {
      return problem(422, 'validation_failed', {
        detail: 'The form could not be read. Try again.',
      });
    }

    const name = (body.name ?? '').trim();
    if (!APP_NAME_PATTERN.test(name)) {
      return problem(422, 'validation_failed', {
        detail: 'App name must be 1-40 lowercase letters, digits or dashes.',
      });
    }
    const hasSource = body.source !== undefined && body.source !== null;
    const hasImage = (body.image ?? '').trim() !== '';
    if (hasSource === hasImage) {
      return problem(422, 'validation_failed', {
        detail: 'Give either an image reference or a Git source, not both.',
      });
    }
    if (hasSource) {
      if (!/^https:\/\/[^\s]+$/.test(body.source?.repo_url ?? '')) {
        return problem(422, 'validation_failed', {
          detail: 'The repository URL must be a public https:// URL.',
        });
      }
      if ((body.source?.ref ?? '').trim() === '') {
        return problem(422, 'validation_failed', {
          detail: 'A branch, tag or commit is required.',
        });
      }
    }
    if (!isIntAtLeast(body.port, 1) || body.port > 65_535) {
      return problem(422, 'validation_failed', {
        detail: 'Port must be a whole number between 1 and 65535.',
      });
    }
    if (!db.projects.some((project) => project.id === body.project_id)) {
      return problem(422, 'validation_failed', {
        detail: 'That project does not exist. Choose another one.',
      });
    }
    if (db.apps.some((app) => app.project_id === body.project_id && app.name === name)) {
      return problem(409, 'conflict', {
        detail: `An app named "${name}" already exists in that project.`,
      });
    }

    const createdAt = now();
    const app: App = {
      id: mockId('app'),
      project_id: body.project_id,
      name,
      stateful: body.stateful ?? false,
      ha_policy: 'none',
      backup_schedule: body.backup_schedule ?? { interval_h: 24, keep: 7 },
      replicas: body.replicas ?? 1,
      image: hasSource ? '' : (body.image ?? '').trim(),
      source: hasSource ? (body.source ?? null) : null,
      port: body.port,
      health_check: body.health_check ?? {
        path: '/',
        interval_s: 10,
        timeout_s: 2,
        failures: 3,
      },
      resources: body.resources ?? { vcpus: 1, memory_mb: 512, disk_gb: 2 },
      placement: body.placement ?? { node_ids: [db.node.id] },
      status: 'stopped',
      current_deployment_id: null,
      restart_generation: 0,
      created_at: createdAt,
      updated_at: createdAt,
    };
    db.apps.push(app);
    db.appEnv[app.id] = [];
    return json(app, { status: 201 });
  }),

  http.get('*/api/v1/apps/:appId', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }
    return json(appDetail(app));
  }),

  http.patch('*/api/v1/apps/:appId', async ({ request, params }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role === 'viewer') return writeRefusal('app:update');

    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }
    const body = await readJson<UpdateAppRequest>(request);
    if (!body) return validationFailed('The form could not be read. Try again.');

    if (body.name !== undefined) {
      const name = body.name.trim();
      if (!APP_NAME_PATTERN.test(name)) {
        return validationFailed('App name must be 1-40 lowercase letters, digits or dashes.');
      }
      if (
        db.apps.some(
          (candidate) =>
            candidate.id !== app.id &&
            candidate.project_id === app.project_id &&
            candidate.name === name,
        )
      ) {
        return conflict(`An app named "${name}" already exists in that project.`);
      }
      app.name = name;
    }

    if (body.port !== undefined) {
      if (!isIntAtLeast(body.port, 1) || body.port > 65_535) {
        return validationFailed('Port must be a whole number between 1 and 65535.');
      }
      app.port = body.port;
    }

    if (body.ha_policy !== undefined) {
      if (body.ha_policy === 'warm' || body.ha_policy === 'hot') {
        return validationFailed('Warm and hot HA are not in this release.');
      }
      if (body.ha_policy === 'cold') {
        const volume = db.volumes.find(
          (candidate) =>
            candidate.app_id === app.id &&
            (candidate.state === 'creating' ||
              candidate.state === 'ready' ||
              candidate.state === 'restoring'),
        );
        if (!volume) return conflict('Cold HA needs a volume. Add one to the app first.');
        if (db.backupRepositories.length === 0) {
          return conflict('Cold HA needs a backup repository.');
        }
      }
      app.ha_policy = body.ha_policy;
    }

    if (body.backup_schedule !== undefined) {
      if (
        !isIntAtLeast(body.backup_schedule.interval_h, 1) ||
        body.backup_schedule.interval_h > 168
      ) {
        return validationFailed('The backup interval must be 1 to 168 hours.');
      }
      if (!isIntAtLeast(body.backup_schedule.keep, 1) || body.backup_schedule.keep > 100) {
        return validationFailed('Keep must be a whole number from 1 to 100.');
      }
      app.backup_schedule = { ...body.backup_schedule };
    }

    app.updated_at = now();
    return json({ ...app, ha_cost: mockHaCost(app) });
  }),

  http.delete('*/api/v1/apps/:appId', ({ params }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role === 'viewer') return writeRefusal('app:delete');

    const appId = String(params.appId);
    const app = findApp(appId);
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }

    db.apps = db.apps.filter((candidate) => candidate.id !== appId);
    db.vms = db.vms.filter((vm) => vm.app_id !== appId);
    db.domains = db.domains.filter((domain) => domain.app_id !== appId);
    for (const deployment of db.deployments.filter((item) => item.app_id === appId)) {
      delete db.deploymentLogs[deployment.id];
    }
    db.deployments = db.deployments.filter((item) => item.app_id !== appId);
    for (const build of db.builds.filter((item) => item.app_id === appId)) {
      delete db.buildLogs[build.id];
    }
    db.builds = db.builds.filter((item) => item.app_id !== appId);
    delete db.appEnv[appId];
    delete db.edgeBackends[appId];

    return json({ ...app, updated_at: now() }, { status: 202 });
  }),

  http.post('*/api/v1/apps/:appId/start', ({ params }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role === 'viewer') return writeRefusal('app:start');

    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }
    app.status = 'running';
    app.updated_at = now();
    for (const vm of db.vms.filter((candidate) => candidate.app_id === app.id)) {
      vm.desired_status = 'running';
      if (vm.observed_status !== 'running') {
        vm.observed_status = 'starting';
        converge(vm.id, [[1_200, 'running']]);
      }
    }
    return json(app, { status: 202 });
  }),

  http.post('*/api/v1/apps/:appId/stop', ({ params }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role === 'viewer') return writeRefusal('app:stop');

    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }
    app.status = 'stopped';
    app.updated_at = now();
    for (const vm of db.vms.filter((candidate) => candidate.app_id === app.id)) {
      vm.desired_status = 'stopped';
      if (vm.observed_status !== 'stopped') {
        vm.observed_status = 'stopping';
        converge(vm.id, [[1_200, 'stopped']]);
      }
    }
    return json(app, { status: 202 });
  }),

  http.post('*/api/v1/apps/:appId/restart', ({ params }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role === 'viewer') return writeRefusal('app:restart');

    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }
    app.status = 'running';
    app.restart_generation += 1;
    app.updated_at = now();
    for (const vm of db.vms.filter((candidate) => candidate.app_id === app.id)) {
      vm.restart_generation = app.restart_generation;
      vm.restart_count += 1;
      vm.desired_status = 'running';
      vm.observed_status = 'starting';
      converge(vm.id, [[1_200, 'running']]);
    }
    return json(app, { status: 202 });
  }),

  http.post('*/api/v1/apps/:appId/redeploy', ({ params }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role === 'viewer') return writeRefusal('app:redeploy');

    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }
    const deployment = runDeployment(app.id, app.image, 'redeploy');
    return json(deployment, { status: 202 });
  }),

  http.get('*/api/v1/apps/:appId/env', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }
    const list: AppEnvList = { items: db.appEnv[app.id] ?? [] };
    return json(list);
  }),

  http.put('*/api/v1/apps/:appId/env', async ({ request, params }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role === 'viewer') return writeRefusal('app:update');

    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }
    const body = await readJson<AppEnvReplaceRequest>(request);
    if (!body || !Array.isArray(body.items)) {
      return problem(422, 'validation_failed', {
        detail: 'The variable list could not be read.',
      });
    }

    const seen = new Set<string>();
    const items: AppEnvVar[] = [];
    for (const item of body.items) {
      const name = (item.name ?? '').trim();
      if (!ENV_NAME_PATTERN.test(name)) {
        return problem(422, 'validation_failed', {
          detail: `"${name}" is not a valid variable name.`,
        });
      }
      if (seen.has(name)) {
        return problem(422, 'validation_failed', {
          detail: `The variable ${name} is listed twice.`,
        });
      }
      seen.add(name);
      items.push({ app_id: app.id, name, updated_at: now() });
    }

    db.appEnv[app.id] = items;
    return json({ items } satisfies AppEnvList);
  }),

  http.get('*/api/v1/apps/:appId/volumes', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }
    const items = db.volumes
      .filter((volume) => volume.app_id === app.id)
      .sort((left, right) => Date.parse(right.created_at) - Date.parse(left.created_at));
    const list: VolumeList = { items };
    return json(list);
  }),

  http.post('*/api/v1/apps/:appId/volumes', async ({ request, params }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role === 'viewer') return writeRefusal('app:update');

    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }
    const body = await readJson<CreateVolumeRequest>(request);
    if (!body) return validationFailed('The form could not be read. Try again.');
    if (!isIntAtLeast(body.size_gb, 1) || body.size_gb > 1024) {
      return validationFailed('Volume size must be a whole number from 1 to 1024 GiB.');
    }
    const mountPath = (body.mount_path ?? '').trim();
    const segments = mountPath.split('/').slice(1);
    if (
      !/^\/[A-Za-z0-9._/-]+$/.test(mountPath) ||
      mountPath.endsWith('/') ||
      segments.some(
        (segment) => segment === '' || segment === '.' || segment === '..',
      ) ||
      /^\/(proc|sys|dev)(\/|$)/.test(mountPath)
    ) {
      return validationFailed('Mount path must be absolute and free of . and .. segments.');
    }
    if (
      db.volumes.some(
        (volume) =>
          volume.app_id === app.id &&
          (volume.state === 'creating' ||
            volume.state === 'ready' ||
            volume.state === 'restoring'),
      )
    ) {
      return conflict('This app already has a volume.');
    }

    const createdAt = now();
    const volume: Volume = {
      id: mockId('vol'),
      app_id: app.id,
      node_id: app.placement.node_ids[0] ?? db.node.id,
      size_gb: body.size_gb,
      mount_path: mountPath,
      state: 'creating',
      allocated_bytes: null,
      restored_from_backup_id: null,
      error: null,
      quarantines: [],
      created_at: createdAt,
      updated_at: createdAt,
    };
    db.volumes.push(volume);
    app.stateful = true;
    app.updated_at = createdAt;

    // The node reports the sparse file a moment later, as it does in the contract.
    schedule(() => {
      const current = db.volumes.find((candidate) => candidate.id === volume.id);
      if (!current) return;
      current.state = 'ready';
      current.allocated_bytes = Math.round(current.size_gb * 1024 ** 3 * 0.05);
      current.updated_at = now();
    }, 1_200);

    return json(volume, { status: 201 });
  }),

  http.get('*/api/v1/apps/:appId/backups', ({ request, params }) => {
    if (!signedIn()) return unauthorized();
    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }
    const items = db.backups
      .filter((backup) => backup.app_id === app.id)
      .sort((left, right) => Date.parse(right.created_at) - Date.parse(left.created_at));
    const list: BackupList = page(items, request);
    return json(list);
  }),

  http.post('*/api/v1/apps/:appId/backups', ({ params }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role === 'viewer') return writeRefusal('backup:write');

    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }
    const volume = db.volumes.find(
      (candidate) => candidate.app_id === app.id && candidate.state === 'ready',
    );
    if (!volume) return conflict('This app has no ready volume to back up.');
    const repository = db.backupRepositories[0];
    if (!repository) return conflict('The cluster has no backup repository.');

    const inFlight = db.backups.find(
      (backup) =>
        backup.volume_id === volume.id &&
        (backup.status === 'queued' || backup.status === 'running'),
    );
    if (inFlight) return conflict('A backup of this volume is already running.');

    const backup: Backup = {
      id: mockId('bkp'),
      repository_id: repository.id,
      source_kind: 'volume',
      app_id: app.id,
      volume_id: volume.id,
      node_id: volume.node_id,
      status: 'queued',
      trigger: 'api',
      snapshot_method: null,
      pause_ms: null,
      logical_bytes: null,
      uploaded_bytes: null,
      chunk_count: null,
      error: null,
      created_at: now(),
      started_at: null,
      finished_at: null,
      pruned_at: null,
    };
    db.backups.push(backup);

    schedule(() => {
      const current = db.backups.find((candidate) => candidate.id === backup.id);
      if (!current) return;
      current.status = 'running';
      current.snapshot_method = 'reflink';
      current.pause_ms = 0;
      current.started_at = now();
    }, 500);
    schedule(() => {
      const current = db.backups.find((candidate) => candidate.id === backup.id);
      if (!current) return;
      const logical = volume.allocated_bytes ?? Math.round(volume.size_gb * 1024 ** 3 * 0.05);
      current.status = 'succeeded';
      current.logical_bytes = logical;
      current.uploaded_bytes = Math.round(logical * 0.2);
      current.chunk_count = Math.max(1, Math.round(logical / (8 * 1024 ** 2)));
      current.finished_at = now();
    }, 2_200);

    return json(backup, { status: 202 });
  }),

  http.get('*/api/v1/backup-repositories', () => {
    if (!signedIn()) return unauthorized();
    const list: BackupRepositoryList = { items: db.backupRepositories };
    return json(list);
  }),

  http.get('*/api/v1/audit-exports', () => {
    if (!signedIn()) return unauthorized();
    const list: AuditExportList = { items: db.auditExports };
    return json(list);
  }),

  http.post('*/api/v1/audit-exports/:exportId/probe', ({ params }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role !== 'owner') {
      return problem(403, 'forbidden', {
        reason: 'human_only',
        scope: 'provider:manage',
        detail: 'Only an owner can probe the off-site copy.',
      });
    }
    const auditExport = db.auditExports.find(
      (candidate) => candidate.id === String(params.exportId),
    );
    if (!auditExport) return notFound('That audit export');
    auditExport.probed_at = now();
    auditExport.probe_detail = 'Overwrite refused with 409; delete refused with 409.';
    auditExport.updated_at = now();
    return json(auditExport);
  }),

  http.get('*/api/v1/apps/:appId/deployments', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }
    const items = db.deployments
      .filter((deployment) => deployment.app_id === app.id)
      .slice()
      .reverse();
    const list: DeploymentList = { items, next_cursor: null };
    return json(list);
  }),

  http.post('*/api/v1/apps/:appId/deployments', async ({ request, params }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role === 'viewer') return writeRefusal('app:deploy');

    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }
    if (app.source) {
      return problem(409, 'conflict', {
        detail: 'This app builds its image from Git. Start a build instead.',
      });
    }
    const body = await readJson<CreateDeploymentRequest>(request);
    if (!body || (body.image ?? '').trim() === '') {
      return problem(422, 'validation_failed', {
        detail: 'An image reference is required.',
      });
    }
    const deployment = runDeployment(app.id, body.image.trim(), 'api');
    return json(deployment, { status: 202 });
  }),

  http.get('*/api/v1/deployments/:deploymentId', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const deployment = findDeployment(String(params.deploymentId));
    if (!deployment) {
      return problem(404, 'not_found', {
        detail: 'That deployment no longer exists.',
      });
    }
    return json(deployment);
  }),

  http.get('*/api/v1/deployments/:deploymentId/logs', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const deployment = findDeployment(String(params.deploymentId));
    if (!deployment) {
      return problem(404, 'not_found', {
        detail: 'That deployment no longer exists.',
      });
    }
    const list: DeploymentLogList = {
      items: db.deploymentLogs[deployment.id] ?? [],
    };
    return json(list);
  }),

  http.get('*/api/v1/apps/:appId/domains', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }
    const list: DomainList = {
      items: db.domains.filter((domain) => domain.app_id === app.id),
      next_cursor: null,
    };
    return json(list);
  }),

  http.post('*/api/v1/apps/:appId/domains', async ({ request, params }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role === 'viewer') return writeRefusal('app:update');

    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }
    const body = await readJson<CreateDomainRequest>(request);
    if (!body) {
      return problem(422, 'validation_failed', {
        detail: 'The form could not be read. Try again.',
      });
    }
    const hostname = (body.hostname ?? '').trim().toLowerCase();
    if (hostname === '' || hostname.includes(' ')) {
      return problem(422, 'validation_failed', {
        detail: 'Enter a hostname like blog.example.com.',
      });
    }
    if (!TLS_MODES.has(body.tls_mode)) {
      return problem(422, 'validation_failed', {
        detail: 'Choose Cloudflare Tunnel or an ACME certificate.',
      });
    }
    if (db.domains.some((domain) => domain.hostname === hostname)) {
      return problem(409, 'conflict', {
        detail: `${hostname} is already routed somewhere.`,
      });
    }

    const domain: Domain = {
      id: mockId('dom'),
      app_id: app.id,
      hostname,
      tls_mode: body.tls_mode,
      cert_status: body.tls_mode === 'tunnel' ? 'not_applicable' : 'pending',
      cert_expires_at: null,
      serving_status: 'serving',
      last_error: null,
      created_at: now(),
    };
    db.domains.push(domain);
    if (body.tls_mode === 'acme') {
      schedule(() => {
        const current = findDomain(domain.id);
        if (current && current.cert_status === 'pending') current.cert_status = 'issued';
      }, 2_000);
    }
    return json(domain, { status: 201 });
  }),

  http.delete('*/api/v1/domains/:domainId', ({ params }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role === 'viewer') return writeRefusal('app:update');

    const domainId = String(params.domainId);
    const domain = findDomain(domainId);
    if (!domain) {
      return problem(404, 'not_found', {
        detail: 'That domain is already gone.',
      });
    }
    db.domains = db.domains.filter((candidate) => candidate.id !== domainId);
    return new HttpResponse(null, { status: 204 });
  }),

  http.get('*/api/v1/apps/:appId/logs/stream', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }

    const vmId = db.vms.find((vm) => vm.app_id === app.id)?.id ?? mockId('vm');
    const encoder = new TextEncoder();
    let sequence = 0;
    let timer: ReturnType<typeof setInterval> | undefined;

    const line = () => {
      sequence += 1;
      const paths = ['/', '/health', '/assets/app.css', '/favicon.ico'];
      const status = sequence % 11 === 0 ? 404 : 200;
      const path = paths[sequence % paths.length] ?? '/';
      return {
        time: now(),
        vm_id: vmId,
        stream: 'stdout',
        line: `10.0.0.1 - - [${new Date().toUTCString()}] "GET ${path} HTTP/1.1" ${status} 615 "-" "Mozilla/5.0"`,
      };
    };

    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        const emit = () => {
          controller.enqueue(
            encoder.encode(
              formatSseFrame({
                id: sequence + 1,
                event: 'log',
                data: JSON.stringify(line()),
              }),
            ),
          );
        };
        emit();
        timer = setInterval(emit, 1_000);
      },
      cancel() {
        if (timer !== undefined) clearInterval(timer);
      },
    });

    return new HttpResponse(stream, {
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
      },
    });
  }),

  // Builds, then the agency screens: users, tokens, agents, grants, approvals, audit,
  // webhooks, incidents.
  ...buildsHandlers,
  ...usersHandlers,
  ...tokensHandlers,
  ...agentsHandlers,
  ...grantsHandlers,
  ...approvalsHandlers,
  ...auditHandlers,
  ...webhooksHandlers,
  ...incidentsHandlers,
  ...metricsHandlers,
  ...interactiveHandlers,
  ...vulnerabilitiesHandlers,

  // Generated fallbacks, kept last so every stateful route above wins.
  ...getHealthMock(),
  ...getAuthMock(),
  ...getNodesMock(),
  ...getProjectsMock(),
  ...getVmsMock(),
  ...getAppsMock(),
  ...getDeploymentsMock(),
  ...getDomainsMock(),
  ...getLogsMock(),
  ...getMetricsMock(),
  ...getUsersMock(),
  ...getAgentsMock(),
  ...getGrantsMock(),
  ...getApprovalsMock(),
  ...getIncidentsMock(),
  ...getAuditMock(),
  ...getEventsMock(),
  ...getWebhooksMock(),
];
