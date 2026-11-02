import { HttpResponse, http } from 'msw';
import type {
  CreateProjectRequest,
  CreateVmRequest,
  LoginRequest,
  MeResponse,
  NodeList,
  Problem,
  ProblemCode,
  Project,
  ProjectList,
  SetupRequest,
  User,
  Vm,
  VmList,
} from '../api/generated/model';
import { getHealthMock } from '../api/generated/health/health.msw';
import { getAuthMock } from '../api/generated/auth/auth.msw';
import { getNodesMock } from '../api/generated/nodes/nodes.msw';
import { getProjectsMock } from '../api/generated/projects/projects.msw';
import { getVmsMock } from '../api/generated/vms/vms.msw';
import { SLUG_PATTERN } from '../lib/slug';
import { converge, db, mockId, now } from './db';

type JsonBody = Parameters<typeof HttpResponse.json>[0];
type JsonInit = Parameters<typeof HttpResponse.json>[1];

/** A JSON response typed as a plain `Response`, so handlers can mix outcomes. */
const json = (body: JsonBody, init?: JsonInit): Response =>
  HttpResponse.json(body, init);

const problem = (
  status: number,
  code: ProblemCode,
  extra: Partial<Problem> = {},
): Response =>
  json(
    { status, code, ...extra },
    { status, headers: { 'Content-Type': 'application/problem+json' } },
  );

const unauthorized = () =>
  problem(401, 'unauthenticated', { detail: 'Log in to continue.' });

const signedIn = (): User | null =>
  db.owner && db.sessionUserId === db.owner.id ? db.owner : null;

const writeRefusal = (scope: string) =>
  problem(403, 'forbidden', {
    reason: 'scope_missing',
    scope,
    detail: `Your role can't use ${scope}.`,
  });

const readJson = async <T>(request: Request): Promise<T | null> => {
  try {
    return (await request.json()) as T;
  } catch {
    return null;
  }
};

const isIntAtLeast = (value: unknown, minimum: number): value is number =>
  typeof value === 'number' && Number.isInteger(value) && value >= minimum;

const findVm = (vmId: string): Vm | undefined =>
  db.vms.find((candidate) => candidate.id === vmId);

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

  http.get('*/api/v1/nodes', () => {
    if (!signedIn()) return unauthorized();
    const list: NodeList = { items: [db.node], next_cursor: null };
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

  // Generated fallbacks, kept last so every stateful route above wins.
  ...getHealthMock(),
  ...getAuthMock(),
  ...getNodesMock(),
  ...getProjectsMock(),
  ...getVmsMock(),
];
