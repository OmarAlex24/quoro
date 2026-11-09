import { http, type HttpHandler } from 'msw';
import type {
  BackupList,
  BackupRepository,
  BackupRepositoryWithRecoveryKey,
  BackupSchedule,
  CreateBackupRepositoryRequest,
  CreateProviderRequest,
  Provider,
  ProviderKind,
  ProviderList,
  RotateProviderSecretRequest,
  UpdateBackupRepositoryRequest,
  User,
} from '../api/generated/model';
import { appendAudit } from './agency';
import { db, mockId, now, repositoryObjectKey } from './db';
import {
  conflict,
  isIntAtLeast,
  json,
  noContent,
  notFound,
  page,
  problem,
  readJson,
  signedIn,
  unauthorized,
  validationFailed,
  writeRefusal,
} from './http';

/*
 * Settings › Backups and Providers in mock mode: provider entries added and
 * listed without their secrets, a provider deletion that the things still
 * holding it refuse, the backup repository created with its recovery key
 * answered once, its name and control-plane schedule changed, the recovery
 * key replaced, and the cluster-wide backup list the control-plane backups
 * are read from. Refusals carry the server's own words.
 */

const CROCKFORD = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

/** A key of the contract's shape, `qrk1` and thirteen groups of four Crockford characters. */
const recoveryKey = (): string => {
  const bytes = crypto.getRandomValues(new Uint8Array(52));
  const groups: string[] = [];
  for (let group = 0; group < 13; group += 1) {
    let chars = '';
    for (let index = 0; index < 4; index += 1) {
      chars += CROCKFORD[(bytes[group * 4 + index] ?? 0) % CROCKFORD.length];
    }
    groups.push(chars);
  }
  return `qrk1-${groups.join('-')}`;
};

const fingerprint = (): string =>
  Array.from(crypto.getRandomValues(new Uint8Array(8)), (byte) =>
    byte.toString(16).padStart(2, '0'),
  ).join('');

/** `provider:manage`: human-only, owner and operator. */
const providerManager = (): User | Response => {
  const user = signedIn();
  if (!user) return unauthorized();
  if (user.role === 'viewer') return writeRefusal('provider:manage');
  return user;
};

/** `backup:manage`: human-only and owner-only. */
const backupManager = (): User | Response => {
  const user = signedIn();
  if (!user) return unauthorized();
  if (user.role !== 'owner') return writeRefusal('backup:manage');
  return user;
};

const PROVIDER_KINDS: readonly ProviderKind[] = ['dns', 'tunnel', 'lb', 'object_store'];

const charCount = (text: string): number => [...text].length;

/** The server's name rule for providers and repositories: 1 to 200 characters. */
const nameRefusal = (name: unknown): Response | null =>
  typeof name !== 'string' || charCount(name) < 1 || charCount(name) > 200
    ? validationFailed('name must be between 1 and 200 characters')
    : null;

const PREFIX = /^[A-Za-z0-9._-]+(\/[A-Za-z0-9._-]+)*$/;

/** `validate_prefix`: the contract pattern, 1 to 200 characters. */
const prefixRefusal = (prefix: unknown): Response | null => {
  if (typeof prefix !== 'string' || prefix.length < 1 || prefix.length > 200) {
    return validationFailed('prefix must be between 1 and 200 characters');
  }
  if (!PREFIX.test(prefix)) {
    return validationFailed('prefix may only hold letters, digits, `.`, `_`, `-` and `/`');
  }
  return null;
};

/** `validate_schedule`: interval 1 to 168 hours, keep 1 to 100. */
const scheduleRefusal = (schedule: unknown): Response | null => {
  const value = (schedule ?? {}) as Partial<BackupSchedule>;
  if (!isIntAtLeast(value.interval_h, 1) || value.interval_h > 168) {
    return validationFailed('control_plane_schedule.interval_h is outside 1-168');
  }
  if (!isIntAtLeast(value.keep, 1) || value.keep > 100) {
    return validationFailed('control_plane_schedule.keep is outside 1-100');
  }
  return null;
};

/** What still holds a provider, as the server would name it in its refusal. */
const holder = (providerId: string): string | null => {
  const repository = db.backupRepositories.find((item) => item.provider_id === providerId);
  if (repository) return `The backup repository ${repository.name} still uses this provider.`;
  if (db.auditExports.some((item) => item.provider_id === providerId)) {
    return 'The off-site copy of the audit log still uses this provider.';
  }
  if (db.domains.some((domain) => domain.provider_id === providerId)) {
    return 'A domain still uses this provider.';
  }
  return null;
};

export const providersHandlers: HttpHandler[] = [
  http.get('*/api/v1/providers', ({ request }) => {
    const user = providerManager();
    if (user instanceof Response) return user;
    const list: ProviderList = page(db.providers, request);
    return json(list);
  }),

  http.post('*/api/v1/providers', async ({ request }) => {
    const user = providerManager();
    if (user instanceof Response) return user;
    const body = await readJson<Partial<CreateProviderRequest>>(request);
    if (!body) return validationFailed('The request body is not JSON.');
    if (!PROVIDER_KINDS.includes(body.kind as ProviderKind)) {
      return validationFailed('kind must be one of dns, tunnel, lb, object_store');
    }
    const badName = nameRefusal(body.name);
    if (badName) return badName;
    const name = body.name as string;
    if (typeof body.config !== 'object' || body.config === null || Array.isArray(body.config)) {
      return validationFailed('config must be a JSON object');
    }
    const secretBytes =
      typeof body.secret === 'string' ? new TextEncoder().encode(body.secret).length : 0;
    if (secretBytes < 1 || secretBytes > 16_384) {
      return validationFailed('secret must be between 1 and 16384 bytes');
    }
    if (db.providers.some((item) => item.name === name)) {
      return conflict(`name ${name} is taken`);
    }

    // The mock keeps no secret at all: only that one is stored.
    const provider: Provider = {
      id: mockId('prv'),
      kind: body.kind as ProviderKind,
      name,
      config: { ...body.config },
      has_secret: true,
      created_by: user.id,
      created_at: now(),
      rotated_at: null,
    };
    db.providers.push(provider);
    appendAudit({
      operation: 'POST /api/v1/providers',
      target_type: 'provider',
      target_id: provider.id,
      params: { kind: provider.kind, name, config: provider.config, secret: '[redacted]' },
    });
    return json(provider, { status: 201 });
  }),

  http.post('*/api/v1/providers/:providerId/rotate', async ({ request, params }) => {
    const user = providerManager();
    if (user instanceof Response) return user;
    const provider = db.providers.find((item) => item.id === String(params.providerId));
    if (!provider) return notFound('That provider');

    const body = await readJson<RotateProviderSecretRequest>(request);
    if (!body || typeof body.secret !== 'string' || body.secret.length === 0) {
      return validationFailed('secret is required.');
    }
    provider.has_secret = true;
    provider.rotated_at = now();
    appendAudit({
      operation: 'POST /api/v1/providers/{provider_id}/rotate',
      target_type: 'provider',
      target_id: provider.id,
      params: {},
    });
    return json(provider);
  }),

  http.delete('*/api/v1/providers/:providerId', ({ params }) => {
    const user = providerManager();
    if (user instanceof Response) return user;
    const provider = db.providers.find((item) => item.id === String(params.providerId));
    if (!provider) return notFound('That provider');

    const held = holder(provider.id);
    if (held) return conflict(held);
    db.providers = db.providers.filter((item) => item.id !== provider.id);
    appendAudit({
      operation: 'DELETE /api/v1/providers/{provider_id}',
      target_type: 'provider',
      target_id: provider.id,
      params: {},
    });
    return noContent();
  }),

  http.post('*/api/v1/backup-repositories', async ({ request }) => {
    const user = backupManager();
    if (user instanceof Response) return user;
    const body = await readJson<Partial<CreateBackupRepositoryRequest>>(request);
    if (!body) return validationFailed('The request body is not JSON.');
    const badName = nameRefusal(body.name);
    if (badName) return badName;
    const prefix = body.prefix ?? 'quoro-backups';
    const badPrefix = prefixRefusal(prefix);
    if (badPrefix) return badPrefix;
    if (body.control_plane_schedule !== undefined) {
      const badSchedule = scheduleRefusal(body.control_plane_schedule);
      if (badSchedule) return badSchedule;
    }
    if (db.backupRepositories.length > 0) {
      return conflict('the cluster already has a backup repository');
    }
    const provider = db.providers.find((item) => item.id === body.provider_id);
    if (!provider) return validationFailed('the provider does not exist');
    if (provider.kind !== 'object_store') {
      return validationFailed('the provider is not an object_store provider');
    }
    if (db.auditExports.some((item) => item.provider_id === provider.id)) {
      return validationFailed('the provider holds the audit export');
    }
    for (const field of ['endpoint', 'bucket', 'access_key_id']) {
      if (typeof provider.config[field] !== 'string') {
        return validationFailed(`the provider config has no ${field}`);
      }
    }
    // The config object is written with put-if-absent: an old one refuses it.
    const objectKey = repositoryObjectKey(provider.config, prefix);
    if (db.repositoryObjects.includes(objectKey)) {
      return conflict('the prefix already holds a config object');
    }

    const repository: BackupRepository = {
      id: mockId('brp'),
      name: body.name as string,
      provider_id: provider.id,
      prefix,
      recovery_key_fingerprint: fingerprint(),
      recovery_key_rotated_at: null,
      control_plane_schedule: body.control_plane_schedule
        ? { ...body.control_plane_schedule }
        : { interval_h: 6, keep: 7 },
      created_by: user.id,
      created_at: now(),
    };
    db.backupRepositories.push(repository);
    db.repositoryObjects.push(objectKey);
    appendAudit({
      operation: 'POST /api/v1/backup-repositories',
      target_type: 'backup_repository',
      target_id: repository.id,
      params: {
        name: repository.name,
        provider_id: provider.id,
        prefix,
        control_plane_schedule: repository.control_plane_schedule,
      },
    });
    // The key is made for this response and kept nowhere.
    const answer: BackupRepositoryWithRecoveryKey = { ...repository, recovery_key: recoveryKey() };
    return json(answer, { status: 201 });
  }),

  http.patch('*/api/v1/backup-repositories/:repositoryId', async ({ request, params }) => {
    const user = backupManager();
    if (user instanceof Response) return user;
    const repository = db.backupRepositories.find(
      (item) => item.id === String(params.repositoryId),
    );
    if (!repository) return notFound('That backup repository');
    const body = await readJson<UpdateBackupRepositoryRequest>(request);
    if (!body) return validationFailed('The request body is not JSON.');
    if (body.name !== undefined) {
      const badName = nameRefusal(body.name);
      if (badName) return badName;
    }
    if (body.control_plane_schedule !== undefined) {
      const badSchedule = scheduleRefusal(body.control_plane_schedule);
      if (badSchedule) return badSchedule;
    }

    if (body.name !== undefined) repository.name = body.name;
    if (body.control_plane_schedule !== undefined) {
      repository.control_plane_schedule = { ...body.control_plane_schedule };
    }
    appendAudit({
      operation: 'PATCH /api/v1/backup-repositories/{repository_id}',
      target_type: 'backup_repository',
      target_id: repository.id,
      params: { ...body },
    });
    return json(repository);
  }),

  http.post('*/api/v1/backup-repositories/:repositoryId/recovery-key', ({ params }) => {
    const user = backupManager();
    if (user instanceof Response) return user;
    const repository = db.backupRepositories.find(
      (item) => item.id === String(params.repositoryId),
    );
    if (!repository) return notFound('That backup repository');

    repository.recovery_key_fingerprint = fingerprint();
    repository.recovery_key_rotated_at = now();
    appendAudit({
      operation: 'POST /api/v1/backup-repositories/{repository_id}/recovery-key',
      target_type: 'backup_repository',
      target_id: repository.id,
      params: {},
    });
    // The key is made for this response and kept nowhere.
    const answer: BackupRepositoryWithRecoveryKey = { ...repository, recovery_key: recoveryKey() };
    return json(answer);
  }),

  http.get('*/api/v1/backups', ({ request }) => {
    if (!signedIn()) return unauthorized();
    const url = new URL(request.url);
    const sourceKind = url.searchParams.get('source_kind');
    const appId = url.searchParams.get('app_id');
    if (sourceKind !== null && sourceKind !== 'volume' && sourceKind !== 'control_plane') {
      return problem(422, 'validation_failed', { detail: 'source_kind is volume or control_plane.' });
    }
    const items = db.backups
      .filter((backup) => (sourceKind ? backup.source_kind === sourceKind : true))
      .filter((backup) => (appId ? backup.app_id === appId : true))
      .sort((left, right) => Date.parse(right.created_at) - Date.parse(left.created_at));
    const list: BackupList = page(items, request);
    return json(list);
  }),
];
