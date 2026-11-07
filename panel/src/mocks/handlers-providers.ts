import { http, type HttpHandler } from 'msw';
import type {
  BackupList,
  BackupRepositoryWithRecoveryKey,
  ProviderList,
  RotateProviderSecretRequest,
  User,
} from '../api/generated/model';
import { appendAudit } from './agency';
import { db, now } from './db';
import {
  conflict,
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
 * Settings › Backups and Providers in mock mode: provider entries without
 * their secrets, a provider deletion that the things still holding it refuse,
 * the recovery key replaced and answered once, and the cluster-wide backup
 * list the control-plane backups are read from.
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

  http.post('*/api/v1/backup-repositories/:repositoryId/recovery-key', ({ params }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role !== 'owner') return writeRefusal('backup:manage');
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
