import { setupServer } from 'msw/node';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type {
  BackupRepository,
  BackupRepositoryWithRecoveryKey,
  Problem,
  Provider,
  ProviderList,
  User,
} from '../api/generated/model';
import { db, mockId, now, repositoryObjectKey } from './db';
import { providersHandlers } from './handlers-providers';

const server = setupServer(...providersHandlers);

const BASE = 'http://panel.test/api/v1';

const user = (role: User['role']): User => ({
  id: mockId('usr'),
  email: `${role}@example.com`,
  name: role,
  role,
  disabled: false,
  created_at: now(),
});

/** The seeded state the tests change, put back before each test. */
const seeded = structuredClone({
  providers: db.providers,
  repositories: db.backupRepositories,
  objects: db.repositoryObjects,
});

const OWNER = user('owner');

const signIn = (who: User): void => {
  db.owner = who;
  db.sessionUserId = who.id;
};

beforeAll(() => server.listen());
afterAll(() => server.close());

beforeEach(() => {
  signIn(OWNER);
  const fresh = structuredClone(seeded);
  db.providers = fresh.providers;
  db.backupRepositories = fresh.repositories;
  db.repositoryObjects = fresh.objects;
});

const send = (method: string, path: string, body: unknown): Promise<Response> =>
  fetch(`${BASE}${path}`, {
    method,
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': crypto.randomUUID() },
    body: JSON.stringify(body),
  });

const refusal = async (response: Response): Promise<[number, Problem['code'], string | undefined]> => {
  const problem = (await response.json()) as Problem;
  return [response.status, problem.code, problem.detail ?? undefined];
};

const objectStore = (): Provider => {
  const provider = db.providers.find(
    (item) =>
      item.kind === 'object_store' && !db.auditExports.some((copy) => copy.provider_id === item.id),
  );
  if (!provider) throw new Error('the seed has no backup provider');
  return provider;
};

const auditProvider = (): Provider => {
  const provider = db.providers.find((item) => item.id === db.auditExports[0]?.provider_id);
  if (!provider) throw new Error('the seed has no audit provider');
  return provider;
};

describe('createProvider mock', () => {
  const dns = { kind: 'dns', name: 'zone-two', config: { zone_id: 'z2' }, secret: 'token-value' };

  it('stores the entry, answers 201 without the secret, and lists it', async () => {
    const response = await send('POST', '/providers', dns);
    expect(response.status).toBe(201);
    const created = (await response.json()) as Provider;
    expect(created).toMatchObject({ kind: 'dns', name: 'zone-two', config: { zone_id: 'z2' }, has_secret: true });
    expect(created.id).toMatch(/^prv_[0-9A-HJKMNP-TV-Z]{26}$/);
    expect(created.created_by).toBe(OWNER.id);
    expect(created).not.toHaveProperty('secret');

    const list = (await (await fetch(`${BASE}/providers?limit=200`)).json()) as ProviderList;
    expect(list.items.map((item) => item.id)).toContain(created.id);
    expect(JSON.stringify(list)).not.toContain('token-value');
  });

  it('refuses a taken name with conflict', async () => {
    const taken = db.providers[0]?.name ?? '';
    expect(await refusal(await send('POST', '/providers', { ...dns, name: taken }))).toEqual([
      409,
      'conflict',
      `name ${taken} is taken`,
    ]);
  });

  it('refuses a bad kind, name, config or secret with validation_failed', async () => {
    const cases: Array<[unknown, string]> = [
      [{ ...dns, kind: 'smtp' }, 'kind must be one of dns, tunnel, lb, object_store'],
      [{ ...dns, name: '' }, 'name must be between 1 and 200 characters'],
      [{ ...dns, name: 'x'.repeat(201) }, 'name must be between 1 and 200 characters'],
      [{ ...dns, config: ['zone'] }, 'config must be a JSON object'],
      [{ ...dns, config: null }, 'config must be a JSON object'],
      [{ ...dns, secret: '' }, 'secret must be between 1 and 16384 bytes'],
      [{ ...dns, secret: 's'.repeat(16_385) }, 'secret must be between 1 and 16384 bytes'],
    ];
    for (const [body, detail] of cases) {
      expect(await refusal(await send('POST', '/providers', body))).toEqual([422, 'validation_failed', detail]);
    }
  });

  it('refuses a viewer', async () => {
    signIn(user('viewer'));
    const [status, code] = await refusal(await send('POST', '/providers', dns));
    expect([status, code]).toEqual([403, 'forbidden']);
  });
});

describe('createBackupRepository mock', () => {
  const body = () => ({
    name: 'offsite',
    provider_id: objectStore().id,
    prefix: 'cluster-two',
    control_plane_schedule: { interval_h: 12, keep: 10 },
  });

  beforeEach(() => {
    db.backupRepositories = [];
  });

  it('answers the recovery key once and keeps the repository without it', async () => {
    const response = await send('POST', '/backup-repositories', body());
    expect(response.status).toBe(201);
    const created = (await response.json()) as BackupRepositoryWithRecoveryKey;
    expect(created.recovery_key).toMatch(/^qrk1(-[0-9A-HJKMNP-TV-Z]{4}){13}$/);
    expect(created.recovery_key_fingerprint).toMatch(/^[0-9a-f]{16}$/);
    expect(created).toMatchObject({
      name: 'offsite',
      prefix: 'cluster-two',
      control_plane_schedule: { interval_h: 12, keep: 10 },
      recovery_key_rotated_at: null,
      created_by: OWNER.id,
    });
    expect(db.backupRepositories).toHaveLength(1);
    expect(db.backupRepositories[0]).not.toHaveProperty('recovery_key');
    expect(JSON.stringify(db.backupRepositories)).not.toContain(created.recovery_key);
  });

  it('takes the default prefix and schedule when they are absent', async () => {
    const { prefix: _prefix, control_plane_schedule: _schedule, ...bare } = body();
    db.repositoryObjects = [];
    const created = (await (await send('POST', '/backup-repositories', bare)).json()) as BackupRepository;
    expect(created.prefix).toBe('quoro-backups');
    expect(created.control_plane_schedule).toEqual({ interval_h: 6, keep: 7 });
  });

  it('refuses a second repository and a prefix that already holds a config object', async () => {
    expect((await send('POST', '/backup-repositories', body())).status).toBe(201);
    expect(await refusal(await send('POST', '/backup-repositories', body()))).toEqual([
      409,
      'conflict',
      'the cluster already has a backup repository',
    ]);

    db.backupRepositories = [];
    db.repositoryObjects = [repositoryObjectKey(objectStore().config, 'kept')];
    expect(await refusal(await send('POST', '/backup-repositories', { ...body(), prefix: 'kept' }))).toEqual([
      409,
      'conflict',
      'the prefix already holds a config object',
    ]);
  });

  it('refuses a provider that cannot hold it, with the server words', async () => {
    const dns = db.providers.find((item) => item.kind === 'dns');
    const cases: Array<[string, string]> = [
      [mockId('prv'), 'the provider does not exist'],
      [dns?.id ?? '', 'the provider is not an object_store provider'],
      [auditProvider().id, 'the provider holds the audit export'],
    ];
    for (const [providerId, detail] of cases) {
      expect(
        await refusal(await send('POST', '/backup-repositories', { ...body(), provider_id: providerId })),
      ).toEqual([422, 'validation_failed', detail]);
    }

    const keyless = objectStore();
    delete keyless.config.access_key_id;
    expect(await refusal(await send('POST', '/backup-repositories', body()))).toEqual([
      422,
      'validation_failed',
      'the provider config has no access_key_id',
    ]);
  });

  it('refuses a bad name, prefix or schedule', async () => {
    const cases: Array<[Record<string, unknown>, string]> = [
      [{ name: '' }, 'name must be between 1 and 200 characters'],
      [{ prefix: 'a//b' }, 'prefix may only hold letters, digits, `.`, `_`, `-` and `/`'],
      [{ prefix: '' }, 'prefix must be between 1 and 200 characters'],
      [{ control_plane_schedule: { interval_h: 0, keep: 7 } }, 'control_plane_schedule.interval_h is outside 1-168'],
      [{ control_plane_schedule: { interval_h: 6, keep: 101 } }, 'control_plane_schedule.keep is outside 1-100'],
    ];
    for (const [patch, detail] of cases) {
      expect(await refusal(await send('POST', '/backup-repositories', { ...body(), ...patch }))).toEqual([
        422,
        'validation_failed',
        detail,
      ]);
    }
    expect(db.backupRepositories).toHaveLength(0);
  });

  it('is owner-only', async () => {
    signIn(user('operator'));
    const [status, code] = await refusal(await send('POST', '/backup-repositories', body()));
    expect([status, code]).toEqual([403, 'forbidden']);
  });
});

describe('updateBackupRepository mock', () => {
  const repository = (): BackupRepository => {
    const found = db.backupRepositories[0];
    if (!found) throw new Error('the seed has no repository');
    return found;
  };

  it('changes only the fields sent', async () => {
    const before = { ...repository() };
    const response = await send('PATCH', `/backup-repositories/${before.id}`, {
      control_plane_schedule: { interval_h: 24, keep: 30 },
    });
    expect(response.status).toBe(200);
    const updated = (await response.json()) as BackupRepository;
    expect(updated).toEqual({ ...before, control_plane_schedule: { interval_h: 24, keep: 30 } });

    const renamed = (await (
      await send('PATCH', `/backup-repositories/${before.id}`, { name: 'offsite' })
    ).json()) as BackupRepository;
    expect(renamed.name).toBe('offsite');
    expect(renamed.control_plane_schedule).toEqual({ interval_h: 24, keep: 30 });
  });

  it('refuses a bad name or schedule and changes nothing', async () => {
    const before = { ...repository() };
    expect(
      await refusal(await send('PATCH', `/backup-repositories/${before.id}`, { name: '', control_plane_schedule: { interval_h: 2, keep: 2 } })),
    ).toEqual([422, 'validation_failed', 'name must be between 1 and 200 characters']);
    expect(
      await refusal(await send('PATCH', `/backup-repositories/${before.id}`, { control_plane_schedule: { interval_h: 169, keep: 7 } })),
    ).toEqual([422, 'validation_failed', 'control_plane_schedule.interval_h is outside 1-168']);
    expect(repository()).toEqual(before);
  });

  it('answers not_found for an unknown repository, and is owner-only', async () => {
    expect((await send('PATCH', `/backup-repositories/${mockId('brp')}`, { name: 'x' })).status).toBe(404);
    signIn(user('operator'));
    expect((await send('PATCH', `/backup-repositories/${repository().id}`, { name: 'x' })).status).toBe(403);
  });
});
