import { describe, expect, it } from 'vitest';
import type { BackupRepository, Provider } from '../api/generated/model';
import {
  bucketWords,
  CONTROL_PLANE_SCHEDULE,
  DEFAULT_PREFIX,
  newRepositoryForm,
  prefixProblem,
  readNewRepository,
  readRepositorySettings,
  readSchedule,
  repositoryNameProblem,
  repositoryProviderChoices,
  repositorySettingsForm,
  scheduleForm,
  settingsChange,
} from './backup-forms';

const provider = (overrides: Partial<Provider>): Provider => ({
  id: 'prv_01JC9Z6Q5W0000000000000000',
  kind: 'object_store',
  name: 'bucket',
  config: { endpoint: 'https://acct.r2.cloudflarestorage.com', bucket: 'lab' },
  has_secret: true,
  created_by: 'usr_01JC9Z6Q5W0000000000000000',
  created_at: '2026-11-01T12:00:00Z',
  rotated_at: null,
  ...overrides,
});

const BACKUPS = provider({ id: 'prv_01JC9Z6Q5W0000000000000001', name: 'r2-backups' });
const AUDIT = provider({ id: 'prv_01JC9Z6Q5W0000000000000002', name: 'r2-audit' });
const DNS = provider({ id: 'prv_01JC9Z6Q5W0000000000000003', kind: 'dns', name: 'a-dns' });

const repository: Pick<BackupRepository, 'name' | 'control_plane_schedule'> = {
  name: 'lab-backups',
  control_plane_schedule: { interval_h: 6, keep: 7 },
};

describe('readSchedule', () => {
  it('reads two whole numbers in range', () => {
    expect(readSchedule({ interval: '12', keep: '30' })).toEqual({
      schedule: { interval_h: 12, keep: 30 },
      errors: {},
    });
    expect(readSchedule({ interval: '1', keep: '1' }).schedule).toEqual({ interval_h: 1, keep: 1 });
    expect(readSchedule({ interval: '168', keep: '100' }).schedule).toEqual({ interval_h: 168, keep: 100 });
  });

  it('says what is wrong with each number, with the app schedule words', () => {
    for (const interval of ['', '0', '169', '1.5', 'six']) {
      const read = readSchedule({ interval, keep: '7' });
      expect(read.schedule).toBeNull();
      expect(read.errors).toEqual({ interval: 'Use a whole number of hours, 1 to 168.' });
    }
    for (const keep of ['', '0', '101', '2.5']) {
      expect(readSchedule({ interval: '6', keep }).errors).toEqual({
        keep: 'Keep a whole number of backups, 1 to 100.',
      });
    }
  });

  it('round-trips a schedule through the form', () => {
    expect(scheduleForm(CONTROL_PLANE_SCHEDULE)).toEqual({ interval: '6', keep: '7' });
    expect(readSchedule(scheduleForm({ interval_h: 24, keep: 14 })).schedule).toEqual({
      interval_h: 24,
      keep: 14,
    });
  });
});

describe('repository fields', () => {
  it('needs a name of 1 to 200 characters once trimmed', () => {
    expect(repositoryNameProblem('   ')).toBe('Name the repository.');
    expect(repositoryNameProblem('x'.repeat(201))).toBe('Use at most 200 characters.');
    expect(repositoryNameProblem(' lab ')).toBeNull();
  });

  it('takes the prefixes the contract pattern takes, and no other', () => {
    for (const ok of [DEFAULT_PREFIX, 'a', 'a/b/c', 'cluster.v2/backups_1-x', ' padded ']) {
      expect(prefixProblem(ok)).toBeNull();
    }
    expect(prefixProblem('')).toBe('Give a prefix, such as quoro-backups.');
    for (const bad of ['/lead', 'trail/', 'a//b', 'with space', 'é', 'a/*']) {
      expect(prefixProblem(bad)).toMatch(/^Use letters, digits/);
    }
    expect(prefixProblem('a'.repeat(201))).toBe('Use at most 200 characters.');
  });

  it('offers object storage only, by name, and refuses the audit copy provider', () => {
    const choices = repositoryProviderChoices([DNS, BACKUPS, AUDIT], [{ provider_id: AUDIT.id }]);
    expect(choices.map((choice) => [choice.provider.name, choice.refusal])).toEqual([
      ['r2-audit', 'Holds the off-site copy of the audit log'],
      ['r2-backups', null],
    ]);
  });

  it('says where the bucket is', () => {
    expect(bucketWords(BACKUPS.config)).toBe('Bucket lab at acct.r2.cloudflarestorage.com');
    expect(bucketWords({ bucket: 'lab', endpoint: 'not a url' })).toBe('Bucket lab');
    expect(bucketWords({})).toBeNull();
  });
});

describe('a new repository', () => {
  const choices = repositoryProviderChoices([AUDIT, BACKUPS], [{ provider_id: AUDIT.id }]);

  it('starts on the first usable provider, the default prefix and the control-plane default', () => {
    expect(newRepositoryForm(choices)).toEqual({
      name: '',
      providerId: BACKUPS.id,
      prefix: 'quoro-backups',
      schedule: { interval: '6', keep: '7' },
    });
    expect(newRepositoryForm([]).providerId).toBe('');
  });

  it('builds the createBackupRepository body, trimmed', () => {
    const form = { ...newRepositoryForm(choices), name: '  lab-backups ', prefix: ' cluster/b ' };
    expect(readNewRepository(form, choices)).toEqual({
      body: {
        name: 'lab-backups',
        provider_id: BACKUPS.id,
        prefix: 'cluster/b',
        control_plane_schedule: { interval_h: 6, keep: 7 },
      },
      errors: {},
    });
  });

  it('names every field that cannot be sent', () => {
    const read = readNewRepository(
      { name: '', providerId: AUDIT.id, prefix: 'a//b', schedule: { interval: '0', keep: '200' } },
      choices,
    );
    expect(read.body).toBeNull();
    expect(Object.keys(read.errors).sort()).toEqual(['interval', 'keep', 'name', 'prefix', 'providerId']);
    expect(read.errors.providerId).toBe(
      'Holds the off-site copy of the audit log; choose another provider.',
    );
    expect(readNewRepository({ ...newRepositoryForm([]), name: 'x' }, []).errors).toEqual({
      providerId: 'Choose an object storage provider.',
    });
  });
});

describe("a repository's settings", () => {
  it('sends only what changed', () => {
    const form = repositorySettingsForm(repository);
    expect(form).toEqual({ name: 'lab-backups', schedule: { interval: '6', keep: '7' } });

    const same = readRepositorySettings(repository, { ...form, name: ' lab-backups ' });
    expect(same.body).toEqual({});
    expect(settingsChange(same.body ?? {})).toBe(false);

    expect(readRepositorySettings(repository, { ...form, name: 'offsite' }).body).toEqual({
      name: 'offsite',
    });
    const schedule = readRepositorySettings(repository, { ...form, schedule: { interval: '12', keep: '7' } });
    expect(schedule.body).toEqual({ control_plane_schedule: { interval_h: 12, keep: 7 } });
    expect(settingsChange(schedule.body ?? {})).toBe(true);
  });

  it('refuses a bad name or schedule before sending anything', () => {
    const read = readRepositorySettings(repository, { name: ' ', schedule: { interval: '200', keep: '7' } });
    expect(read.body).toBeNull();
    expect(read.errors).toEqual({
      name: 'Name the repository.',
      interval: 'Use a whole number of hours, 1 to 168.',
    });
  });
});
