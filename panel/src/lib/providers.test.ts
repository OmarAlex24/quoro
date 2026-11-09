import { describe, expect, it } from 'vitest';
import type { Provider } from '../api/generated/model';
import {
  buildCreateProviderBody,
  configEntries,
  configSummary,
  CREATABLE_KINDS,
  newProviderForm,
  PROVIDER_CONFIG_FIELDS,
  PROVIDER_SECRET_WORDS,
  providerFormProblems,
  type CreatableKind,
  type ProviderForm,
  PROVIDER_KIND_USE,
  PROVIDER_KIND_WORDS,
  PROVIDER_KINDS,
  providerUses,
  secretLine,
  secretProblem,
  sortProviders,
} from './providers';

const NOW = Date.parse('2026-11-07T12:00:00Z');

const provider = (overrides: Partial<Provider>): Provider => ({
  id: 'prv_01JC9Z6Q5W0000000000000000',
  kind: 'dns',
  name: 'zone',
  config: {},
  has_secret: true,
  created_by: 'usr_01JC9Z6Q5W0000000000000000',
  created_at: '2026-11-01T12:00:00Z',
  rotated_at: null,
  ...overrides,
});

describe('providers', () => {
  it('names and explains every kind the contract has', () => {
    expect(PROVIDER_KINDS).toEqual(['dns', 'tunnel', 'lb', 'object_store']);
    for (const kind of PROVIDER_KINDS) {
      expect(PROVIDER_KIND_WORDS[kind]).toMatch(/^[A-Z]/);
      expect(PROVIDER_KIND_USE[kind]).toMatch(/\.$/);
    }
  });

  it('groups by kind, then sorts by name', () => {
    const sorted = sortProviders([
      provider({ kind: 'object_store', name: 'b-bucket' }),
      provider({ kind: 'tunnel', name: 'tunnel' }),
      provider({ kind: 'object_store', name: 'a-bucket' }),
      provider({ kind: 'dns', name: 'zone' }),
    ]);
    expect(sorted.map((item) => `${item.kind}:${item.name}`)).toEqual([
      'dns:zone',
      'tunnel:tunnel',
      'object_store:a-bucket',
      'object_store:b-bucket',
    ]);
  });

  it('writes the config as sorted text, nested values as JSON', () => {
    expect(
      configEntries({ region: 'auto', bucket: 'lab', port: 443, tls: true, tags: ['a'], none: null }),
    ).toEqual([
      { key: 'bucket', text: 'lab' },
      { key: 'none', text: 'null' },
      { key: 'port', text: '443' },
      { key: 'region', text: 'auto' },
      { key: 'tags', text: '["a"]' },
      { key: 'tls', text: 'true' },
    ]);
    expect(configSummary({ a: 1, b: 2, c: 3, d: 4, e: 5 })).toEqual({
      shown: [
        { key: 'a', text: '1' },
        { key: 'b', text: '2' },
        { key: 'c', text: '3' },
      ],
      more: 2,
    });
    expect(configSummary({})).toEqual({ shown: [], more: 0 });
  });

  it('says whether a secret is stored and when it last changed, never what it is', () => {
    expect(secretLine(provider({ has_secret: false }), NOW)).toBe('No secret stored');
    expect(secretLine(provider({}), NOW)).toBe('Secret stored 6 d ago');
    expect(secretLine(provider({ rotated_at: '2026-11-07T10:00:00Z' }), NOW)).toBe(
      'Secret replaced 2 h ago',
    );
  });

  it('lists what still holds a provider', () => {
    const id = 'prv_01JC9Z6Q5W0000000000000001';
    expect(
      providerUses(
        id,
        [
          { provider_id: id, name: 'lab-backups' },
          { provider_id: 'prv_other', name: 'other' },
        ],
        [{ provider_id: id }],
      ),
    ).toEqual(['Holds the backup repository lab-backups', 'Holds the off-site copy of the audit log']);
    expect(providerUses(id, [], [])).toEqual([]);
  });

  it('asks for a secret before it sends one', () => {
    expect(secretProblem('')).toBe('Paste the new secret.');
    expect(secretProblem(' token ')).toBeNull();
  });
});

describe('adding a provider', () => {
  it('offers every kind something in the cluster reads, with the settings the server reads', () => {
    expect(CREATABLE_KINDS).toEqual(['dns', 'tunnel', 'object_store']);
    const keys = (kind: CreatableKind) => PROVIDER_CONFIG_FIELDS[kind].map((field) => field.key);
    expect(keys('dns')).toEqual(['zone_id']);
    expect(keys('tunnel')).toEqual(['account_id', 'zone_id']);
    expect(keys('object_store')).toEqual(['endpoint', 'bucket', 'access_key_id', 'region']);
    expect(PROVIDER_CONFIG_FIELDS.object_store.find((field) => field.key === 'region')?.missing).toBeUndefined();
    expect(PROVIDER_SECRET_WORDS.object_store.label).toBe('Secret access key');
  });

  it('names every empty field of the chosen kind', () => {
    expect(providerFormProblems(newProviderForm('tunnel'))).toEqual({
      name: 'Name the provider.',
      'config.account_id': 'Enter the account ID.',
      'config.zone_id': 'Enter the zone ID.',
      secret: 'Paste the API token.',
    });
    expect(providerFormProblems(newProviderForm('object_store'))).toEqual({
      name: 'Name the provider.',
      'config.endpoint': 'Enter the endpoint.',
      'config.bucket': 'Enter the bucket.',
      'config.access_key_id': 'Enter the access key ID.',
      secret: 'Paste the secret access key.',
    });
  });

  it('wants an https endpoint, a short name and a secret under 16 KiB', () => {
    const form: ProviderForm = {
      kind: 'object_store',
      name: 'x'.repeat(201),
      config: { endpoint: 'http://minio.local:9000', bucket: 'b', access_key_id: 'k' },
      secret: 's'.repeat(16_385),
    };
    expect(providerFormProblems(form)).toEqual({
      name: 'Use at most 200 characters.',
      'config.endpoint': 'Use an https:// address.',
      secret: 'The secret is longer than 16 KiB.',
    });
    expect(
      providerFormProblems({ ...form, name: 'r2', config: { ...form.config, endpoint: 'https:' }, secret: 's' }),
    ).toEqual({ 'config.endpoint': 'Use an https:// address.' });
  });

  it("sends only the chosen kind's settings, trimmed, with the secret exactly as pasted", () => {
    const form: ProviderForm = {
      kind: 'object_store',
      name: '  r2-backups ',
      config: {
        zone_id: 'left over from DNS',
        endpoint: ' https://acct.r2.cloudflarestorage.com ',
        bucket: 'lab',
        access_key_id: 'key-id',
        region: '  ',
      },
      secret: ' secret with spaces ',
    };
    expect(providerFormProblems(form)).toEqual({});
    expect(buildCreateProviderBody(form)).toEqual({
      kind: 'object_store',
      name: 'r2-backups',
      config: { endpoint: 'https://acct.r2.cloudflarestorage.com', bucket: 'lab', access_key_id: 'key-id' },
      secret: ' secret with spaces ',
    });
    expect(buildCreateProviderBody({ ...form, kind: 'dns', config: { zone_id: 'z1' } }).config).toEqual({
      zone_id: 'z1',
    });
    expect(
      buildCreateProviderBody({ ...form, config: { ...form.config, region: 'eu-west-1' } }).config.region,
    ).toBe('eu-west-1');
  });
});
