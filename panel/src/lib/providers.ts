import type {
  AuditExport,
  BackupRepository,
  CreateProviderRequest,
  Provider,
  ProviderConfig,
  ProviderKind,
} from '../api/generated/model';
import { relativeTime } from './app-status';

/*
 * Providers as the Settings page says them: what each kind is for, its
 * non-secret config, whether a secret is stored, and what in the cluster still
 * uses it. The secret itself is write-only (D-0050, D-0051): nothing here ever
 * sees one.
 */

/** The kinds in the order the page lists them: names first, then traffic, then storage. */
export const PROVIDER_KINDS: readonly ProviderKind[] = ['dns', 'tunnel', 'lb', 'object_store'];

export const PROVIDER_KIND_WORDS: Record<ProviderKind, string> = {
  dns: 'DNS',
  tunnel: 'Tunnel',
  lb: 'Load balancer',
  object_store: 'Object storage',
};

/** What a provider of each kind is used for, in a sentence. */
export const PROVIDER_KIND_USE: Record<ProviderKind, string> = {
  dns: 'Writes the DNS-01 records that prove a domain for its ACME certificate.',
  tunnel: "Runs nodes' tunnels and writes the DNS records of tunnel domains.",
  lb: 'Holds the credentials of a load balancer.',
  object_store: 'A bucket for the backup repository or the off-site copy of the audit log.',
};

/** Providers grouped by kind, in `PROVIDER_KINDS` order, then by name. */
export const sortProviders = <P extends Pick<Provider, 'kind' | 'name'>>(providers: readonly P[]): P[] =>
  [...providers].sort(
    (left, right) =>
      PROVIDER_KINDS.indexOf(left.kind) - PROVIDER_KINDS.indexOf(right.kind) ||
      left.name.localeCompare(right.name),
  );

export interface ConfigEntry {
  readonly key: string;
  readonly text: string;
}

const configText = (value: unknown): string => {
  if (value === null || value === undefined) return 'null';
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  return JSON.stringify(value);
};

/** The provider's non-secret config as key and text, sorted by key. */
export const configEntries = (config: ProviderConfig): ConfigEntry[] =>
  Object.keys(config)
    .sort()
    .map((key) => ({ key, text: configText(config[key]) }));

/** The first `max` config entries for a row, and how many more there are. */
export const configSummary = (
  config: ProviderConfig,
  max = 3,
): { shown: ConfigEntry[]; more: number } => {
  const entries = configEntries(config);
  return { shown: entries.slice(0, max), more: Math.max(0, entries.length - max) };
};

/** Whether a secret is stored, and since when it has not changed. */
export const secretLine = (
  provider: Pick<Provider, 'has_secret' | 'created_at' | 'rotated_at'>,
  now: number = Date.now(),
): string => {
  if (!provider.has_secret) return 'No secret stored';
  if (provider.rotated_at) return `Secret replaced ${relativeTime(provider.rotated_at, now)}`;
  return `Secret stored ${relativeTime(provider.created_at, now)}`;
};

/**
 * What in the cluster still holds this provider. A provider that holds
 * something cannot be deleted: the server refuses with `conflict` (D-0095).
 * Domains and node tunnels also hold providers; their lists are not read here,
 * so the server's refusal stays the final word.
 */
export const providerUses = (
  providerId: string,
  repositories: ReadonlyArray<Pick<BackupRepository, 'provider_id' | 'name'>>,
  auditExports: ReadonlyArray<Pick<AuditExport, 'provider_id'>>,
): string[] => [
  ...repositories
    .filter((repository) => repository.provider_id === providerId)
    .map((repository) => `Holds the backup repository ${repository.name}`),
  ...(auditExports.some((copy) => copy.provider_id === providerId)
    ? ['Holds the off-site copy of the audit log']
    : []),
];

/** Why a new secret cannot be sent, or null when it can. */
export const secretProblem = (secret: string): string | null =>
  secret.length === 0 ? 'Paste the new secret.' : null;

/*
 * Adding a provider (`createProvider`). The panel offers the three kinds
 * something in the cluster reads: `dns` for ACME DNS-01 records, `tunnel` for
 * nodes' tunnels and tunnel domains, `object_store` for the backup repository
 * and the off-site audit copy. Nothing reads an `lb` provider's config yet, so
 * there is no form for one; the API still takes it.
 */

export type CreatableKind = Exclude<ProviderKind, 'lb'>;

export const CREATABLE_KINDS: readonly CreatableKind[] = ['dns', 'tunnel', 'object_store'];

/** One non-secret setting of a kind, and the key it has in `config`. */
export interface ConfigField {
  readonly key: string;
  readonly label: string;
  readonly hint: string;
  /** What the error says when the field is left empty; absent for an optional field. */
  readonly missing?: string;
  readonly placeholder?: string;
  /** The value is a URL and must be `https://`. */
  readonly https?: boolean;
}

/**
 * The settings each kind needs, as the server reads them: a DNS provider's
 * `zone_id`; a tunnel provider's `account_id` and `zone_id`; object storage's
 * `endpoint` (`https://`), `bucket`, `access_key_id` and optional `region`
 * (`openapi.yaml` `CreateProviderRequest`, `backup-format.md` section 10).
 */
export const PROVIDER_CONFIG_FIELDS: Record<CreatableKind, readonly ConfigField[]> = {
  dns: [
    {
      key: 'zone_id',
      label: 'Zone ID',
      hint: "The Cloudflare zone of your domains. Quoro writes each domain's DNS-01 record there.",
      missing: 'Enter the zone ID.',
    },
  ],
  tunnel: [
    {
      key: 'account_id',
      label: 'Account ID',
      hint: 'The Cloudflare account that holds the tunnels.',
      missing: 'Enter the account ID.',
    },
    {
      key: 'zone_id',
      label: 'Zone ID',
      hint: 'The zone where Quoro writes the DNS records of tunnel domains.',
      missing: 'Enter the zone ID.',
    },
  ],
  object_store: [
    {
      key: 'endpoint',
      label: 'Endpoint',
      hint: "The bucket's S3 API address.",
      missing: 'Enter the endpoint.',
      placeholder: 'https://<account>.r2.cloudflarestorage.com',
      https: true,
    },
    {
      key: 'bucket',
      label: 'Bucket',
      hint: 'An existing bucket. Quoro does not create it.',
      missing: 'Enter the bucket.',
    },
    {
      key: 'access_key_id',
      label: 'Access key ID',
      hint: 'The public half of the key; its secret goes below.',
      missing: 'Enter the access key ID.',
    },
    {
      key: 'region',
      label: 'Region',
      hint: 'Optional. Leave it empty for auto.',
      placeholder: 'auto',
    },
  ],
};

/** What the secret of each kind is, and what it should be allowed to do. */
export const PROVIDER_SECRET_WORDS: Record<
  CreatableKind,
  { label: string; hint: string; missing: string }
> = {
  dns: {
    label: 'API token',
    hint: 'Scoped to this zone, with permission to edit its DNS records.',
    missing: 'Paste the API token.',
  },
  tunnel: {
    label: 'API token',
    hint: "With permission to manage the account's tunnels and edit the zone's DNS records.",
    missing: 'Paste the API token.',
  },
  object_store: {
    label: 'Secret access key',
    hint: 'The secret that goes with the access key ID.',
    missing: 'Paste the secret access key.',
  },
};

/** A new provider as typed. `config` keeps every kind's fields so switching kind loses nothing. */
export interface ProviderForm {
  kind: CreatableKind;
  name: string;
  config: Record<string, string>;
  secret: string;
}

export const newProviderForm = (kind: CreatableKind = 'dns'): ProviderForm => ({
  kind,
  name: '',
  config: {},
  secret: '',
});

/** Errors by field: `name`, `secret`, and `config.<key>` for a setting. */
export type ProviderErrors = Record<string, string>;

const MAX_SECRET_BYTES = 16_384;

const isHttpsUrl = (value: string): boolean => {
  try {
    const url = new URL(value);
    return url.protocol === 'https:' && url.host !== '';
  } catch {
    return false;
  }
};

/** Why each field of the form cannot be sent; empty when it can. */
export const providerFormProblems = (form: ProviderForm): ProviderErrors => {
  const errors: ProviderErrors = {};
  const name = [...form.name.trim()].length;
  if (name === 0) errors.name = 'Name the provider.';
  else if (name > 200) errors.name = 'Use at most 200 characters.';

  for (const field of PROVIDER_CONFIG_FIELDS[form.kind]) {
    const value = (form.config[field.key] ?? '').trim();
    if (value === '') {
      if (field.missing) errors[`config.${field.key}`] = field.missing;
    } else if (field.https && !isHttpsUrl(value)) {
      errors[`config.${field.key}`] = 'Use an https:// address.';
    }
  }

  // The secret is sent exactly as pasted: never trimmed, never shown again.
  if (form.secret.length === 0) {
    errors.secret = PROVIDER_SECRET_WORDS[form.kind].missing;
  } else if (new TextEncoder().encode(form.secret).length > MAX_SECRET_BYTES) {
    errors.secret = 'The secret is longer than 16 KiB.';
  }
  return errors;
};

/**
 * The body `createProvider` takes: the trimmed name, only the chosen kind's
 * settings with empty optional ones left out (an absent `region` means
 * `auto`), and the secret as pasted.
 */
export const buildCreateProviderBody = (form: ProviderForm): CreateProviderRequest => {
  const config: Record<string, string> = {};
  for (const field of PROVIDER_CONFIG_FIELDS[form.kind]) {
    const value = (form.config[field.key] ?? '').trim();
    if (value !== '') config[field.key] = value;
  }
  return { kind: form.kind, name: form.name.trim(), config, secret: form.secret };
};
