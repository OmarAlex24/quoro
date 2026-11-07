import type {
  AuditExport,
  BackupRepository,
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
