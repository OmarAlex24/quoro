import type { ApiToken } from '../api/generated/model';
import type { LampTone } from './lamp';

/** How long a new personal API token lives. `never` sends no expiry. */
export const TOKEN_EXPIRY_CHOICES = [
  { key: '7d', label: '7 days', days: 7 },
  { key: '30d', label: '30 days', days: 30 },
  { key: '90d', label: '90 days', days: 90 },
  { key: '365d', label: '1 year', days: 365 },
  { key: 'never', label: 'Never', days: null },
] as const;

export type TokenExpiryChoice = (typeof TOKEN_EXPIRY_CHOICES)[number]['key'];

export const DEFAULT_TOKEN_EXPIRY: TokenExpiryChoice = '90d';

export const TOKEN_NAME_MAX = 200;

/** The `expires_at` to send for a choice, or null for a token that never expires. */
export const tokenExpiresAt = (
  choice: TokenExpiryChoice,
  now: number = Date.now(),
): string | null => {
  const days = TOKEN_EXPIRY_CHOICES.find((candidate) => candidate.key === choice)?.days;
  if (days === null || days === undefined) return null;
  return new Date(now + days * 86_400_000).toISOString();
};

/** Why a label cannot be sent, or null when it can. */
export const tokenNameProblem = (name: string): string | null => {
  const trimmed = name.trim();
  if (trimmed.length === 0) return 'Name the token, for example after the machine that uses it.';
  if (trimmed.length > TOKEN_NAME_MAX) return `Keep the name under ${TOKEN_NAME_MAX} characters.`;
  return null;
};

export type TokenState = 'active' | 'expired' | 'revoked';

/** Revoked wins over expired: a revoked token stays revoked whatever its expiry. */
export const tokenState = (
  token: Pick<ApiToken, 'expires_at' | 'revoked_at'>,
  now: number = Date.now(),
): TokenState => {
  if (token.revoked_at) return 'revoked';
  if (token.expires_at) {
    const at = Date.parse(token.expires_at);
    if (!Number.isNaN(at) && at <= now) return 'expired';
  }
  return 'active';
};

export const tokenStateWords: Record<TokenState, string> = {
  active: 'Active',
  expired: 'Expired',
  revoked: 'Revoked',
};

/** The lamp burns while the token still authenticates. */
export const tokenLamp = (state: TokenState): LampTone => (state === 'active' ? 'run' : 'idle');
