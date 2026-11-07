import type {
  CreateGrantRequest,
  Grant,
  GrantKind,
  GrantStatus,
  Scope,
  Selector,
} from '../api/generated/model';
import { describeSelector, expiryWords, formatDuration } from './agency-format';
import { clamp } from './dither';
import { DESTRUCTIVE_SCOPES, HUMAN_ONLY_SCOPES, isGrantableScope } from './scopes';

export const grantStatusWords: Record<GrantStatus, string> = {
  active: 'Active',
  expired: 'Expired',
  revoked: 'Revoked',
  exhausted: 'Exhausted',
};

export const grantKindWords: Record<GrantKind, string> = {
  standard: 'Standard grant',
  incident: 'Incident grant',
  approval: 'Approval grant',
};

/** "4 of 10 uses", or "4 uses, no cap" when `max_uses` is null. */
export const usesWords = (grant: Pick<Grant, 'uses' | 'max_uses'>): string => {
  const uses = grant.uses;
  if (grant.max_uses === null || grant.max_uses === undefined) {
    return `${uses} ${uses === 1 ? 'use' : 'uses'}, no cap`;
  }
  return `${uses} of ${grant.max_uses} uses`;
};

/** "Expires in 45 min" / "Expired 3 h ago", relative to `now`. */
export const expirySentence = (iso: string, now: number = Date.now()): string => {
  const at = Date.parse(iso);
  if (Number.isNaN(at)) return 'Unknown expiry';
  const words = expiryWords(iso, now);
  return at >= now ? `Expires ${words}` : `Expired ${words}`;
};

export const TTL_PRESETS = [
  { key: '15m', label: '15 min', seconds: 900 },
  { key: '1h', label: '1 h', seconds: 3_600 },
  { key: '8h', label: '8 h', seconds: 28_800 },
  { key: '24h', label: '24 h', seconds: 86_400 },
  { key: '7d', label: '7 d', seconds: 604_800 },
] as const;

export type TtlPresetKey = (typeof TTL_PRESETS)[number]['key'];
export type TtlChoice = TtlPresetKey | 'custom';
export type TtlUnit = 'minutes' | 'hours' | 'days';

export const TTL_UNITS: ReadonlyArray<{ value: TtlUnit; label: string; seconds: number }> = [
  { value: 'minutes', label: 'Minutes', seconds: 60 },
  { value: 'hours', label: 'Hours', seconds: 3_600 },
  { value: 'days', label: 'Days', seconds: 86_400 },
];

/** Seconds for a preset, or null for Custom (the number and unit decide). */
export const ttlFromPreset = (choice: TtlChoice): number | null => {
  if (choice === 'custom') return null;
  const preset = TTL_PRESETS.find((candidate) => candidate.key === choice);
  return preset?.seconds ?? null;
};

/** Seconds for a custom number and unit, or null when the number is not a positive integer. */
export const ttlFromCustom = (amount: string, unit: TtlUnit): number | null => {
  const parsed = Number(amount);
  if (!Number.isInteger(parsed) || parsed < 1) return null;
  const unitSeconds = TTL_UNITS.find((candidate) => candidate.value === unit)?.seconds;
  return unitSeconds === undefined ? null : parsed * unitSeconds;
};

/** `{}` matches nothing, so a grant form must never send it. */
export const isSelectorEmpty = (selector: Selector): boolean =>
  !selector.all &&
  !selector.projects?.length &&
  !selector.apps?.length &&
  !selector.nodes?.length;

export interface GrantDraft {
  agentId: string;
  scopes: readonly Scope[];
  selector: Selector;
  /** Null when the lifetime is missing or the custom number is invalid. */
  ttlSeconds: number | null;
  /** The raw form field: empty means no cap. */
  maxUses: string;
  reason: string;
}

export type GrantBuild =
  | { ok: true; body: CreateGrantRequest }
  | { ok: false; message: string };

const ungrantableMessage = (scope: Scope): string => {
  if (DESTRUCTIVE_SCOPES.has(scope)) {
    return `${scope} can't be granted: it is destructive and needs an approval.`;
  }
  if (HUMAN_ONLY_SCOPES.has(scope)) {
    return `${scope} can't be granted: it is human-only.`;
  }
  return `${scope} can't be granted.`;
};

/**
 * Validate the issue-grant form and build the request. The incident travels in
 * the `Quoro-Incident` header, never in the body, so it is not part of this
 * result; use `grantRequestHeaders` for it.
 */
export const buildCreateGrant = (draft: GrantDraft): GrantBuild => {
  if (draft.agentId.trim() === '') {
    return { ok: false, message: 'Choose an agent.' };
  }
  if (draft.scopes.length === 0) {
    return { ok: false, message: 'Choose at least one scope.' };
  }
  const ungrantable = draft.scopes.find((scope) => !isGrantableScope(scope));
  if (ungrantable) {
    return { ok: false, message: ungrantableMessage(ungrantable) };
  }
  if (isSelectorEmpty(draft.selector)) {
    return { ok: false, message: 'Choose at least one resource, or every resource.' };
  }
  if (
    draft.ttlSeconds === null ||
    !Number.isInteger(draft.ttlSeconds) ||
    draft.ttlSeconds < 1
  ) {
    return { ok: false, message: 'Choose how long the grant lasts, at least 1 minute.' };
  }

  const rawMaxUses = draft.maxUses.trim();
  let maxUses: number | undefined;
  if (rawMaxUses !== '') {
    const parsed = Number(rawMaxUses);
    if (!Number.isInteger(parsed) || parsed < 1) {
      return {
        ok: false,
        message: 'Max uses must be a whole number of at least 1, or left empty.',
      };
    }
    maxUses = parsed;
  }

  const reason = draft.reason.trim();
  if (reason === '') {
    return {
      ok: false,
      message: 'Enter a reason. It is recorded with the grant and in the audit log.',
    };
  }

  const body: CreateGrantRequest = {
    agent_id: draft.agentId,
    scopes: [...draft.scopes],
    selector: draft.selector,
    ttl_s: draft.ttlSeconds,
    reason,
  };
  if (maxUses !== undefined) body.max_uses = maxUses;
  return { ok: true, body };
};

/** The headers `createGrant` needs: a fresh idempotency key and the incident, if chosen. */
export const grantRequestHeaders = (
  incidentId?: string | null,
  idempotencyKey: string = crypto.randomUUID(),
): Record<string, string> => {
  const headers: Record<string, string> = { 'Idempotency-Key': idempotencyKey };
  if (incidentId) headers['Quoro-Incident'] = incidentId;
  return headers;
};

/** One sentence the operator can check before issuing the grant. */
export const grantSummary = (input: {
  agentName: string;
  scopes: readonly Scope[];
  selector: Selector;
  ttlSeconds: number | null;
  maxUses: string;
}): string => {
  const agent = input.agentName.trim() === '' ? 'The agent' : input.agentName.trim();
  const scopes = input.scopes.length > 0 ? input.scopes.join(', ') : 'no scopes';
  const where = describeSelector(input.selector).toLowerCase();
  const lifetime =
    input.ttlSeconds !== null && input.ttlSeconds >= 1
      ? `for ${formatDuration(input.ttlSeconds)}`
      : 'for an unchosen lifetime';
  const rawMaxUses = input.maxUses.trim();
  const maxUses = Number(rawMaxUses);
  const cap =
    rawMaxUses !== '' && Number.isInteger(maxUses) && maxUses >= 1
      ? `, up to ${maxUses} ${maxUses === 1 ? 'use' : 'uses'}`
      : '';
  return `${agent} may ${scopes} on ${where} ${lifetime}${cap}.`;
};

/* ---------- the grant meter ---------- */

export interface GrantMeter {
  /** The share of its lifetime the grant has left, 0..1. */
  readonly share: number;
  /** "42 of 60 min left", "20 of 24 h left", "Expired". */
  readonly words: string;
  /** No time left: expired, revoked or used up. */
  readonly over: boolean;
}

const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

/** The unit a lifetime is counted in: minutes up to 2 h, hours up to 2 d, then days. */
const unitFor = (total: number): readonly [number, string] =>
  total <= 2 * HOUR ? [MINUTE, 'min'] : total <= 2 * DAY ? [HOUR, 'h'] : [DAY, 'd'];

/**
 * How much of its lifetime a grant has left, from `created_at` to
 * `expires_at`. A grant that is not active, or whose expiry has passed, has
 * none. Null when the times cannot be read: there is nothing honest to draw.
 */
export const grantMeter = (
  grant: Pick<Grant, 'created_at' | 'expires_at' | 'status'>,
  now: number = Date.now(),
): GrantMeter | null => {
  const from = Date.parse(grant.created_at);
  const until = Date.parse(grant.expires_at);
  if (Number.isNaN(from) || Number.isNaN(until) || until <= from) return null;
  if (grant.status !== 'active' || until <= now) {
    return { share: 0, words: grantStatusWords[grant.status === 'active' ? 'expired' : grant.status], over: true };
  }
  const total = until - from;
  const left = until - now;
  const [unit, label] = unitFor(total);
  const totalCount = Math.round(total / unit);
  if (left < unit) {
    // Less than one whole unit left: count what is left in minutes.
    const minutes = Math.max(1, Math.ceil(left / MINUTE));
    return {
      share: clamp(left / total),
      words: label === 'min' ? `${minutes} of ${totalCount} min left` : `${minutes} min of ${totalCount} ${label} left`,
      over: false,
    };
  }
  const leftCount = Math.min(totalCount, label === 'min' ? Math.ceil(left / unit) : Math.round(left / unit));
  return { share: clamp(left / total), words: `${leftCount} of ${totalCount} ${label} left`, over: false };
};

/**
 * The grant a card's meter shows: an incident grant first (it is what the
 * agent works under right now), then the one that ends soonest.
 */
export const meterGrant = <T extends Pick<Grant, 'kind' | 'status' | 'expires_at'>>(
  grants: readonly T[],
): T | undefined =>
  [...grants]
    .filter((grant) => grant.status === 'active')
    .sort(
      (left, right) =>
        Number(right.kind === 'incident') - Number(left.kind === 'incident') ||
        Date.parse(left.expires_at) - Date.parse(right.expires_at),
    )[0];
