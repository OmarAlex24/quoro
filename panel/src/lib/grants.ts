import type {
  CreateGrantRequest,
  Grant,
  GrantKind,
  GrantStatus,
  Scope,
  Selector,
} from '../api/generated/model';
import { describeSelector, expiryWords, formatDuration } from './agency-format';
import type { LampTone } from './lamp';
import { DESTRUCTIVE_SCOPES, HUMAN_ONLY_SCOPES, isGrantableScope } from './scopes';

/** Grant lifecycle in the lamp language: active burns, terminal states settle. */
export const grantLamp = (status: GrantStatus): LampTone => {
  switch (status) {
    case 'active':
      return 'run';
    case 'exhausted':
      return 'idle';
    case 'expired':
      return 'idle';
    case 'revoked':
      return 'fault';
  }
};

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
