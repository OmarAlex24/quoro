import type {
  Approval,
  ApprovalParams,
  ApprovalStatus,
  DecisionRequest,
  Scope,
} from '../api/generated/model';
import { DESTRUCTIVE_SCOPES } from './scopes';
import type { LampTone } from './lamp';

/**
 * The approvals queue, as pure rules: which lamp a request's state takes, what
 * its params look like when flattened for display, and exactly what a decision
 * sends. Keeping these here lets the screen and the plate stay thin and lets
 * the rules be tested without a DOM.
 */

/** Pending is waiting (amber), approved run, denied fault, expired idle. */
export const approvalLamp = (status: ApprovalStatus): LampTone => {
  switch (status) {
    case 'pending':
      return 'shift';
    case 'approved':
      return 'run';
    case 'denied':
      return 'fault';
    case 'expired':
      return 'idle';
  }
};

/** The status in one word, title case: "Waiting", "Approved", "Denied", "Expired". */
export const approvalWords: Record<ApprovalStatus, string> = {
  pending: 'Waiting',
  approved: 'Approved',
  denied: 'Denied',
  expired: 'Expired',
};

/** A destructive scope is one the panel marks fault-red on a request. */
export const isDestructiveScope = (scope: Scope): boolean => DESTRUCTIVE_SCOPES.has(scope);

/** One parameter as the plate shows it: a key, its text and whether it is nested. */
export interface ParamRow {
  readonly key: string;
  readonly text: string;
  /** Objects and arrays print as indented JSON; scalars print as one value. */
  readonly nested: boolean;
}

const isNested = (value: unknown): boolean =>
  value !== null && typeof value === 'object';

/**
 * Flatten the approved call's parameters for display, sorted by key. Nested
 * values become indented JSON text; the plate renders it in `type-data`, never
 * in a monospace face.
 */
export const paramRows = (params: ApprovalParams): ParamRow[] =>
  Object.keys(params)
    .sort()
    .map((key) => {
      const value = params[key];
      if (isNested(value)) {
        return { key, text: JSON.stringify(value, null, 2), nested: true };
      }
      if (value === null) return { key, text: 'null', nested: false };
      if (typeof value === 'string') return { key, text: value, nested: false };
      return { key, text: String(value), nested: false };
    });

/**
 * What the confirm step says, said exactly: approve issues one grant for this
 * scope on this target only; deny issues nothing.
 */
export type Decision = 'approve' | 'deny';

export const approvalConfirm = (
  decision: Decision,
  approval: Pick<Approval, 'scope' | 'target'>,
): string =>
  decision === 'approve'
    ? `Issues a one-time grant for ${approval.scope} on this ${approval.target.type} only, valid for 10 minutes. The agent must repeat the exact call.`
    : "No grant is issued; the agent's retry is refused.";

/** The note is optional: blank sends `{}`, anything else sends `{ note }`. */
export const decisionBody = (note: string | null | undefined): DecisionRequest => {
  const trimmed = (note ?? '').trim();
  return trimmed === '' ? {} : { note: trimmed };
};

const timeOf = (iso: string | null | undefined): number => {
  const parsed = Date.parse(iso ?? '');
  return Number.isNaN(parsed) ? 0 : parsed;
};

/** The pending queue, soonest expiry first: the next deadline is the first row. */
export const pendingByExpiry = (approvals: readonly Approval[]): Approval[] =>
  [...approvals].sort((left, right) => timeOf(left.expires_at) - timeOf(right.expires_at));

/** The decided list, newest decision first; an expired request falls back to its expiry. */
export const decidedNewestFirst = (approvals: readonly Approval[]): Approval[] =>
  [...approvals].sort(
    (left, right) =>
      timeOf(right.decided_at ?? right.expires_at) -
      timeOf(left.decided_at ?? left.expires_at),
  );

/** When a decided request was decided: the decision time, or its expiry when it lapsed. */
export const decidedAt = (approval: Approval): string =>
  approval.decided_at ?? approval.expires_at;

/** Said once to anyone who may read but not decide. */
export const DECIDER_ONLY_SENTENCE =
  'Only an owner or operator can approve or deny. Agents can never approve, including their own requests.';
