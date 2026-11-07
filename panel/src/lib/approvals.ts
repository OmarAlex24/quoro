import type {
  Approval,
  ApprovalParams,
  ApprovalStatus,
  DecisionRequest,
  Scope,
} from '../api/generated/model';
import { scopeVerb } from './activity';
import { clamp } from './dither';
import { expiryWords, shortId } from './agency-format';
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

/**
 * The expiry meter of a waiting request: the share of its time it has left,
 * from filing to `expires_at`, and the words over it.
 */
export const approvalMeter = (
  approval: Pick<Approval, 'created_at' | 'expires_at'>,
  now: number = Date.now(),
): { share: number; words: string; over: boolean } => {
  const from = Date.parse(approval.created_at);
  const until = Date.parse(approval.expires_at);
  if (Number.isNaN(until)) return { share: 0, words: 'No expiry recorded', over: false };
  if (until <= now) return { share: 0, words: 'Expired', over: true };
  const share = Number.isNaN(from) || until <= from ? 1 : clamp((until - now) / (until - from));
  return { share, words: `Expires ${expiryWords(approval.expires_at, now)}`, over: false };
};

/** The button that approves: it says what it approves when the action destroys something. */
export const approveWords = (scope: Scope): string => {
  const verb = scope.slice(scope.indexOf(':') + 1);
  if (verb === 'delete' || verb === 'delete_quarantine') return 'Approve deletion';
  if (verb === 'restore') return 'Approve restore';
  if (verb === 'failover') return 'Approve failover';
  return 'Approve';
};

/**
 * The one call an approval allows, written out the way the agent made it:
 * `delete_app { app_id: "app_01…" }`. Every parameter, in the order it was
 * recorded; nested values as compact JSON.
 */
export const callText = (approval: Pick<Approval, 'operation' | 'params'>): string => {
  const entries = Object.entries(approval.params ?? {});
  if (entries.length === 0) return `${approval.operation} {}`;
  const parts = entries.map(([key, value]) => `${key}: ${JSON.stringify(value) ?? 'null'}`);
  return `${approval.operation} { ${parts.join(', ')} }`;
};

/** The target of a request by name: "blog" for an app or VM, "project docs" otherwise. */
const askTarget = (
  approval: Pick<Approval, 'target'>,
  name: (type: string, id: string) => string | undefined,
): { name: string; kind: string } => {
  const { type, id } = approval.target;
  const known = name(type, id);
  return { name: known ?? shortId(id), kind: type };
};

/** A request in two words for a list: "Delete blog", "Delete project docs". */
export const approvalTitle = (
  approval: Pick<Approval, 'scope' | 'target'>,
  name: (type: string, id: string) => string | undefined,
): string => {
  const [base] = scopeVerb(approval.scope);
  const target = askTarget(approval, name);
  const object = target.kind === 'app' || target.kind === 'vm' ? target.name : `${target.kind} ${target.name}`;
  return `${base.charAt(0).toUpperCase()}${base.slice(1)} ${object}`;
};

/** What the agent wants, after its name: "wants to delete the app blog". */
export const approvalWants = (
  approval: Pick<Approval, 'scope' | 'target'>,
  name: (type: string, id: string) => string | undefined,
): string => {
  const [base] = scopeVerb(approval.scope);
  const target = askTarget(approval, name);
  return `wants to ${base} the ${target.kind} ${target.name}`;
};
