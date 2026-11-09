import type {
  Approval,
  ApprovalStatus,
  DecisionRequest,
  Scope,
} from '../api/generated/model';
import { deletedWords, kindWord, scopeVerb, type RecordContext } from './activity';
import { clamp } from './dither';
import { expiryWords, shortId } from './agency-format';
import { DESTRUCTIVE_SCOPES } from './scopes';

/**
 * The approvals queue, as pure rules: how a request's state is said, how the
 * inbox orders requests, and exactly what a decision sends. Keeping these here
 * lets the screens stay thin and lets the rules be tested without a DOM.
 */

/** The status in one word, title case: "Waiting", "Approved", "Denied", "Expired". */
export const approvalWords: Record<ApprovalStatus, string> = {
  pending: 'Waiting',
  approved: 'Approved',
  denied: 'Denied',
  expired: 'Expired',
};

/** A destructive scope is one the panel marks fault-red on a request. */
export const isDestructiveScope = (scope: Scope): boolean => DESTRUCTIVE_SCOPES.has(scope);

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

/**
 * True once the one call an approval allowed has run: it was approved and the
 * one-time grant it issued was used. Until then the request is said as asked.
 */
export const approvalRan = (
  approval: Pick<Approval, 'status'>,
  grant: { readonly uses?: number } | undefined,
): boolean => approval.status === 'approved' && (grant?.uses ?? 0) > 0;

/** Where a request stands for its words: whether its call ran, and whether its target is gone. */
export interface AskState {
  readonly ran?: boolean;
  readonly gone?: boolean;
}

/** The state of a request from the grants and lists the context holds. */
export const askState = (
  approval: Pick<Approval, 'status' | 'grant_id' | 'target'>,
  ctx: Pick<RecordContext, 'grant' | 'gone'>,
): AskState => ({
  ran: approvalRan(approval, approval.grant_id ? ctx.grant?.(approval.grant_id) : undefined),
  gone: ctx.gone?.(approval.target.type, approval.target.id) ?? false,
});

/** "an app", "a VM": a kind with its article. */
const aKind = (type: string): string => {
  const word = kindWord(type);
  return `${/^[aeiou]/i.test(word) ? 'an' : 'a'} ${word}`;
};

/**
 * What a request acts on, in words. A known name stays, also once the target
 * is gone. A gone target with no name left is "a deleted app", or just "an
 * app" after a verb that already says it was deleted. Otherwise the short id.
 */
const askObject = (
  approval: Pick<Approval, 'scope' | 'target'>,
  name: (type: string, id: string) => string | undefined,
  state: AskState,
  withKind: boolean,
): string => {
  const { type, id } = approval.target;
  const known = name(type, id);
  if (known) return withKind || (type !== 'app' && type !== 'vm') ? `${withKind ? 'the ' : ''}${kindWord(type)} ${known}` : known;
  if (state.gone) {
    const verb = approval.scope.slice(approval.scope.indexOf(':') + 1);
    return state.ran && (verb === 'delete' || verb === 'remove') ? aKind(type) : deletedWords(type);
  }
  return withKind ? `the ${kindWord(type)} ${shortId(id)}` : shortId(id);
};

/**
 * A request in two words for a list: "Delete blog", "Delete project docs";
 * once its call ran, "Deleted blog".
 */
export const approvalTitle = (
  approval: Pick<Approval, 'scope' | 'target'>,
  name: (type: string, id: string) => string | undefined,
  state: AskState = {},
): string => {
  const [base, , past] = scopeVerb(approval.scope);
  const verb = state.ran ? past : base;
  return `${verb.charAt(0).toUpperCase()}${verb.slice(1)} ${askObject(approval, name, state, false)}`;
};

/**
 * What the agent asked, after its name: "wants to delete the app blog" while
 * it waits, "asked to delete the app blog" once decided, and "deleted the app
 * blog" once the call ran.
 */
export const approvalWants = (
  approval: Pick<Approval, 'scope' | 'target' | 'status'>,
  name: (type: string, id: string) => string | undefined,
  state: AskState = {},
): string => {
  const [base, , past] = scopeVerb(approval.scope);
  const object = askObject(approval, name, state, true);
  if (state.ran) return `${past} ${object}`;
  return approval.status === 'pending' ? `wants to ${base} ${object}` : `asked to ${base} ${object}`;
};
