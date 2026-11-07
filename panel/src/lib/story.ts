// SPDX-License-Identifier: AGPL-3.0-only
import type { Approval, AuditRecord, Event, Grant, Incident } from '../api/generated/model';
import {
  capitalize,
  grantSpan,
  REFUSAL_WORDS,
  recordAction,
  recordSentence,
  scopeVerb,
  type RecordContext,
  type Segment,
} from './activity';
import { expiryWords, shortId } from './agency-format';
import { approvalWords } from './approvals';

/*
 * The story of an incident, as an agent handled it: one step per audit record
 * of the incident, in order, said from the agent's side ("Restarted blog",
 * "Asked you to approve deleting blog"), with the agent's own reason, its
 * `justification`, as the Why line. A request that still waits for a person
 * is where the story ends, with the decision itself.
 */

/** The marker on the spine: what kind of step it is. */
export type StoryMark = 'opened' | 'system' | 'grant' | 'call' | 'refused' | 'ask' | 'person';

export interface StoryStep {
  readonly key: string;
  /** The audit record behind the step; null for a request the log does not show yet. */
  readonly seq: number | null;
  readonly time: string;
  readonly mark: StoryMark;
  readonly sentence: Segment[];
  /** OK, Refused, Approved…: the result, after the sentence. */
  readonly tag?: { readonly tone: 'run' | 'shift' | 'fault' | 'idle'; readonly words: string };
  /** Words before the tags in the meta line ("Not in its grant:"). */
  readonly lead?: string;
  /** Code tags in the meta line: an event type, scopes. */
  readonly tags?: readonly string[];
  /** How many more scopes the tags leave out. */
  readonly more?: number;
  /** Plain words after the tags ("42 min left", "health check failed on lab-2"). */
  readonly note?: string;
  /** The agent's stated reason. */
  readonly why?: string;
  /** A request that still waits: the step carries the decision. */
  readonly pendingApprovalId?: string;
}

export interface StoryInput {
  readonly agentId: string;
  readonly incident: Pick<Incident, 'id' | 'subject' | 'title'> & { trigger_event?: Pick<Event, 'type' | 'data'> | null };
  /** Every audit record of the incident, any order. */
  readonly records: readonly AuditRecord[];
  /** The agent's requests for this incident. */
  readonly approvals: readonly Approval[];
  readonly grants: readonly Grant[];
  readonly ctx: RecordContext;
  readonly now: number;
}

/** How many scopes a grant step shows before "+N". */
const SCOPES_SHOWN = 3;

const isAsk = (operation: string) => operation === 'request_approval' || operation === 'POST /api/v1/approvals';
const isDecision = (operation: string) => /^POST \/api\/v1\/approvals\/\{approval_id\}\/(approve|deny)$/.test(operation);

/** What the event that opened the incident says about why, from its documented data. */
export const triggerWords = (
  event: Pick<Event, 'type' | 'data'> | null | undefined,
  ctx: RecordContext,
): string | undefined => {
  if (!event) return undefined;
  const data = (event.data ?? {}) as Record<string, unknown>;
  const node = typeof data.node_id === 'string' ? (ctx.name?.('node', data.node_id) ?? shortId(data.node_id)) : null;
  const where = node ? ` on ${node}` : '';
  const times = typeof data.failures === 'number' && data.failures > 1 ? ` ${data.failures} times` : '';
  switch (data.check) {
    case 'health_check':
      return `health check failed${times}${where}`;
    case 'vm_crashed':
      return `its VM crashed${where}`;
    case 'edge_health':
      return `the edge has no instance in rotation${where}`;
    default:
      return undefined;
  }
};

/** "blog went down and Quoro opened an incident", or the incident's own title. */
const openedSentence = (input: StoryInput): Segment[] => {
  const { incident, ctx } = input;
  const subject = ctx.name?.(incident.subject.type, incident.subject.id);
  const type = incident.trigger_event?.type;
  if (subject && (type === 'app.down' || type === 'vm.crashed')) {
    return [{ text: subject, strong: true }, { text: ' went down and Quoro opened an incident' }];
  }
  return [{ text: 'Quoro opened an incident: ' }, { text: incident.title, strong: true }];
};

/** The grant's time left in words, or how it ended. */
const grantNote = (grant: Grant, now: number): string => {
  if (grant.status === 'revoked') return 'revoked';
  if (grant.status === 'exhausted') return 'used up';
  const ends = Date.parse(grant.expires_at);
  if (Number.isNaN(ends) || ends <= now || grant.status === 'expired') return 'expired';
  const words = expiryWords(grant.expires_at, now);
  return words === 'now' ? 'ending now' : `${words.replace(/^in /, '')} left`;
};

const decisionTag = (status: Approval['status']): StoryStep['tag'] => ({
  tone: status === 'approved' ? 'run' : status === 'denied' ? 'fault' : status === 'pending' ? 'shift' : 'idle',
  words: approvalWords[status],
});

function stepFor(record: AuditRecord, input: StoryInput): StoryStep {
  const { ctx, agentId, now } = input;
  const base = { key: `seq-${record.seq}`, seq: record.seq, time: record.time };
  const why = record.justification ?? undefined;

  // Quoro opened the incident.
  if (record.actor_type === 'system' && record.target_type === 'incident' && !record.operation.toLowerCase().includes('resolve')) {
    const event = input.incident.trigger_event;
    return {
      ...base,
      mark: 'opened',
      sentence: openedSentence(input),
      tags: event ? [event.type] : undefined,
      note: triggerWords(event, ctx),
    };
  }

  // Quoro gave the agent its incident grant.
  if (record.actor_type === 'system' && record.target_type === 'grant' && record.target_id) {
    const grant = input.grants.find((candidate) => candidate.id === record.target_id);
    if (grant && grant.agent_id === agentId && !record.operation.toLowerCase().includes('revoke')) {
      const span = grantSpan(grant);
      const kind = grant.kind === 'incident' ? 'an incident grant' : 'a grant';
      return {
        ...base,
        mark: 'grant',
        sentence: [{ text: `It got ${kind}${span ? ` for ${span}` : ''}` }],
        tags: grant.scopes.slice(0, SCOPES_SHOWN),
        more: Math.max(0, grant.scopes.length - SCOPES_SHOWN),
        note: grantNote(grant, now),
      };
    }
  }

  // The agent's own calls, said from its side.
  if (record.actor_type === 'agent' && record.actor_id === agentId) {
    if (isAsk(record.operation) && record.result === 'ok') {
      const approval = input.approvals.find((candidate) => candidate.id === record.target_id);
      const scope = approval?.scope ?? (typeof record.params?.scope === 'string' ? record.params.scope : null);
      const target = approval?.target;
      const [, gerund] = scope ? scopeVerb(scope) : ['', 'acting on'];
      const named = target ? (ctx.name?.(target.type, target.id) ?? `${target.type} ${shortId(target.id)}`) : null;
      const sentence: Segment[] = named
        ? [{ text: `Asked you to approve ${gerund} ` }, { text: named, strong: true }]
        : [{ text: 'Asked you for an approval' }];
      if (approval?.status === 'pending') {
        return { ...base, mark: 'ask', sentence, pendingApprovalId: approval.id };
      }
      return { ...base, mark: 'ask', sentence, tag: approval ? decisionTag(approval.status) : undefined, why };
    }
    const went = record.result === 'ok';
    const step: StoryStep = {
      ...base,
      mark: went ? 'call' : 'refused',
      sentence: capitalize(recordAction(record, ctx)),
      tag: went ? { tone: 'run', words: 'OK' } : { tone: 'fault', words: record.result === 'error' ? 'Error' : 'Refused' },
      why,
    };
    if (went || !record.result_detail) return step;
    const reason = REFUSAL_WORDS[record.result_detail];
    if (record.result_detail === 'scope_missing' || record.result_detail === 'approval_required') {
      return {
        ...step,
        lead: record.result_detail === 'scope_missing' ? 'Not in its grant:' : 'Needs a person’s approval:',
        tags: record.scope ? [record.scope] : undefined,
      };
    }
    return { ...step, note: reason ?? record.result_detail };
  }

  // A person's decision on one of its requests.
  if (record.actor_type === 'user' && isDecision(record.operation)) {
    const note = typeof record.params?.note === 'string' ? record.params.note : undefined;
    return {
      ...base,
      mark: 'person',
      sentence: recordSentence({ ...record, channel: 'system' }, ctx),
      note: note ? `“${note}”` : undefined,
    };
  }

  // Anything else, by whoever did it.
  return {
    ...base,
    mark: record.actor_type === 'user' ? 'person' : 'system',
    sentence: recordSentence({ ...record, channel: 'system' }, ctx),
    tag: record.result === 'ok' ? undefined : { tone: 'fault', words: record.result === 'error' ? 'Error' : 'Refused' },
    why,
  };
}

/** The steps of the story, oldest first; a waiting request ends it. */
export function buildStory(input: StoryInput): StoryStep[] {
  const records = [...input.records].sort((left, right) => left.seq - right.seq);
  const steps = records.map((record) => stepFor(record, input));

  // A request the log does not show yet still belongs at the end.
  const shown = new Set(steps.map((step) => step.pendingApprovalId).filter(Boolean));
  for (const approval of input.approvals) {
    if (approval.status !== 'pending' || shown.has(approval.id) || approval.agent_id !== input.agentId) continue;
    const [, gerund] = scopeVerb(approval.scope);
    const named =
      input.ctx.name?.(approval.target.type, approval.target.id) ??
      `${approval.target.type} ${shortId(approval.target.id)}`;
    steps.push({
      key: `apr-${approval.id}`,
      seq: null,
      time: approval.created_at,
      mark: 'ask',
      sentence: [{ text: `Asked you to approve ${gerund} ` }, { text: named, strong: true }],
      pendingApprovalId: approval.id,
    });
  }
  return steps;
}
