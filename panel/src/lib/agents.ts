import type { Agent, Approval, UpdateAgentRequest } from '../api/generated/model';
import { scopeVerb } from './activity';
import type { LampTone } from './lamp';

export const AGENT_NAME_MAX = 200;
export const AGENT_DESCRIPTION_MAX = 2_000;

export interface AgentDraft {
  name: string;
  model: string;
  description: string;
}

/** The first thing wrong with a draft, said plainly; `null` when it is fine. */
export const agentDraftProblem = (draft: AgentDraft): string | null => {
  const name = draft.name.trim();
  if (name.length === 0) return 'Give the agent a name.';
  if (name.length > AGENT_NAME_MAX) return 'Keep the name under 200 characters.';
  if (draft.model.trim().length === 0) {
    return 'Enter the model identifier the agent runs.';
  }
  if (draft.description.trim().length > AGENT_DESCRIPTION_MAX) {
    return 'Keep the description under 2000 characters.';
  }
  return null;
};

/**
 * A PATCH body with only the fields the operator actually changed. Comparing
 * trimmed values means re-typing a name with a trailing space sends nothing.
 */
export const buildAgentPatch = (
  original: Pick<Agent, 'name' | 'model' | 'description'>,
  draft: AgentDraft,
): UpdateAgentRequest => {
  const patch: UpdateAgentRequest = {};

  const name = draft.name.trim();
  if (name !== original.name) patch.name = name;

  const model = draft.model.trim();
  if (model !== original.model) patch.model = model;

  const description = draft.description.trim();
  if (description !== (original.description ?? '')) patch.description = description;

  return patch;
};

export const agentPatchIsEmpty = (patch: UpdateAgentRequest): boolean =>
  Object.keys(patch).length === 0;

/** The body that disables or re-enables an agent. */
export const agentDisabledPatch = (disabled: boolean): UpdateAgentRequest => ({ disabled });

/** `akey_` plus 26 Crockford base32 characters, as the contract fixes it. */
export const AGENT_KEY_ID_PATTERN = /^akey_[0-9A-HJKMNP-TV-Z]{26}$/;

export const isAgentKeyId = (value: string): boolean =>
  AGENT_KEY_ID_PATTERN.test(value.trim());

/* ---------- the agent card ---------- */

export interface AgentStatus {
  readonly tone: LampTone;
  /** The lamp breathes while the agent waits on a person. */
  readonly pulse: boolean;
  /** Something here needs a person: the phrase takes the amber ink. */
  readonly attention: boolean;
  /** "Waiting on you · delete blog". */
  readonly phrase: string;
}

const grantCount = (count: number): string =>
  count === 0 ? 'no active grants' : count === 1 ? '1 active grant' : `${count} active grants`;

/**
 * One phrase for where an agent stands, most pressing first: disabled; waiting
 * on a person for a request; working an open incident; ready with grants;
 * enabled but with nothing it may do.
 */
export const agentStatus = (input: {
  agent: Pick<Agent, 'disabled'>;
  activeGrants: number;
  /** The agent's requests that still wait for a person. */
  pending: ReadonlyArray<Pick<Approval, 'scope' | 'target'>>;
  /** The name of the first request's target, when a list holds it. */
  pendingTargetName?: string | null;
  /** The open incident it holds a grant for. */
  incident?: { title: string } | null;
}): AgentStatus => {
  const { agent, activeGrants, pending, incident } = input;
  if (agent.disabled) {
    return { tone: 'idle', pulse: false, attention: false, phrase: `Disabled · ${grantCount(activeGrants)}` };
  }
  const first = pending[0];
  if (first) {
    const what =
      pending.length > 1
        ? `${pending.length} requests`
        : `${scopeVerb(first.scope)[0]} ${input.pendingTargetName ?? first.target.type}`;
    return { tone: 'shift', pulse: true, attention: true, phrase: `Waiting on you · ${what}` };
  }
  if (incident) {
    return { tone: 'shift', pulse: false, attention: false, phrase: `On an incident · ${incident.title}` };
  }
  if (activeGrants > 0) {
    return { tone: 'run', pulse: false, attention: false, phrase: `Ready · ${grantCount(activeGrants)}` };
  }
  return { tone: 'idle', pulse: false, attention: false, phrase: 'Enabled · no active grants' };
};

const HOUR = 3_600_000;

export interface CallBuckets {
  /** Calls per hour, oldest hour first; the last bucket ends at `now`. */
  readonly buckets: readonly number[];
  readonly total: number;
}

/**
 * Calls per hour over the last `hours` hours. A time a little past `now`
 * (a clock ahead of this one) counts in the last hour; anything older than
 * the window, or unreadable, is left out.
 */
export const callBuckets = (times: readonly string[], now: number, hours = 24): CallBuckets => {
  const buckets = Array.from({ length: hours }, () => 0);
  let total = 0;
  for (const iso of times) {
    const at = Date.parse(iso);
    if (Number.isNaN(at)) continue;
    const age = Math.max(0, now - at);
    if (age >= hours * HOUR) continue;
    const index = hours - 1 - Math.floor(age / HOUR);
    buckets[index]! += 1;
    total += 1;
  }
  return { buckets, total };
};
