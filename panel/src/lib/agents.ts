import type { Agent, UpdateAgentRequest } from '../api/generated/model';
import type { LampTone } from './lamp';

/** What the agent bay and plate show next to the lamp. */
export interface AgentLampState {
  readonly tone: LampTone;
  readonly statusText: string;
}

/**
 * A disabled agent cannot mint access tokens, so it is off until someone
 * enables it again: `idle` and the word "Disabled". A live agent is `run`.
 */
export const agentLamp = (agent: Pick<Agent, 'disabled'>): AgentLampState =>
  agent.disabled
    ? { tone: 'idle', statusText: 'Disabled' }
    : { tone: 'run', statusText: 'Enabled' };

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
