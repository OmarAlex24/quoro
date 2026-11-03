import { http, type HttpHandler } from 'msw';
import type {
  Agent,
  AgentKey,
  AgentKeyCreated,
  AgentKeyList,
  AgentList,
  CreateAgentRequest,
  UpdateAgentRequest,
} from '../api/generated/model';
import { agency, agentKeys, appendAudit, findAgent } from './agency';
import { db, mockId, now } from './db';
import {
  conflict,
  json,
  noContent,
  notFound,
  page,
  readJson,
  refuseUnlessWriter,
  signedIn,
  unauthorized,
  validationFailed,
} from './http';

const AGENT_NAME_MAX = 200;
const AGENT_DESCRIPTION_MAX = 2_000;

const BASE64URL = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';

/** Unpadded base64url of `bytes`, so the key secret reads like the contract's. */
const base64url = (bytes: Uint8Array): string => {
  let out = '';
  for (let index = 0; index < bytes.length; index += 3) {
    const first = bytes[index] ?? 0;
    const second = bytes[index + 1];
    const third = bytes[index + 2];
    out += BASE64URL[first >> 2];
    if (second === undefined) {
      out += BASE64URL[(first & 3) << 4];
      break;
    }
    out += BASE64URL[((first & 3) << 4) | (second >> 4)];
    if (third === undefined) {
      out += BASE64URL[(second & 15) << 2];
      break;
    }
    out += BASE64URL[((second & 15) << 2) | (third >> 6)];
    out += BASE64URL[third & 63];
  }
  return out;
};

/** A fresh 43-character secret, 32 random bytes. Never stored anywhere. */
const freshKeySecret = (): string => {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  return base64url(bytes);
};

const descriptionOf = (value: string | undefined): string | null => {
  const description = (value ?? '').trim();
  return description === '' ? null : description;
};

const nameProblem = (name: string): string | null => {
  if (name.length === 0 || name.length > AGENT_NAME_MAX) {
    return 'Agent name must be 1-200 characters.';
  }
  return null;
};

const modelProblem = (model: string): string | null =>
  model.length === 0 ? 'Enter the model identifier the agent runs.' : null;

const descriptionProblem = (description: string): string | null =>
  description.length > AGENT_DESCRIPTION_MAX
    ? 'Keep the description under 2000 characters.'
    : null;

/** Stateful mock handlers for the agents screens. Mock mode only. */
export const agentsHandlers: HttpHandler[] = [
  http.get('*/api/v1/agents', ({ request }) => {
    if (!signedIn()) return unauthorized();
    const items = [...agency().agents].sort((left, right) =>
      left.name.localeCompare(right.name),
    );
    const list: AgentList = page(items, request);
    return json(list);
  }),

  http.post('*/api/v1/agents', async ({ request }) => {
    const refusal = refuseUnlessWriter('agent:write');
    if (refusal) return refusal;

    const state = agency();
    const body = await readJson<CreateAgentRequest>(request);
    if (!body) return validationFailed('The form could not be read. Try again.');

    const name = (body.name ?? '').trim();
    const model = (body.model ?? '').trim();
    const description = (body.description ?? '').trim();

    const invalid =
      nameProblem(name) ?? modelProblem(model) ?? descriptionProblem(description);
    if (invalid) return validationFailed(invalid);

    if (!db.owner || body.owner_user_id !== db.owner.id) {
      return validationFailed('The owner must be an existing user.');
    }
    if (state.agents.some((agent) => agent.name === name)) {
      return conflict(`An agent named "${name}" already exists.`);
    }

    const createdAt = now();
    const agent: Agent = {
      id: mockId('agt'),
      name,
      model,
      owner_user_id: body.owner_user_id,
      description: description === '' ? null : description,
      disabled: false,
      created_at: createdAt,
      updated_at: createdAt,
    };
    state.agents.push(agent);

    appendAudit({
      operation: 'POST /api/v1/agents',
      scope: 'agent:write',
      target_type: 'agent',
      target_id: agent.id,
      params: { name, model, owner_user_id: agent.owner_user_id },
    });

    return json(agent, { status: 201 });
  }),

  http.get('*/api/v1/agents/:agentId', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const agent = findAgent(String(params.agentId));
    if (!agent) return notFound('That agent');
    return json(agent);
  }),

  http.patch('*/api/v1/agents/:agentId', async ({ request, params }) => {
    const refusal = refuseUnlessWriter('agent:write');
    if (refusal) return refusal;

    const state = agency();
    const agentId = String(params.agentId);
    const agent = findAgent(agentId);
    if (!agent) return notFound('That agent');

    const body = await readJson<UpdateAgentRequest>(request);
    if (!body) return validationFailed('The form could not be read. Try again.');

    const changed: Record<string, unknown> = {};

    if (body.name !== undefined) {
      const name = body.name.trim();
      const invalid = nameProblem(name);
      if (invalid) return validationFailed(invalid);
      if (state.agents.some((other) => other.id !== agent.id && other.name === name)) {
        return conflict(`An agent named "${name}" already exists.`);
      }
      agent.name = name;
      changed.name = name;
    }

    if (body.model !== undefined) {
      const model = body.model.trim();
      const invalid = modelProblem(model);
      if (invalid) return validationFailed(invalid);
      agent.model = model;
      changed.model = model;
    }

    if (body.description !== undefined) {
      const description = body.description.trim();
      const invalid = descriptionProblem(description);
      if (invalid) return validationFailed(invalid);
      agent.description = description === '' ? null : description;
      changed.description = description;
    }

    if (body.owner_user_id !== undefined) {
      if (!db.owner || body.owner_user_id !== db.owner.id) {
        return validationFailed('The owner must be an existing user.');
      }
      agent.owner_user_id = body.owner_user_id;
      changed.owner_user_id = body.owner_user_id;
    }

    if (body.disabled !== undefined) {
      if (typeof body.disabled !== 'boolean') {
        return validationFailed('Disabled must be true or false.');
      }
      agent.disabled = body.disabled;
      changed.disabled = body.disabled;
    }

    agent.updated_at = now();

    appendAudit({
      operation: 'PATCH /api/v1/agents/{agent_id}',
      scope: 'agent:write',
      target_type: 'agent',
      target_id: agent.id,
      params: changed,
    });

    return json(agent);
  }),

  http.delete('*/api/v1/agents/:agentId', ({ params }) => {
    const refusal = refuseUnlessWriter('agent:write');
    if (refusal) return refusal;

    const state = agency();
    const agentId = String(params.agentId);
    const agent = findAgent(agentId);
    if (!agent) return notFound('That agent');

    const revokedAt = now();
    for (const key of state.keys) {
      if (key.agent_id === agentId && !key.revoked_at) key.revoked_at = revokedAt;
    }
    state.agents = state.agents.filter((candidate) => candidate.id !== agentId);

    appendAudit({
      operation: 'DELETE /api/v1/agents/{agent_id}',
      scope: 'agent:write',
      target_type: 'agent',
      target_id: agentId,
      params: { agent_id: agentId },
    });

    return noContent();
  }),

  http.get('*/api/v1/agents/:agentId/keys', ({ request, params }) => {
    if (!signedIn()) return unauthorized();
    const agentId = String(params.agentId);
    if (!findAgent(agentId)) return notFound('That agent');
    const list: AgentKeyList = page(agentKeys(agentId), request);
    return json(list);
  }),

  http.post('*/api/v1/agents/:agentId/keys', ({ params }) => {
    const refusal = refuseUnlessWriter('agent:write');
    if (refusal) return refusal;

    const state = agency();
    const agentId = String(params.agentId);
    const agent = findAgent(agentId);
    if (!agent) return notFound('That agent');

    const key: AgentKey = {
      id: mockId('akey'),
      agent_id: agentId,
      created_at: now(),
      revoked_at: null,
      last_used_at: null,
    };
    state.keys.push(key);

    appendAudit({
      operation: 'POST /api/v1/agents/{agent_id}/keys',
      scope: 'agent:write',
      target_type: 'agent',
      target_id: agentId,
      params: { agent_id: agentId, key: '[redacted]' },
    });

    const created: AgentKeyCreated = { ...key, key: freshKeySecret() };
    return json(created, { status: 201 });
  }),

  http.delete('*/api/v1/agents/:agentId/keys/:keyId', ({ params }) => {
    const refusal = refuseUnlessWriter('agent:write');
    if (refusal) return refusal;

    const state = agency();
    const agentId = String(params.agentId);
    const keyId = String(params.keyId);

    const key = state.keys.find(
      (candidate) => candidate.id === keyId && candidate.agent_id === agentId,
    );
    if (!key) return notFound('That key');

    if (!key.revoked_at) key.revoked_at = now();

    appendAudit({
      operation: 'DELETE /api/v1/agents/{agent_id}/keys/{key_id}',
      scope: 'agent:write',
      target_type: 'agent',
      target_id: agentId,
      params: { agent_id: agentId, key_id: keyId },
    });

    return noContent();
  }),
];
