import { setupServer } from 'msw/node';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { Agent, AgentKey, AgentKeyCreated, AgentKeyList } from '../api/generated/model';
import { agency } from './agency';
import { db, mockId, now } from './db';
import { agentsHandlers } from './handlers-agents';

const server = setupServer(...agentsHandlers);

const BASE = 'http://panel.test/api/v1';

beforeAll(() => server.listen());
afterAll(() => server.close());

beforeEach(() => {
  if (!db.owner) {
    db.owner = {
      id: mockId('usr'),
      email: 'owner@example.com',
      name: 'Owner',
      role: 'owner',
      disabled: false,
      created_at: now(),
    };
  }
  db.sessionUserId = db.owner.id;
});

const agentFor = (name: string): Agent => {
  const agent: Agent = {
    id: mockId('agt'),
    name,
    model: 'example-model-id',
    owner_user_id: db.owner?.id ?? 'system',
    description: null,
    disabled: false,
    created_at: now(),
    updated_at: now(),
  };
  agency().agents.push(agent);
  return agent;
};

const keyFor = (agentId: string, createdAt: string, revokedAt: string | null = null): AgentKey => {
  const key: AgentKey = {
    id: mockId('akey'),
    agent_id: agentId,
    created_at: createdAt,
    revoked_at: revokedAt,
    last_used_at: null,
  };
  agency().keys.push(key);
  return key;
};

const listKeys = async (agentId: string): Promise<AgentKeyList> => {
  const response = await fetch(`${BASE}/agents/${agentId}/keys?limit=200`);
  expect(response.status).toBe(200);
  return (await response.json()) as AgentKeyList;
};

describe('agent keys mock', () => {
  it('lists an agent keys newest first, revoked keys included', async () => {
    const agent = agentFor('keys-order');
    const older = keyFor(agent.id, '2026-10-30T10:00:00Z');
    const newer = keyFor(agent.id, '2026-11-02T10:00:00Z');
    const revoked = keyFor(agent.id, '2026-10-20T10:00:00Z', '2026-10-21T10:00:00Z');

    const list = await listKeys(agent.id);
    const ids = list.items.map((key) => key.id);
    expect(ids.indexOf(newer.id)).toBeLessThan(ids.indexOf(older.id));
    expect(ids.indexOf(older.id)).toBeLessThan(ids.indexOf(revoked.id));
    expect(ids).toContain(revoked.id);
    expect(list.items.find((key) => key.id === revoked.id)?.revoked_at).toBe(
      '2026-10-21T10:00:00Z',
    );
    expect(list.next_cursor).toBeNull();
  });

  it('never returns another agent keys', async () => {
    const mine = agentFor('keys-mine');
    const other = agentFor('keys-other');
    const own = keyFor(mine.id, now());
    keyFor(other.id, now());

    const list = await listKeys(mine.id);
    expect(list.items.map((key) => key.id)).toEqual([own.id]);
  });

  it('shows a created key in the list and never echoes its secret', async () => {
    const agent = agentFor('keys-create');
    const response = await fetch(`${BASE}/agents/${agent.id}/keys`, {
      method: 'POST',
      headers: { 'Idempotency-Key': 'test-create-key' },
    });
    expect(response.status).toBe(201);
    const created = (await response.json()) as AgentKeyCreated;
    expect(created.key).not.toBe('');

    const list = await listKeys(agent.id);
    const stored = list.items.find((key) => key.id === created.id);
    expect(stored).toBeDefined();
    expect(stored).not.toHaveProperty('key');
    expect(stored?.revoked_at).toBeNull();
  });

  it('sets revoked_at when a key is revoked', async () => {
    const agent = agentFor('keys-revoke');
    const key = keyFor(agent.id, now());

    const response = await fetch(`${BASE}/agents/${agent.id}/keys/${key.id}`, {
      method: 'DELETE',
    });
    expect(response.status).toBe(204);

    const list = await listKeys(agent.id);
    const stored = list.items.find((candidate) => candidate.id === key.id);
    expect(stored?.revoked_at).not.toBeNull();
  });
});
