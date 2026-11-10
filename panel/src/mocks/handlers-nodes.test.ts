import { setupServer } from 'msw/node';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { Cluster, Node, NodeJoinTokenCreated, Problem } from '../api/generated/model';
import { setRole } from './agency';
import { db, mockId, now } from './db';
import { handlers } from './handlers';
import { failNextRolesCall, resetNodesMock, VOTERS_CHANGING } from './handlers-nodes';

const server = setupServer(...handlers);

const BASE = 'http://panel.test/api/v1';

beforeAll(() => server.listen());
afterAll(() => server.close());

const edge = (): Node => {
  const found = db.extraNodes.find((node) => node.name === 'lab-edge');
  if (!found) throw new Error('the seed has no lab-edge');
  return found;
};

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
  setRole('owner');
  db.sessionUserId = db.owner.id;
  resetNodesMock();
});

// Every test starts from the seed: lab-edge is an edge without a vote.
afterEach(() => {
  edge().roles = ['edge'];
  edge().status = 'connected';
});

const roles = (nodeId: string, body: unknown): Promise<Response> =>
  fetch(`${BASE}/nodes/${nodeId}/roles`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

const cluster = async (): Promise<Cluster> => {
  const response = await fetch(`${BASE}/cluster`);
  expect(response.status).toBe(200);
  return (await response.json()) as Cluster;
};

describe('the seeded voters', () => {
  it('starts with two voters and an edge without a vote', async () => {
    const { votes } = await cluster();
    expect(votes.count).toBe(2);
    expect(votes.required).toBe(3);
    expect(votes.ha_ready).toBe(false);
    expect(votes.voters.map((voter) => voter.node_id)).not.toContain(edge().id);
  });
});

describe('updateNodeRoles mock', () => {
  it('adds the witness role, and the votes follow', async () => {
    const response = await roles(edge().id, { add: ['witness'] });
    expect(response.status).toBe(200);
    const node = (await response.json()) as Node;
    expect(node.roles).toEqual(['edge', 'witness']);

    const { votes } = await cluster();
    expect(votes.count).toBe(3);
    expect(votes.ha_ready).toBe(true);
    expect(votes.voters.at(-1)).toEqual({ node_id: edge().id, roles: ['edge', 'witness'], connected: true });
  });

  it('removes the witness role, and the vote goes', async () => {
    await roles(edge().id, { add: ['witness'] });
    const response = await roles(edge().id, { remove: ['witness'] });
    expect(response.status).toBe(200);
    expect(((await response.json()) as Node).roles).toEqual(['edge']);

    const { votes } = await cluster();
    expect(votes.count).toBe(2);
    expect(votes.ha_ready).toBe(false);
  });

  it('answers 200 and changes nothing for a role the machine has, or lacks', async () => {
    const lacking = await roles(edge().id, { remove: ['witness'] });
    expect(lacking.status).toBe(200);
    expect(((await lacking.json()) as Node).roles).toEqual(['edge']);

    await roles(edge().id, { add: ['witness'] });
    const having = await roles(edge().id, { add: ['witness'] });
    expect(having.status).toBe(200);
    expect(((await having.json()) as Node).roles).toEqual(['edge', 'witness']);
    expect((await cluster()).votes.count).toBe(3);
  });

  it('refuses both or neither of add and remove, and any role but witness', async () => {
    for (const body of [{}, { add: ['witness'], remove: ['witness'] }, { add: ['node'] }, { add: [] }]) {
      const response = await roles(edge().id, body);
      expect(response.status).toBe(422);
      expect(((await response.json()) as Problem).code).toBe('validation_failed');
    }
    expect(edge().roles).toEqual(['edge']);
  });

  it('refuses to leave a machine with no role', async () => {
    edge().roles = ['witness'];
    const response = await roles(edge().id, { remove: ['witness'] });
    expect(response.status).toBe(422);
    expect(edge().roles).toEqual(['witness']);
  });

  it('answers 404 for an unknown node and 409 for a removed one', async () => {
    expect((await roles(mockId('nod'), { add: ['witness'] })).status).toBe(404);
    edge().status = 'removed';
    const removed = await roles(edge().id, { add: ['witness'] });
    expect(removed.status).toBe(409);
  });

  it('refuses a viewer', async () => {
    setRole('viewer');
    const response = await roles(edge().id, { add: ['witness'] });
    expect(response.status).toBe(403);
    expect(edge().roles).toEqual(['edge']);
  });

  it('answers the next vote change with 409 when a test asks for it, once', async () => {
    failNextRolesCall();
    const refused = await roles(edge().id, { add: ['witness'] });
    expect(refused.status).toBe(409);
    const problem = (await refused.json()) as Problem;
    expect(problem.code).toBe('conflict');
    expect(problem.detail).toBe(VOTERS_CHANGING);
    expect(edge().roles).toEqual(['edge']);

    expect((await roles(edge().id, { add: ['witness'] })).status).toBe(200);
  });
});

describe('createNodeJoinToken mock', () => {
  const create = (body?: unknown): Promise<Response> =>
    fetch(`${BASE}/nodes/join-tokens`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': crypto.randomUUID() },
      body: body === undefined ? undefined : JSON.stringify(body),
    });

  it('returns the secret once, in a join command, valid for an hour', async () => {
    const response = await create({});
    expect(response.status).toBe(201);
    const created = (await response.json()) as NodeJoinTokenCreated;
    expect(created.id).toMatch(/^njt_[0-9A-HJKMNP-TV-Z]{26}$/);
    expect(created.token).toMatch(/^njt_[A-Za-z0-9_-]{43}$/);
    expect(created.join_command).toContain(`--token ${created.token}`);
    expect(created.join_command).toMatch(/^quoro join https:\/\/\S+ --token \S+ --fingerprint [0-9a-f]{64}$/);
    const minutes = (Date.parse(created.expires_at) - Date.now()) / 60_000;
    expect(minutes).toBeGreaterThan(59);
    expect(minutes).toBeLessThanOrEqual(60);
  });

  it('refuses an expiry in the past, and a viewer', async () => {
    expect((await create({ expires_at: '2020-01-01T00:00:00Z' })).status).toBe(422);
    setRole('viewer');
    expect((await create({})).status).toBe(403);
  });
});
