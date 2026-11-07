import { setupServer } from 'msw/node';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { ApiTokenCreated, ApiTokenList } from '../api/generated/model';
import { db, mockId, now } from './db';
import { resetMockTokens, tokensHandlers } from './handlers-tokens';

const server = setupServer(...tokensHandlers);

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
  resetMockTokens();
});

const create = (body: unknown): Promise<Response> =>
  fetch(`${BASE}/tokens`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': crypto.randomUUID() },
    body: JSON.stringify(body),
  });

const list = async (): Promise<ApiTokenList> => {
  const response = await fetch(`${BASE}/tokens?limit=200`);
  expect(response.status).toBe(200);
  return (await response.json()) as ApiTokenList;
};

describe('personal API tokens mock', () => {
  it('returns the secret once and lists the token without it', async () => {
    const response = await create({ name: '  laptop  ', expires_at: null });
    expect(response.status).toBe(201);
    const created = (await response.json()) as ApiTokenCreated;
    expect(created.token).toMatch(/^tok_[0-9a-f]{64}$/);
    expect(created.name).toBe('laptop');
    expect(created.user_id).toBe(db.owner?.id);

    const page = await list();
    expect(page.items.map((token) => token.id)).toEqual([created.id]);
    expect(page.items[0]).not.toHaveProperty('token');
    expect(JSON.stringify(page)).not.toContain(created.token);
  });

  it('lists the newest token first', async () => {
    const first = (await (await create({ name: 'first' })).json()) as ApiTokenCreated;
    const second = (await (await create({ name: 'second' })).json()) as ApiTokenCreated;
    const page = await list();
    expect(page.items.map((token) => token.id)).toEqual([second.id, first.id]);
  });

  it('refuses an empty name and an expiry in the past', async () => {
    expect((await create({ name: '   ' })).status).toBe(422);
    expect((await create({ name: 'old', expires_at: '2020-01-01T00:00:00Z' })).status).toBe(422);
    expect((await list()).items).toHaveLength(0);
  });

  it('sets revoked_at when a token is revoked, and keeps it listed', async () => {
    const created = (await (await create({ name: 'ci' })).json()) as ApiTokenCreated;
    const response = await fetch(`${BASE}/tokens/${created.id}`, { method: 'DELETE' });
    expect(response.status).toBe(204);
    const stored = (await list()).items.find((token) => token.id === created.id);
    expect(stored?.revoked_at).not.toBeNull();
  });

  it('answers not found for an unknown token', async () => {
    const response = await fetch(`${BASE}/tokens/${mockId('tok')}`, { method: 'DELETE' });
    expect(response.status).toBe(404);
  });

  it('is unauthenticated without a session', async () => {
    db.sessionUserId = null;
    expect((await fetch(`${BASE}/tokens`)).status).toBe(401);
  });
});
