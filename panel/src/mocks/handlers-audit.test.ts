import { setupServer } from 'msw/node';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { AuditList, AuditRecord, EventList } from '../api/generated/model';
import { agency, appendAudit, reconnectNode } from './agency';
import { db, mockId, now } from './db';
import { auditHandlers } from './handlers-audit';

const server = setupServer(...auditHandlers);

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
  agency();
});

/** `count` records of one new agent, oldest first; every third is refused. */
const recordsOf = (count: number): { actorId: string; records: AuditRecord[] } => {
  const actorId = mockId('agt');
  const records = Array.from({ length: count }, (_, index) =>
    appendAudit({
      actor_type: 'agent',
      actor_id: actorId,
      channel: 'mcp',
      operation: 'get_app',
      scope: 'app:read',
      params: {},
      result: index % 3 === 2 ? 'refused' : 'ok',
    }),
  );
  return { actorId, records };
};

const listResponse = (params: Record<string, string | number>) =>
  fetch(
    `${BASE}/audit?${new URLSearchParams(Object.entries(params).map(([key, value]) => [key, String(value)]))}`,
  );

const list = async (params: Record<string, string | number>): Promise<AuditList> => {
  const response = await listResponse(params);
  expect(response.status).toBe(200);
  return (await response.json()) as AuditList;
};

const seqs = (page: AuditList) => page.items.map((record) => record.seq);

/** Follow `next_cursor` from the first page to the last, keeping the other parameters. */
const readAll = async (params: Record<string, string | number>): Promise<number[][]> => {
  const pages: number[][] = [];
  let cursor: string | null = null;
  do {
    const page: AuditList = await list(cursor ? { ...params, cursor } : params);
    pages.push(seqs(page));
    cursor = page.next_cursor;
  } while (cursor && pages.length < 20);
  return pages;
};

describe('audit list mock: order', () => {
  it('lists oldest first without an order, as with asc', async () => {
    const { actorId, records } = recordsOf(4);
    const all = records.map((record) => record.seq);
    expect(seqs(await list({ actor_id: actorId }))).toEqual(all);
    expect(seqs(await list({ actor_id: actorId, order: 'asc' }))).toEqual(all);
  });

  it('lists newest first with desc', async () => {
    const { actorId, records } = recordsOf(4);
    const page = await list({ actor_id: actorId, order: 'desc' });
    expect(seqs(page)).toEqual(records.map((record) => record.seq).reverse());
    expect(page.next_cursor).toBeNull();
  });

  it('opens the whole log at its newest record with desc', async () => {
    recordsOf(2);
    const log = agency().audit;
    const page = await list({ order: 'desc', limit: 5 });
    expect(seqs(page)).toEqual(log.slice(-5).map((record) => record.seq).reverse());
  });

  it('applies the filters the same way in both orders', async () => {
    const { actorId, records } = recordsOf(7);
    const refused = records.filter((record) => record.result === 'refused').map((record) => record.seq);
    expect(refused).toHaveLength(2);
    expect(seqs(await list({ actor_id: actorId, result: 'refused' }))).toEqual(refused);
    expect(seqs(await list({ actor_id: actorId, result: 'refused', order: 'desc' }))).toEqual([...refused].reverse());
  });
});

describe('audit list mock: cursor', () => {
  it('continues toward older records with desc, to the first one', async () => {
    const { actorId, records } = recordsOf(5);
    const pages = await readAll({ actor_id: actorId, order: 'desc', limit: 2 });
    const [s1, s2, s3, s4, s5] = records.map((record) => record.seq);
    expect(pages).toEqual([[s5, s4], [s3, s2], [s1]]);
  });

  it('continues toward newer records with asc, to the last one', async () => {
    const { actorId, records } = recordsOf(5);
    const pages = await readAll({ actor_id: actorId, limit: 2 });
    const [s1, s2, s3, s4, s5] = records.map((record) => record.seq);
    expect(pages).toEqual([[s1, s2], [s3, s4], [s5]]);
  });

  it('ends a page that holds the last record with a null cursor', async () => {
    const { actorId } = recordsOf(4);
    const page = await list({ actor_id: actorId, order: 'desc', limit: 4 });
    expect(page.items).toHaveLength(4);
    expect(page.next_cursor).toBeNull();
  });

  it('reads the cursor as a bound of its own direction: the tighter bound wins', async () => {
    const { actorId, records } = recordsOf(6);
    const [s1, s2, s3, s4, s5] = records.map((record) => record.seq) as [number, number, number, number, number];
    // desc: the cursor narrows before_seq, so the smaller of the two applies.
    expect(seqs(await list({ actor_id: actorId, order: 'desc', cursor: s4, before_seq: s5 + 1 }))).toEqual([s3, s2, s1]);
    expect(seqs(await list({ actor_id: actorId, order: 'desc', cursor: s5, before_seq: s3 }))).toEqual([s2, s1]);
    // asc: the cursor narrows after_seq, so the larger of the two applies.
    expect(seqs(await list({ actor_id: actorId, cursor: s2, after_seq: s1 }))).toEqual(records.slice(2).map((record) => record.seq));
    expect(seqs(await list({ actor_id: actorId, cursor: s1, after_seq: s3 }))).toEqual(records.slice(3).map((record) => record.seq));
  });
});

describe('audit list mock: window', () => {
  it('reads after_seq < seq < before_seq in both orders', async () => {
    const { actorId, records } = recordsOf(6);
    const [s1, s2, s3, s4, s5] = records.map((record) => record.seq);
    expect(seqs(await list({ actor_id: actorId, after_seq: s1!, before_seq: s5! }))).toEqual([s2, s3, s4]);
    expect(seqs(await list({ actor_id: actorId, after_seq: s1!, before_seq: s5!, order: 'desc' }))).toEqual([s4, s3, s2]);
  });

  it('pages a window newest first and stops at its lower bound', async () => {
    const { actorId, records } = recordsOf(7);
    const [s1, s2, s3, s4, s5, s6] = records.map((record) => record.seq);
    const pages = await readAll({ actor_id: actorId, order: 'desc', after_seq: s1!, before_seq: s6!, limit: 2 });
    expect(pages).toEqual([[s5, s4], [s3, s2]]);
  });

  it('answers a window that holds no seq with an empty page, not an error', async () => {
    const { actorId, records } = recordsOf(3);
    const [s1, s2] = records.map((record) => record.seq) as [number, number];
    for (const order of ['asc', 'desc']) {
      expect(await list({ actor_id: actorId, order, after_seq: s2, before_seq: s2 })).toEqual({ items: [], next_cursor: null });
      expect(await list({ actor_id: actorId, order, after_seq: s2, before_seq: s1 })).toEqual({ items: [], next_cursor: null });
    }
    expect(await list({ order: 'desc', before_seq: 1 })).toEqual({ items: [], next_cursor: null });
  });
});

describe('audit list mock: validation', () => {
  it('refuses an order other than asc or desc', async () => {
    expect((await listResponse({ order: 'newest' })).status).toBe(422);
  });

  it('refuses bounds and cursors the contract does not allow', async () => {
    expect((await listResponse({ before_seq: 0 })).status).toBe(422);
    expect((await listResponse({ after_seq: -1 })).status).toBe(422);
    expect((await listResponse({ before_seq: 'soon' })).status).toBe(422);
    expect((await listResponse({ order: 'desc', cursor: 'next' })).status).toBe(422);
  });
});

describe('the event log', () => {
  const events = async (query: string): Promise<EventList> => {
    const response = await fetch(`${BASE}/events?${query}`);
    expect(response.status).toBe(200);
    return (await response.json()) as EventList;
  };
  const NODE_TYPES = 'type=node.enrolled&type=node.connected&type=node.disconnected';

  it('holds the seeded node sessions, by type, in ascending seq', async () => {
    const page = await events(NODE_TYPES);
    expect(page.items.length).toBeGreaterThan(0);
    expect(page.items.every((event) => event.type.startsWith('node.'))).toBe(true);
    const order = page.items.map((event) => event.seq);
    expect(order).toEqual([...order].sort((left, right) => left - right));
    expect(page.items.some((event) => event.type === 'node.connected' && event.subject.id === db.node.id)).toBe(true);
  });

  it('serves only what came after a seq, and pages with a cursor', async () => {
    const before = await events(NODE_TYPES);
    const last = before.items[before.items.length - 1]!.seq;
    expect(reconnectNode(db.node.name)).toBe(true);
    expect(reconnectNode('no-such-node')).toBe(false);

    const after = await events(`${NODE_TYPES}&after_seq=${last}`);
    expect(after.items.map((event) => event.type)).toEqual(['node.disconnected', 'node.connected']);
    expect(after.items[0]!.data).toMatchObject({ node_id: db.node.id, reason: 'session_closed' });

    const first = await events(`${NODE_TYPES}&after_seq=${last}&limit=1`);
    expect(first.items).toHaveLength(1);
    expect(first.next_cursor).toBe(String(first.items[0]!.seq));
    const second = await events(`${NODE_TYPES}&after_seq=${last}&limit=1&cursor=${first.next_cursor}`);
    expect(second.items.map((event) => event.type)).toEqual(['node.connected']);
    expect(second.next_cursor).toBeNull();
  });
});
