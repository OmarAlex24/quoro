import { setupServer } from 'msw/node';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { AuditList, ListAuditParams } from '../api/generated/model';
import { agency, appendAudit } from '../mocks/agency';
import { db, mockId, now as nowIso } from '../mocks/db';
import { auditHandlers } from '../mocks/handlers-audit';
import { callBuckets } from './agents';
import { CALL_PAGES, readAgentCalls, type AuditLister } from './use-agent-calls';

const server = setupServer(...auditHandlers);

const BASE = 'http://panel.test/api/v1';
const HOUR = 3_600_000;
const NOW = Date.parse('2026-11-09T12:00:00Z');

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
      created_at: nowIso(),
    };
  }
  db.sessionUserId = db.owner.id;
  agency();
});

/** One new agent's calls, `hoursAgo` before NOW each, appended oldest first as a real log is. */
const callsAt = (hoursAgo: readonly number[]): string => {
  const agentId = mockId('agt');
  for (const hours of [...hoursAgo].sort((left, right) => right - left)) {
    appendAudit({
      time: new Date(NOW - hours * HOUR).toISOString(),
      actor_type: 'agent',
      actor_id: agentId,
      channel: 'mcp',
      operation: 'get_app',
      scope: 'app:read',
      params: {},
    });
  }
  return agentId;
};

/** `listAudit` through the mock, remembering what each call asked for. */
const recording = (): { list: AuditLister; asked: ListAuditParams[] } => {
  const asked: ListAuditParams[] = [];
  const list: AuditLister = async (params) => {
    asked.push(params);
    const query = new URLSearchParams(Object.entries(params).map(([key, value]) => [key, String(value)]));
    const response = await fetch(`${BASE}/audit?${query}`);
    expect(response.status).toBe(200);
    return (await response.json()) as AuditList;
  };
  return { list, asked };
};

const hoursOf = (times: readonly string[]) => times.map((time) => (NOW - Date.parse(time)) / HOUR);

describe('readAgentCalls', () => {
  it('reads newest first and stops at the page that reaches past 24 h', async () => {
    const agentId = callsAt([2, 8, 14, 17, 20, 23, 26, 30, 36, 40, 44]);
    const { list, asked } = recording();

    const calls = await readAgentCalls(agentId, { now: NOW, list, pageSize: 3 });

    // Three pages: [2, 8, 14], [17, 20, 23], then [26, 30, 36], whose oldest is past the window.
    expect(asked).toHaveLength(3);
    expect(hoursOf(calls.records.map((record) => record.time))).toEqual([2, 8, 14, 17, 20, 23, 26, 30, 36]);
    expect(calls.capped).toBe(false);
    const seqs = calls.records.map((record) => record.seq);
    expect(seqs).toEqual([...seqs].sort((left, right) => right - left));
    expect(callBuckets(calls.records.map((record) => record.time), NOW).total).toBe(6);
  });

  it('asks for the agent newest first and follows the cursor toward older records', async () => {
    const agentId = callsAt([1, 2, 3, 30]);
    const { list, asked } = recording();

    await readAgentCalls(agentId, { now: NOW, list, pageSize: 2 });

    expect(asked[0]).toEqual({ actor_id: agentId, order: 'desc', limit: 2 });
    expect(asked[1]).toMatchObject({ actor_id: agentId, order: 'desc', limit: 2 });
    expect(Number(asked[1]?.cursor)).toBeGreaterThan(0);
  });

  it('stops at a record exactly 24 h old', async () => {
    const agentId = callsAt([1, 24, 25]);
    const { list, asked } = recording();

    const calls = await readAgentCalls(agentId, { now: NOW, list, pageSize: 2 });

    expect(asked).toHaveLength(1);
    expect(calls.capped).toBe(false);
    expect(callBuckets(calls.records.map((record) => record.time), NOW).total).toBe(1);
  });

  it('stops when the log ends inside the window', async () => {
    const agentId = callsAt([1, 5, 9]);
    const { list, asked } = recording();

    const calls = await readAgentCalls(agentId, { now: NOW, list, pageSize: 2 });

    expect(asked).toHaveLength(2);
    expect(calls.records).toHaveLength(3);
    expect(calls.capped).toBe(false);
  });

  it('says capped when the pages run out inside the window', async () => {
    const agentId = callsAt(Array.from({ length: 2 * CALL_PAGES + 3 }, (_, index) => 1 + index));
    const { list, asked } = recording();

    const calls = await readAgentCalls(agentId, { now: NOW, list, pageSize: 2 });

    expect(asked).toHaveLength(CALL_PAGES);
    expect(calls.records).toHaveLength(2 * CALL_PAGES);
    expect(calls.capped).toBe(true);
  });

  it('reads nothing for an agent with no calls', async () => {
    const { list, asked } = recording();

    const calls = await readAgentCalls(mockId('agt'), { now: NOW, list });

    expect(asked).toHaveLength(1);
    expect(calls).toEqual({ records: [], capped: false });
  });
});
