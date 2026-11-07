import { describe, expect, it } from 'vitest';
import type { Approval } from '../api/generated/model';
import {
  approvalConfirm,
  approvalTitle,
  approvalWants,
  callText,
  approvalLamp,
  approvalMeter,
  approveWords,
  approvalWords,
  decidedAt,
  decidedNewestFirst,
  decisionBody,
  isDestructiveScope,
  paramRows,
  pendingByExpiry,
} from './approvals';

const approval = (overrides: Partial<Approval>): Approval => ({
  id: 'apr_01JC9Z6Q5W0000000000000000',
  agent_id: 'agt_01JC9Z6Q5W0000000000000000',
  scope: 'app:delete',
  target: { type: 'app', id: 'app_01JC9Z6Q5W0000000000000000' },
  operation: 'delete_app',
  params: {},
  params_hash: 'a'.repeat(64),
  justification: 'The image is gone; a clean deploy replaces it.',
  incident_id: null,
  status: 'pending',
  expires_at: '2026-11-02T12:52:00.000Z',
  decided_by: null,
  decided_at: null,
  decision_note: null,
  grant_id: null,
  created_at: '2026-11-02T11:52:00.000Z',
  ...overrides,
});

describe('approvals', () => {
  it('maps each status to its lamp and word', () => {
    expect(approvalLamp('pending')).toBe('shift');
    expect(approvalLamp('approved')).toBe('run');
    expect(approvalLamp('denied')).toBe('fault');
    expect(approvalLamp('expired')).toBe('idle');
    expect(approvalWords.pending).toBe('Waiting');
    expect(approvalWords.expired).toBe('Expired');
  });

  it('marks exactly the destructive scopes', () => {
    expect(isDestructiveScope('app:delete')).toBe(true);
    expect(isDestructiveScope('vm:delete')).toBe(true);
    expect(isDestructiveScope('project:delete')).toBe(true);
    expect(isDestructiveScope('node:remove')).toBe(true);
    expect(isDestructiveScope('app:restart')).toBe(false);
    expect(isDestructiveScope('approval:create')).toBe(false);
  });

  it('flattens params by key, printing nested values as indented JSON', () => {
    expect(paramRows({})).toEqual([]);
    const rows = paramRows({
      app_id: 'app_1',
      force: true,
      attempts: 2,
      nothing: null,
      limits: { cpu: 1, mem: 512 },
      labels: ['a', 'b'],
    });
    expect(rows.map((row) => row.key)).toEqual([
      'app_id',
      'attempts',
      'force',
      'labels',
      'limits',
      'nothing',
    ]);
    expect(rows[0]).toEqual({ key: 'app_id', text: 'app_1', nested: false });
    expect(rows[1]?.text).toBe('2');
    expect(rows[2]?.text).toBe('true');
    expect(rows[3]).toEqual({ key: 'labels', text: '[\n  "a",\n  "b"\n]', nested: true });
    expect(rows[4]).toEqual({
      key: 'limits',
      text: '{\n  "cpu": 1,\n  "mem": 512\n}',
      nested: true,
    });
    expect(rows[5]).toEqual({ key: 'nothing', text: 'null', nested: false });
  });

  it('says exactly what approving and denying do', () => {
    const request = approval({});
    expect(approvalConfirm('approve', request)).toBe(
      'Issues a one-time grant for app:delete on this app only, valid for 10 minutes. The agent must repeat the exact call.',
    );
    expect(approvalConfirm('deny', request)).toBe(
      "No grant is issued; the agent's retry is refused.",
    );
    expect(
      approvalConfirm('approve', {
        scope: 'node:remove',
        target: { type: 'node', id: 'nod_1' },
      }),
    ).toContain('on this node only');
  });

  it('sends no body field when the note is blank', () => {
    expect(decisionBody(undefined)).toEqual({});
    expect(decisionBody(null)).toEqual({});
    expect(decisionBody('')).toEqual({});
    expect(decisionBody('   \n ')).toEqual({});
    expect(decisionBody('Keep the blog project.')).toEqual({ note: 'Keep the blog project.' });
    expect(decisionBody('  trimmed  ')).toEqual({ note: 'trimmed' });
  });

  it('puts the next deadline first in the pending queue', () => {
    const later = approval({ id: 'apr_later', expires_at: '2026-11-02T13:52:00.000Z' });
    const sooner = approval({ id: 'apr_sooner', expires_at: '2026-11-02T12:12:00.000Z' });
    const middle = approval({ id: 'apr_middle', expires_at: '2026-11-02T12:32:00.000Z' });
    expect(pendingByExpiry([later, sooner, middle]).map((item) => item.id)).toEqual([
      'apr_sooner',
      'apr_middle',
      'apr_later',
    ]);
  });

  it('puts the newest decision first and dates an expired request by its expiry', () => {
    const denied = approval({
      id: 'apr_denied',
      status: 'denied',
      decided_at: '2026-11-02T11:58:00.000Z',
    });
    const approved = approval({
      id: 'apr_approved',
      status: 'approved',
      decided_at: '2026-11-02T12:02:00.000Z',
    });
    const expired = approval({
      id: 'apr_expired',
      status: 'expired',
      expires_at: '2026-11-02T12:10:00.000Z',
    });
    expect(decidedNewestFirst([denied, expired, approved]).map((item) => item.id)).toEqual([
      'apr_expired',
      'apr_approved',
      'apr_denied',
    ]);
    expect(decidedAt(expired)).toBe('2026-11-02T12:10:00.000Z');
    expect(decidedAt(denied)).toBe('2026-11-02T11:58:00.000Z');
  });
});

describe('approvalMeter', () => {
  const now = Date.UTC(2026, 10, 7, 17, 35);
  const at = (minutes: number) => new Date(now + minutes * 60_000).toISOString();

  it('drains from filing to expiry', () => {
    const meter = approvalMeter({ created_at: at(-11), expires_at: at(49) }, now);
    expect(meter.words).toBe('Expires in 49 min');
    expect(meter.share).toBeCloseTo(49 / 60);
    expect(meter.over).toBe(false);
  });

  it('is empty once the request lapsed', () => {
    expect(approvalMeter({ created_at: at(-70), expires_at: at(-10) }, now)).toEqual({
      share: 0,
      words: 'Expired',
      over: true,
    });
  });
});

describe('approveWords', () => {
  it('says what the red button approves', () => {
    expect(approveWords('app:delete')).toBe('Approve deletion');
    expect(approveWords('backup:restore')).toBe('Approve restore');
    expect(approveWords('app:restart')).toBe('Approve');
  });
});

describe('the exact call', () => {
  it('writes the operation with every parameter, as recorded', () => {
    expect(callText({ operation: 'delete_app', params: { app_id: 'app_01JC9Z6Q5W0000000000000000' } })).toBe(
      'delete_app { app_id: "app_01JC9Z6Q5W0000000000000000" }',
    );
    expect(callText({ operation: 'scale', params: { replicas: 2, nodes: ['a', 'b'], force: null } })).toBe(
      'scale { replicas: 2, nodes: ["a","b"], force: null }',
    );
    expect(callText({ operation: 'list_apps', params: {} })).toBe('list_apps {}');
  });

  it('names what the request asks for', () => {
    const names: Record<string, string> = { app_1: 'blog', prj_1: 'docs' };
    const name = (_type: string, id: string) => names[id];
    expect(approvalTitle({ scope: 'app:delete', target: { type: 'app', id: 'app_1' } }, name)).toBe('Delete blog');
    expect(approvalTitle({ scope: 'project:delete', target: { type: 'project', id: 'prj_1' } }, name)).toBe(
      'Delete project docs',
    );
    expect(approvalWants({ scope: 'app:delete', target: { type: 'app', id: 'app_1' } }, name)).toBe(
      'wants to delete the app blog',
    );
    expect(approvalTitle({ scope: 'app:delete', target: { type: 'app', id: 'app_01JC9Z6Q5W0000000000000000' } }, name)).toBe(
      'Delete app_01JC9Z…',
    );
  });
});
