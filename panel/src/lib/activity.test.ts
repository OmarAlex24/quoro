import { describe, expect, it } from 'vitest';
import type { AuditRecord } from '../api/generated/model';
import {
  capitalize,
  dayLabel,
  groupByDay,
  recordAction,
  recordSentence,
  resultTag,
  segmentsText,
  type RecordContext,
} from './activity';

const AGENT = 'agt_01JD5Z8Q4XK3M9V2T7B6N1R0SD';
const USER = 'usr_01JD5Z8Q4XK3M9V2T7B6N1R0SE';
const APP = 'app_01JD5Z8Q4XK3M9V2T7B6N1R0SA';
const APPROVAL = 'apr_01JD5Z8Q4XK3M9V2T7B6N1R0SF';
const GRANT = 'grt_01JD5Z8Q4XK3M9V2T7B6N1R0SG';
const INCIDENT = 'inc_01JD5Z8Q4XK3M9V2T7B6N1R0SH';

const NAMES: Record<string, string> = {
  [AGENT]: 'on-call-restarter',
  [USER]: 'Omar',
  [APP]: 'blog',
  [INCIDENT]: 'blog is down',
};

const ctx: RecordContext = {
  name: (_type, id) => NAMES[id],
  approval: (id) =>
    id === APPROVAL
      ? { scope: 'app:delete', target: { type: 'app', id: APP }, status: 'pending', agent_id: AGENT }
      : undefined,
  grant: (id) =>
    id === GRANT
      ? {
          agent_id: AGENT,
          kind: 'incident',
          scopes: ['app:read', 'app:restart'],
          selector: { apps: [APP] },
          created_at: '2026-11-07T17:17:00Z',
          expires_at: '2026-11-07T18:17:00Z',
        }
      : undefined,
};

const record = (patch: Partial<AuditRecord>): AuditRecord => ({
  seq: 7,
  time: '2026-11-07T17:20:00Z',
  actor_type: 'agent',
  actor_id: AGENT,
  channel: 'mcp',
  operation: 'restart_app',
  scope: 'app:restart',
  target_type: 'app',
  target_id: APP,
  params: { app_id: APP },
  request_id: 'req_01JD5Z8Q4XK3M9V2T7B6N1R0SJ',
  result: 'ok',
  prev_hash: '0'.repeat(64),
  hash: '1'.repeat(64),
  ...patch,
});

const say = (patch: Partial<AuditRecord>) => segmentsText(recordSentence(record(patch), ctx));

describe('recordSentence', () => {
  it('says an agent call by name, with the channel', () => {
    expect(say({})).toBe('on-call-restarter restarted blog via MCP');
  });

  it('says a refused call as an attempt', () => {
    expect(say({ operation: 'delete_app', scope: 'app:delete', result: 'refused' })).toBe(
      'on-call-restarter tried to delete blog via MCP',
    );
  });

  it('reads the same operation as an endpoint template', () => {
    expect(say({ operation: 'POST /api/v1/apps/{app_id}/restart', channel: 'api' })).toBe(
      'on-call-restarter restarted blog via the API',
    );
  });

  it('counts the log lines a read asked for', () => {
    expect(say({ operation: 'get_app_logs', params: { app_id: APP, tail: 200 } })).toBe(
      'on-call-restarter read the last 200 log lines of blog via MCP',
    );
  });

  it('says what a request for approval asks', () => {
    expect(
      say({ operation: 'request_approval', target_type: 'approval', target_id: APPROVAL, scope: 'approval:create' }),
    ).toBe('on-call-restarter asked to delete blog via MCP');
  });

  it('says a person’s decision with the agent and what it wanted', () => {
    expect(
      say({
        actor_type: 'user',
        actor_id: USER,
        channel: 'panel',
        operation: 'POST /api/v1/approvals/{approval_id}/deny',
        target_type: 'approval',
        target_id: APPROVAL,
      }),
    ).toBe('Omar denied on-call-restarter’s request to delete blog in the panel');
  });

  it('says what a person created, from the parameters', () => {
    expect(
      say({
        actor_type: 'user',
        actor_id: USER,
        channel: 'panel',
        operation: 'POST /api/v1/agents',
        target_type: 'agent',
        target_id: AGENT,
        params: { name: 'on-call-restarter' },
      }),
    ).toBe('Omar created the agent on-call-restarter in the panel');
    expect(
      say({
        actor_type: 'user',
        actor_id: USER,
        channel: 'panel',
        operation: 'POST /api/v1/grants',
        target_type: 'grant',
        target_id: GRANT,
        params: { agent_id: AGENT, scopes: ['app:read', 'app:logs'], selector: { projects: ['blog'] } },
      }),
    ).toBe('Omar gave on-call-restarter a grant to read apps on project blog in the panel');
  });

  it('says what Quoro did from the target, not the operation name', () => {
    expect(
      say({ actor_type: 'system', actor_id: 'incidents', channel: 'system', operation: 'whatever', target_type: 'grant', target_id: GRANT }),
    ).toBe('Quoro gave on-call-restarter an incident grant for 1 hour');
    expect(
      say({ actor_type: 'system', actor_id: 'incidents', channel: 'system', operation: 'open', target_type: 'incident', target_id: INCIDENT }),
    ).toBe('Quoro opened an incident blog is down');
  });

  it('shows an operation it does not know as recorded', () => {
    const segments = recordSentence(record({ operation: 'rotate_widgets' }), ctx);
    expect(segmentsText(segments)).toBe('on-call-restarter called rotate_widgets on blog via MCP');
    expect(segments.find((segment) => segment.text === 'rotate_widgets')?.code).toBe(true);
  });

  it('falls back to ids it cannot name', () => {
    const unnamed = record({ actor_id: 'agt_01JD5Z8Q4XK3M9V2T7B6N1R0ZZ', target_id: 'app_01JD5Z8Q4XK3M9V2T7B6N1R0ZZ' });
    expect(segmentsText(recordSentence(unnamed, {}))).toBe('agt_01JD5Z… restarted app app_01JD5Z… via MCP');
  });

  it('starts a story step with a capital', () => {
    expect(segmentsText(capitalize(recordAction(record({}), ctx)))).toBe('Restarted blog');
  });
});

describe('resultTag', () => {
  it('names the refusal reason', () => {
    expect(resultTag(record({ result: 'refused', result_detail: 'approval_required' }))).toEqual({
      tone: 'fault',
      words: 'Refused · needs approval',
    });
    expect(resultTag(record({ result: 'refused', result_detail: 'mystery' })).words).toBe('Refused · mystery');
    expect(resultTag(record({ result: 'error', result_detail: 'internal' })).words).toBe('Error · internal');
  });

  it('says Waiting for a request that still waits', () => {
    expect(
      resultTag(record({ operation: 'request_approval', target_type: 'approval', target_id: APPROVAL }), ctx),
    ).toEqual({ tone: 'shift', words: 'Waiting' });
    expect(resultTag(record({})).words).toBe('OK');
  });
});

describe('days', () => {
  const now = new Date(2026, 10, 7, 18, 0).getTime();

  it('says today and yesterday in the reader’s zone', () => {
    expect(dayLabel(new Date(2026, 10, 7, 0, 5).toISOString(), now)).toBe('Today');
    expect(dayLabel(new Date(2026, 10, 6, 23, 55).toISOString(), now)).toBe('Yesterday');
    expect(dayLabel(new Date(2026, 10, 3, 12, 0).toISOString(), now)).not.toMatch(/Today|Yesterday/);
  });

  it('groups records in their order, one run per day', () => {
    const times = [new Date(2026, 10, 6, 9).toISOString(), new Date(2026, 10, 6, 10).toISOString(), new Date(2026, 10, 7, 9).toISOString()];
    const groups = groupByDay(times.map((time) => ({ time })), now);
    expect(groups.map((group) => [group.label, group.records.length])).toEqual([
      ['Yesterday', 2],
      ['Today', 1],
    ]);
  });
});
