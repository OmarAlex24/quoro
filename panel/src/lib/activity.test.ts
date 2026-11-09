import { describe, expect, it } from 'vitest';
import type { AuditRecord } from '../api/generated/model';
import {
  capitalize,
  channelPhrase,
  createdName,
  dayLabel,
  clockTime,
  groupByDay,
  groupReads,
  isPlainRead,
  namesFromRecords,
  recordAction,
  recordTargets,
  readsSentence,
  readsWhen,
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
    ).toBe('Omar denied on-call-restarter’s request to delete blog while signed in');
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
    ).toBe('Omar created the agent on-call-restarter while signed in');
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
    ).toBe('Omar gave on-call-restarter a grant to read apps on project blog while signed in');
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

describe('issue 637: what the record can and cannot tell', () => {
  const say = (patch: Partial<AuditRecord>, context: RecordContext = ctx) =>
    segmentsText(recordSentence(record(patch), context));

  it('says "signed in" for a session call: a script with the cookie records the same channel as the panel', () => {
    expect(channelPhrase('panel')).toBe('while signed in');
    expect(channelPhrase('api')).toBe('via the API');
    expect(
      say({ actor_type: 'user', actor_id: USER, channel: 'panel', operation: 'POST /api/v1/agents', target_type: 'agent', target_id: AGENT, params: { name: 'x' } }),
    ).not.toContain('in the panel');
  });

  it('names every scope of a grant the plain phrase cannot say', () => {
    const grantRecord = (scopes: string[]) =>
      say({
        actor_type: 'user',
        actor_id: USER,
        channel: 'panel',
        operation: 'POST /api/v1/grants',
        target_type: 'grant',
        target_id: GRANT,
        params: { agent_id: AGENT, scopes, selector: { projects: ['approval-demo'] } },
      });
    expect(grantRecord(['app:read', 'approval:create', 'approval:read'])).toBe(
      `${NAMES[USER]} gave on-call-restarter a grant for app:read, approval:create and approval:read on project approval-demo while signed in`,
    );
    expect(grantRecord(['app:read', 'app:restart'])).toBe(
      `${NAMES[USER]} gave on-call-restarter a grant to read and restart apps on project approval-demo while signed in`,
    );
    expect(grantRecord(['app:read', 'approval:create', 'approval:read', 'event:read', 'audit:read'])).toBe(
      `${NAMES[USER]} gave on-call-restarter a grant for app:read and 4 more on project approval-demo while signed in`,
    );
  });

  it('sets the scopes of such a grant in code type', () => {
    const segments = recordSentence(
      record({
        actor_type: 'user',
        actor_id: USER,
        channel: 'panel',
        operation: 'POST /api/v1/grants',
        target_type: 'grant',
        target_id: GRANT,
        params: { agent_id: AGENT, scopes: ['app:read', 'approval:create'] },
      }),
      ctx,
    );
    expect(segments.filter((segment) => segment.code).map((segment) => segment.text)).toEqual([
      'app:read',
      'approval:create',
    ]);
  });

  it('says "a deleted app" for a target that is gone and whose name nothing kept', () => {
    const GONE = 'app_01JD5Z8Q4XK3M9V2T7B6N1R0SZ';
    const goneCtx: RecordContext = { ...ctx, gone: (type, id) => type === 'app' && id === GONE };
    expect(say({ operation: 'GET /api/v1/apps/{app_id}', channel: 'api', target_id: GONE }, goneCtx)).toBe(
      'on-call-restarter read a deleted app via the API',
    );
    // Still listed, or not known to be gone: the short id, as before.
    expect(say({ operation: 'GET /api/v1/apps/{app_id}', channel: 'api', target_id: GONE })).toBe(
      'on-call-restarter read app app_01JD5Z… via the API',
    );
  });

  it('keeps the name a create record gave a target', () => {
    const create = record({
      actor_type: 'user',
      actor_id: USER,
      channel: 'panel',
      operation: 'POST /api/v1/apps',
      target_type: 'app',
      target_id: 'app_01M70C9KQ6M3BRAVMXJ0G2DJAJ',
      params: { name: 'scratch', image: 'docker.io/traefik/whoami:v1.10', port: 80 },
    });
    expect(createdName(create)).toBe('scratch');
    expect(createdName({ ...create, result: 'refused' })).toBeUndefined();
    expect(createdName({ ...create, operation: 'DELETE /api/v1/apps/{app_id}' })).toBeUndefined();
    expect([...namesFromRecords([create, record({})])]).toEqual([['app_01M70C9KQ6M3BRAVMXJ0G2DJAJ', 'scratch']]);
  });

  it('lists the targets a sentence names, with the target of an approval record', () => {
    expect(
      recordTargets(
        [
          record({}),
          record({ operation: 'POST /api/v1/approvals/{approval_id}/approve', target_type: 'approval', target_id: APPROVAL }),
        ],
        ctx,
      ),
    ).toEqual([
      { type: 'app', id: APP },
      { type: 'approval', id: APPROVAL },
    ]);
  });
});

describe('issue 641: runs of the same read fold into one row', () => {
  const at = (seconds: number) => new Date(Date.parse('2026-11-09T14:45:00Z') + seconds * 1_000).toISOString();
  // As on the lab: the agent asks, polls its request every second, the owner
  // approves, the agent reads once more, then deletes with the approval.
  const poll = (seq: number) =>
    record({
      seq,
      time: at(seq * 10),
      channel: 'api',
      operation: 'GET /api/v1/approvals/{approval_id}',
      scope: 'approval:read',
      target_type: 'approval',
      target_id: APPROVAL,
      params: {},
    });
  const ask = record({
    seq: 19,
    time: at(190),
    channel: 'api',
    operation: 'POST /api/v1/approvals',
    target_type: 'approval',
    target_id: APPROVAL,
  });
  const approve = record({
    seq: 60,
    time: at(600),
    actor_type: 'user',
    actor_id: USER,
    channel: 'panel',
    operation: 'POST /api/v1/approvals/{approval_id}/approve',
    target_type: 'approval',
    target_id: APPROVAL,
    approval_id: APPROVAL,
  });
  const deleted = record({
    seq: 62,
    time: at(620),
    channel: 'api',
    operation: 'DELETE /api/v1/apps/{app_id}',
    approval_id: APPROVAL,
  });
  const ascending = [ask, ...Array.from({ length: 40 }, (_, index) => poll(20 + index)), approve, poll(61), deleted];
  const shape = (items: ReturnType<typeof groupReads>) =>
    items.map((item) => (item.kind === 'one' ? item.record.seq : `${item.records.length} reads`));

  it('folds 40 polls into one row and keeps the request, the decision and the delete', () => {
    expect(shape(groupReads(ascending))).toEqual([19, '40 reads', 60, 61, 62]);
  });

  it('folds the same way when the feed reads newest first', () => {
    const items = groupReads([...ascending].reverse());
    expect(shape(items)).toEqual([62, 61, 60, '40 reads', 19]);
    const run = items[3]!;
    expect(run.kind === 'reads' && run.key).toBe('reads-20');
  });

  it('says the run once, with how many and when', () => {
    const run = groupReads(ascending)[1]!;
    if (run.kind !== 'reads') throw new Error('expected a run');
    expect(segmentsText(readsSentence([...run.records], ctx))).toBe(
      `on-call-restarter read the request to delete blog 40 times via the API, ${clockTime(at(200))}–${clockTime(at(590))}`,
    );
    expect(readsWhen(run.records).words).toBe(`${clockTime(at(200))}–${clockTime(at(590))}`);
    expect(readsWhen([poll(20), poll(21)]).words).toBe(clockTime(at(200)));
  });

  it('never folds a write, a refusal, an error, an approval or the system', () => {
    expect(isPlainRead(poll(20))).toBe(true);
    expect(isPlainRead(record({ operation: 'get_app' }))).toBe(true);
    expect(isPlainRead(record({ operation: 'list_apps', target_type: null, target_id: null }))).toBe(true);
    expect(isPlainRead(record({ operation: 'restart_app' }))).toBe(false);
    expect(isPlainRead({ ...poll(20), result: 'refused' })).toBe(false);
    expect(isPlainRead({ ...poll(20), result: 'error' })).toBe(false);
    expect(isPlainRead({ ...poll(20), approval_id: APPROVAL })).toBe(false);
    expect(isPlainRead({ ...poll(20), actor_type: 'system' })).toBe(false);
    const refused = [20, 21, 22].map((seq) => ({ ...poll(seq), result: 'refused' as const }));
    expect(shape(groupReads(refused))).toEqual([20, 21, 22]);
  });

  it('starts a new run on another actor, channel, operation or target, and leaves a single read alone', () => {
    const other = (seq: number, patch: Partial<AuditRecord>) => ({ ...poll(seq), ...patch });
    expect(shape(groupReads([poll(1), other(2, { actor_id: 'agt_other' }), poll(3)]))).toEqual([1, 2, 3]);
    expect(shape(groupReads([poll(1), poll(2), other(3, { channel: 'mcp' }), other(4, { channel: 'mcp' })]))).toEqual([
      '2 reads',
      '2 reads',
    ]);
    expect(shape(groupReads([poll(1), other(2, { target_id: 'apr_other' })]))).toEqual([1, 2]);
    expect(shape(groupReads([poll(1), other(2, { operation: 'get_approval' })]))).toEqual([1, 2]);
    const list = (seq: number) =>
      record({ seq, operation: 'list_apps', scope: 'app:read', target_type: null, target_id: null, params: {} });
    expect(shape(groupReads([list(1), list(2), list(3)]))).toEqual(['3 reads']);
    expect(shape(groupReads([poll(5)]))).toEqual([5]);
  });

  it('says what a read of a request asks for', () => {
    expect(say({ operation: 'GET /api/v1/approvals/{approval_id}', target_type: 'approval', target_id: APPROVAL })).toBe(
      'on-call-restarter read the request to delete blog via MCP',
    );
    expect(say({ operation: 'get_approval', target_type: 'approval', target_id: 'apr_unknown' })).toBe(
      'on-call-restarter read a request via MCP',
    );
  });
});
