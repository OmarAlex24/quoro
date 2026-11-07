import { describe, expect, it } from 'vitest';
import type { Approval, AuditRecord, Grant } from '../api/generated/model';
import { segmentsText, type RecordContext } from './activity';
import { buildStory, triggerWords, type StoryInput } from './story';

const AGENT = 'agt_01JD5Z8Q4XK3M9V2T7B6N1R0SD';
const OTHER_AGENT = 'agt_01JD5Z8Q4XK3M9V2T7B6N1R0SK';
const USER = 'usr_01JD5Z8Q4XK3M9V2T7B6N1R0SE';
const APP = 'app_01JD5Z8Q4XK3M9V2T7B6N1R0SA';
const NODE = 'nod_01JD5Z8Q4XK3M9V2T7B6N1R0SN';
const APPROVAL = 'apr_01JD5Z8Q4XK3M9V2T7B6N1R0SF';
const GRANT = 'grt_01JD5Z8Q4XK3M9V2T7B6N1R0SG';
const INCIDENT = 'inc_01JD5Z8Q4XK3M9V2T7B6N1R0SH';

const T0 = Date.UTC(2026, 10, 7, 17, 17);
const at = (minutes: number) => new Date(T0 + minutes * 60_000).toISOString();

const NAMES: Record<string, string> = {
  [AGENT]: 'on-call-restarter',
  [OTHER_AGENT]: 'weekly-reporter',
  [USER]: 'Omar',
  [APP]: 'blog',
  [NODE]: 'lab-2',
};
const ctx: RecordContext = { name: (_type, id) => NAMES[id] };

let seq = 0;
const record = (patch: Partial<AuditRecord>): AuditRecord => {
  seq += 1;
  return {
    seq,
    time: at(seq),
    actor_type: 'agent',
    actor_id: AGENT,
    channel: 'mcp',
    operation: 'restart_app',
    scope: 'app:restart',
    target_type: 'app',
    target_id: APP,
    params: { app_id: APP },
    incident_id: INCIDENT,
    request_id: 'req_01JD5Z8Q4XK3M9V2T7B6N1R0SJ',
    result: 'ok',
    prev_hash: '0'.repeat(64),
    hash: '1'.repeat(64),
    ...patch,
  };
};

const grant: Grant = {
  id: GRANT,
  agent_id: AGENT,
  kind: 'incident',
  scopes: ['app:read', 'app:restart', 'app:logs', 'incident:read', 'approval:create'],
  selector: { apps: [APP] },
  expires_at: at(60),
  uses: 2,
  status: 'active',
  incident_id: INCIDENT,
  on_behalf_of: USER,
  created_by_type: 'system',
  created_by_id: 'system',
  reason: 'woken',
  created_at: at(0),
};

const approval = (patch: Partial<Approval> = {}): Approval => ({
  id: APPROVAL,
  agent_id: AGENT,
  scope: 'app:delete',
  target: { type: 'app', id: APP },
  operation: 'delete_app',
  params: { app_id: APP },
  params_hash: 'a'.repeat(64),
  justification: 'The image is gone; a clean deploy replaces the app.',
  incident_id: INCIDENT,
  status: 'pending',
  expires_at: at(67),
  created_at: at(7),
  ...patch,
});

const input = (records: AuditRecord[], patch: Partial<StoryInput> = {}): StoryInput => ({
  agentId: AGENT,
  incident: {
    id: INCIDENT,
    title: 'blog is down',
    subject: { type: 'app', id: APP },
    trigger_event: { type: 'app.down', data: { app_id: APP, node_id: NODE, check: 'health_check', failures: 1 } },
  },
  records,
  approvals: [approval()],
  grants: [grant],
  ctx,
  now: T0 + 18 * 60_000,
  ...patch,
});

const text = (records: AuditRecord[], patch?: Partial<StoryInput>) =>
  buildStory(input(records, patch)).map((step) => segmentsText(step.sentence));

describe('buildStory', () => {
  it('tells the reference incident from its records, in order', () => {
    seq = 0;
    const records = [
      record({ actor_type: 'system', actor_id: 'incidents', channel: 'system', operation: 'open_incident', target_type: 'incident', target_id: INCIDENT, params: {} }),
      record({ actor_type: 'system', actor_id: 'incidents', channel: 'system', operation: 'issue_incident_grant', target_type: 'grant', target_id: GRANT, params: {} }),
      record({ justification: 'blog is down after a failed health check.' }),
      record({ operation: 'get_app_logs', scope: 'app:logs', params: { app_id: APP, tail: 200 }, justification: 'Reading why.' }),
      record({ operation: 'delete_app', scope: 'app:delete', result: 'refused', result_detail: 'scope_missing' }),
      record({ operation: 'request_approval', scope: 'approval:create', target_type: 'approval', target_id: APPROVAL }),
    ];
    // Shuffled on the way in: the story still follows seq.
    const steps = buildStory(input([...records].reverse()));
    expect(steps.map((step) => segmentsText(step.sentence))).toEqual([
      'blog went down and Quoro opened an incident',
      'It got an incident grant for 1 hour',
      'Restarted blog',
      'Read the last 200 log lines of blog',
      'Tried to delete blog',
      'Asked you to approve deleting blog',
    ]);
    expect(steps.map((step) => step.mark)).toEqual(['opened', 'grant', 'call', 'call', 'refused', 'ask']);
  });

  it('carries the trigger, the scopes, the result and the Why line', () => {
    seq = 0;
    const steps = buildStory(
      input([
        record({ actor_type: 'system', actor_id: 'incidents', channel: 'system', operation: 'open', target_type: 'incident', target_id: INCIDENT, params: {} }),
        record({ actor_type: 'system', actor_id: 'incidents', channel: 'system', operation: 'issue', target_type: 'grant', target_id: GRANT, params: {} }),
        record({ justification: 'Restarting is the first step of the runbook.' }),
        record({ operation: 'delete_app', scope: 'app:delete', result: 'refused', result_detail: 'scope_missing' }),
      ]),
    );
    expect(steps[0]).toMatchObject({ tags: ['app.down'], note: 'health check failed on lab-2' });
    expect(steps[1]).toMatchObject({ tags: ['app:read', 'app:restart', 'app:logs'], more: 2, note: '42 min left' });
    expect(steps[2]).toMatchObject({ tag: { tone: 'run', words: 'OK' }, why: 'Restarting is the first step of the runbook.' });
    expect(steps[3]).toMatchObject({ tag: { tone: 'fault', words: 'Refused' }, lead: 'Not in its grant:', tags: ['app:delete'] });
  });

  it('ends on the waiting request, which carries the decision', () => {
    seq = 0;
    const steps = buildStory(
      input([record({ operation: 'request_approval', scope: 'approval:create', target_type: 'approval', target_id: APPROVAL })]),
    );
    expect(steps.at(-1)).toMatchObject({ mark: 'ask', pendingApprovalId: APPROVAL });
    expect(steps.at(-1)!.tag).toBeUndefined();
  });

  it('adds a waiting request the log does not show yet', () => {
    seq = 0;
    const steps = buildStory(input([record({})]));
    expect(steps).toHaveLength(2);
    expect(steps[1]).toMatchObject({ seq: null, mark: 'ask', pendingApprovalId: APPROVAL, time: at(7) });
  });

  it('tags a decided request and says who decided', () => {
    seq = 0;
    const decided = approval({ status: 'denied', decided_by: USER, decided_at: at(9) });
    const steps = buildStory(
      input(
        [
          record({ operation: 'request_approval', scope: 'approval:create', target_type: 'approval', target_id: APPROVAL }),
          record({
            actor_type: 'user',
            actor_id: USER,
            channel: 'panel',
            operation: 'POST /api/v1/approvals/{approval_id}/deny',
            scope: 'approval:decide',
            target_type: 'approval',
            target_id: APPROVAL,
            params: { approval_id: APPROVAL, note: 'Not today.' },
          }),
        ],
        { approvals: [decided], ctx: { ...ctx, approval: () => decided } },
      ),
    );
    expect(steps[0]).toMatchObject({ tag: { tone: 'fault', words: 'Denied' } });
    expect(steps[0]!.pendingApprovalId).toBeUndefined();
    expect(segmentsText(steps[1]!.sentence)).toBe('Omar denied on-call-restarter’s request to delete blog');
    expect(steps[1]).toMatchObject({ mark: 'person', note: '“Not today.”' });
  });

  it('names another actor in the incident by name', () => {
    seq = 0;
    expect(text([record({ actor_id: OTHER_AGENT })])).toEqual([
      'weekly-reporter restarted blog',
      'Asked you to approve deleting blog',
    ]);
  });

  it('opens with the title when the trigger is not an app going down', () => {
    seq = 0;
    const steps = buildStory(
      input(
        [record({ actor_type: 'system', actor_id: 'incidents', channel: 'system', operation: 'open', target_type: 'incident', target_id: INCIDENT, params: {} })],
        {
          approvals: [],
          incident: { id: INCIDENT, title: 'blog runs a vulnerable image', subject: { type: 'app', id: APP }, trigger_event: { type: 'app.vulnerable', data: {} } },
        },
      ),
    );
    expect(segmentsText(steps[0]!.sentence)).toBe('Quoro opened an incident: blog runs a vulnerable image');
    expect(steps[0]!.note).toBeUndefined();
  });
});

describe('triggerWords', () => {
  it('reads the documented app.down data', () => {
    expect(triggerWords({ type: 'app.down', data: { node_id: NODE, check: 'health_check', failures: 3 } }, ctx)).toBe(
      'health check failed 3 times on lab-2',
    );
    expect(triggerWords({ type: 'app.down', data: { node_id: NODE, check: 'vm_crashed', failures: 1 } }, ctx)).toBe(
      'its VM crashed on lab-2',
    );
    expect(triggerWords({ type: 'app.down', data: {} }, ctx)).toBeUndefined();
    expect(triggerWords(null, ctx)).toBeUndefined();
  });
});
