import { describe, expect, it } from 'vitest';
import type { AuditRecord, AuditRecordActorType } from '../api/generated/model';
import {
  actorWords,
  AUDIT_DETAIL_FIELDS,
  auditFieldListIsComplete,
  buildVerifyBody,
  channelWords,
  isConnectorBroken,
  resultLamp,
  resultWords,
} from './audit';

/** Every field of the format, in the order of audit-log-format.md section 2. */
const fullRecord: AuditRecord = {
  seq: 7,
  time: '2026-11-02T15:42:07Z',
  actor_type: 'agent',
  actor_id: 'agt_01JC9Z6Q5W8N3K4T7R2M1XHB0D',
  agent_model: 'example-model-id',
  on_behalf_of: 'usr_01JC9Z6Q5W8N3K4T7R2M1XHB0E',
  channel: 'mcp',
  operation: 'restart_app',
  scope: 'app:restart',
  target_type: 'app',
  target_id: 'app_01JC9Z6Q5W8N3K4T7R2M1XHB0F',
  project_id: 'prj_01JC9Z6Q5W8N3K4T7R2M1XHB0G',
  params: { app_id: 'app_01JC9Z6Q5W8N3K4T7R2M1XHB0F' },
  justification: 'app down; restarting its instance',
  trigger_event_id: 'evt_01JC9Z6Q5W8N3K4T7R2M1XHB0J',
  incident_id: 'inc_01JC9Z6Q5W8N3K4T7R2M1XHB0H',
  grant_id: 'grt_01JC9Z6Q5W8N3K4T7R2M1XHB0K',
  approval_id: 'apr_01JC9Z6Q5W8N3K4T7R2M1XHB0Q',
  request_id: 'req_01JC9Z6Q5W8N3K4T7R2M1XHB0M',
  result: 'ok',
  result_detail: null,
  change_seq: 4077,
  prev_hash: '0'.repeat(64),
  hash: 'a'.repeat(64),
};

describe('audit result lamps', () => {
  it('maps results to the panel lamps', () => {
    expect(resultLamp('ok')).toBe('run');
    expect(resultLamp('refused')).toBe('shift');
    expect(resultLamp('error')).toBe('fault');
  });

  it('names every result', () => {
    expect(resultWords).toEqual({ ok: 'OK', refused: 'Refused', error: 'Error' });
  });
});

describe('audit detail field list', () => {
  it('covers every AuditRecord field exactly once, in format order', () => {
    expect(auditFieldListIsComplete).toBe(true);
    const listed = AUDIT_DETAIL_FIELDS.flatMap((field) => field.fields);
    expect(listed).toEqual(Object.keys(fullRecord));
    expect(new Set(listed).size).toBe(listed.length);
  });

  it('gives every row a plain label', () => {
    expect(AUDIT_DETAIL_FIELDS.map((field) => field.label)).toEqual([
      'Sequence',
      'Time',
      'Actor',
      'Agent model',
      'Agent owner',
      'On behalf of',
      'Channel',
      'Operation',
      'Scope',
      'Target',
      'Project',
      'Parameters',
      'Justification',
      'Triggering event',
      'Incident',
      'Grant',
      'Approval',
      'Request id',
      'Result',
      'Result detail',
      'Change seq',
      'Previous hash',
      'Hash',
    ]);
    for (const field of AUDIT_DETAIL_FIELDS) {
      expect(field.label.trim()).not.toBe('');
    }
  });
});

describe('audit actor and channel words', () => {
  it('says which kind of identity acted', () => {
    const kinds: AuditRecordActorType[] = ['user', 'agent', 'node', 'system'];
    expect(kinds.map((kind) => actorWords[kind])).toEqual([
      'Person',
      'Agent',
      'Node',
      'System',
    ]);
  });

  it('says how the call arrived', () => {
    expect(channelWords).toEqual({
      api: 'API',
      panel: 'Signed-in session',
      mcp: 'MCP',
      cli: 'CLI',
      system: 'System',
    });
  });
});

describe('verify body', () => {
  it('omits bounds that were left empty', () => {
    expect(buildVerifyBody(undefined, undefined)).toEqual({ ok: true, body: {} });
    expect(buildVerifyBody(null, null)).toEqual({ ok: true, body: {} });
  });

  it('sends only the bounds that were filled', () => {
    expect(buildVerifyBody(1, undefined)).toEqual({ ok: true, body: { from_seq: 1 } });
    expect(buildVerifyBody(undefined, 12)).toEqual({ ok: true, body: { to_seq: 12 } });
    expect(buildVerifyBody(3, 9)).toEqual({
      ok: true,
      body: { from_seq: 3, to_seq: 9 },
    });
  });

  it('refuses a range that runs backwards', () => {
    expect(buildVerifyBody(9, 3)).toEqual({
      ok: false,
      error: 'From seq must not be after To seq.',
    });
  });

  it('refuses bounds that are not a whole number of 1 or more', () => {
    expect(buildVerifyBody(0, undefined).ok).toBe(false);
    expect(buildVerifyBody(undefined, 1.5).ok).toBe(false);
    expect(buildVerifyBody(Number.NaN, undefined).ok).toBe(false);
  });
});

describe('rail connectors', () => {
  it('breaks only the connector into the failed sequence', () => {
    expect(isConnectorBroken(3, 3)).toBe(true);
    expect(isConnectorBroken(3, 4)).toBe(false);
    expect(isConnectorBroken(3, 2)).toBe(false);
    expect(isConnectorBroken(null, 3)).toBe(false);
    expect(isConnectorBroken(undefined, 3)).toBe(false);
  });

  it('never breaks a connector above the first entry', () => {
    expect(isConnectorBroken(1, 1)).toBe(false);
  });
});
