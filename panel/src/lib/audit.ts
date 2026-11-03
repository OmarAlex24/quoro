import type {
  AuditRecord,
  AuditRecordActorType,
  AuditRecordChannel,
  AuditRecordResult,
  AuditVerificationRequest,
} from '../api/generated/model';
import type { LampTone } from './lamp';

/** Lamp tone for an audit result: ok burns run, refused shift, error fault. */
export const resultLamp = (result: AuditRecordResult): LampTone => {
  switch (result) {
    case 'ok':
      return 'run';
    case 'refused':
      return 'shift';
    case 'error':
      return 'fault';
  }
};

export const resultWords: Record<AuditRecordResult, string> = {
  ok: 'OK',
  refused: 'Refused',
  error: 'Error',
};

/** What kind of identity acted, said plainly. */
export const actorWords: Record<AuditRecordActorType, string> = {
  user: 'Person',
  agent: 'Agent',
  node: 'Node',
  system: 'System',
};

export const channelWords: Record<AuditRecordChannel, string> = {
  api: 'API',
  panel: 'Panel',
  mcp: 'MCP',
  cli: 'CLI',
  system: 'System',
};

/** The special renderings the detail needs; everything else is plain text. */
export type AuditDetailFieldKey =
  | 'seq'
  | 'time'
  | 'actor'
  | 'agent_model'
  | 'agent_owner'
  | 'on_behalf_of'
  | 'channel'
  | 'operation'
  | 'scope'
  | 'target'
  | 'project'
  | 'params'
  | 'justification'
  | 'trigger_event'
  | 'incident'
  | 'grant'
  | 'approval'
  | 'request_id'
  | 'result'
  | 'result_detail'
  | 'change_seq'
  | 'prev_hash'
  | 'hash';

export interface AuditDetailField {
  readonly key: AuditDetailFieldKey;
  readonly label: string;
  /** The record fields this row shows; empty for a value derived from another call. */
  readonly fields: readonly (keyof AuditRecord)[];
}

/**
 * The detail panel's rows, in the order of audit-log-format.md section 2.
 * Agent owner is derived from the agent (it is not a record field), so it
 * carries no field of its own.
 */
export const AUDIT_DETAIL_FIELDS = [
  { key: 'seq', label: 'Sequence', fields: ['seq'] },
  { key: 'time', label: 'Time', fields: ['time'] },
  { key: 'actor', label: 'Actor', fields: ['actor_type', 'actor_id'] },
  { key: 'agent_model', label: 'Agent model', fields: ['agent_model'] },
  { key: 'agent_owner', label: 'Agent owner', fields: [] },
  { key: 'on_behalf_of', label: 'On behalf of', fields: ['on_behalf_of'] },
  { key: 'channel', label: 'Channel', fields: ['channel'] },
  { key: 'operation', label: 'Operation', fields: ['operation'] },
  { key: 'scope', label: 'Scope', fields: ['scope'] },
  { key: 'target', label: 'Target', fields: ['target_type', 'target_id'] },
  { key: 'project', label: 'Project', fields: ['project_id'] },
  { key: 'params', label: 'Parameters', fields: ['params'] },
  { key: 'justification', label: 'Justification', fields: ['justification'] },
  { key: 'trigger_event', label: 'Triggering event', fields: ['trigger_event_id'] },
  { key: 'incident', label: 'Incident', fields: ['incident_id'] },
  { key: 'grant', label: 'Grant', fields: ['grant_id'] },
  { key: 'approval', label: 'Approval', fields: ['approval_id'] },
  { key: 'request_id', label: 'Request id', fields: ['request_id'] },
  { key: 'result', label: 'Result', fields: ['result'] },
  { key: 'result_detail', label: 'Result detail', fields: ['result_detail'] },
  { key: 'change_seq', label: 'Change seq', fields: ['change_seq'] },
  { key: 'prev_hash', label: 'Previous hash', fields: ['prev_hash'] },
  { key: 'hash', label: 'Hash', fields: ['hash'] },
] as const satisfies readonly AuditDetailField[];

type ListedAuditField = (typeof AUDIT_DETAIL_FIELDS)[number]['fields'][number];
type MissingAuditField = Exclude<keyof AuditRecord, ListedAuditField>;

/** Compile-time proof that the ordered list names every AuditRecord field. */
export const auditFieldListIsComplete: MissingAuditField extends never ? true : false = true;

export type VerifyBodyResult =
  | { ok: true; body: AuditVerificationRequest }
  | { ok: false; error: string };

/**
 * Build the verify request from the two optional inputs. Empty bounds are
 * omitted (the API verifies the whole log); a bound that is not a whole number
 * of 1 or more, or a range that runs backwards, is refused before the call.
 */
export const buildVerifyBody = (
  from: number | null | undefined,
  to: number | null | undefined,
): VerifyBodyResult => {
  if (from !== null && from !== undefined && (!Number.isInteger(from) || from < 1)) {
    return { ok: false, error: 'From seq must be a whole number of 1 or more.' };
  }
  if (to !== null && to !== undefined && (!Number.isInteger(to) || to < 1)) {
    return { ok: false, error: 'To seq must be a whole number of 1 or more.' };
  }
  if (from !== null && from !== undefined && to !== null && to !== undefined && from > to) {
    return { ok: false, error: 'From seq must not be after To seq.' };
  }

  const body: AuditVerificationRequest = {};
  if (from !== null && from !== undefined) body.from_seq = from;
  if (to !== null && to !== undefined) body.to_seq = to;
  return { ok: true, body };
};

/** A record's time in the operator's local zone; the raw value when unreadable. */
export const localTime = (iso: string): string => {
  const at = new Date(iso);
  if (Number.isNaN(at.getTime())) return iso;
  return at.toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
};

/**
 * Whether the rail connector into `seq` must be drawn broken. The connector
 * enters the first record that failed verification; the links above stay
 * intact.
 */
export const isConnectorBroken = (
  brokenAtSeq: number | null | undefined,
  seq: number,
): boolean =>
  typeof brokenAtSeq === 'number' && Number.isInteger(brokenAtSeq) && brokenAtSeq === seq && seq > 1;
