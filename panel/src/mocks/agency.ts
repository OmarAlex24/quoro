/**
 * Shared mock state for the agency screens: agents and their keys, grants,
 * approvals, incidents, events, the audit log and webhooks. Mock mode only.
 *
 * The audit log is a real hash chain (audit-log-format.md, section 5): each
 * `hash` is the SHA-256 of the canonical record without `hash`, absent
 * optional fields omitted, and `prev_hash` links to the record before. The
 * mock's verify recomputes it, so `quoroMock.tamperAudit(seq)` in the browser
 * console produces a real broken link at exactly that `seq`.
 */
import type {
  Agent,
  AgentKey,
  Approval,
  AuditRecord,
  AuditVerificationResult,
  Event,
  EventType,
  Grant,
  Incident,
  Subject,
  User,
  Webhook,
  WebhookDelivery,
} from '../api/generated/model';
import { db, mockId, now } from './db';

// ---------------------------------------------------------------- SHA-256

const K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);

/** Synchronous SHA-256 hex of a UTF-8 string, so handlers stay simple. */
export const sha256Hex = (text: string): string => {
  const bytes = new TextEncoder().encode(text);
  const bitLength = bytes.length * 8;
  const padded = new Uint8Array((((bytes.length + 9 + 63) >> 6) << 6));
  padded.set(bytes);
  padded[bytes.length] = 0x80;
  const view = new DataView(padded.buffer);
  view.setUint32(padded.length - 8, Math.floor(bitLength / 0x1_0000_0000));
  view.setUint32(padded.length - 4, bitLength >>> 0);

  const h = new Uint32Array([
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
  ]);
  const w = new Uint32Array(64);
  const rotr = (x: number, n: number) => (x >>> n) | (x << (32 - n));

  for (let offset = 0; offset < padded.length; offset += 64) {
    for (let i = 0; i < 16; i += 1) w[i] = view.getUint32(offset + i * 4);
    for (let i = 16; i < 64; i += 1) {
      const w15 = w[i - 15] ?? 0;
      const w2 = w[i - 2] ?? 0;
      const s0 = rotr(w15, 7) ^ rotr(w15, 18) ^ (w15 >>> 3);
      const s1 = rotr(w2, 17) ^ rotr(w2, 19) ^ (w2 >>> 10);
      w[i] = ((w[i - 16] ?? 0) + s0 + (w[i - 7] ?? 0) + s1) >>> 0;
    }
    let [a, b, c, d, e, f, g, hh] = [h[0]!, h[1]!, h[2]!, h[3]!, h[4]!, h[5]!, h[6]!, h[7]!];
    for (let i = 0; i < 64; i += 1) {
      const s1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
      const ch = (e & f) ^ (~e & g);
      const t1 = (hh + s1 + ch + (K[i] ?? 0) + (w[i] ?? 0)) >>> 0;
      const s0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
      const maj = (a & b) ^ (a & c) ^ (b & c);
      const t2 = (s0 + maj) >>> 0;
      hh = g;
      g = f;
      f = e;
      e = (d + t1) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (t1 + t2) >>> 0;
    }
    h[0] = (h[0]! + a) >>> 0;
    h[1] = (h[1]! + b) >>> 0;
    h[2] = (h[2]! + c) >>> 0;
    h[3] = (h[3]! + d) >>> 0;
    h[4] = (h[4]! + e) >>> 0;
    h[5] = (h[5]! + f) >>> 0;
    h[6] = (h[6]! + g) >>> 0;
    h[7] = (h[7]! + hh) >>> 0;
  }
  return Array.from(h, (word) => word.toString(16).padStart(8, '0')).join('');
};

/** Sorted-key JSON, enough of RFC 8785 for the mock's own records. */
export const canonicalJson = (value: unknown): string => {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, item]) => item !== undefined)
    .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0));
  return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${canonicalJson(item)}`).join(',')}}`;
};

const ZERO_HASH = '0'.repeat(64);

// ---------------------------------------------------------------- state

export interface AgencyState {
  agents: Agent[];
  keys: AgentKey[];
  grants: Grant[];
  approvals: Approval[];
  incidents: Incident[];
  events: Event[];
  audit: AuditRecord[];
  webhooks: Webhook[];
  deliveries: WebhookDelivery[];
}

const state: AgencyState = {
  agents: [],
  keys: [],
  grants: [],
  approvals: [],
  incidents: [],
  events: [],
  audit: [],
  webhooks: [],
  deliveries: [],
};

let seededFor: string | null = null;

const owner = (): User => {
  if (!db.owner) throw new Error('agency mock used before setup');
  return db.owner;
};

/**
 * The agency state, seeded on first use after setup so the seeded records can
 * name the owner who set the cluster up. Call it after the session check.
 */
export const agency = (): AgencyState => {
  if (db.owner && seededFor !== db.owner.id) {
    seededFor = db.owner.id;
    seedAgency();
  }
  return state;
};

export const findAgent = (agentId: string): Agent | undefined =>
  state.agents.find((agent) => agent.id === agentId);

/**
 * An agent's keys, newest first, revoked keys included. Keys minted in the same
 * millisecond keep the later one first, so a just-created key leads the list.
 */
export const agentKeys = (agentId: string): AgentKey[] =>
  state.keys
    .map((key, index) => ({ key, index }))
    .filter(({ key }) => key.agent_id === agentId)
    .sort(
      (left, right) =>
        right.key.created_at.localeCompare(left.key.created_at) ||
        right.index - left.index,
    )
    .map(({ key }) => key);

// ---------------------------------------------------------------- audit and events

/** What a handler passes: everything but the chain fields, which are computed here. */
export type AuditInput = Omit<
  AuditRecord,
  'seq' | 'time' | 'prev_hash' | 'hash' | 'request_id' | 'actor_type' | 'actor_id' | 'channel' | 'result'
> &
  Partial<Pick<AuditRecord, 'actor_type' | 'actor_id' | 'channel' | 'result' | 'time' | 'request_id'>>;

/** Drop null and undefined optional fields: absent, never null (audit-log-format.md, section 2). */
const omitAbsent = <T extends object>(record: T): T =>
  Object.fromEntries(
    Object.entries(record).filter(([, value]) => value !== null && value !== undefined),
  ) as T;

/** Hash of a record as stored: SHA-256 of the canonical record without `hash`. */
export const auditHash = (record: AuditRecord): string => {
  const { hash: _hash, ...rest } = record;
  return sha256Hex(canonicalJson(rest));
};

/**
 * Append one audit record. Defaults: the signed-in owner acting through the
 * panel, result `ok`. Agent records must pass `actor_type: 'agent'`,
 * `actor_id`, `agent_model` and `on_behalf_of` themselves.
 */
export const appendAudit = (input: AuditInput): AuditRecord => {
  const previous = state.audit[state.audit.length - 1];
  const draft = omitAbsent({
    actor_type: 'user' as const,
    actor_id: db.owner?.id ?? 'system',
    channel: 'panel' as const,
    result: 'ok' as const,
    ...input,
    seq: (previous?.seq ?? 0) + 1,
    time: input.time ?? now(),
    request_id: input.request_id ?? mockId('req'),
    prev_hash: previous?.hash ?? ZERO_HASH,
    hash: '',
  }) as AuditRecord;
  draft.hash = auditHash(draft);
  state.audit.push(draft);
  return draft;
};

/**
 * Re-walk `[from, to]` as audit-log-format.md section 6 does: a seq gap, then
 * the record's own hash, then its link to the record before. The first
 * failure is the broken seq; `checked` counts the records verified before it.
 */
export const verifyAuditChain = (from?: number, to?: number): AuditVerificationResult => {
  const records = state.audit;
  const last = records[records.length - 1];
  const firstSeq = Math.max(1, from ?? 1);
  const lastSeq = Math.min(last?.seq ?? 1, to ?? last?.seq ?? 1);
  const headHash = records.find((record) => record.seq === lastSeq)?.hash ?? ZERO_HASH;
  let prev = firstSeq === 1 ? ZERO_HASH : (records.find((r) => r.seq === firstSeq - 1)?.hash ?? ZERO_HASH);
  let expectSeq = firstSeq;
  let checked = 0;

  const broken = (seq: number, reason: 'seq_gap' | 'hash_mismatch' | 'prev_mismatch'): AuditVerificationResult => {
    emitEvent('audit.chain_broken', { type: 'cluster', id: 'cluster' }, { broken_at_seq: seq, reason });
    return {
      ok: false,
      checked,
      first_seq: firstSeq,
      last_seq: lastSeq,
      head_hash: headHash,
      broken_at_seq: seq,
      reason,
    };
  };

  for (const record of records) {
    if (record.seq < firstSeq || record.seq > lastSeq) continue;
    if (record.seq !== expectSeq) return broken(record.seq, 'seq_gap');
    if (auditHash(record) !== record.hash) return broken(record.seq, 'hash_mismatch');
    if (record.prev_hash !== prev) return broken(record.seq, 'prev_mismatch');
    prev = record.hash;
    expectSeq += 1;
    checked += 1;
  }
  return {
    ok: true,
    checked,
    first_seq: firstSeq,
    last_seq: lastSeq,
    head_hash: headHash,
  };
};

/** Append one event to the event log. */
export const emitEvent = (
  type: EventType,
  subject: Subject,
  data: Record<string, unknown> = {},
  extra: Partial<Pick<Event, 'project_id' | 'incident_id' | 'request_id' | 'actor' | 'time'>> = {},
): Event => {
  const previous = state.events[state.events.length - 1];
  const event: Event = {
    id: mockId('evt'),
    seq: (previous?.seq ?? 0) + 1,
    type,
    time: extra.time ?? now(),
    source: { kind: 'control_plane' },
    subject,
    data,
    ...extra,
  };
  state.events.push(event);
  return event;
};

// ---------------------------------------------------------------- console helpers

/** Edit one stored record without rehashing it, as a local tamper would. */
export const tamperAudit = (seq: number): boolean => {
  const record = state.audit.find((candidate) => candidate.seq === seq);
  if (!record) return false;
  record.params = { ...record.params, edited_by_hand: true };
  return true;
};

/** Switch the signed-in owner's role, to see the panel as an operator or viewer. */
export const setRole = (role: User['role']): void => {
  if (db.owner) db.owner.role = role;
};

// ---------------------------------------------------------------- seed

const minutesAgo = (minutes: number): string =>
  new Date(Date.now() - minutes * 60_000).toISOString();
const minutesAhead = (minutes: number): string =>
  new Date(Date.now() + minutes * 60_000).toISOString();

const seedAgency = (): void => {
  const human = owner();
  const blogApp = db.apps.find((app) => app.name === 'blog');
  const blogProject = db.projects.find((project) => project.slug === 'blog');
  const appId = blogApp?.id ?? mockId('app');
  const projectId = blogProject?.id ?? null;

  const restarter: Agent = {
    id: mockId('agt'),
    name: 'on-call-restarter',
    model: 'claude-sonnet-5-5',
    owner_user_id: human.id,
    description: 'Woken by app.down; restarts the app and resolves the incident.',
    disabled: false,
    created_at: minutesAgo(180),
    updated_at: minutesAgo(180),
  };
  const reporter: Agent = {
    id: mockId('agt'),
    name: 'weekly-reporter',
    model: 'claude-haiku-5-5',
    owner_user_id: human.id,
    description: 'Reads the audit log and events once a week and writes a summary.',
    disabled: true,
    created_at: minutesAgo(4_000),
    updated_at: minutesAgo(600),
  };
  state.agents.push(restarter, reporter);

  state.keys.push(
    {
      id: mockId('akey'),
      agent_id: restarter.id,
      created_at: minutesAgo(178),
      revoked_at: null,
      last_used_at: minutesAgo(12),
    },
    {
      id: mockId('akey'),
      agent_id: restarter.id,
      created_at: minutesAgo(3_900),
      revoked_at: minutesAgo(1_000),
      last_used_at: minutesAgo(1_100),
    },
    {
      id: mockId('akey'),
      agent_id: reporter.id,
      created_at: minutesAgo(3_990),
      revoked_at: minutesAgo(600),
      last_used_at: minutesAgo(1_500),
    },
  );

  // The incident the demo restart belongs to.
  const appDown = emitEvent(
    'app.down',
    { type: 'app', id: appId },
    { app_id: appId, reason: 'health check failed 3 times' },
    { project_id: projectId, time: minutesAgo(15), actor: { type: 'node', id: db.node.id } },
  );
  const incident: Incident = {
    id: mockId('inc'),
    status: 'open',
    title: 'blog is down: health check failed 3 times',
    trigger_event_id: appDown.id,
    subject: { type: 'app', id: appId },
    project_id: projectId,
    opened_at: minutesAgo(15),
    resolved_at: null,
    resolved_by_type: null,
    resolved_by_id: null,
    resolution_note: null,
  };
  state.incidents.push(incident);
  emitEvent(
    'incident.opened',
    { type: 'app', id: appId },
    { incident_id: incident.id },
    { project_id: projectId, incident_id: incident.id, time: minutesAgo(15) },
  );

  const webhook: Webhook = {
    id: mockId('whk'),
    name: 'Wake the on-call agent',
    url: 'https://agents.example.com/hooks/quoro',
    event_types: ['app.down', 'incident.opened'],
    selector: { projects: ['blog'] },
    enabled: true,
    wake: {
      agent_id: restarter.id,
      grant_scopes: ['app:read', 'app:logs', 'app:restart', 'incident:read', 'incident:resolve', 'approval:create'],
      grant_ttl_s: 3600,
      grant_max_uses: null,
    },
    created_by: human.id,
    created_at: minutesAgo(170),
    updated_at: minutesAgo(170),
  };
  const notifier: Webhook = {
    id: mockId('whk'),
    name: 'Chat notifications',
    url: 'https://hooks.example.com/services/quoro',
    event_types: [],
    selector: { all: true },
    enabled: false,
    created_by: human.id,
    created_at: minutesAgo(2_000),
    updated_at: minutesAgo(900),
  };
  state.webhooks.push(webhook, notifier);
  state.deliveries.push(
    {
      id: mockId('whd'),
      webhook_id: webhook.id,
      event_id: appDown.id,
      attempt: 1,
      status: 'delivered',
      response_code: 202,
      error: null,
      next_attempt_at: null,
      test: false,
      created_at: minutesAgo(15),
    },
    {
      id: mockId('whd'),
      webhook_id: notifier.id,
      event_id: appDown.id,
      attempt: 3,
      status: 'failed',
      response_code: 503,
      error: 'receiver answered 503 Service Unavailable',
      next_attempt_at: minutesAhead(25),
      test: false,
      created_at: minutesAgo(5),
    },
  );

  const standard: Grant = {
    id: mockId('grt'),
    agent_id: restarter.id,
    kind: 'standard',
    scopes: ['app:read', 'app:logs'],
    selector: { projects: ['blog'] },
    expires_at: minutesAhead(60 * 20),
    max_uses: null,
    uses: 4,
    status: 'active',
    incident_id: null,
    approval_id: null,
    on_behalf_of: human.id,
    created_by_type: 'user',
    created_by_id: human.id,
    reason: 'Read access to the blog project for on-call triage',
    created_at: minutesAgo(176),
    revoked_at: null,
    revoked_by: null,
  };
  const incidentGrant: Grant = {
    id: mockId('grt'),
    agent_id: restarter.id,
    kind: 'incident',
    scopes: webhook.wake?.grant_scopes ?? [],
    selector: { apps: [appId] },
    expires_at: minutesAhead(45),
    max_uses: null,
    uses: 1,
    status: 'active',
    incident_id: incident.id,
    approval_id: null,
    on_behalf_of: human.id,
    created_by_type: 'system',
    created_by_id: 'system',
    reason: `Incident ${incident.id} woke the agent through webhook ${webhook.name}`,
    created_at: minutesAgo(15),
    revoked_at: null,
    revoked_by: null,
  };
  const expired: Grant = {
    id: mockId('grt'),
    agent_id: reporter.id,
    kind: 'standard',
    scopes: ['audit:read', 'event:read'],
    selector: { all: true },
    expires_at: minutesAgo(700),
    max_uses: 10,
    uses: 3,
    status: 'expired',
    incident_id: null,
    approval_id: null,
    on_behalf_of: human.id,
    created_by_type: 'user',
    created_by_id: human.id,
    reason: 'Weekly summary of the audit log',
    created_at: minutesAgo(4_000),
    revoked_at: null,
    revoked_by: null,
  };
  state.grants.push(standard, incidentGrant, expired);
  emitEvent('grant.created', { type: 'grant', id: incidentGrant.id }, { grant_id: incidentGrant.id }, {
    incident_id: incident.id,
    time: minutesAgo(15),
  });

  const pending: Approval = {
    id: mockId('apr'),
    agent_id: restarter.id,
    scope: 'app:delete',
    target: { type: 'app', id: appId },
    operation: 'delete_app',
    params: { app_id: appId },
    params_hash: sha256Hex(canonicalJson({ app_id: appId })),
    justification:
      'Restart did not bring blog back and the image is gone from the registry; deleting the app so a clean deploy can replace it.',
    incident_id: incident.id,
    status: 'pending',
    expires_at: minutesAhead(52),
    decided_by: null,
    decided_at: null,
    decision_note: null,
    grant_id: null,
    created_at: minutesAgo(8),
  };
  const denied: Approval = {
    id: mockId('apr'),
    agent_id: reporter.id,
    scope: 'project:delete',
    target: { type: 'project', id: projectId ?? mockId('prj') },
    operation: 'DELETE /api/v1/projects/{project_id}',
    params: { project_id: projectId },
    params_hash: sha256Hex(canonicalJson({ project_id: projectId })),
    justification: 'The project had no traffic for a week.',
    incident_id: null,
    status: 'denied',
    expires_at: minutesAgo(1_940),
    decided_by: human.id,
    decided_at: minutesAgo(1_990),
    decision_note: 'Low traffic is not a reason to delete it.',
    grant_id: null,
    created_at: minutesAgo(2_000),
  };
  state.approvals.push(pending, denied);
  emitEvent('approval.requested', { type: 'approval', id: pending.id }, { approval_id: pending.id }, {
    incident_id: incident.id,
    time: minutesAgo(8),
    actor: { type: 'agent', id: restarter.id },
  });

  // The audit trail behind the records above, oldest first.
  appendAudit({
    time: minutesAgo(180),
    operation: 'POST /api/v1/agents',
    scope: 'agent:write',
    target_type: 'agent',
    target_id: restarter.id,
    params: { name: restarter.name, model: restarter.model, owner_user_id: human.id },
  });
  appendAudit({
    time: minutesAgo(178),
    operation: 'POST /api/v1/agents/{agent_id}/keys',
    scope: 'agent:write',
    target_type: 'agent',
    target_id: restarter.id,
    params: { agent_id: restarter.id, key: '[redacted]' },
  });
  appendAudit({
    time: minutesAgo(176),
    operation: 'POST /api/v1/grants',
    scope: 'grant:write',
    target_type: 'grant',
    target_id: standard.id,
    params: { agent_id: restarter.id, scopes: standard.scopes, selector: standard.selector, ttl_s: 86_400 },
  });
  appendAudit({
    time: minutesAgo(170),
    operation: 'POST /api/v1/webhooks',
    scope: 'webhook:write',
    target_type: 'webhook',
    target_id: webhook.id,
    params: { name: webhook.name, url: webhook.url, secret: '[redacted]' },
  });
  appendAudit({
    time: minutesAgo(15),
    actor_type: 'system',
    actor_id: 'incidents',
    channel: 'system',
    operation: 'issue_incident_grant',
    target_type: 'grant',
    target_id: incidentGrant.id,
    params: { incident_id: incident.id, webhook_id: webhook.id },
    trigger_event_id: appDown.id,
    incident_id: incident.id,
  });
  // The M3 demo record: an agent restarted the app through MCP.
  appendAudit({
    time: minutesAgo(12),
    actor_type: 'agent',
    actor_id: restarter.id,
    agent_model: restarter.model,
    on_behalf_of: human.id,
    channel: 'mcp',
    operation: 'restart_app',
    scope: 'app:restart',
    target_type: 'app',
    target_id: appId,
    project_id: projectId,
    params: { app_id: appId },
    justification: `app down per incident ${incident.id}; restarting its instance`,
    trigger_event_id: appDown.id,
    incident_id: incident.id,
    grant_id: incidentGrant.id,
    change_seq: 412,
  });
  // A refusal: the same agent tried to delete without approval.
  appendAudit({
    time: minutesAgo(9),
    actor_type: 'agent',
    actor_id: restarter.id,
    agent_model: restarter.model,
    on_behalf_of: human.id,
    channel: 'mcp',
    operation: 'delete_app',
    scope: 'app:delete',
    target_type: 'app',
    target_id: appId,
    project_id: projectId,
    params: { app_id: appId },
    justification: 'restart did not help; checking whether deleting is allowed',
    trigger_event_id: appDown.id,
    incident_id: incident.id,
    result: 'refused',
    result_detail: 'approval_required',
  });
  appendAudit({
    time: minutesAgo(8),
    actor_type: 'agent',
    actor_id: restarter.id,
    agent_model: restarter.model,
    on_behalf_of: human.id,
    channel: 'mcp',
    operation: 'request_approval',
    scope: 'approval:create',
    target_type: 'approval',
    target_id: pending.id,
    project_id: projectId,
    params: { scope: pending.scope, target: pending.target, operation: pending.operation, params: pending.params },
    justification: pending.justification,
    trigger_event_id: appDown.id,
    incident_id: incident.id,
    grant_id: incidentGrant.id,
  });
};
