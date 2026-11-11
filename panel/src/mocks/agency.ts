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
  Node,
  Subject,
  User,
  Webhook,
  WebhookDelivery,
} from '../api/generated/model';
import { advanceDeployCycle, db, findApp, findDeployment, mockId, now } from './db';

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
  extra: Partial<Pick<Event, 'project_id' | 'incident_id' | 'request_id' | 'actor' | 'time' | 'source'>> = {},
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

// ---------------------------------------------------------------- the deploy cycle

/** What of the deploy cycle is already in the audit log and the events, `<deployment id>:<what>`. */
const recordedCycle = new Set<string>();

/** `time`, or the newest event's time when that is later: `seq` and `time` rise together. */
const notBeforeNewestEvent = (time: string): string => {
  const newest = state.events[state.events.length - 1]?.time;
  return newest !== undefined && newest > time ? newest : time;
};

/**
 * Put the deploy cycle's deployments in the logs, oldest first, each once:
 * the owner's redeploy call in the audit log, stamped when it is appended,
 * with `deployment.started`; then `deployment.succeeded` from the node the
 * rollout ended on, or `deployment.failed`, once it ends.
 */
const recordCycle = (): void => {
  for (const deploymentId of db.deployCycle?.deploymentIds ?? []) {
    const deployment = findDeployment(deploymentId);
    if (!deployment) continue;
    const appId = deployment.app_id;
    const projectId = findApp(appId)?.project_id;
    const subject = { type: 'deployment', id: deployment.id };
    const once = (what: string, write: () => void) => {
      const key = `${deployment.id}:${what}`;
      if (recordedCycle.has(key)) return;
      recordedCycle.add(key);
      write();
    };

    once('started', () => {
      appendAudit({
        actor_id: deployment.created_by_id,
        operation: 'POST /api/v1/apps/{app_id}/redeploy',
        scope: 'app:redeploy',
        target_type: 'app',
        target_id: appId,
        project_id: projectId,
        params: { app_id: appId },
      });
      emitEvent(
        'deployment.started',
        subject,
        { deployment_id: deployment.id, app_id: appId, image: deployment.image, trigger: deployment.trigger },
        {
          project_id: projectId ?? null,
          time: notBeforeNewestEvent(deployment.created_at),
          actor: { type: 'user', id: deployment.created_by_id },
        },
      );
    });
    const ended = notBeforeNewestEvent(deployment.finished_at ?? deployment.created_at);
    if (deployment.status === 'succeeded') {
      // The last instance the rollout took reports the deployment healthy.
      const lastNode = db.vms.filter((vm) => vm.app_id === appId).at(-1)?.node_id;
      once('succeeded', () =>
        emitEvent(
          'deployment.succeeded',
          subject,
          {
            deployment_id: deployment.id,
            app_id: appId,
            ...(deployment.image_digest ? { image_digest: deployment.image_digest } : {}),
          },
          {
            project_id: projectId ?? null,
            time: ended,
            ...(lastNode ? { source: { kind: 'node' as const, node_id: lastNode } } : {}),
          },
        ),
      );
    } else if (deployment.status === 'failed') {
      once('failed', () =>
        emitEvent(
          'deployment.failed',
          subject,
          { deployment_id: deployment.id, app_id: appId, phase: deployment.phase ?? 'pull', error: deployment.error ?? '' },
          { project_id: projectId ?? null, time: ended, actor: { type: 'system', id: 'system' } },
        ),
      );
    }
  }
};

/**
 * Bring the deploy cycle up to `nowMs` and record what it did. Every mock
 * request passes through here before its handler answers.
 */
export const tickDeployCycle = (nowMs: number = Date.now()): void => {
  advanceDeployCycle(nowMs);
  if (!db.owner) return;
  agency();
  recordCycle();
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
    created_at: minutesAgo(30 * 60),
    updated_at: minutesAgo(30 * 60),
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
      created_at: minutesAgo(30 * 60 - 2),
      revoked_at: null,
      last_used_at: minutesAgo(8),
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

  // The incident the demo restart belongs to. The event data is the
  // documented `app.down` payload (events.schema.json, data_app_down).
  // blog has two replicas, so the primary emits it when the edge has every
  // instance out of rotation: `edge_health`, with the edge's node.
  const edgeId = db.edgeBackends[appId]?.[0]?.edge_node_id ?? db.node.id;
  const appDown = emitEvent(
    'app.down',
    { type: 'app', id: appId },
    {
      app_id: appId,
      node_id: edgeId,
      check: 'edge_health',
      failures: 3,
      last_error: 'GET http://10.0.0.21:80/: request canceled after 2s',
    },
    { project_id: projectId, time: minutesAgo(15), actor: { type: 'node', id: edgeId } },
  );
  const incident: Incident = {
    id: mockId('inc'),
    status: 'open',
    title: 'blog is down',
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
    created_at: minutesAgo(28 * 60),
    updated_at: minutesAgo(28 * 60),
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
      webhook_id: webhook.id,
      event_id: appDown.id,
      attempt: 1,
      status: 'delivered',
      response_code: 200,
      error: null,
      next_attempt_at: null,
      test: true,
      created_at: minutesAgo(12),
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
    expires_at: minutesAhead(7 * 24 * 60 - 29 * 60),
    max_uses: null,
    uses: 10,
    status: 'active',
    incident_id: null,
    approval_id: null,
    on_behalf_of: human.id,
    created_by_type: 'user',
    created_by_id: human.id,
    reason: 'Read access to the blog project for on-call triage',
    created_at: minutesAgo(29 * 60),
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
      'Restart did not bring blog back and the image is gone from the registry. Deleting the app lets a clean deploy replace it.',
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

  // The audit trail behind the records above, oldest first: `seq` and
  // `time` rise together, as they do in a real log.
  const agentCall = (
    agent: Agent,
    minutes: number,
    operation: string,
    scope: string,
    extra: Partial<AuditInput> = {},
  ) =>
    appendAudit({
      time: minutesAgo(minutes),
      actor_type: 'agent',
      actor_id: agent.id,
      agent_model: agent.model,
      on_behalf_of: human.id,
      channel: 'mcp',
      operation,
      scope,
      params: {},
      ...extra,
    });

  // weekly-reporter: created, granted, a few reads, one denied request, disabled.
  appendAudit({
    time: minutesAgo(4_000),
    operation: 'POST /api/v1/agents',
    scope: 'agent:write',
    target_type: 'agent',
    target_id: reporter.id,
    params: { name: reporter.name, model: reporter.model, owner_user_id: human.id },
  });
  appendAudit({
    time: minutesAgo(3_998),
    operation: 'POST /api/v1/grants',
    scope: 'grant:write',
    target_type: 'grant',
    target_id: expired.id,
    params: { agent_id: reporter.id, scopes: expired.scopes, selector: expired.selector, ttl_s: 3_300 * 60, max_uses: 10 },
  });
  agentCall(reporter, 3_990, 'list_audit', 'audit:read', { grant_id: expired.id, target_type: 'cluster', target_id: 'cluster' });
  agentCall(reporter, 2_012, 'list_events', 'event:read', { grant_id: expired.id, target_type: 'cluster', target_id: 'cluster' });
  agentCall(reporter, 2_000, 'request_approval', 'approval:create', {
    target_type: 'approval',
    target_id: denied.id,
    project_id: projectId,
    params: { scope: denied.scope, target: denied.target, operation: denied.operation, params: denied.params },
    justification: denied.justification,
  });
  appendAudit({
    time: minutesAgo(1_990),
    operation: 'POST /api/v1/approvals/{approval_id}/deny',
    scope: 'approval:decide',
    target_type: 'approval',
    target_id: denied.id,
    project_id: projectId,
    params: { approval_id: denied.id, note: denied.decision_note },
  });

  // on-call-restarter: created, keyed, granted read access, wired to a webhook.
  appendAudit({
    time: minutesAgo(30 * 60),
    operation: 'POST /api/v1/agents',
    scope: 'agent:write',
    target_type: 'agent',
    target_id: restarter.id,
    params: { name: restarter.name, model: restarter.model, owner_user_id: human.id },
  });
  appendAudit({
    time: minutesAgo(30 * 60 - 2),
    operation: 'POST /api/v1/agents/{agent_id}/keys',
    scope: 'agent:write',
    target_type: 'agent',
    target_id: restarter.id,
    params: { agent_id: restarter.id, key: '[redacted]' },
  });
  appendAudit({
    time: minutesAgo(29 * 60),
    operation: 'POST /api/v1/grants',
    scope: 'grant:write',
    target_type: 'grant',
    target_id: standard.id,
    params: { agent_id: restarter.id, scopes: standard.scopes, selector: standard.selector, ttl_s: 7 * 86_400 },
  });
  appendAudit({
    time: minutesAgo(28 * 60),
    operation: 'POST /api/v1/webhooks',
    scope: 'webhook:write',
    target_type: 'webhook',
    target_id: webhook.id,
    params: { name: webhook.name, url: webhook.url, secret: '[redacted]' },
  });

  // Its routine reads of blog under the read grant: a health check every
  // 45 minutes since the grant, and a closer look now and then. With them the
  // log runs past one page of Activity, so "Load older" shows in mock mode.
  const onBlog = { target_type: 'app', target_id: appId, project_id: projectId, params: { app_id: appId }, grant_id: standard.id };
  const reads: [number, string, string][] = [
    [22.4 * 60, 'list_apps', 'app:read'],
    [22.3 * 60, 'get_app', 'app:read'],
    [18.9 * 60, 'get_app', 'app:read'],
    [12.6 * 60, 'list_apps', 'app:read'],
    [12.5 * 60, 'get_app', 'app:read'],
    [12.4 * 60, 'get_app_logs', 'app:logs'],
    [6.8 * 60, 'get_app', 'app:read'],
    [3.3 * 60, 'list_apps', 'app:read'],
    [3.2 * 60, 'get_app', 'app:read'],
    [1.4 * 60, 'get_app', 'app:read'],
  ];
  for (let minutes = 27.6 * 60; minutes > 60; minutes -= 45) reads.push([minutes, 'get_app', 'app:read']);

  // Appended in time order, with the reporter disabled in between.
  const timeline: [number, () => void][] = [
    [
      600,
      () =>
        appendAudit({
          time: minutesAgo(600),
          operation: 'PATCH /api/v1/agents/{agent_id}',
          scope: 'agent:write',
          target_type: 'agent',
          target_id: reporter.id,
          params: { agent_id: reporter.id, disabled: true },
        }),
    ],
    ...reads.map(([minutes, operation, scope]): [number, () => void] => [
      minutes,
      () =>
        agentCall(restarter, minutes, operation, scope, operation === 'list_apps' ? { grant_id: standard.id, params: {} } : onBlog),
    ]),
  ];
  for (const [, append] of timeline.sort((left, right) => right[0] - left[0])) append();

  // The incident: Quoro opens it and wakes the agent with an incident grant.
  appendAudit({
    time: minutesAgo(15),
    actor_type: 'system',
    actor_id: 'incidents',
    channel: 'system',
    operation: 'open_incident',
    target_type: 'incident',
    target_id: incident.id,
    project_id: projectId,
    params: { subject: incident.subject, trigger_event_id: appDown.id },
    trigger_event_id: appDown.id,
    incident_id: incident.id,
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
  const inIncident = {
    target_type: 'app',
    target_id: appId,
    project_id: projectId,
    params: { app_id: appId },
    trigger_event_id: appDown.id,
    incident_id: incident.id,
  };
  // The M3 demo record: the agent restarted the app through MCP.
  agentCall(restarter, 12, 'restart_app', 'app:restart', {
    ...inIncident,
    justification: 'blog is down after a failed health check. Restarting is the first step of the runbook.',
    grant_id: incidentGrant.id,
    change_seq: 412,
  });
  agentCall(restarter, 10.5, 'get_app_logs', 'app:logs', {
    ...inIncident,
    params: { app_id: appId, tail: 200 },
    justification: 'blog did not come back after the restart. Reading why.',
    grant_id: incidentGrant.id,
  });
  // A refusal: deleting needs a person's approval first.
  agentCall(restarter, 9, 'delete_app', 'app:delete', {
    ...inIncident,
    justification: 'The image is gone from the registry; checking whether deleting is allowed.',
    result: 'refused',
    result_detail: 'approval_required',
  });
  agentCall(restarter, 8, 'request_approval', 'approval:create', {
    ...inIncident,
    target_type: 'approval',
    target_id: pending.id,
    params: { scope: pending.scope, target: pending.target, operation: pending.operation, params: pending.params },
    justification: pending.justification,
    grant_id: incidentGrant.id,
  });

  recordNodeSessions();
};

/**
 * The machines' own events, as the seed has them: each machine but the
 * primary enrolled with a join token, then every one connected a second after
 * it was created. The primary was set up, not enrolled. They are emitted in
 * time order, so `time` never goes back as `seq` rises.
 */
const recordNodeSessions = (): void => {
  const sessions: { at: number; type: EventType; node: Node; data: Record<string, unknown> }[] = [];
  for (const node of [db.node, ...db.extraNodes]) {
    const at = Date.parse(node.created_at);
    if (node.cp_status !== 'primary') {
      sessions.push({
        at,
        type: 'node.enrolled',
        node,
        data: { node_id: node.id, name: node.name, roles: [...node.roles], address: node.address ?? null },
      });
    }
    sessions.push({
      at: at + 1_000,
      type: 'node.connected',
      node,
      data: { node_id: node.id, ...(node.agent_version ? { agent_version: node.agent_version } : {}), log_seq: node.log_seq },
    });
  }
  for (const { at, type, node, data } of sessions.sort((left, right) => left.at - right.at)) {
    emitEvent(type, { type: 'node', id: node.id }, data, {
      time: new Date(at).toISOString(),
      actor: { type: 'node', id: node.id },
    });
  }
};

/**
 * Mock-mode console helper: the named machine's session drops and it connects
 * again at once, as after a restart of its agent, so a new node event arrives
 * while the Nodes page is open. False when no machine has that name.
 */
export const reconnectNode = (name: string): boolean => {
  const node = [db.node, ...db.extraNodes].find((candidate) => candidate.name === name);
  if (!node || node.status === 'removed') return false;
  agency();
  const subject = { type: 'node', id: node.id };
  const actor = { type: 'node' as const, id: node.id };
  emitEvent('node.disconnected', subject, { node_id: node.id, last_seen_at: now(), reason: 'session_closed' }, { actor });
  emitEvent(
    'node.connected',
    subject,
    { node_id: node.id, ...(node.agent_version ? { agent_version: node.agent_version } : {}), log_seq: node.log_seq },
    { actor },
  );
  return true;
};
