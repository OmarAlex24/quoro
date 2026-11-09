// SPDX-License-Identifier: AGPL-3.0-only
import type {
  Approval,
  AuditRecord,
  AuditRecordChannel,
  Grant,
  Scope,
  Selector,
} from '../api/generated/model';
import { accessCovers, accessPhrase } from './access';
import { describeSelector, formatDuration, shortId } from './agency-format';

/*
 * An audit record said as a sentence: "on-call-restarter restarted blog via
 * MCP" instead of `restart_app`. The feed and the incident story share it.
 * Every word comes from the record's own fields (audit-log-format.md,
 * section 2) and from names the panel already lists; an operation the table
 * does not know is shown as it was recorded, in code type.
 */

/** One run of a sentence: plain, bold (a name), code (an operation or scope) or faint. */
export interface Segment {
  readonly text: string;
  readonly strong?: boolean;
  readonly code?: boolean;
  readonly faint?: boolean;
}

/** What the sentences need to turn ids into names; every part is optional. */
export interface RecordContext {
  /** A name for an id of this kind (`agent`, `user`, `app`, `vm`, `project`, `webhook`, `incident`, `node`). */
  readonly name?: (type: string, id: string) => string | undefined;
  readonly approval?: (id: string) => Pick<Approval, 'scope' | 'target' | 'status' | 'agent_id'> | undefined;
  readonly grant?: (
    id: string,
  ) =>
    | (Pick<Grant, 'agent_id' | 'kind' | 'scopes' | 'selector' | 'created_at' | 'expires_at'> &
        Partial<Pick<Grant, 'uses'>>)
    | undefined;
  /**
   * True when the id is known to be gone: its kind's whole list was read and
   * does not hold it (an app, VM or project deleted since). Unknown is false.
   */
  readonly gone?: (type: string, id: string) => boolean;
}

const plain = (text: string): Segment => ({ text });
const strong = (text: string): Segment => ({ text, strong: true });
const code = (text: string): Segment => ({ text, code: true });

const str = (value: unknown): string | undefined =>
  typeof value === 'string' && value.trim() !== '' ? value : undefined;

/** A name for an id, or its kind and a short id when no list holds it. */
export const nameOf = (ctx: RecordContext, type: string, id: string): string =>
  ctx.name?.(type, id) ?? `${type} ${shortId(id)}`;

/** Who acted: an agent's or a person's name, Quoro for the system, a node's name. */
export const actorName = (
  record: Pick<AuditRecord, 'actor_type' | 'actor_id'>,
  ctx: RecordContext,
): string => {
  switch (record.actor_type) {
    case 'system':
      return 'Quoro';
    case 'agent':
      return ctx.name?.('agent', record.actor_id) ?? shortId(record.actor_id);
    case 'user':
      return ctx.name?.('user', record.actor_id) ?? shortId(record.actor_id);
    case 'node':
      return ctx.name?.('node', record.actor_id) ?? shortId(record.actor_id);
  }
};

/** How the call arrived, as the end of the sentence; nothing for the system. */
export const channelPhrase = (channel: AuditRecordChannel): string | null => {
  switch (channel) {
    case 'mcp':
      return 'via MCP';
    // The server records `panel` for every call made with a session cookie
    // (README "Channels"), so a script that borrows the cookie is `panel` too.
    // The record cannot tell them apart; "signed in" is what it does know.
    case 'panel':
      return 'while signed in';
    case 'api':
      return 'via the API';
    case 'cli':
      return 'from the CLI';
    case 'system':
      return null;
  }
};

/* ---------- what an approval asks, in three grammatical forms ---------- */

const ASK: Readonly<Record<string, readonly [base: string, gerund: string, past: string]>> = {
  delete: ['delete', 'deleting', 'deleted'],
  remove: ['remove', 'removing', 'removed'],
  restore: ['restore', 'restoring', 'restored'],
  failover: ['fail over', 'failing over', 'failed over'],
  delete_quarantine: [
    'delete a quarantined copy of',
    'deleting a quarantined copy of',
    'deleted a quarantined copy of',
  ],
};

/** The verb of a scope's action: `app:delete` asks to delete, `backup:restore` to restore. */
export const scopeVerb = (scope: string): readonly [string, string, string] => {
  const verb = scope.slice(scope.indexOf(':') + 1);
  const known = ASK[verb];
  if (known) return known;
  const words = verb.replace(/_/g, ' ');
  const stem = words.endsWith('e') ? words.slice(0, -1) : words;
  return [words, `${stem}ing`, `${stem}ed`];
};

/** How a kind is said inside a sentence: `app`, `VM`, `project`. */
export const kindWord = (type: string): string => (type === 'vm' ? 'VM' : type);

/** "a deleted app": a target that is gone and whose name nothing kept. */
export const deletedWords = (type: string): string => `a deleted ${kindWord(type)}`;

/** "the app blog", "the project docs": a target said with its kind, unless it is an app or VM. */
const targetWords = (ctx: RecordContext, type: string, id: string): Segment[] => {
  const name = ctx.name?.(type, id);
  if (!name) return ctx.gone?.(type, id) ? [plain(deletedWords(type))] : [strong(`${type} ${shortId(id)}`)];
  if (type === 'app' || type === 'vm') return [strong(name)];
  return [plain(`the ${type} `), strong(name)];
};

/* ---------- the verb table ---------- */

interface Phrase {
  /** The action when it went through: "restarted". */
  readonly done: (record: AuditRecord, ctx: RecordContext) => Segment[];
  /** The action when it was refused or failed: "tried to restart". */
  readonly tried: (record: AuditRecord, ctx: RecordContext) => Segment[];
}

/** The record's own target, bold. */
const theTarget = (record: AuditRecord, ctx: RecordContext): Segment[] =>
  record.target_type && record.target_id
    ? targetWords(ctx, record.target_type, record.target_id)
    : [];

/**
 * "restarted blog" and "tried to restart blog", for a verb that acts on the
 * target. When the verb already names the kind ("created the app"), only the
 * bare name follows it.
 */
const onTarget = (base: string, past: string, named = false): Phrase => ({
  done: (record, ctx) => [plain(`${past} `), ...nameOrTarget(record, ctx, named)],
  tried: (record, ctx) => [plain(`tried to ${base} `), ...nameOrTarget(record, ctx, named)],
});

const nameOrTarget = (record: AuditRecord, ctx: RecordContext, named: boolean): Segment[] => {
  const given = str(record.params?.name);
  if (!record.target_type || !record.target_id) return given ? [strong(given)] : [];
  if (!named) return theTarget(record, ctx);
  const name = ctx.name?.(record.target_type, record.target_id) ?? given;
  return [strong(name ?? shortId(record.target_id))];
};

/** A verb with no object: "listed the apps", "read the audit log". */
const alone = (base: string, past: string): Phrase => ({
  done: () => [plain(past)],
  tried: () => [plain(`tried to ${base}`)],
});

/** A grant names this many scopes one by one; past it, the first and how many more. */
export const GRANT_SCOPES_NAMED = 3;

/**
 * "a grant to read and restart apps on project blog", from scopes and a
 * selector. The plain phrase stands only when it says every scope; otherwise
 * every scope is named ("a grant for app:read, approval:create and
 * approval:read"), or the first one and how many more past three.
 */
const grantWords = (scopes: readonly Scope[], selector: Selector | undefined): Segment[] => {
  const access = accessPhrase(scopes);
  const where = selector ? describeSelector(selector).toLowerCase() : '';
  const what: Segment[] =
    access !== null && accessCovers(scopes)
      ? [plain(` a grant to ${access}`)]
      : scopes.length === 0
        ? [plain(' a grant with no scopes')]
        : [plain(' a grant for '), ...scopeList(scopes)];
  return where ? [...what, plain(` on ${where}`)] : what;
};

/** `app:read, approval:create and approval:read`, or `app:read and 4 more`. */
const scopeList = (scopes: readonly Scope[]): Segment[] => {
  if (scopes.length > GRANT_SCOPES_NAMED) {
    return [code(scopes[0]!), plain(` and ${scopes.length - 1} more`)];
  }
  return scopes.flatMap((scope, index) => [
    ...(index === 0 ? [] : [plain(index === scopes.length - 1 ? ' and ' : ', ')]),
    code(scope),
  ]);
};

/** "1 hour", "45 minutes", "7 days"; anything uneven in the short form, "2 h 5 min". */
const durationLong = (seconds: number): string => {
  const total = Math.round(seconds);
  const plural = (count: number, unit: string) => `${count} ${unit}${count === 1 ? '' : 's'}`;
  if (total % 86_400 === 0) return plural(total / 86_400, 'day');
  if (total % 3_600 === 0 && total < 86_400) return plural(total / 3_600, 'hour');
  if (total % 60 === 0 && total < 3_600) return plural(total / 60, 'minute');
  return formatDuration(total);
};

/** The duration a grant was issued for: `expires_at` less `created_at`. */
export const grantSpan = (grant: Pick<Grant, 'created_at' | 'expires_at'>): string | null => {
  const from = Date.parse(grant.created_at);
  const to = Date.parse(grant.expires_at);
  if (Number.isNaN(from) || Number.isNaN(to) || to <= from) return null;
  return durationLong((to - from) / 1_000);
};

const approvalTarget = (record: AuditRecord, ctx: RecordContext) => {
  const id = record.target_type === 'approval' ? record.target_id : record.approval_id;
  const approval = id ? ctx.approval?.(id) : undefined;
  if (approval) return { scope: approval.scope as string, target: approval.target, agentId: approval.agent_id };
  const params = record.params ?? {};
  const scope = str(params.scope);
  const target = params.target as { type?: unknown; id?: unknown } | undefined;
  if (scope && target && typeof target.type === 'string' && typeof target.id === 'string') {
    return { scope, target: { type: target.type, id: target.id }, agentId: undefined };
  }
  return null;
};

const decision = (word: 'approved' | 'denied'): Phrase => ({
  done: (record, ctx) => {
    const asked = approvalTarget(record, ctx);
    if (!asked) return [plain(`${word} a request`)];
    const [base] = scopeVerb(asked.scope);
    const who = asked.agentId ? ctx.name?.('agent', asked.agentId) : undefined;
    return [
      plain(`${word} `),
      ...(who ? [strong(who), plain('’s request to ')] : [plain('the request to ')]),
      plain(`${base} `),
      ...targetWords(ctx, asked.target.type, asked.target.id),
    ];
  },
  tried: (record, ctx) => {
    const asked = approvalTarget(record, ctx);
    const verb = word === 'approved' ? 'approve' : 'deny';
    if (!asked) return [plain(`tried to ${verb} a request`)];
    const [base] = scopeVerb(asked.scope);
    return [plain(`tried to ${verb} the request to ${base} `), ...targetWords(ctx, asked.target.type, asked.target.id)];
  },
});

const askPhrase: Phrase = {
  done: (record, ctx) => {
    const asked = approvalTarget(record, ctx);
    if (!asked) return [plain('asked for an approval')];
    const [base] = scopeVerb(asked.scope);
    return [plain(`asked to ${base} `), ...targetWords(ctx, asked.target.type, asked.target.id)];
  },
  tried: (record, ctx) => {
    const asked = approvalTarget(record, ctx);
    if (!asked) return [plain('tried to ask for an approval')];
    const [base] = scopeVerb(asked.scope);
    return [plain(`tried to ask to ${base} `), ...targetWords(ctx, asked.target.type, asked.target.id)];
  },
};

const logsPhrase: Phrase = {
  done: (record, ctx) => {
    const tail = record.params?.tail;
    const lead = typeof tail === 'number' && tail > 0 ? `read the last ${tail} log lines of ` : 'read the logs of ';
    return [plain(lead), ...theTarget(record, ctx)];
  },
  tried: (record, ctx) => [plain('tried to read the logs of '), ...theTarget(record, ctx)],
};

const grantIssued: Phrase = {
  done: (record, ctx) => {
    const params = record.params ?? {};
    const agentId = str(params.agent_id) ?? (record.target_id ? ctx.grant?.(record.target_id)?.agent_id : undefined);
    const scopes = Array.isArray(params.scopes) ? (params.scopes as Scope[]) : [];
    const selector = (params.selector as Selector | undefined) ?? undefined;
    return [
      plain('gave '),
      strong(agentId ? nameOf(ctx, 'agent', agentId) : 'an agent'),
      ...grantWords(scopes, selector),
    ];
  },
  tried: () => [plain('tried to issue a grant')],
};

const grantRevoked: Phrase = {
  done: (record, ctx) => {
    const grant = record.target_id ? ctx.grant?.(record.target_id) : undefined;
    return grant
      ? [plain('revoked a grant of '), strong(nameOf(ctx, 'agent', grant.agent_id))]
      : [plain('revoked grant '), code(shortId(record.target_id ?? 'grant'))];
  },
  tried: () => [plain('tried to revoke a grant')],
};

const agentChanged: Phrase = {
  done: (record, ctx) => {
    const disabled = record.params?.disabled;
    const verb = disabled === true ? 'disabled' : disabled === false ? 'enabled' : 'changed';
    return [plain(`${verb} the agent `), ...nameOrTarget(record, ctx, true)];
  },
  tried: (record, ctx) => [plain('tried to change the agent '), ...nameOrTarget(record, ctx, true)],
};

const memberInvited: Phrase = {
  done: (record) => [plain('invited '), strong(str(record.params?.email) ?? str(record.params?.name) ?? 'a member')],
  tried: () => [plain('tried to invite a member')],
};

/**
 * The operations the panel can say as a sentence, by MCP tool name and by
 * endpoint template (`operation` holds either, section 2).
 */
const PHRASES: Readonly<Record<string, Phrase>> = {
  restart_app: onTarget('restart', 'restarted'),
  'POST /api/v1/apps/{app_id}/restart': onTarget('restart', 'restarted'),
  redeploy_app: onTarget('redeploy', 'redeployed'),
  'POST /api/v1/apps/{app_id}/redeploy': onTarget('redeploy', 'redeployed'),
  delete_app: onTarget('delete', 'deleted'),
  'DELETE /api/v1/apps/{app_id}': onTarget('delete', 'deleted'),
  'POST /api/v1/apps/{app_id}/start': onTarget('start', 'started'),
  'POST /api/v1/apps/{app_id}/stop': onTarget('stop', 'stopped'),
  'POST /api/v1/apps/{app_id}/failover': onTarget('fail over', 'failed over'),
  'POST /api/v1/apps/{app_id}/switchover': onTarget('switch over', 'switched over'),
  'PATCH /api/v1/apps/{app_id}': onTarget('change', 'changed'),
  'PUT /api/v1/apps/{app_id}/env': onTarget('change the variables of', 'changed the variables of'),
  'POST /api/v1/apps/{app_id}/deployments': onTarget('deploy', 'deployed'),
  'POST /api/v1/apps/{app_id}/builds': onTarget('build', 'started a build of'),
  'POST /api/v1/apps/{app_id}/domains': onTarget('add a domain to', 'added a domain to'),
  'POST /api/v1/apps/{app_id}/terminal-sessions': onTarget('open a terminal in', 'opened a terminal in'),
  'POST /api/v1/vms/{vm_id}/console-sessions': onTarget('open the console of', 'opened the console of'),
  'POST /api/v1/apps': onTarget('create the app', 'created the app', true),
  get_app: onTarget('read', 'read'),
  'GET /api/v1/apps/{app_id}': onTarget('read', 'read'),
  list_apps: alone('list the apps', 'listed the apps'),
  get_app_logs: logsPhrase,
  'GET /api/v1/apps/{app_id}/logs': logsPhrase,
  list_deployments: onTarget('list the deployments of', 'listed the deployments of'),
  get_deployment: alone('read a deployment', 'read a deployment'),
  get_cluster: alone('read the cluster', 'read the cluster'),
  list_nodes: alone('list the nodes', 'listed the nodes'),
  get_node: onTarget('read', 'read'),
  list_projects: alone('list the projects', 'listed the projects'),
  list_incidents: alone('list the incidents', 'listed the incidents'),
  get_incident: onTarget('read', 'read'),
  list_audit: alone('read the audit log', 'read the audit log'),
  list_events: alone('read the events', 'read the events'),
  get_approval: alone('read a request', 'read a request'),
  request_approval: askPhrase,
  'POST /api/v1/approvals': askPhrase,
  'POST /api/v1/approvals/{approval_id}/approve': decision('approved'),
  'POST /api/v1/approvals/{approval_id}/deny': decision('denied'),
  resolve_incident: onTarget('resolve', 'resolved'),
  'POST /api/v1/incidents/{incident_id}/resolve': onTarget('resolve', 'resolved'),
  'POST /api/v1/agents': onTarget('create the agent', 'created the agent', true),
  'PATCH /api/v1/agents/{agent_id}': agentChanged,
  'DELETE /api/v1/agents/{agent_id}': onTarget('delete the agent', 'deleted the agent', true),
  'POST /api/v1/agents/{agent_id}/keys': onTarget('make a key for', 'made a key for', true),
  'DELETE /api/v1/agents/{agent_id}/keys/{key_id}': onTarget('revoke a key of', 'revoked a key of', true),
  'POST /api/v1/grants': grantIssued,
  'POST /api/v1/grants/{grant_id}/revoke': grantRevoked,
  'POST /api/v1/webhooks': onTarget('add the webhook', 'added the webhook', true),
  'PATCH /api/v1/webhooks/{webhook_id}': onTarget('change the webhook', 'changed the webhook', true),
  'DELETE /api/v1/webhooks/{webhook_id}': onTarget('delete the webhook', 'deleted the webhook', true),
  'POST /api/v1/webhooks/{webhook_id}/test': onTarget('test the webhook', 'tested the webhook', true),
  'POST /api/v1/audit/verify': alone('verify the audit chain', 'verified the audit chain'),
  'POST /api/v1/projects': onTarget('create the project', 'created the project', true),
  'DELETE /api/v1/projects/{project_id}': onTarget('delete', 'deleted'),
  'POST /api/v1/vms': onTarget('create the VM', 'created the VM', true),
  'DELETE /api/v1/vms/{vm_id}': onTarget('delete', 'deleted'),
  'POST /api/v1/vms/{vm_id}/start': onTarget('start', 'started'),
  'POST /api/v1/vms/{vm_id}/stop': onTarget('stop', 'stopped'),
  'POST /api/v1/users': memberInvited,
  'POST /api/v1/tokens': alone('make an API token', 'made an API token'),
  'DELETE /api/v1/tokens/{token_id}': alone('revoke an API token', 'revoked an API token'),
};

/**
 * What the system did, read from the target rather than the operation name,
 * which the contract does not fix for system records: an incident opened or
 * resolved, an incident grant issued or revoked.
 */
const systemPhrase = (record: AuditRecord, ctx: RecordContext): Segment[] | null => {
  if (record.actor_type !== 'system') return null;
  const op = record.operation.toLowerCase();
  if (record.target_type === 'incident' && record.target_id) {
    const title = ctx.name?.('incident', record.target_id);
    const verb = op.includes('resolve') ? 'resolved the incident' : 'opened an incident';
    return title ? [plain(`${verb} `), strong(title)] : [plain(verb)];
  }
  if (record.target_type === 'grant' && record.target_id) {
    const grant = ctx.grant?.(record.target_id);
    if (op.includes('revoke')) {
      return grant
        ? [plain('revoked the grant of '), strong(nameOf(ctx, 'agent', grant.agent_id))]
        : [plain('revoked a grant')];
    }
    if (!grant) return [plain('issued a grant')];
    const span = grantSpan(grant);
    const kind = grant.kind === 'incident' ? 'an incident grant' : grant.kind === 'approval' ? 'an approval grant' : 'a grant';
    return [plain('gave '), strong(nameOf(ctx, 'agent', grant.agent_id)), plain(` ${kind}${span ? ` for ${span}` : ''}`)];
  }
  return null;
};

/** The predicate of the sentence: what was done, without who did it. */
export const recordAction = (record: AuditRecord, ctx: RecordContext = {}): Segment[] => {
  const system = systemPhrase(record, ctx);
  if (system) return system;
  const phrase = PHRASES[record.operation];
  const went = record.result === 'ok';
  if (phrase) return went ? phrase.done(record, ctx) : phrase.tried(record, ctx);
  // Unknown to the table: the operation as recorded, and its target.
  const target = theTarget(record, ctx);
  return [
    plain(went ? 'called ' : 'tried to call '),
    code(record.operation),
    ...(target.length > 0 ? [plain(' on '), ...target] : []),
  ];
};

/* ---------- names a deleted target keeps ---------- */

/** The call that creates each kind a sentence names by its own name (`params.name`). */
export const CREATED_BY: Readonly<Record<string, string>> = {
  app: 'POST /api/v1/apps',
  vm: 'POST /api/v1/vms',
  project: 'POST /api/v1/projects',
};

/**
 * The name a create record gave its target, from the request it recorded
 * (`params.name`), or undefined. A deleted app leaves no list to name it, but
 * the record of its creation still does.
 */
export const createdName = (
  record: Pick<AuditRecord, 'operation' | 'result' | 'target_type' | 'target_id' | 'params'>,
): string | undefined =>
  record.result === 'ok' && record.target_type && CREATED_BY[record.target_type] === record.operation
    ? str(record.params?.name)
    : undefined;

/** Names the loaded records already hold for their targets, by id. */
export const namesFromRecords = (
  records: ReadonlyArray<Pick<AuditRecord, 'operation' | 'result' | 'target_type' | 'target_id' | 'params'>>,
): Map<string, string> => {
  const names = new Map<string, string>();
  for (const record of records) {
    const name = createdName(record);
    if (name && record.target_id) names.set(record.target_id, name);
  }
  return names;
};

/**
 * The targets a sentence about these records may name: each record's own,
 * and the target of the request an approval record decides or files.
 */
export const recordTargets = (
  records: ReadonlyArray<Pick<AuditRecord, 'target_type' | 'target_id' | 'approval_id'>>,
  ctx: RecordContext,
): { type: string; id: string }[] => {
  const seen = new Map<string, { type: string; id: string }>();
  for (const record of records) {
    if (record.target_type && record.target_id) seen.set(record.target_id, { type: record.target_type, id: record.target_id });
    const approvalId = record.target_type === 'approval' ? record.target_id : record.approval_id;
    const target = approvalId ? ctx.approval?.(approvalId)?.target : undefined;
    if (target) seen.set(target.id, { type: target.type, id: target.id });
  }
  return [...seen.values()];
};

/** The whole sentence for the feed: who, what, and how it arrived. */
export const recordSentence = (record: AuditRecord, ctx: RecordContext = {}): Segment[] => {
  const channel = channelPhrase(record.channel);
  return [
    strong(actorName(record, ctx)),
    plain(' '),
    ...recordAction(record, ctx),
    ...(channel ? [{ text: ` ${channel}`, faint: true }] : []),
  ];
};

/** The sentence as plain text, for labels and tests. */
export const segmentsText = (segments: readonly Segment[]): string =>
  segments.map((segment) => segment.text).join('');

/** Upper-case the first letter of the first segment: "Restarted blog". */
export const capitalize = (segments: readonly Segment[]): Segment[] => {
  const [first, ...rest] = segments;
  if (!first) return [];
  return [{ ...first, text: first.text.charAt(0).toUpperCase() + first.text.slice(1) }, ...rest];
};

/* ---------- the result tag ---------- */

/** The refusal reasons of permission-model.md, said plainly. */
export const REFUSAL_WORDS: Readonly<Record<string, string>> = {
  approval_required: 'needs approval',
  scope_missing: 'not in its grant',
  selector_mismatch: 'outside its grant',
  grant_expired: 'grant expired',
  grant_revoked: 'grant revoked',
  grant_exhausted: 'grant used up',
  human_only: 'people only',
  agent_disabled: 'agent disabled',
};

export interface ResultTag {
  readonly tone: 'run' | 'shift' | 'fault';
  readonly words: string;
}

/**
 * The tag at the end of a row: OK, "Refused · needs approval", "Error · …".
 * A request whose approval still waits says Waiting.
 */
export const resultTag = (
  record: Pick<AuditRecord, 'result' | 'result_detail' | 'operation' | 'target_type' | 'target_id'>,
  ctx: RecordContext = {},
): ResultTag => {
  if (record.result === 'refused') {
    const words = record.result_detail ? (REFUSAL_WORDS[record.result_detail] ?? record.result_detail) : null;
    return { tone: 'fault', words: words ? `Refused · ${words}` : 'Refused' };
  }
  if (record.result === 'error') {
    return { tone: 'fault', words: record.result_detail ? `Error · ${record.result_detail}` : 'Error' };
  }
  if (record.target_type === 'approval' && record.target_id && PHRASES[record.operation] === askPhrase) {
    const approval = ctx.approval?.(record.target_id);
    if (approval?.status === 'pending') return { tone: 'shift', words: 'Waiting' };
  }
  return { tone: 'run', words: 'OK' };
};

/* ---------- days ---------- */

const dayKey = (at: Date): string => `${at.getFullYear()}-${at.getMonth()}-${at.getDate()}`;

/** "Today", "Yesterday", or the date, in the reader's own zone. */
export const dayLabel = (iso: string, now: number = Date.now()): string => {
  const at = new Date(iso);
  if (Number.isNaN(at.getTime())) return 'Unknown day';
  const today = new Date(now);
  if (dayKey(at) === dayKey(today)) return 'Today';
  const yesterday = new Date(now - 86_400_000);
  if (dayKey(at) === dayKey(yesterday)) return 'Yesterday';
  return at.toLocaleDateString(undefined, {
    weekday: 'short',
    day: 'numeric',
    month: 'short',
    ...(at.getFullYear() === today.getFullYear() ? {} : { year: 'numeric' }),
  });
};

/** Records in their order, cut into runs of one day each. */
export const groupByDay = <T extends Pick<AuditRecord, 'time'>>(
  records: readonly T[],
  now: number = Date.now(),
): { label: string; records: T[] }[] => {
  const groups: { label: string; records: T[] }[] = [];
  for (const record of records) {
    const label = dayLabel(record.time, now);
    const last = groups[groups.length - 1];
    if (last && last.label === label) last.records.push(record);
    else groups.push({ label, records: [record] });
  }
  return groups;
};

/** "17:24": the clock time of a record, 24-hour, in the reader's zone. */
export const clockTime = (iso: string | number): string => {
  const at = new Date(iso);
  if (Number.isNaN(at.getTime())) return '--:--';
  return `${String(at.getHours()).padStart(2, '0')}:${String(at.getMinutes()).padStart(2, '0')}`;
};
