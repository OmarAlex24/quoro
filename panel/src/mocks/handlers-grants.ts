import { http, type HttpHandler } from 'msw';
import { Scope } from '../api/generated/model';
import type { CreateGrantRequest, Grant, Selector } from '../api/generated/model';
import {
  DESTRUCTIVE_SCOPES,
  HUMAN_ONLY_SCOPES,
  isGrantableScope,
} from '../lib/scopes';
import { agency, appendAudit, emitEvent } from './agency';
import { mockId, now } from './db';
import {
  conflict,
  isIntAtLeast,
  json,
  notFound,
  page,
  readJson,
  refuseUnlessWriter,
  signedIn,
  unauthorized,
  validationFailed,
} from './http';

const ALL_SCOPES = Object.values(Scope) as string[];

const isStringList = (value: unknown): value is string[] =>
  Array.isArray(value) && value.every((item) => typeof item === 'string');

const selectorIsEmpty = (selector: Selector): boolean =>
  !selector.all &&
  !selector.projects?.length &&
  !selector.apps?.length &&
  !selector.nodes?.length;

/** A selector is the documented shape: `all`, or non-empty lists of strings. */
const selectorIsValid = (selector: unknown): selector is Selector => {
  if (selector === null || typeof selector !== 'object' || Array.isArray(selector)) return false;
  const candidate = selector as Selector;
  if (candidate.all === true) return true;
  for (const key of ['projects', 'apps', 'nodes'] as const) {
    const value = candidate[key];
    if (value !== undefined && !isStringList(value)) return false;
  }
  return !selectorIsEmpty(candidate);
};

const scopeRefusal = (scope: Scope): string => {
  if (DESTRUCTIVE_SCOPES.has(scope)) {
    return `${scope} can't be granted: it is destructive and needs an approval.`;
  }
  if (HUMAN_ONLY_SCOPES.has(scope)) {
    return `${scope} can't be granted: it is human-only.`;
  }
  return `${scope} can't be granted.`;
};

/** Flip active grants whose TTL has passed, emitting `grant.expired` for each. */
const expireDueGrants = (): void => {
  const current = Date.now();
  for (const grant of agency().grants) {
    if (grant.status !== 'active') continue;
    if (Date.parse(grant.expires_at) > current) continue;
    grant.status = 'expired';
    emitEvent(
      'grant.expired',
      { type: 'grant', id: grant.id },
      { grant_id: grant.id, reason: 'ttl' },
    );
  }
};

/** Stateful mock handlers for the grants screens. Mock mode only. */
export const grantsHandlers: HttpHandler[] = [
  http.get('*/api/v1/grants', ({ request }) => {
    if (!signedIn()) return unauthorized();
    expireDueGrants();

    const state = agency();
    const url = new URL(request.url);
    const agentId = url.searchParams.get('agent_id');
    const status = url.searchParams.get('status');
    const kind = url.searchParams.get('kind');

    const items = state.grants
      .filter((grant) => !agentId || grant.agent_id === agentId)
      .filter((grant) => !status || grant.status === status)
      .filter((grant) => !kind || grant.kind === kind)
      .sort((left, right) => right.created_at.localeCompare(left.created_at));

    return json(page(items, request));
  }),

  http.post('*/api/v1/grants', async ({ request }) => {
    const refusal = refuseUnlessWriter('grant:write');
    if (refusal) return refusal;

    const body = await readJson<CreateGrantRequest>(request);
    if (!body) {
      return validationFailed('The form could not be read. Try again.');
    }

    const state = agency();
    const agent = state.agents.find((candidate) => candidate.id === body.agent_id);
    if (!agent) return notFound('That agent');

    if (body.kind !== undefined && body.kind !== 'standard') {
      return validationFailed('Only standard grants can be issued by hand.');
    }

    if (!Array.isArray(body.scopes) || body.scopes.length === 0) {
      return validationFailed('Choose at least one scope.');
    }
    for (const scope of body.scopes) {
      if (!ALL_SCOPES.includes(scope)) {
        return validationFailed(`"${scope}" is not a scope.`);
      }
      if (!isGrantableScope(scope)) {
        return validationFailed(scopeRefusal(scope));
      }
    }

    if (!selectorIsValid(body.selector)) {
      return validationFailed('Choose at least one resource, or every resource.');
    }

    const hasExpiresAt = typeof body.expires_at === 'string' && body.expires_at.trim() !== '';
    const hasTtl = body.ttl_s !== undefined && body.ttl_s !== null;
    if (hasExpiresAt === hasTtl) {
      return validationFailed('Set either an absolute expiry or a lifetime, not both.');
    }

    let expiresAt: string;
    if (hasTtl) {
      if (!isIntAtLeast(body.ttl_s, 1)) {
        return validationFailed('Lifetime must be a whole number of at least 1 second.');
      }
      expiresAt = new Date(Date.now() + body.ttl_s * 1_000).toISOString();
    } else {
      const parsed = Date.parse(body.expires_at ?? '');
      if (Number.isNaN(parsed) || parsed <= Date.now()) {
        return validationFailed('The expiry must be a time in the future.');
      }
      expiresAt = new Date(parsed).toISOString();
    }

    if (body.max_uses !== undefined && body.max_uses !== null && !isIntAtLeast(body.max_uses, 1)) {
      return validationFailed('Max uses must be a whole number of at least 1.');
    }

    const reason = (body.reason ?? '').trim();
    if (reason === '') {
      return validationFailed('Enter a reason. It is recorded with the grant and in the audit log.');
    }

    const human = signedIn();
    if (!human) return unauthorized();
    const incidentId = request.headers.get('Quoro-Incident')?.trim() || null;

    const grant: Grant = {
      id: mockId('grt'),
      agent_id: agent.id,
      kind: 'standard',
      scopes: body.scopes,
      selector: body.selector,
      expires_at: expiresAt,
      max_uses: body.max_uses ?? null,
      uses: 0,
      status: 'active',
      incident_id: null,
      approval_id: null,
      on_behalf_of: human.id,
      created_by_type: 'user',
      created_by_id: human.id,
      reason,
      created_at: now(),
      revoked_at: null,
      revoked_by: null,
    };
    state.grants.push(grant);

    emitEvent(
      'grant.created',
      { type: 'grant', id: grant.id },
      { grant_id: grant.id },
      incidentId ? { incident_id: incidentId } : {},
    );

    const params: Record<string, unknown> = {
      agent_id: grant.agent_id,
      scopes: grant.scopes,
      selector: grant.selector,
      expires_at: grant.expires_at,
      max_uses: grant.max_uses,
      reason: grant.reason,
    };
    if (body.ttl_s !== undefined) params.ttl_s = body.ttl_s;

    appendAudit({
      operation: 'POST /api/v1/grants',
      scope: 'grant:write',
      target_type: 'grant',
      target_id: grant.id,
      params,
      ...(incidentId ? { incident_id: incidentId } : {}),
    });

    return json(grant, { status: 201 });
  }),

  http.get('*/api/v1/grants/:grantId', ({ params }) => {
    if (!signedIn()) return unauthorized();
    expireDueGrants();
    const grant = agency().grants.find((candidate) => candidate.id === params.grantId);
    if (!grant) return notFound('That grant');
    return json(grant);
  }),

  http.post('*/api/v1/grants/:grantId/revoke', ({ params }) => {
    const refusal = refuseUnlessWriter('grant:write');
    if (refusal) return refusal;

    const human = signedIn();
    if (!human) return unauthorized();
    const grant = agency().grants.find((candidate) => candidate.id === params.grantId);
    if (!grant) return notFound('That grant');
    if (grant.status !== 'active') {
      return conflict('That grant is no longer active. The list has been refreshed.');
    }

    grant.status = 'revoked';
    grant.revoked_at = now();
    grant.revoked_by = human.id;

    emitEvent(
      'grant.revoked',
      { type: 'grant', id: grant.id },
      { grant_id: grant.id, revoked_by: human.id },
      grant.incident_id ? { incident_id: grant.incident_id } : {},
    );

    appendAudit({
      operation: 'POST /api/v1/grants/{grant_id}/revoke',
      scope: 'grant:write',
      target_type: 'grant',
      target_id: grant.id,
      params: { grant_id: grant.id },
      ...(grant.incident_id ? { incident_id: grant.incident_id } : {}),
    });

    return json(grant);
  }),
];
