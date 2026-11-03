import { http, type HttpHandler } from 'msw';
import type {
  Approval,
  ApprovalList,
  DecisionRequest,
  Grant,
  Selector,
  Target,
} from '../api/generated/model';
import {
  agency,
  appendAudit,
  canonicalJson,
  emitEvent,
  sha256Hex,
  type AgencyState,
} from './agency';
import { db, mockId, now } from './db';
import {
  conflict,
  json,
  notFound,
  page,
  readJson,
  refuseUnlessWriter,
  signedIn,
  unauthorized,
  validationFailed,
} from './http';

/**
 * Stateful mock handlers for the approvals queue. Mock mode only.
 *
 * A pending request lapses one hour after filing; every read sweeps the ones
 * past `expires_at` to `expired` and emits `approval.expired`, so the queue
 * shows the same deadlines the real service would. Approving issues the one
 * approval grant from permission-model.md, section 9.5.
 */

const STATUSES = new Set<string>(['pending', 'approved', 'denied', 'expired']);

/** A request lapses one hour after it was filed. */
const REQUEST_TTL_MS = 60 * 60_000;

/** An approval grant is valid for ten minutes and one use. */
const GRANT_TTL_MS = 10 * 60_000;

const isoAfter = (iso: string, milliseconds: number): string =>
  new Date(Date.parse(iso) + milliseconds).toISOString();

/** Flip every pending request past its expiry, emitting one event each. */
const sweepExpired = (state: AgencyState): void => {
  const current = Date.parse(now());
  for (const approval of state.approvals) {
    if (approval.status !== 'pending') continue;
    if (Date.parse(approval.expires_at) > current) continue;
    approval.status = 'expired';
    emitEvent(
      'approval.expired',
      { type: 'approval', id: approval.id },
      { approval_id: approval.id },
      approval.incident_id ? { incident_id: approval.incident_id } : {},
    );
  }
};

/**
 * The approval grant's selector: the target only. A project matches by slug and
 * a VM by the slug of the project it belongs to; anything else matches nothing,
 * which is the safe default.
 */
const selectorForTarget = (target: Target): Selector => {
  switch (target.type) {
    case 'app':
      return { apps: [target.id] };
    case 'node':
      return { nodes: [target.id] };
    case 'project': {
      const project = db.projects.find((candidate) => candidate.id === target.id);
      return project ? { projects: [project.slug] } : {};
    }
    case 'vm': {
      const vm = db.vms.find((candidate) => candidate.id === target.id);
      const project = vm
        ? db.projects.find((candidate) => candidate.id === vm.project_id)
        : undefined;
      return project ? { projects: [project.slug] } : {};
    }
    default:
      return {};
  }
};

/** The one extra grant an approval issues: one scope, one target, one use, 10 min. */
const issueApprovalGrant = (
  approval: Approval,
  deciderId: string,
  decidedAt: string,
): Grant => {
  const grant: Grant = {
    id: mockId('grt'),
    agent_id: approval.agent_id,
    kind: 'approval',
    scopes: [approval.scope],
    selector: selectorForTarget(approval.target),
    expires_at: isoAfter(decidedAt, GRANT_TTL_MS),
    max_uses: 1,
    uses: 0,
    status: 'active',
    incident_id: null,
    approval_id: approval.id,
    on_behalf_of: deciderId,
    created_by_type: 'user',
    created_by_id: deciderId,
    reason: `Approved request ${approval.id}`,
    created_at: decidedAt,
    revoked_at: null,
    revoked_by: null,
  };
  agency().grants.push(grant);
  return grant;
};

/** Shared by approve and deny: the checks, the state change, the events, the record. */
const decide = async (
  request: Request,
  approvalId: string,
  decision: 'approve' | 'deny',
): Promise<Response> => {
  const refusal = refuseUnlessWriter('approval:decide');
  if (refusal) return refusal;

  const state = agency();
  sweepExpired(state);
  const approval = state.approvals.find((candidate) => candidate.id === approvalId);
  if (!approval) return notFound('That approval request');
  if (approval.status !== 'pending') {
    return conflict('Someone already decided this request, or it expired.');
  }

  const body = await readJson<DecisionRequest>(request);
  const note = (body?.note ?? '').trim();
  if (note.length > 2000) {
    return validationFailed('A decision note can be at most 2000 characters.');
  }

  const decider = signedIn();
  if (!decider) return unauthorized();

  const decidedAt = now();
  approval.status = decision === 'approve' ? 'approved' : 'denied';
  approval.decided_by = decider.id;
  approval.decided_at = decidedAt;
  approval.decision_note = note === '' ? null : note;

  const incidentExtra = approval.incident_id
    ? { incident_id: approval.incident_id }
    : {};
  const actor = { type: 'user' as const, id: decider.id };

  let grant: Grant | null = null;
  if (decision === 'approve') {
    grant = issueApprovalGrant(approval, decider.id, decidedAt);
    approval.grant_id = grant.id;
    emitEvent(
      'approval.approved',
      { type: 'approval', id: approval.id },
      { approval_id: approval.id, grant_id: grant.id },
      { ...incidentExtra, actor },
    );
    emitEvent(
      'grant.created',
      { type: 'grant', id: grant.id },
      { grant_id: grant.id, approval_id: approval.id },
      { ...incidentExtra, actor },
    );
  } else {
    emitEvent(
      'approval.denied',
      { type: 'approval', id: approval.id },
      { approval_id: approval.id },
      { ...incidentExtra, actor },
    );
  }

  appendAudit({
    operation: `POST /api/v1/approvals/{approval_id}/${decision}`,
    scope: 'approval:decide',
    target_type: 'approval',
    target_id: approval.id,
    params: { approval_id: approval.id, note: note === '' ? null : note },
    ...(approval.incident_id ? { incident_id: approval.incident_id } : {}),
  });

  return json(approval);
};

export const approvalsHandlers: HttpHandler[] = [
  http.get('*/api/v1/approvals', ({ request }) => {
    if (!signedIn()) return unauthorized();
    const state = agency();
    sweepExpired(state);
    const status = new URL(request.url).searchParams.get('status');
    if (status !== null && !STATUSES.has(status)) {
      return validationFailed(
        'status must be pending, approved, denied or expired.',
      );
    }
    const items = [...state.approvals]
      .filter((approval) => status === null || approval.status === status)
      .sort((left, right) => right.created_at.localeCompare(left.created_at));
    const list: ApprovalList = page(items, request);
    return json(list);
  }),

  http.get('*/api/v1/approvals/:approvalId', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const state = agency();
    sweepExpired(state);
    const approval = state.approvals.find(
      (candidate) => candidate.id === String(params.approvalId),
    );
    if (!approval) return notFound('That approval request');
    return json(approval);
  }),

  http.post('*/api/v1/approvals/:approvalId/approve', ({ request, params }) =>
    decide(request, String(params.approvalId), 'approve'),
  ),

  http.post('*/api/v1/approvals/:approvalId/deny', ({ request, params }) =>
    decide(request, String(params.approvalId), 'deny'),
  ),
];

/**
 * File one new pending request the way the seeded `on-call-restarter` would:
 * delete the seeded `blog` app, with the exact params and their hash. Wired to
 * the browser console by the coordinator; it is not a route.
 */
export function fileMockApproval(): Approval {
  const state = agency();
  const owner = db.owner;
  if (!owner) throw new Error('fileMockApproval needs setup to have run.');
  const agent =
    state.agents.find((candidate) => candidate.name === 'on-call-restarter') ??
    state.agents[0];
  if (!agent) throw new Error('fileMockApproval needs the seeded agent.');
  const app = db.apps.find((candidate) => candidate.name === 'blog') ?? db.apps[0];
  if (!app) throw new Error('fileMockApproval needs the seeded blog app.');

  const incident = state.incidents.find(
    (candidate) =>
      candidate.status === 'open' &&
      candidate.subject.type === 'app' &&
      candidate.subject.id === app.id,
  );
  const params = { app_id: app.id };
  const filedAt = now();
  const approval: Approval = {
    id: mockId('apr'),
    agent_id: agent.id,
    scope: 'app:delete',
    target: { type: 'app', id: app.id },
    operation: 'delete_app',
    params,
    params_hash: sha256Hex(canonicalJson(params)),
    justification:
      'The restart did not bring blog back and its image is gone from the registry; deleting the app so a clean deploy can replace it.',
    incident_id: incident?.id ?? null,
    status: 'pending',
    expires_at: isoAfter(filedAt, REQUEST_TTL_MS),
    decided_by: null,
    decided_at: null,
    decision_note: null,
    grant_id: null,
    created_at: filedAt,
  };
  state.approvals.push(approval);

  emitEvent(
    'approval.requested',
    { type: 'approval', id: approval.id },
    { approval_id: approval.id },
    {
      ...(incident ? { incident_id: incident.id } : {}),
      actor: { type: 'agent', id: agent.id },
      time: filedAt,
    },
  );

  appendAudit({
    time: filedAt,
    actor_type: 'agent',
    actor_id: agent.id,
    agent_model: agent.model,
    on_behalf_of: owner.id,
    channel: 'mcp',
    operation: 'request_approval',
    scope: 'approval:create',
    target_type: 'approval',
    target_id: approval.id,
    project_id: app.project_id,
    params: {
      scope: approval.scope,
      target: approval.target,
      operation: approval.operation,
      params: approval.params,
    },
    justification: approval.justification,
    ...(incident ? { incident_id: incident.id } : {}),
  });

  return approval;
}
