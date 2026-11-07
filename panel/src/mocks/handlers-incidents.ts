import { http, type HttpHandler } from 'msw';
import type { IncidentDetail, ResolveIncidentRequest, User, UserList } from '../api/generated/model';
import { agency, appendAudit, emitEvent } from './agency';
import { db, now } from './db';
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
 * Incidents (the grant form picks one, the audit view links one, an agent's
 * story is told around one, and a person resolves one) and the user list,
 * which needs `user:read` and so answers every human role.
 */
export const incidentsHandlers: HttpHandler[] = [
  http.get('*/api/v1/incidents', ({ request }) => {
    if (!signedIn()) return unauthorized();
    const status = new URL(request.url).searchParams.get('status');
    const items = [...agency().incidents]
      .filter((incident) => !status || incident.status === status)
      .sort((left, right) => right.opened_at.localeCompare(left.opened_at));
    return json(page(items, request));
  }),

  http.get('*/api/v1/incidents/:incidentId', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const state = agency();
    const incident = state.incidents.find((candidate) => candidate.id === params.incidentId);
    if (!incident) return notFound('That incident');
    const trigger = state.events.find((event) => event.id === incident.trigger_event_id);
    if (!trigger) return notFound('The event that opened the incident');
    const detail: IncidentDetail = {
      ...incident,
      trigger_event: trigger,
      grants: state.grants.filter((grant) => grant.incident_id === incident.id),
    };
    return json(detail);
  }),

  /**
   * Resolve an open incident. Its incident grants end with it
   * (permission-model.md, the incident walk-through: "On resolve, the grant
   * becomes revoked"), and the resolution is in the audit log and the events.
   */
  http.post('*/api/v1/incidents/:incidentId/resolve', async ({ request, params }) => {
    const refusal = refuseUnlessWriter('incident:resolve');
    if (refusal) return refusal;
    const human = signedIn();
    if (!human) return unauthorized();
    const state = agency();
    const incident = state.incidents.find((candidate) => candidate.id === params.incidentId);
    if (!incident) return notFound('That incident');
    if (incident.status !== 'open') return conflict('That incident is already resolved.');

    const body = (await readJson<ResolveIncidentRequest>(request)) ?? {};
    const note = typeof body.resolution_note === 'string' ? body.resolution_note.trim() : '';
    if (note.length > 2_000) return validationFailed('resolution_note must be at most 2000 characters.');

    const at = now();
    incident.status = 'resolved';
    incident.resolved_at = at;
    incident.resolved_by_type = 'user';
    incident.resolved_by_id = human.id;
    incident.resolution_note = note === '' ? null : note;

    for (const grant of state.grants) {
      if (grant.incident_id !== incident.id || grant.kind !== 'incident' || grant.status !== 'active') continue;
      grant.status = 'revoked';
      grant.revoked_at = at;
      grant.revoked_by = human.id;
    }

    emitEvent(
      'incident.resolved',
      incident.subject,
      {
        incident_id: incident.id,
        resolved_by_type: 'user',
        resolved_by_id: human.id,
        ...(note === '' ? {} : { resolution_note: note }),
      },
      { project_id: incident.project_id ?? null, incident_id: incident.id, actor: { type: 'user', id: human.id } },
    );
    appendAudit({
      operation: 'POST /api/v1/incidents/{incident_id}/resolve',
      scope: 'incident:resolve',
      target_type: 'incident',
      target_id: incident.id,
      ...(incident.project_id ? { project_id: incident.project_id } : {}),
      params: { incident_id: incident.id, ...(note === '' ? {} : { resolution_note: note }) },
      incident_id: incident.id,
    });
    return json(incident);
  }),

  http.get('*/api/v1/users', ({ request }) => {
    if (!signedIn()) return unauthorized();
    const users: User[] = db.owner ? [db.owner] : [];
    const list: UserList = page(users, request);
    return json(list);
  }),
];
