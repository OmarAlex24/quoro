import { http, type HttpHandler } from 'msw';
import type { IncidentDetail, User, UserList } from '../api/generated/model';
import { agency } from './agency';
import { db } from './db';
import { json, notFound, page, signedIn, unauthorized, writeRefusal } from './http';

/**
 * Incidents (read only: the grant form picks one, the audit view links one)
 * and the user list, which needs `user:write` and so answers the owner only.
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

  http.get('*/api/v1/users', ({ request }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role !== 'owner') return writeRefusal('user:write');
    const users: User[] = db.owner ? [db.owner] : [];
    const list: UserList = page(users, request);
    return json(list);
  }),
];
