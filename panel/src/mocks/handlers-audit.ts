import { http, type HttpHandler } from 'msw';
import type {
  AuditList,
  AuditRecord,
  AuditVerificationRequest,
  AuditVerificationResult,
} from '../api/generated/model';
import { agency, appendAudit, verifyAuditChain } from './agency';
import {
  isIntAtLeast,
  json,
  notFound,
  readJson,
  signedIn,
  unauthorized,
  validationFailed,
} from './http';

/** Stateful mock handlers for the audit log. Mock mode only. */

const queryText = (params: URLSearchParams, name: string): string => {
  const value = params.get(name);
  return value === null ? '' : value.trim();
};

/** `limit` as the contract states it here: default 50, maximum 200. */
const pageLimit = (params: URLSearchParams): number => {
  const parsed = Number(params.get('limit') ?? '50');
  if (!Number.isInteger(parsed) || parsed < 1) return 50;
  return Math.min(parsed, 200);
};

/** A cursor is the last returned `seq`, as an opaque string. */
const resumeSeq = (params: URLSearchParams): number | null => {
  const cursor = queryText(params, 'cursor');
  const after = queryText(params, 'after_seq');
  const raw = cursor !== '' ? cursor : after;
  if (raw === '') return null;
  const parsed = Number(raw);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : null;
};

export const auditHandlers: HttpHandler[] = [
  http.get('*/api/v1/audit', ({ request }) => {
    if (!signedIn()) return unauthorized();

    const url = new URL(request.url);
    const actorId = queryText(url.searchParams, 'actor_id');
    const targetId = queryText(url.searchParams, 'target_id');
    const result = queryText(url.searchParams, 'result');
    const incidentId = queryText(url.searchParams, 'incident_id');
    const after = resumeSeq(url.searchParams);

    let records: AuditRecord[] = agency()
      .audit.filter(
        (record) =>
          (actorId === '' || record.actor_id === actorId) &&
          (targetId === '' || record.target_id === targetId) &&
          (result === '' || record.result === result) &&
          (incidentId === '' || record.incident_id === incidentId),
      )
      .sort((left, right) => left.seq - right.seq);

    if (after !== null) records = records.filter((record) => record.seq > after);

    const items = records.slice(0, pageLimit(url.searchParams));
    const last = items[items.length - 1];
    const hasMore = records.length > items.length;
    const list: AuditList = {
      items,
      next_cursor: hasMore && last ? String(last.seq) : null,
    };
    return json(list);
  }),

  http.get('*/api/v1/audit/:seq', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const raw = String(params.seq ?? '');
    if (!/^[1-9][0-9]*$/.test(raw)) return notFound('That audit record');
    const seq = Number(raw);
    const record = agency().audit.find((candidate) => candidate.seq === seq);
    if (!record) return notFound('That audit record');
    return json(record);
  }),

  http.post('*/api/v1/audit/verify', async ({ request }) => {
    if (!signedIn()) return unauthorized();

    const body = (await readJson<AuditVerificationRequest>(request)) ?? {};
    const from = body.from_seq;
    const to = body.to_seq;

    if (from !== undefined && !isIntAtLeast(from, 1)) {
      return validationFailed('from_seq must be a whole number of 1 or more.');
    }
    if (to !== undefined && !isIntAtLeast(to, 1)) {
      return validationFailed('to_seq must be a whole number of 1 or more.');
    }
    if (from !== undefined && to !== undefined && from > to) {
      return validationFailed('from_seq must not be after to_seq.');
    }

    const result: AuditVerificationResult = verifyAuditChain(from, to);
    const params: Record<string, unknown> = {};
    if (from !== undefined) params.from_seq = from;
    if (to !== undefined) params.to_seq = to;
    appendAudit({
      operation: 'POST /api/v1/audit/verify',
      scope: 'audit:read',
      target_type: 'cluster',
      target_id: 'cluster',
      params,
      result: result.ok ? 'ok' : 'error',
      result_detail: result.ok ? undefined : result.reason ?? 'chain_broken',
    });
    return json(result);
  }),
];
