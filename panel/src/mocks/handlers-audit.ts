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

/** `limit` as the contract states it (audit-log-format.md, section 2.4): default 50, maximum 500. */
const pageLimit = (params: URLSearchParams): number => {
  const parsed = Number(params.get('limit') ?? '50');
  if (!Number.isInteger(parsed) || parsed < 1) return 50;
  return Math.min(parsed, 500);
};

/** A whole number read from the query, `null` when absent, `NaN` when unreadable. */
const queryInt = (params: URLSearchParams, name: string): number | null => {
  const raw = queryText(params, name);
  if (raw === '') return null;
  return /^-?[0-9]+$/.test(raw) ? Number(raw) : Number.NaN;
};

/** The `seq` window one page reads, `after < seq < before`, and its order (ADR 0283). */
interface SeqWindow {
  readonly after: number;
  readonly before: number | null;
  readonly newestFirst: boolean;
}

/**
 * `order`, `after_seq`, `before_seq` and the cursor as the page's window, as
 * the control plane reads them. The cursor is the last returned `seq`, as an
 * opaque string: oldest first the page continues after it, so it narrows
 * `after_seq`; newest first it continues before it, so it narrows
 * `before_seq`. A window that holds no `seq` is not an error.
 */
const windowOf = (params: URLSearchParams): SeqWindow | string => {
  const order = queryText(params, 'order');
  if (order !== '' && order !== 'asc' && order !== 'desc') return `order "${order}" is not asc or desc.`;
  const newestFirst = order === 'desc';

  const afterSeq = queryInt(params, 'after_seq');
  if (afterSeq !== null && !(afterSeq >= 0)) return 'after_seq must be a whole number of 0 or more.';
  const beforeSeq = queryInt(params, 'before_seq');
  if (beforeSeq !== null && !(beforeSeq >= 1)) return 'before_seq must be a whole number of 1 or more.';
  const cursor = queryInt(params, 'cursor');
  if (cursor !== null && Number.isNaN(cursor)) return 'cursor is not a sequence number.';

  if (newestFirst) {
    const bounds = [beforeSeq, cursor].filter((bound): bound is number => bound !== null);
    return { after: afterSeq ?? 0, before: bounds.length > 0 ? Math.min(...bounds) : null, newestFirst };
  }
  return { after: Math.max(afterSeq ?? 0, cursor ?? 0), before: beforeSeq, newestFirst };
};

export const auditHandlers: HttpHandler[] = [
  http.get('*/api/v1/audit', ({ request }) => {
    if (!signedIn()) return unauthorized();

    const url = new URL(request.url);
    const actorId = queryText(url.searchParams, 'actor_id');
    const targetId = queryText(url.searchParams, 'target_id');
    const result = queryText(url.searchParams, 'result');
    const incidentId = queryText(url.searchParams, 'incident_id');
    const seqWindow = windowOf(url.searchParams);
    if (typeof seqWindow === 'string') return validationFailed(seqWindow);

    const records: AuditRecord[] = agency()
      .audit.filter(
        (record) =>
          record.seq > seqWindow.after &&
          (seqWindow.before === null || record.seq < seqWindow.before) &&
          (actorId === '' || record.actor_id === actorId) &&
          (targetId === '' || record.target_id === targetId) &&
          (result === '' || record.result === result) &&
          (incidentId === '' || record.incident_id === incidentId),
      )
      .sort((left, right) => (seqWindow.newestFirst ? right.seq - left.seq : left.seq - right.seq));

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
