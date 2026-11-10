/**
 * The single HTTP entry point for the generated client.
 *
 * Every request goes through `fetch` with the session cookie (`same-origin`),
 * JSON in and out. A non-2xx answer is turned into an `ApiProblem` built from
 * the `application/problem+json` body (docs/contracts/openapi.yaml, Problem).
 * Generated code calls this as `apiFetch<T>(url, init)`.
 */
import type { Problem } from './generated/model';

/** A refused or failed API call, carrying the problem document verbatim. */
export class ApiProblem extends Error {
  readonly status: number;
  readonly code: Problem['code'];
  readonly reason?: Problem['reason'];
  readonly detail?: string;
  readonly scope?: string;
  readonly title?: string;

  constructor(problem: {
    status: number;
    code: Problem['code'];
    reason?: Problem['reason'];
    detail?: string;
    scope?: string;
    title?: string;
  }) {
    super(problem.detail ?? problem.title ?? problem.code);
    this.name = 'ApiProblem';
    this.status = problem.status;
    this.code = problem.code;
    this.reason = problem.reason;
    this.detail = problem.detail;
    this.scope = problem.scope;
    this.title = problem.title;
  }
}

export const isApiProblem = (error: unknown): error is ApiProblem =>
  error instanceof ApiProblem;

const isProblemCode = (value: unknown): value is Problem['code'] =>
  value === 'unauthenticated' ||
  value === 'forbidden' ||
  value === 'approval_required' ||
  value === 'not_found' ||
  value === 'conflict' ||
  value === 'payload_too_large' ||
  value === 'validation_failed' ||
  value === 'rate_limited' ||
  value === 'not_primary' ||
  value === 'node_unreachable' ||
  value === 'internal';

const isProblemReason = (value: unknown): value is NonNullable<Problem['reason']> =>
  value === 'scope_missing' ||
  value === 'selector_mismatch' ||
  value === 'grant_expired' ||
  value === 'grant_revoked' ||
  value === 'grant_exhausted' ||
  value === 'human_only' ||
  value === 'agent_disabled';

const problemFromResponse = async (response: Response): Promise<ApiProblem> => {
  let parsed: Partial<Problem> = {};
  try {
    parsed = (await response.json()) as Partial<Problem>;
  } catch {
    parsed = {};
  }
  return new ApiProblem({
    status: response.status,
    code: isProblemCode(parsed.code) ? parsed.code : 'internal',
    reason: isProblemReason(parsed.reason) ? parsed.reason : undefined,
    detail: typeof parsed.detail === 'string' ? parsed.detail : undefined,
    scope: typeof parsed.scope === 'string' ? parsed.scope : undefined,
    title: typeof parsed.title === 'string' ? parsed.title : undefined,
  });
};

const mergeHeaders = (headers?: HeadersInit): Record<string, string> => {
  if (!headers) return { Accept: 'application/json' };
  if (headers instanceof Headers) {
    return { Accept: 'application/json', ...Object.fromEntries(headers.entries()) };
  }
  if (Array.isArray(headers)) {
    return { Accept: 'application/json', ...Object.fromEntries(headers) };
  }
  return { Accept: 'application/json', ...headers };
};

export const apiFetch = async <T>(url: string, init: RequestInit = {}): Promise<T> => {
  const response = await fetch(url, {
    ...init,
    credentials: 'same-origin',
    headers: mergeHeaders(init.headers),
  });

  if (!response.ok) throw await problemFromResponse(response);
  if (response.status === 204) return undefined as T;
  return (await response.json()) as T;
};

export default apiFetch;
