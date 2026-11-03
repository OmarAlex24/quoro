import { HttpResponse } from 'msw';
import type { Problem, ProblemCode, User } from '../api/generated/model';
import { db } from './db';

/** Response helpers shared by every mock handler file. Mock mode only. */

type JsonBody = Parameters<typeof HttpResponse.json>[0];
type JsonInit = Parameters<typeof HttpResponse.json>[1];

/** A JSON response typed as a plain `Response`, so handlers can mix outcomes. */
export const json = (body: JsonBody, init?: JsonInit): Response =>
  HttpResponse.json(body, init);

export const noContent = (): Response => new HttpResponse(null, { status: 204 });

export const problem = (
  status: number,
  code: ProblemCode,
  extra: Partial<Problem> = {},
): Response =>
  json(
    { status, code, ...extra },
    { status, headers: { 'Content-Type': 'application/problem+json' } },
  );

export const unauthorized = () =>
  problem(401, 'unauthenticated', { detail: 'Log in to continue.' });

export const notFound = (what: string) =>
  problem(404, 'not_found', { detail: `${what} does not exist.` });

export const validationFailed = (detail: string) =>
  problem(422, 'validation_failed', { detail });

export const conflict = (detail: string) => problem(409, 'conflict', { detail });

export const signedIn = (): User | null =>
  db.owner && db.sessionUserId === db.owner.id ? db.owner : null;

export const writeRefusal = (scope: string) =>
  problem(403, 'forbidden', {
    reason: 'scope_missing',
    scope,
    detail: `Your role can't use ${scope}.`,
  });

/**
 * The session check every mutating agency handler starts with: a 401 without a
 * session, a 403 `scope_missing` for a viewer. `null` means go ahead.
 */
export const refuseUnlessWriter = (scope: string): Response | null => {
  const user = signedIn();
  if (!user) return unauthorized();
  if (user.role === 'viewer') return writeRefusal(scope);
  return null;
};

export const readJson = async <T>(request: Request): Promise<T | null> => {
  try {
    return (await request.json()) as T;
  } catch {
    return null;
  }
};

export const isIntAtLeast = (value: unknown, minimum: number): value is number =>
  typeof value === 'number' && Number.isInteger(value) && value >= minimum;

/** One page of a list: `limit` from the query (default 50), no cursor paging in the mock. */
export const page = <T>(items: T[], request: Request): { items: T[]; next_cursor: null } => {
  const limit = Number(new URL(request.url).searchParams.get('limit') ?? '50');
  const size = Number.isInteger(limit) && limit > 0 ? limit : 50;
  return { items: items.slice(0, size), next_cursor: null };
};
