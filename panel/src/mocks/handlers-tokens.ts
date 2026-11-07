import { http, type HttpHandler } from 'msw';
import type {
  ApiToken,
  ApiTokenCreated,
  ApiTokenList,
  CreateTokenRequest,
} from '../api/generated/model';
import { TOKEN_NAME_MAX } from '../lib/tokens';
import { appendAudit } from './agency';
import { mockId, now } from './db';
import {
  json,
  noContent,
  notFound,
  page,
  readJson,
  signedIn,
  unauthorized,
  validationFailed,
} from './http';

/**
 * Personal API tokens in mock mode. Like the real primary, only the token
 * rows are kept: the secret is made for the create response and dropped.
 */
let tokens: ApiToken[] | null = null;

const daysAgo = (days: number): string => new Date(Date.now() - days * 86_400_000).toISOString();
const inDays = (days: number): string => new Date(Date.now() + days * 86_400_000).toISOString();

/** Two tokens for the signed-in owner on first use, so the screen has something to show. */
const state = (): ApiToken[] => {
  if (tokens) return tokens;
  const owner = signedIn();
  tokens = owner
    ? [
        {
          id: mockId('tok'),
          user_id: owner.id,
          name: 'release pipeline',
          expires_at: daysAgo(4),
          last_used_at: daysAgo(5),
          created_at: daysAgo(94),
          revoked_at: daysAgo(30),
        },
        {
          id: mockId('tok'),
          user_id: owner.id,
          name: 'laptop cli',
          expires_at: inDays(76),
          last_used_at: daysAgo(0.1),
          created_at: daysAgo(14),
          revoked_at: null,
        },
      ]
    : [];
  return tokens;
};

/** Empties the mock store; tests start from it. */
export const resetMockTokens = (): void => {
  tokens = [];
};

/** A fresh `tok_` secret of 64 hex characters, the shape the primary issues. */
const freshTokenSecret = (): string => {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  return `tok_${Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('')}`;
};

/** The caller's tokens, newest first. Same-millisecond creates keep the later one first. */
const ownTokens = (userId: string): ApiToken[] =>
  state()
    .map((token, index) => ({ token, index }))
    .filter(({ token }) => token.user_id === userId)
    .sort(
      (left, right) =>
        right.token.created_at.localeCompare(left.token.created_at) ||
        right.index - left.index,
    )
    .map(({ token }) => token);

export const tokensHandlers: HttpHandler[] = [
  http.get('*/api/v1/tokens', ({ request }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    const list: ApiTokenList = page(ownTokens(user.id), request);
    return json(list);
  }),

  http.post('*/api/v1/tokens', async ({ request }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    const body = await readJson<CreateTokenRequest>(request);
    if (!body) return validationFailed('The form could not be read. Try again.');

    const name = (body.name ?? '').trim();
    if (name.length === 0 || name.length > TOKEN_NAME_MAX) {
      return validationFailed('name must be between 1 and 200 characters');
    }
    let expiresAt: string | null = null;
    if (body.expires_at !== undefined && body.expires_at !== null) {
      const at = Date.parse(body.expires_at);
      if (Number.isNaN(at)) return validationFailed('expires_at is not an RFC 3339 date-time');
      if (at <= Date.now()) return validationFailed('expires_at is in the past');
      expiresAt = new Date(at).toISOString();
    }

    const token: ApiToken = {
      id: mockId('tok'),
      user_id: user.id,
      name,
      expires_at: expiresAt,
      last_used_at: null,
      created_at: now(),
      revoked_at: null,
    };
    state().push(token);

    appendAudit({
      operation: 'POST /api/v1/tokens',
      target_type: 'user',
      target_id: user.id,
      params: { name, expires_at: body.expires_at ?? null, token_id: token.id },
    });

    const created: ApiTokenCreated = { ...token, token: freshTokenSecret() };
    return json(created, { status: 201 });
  }),

  http.delete('*/api/v1/tokens/:tokenId', ({ params }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    const tokenId = String(params.tokenId);
    // Another user's token answers exactly like an unknown one.
    const token = state().find(
      (candidate) => candidate.id === tokenId && candidate.user_id === user.id,
    );
    if (!token) return notFound('That token');
    if (!token.revoked_at) token.revoked_at = now();

    appendAudit({
      operation: 'DELETE /api/v1/tokens/{token_id}',
      target_type: 'user',
      target_id: user.id,
      params: { token_id: tokenId },
    });

    return noContent();
  }),
];
