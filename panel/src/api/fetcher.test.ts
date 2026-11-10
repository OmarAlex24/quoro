import { afterEach, describe, expect, it, vi } from 'vitest';
import { ApiProblem, apiFetch } from './fetcher';

const answer = (status: number, body: unknown) =>
  vi.fn(async () =>
    new Response(JSON.stringify(body), {
      status,
      headers: { 'Content-Type': 'application/problem+json' },
    }),
  );

const refusal = async (status: number, body: unknown): Promise<ApiProblem> => {
  vi.stubGlobal('fetch', answer(status, body));
  try {
    await apiFetch('/api/v1/projects', { method: 'POST' });
  } catch (error) {
    if (error instanceof ApiProblem) return error;
    throw error;
  }
  throw new Error('the call did not fail');
};

describe('apiFetch problems', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('keeps the code of a body above the limit', async () => {
    const problem = await refusal(413, {
      status: 413,
      code: 'payload_too_large',
      detail: 'the request body is larger than 262144 bytes (256 KiB)',
    });
    expect(problem.status).toBe(413);
    expect(problem.code).toBe('payload_too_large');
    expect(problem.detail).toBe('the request body is larger than 262144 bytes (256 KiB)');
  });

  it('reads an unknown code as internal', async () => {
    const problem = await refusal(500, { status: 500, code: 'something_new' });
    expect(problem.code).toBe('internal');
  });
});
