// SPDX-License-Identifier: AGPL-3.0-only
import { QueryClient, type MutationOptions } from '@tanstack/react-query';
import { setupServer } from 'msw/node';
import { afterAll, beforeAll, beforeEach, vi } from 'vitest';
import { setRole } from './agency';
import { db, mockId, now } from './db';
import { handlers } from './handlers';
import { resetNodesMock } from './handlers-nodes';

/** Where the generated client's relative URLs (`/api/v1/...`) go in a test. */
export const TEST_ORIGIN = 'http://panel.test';

/** One request as the mock received it. */
export interface SeenRequest {
  method: string;
  path: string;
  headers: Headers;
  /** The body as text, once read. */
  body: Promise<string>;
}

/**
 * Tests only: the mock API under `msw/node`, as `browser.ts` serves it in
 * mock mode, for the generated client and `apiFetch` unchanged. Node's fetch
 * takes no relative URL, so a path is sent to `TEST_ORIGIN`. Each test starts
 * signed in as the owner. Returns the requests the mock received, newest
 * last, emptied before each test.
 */
export const serveMockApi = (): { requests: SeenRequest[] } => {
  const server = setupServer(...handlers);
  const requests: SeenRequest[] = [];

  beforeAll(() => {
    server.listen({ onUnhandledFrame: 'error' });
    server.events.on('request:start', ({ request }) => {
      requests.push({
        method: request.method,
        path: new URL(request.url).pathname,
        headers: new Headers(request.headers),
        body: request.clone().text(),
      });
    });
    // msw has put its own fetch in place by now; a path goes to it with the test origin.
    const intercepted = globalThis.fetch;
    vi.stubGlobal('fetch', (input: RequestInfo | URL, init?: RequestInit) =>
      intercepted(typeof input === 'string' && input.startsWith('/') ? `${TEST_ORIGIN}${input}` : input, init),
    );
  });

  afterAll(() => {
    vi.unstubAllGlobals();
    server.close();
  });

  beforeEach(() => {
    requests.length = 0;
    if (!db.owner) {
      db.owner = {
        id: mockId('usr'),
        email: 'owner@example.com',
        name: 'Owner',
        role: 'owner',
        disabled: false,
        created_at: now(),
      };
    }
    setRole('owner');
    db.sessionUserId = db.owner.id;
    resetNodesMock();
  });

  return { requests };
};

/**
 * Tests only: a QueryClient that keeps the options each `useMutation` of a
 * container gives it, in the order the container calls `useMutation`.
 * `useMutation` builds its `MutationObserver` while it renders, and the
 * observer passes its options through `defaultMutationOptions`, so a render
 * with `renderToStaticMarkup` yields the container's own options, to run
 * through a `MutationObserver` against the mock API. Read them right after
 * the render: running a mutation records its options again.
 */
export class RecordingClient extends QueryClient {
  readonly mutations: MutationOptions<unknown, unknown, unknown, unknown>[] = [];

  // The signature is QueryClient's own.
  override defaultMutationOptions<T extends MutationOptions<any, any, any, any>>(options?: T): T {
    if (options) this.mutations.push(options);
    return super.defaultMutationOptions(options);
  }
}
