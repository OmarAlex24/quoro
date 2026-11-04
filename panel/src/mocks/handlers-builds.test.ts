// SPDX-License-Identifier: AGPL-3.0-only
import { setupServer } from 'msw/node';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import type { Build, BuildList, BuildLogList } from '../api/generated/model';
import { buildsHandlers } from './handlers-builds';
import { db, mockId, now } from './db';

const server = setupServer(...buildsHandlers);
const BASE = 'http://panel.test/api/v1';

beforeAll(() => {
  (globalThis as unknown as { window: typeof globalThis }).window = globalThis;
  vi.useFakeTimers();
  server.listen();
});

afterAll(() => {
  server.close();
  vi.useRealTimers();
});

const signedIn = () => {
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
  db.sessionUserId = db.owner.id;
};

const docsApp = () => {
  const app = db.apps.find((candidate) => candidate.name === 'docs-site');
  if (!app) throw new Error('the mock no longer seeds docs-site');
  return app;
};

describe('build handlers', () => {
  it('lists the seeded builds newest first', async () => {
    signedIn();
    const app = docsApp();
    const response = await fetch(`${BASE}/apps/${app.id}/builds`);
    expect(response.status).toBe(200);
    const list = (await response.json()) as BuildList;
    expect(list.items).toHaveLength(2);
    expect(list.items[0]?.status).toBe('failed');
    expect(list.items[1]?.status).toBe('succeeded');
  });

  it('starts one build at a time and cancels it', async () => {
    signedIn();
    const app = docsApp();
    const started = await fetch(`${BASE}/apps/${app.id}/builds`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ref: 'preview' }),
    });
    expect(started.status).toBe(202);
    const build = (await started.json()) as Build;
    expect(build.status).toBe('queued');
    expect(build.source.ref).toBe('preview');

    const second = await fetch(`${BASE}/apps/${app.id}/builds`, { method: 'POST' });
    expect(second.status).toBe(409);

    const read = await fetch(`${BASE}/builds/${build.id}`);
    expect(read.status).toBe(200);

    const cancelled = await fetch(`${BASE}/builds/${build.id}/cancel`, {
      method: 'POST',
    });
    expect(cancelled.status).toBe(202);
    expect(((await cancelled.json()) as Build).status).toBe('cancelled');

    const again = await fetch(`${BASE}/builds/${build.id}/cancel`, {
      method: 'POST',
    });
    expect(again.status).toBe(409);
  });

  it('refuses a build for an app that deploys an image', async () => {
    signedIn();
    const blog = db.apps.find((candidate) => candidate.name === 'blog');
    if (!blog) throw new Error('the mock no longer seeds blog');
    const response = await fetch(`${BASE}/apps/${blog.id}/builds`, {
      method: 'POST',
    });
    expect(response.status).toBe(409);
  });

  it('pages a finished log and streams it with seq ids', async () => {
    signedIn();
    const failed = db.builds.find((build) => build.status === 'failed');
    if (!failed) throw new Error('the mock no longer seeds a failed build');

    const page = await fetch(`${BASE}/builds/${failed.id}/logs?limit=3`);
    expect(page.status).toBe(200);
    const list = (await page.json()) as BuildLogList;
    expect(list.items.map((line) => line.seq)).toEqual([1, 2, 3]);
    expect(list.next_cursor).toBe('3');

    const response = await fetch(`${BASE}/builds/${failed.id}/logs/stream`);
    expect(response.status).toBe(200);
    expect(response.headers.get('Content-Type')).toContain('text/event-stream');
    const reader = response.body?.getReader();
    if (!reader) throw new Error('the stream has no body');
    const decoder = new TextDecoder();
    let text = '';
    for (let read = 0; read < 200; read += 1) {
      const chunk = await reader.read();
      if (chunk.done) break;
      text += decoder.decode(chunk.value, { stream: true });
      if (text.includes('event: end')) break;
    }
    await reader.cancel();
    expect(text).toContain('id: 1');
    expect(text).toContain('event: log');
    expect(text).toContain('event: status');
    expect(text).toContain('event: end');
  });
});
