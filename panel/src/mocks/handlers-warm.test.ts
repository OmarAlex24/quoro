// SPDX-License-Identifier: AGPL-3.0-only
import { setupServer } from 'msw/node';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import type { App, AppDetail, AppList, WarmCopy } from '../api/generated/model';
import { setRole } from './agency';
import { db, mockId, now } from './db';
import { handlers } from './handlers';

const server = setupServer(...handlers);

const BASE = 'http://panel.test/api/v1';

beforeAll(() => server.listen());
afterAll(() => server.close());
afterEach(() => vi.useRealTimers());

beforeEach(() => {
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
});

/** The nine keys `WarmCopy` requires. */
const WARM_KEYS = [
  'node_id',
  'node_pinned',
  'interval_s',
  'state',
  'last_snapshot_seq',
  'last_shipped_at',
  'lag_s',
  'extent_count',
  'error',
].sort();

const listApps = async (): Promise<App[]> => {
  const response = await fetch(`${BASE}/apps?limit=200`);
  expect(response.status).toBe(200);
  return ((await response.json()) as AppList).items;
};

const getApp = async (appId: string): Promise<AppDetail> => {
  const response = await fetch(`${BASE}/apps/${appId}`);
  expect(response.status).toBe(200);
  return (await response.json()) as AppDetail;
};

const patchApp = (appId: string, body: unknown): Promise<Response> =>
  fetch(`${BASE}/apps/${appId}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

/** Every answer of the two reads, so each rule is checked on both. */
const everyAnswer = async (): Promise<Array<{ from: string; app: App }>> => {
  const listed = await listApps();
  const read = await Promise.all(listed.map((app) => getApp(app.id)));
  return [
    ...listed.map((app) => ({ from: `listApps ${app.name}`, app })),
    ...read.map((app) => ({ from: `getApp ${app.name}`, app })),
  ];
};

const warmOf = (app: App): WarmCopy => {
  if (app.warm === null || app.warm === undefined) throw new Error(`${app.name} has no warm copy`);
  return app.warm;
};

describe('App.warm in the mock answers', () => {
  it('is present on every app, null for one that is not warm', async () => {
    for (const { from, app } of await everyAnswer()) {
      expect(Object.hasOwn(app, 'warm'), from).toBe(true);
      if (app.ha_policy !== 'warm') expect(app.warm, from).toBeNull();
    }
  });

  it('carries exactly the nine WarmCopy keys on a warm app', async () => {
    const answers = (await everyAnswer()).filter(({ app }) => app.ha_policy === 'warm');
    expect(answers.length).toBe(12);
    for (const { from, app } of answers) {
      expect(Object.keys(warmOf(app)).sort(), from).toEqual(WARM_KEYS);
    }
  });

  // Now, ten minutes on and three hours on: the progress moves with the clock.
  it.each([0, 600, 10_800])('follows what the primary answers, %i s after the seed', async (seconds) => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(Date.now() + seconds * 1_000);
    const now = Date.now();
    const answers = (await everyAnswer()).filter(({ app }) => app.ha_policy === 'warm');
    expect(answers.length).toBe(12);
    for (const { from, app } of answers) {
      const warm = warmOf(app);
      if (warm.last_snapshot_seq === null) {
        expect(warm.last_shipped_at, from).toBeNull();
        expect(warm.lag_s, from).toBeNull();
      }
      if (warm.state === 'syncing') expect(warm.last_snapshot_seq, from).toBeNull();
      if (warm.node_id === null) expect(warm.state, from).toBe('failed');
      if (warm.state === 'in_sync') {
        expect(warm.lag_s, from).not.toBeNull();
        expect(warm.lag_s!, from).toBeLessThanOrEqual(2 * warm.interval_s);
      }
      if (warm.state === 'lagging' && warm.lag_s !== null) {
        expect(warm.lag_s, from).toBeGreaterThan(2 * warm.interval_s);
      }
      // A copy completes after its snapshot is taken, so it is never older than the lag.
      if (warm.last_shipped_at !== null && warm.lag_s !== null) {
        expect((now - Date.parse(warm.last_shipped_at)) / 1_000, from).toBeLessThanOrEqual(warm.lag_s);
      }
      expect(warm.error !== null, from).toBe(warm.state === 'failed');
      expect((warm.error ?? '').length, from).toBeLessThanOrEqual(500);
      expect(warm.interval_s, from).toBeGreaterThanOrEqual(5);
      expect(warm.interval_s, from).toBeLessThanOrEqual(3_600);
    }
  });

  it('moves the in-sync copy on with the clock, and the lag of the others', async () => {
    const orders = db.apps.find((app) => app.name === 'orders')!;
    const analytics = db.apps.find((app) => app.name === 'analytics')!;
    const first = { orders: warmOf(await getApp(orders.id)), analytics: warmOf(await getApp(analytics.id)) };
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(Date.now() + 600_000);
    const later = { orders: warmOf(await getApp(orders.id)), analytics: warmOf(await getApp(analytics.id)) };
    expect(later.orders.last_snapshot_seq).toBeGreaterThanOrEqual(first.orders.last_snapshot_seq! + 19);
    expect(Date.parse(later.orders.last_shipped_at!)).toBeGreaterThan(Date.parse(first.orders.last_shipped_at!));
    expect(later.analytics.last_snapshot_seq).toBe(first.analytics.last_snapshot_seq);
    expect(later.analytics.last_shipped_at).toBe(first.analytics.last_shipped_at);
    expect(later.analytics.lag_s! - first.analytics.lag_s!).toBeGreaterThanOrEqual(599);
    // The stored app stays as seeded; only the answer moves.
    expect(orders.warm?.last_snapshot_seq).toBe(4182);
  });

  it('seeds one warm app in each state the issue lists, in a project of its own', async () => {
    const warmApps = (await listApps()).filter((app) => app.ha_policy === 'warm');
    const atlas = db.projects.find((project) => project.slug === 'atlas')!;
    expect(warmApps.every((app) => app.project_id !== atlas.id)).toBe(true);
    expect(new Set(warmApps.map((app) => app.project_id)).size).toBe(1);

    const byName = Object.fromEntries(warmApps.map((app) => [app.name, warmOf(app)]));
    // Syncing, no copy yet, the source's extents already reported.
    expect(byName.inventory).toMatchObject({ state: 'syncing', last_snapshot_seq: null });
    expect(byName.inventory!.extent_count).not.toBeNull();
    expect(byName.orders).toMatchObject({ state: 'in_sync' });
    // Lagging, well above twice the interval.
    expect(byName.analytics!.state).toBe('lagging');
    expect(byName.analytics!.lag_s!).toBeGreaterThan(4 * byName.analytics!.interval_s);
    // Failed on a node's error over several lines, with its progress kept.
    expect(byName.media!.state).toBe('failed');
    expect(byName.media!.error!.startsWith('warm node: ')).toBe(true);
    expect(byName.media!.error!.split('\n').length).toBeGreaterThan(1);
    expect(byName.media!.last_snapshot_seq).not.toBeNull();
    expect(byName.search).toMatchObject({
      state: 'failed',
      node_id: null,
      error: 'no node is eligible as the warm node',
    });
    // After a primary restart: every progress field null, both nodes connected.
    expect(byName.billing).toMatchObject({
      state: 'syncing',
      last_snapshot_seq: null,
      last_shipped_at: null,
      lag_s: null,
      extent_count: null,
      error: null,
    });
  });

  it('keeps each warm copy on a real node other than the volume', async () => {
    const nodeIds = new Set([db.node, ...db.extraNodes].map((node) => node.id));
    for (const app of (await listApps()).filter((candidate) => candidate.ha_policy === 'warm')) {
      expect(app.stateful, app.name).toBe(true);
      const volume = db.volumes.find((candidate) => candidate.app_id === app.id && candidate.state === 'ready');
      expect(volume, app.name).toBeDefined();
      expect(nodeIds.has(volume!.node_id), app.name).toBe(true);
      const warm = warmOf(app);
      if (warm.node_id !== null) {
        expect(nodeIds.has(warm.node_id), app.name).toBe(true);
        expect(warm.node_id, app.name).not.toBe(volume!.node_id);
      }
    }
  });

  it('prices the warm policy as chosen, never as not in this release', async () => {
    for (const app of (await listApps()).filter((candidate) => candidate.ha_policy === 'warm')) {
      const policies = (await getApp(app.id)).ha_cost?.policies ?? [];
      const row = policies.find((policy) => policy.policy === 'warm');
      if (warmOf(app).node_id === null) {
        expect(row, app.name).toMatchObject({ available: false, unavailable_reason: 'no_warm_node' });
      } else {
        expect(row, app.name).toMatchObject({ available: true, unavailable_reason: null, pause_estimate_ms: 0 });
      }
      const hot = policies.find((policy) => policy.policy === 'hot');
      expect(hot?.unavailable_reason, app.name).toBe('not_in_this_release');
    }
  });

  it('answers warm null for a new app', async () => {
    const response = await fetch(`${BASE}/apps`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        project_id: db.projects[0]!.id,
        name: 'fresh-warm-check',
        image: 'docker.io/library/nginx:alpine',
        port: 80,
      }),
    });
    expect(response.status).toBe(201);
    const created = (await response.json()) as App;
    expect(Object.hasOwn(created, 'warm')).toBe(true);
    expect(created.warm).toBeNull();
    expect((await getApp(created.id)).warm).toBeNull();
  });

  it('still refuses warm, and drops the copy when a warm app leaves the policy', async () => {
    const blog = db.apps.find((app) => app.name === 'blog')!;
    expect((await patchApp(blog.id, { ha_policy: 'warm' })).status).toBe(422);

    const billing = db.apps.find((app) => app.name === 'billing')!;
    const before = { ha_policy: billing.ha_policy, warm: billing.warm };
    try {
      const response = await patchApp(billing.id, { ha_policy: 'none' });
      expect(response.status).toBe(200);
      const answer = (await response.json()) as App;
      expect(answer.ha_policy).toBe('none');
      expect(answer.warm).toBeNull();
      expect((await getApp(billing.id)).warm).toBeNull();
      // Back to warm is refused, as the issue says, so the copy cannot return.
      expect((await patchApp(billing.id, { ha_policy: 'warm' })).status).toBe(422);
    } finally {
      billing.ha_policy = before.ha_policy;
      billing.warm = before.warm;
    }
  });
});
