// SPDX-License-Identifier: AGPL-3.0-only
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import type { App, AppSource } from '../api/generated/model';
import { cancelMockBuild, findBuild, startMockBuild } from './builds';
import { db, findApp, mockId, now } from './db';

/**
 * The mock build walks its statuses on `setTimeout`; the test runs the clock
 * forward instead of waiting four seconds per case. `db.schedule` uses the
 * browser's `window`, so it is pointed at the test's global here.
 */

const sourceFor = (repoUrl: string): AppSource => ({
  kind: 'git',
  repo_url: repoUrl,
  ref: 'main',
  dockerfile_path: 'Dockerfile',
  context_dir: '.',
  target: null,
  build_args: [],
  secrets: [],
});

const makeApp = (repoUrl: string): App => {
  const created = now();
  const app: App = {
    id: mockId('app'),
    project_id: db.projects[0]?.id ?? mockId('prj'),
    name: `build-test-${db.apps.length}`.slice(0, 40),
    stateful: false,
    ha_policy: 'none',
    image: '',
    source: sourceFor(repoUrl),
    port: 3000,
    health_check: { path: '/', interval_s: 10, timeout_s: 2, failures: 3 },
    resources: { vcpus: 1, memory_mb: 512, disk_gb: 2 },
    placement: { node_ids: [db.node.id] },
    status: 'stopped',
    current_deployment_id: null,
    restart_generation: 0,
    created_at: created,
    updated_at: created,
  };
  db.apps.push(app);
  return app;
};

beforeAll(() => {
  (globalThis as unknown as { window: typeof globalThis }).window = globalThis;
  vi.useFakeTimers();
});

afterAll(() => {
  vi.useRealTimers();
});

describe('startMockBuild', () => {
  it('walks a successful build and deploys the built image', () => {
    const app = makeApp('https://github.com/example/docs-site.git');
    const build = startMockBuild(app);
    expect(build.status).toBe('queued');

    vi.advanceTimersByTime(1_100);
    expect(findBuild(build.id)?.status).toBe('building');
    expect(findBuild(build.id)?.commit).toMatch(/^[0-9a-f]{40}$/);

    vi.advanceTimersByTime(3_000);
    const finished = findBuild(build.id);
    expect(finished?.status).toBe('succeeded');
    expect(finished?.image).toBe(`build.quoro.invalid/${app.id}:${build.id}`);
    expect(finished?.deployment_id).not.toBeNull();
    expect(findApp(app.id)?.image).toBe(finished?.image);

    const lines = db.buildLogs[build.id] ?? [];
    expect(lines.length).toBeGreaterThan(10);
    expect(lines.map((line) => line.seq)).toEqual(
      lines.map((_line, index) => index + 1),
    );
    expect(lines.some((line) => line.vertex === 5 && line.line.includes('npm ci'))).toBe(
      true,
    );
  });

  it('fails in building at [3/4] for a repository URL containing fail', () => {
    const app = makeApp('https://github.com/example/fail-demo.git');
    const build = startMockBuild(app);

    vi.advanceTimersByTime(1_100);
    expect(findBuild(build.id)?.status).toBe('building');

    vi.advanceTimersByTime(1_400);
    const failed = findBuild(build.id);
    expect(failed?.status).toBe('failed');
    expect(failed?.failed_phase).toBe('building');
    expect(failed?.failed_step?.name).toBe('[3/4] RUN ls /does-not-exist');
    expect(failed?.failed_step?.dockerfile_line).toBe(3);
    expect(failed?.failed_step?.excerpt).toContainEqual(
      '   3 | >>> RUN ls /does-not-exist',
    );
    expect(failed?.deployment_id).toBeNull();
    expect(
      (db.buildLogs[build.id] ?? []).some(
        (line) => line.stream === 'stderr' && line.line.includes('ERROR'),
      ),
    ).toBe(true);
  });

  it('stays cancelled when the clock runs on', () => {
    const app = makeApp('https://github.com/example/docs-site.git');
    const build = startMockBuild(app);

    vi.advanceTimersByTime(1_100);
    const running = findBuild(build.id);
    expect(running).toBeDefined();
    if (running) cancelMockBuild(running);
    expect(running?.status).toBe('cancelled');

    vi.advanceTimersByTime(4_000);
    expect(findBuild(build.id)?.status).toBe('cancelled');
    expect(findBuild(build.id)?.deployment_id).toBeNull();
  });
});
