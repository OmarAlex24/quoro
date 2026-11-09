// SPDX-License-Identifier: AGPL-3.0-only
import { QueryClient } from '@tanstack/react-query';
import { describe, expect, it } from 'vitest';
import { getListAppBuildsQueryKey } from '../api/generated/builds/builds';
import { getListAppDeploymentsQueryKey } from '../api/generated/deployments/deployments';
import type { App, AppSource, AppStatus, Build, Deployment } from '../api/generated/model';
import { serviceLampState } from './app-status';
import { activeDeploymentId, joinRuns, runPhase, runTag, runTimeline } from './deploy-tape';
import { appReadInterval, cachedRunLive, RUN_PAGE } from './use-app-runs';
import { RECENT_BUILDS } from './use-builds';

const T0 = Date.parse('2026-11-09T17:00:00Z');
const at = (seconds: number): string => new Date(T0 + seconds * 1_000).toISOString();
const APP = 'app_A';

const SOURCE: AppSource = {
  kind: 'git',
  repo_url: 'https://github.com/example/api.git',
  ref: 'main',
  dockerfile_path: 'Dockerfile',
  context_dir: '.',
  target: null,
  build_args: [],
  secrets: [],
};

const deployment = (overrides: Partial<Deployment> = {}): Deployment => ({
  id: 'dep_A',
  app_id: APP,
  image: 'ghcr.io/example/api:1',
  image_digest: null,
  status: 'succeeded',
  phase: 'health',
  error: null,
  trigger: 'api',
  created_by_type: 'user',
  created_by_id: 'usr_A',
  created_at: at(0),
  finished_at: at(4),
  ...overrides,
});

const build = (overrides: Partial<Build> = {}): Build => ({
  id: 'bld_A',
  app_id: APP,
  node_id: 'nod_A',
  source: SOURCE,
  status: 'succeeded',
  commit: '4f2a9c1e8b7d6c5a4f3e2d1c0b9a8f7e6d5c4b3a',
  image: `build.quoro.invalid/${APP}:bld_A`,
  image_digest: null,
  failed_phase: null,
  error: null,
  failed_step: null,
  deployment_id: 'dep_A',
  trigger: 'api',
  refresh_base: false,
  created_by_type: 'user',
  created_by_id: 'usr_A',
  created_at: at(0),
  started_at: at(1),
  finished_at: at(6),
  ...overrides,
});

type SheetApp = Pick<App, 'id' | 'status' | 'source'>;

/**
 * One moment of the open sheet, from what each read last said: the app read,
 * and the Deployments tab's two lists, which the sheet also reads
 * (`useAppRuns`). `recent` is the page of builds the canvas shares.
 */
const sheet = (
  app: SheetApp,
  builds: Build[],
  deployments: Deployment[],
  recent: Build[] = builds,
) => {
  const client = new QueryClient();
  client.setQueryData(getListAppDeploymentsQueryKey(app.id, RUN_PAGE), { items: deployments, next_cursor: null });
  if (app.source != null) {
    client.setQueryData(getListAppBuildsQueryKey(app.id, RUN_PAGE), { items: builds, next_cursor: null });
    client.setQueryData(getListAppBuildsQueryKey(app.id, RECENT_BUILDS), { items: recent, next_cursor: null });
  }
  // What `useAppRuns` hands the header, and what the tab draws.
  const runs = joinRuns(app.source != null ? builds : [], deployments);
  const timelines = runs.map((run) => runTimeline(run, undefined, T0 + 400_000));
  const activeId = activeDeploymentId(timelines);
  return {
    header: serviceLampState(app.status, recent, runPhase(runs[0])).statusText,
    tab: runTag(timelines[0]!, activeId).word,
    interval: appReadInterval(client, app),
  };
};

const gitApp = (status: AppStatus): SheetApp => ({ id: APP, status, source: SOURCE });
const imageApp = (status: AppStatus): SheetApp => ({ id: APP, status, source: null });

/**
 * A Git build and the deployment it starts, read as on a real cluster: the
 * app read lags the two lists, so it still says `running` while the build
 * runs and its deployment starts, then `deploying` after the lists say the
 * run is deployed. The header says what the tab's tag says while the run
 * moves, and the app read refreshes every 2 s through it.
 */
describe("the sheet's header through a Git build", () => {
  const previous = build({ id: 'bld_P', deployment_id: 'dep_P', created_at: at(0), started_at: at(1), finished_at: at(6) });
  const previousDeployment = deployment({ id: 'dep_P', trigger: 'build', created_at: at(6), finished_at: at(8) });
  const next = (overrides: Partial<Build>) =>
    build({ id: 'bld_N', created_at: at(300), started_at: at(301), finished_at: null, deployment_id: null, ...overrides });
  const newDeployment = (overrides: Partial<Deployment>) =>
    deployment({ id: 'dep_N', trigger: 'build', created_at: at(311), finished_at: null, ...overrides });
  const built = next({ status: 'succeeded', finished_at: at(310), deployment_id: 'dep_N' });

  const steps = [
    {
      what: 'nothing runs',
      app: 'running',
      builds: [previous],
      deployments: [previousDeployment],
      header: 'Running',
      tab: 'Active',
      interval: 10_000,
    },
    {
      what: 'the build is queued',
      app: 'running',
      builds: [next({ status: 'queued', started_at: null }), previous],
      deployments: [previousDeployment],
      header: 'Building',
      tab: 'Building',
      interval: 2_000,
    },
    {
      what: 'the build builds the image',
      app: 'running',
      builds: [next({ status: 'building' }), previous],
      deployments: [previousDeployment],
      header: 'Building',
      tab: 'Building',
      interval: 2_000,
    },
    {
      what: 'the deployments read lists the new deployment before the builds read ends the build',
      app: 'running',
      builds: [next({ status: 'exporting' }), previous],
      deployments: [newDeployment({ status: 'preparing', phase: 'pull' }), previousDeployment],
      header: 'Building',
      tab: 'Building',
      interval: 2_000,
    },
    {
      what: 'the builds read names a deployment the deployments read does not have yet',
      app: 'running',
      builds: [built, previous],
      deployments: [previousDeployment],
      header: 'Deploying',
      tab: 'Deploying',
      interval: 2_000,
    },
    {
      what: 'the new deployment rolls out and the app read still says running',
      app: 'running',
      builds: [built, previous],
      deployments: [newDeployment({ status: 'starting', phase: 'boot' }), previousDeployment],
      header: 'Deploying',
      tab: 'Deploying',
      interval: 2_000,
    },
    {
      what: 'the lists say deployed and the app read still says deploying',
      app: 'deploying',
      builds: [built, previous],
      deployments: [newDeployment({ status: 'succeeded', finished_at: at(313) }), previousDeployment],
      header: 'Deploying',
      tab: 'Active',
      interval: 2_000,
    },
    {
      what: 'the app read catches up',
      app: 'running',
      builds: [built, previous],
      deployments: [newDeployment({ status: 'succeeded', finished_at: at(313) }), previousDeployment],
      header: 'Running',
      tab: 'Active',
      interval: 10_000,
    },
  ] as const;

  for (const step of steps) {
    it(`reads ${step.header} every ${step.interval / 1_000} s when ${step.what}`, () => {
      const view = sheet(gitApp(step.app), [...step.builds], [...step.deployments]);
      expect(view).toEqual({ header: step.header, tab: step.tab, interval: step.interval });
    });
  }

  it('says Building with the tab while the shared page of builds still says nothing runs', () => {
    const view = sheet(gitApp('running'), [next({ status: 'preparing' }), previous], [previousDeployment], [previous]);
    expect(view).toEqual({ header: 'Building', tab: 'Building', interval: 2_000 });
  });

  it('falls back to the app once the build failed', () => {
    const failed = next({ status: 'failed', failed_phase: 'building', finished_at: at(320) });
    const view = sheet(gitApp('running'), [failed, previous], [previousDeployment]);
    expect(view).toEqual({ header: 'Running', tab: 'Failed', interval: 10_000 });
  });
});

describe("the sheet's header through an image redeploy", () => {
  const current = deployment({ id: 'dep_A', created_at: at(0), finished_at: at(4) });
  const redeploy = (overrides: Partial<Deployment>) =>
    deployment({ id: 'dep_R', trigger: 'redeploy', created_at: at(100), finished_at: null, ...overrides });

  const steps = [
    { what: 'nothing runs', app: 'running', deployments: [current], header: 'Running', tab: 'Active', interval: 10_000 },
    {
      what: 'the redeploy is pending and the app read still says running',
      app: 'running',
      deployments: [redeploy({ status: 'pending', phase: null }), current],
      header: 'Deploying',
      tab: 'Deploying',
      interval: 2_000,
    },
    {
      what: 'the redeploy prepares the root disk',
      app: 'running',
      deployments: [redeploy({ status: 'preparing', phase: 'rootfs' }), current],
      header: 'Deploying',
      tab: 'Deploying',
      interval: 2_000,
    },
    {
      what: 'the redeploy boots',
      app: 'deploying',
      deployments: [redeploy({ status: 'starting', phase: 'boot' }), current],
      header: 'Deploying',
      tab: 'Deploying',
      interval: 2_000,
    },
    {
      what: 'the list says deployed and the app read still says deploying',
      app: 'deploying',
      deployments: [redeploy({ status: 'succeeded', finished_at: at(104) }), current],
      header: 'Deploying',
      tab: 'Active',
      interval: 2_000,
    },
    {
      what: 'the app read catches up',
      app: 'running',
      deployments: [redeploy({ status: 'succeeded', finished_at: at(104) }), current],
      header: 'Running',
      tab: 'Active',
      interval: 10_000,
    },
  ] as const;

  for (const step of steps) {
    it(`reads ${step.header} every ${step.interval / 1_000} s when ${step.what}`, () => {
      const view = sheet(imageApp(step.app), [], [...step.deployments]);
      expect(view).toEqual({ header: step.header, tab: step.tab, interval: step.interval });
    });
  }
});

describe('cachedRunLive', () => {
  it('is false until the deployments list is read', () => {
    const client = new QueryClient();
    client.setQueryData(getListAppBuildsQueryKey(APP, RUN_PAGE), {
      items: [build({ status: 'building', deployment_id: null, finished_at: null })],
      next_cursor: null,
    });
    expect(cachedRunLive(client, gitApp('running'))).toBe(false);
  });

  it('leaves builds out for an app given by image', () => {
    const client = new QueryClient();
    client.setQueryData(getListAppDeploymentsQueryKey(APP, RUN_PAGE), { items: [deployment()], next_cursor: null });
    client.setQueryData(getListAppBuildsQueryKey(APP, RUN_PAGE), {
      items: [build({ status: 'building', deployment_id: null, created_at: at(50), finished_at: null })],
      next_cursor: null,
    });
    expect(cachedRunLive(client, imageApp('running'))).toBe(false);
    expect(cachedRunLive(client, gitApp('running'))).toBe(true);
  });
});

describe('appReadInterval', () => {
  it('keeps the app rule before the lists are read', () => {
    const client = new QueryClient();
    expect(appReadInterval(client, undefined)).toBe(10_000);
    expect(appReadInterval(client, gitApp('running'))).toBe(10_000);
    expect(appReadInterval(client, gitApp('deploying'))).toBe(2_000);
    client.setQueryData(getListAppBuildsQueryKey(APP, RECENT_BUILDS), {
      items: [build({ status: 'building', deployment_id: null, finished_at: null })],
      next_cursor: null,
    });
    expect(appReadInterval(client, gitApp('running'))).toBe(2_000);
  });
});
