// SPDX-License-Identifier: AGPL-3.0-only
import type { QueryClient } from '@tanstack/react-query';
import { useMemo } from 'react';
import { getListAppBuildsQueryKey, useListAppBuilds } from '../api/generated/builds/builds';
import {
  getListAppDeploymentsQueryKey,
  useListAppDeployments,
} from '../api/generated/deployments/deployments';
import type { App, BuildList, DeploymentList } from '../api/generated/model';
import { appsPollingInterval } from './app-status';
import { isBuildTerminal } from './build-status';
import { isRunLive, joinRuns, runPhase, type DeployRun, type RunPhase } from './deploy-tape';
import { cachedBuildLive } from './use-builds';

/** One page of builds and deployments: the newest run and its history. */
export const RUN_PAGE = { limit: 20 } as const;

export interface AppRuns {
  /** Builds joined to their deployments, newest run first (`joinRuns`). */
  readonly runs: DeployRun[];
  /** What the newest run is doing; undefined until both lists are read. */
  readonly phase: RunPhase | null | undefined;
  readonly error: unknown;
  readonly loading: boolean;
}

/**
 * An app's runs as the Deployments tab reads them: its builds, every 2 s
 * while the newest one runs, and its deployments, every 2 s while the newest
 * run moves, its build included; every 10 s otherwise. The sheet's header and
 * the canvas card of the open app read the same two lists with the same rules
 * (the same query keys, so no second request), so they say what the tab says
 * at the moment it says it. Nothing is read until `app` is.
 */
export function useAppRuns(app: Pick<App, 'id' | 'source'> | undefined): AppRuns {
  const hasSource = app?.source != null;
  const buildsQuery = useListAppBuilds(app?.id ?? '', RUN_PAGE, {
    query: {
      enabled: hasSource,
      refetchInterval: (query) => {
        const newest = query.state.data?.items[0];
        return newest !== undefined && !isBuildTerminal(newest.status) ? 2_000 : 10_000;
      },
    },
  });
  const builds = useMemo(
    () => (hasSource ? (buildsQuery.data?.items ?? []) : []),
    [hasSource, buildsQuery.data],
  );
  // When the build ends, the deployment it starts is read without waiting 10 s.
  const deploymentsQuery = useListAppDeployments(app?.id ?? '', RUN_PAGE, {
    query: {
      enabled: app !== undefined,
      refetchInterval: (query) =>
        isRunLive(joinRuns(builds, query.state.data?.items)[0]) ? 2_000 : 10_000,
    },
  });
  const deployments = deploymentsQuery.data?.items;
  const runs = useMemo(() => joinRuns(builds, deployments), [builds, deployments]);
  const loading = deploymentsQuery.isPending || (hasSource && buildsQuery.isPending);
  return {
    runs,
    phase: loading ? undefined : runPhase(runs[0]),
    error: deploymentsQuery.error ?? (hasSource ? buildsQuery.error : null),
    loading,
  };
}

/**
 * Whether an app's newest run moves, from the two lists `useAppRuns` keeps in
 * the cache. A refetch rule runs outside render, so it reads the cache instead
 * of a hook.
 */
export const cachedRunLive = (client: QueryClient, app: Pick<App, 'id' | 'source'>): boolean => {
  const deployments = client.getQueryData<DeploymentList>(getListAppDeploymentsQueryKey(app.id, RUN_PAGE));
  if (!deployments) return false;
  const builds =
    app.source != null
      ? (client.getQueryData<BuildList>(getListAppBuildsQueryKey(app.id, RUN_PAGE))?.items ?? [])
      : [];
  return isRunLive(joinRuns(builds, deployments.items)[0]);
};

/**
 * How often the service sheet reads its app: every 2 s while the app
 * deploys, while its newest run in the Deployments tab's lists moves, or
 * while its newest build on the page the canvas shares runs; every 10 s
 * otherwise. The lists refresh every 2 s at most, so this never polls faster
 * than the tab. The sheet subscribes to those lists, so it renders again when
 * they change and this rule is applied again then, not only when the app read
 * returns.
 */
export const appReadInterval = (
  client: QueryClient,
  app: Pick<App, 'id' | 'status' | 'source'> | undefined,
): number =>
  appsPollingInterval(
    app ? [app] : [],
    app !== undefined &&
      (cachedRunLive(client, app) || (app.source != null && cachedBuildLive(client, app.id))),
  );
