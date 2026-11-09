import { useQueries, type QueryClient } from '@tanstack/react-query';
import { getListAppBuildsQueryKey, getListAppBuildsQueryOptions } from '../api/generated/builds/builds';
import type { App, Build, BuildList } from '../api/generated/model';
import { newestBuildLive } from './app-status';

/** The page of builds the grid, the canvas and the sheet's header share, newest first. */
export const RECENT_BUILDS = { limit: 10 } as const;

/**
 * Whether an app's newest build runs, from the shared page already in the
 * cache. A refetch rule runs outside render, so it reads the cache instead of
 * a hook.
 */
export const cachedBuildLive = (client: QueryClient, appId: string): boolean =>
  newestBuildLive(client.getQueryData<BuildList>(getListAppBuildsQueryKey(appId, RECENT_BUILDS))?.items);

/**
 * The recent builds of every app that builds from Git, newest first, keyed by
 * app id. An app given by image has no builds and is not asked. A refusal or
 * an error leaves that app out, so the view shows less rather than fails.
 * While an app's newest build runs, its page refreshes every 2 s, so the card
 * and the sheet see the build end when the Deployments tab does.
 */
export function useRecentBuilds(apps: ReadonlyArray<Pick<App, 'id' | 'source'>>): Record<string, Build[]> {
  const gitApps = apps.filter((app) => app.source != null);
  const results = useQueries({
    queries: gitApps.map((app) =>
      getListAppBuildsQueryOptions(app.id, RECENT_BUILDS, {
        query: {
          staleTime: 30_000,
          refetchInterval: (query) => (newestBuildLive(query.state.data?.items) ? 2_000 : 30_000),
          retry: false,
        },
      }),
    ),
  });
  const out: Record<string, Build[]> = {};
  gitApps.forEach((app, index) => {
    const items = results[index]?.data?.items;
    if (items) out[app.id] = items;
  });
  return out;
}
