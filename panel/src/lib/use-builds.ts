import { useQueries } from '@tanstack/react-query';
import { getListAppBuildsQueryOptions } from '../api/generated/builds/builds';
import type { App, Build } from '../api/generated/model';

/** The page of builds the grid and the canvas share, newest first. */
export const RECENT_BUILDS = { limit: 10 } as const;

/**
 * The recent builds of every app that builds from Git, newest first, keyed by
 * app id. An app given by image has no builds and is not asked. A refusal or
 * an error leaves that app out, so the view shows less rather than fails.
 */
export function useRecentBuilds(apps: ReadonlyArray<Pick<App, 'id' | 'source'>>): Record<string, Build[]> {
  const gitApps = apps.filter((app) => app.source != null);
  const results = useQueries({
    queries: gitApps.map((app) =>
      getListAppBuildsQueryOptions(app.id, RECENT_BUILDS, {
        query: { staleTime: 30_000, refetchInterval: 30_000, retry: false },
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
