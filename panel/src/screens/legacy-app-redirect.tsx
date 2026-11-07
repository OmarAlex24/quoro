import { Link, Navigate } from '@tanstack/react-router';
import { useGetApp } from '../api/generated/apps/apps';
import { problemView } from '../lib/problem';
import { appHome } from '../lib/routes';
import { Button } from '../components/ui/button';
import { ProblemBanner } from '../components/ui/problem-banner';

/**
 * `/apps/$appId` and `/apps/$appId/builds/$buildId` from before the redesign:
 * the app's project comes from the API, then the URL is replaced with the
 * project's, the app open on its Deployments tab.
 */
export function LegacyAppRedirect({ appId, buildId }: { appId: string; buildId?: string }) {
  const appQuery = useGetApp(appId, { query: { retry: false } });
  const app = appQuery.data;

  if (app) {
    const home = buildId
      ? appHome({ path: '/apps/$appId/builds/$buildId', appId, buildId, projectId: app.project_id })
      : appHome({ path: '/apps/$appId', appId, projectId: app.project_id });
    return <Navigate to={home.to} params={home.params} search={home.search} replace />;
  }

  if (appQuery.error !== null && appQuery.error !== undefined) {
    return (
      <main className="mx-auto w-full max-w-4xl px-4 py-6 sm:px-6 sm:py-8">
        <ProblemBanner view={problemView(appQuery.error, 'see this app')} />
        <Button variant="default" className="mt-4" asChild>
          <Link to="/projects">Back to projects</Link>
        </Button>
      </main>
    );
  }

  return (
    <main className="mx-auto w-full max-w-4xl px-4 py-8 sm:px-6">
      <p className="text-[15px] text-ink-2">Opening the app…</p>
    </main>
  );
}
