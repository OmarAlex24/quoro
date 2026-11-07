import { Link } from '@tanstack/react-router';
import { useListApps } from '../api/generated/apps/apps';
import { appsPollingInterval } from '../lib/app-status';
import { problemView } from '../lib/problem';
import { useSession } from '../session';
import { AppBay } from '../components/app-bay';
import { Button } from '../components/ui/button';
import { ProblemBanner } from '../components/ui/problem-banner';
import { LoginScreen } from './login-screen';

export function AppsScreen() {
  const { user, loading, unauthenticated, canWrite } = useSession();

  const appsQuery = useListApps(undefined, {
    query: {
      refetchInterval: (query) => appsPollingInterval(query.state.data?.items ?? []),
    },
  });

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-6xl px-4 py-8 sm:px-6">
        <p className="text-[15px] text-ink-2">Loading…</p>
      </main>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  const apps = appsQuery.data?.items ?? [];
  const listError =
    appsQuery.error !== null && appsQuery.error !== undefined
      ? problemView(appsQuery.error, 'see apps')
      : null;

  return (
    <main className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 sm:py-8">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="type-display text-[24px] leading-none">Apps</h1>
        {canWrite ? (
          <Button variant="primary" asChild>
            <Link to="/apps/new">Deploy an app</Link>
          </Button>
        ) : null}
      </div>

      {listError ? <ProblemBanner view={listError} className="mt-4" /> : null}

      <div className="mt-5 flex flex-col gap-2">
        {appsQuery.isPending ? (
          <div className="h-[104px] rounded-[10px] border border-line bg-surface/60" />
        ) : apps.length === 0 ? (
          <div className="rounded-[10px] border border-dashed border-line bg-surface/50 px-4 py-10 text-center">
            <p className="text-[15px] text-ink">
              No apps yet. Deploy an image to start one.
            </p>
            {canWrite ? (
              <Button variant="primary" className="mt-4" asChild>
                <Link to="/apps/new">Deploy an app</Link>
              </Button>
            ) : null}
          </div>
        ) : (
          apps.map((app) => <AppBay key={app.id} app={app} />)
        )}
      </div>
    </main>
  );
}
