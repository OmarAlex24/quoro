// SPDX-License-Identifier: AGPL-3.0-only
import { Link } from '@tanstack/react-router';
import { useEffect, useState } from 'react';
import type { Build } from '../api/generated/model';
import { useGetApp } from '../api/generated/apps/apps';
import { useGetBuild } from '../api/generated/builds/builds';
import { isBuildTerminal, shortBuildId } from '../lib/build-status';
import { problemView } from '../lib/problem';
import { useSession } from '../session';
import { BuildView } from '../components/build-view';
import { Button } from '../components/ui/button';
import { ProblemBanner } from '../components/ui/problem-banner';
import { LoginScreen } from './login-screen';

/**
 * `/apps/$appId/builds/$buildId`: one build with its phase rail, its failing
 * step and the log streamed from the node. The live `status` events move the
 * view; the build read fills in everything else.
 */
export function BuildScreen({ appId, buildId }: { appId: string; buildId: string }) {
  const { user, loading, unauthenticated, canWrite } = useSession();

  const [streamBuild, setStreamBuild] = useState<Build | null>(null);

  const appQuery = useGetApp(appId, { query: { enabled: appId !== '' } });
  const buildQuery = useGetBuild(buildId, {
    query: {
      enabled: buildId !== '',
      refetchInterval: (query) =>
        query.state.data !== undefined && isBuildTerminal(query.state.data.status)
          ? false
          : 2_000,
    },
  });

  useEffect(() => {
    setStreamBuild(null);
  }, [buildId]);

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-4xl px-4 py-8 sm:px-6">
        <p className="text-[15px] text-ink-2">Loading…</p>
      </main>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  if (appQuery.isPending || buildQuery.isPending) {
    return (
      <main className="mx-auto w-full max-w-4xl px-4 py-8 sm:px-6">
        <div className="h-[180px] rounded-[10px] border border-line bg-surface/60" />
      </main>
    );
  }

  const app = appQuery.data;
  const build = streamBuild ?? buildQuery.data;

  if (!app || !build) {
    const view = problemView(
      appQuery.error ?? buildQuery.error,
      'see this build',
    );
    return (
      <main className="mx-auto w-full max-w-4xl px-4 py-6 sm:px-6 sm:py-8">
        <ProblemBanner view={view} />
        <Button variant="default" className="mt-4" asChild>
          <Link to="/apps/$appId" params={{ appId }}>
            Back to the app
          </Link>
        </Button>
      </main>
    );
  }

  return (
    <main className="mx-auto w-full max-w-4xl px-4 py-6 sm:px-6 sm:py-8">
      <div className="mb-3">
        <Link
          to="/apps/$appId"
          params={{ appId }}
          className="text-[13px] text-ink-2 underline-offset-4 hover:text-ink hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
        >
          {app.name}
        </Link>
      </div>

      <div className="flex flex-wrap items-end justify-between gap-3">
        <h1 className="type-display text-[24px] leading-none">
          Build <span className="type-code text-[18px]">{shortBuildId(build.id)}</span>
        </h1>
      </div>

      <section className="mt-4 rounded-[10px] border border-line bg-surface px-4 py-4 card-sheen sm:px-5">
        <BuildView
          build={build}
          appId={app.id}
          appName={app.name}
          canWrite={canWrite}
          onBuildChanged={setStreamBuild}
        />
      </section>
    </main>
  );
}
