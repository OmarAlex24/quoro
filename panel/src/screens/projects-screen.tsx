import { getRouteApi, Link } from '@tanstack/react-router';
import { Plus } from 'lucide-react';
import { useListApps } from '../api/generated/apps/apps';
import { useListProjects } from '../api/generated/projects/projects';
import { shortId } from '../lib/agency-format';
import { appsPollingInterval } from '../lib/app-status';
import { problemView } from '../lib/problem';
import { groupAppsByProject, PROJECT_LIST_PARAMS, servicesLine } from '../lib/projects';
import { useSession } from '../session';
import { AppBay } from '../components/app-bay';
import { Button } from '../components/ui/button';
import { ProblemBanner } from '../components/ui/problem-banner';
import { DeployScreen } from './deploy-screen';

const projectsRoute = getRouteApi('/shell/projects');

/**
 * `/projects`: every project with its apps, as the apps rack showed them.
 * The project tiles replace this list later. `?add=app` opens the deploy
 * work order that `/apps/new` used to be.
 */
export function ProjectsScreen() {
  const search = projectsRoute.useSearch();
  if (search.add === 'app') return <DeployScreen />;
  return <ProjectsList />;
}

function ProjectsList() {
  const { canWrite } = useSession();
  const projectsQuery = useListProjects(PROJECT_LIST_PARAMS);
  const appsQuery = useListApps(undefined, {
    query: {
      refetchInterval: (query) => appsPollingInterval(query.state.data?.items ?? []),
    },
  });

  const groups = groupAppsByProject(
    projectsQuery.data?.items ?? [],
    appsQuery.data?.items ?? [],
  );
  const error = projectsQuery.error ?? appsQuery.error;
  const listError = error !== null && error !== undefined ? problemView(error, 'see projects') : null;
  const pending = projectsQuery.isPending || appsQuery.isPending;

  const deploy = canWrite ? (
    <Button variant="primary" asChild>
      <Link to="/projects" search={{ add: 'app' }}>
        <Plus strokeWidth={1.7} aria-hidden="true" className="size-4" />
        Deploy an app
      </Link>
    </Button>
  ) : null;

  return (
    <main className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 sm:py-8">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="type-display text-[24px] leading-none">Projects</h1>
        {deploy}
      </div>

      {listError ? <ProblemBanner view={listError} className="mt-4" /> : null}

      {pending ? (
        <div className="mt-5 h-[104px] rounded-[10px] border border-line bg-surface/60" />
      ) : groups.length === 0 ? (
        <div className="mt-5 rounded-[10px] border border-dashed border-line bg-surface/50 px-4 py-10 text-center">
          <p className="text-[15px] text-ink">
            No projects yet. Deploying an app creates the first one.
          </p>
          {deploy ? <div className="mt-4">{deploy}</div> : null}
        </div>
      ) : (
        <div className="mt-6 flex flex-col gap-7">
          {groups.map((group) => {
            const name = group.project?.name ?? shortId(group.projectId);
            const headingId = `project-${group.projectId}`;
            return (
              <section key={group.projectId} aria-labelledby={headingId}>
                <div className="mb-2.5 flex flex-wrap items-baseline gap-x-3 gap-y-1">
                  <h2 id={headingId} className="type-bay-title text-[17px] leading-tight">
                    <Link
                      to="/projects/$projectId"
                      params={{ projectId: group.projectId }}
                      className="text-ink underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
                    >
                      {name}
                    </Link>
                  </h2>
                  <span className="type-data text-[12.5px] text-ink-3">
                    {servicesLine(group.apps.length)}
                  </span>
                </div>
                {group.apps.length === 0 ? (
                  <p className="rounded-[10px] border border-dashed border-line bg-surface/50 px-4 py-5 text-[13.5px] text-ink-2">
                    Nothing runs in {name} yet.
                  </p>
                ) : (
                  <div className="flex flex-col gap-2">
                    {group.apps.map((app) => (
                      <AppBay key={app.id} app={app} />
                    ))}
                  </div>
                )}
              </section>
            );
          })}
        </div>
      )}
    </main>
  );
}
