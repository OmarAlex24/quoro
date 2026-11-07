import { getRouteApi, Link } from '@tanstack/react-router';
import type { ReactNode } from 'react';
import { useGetApp, useListApps } from '../api/generated/apps/apps';
import type { App, Project } from '../api/generated/model';
import { useListNodes } from '../api/generated/nodes/nodes';
import { useListProjects } from '../api/generated/projects/projects';
import { useGetVm } from '../api/generated/vms/vms';
import { problemView } from '../lib/problem';
import { PROJECT_LIST_PARAMS } from '../lib/projects';
import { AppMetricsBay } from '../components/metrics-bay';
import { LogTape } from '../components/log-tape';
import { ProjectShell } from '../components/shell/project-shell';
import { SessionGate } from '../components/shell/side-shell';
import { Button } from '../components/ui/button';
import { ProblemBanner } from '../components/ui/problem-banner';
import { AppScreen } from './app-screen';
import { AppsScreen } from './apps-screen';
import { BuildScreen } from './build-screen';
import { VmsScreen } from './vms-screen';

const projectRoute = getRouteApi('/projects/$projectId');

/**
 * `/projects/$projectId`: the project's top bar and rail around what is open.
 * Until the canvas and the service sheet arrive, each place shows the screen
 * that held it before: the project's apps on the canvas, the app screen for
 * `?app=`, the build screen for `?app=&build=`, the VM bay for `?vm=`.
 */
export function ProjectScreen() {
  return (
    <SessionGate>
      <ProjectPage />
    </SessionGate>
  );
}

function ProjectPage() {
  const { projectId } = projectRoute.useParams();
  const search = projectRoute.useSearch();
  const view = search.view ?? 'canvas';

  const projectsQuery = useListProjects(PROJECT_LIST_PARAMS);
  const projects = projectsQuery.data?.items ?? [];
  const project = projects.find((item) => item.id === projectId);
  const missing = projectsQuery.isSuccess && project === undefined;

  const appQuery = useGetApp(search.app ?? '', {
    query: { enabled: search.app !== undefined, retry: false },
  });
  const vmQuery = useGetVm(search.vm ?? '', {
    query: { enabled: search.app === undefined && search.vm !== undefined, retry: false },
  });
  const leaf = search.app ? appQuery.data?.name : search.vm ? vmQuery.data?.name : undefined;

  let content: ReactNode;
  if (missing) {
    content = <ProjectMissing />;
  } else if (search.app && search.build) {
    content = <BuildScreen key={search.build} appId={search.app} buildId={search.build} />;
  } else if (search.app) {
    content = <AppScreen key={search.app} appId={search.app} />;
  } else if (search.vm) {
    content = <VmsScreen key={search.vm} projectId={projectId} vmId={search.vm} />;
  } else if (view === 'metrics') {
    content = <ProjectMetrics projectId={projectId} />;
  } else if (view === 'logs') {
    content = <ProjectLogs projectId={projectId} />;
  } else if (view === 'settings') {
    content = project ? <ProjectSettings project={project} /> : null;
  } else {
    content = <AppsScreen projectId={projectId} />;
  }

  return (
    <ProjectShell
      projectId={projectId}
      project={project}
      projects={projects}
      leaf={leaf}
      view={view}
      canvas={!missing && !search.app && !search.vm && view === 'canvas'}
    >
      {content}
    </ProjectShell>
  );
}

function ProjectMissing() {
  return (
    <main className="mx-auto w-full max-w-4xl px-4 py-6 sm:px-6 sm:py-8">
      <h1 className="type-display text-[24px] leading-none">Project not found</h1>
      <p className="mt-2 text-[14px] text-ink-2">
        No project with this id exists, or your session cannot see it.
      </p>
      <Button variant="default" className="mt-4" asChild>
        <Link to="/projects">All projects</Link>
      </Button>
    </main>
  );
}

/** The project's apps, each under its name, for a view that shows one thing per app. */
function PerApp({
  projectId,
  title,
  lede,
  children,
}: {
  projectId: string;
  title: string;
  lede: string;
  children: (app: App) => ReactNode;
}) {
  const appsQuery = useListApps({ project_id: projectId });
  const apps = appsQuery.data?.items ?? [];
  const listError =
    appsQuery.error !== null && appsQuery.error !== undefined
      ? problemView(appsQuery.error, 'see apps')
      : null;

  return (
    <main className="mx-auto w-full max-w-4xl px-4 py-6 sm:px-6 sm:py-8">
      <h1 className="type-display text-[24px] leading-none">{title}</h1>
      <p className="mt-2 text-[14px] text-ink-2">{lede}</p>
      {listError ? <ProblemBanner view={listError} className="mt-4" /> : null}
      {appsQuery.isPending ? (
        <div className="mt-5 h-[104px] rounded-[10px] border border-line bg-surface/60" />
      ) : apps.length === 0 && !listError ? (
        <p className="mt-5 rounded-[10px] border border-dashed border-line bg-surface/50 px-4 py-6 text-[14px] text-ink-2">
          No apps in this project yet.
        </p>
      ) : (
        apps.map((app) => (
          <section key={app.id} className="mt-6" aria-label={app.name}>
            <h2 className="type-display-light mb-2 text-[15px] leading-none">
              <Link
                to="/projects/$projectId"
                params={{ projectId }}
                search={{ app: app.id, tab: 'deployments' }}
                className="text-ink underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
              >
                {app.name}
              </Link>
            </h2>
            {children(app)}
          </section>
        ))
      )}
    </main>
  );
}

function ProjectMetrics({ projectId }: { projectId: string }) {
  const nodes = useListNodes(undefined, { query: { staleTime: 10_000 } });
  const nodeNames = Object.fromEntries(
    (nodes.data?.items ?? []).map((node) => [node.id, node.name]),
  );
  return (
    <PerApp
      projectId={projectId}
      title="Metrics"
      lede="CPU and memory of each app's instances over the last 10 minutes."
    >
      {(app) => <AppMetricsBay appId={app.id} nodeNames={nodeNames} />}
    </PerApp>
  );
}

function ProjectLogs({ projectId }: { projectId: string }) {
  return (
    <PerApp projectId={projectId} title="Logs" lede="What each app writes, as it writes it.">
      {(app) => <LogTape appId={app.id} />}
    </PerApp>
  );
}

function ProjectSettings({ project }: { project: Project }) {
  const rows: Array<[string, ReactNode]> = [
    ['Name', project.name],
    ['Slug', <span className="type-code">{project.slug}</span>],
    ['Id', <span className="type-code break-all">{project.id}</span>],
    ['Created', new Date(project.created_at).toLocaleString()],
  ];
  return (
    <main className="mx-auto w-full max-w-4xl px-4 py-6 sm:px-6 sm:py-8">
      <h1 className="type-display text-[24px] leading-none">Project settings</h1>
      <dl className="mt-5 grid grid-cols-[minmax(0,110px)_minmax(0,1fr)] gap-x-4 gap-y-2.5 rounded-[10px] border border-line bg-surface px-4 py-4 text-[13.5px] sm:px-5">
        {rows.map(([label, value]) => (
          <div key={label} className="contents">
            <dt className="text-ink-2">{label}</dt>
            <dd className="min-w-0 text-ink">{value}</dd>
          </div>
        ))}
      </dl>
    </main>
  );
}
