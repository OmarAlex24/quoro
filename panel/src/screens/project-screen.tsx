import { getRouteApi, Link, useNavigate } from '@tanstack/react-router';
import { useEffect, useState, type ReactNode } from 'react';
import { useGetApp, useListApps } from '../api/generated/apps/apps';
import type { App, Project } from '../api/generated/model';
import { useListNodes } from '../api/generated/nodes/nodes';
import { useListProjects } from '../api/generated/projects/projects';
import { useGetVm } from '../api/generated/vms/vms';
import { problemView } from '../lib/problem';
import { PROJECT_LIST_PARAMS } from '../lib/projects';
import { sheetTab, vmSheetTab, type AddKind } from '../lib/routes';
import { appKey, vmKey } from '../lib/services';
import { useSession } from '../session';
import { CommandMenu } from '../components/command-menu';
import { CreateSheet } from '../components/create-sheet';
import { AppMetricsBay } from '../components/metrics-bay';
import { LogTape } from '../components/log-tape';
import { ProjectCanvas } from '../components/project-canvas';
import { ServiceSheet } from '../components/service-sheet';
import { VmSheet } from '../components/vm-sheet';
import { ProjectShell } from '../components/shell/project-shell';
import { SessionGate } from '../components/shell/side-shell';
import { Button } from '../components/ui/button';
import { ProblemBanner } from '../components/ui/problem-banner';

const projectRoute = getRouteApi('/projects/$projectId');

/**
 * `/projects/$projectId`: the project's top bar and rail around what is open.
 * The canvas shows the project's services; `?add=` opens a create sheet over
 * it and `?app=` the service sheet, on the tab in `?tab=` and the build in
 * `?build=`; `?vm=` opens a VM in the same kind of sheet, on Overview or
 * Settings. The canvas stays behind either sheet.
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
  const navigate = useNavigate();
  const { canWrite } = useSession();
  const view = search.view ?? 'canvas';
  const [menuOpen, setMenuOpen] = useState(false);

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
  const serviceApp = missing ? undefined : search.app;
  // An app wins when the URL names both.
  const serviceVm = missing || serviceApp ? undefined : search.vm;
  const onCanvas = !missing && view === 'canvas';
  const open = serviceApp !== undefined || serviceVm !== undefined;
  const sheet = canWrite && onCanvas && !open ? search.add : undefined;
  const canAdd = canWrite && onCanvas && !open;
  // The card the open sheet belongs to: the VM's own, or its app's for an instance.
  const vmAppId = serviceVm && vmQuery.data?.id === serviceVm ? vmQuery.data.app_id : null;
  const selectedKey = serviceApp
    ? appKey(serviceApp)
    : serviceVm
      ? vmAppId
        ? appKey(vmAppId)
        : vmKey(serviceVm)
      : undefined;
  const shortcut = canAdd && sheet === undefined;

  // Cmd+K (Ctrl+K elsewhere) opens the Add menu while the canvas is in view
  // and no create sheet covers it.
  useEffect(() => {
    if (!shortcut) return;
    const onKey = (event: KeyboardEvent) => {
      if (event.key.toLowerCase() !== 'k' || !(event.metaKey || event.ctrlKey)) return;
      if (event.altKey || event.shiftKey) return;
      event.preventDefault();
      setMenuOpen((open) => !open);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [shortcut]);

  const openSheet = (kind: AddKind) =>
    void navigate({ to: '/projects/$projectId', params: { projectId }, search: { add: kind } });
  // A sheet opens only over the canvas, which has no other search state.
  const closeSheet = () =>
    void navigate({ to: '/projects/$projectId', params: { projectId }, search: {} });
  // Closing the service sheet goes back to what was behind it and puts focus
  // on the card that opened it.
  const closeService = () => {
    const key = selectedKey ?? null;
    void navigate({
      to: '/projects/$projectId',
      params: { projectId },
      search: view === 'canvas' ? {} : { view },
    }).then(() => {
      if (!key) return;
      requestAnimationFrame(() => {
        document.querySelector<HTMLElement>(`[data-service="${CSS.escape(key)}"]`)?.focus();
      });
    });
  };

  let content: ReactNode;
  if (missing) {
    content = <ProjectMissing />;
  } else if (view === 'metrics') {
    content = <ProjectMetrics projectId={projectId} />;
  } else if (view === 'logs') {
    content = <ProjectLogs projectId={projectId} />;
  } else if (view === 'settings') {
    content = project ? <ProjectSettings project={project} /> : null;
  } else {
    content = (
      <ProjectCanvas
        key={projectId}
        projectId={projectId}
        project={project}
        selected={selectedKey}
        onAdd={canAdd ? () => setMenuOpen(true) : undefined}
      />
    );
  }

  return (
    <ProjectShell
      projectId={projectId}
      project={project}
      projects={projects}
      leaf={leaf}
      view={view}
      canvas={onCanvas}
    >
      {content}
      {canAdd ? (
        <CommandMenu open={menuOpen} onOpenChange={setMenuOpen} onChoose={openSheet} />
      ) : null}
      {sheet ? (
        <CreateSheet key={sheet} kind={sheet} projectId={projectId} onClose={closeSheet} />
      ) : null}
      {serviceApp ? (
        <ServiceSheet
          projectId={projectId}
          appId={serviceApp}
          tab={sheetTab(search)}
          buildId={search.build}
          onClose={closeService}
        />
      ) : null}
      {serviceVm ? (
        <VmSheet projectId={projectId} vmId={serviceVm} tab={vmSheetTab(search)} onClose={closeService} />
      ) : null}
    </ProjectShell>
  );
}

function ProjectMissing() {
  return (
    <main className="mx-auto w-full max-w-4xl px-4 py-6 sm:px-6 sm:py-8">
      <h1 className="type-title">Project not found</h1>
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
      <h1 className="type-title">{title}</h1>
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
      <h1 className="type-title">Project settings</h1>
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
