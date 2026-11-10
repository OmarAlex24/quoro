import { getRouteApi } from '@tanstack/react-router';
import { Plus } from 'lucide-react';
import { useMemo, useState } from 'react';
import { useListApps } from '../api/generated/apps/apps';
import { useListIncidents } from '../api/generated/incidents/incidents';
import type { App, Approval, Project, Vm } from '../api/generated/model';
import { useListProjects } from '../api/generated/projects/projects';
import { useListVms } from '../api/generated/vms/vms';
import { appsPollingInterval } from '../lib/app-status';
import { listIsConverging } from '../lib/lamp';
import { problemView } from '../lib/problem';
import {
  PROJECT_LIST_PARAMS,
  projectSummary,
  projectTiles,
  SERVICE_LIST_PARAMS,
} from '../lib/projects';
import { lastBuildFailed } from '../lib/services';
import { useRecentBuilds } from '../lib/use-builds';
import { useSession } from '../session';
import { ApprovalCallout } from '../components/approval-callout';
import { EmptyState } from '../components/empty-state';
import { NewProjectDialog } from '../components/new-project-dialog';
import { ProjectTile } from '../components/project-tile';
import { usePendingApprovals } from '../components/shell/shell-data';
import { Button } from '../components/ui/button';
import { ProblemBanner } from '../components/ui/problem-banner';
import { DeployScreen } from './deploy-screen';

const projectsRoute = getRouteApi('/shell/projects');

const OPEN_INCIDENTS = { status: 'open', limit: 500 } as const;

/**
 * `/projects`: one tile per project, with what waits on a person above the
 * tiles. `?add=app` opens the deploy work order that `/apps/new` used to be.
 */
export function ProjectsScreen() {
  const search = projectsRoute.useSearch();
  if (search.add === 'app') return <DeployScreen />;
  return <ProjectsGrid />;
}

/** The name of what an approval targets, from the lists this page holds. */
const targetName = (
  approval: Approval,
  lists: { apps: readonly App[]; vms: readonly Vm[]; projects: readonly Project[] },
): string | undefined => {
  const { type, id } = approval.target;
  if (type === 'app') return lists.apps.find((app) => app.id === id)?.name;
  if (type === 'vm') return lists.vms.find((vm) => vm.id === id)?.name;
  if (type === 'project') return lists.projects.find((project) => project.id === id)?.name;
  return undefined;
};

function ProjectsGrid() {
  const { canWrite } = useSession();
  const [creating, setCreating] = useState(false);
  const projectsQuery = useListProjects(PROJECT_LIST_PARAMS);
  const appsQuery = useListApps(SERVICE_LIST_PARAMS, {
    query: {
      refetchInterval: (query) => appsPollingInterval(query.state.data?.items ?? []),
    },
  });
  const vmsQuery = useListVms(SERVICE_LIST_PARAMS, {
    query: {
      refetchInterval: (query) =>
        listIsConverging(query.state.data?.items ?? []) ? 2_000 : 10_000,
    },
  });
  const incidentsQuery = useListIncidents(OPEN_INCIDENTS, {
    query: { refetchInterval: 15_000, retry: false },
  });
  const projects = useMemo(() => projectsQuery.data?.items ?? [], [projectsQuery.data]);
  const apps = useMemo(() => appsQuery.data?.items ?? [], [appsQuery.data]);
  const vms = useMemo(() => vmsQuery.data?.items ?? [], [vmsQuery.data]);
  const builds = useRecentBuilds(apps);
  const approvals = usePendingApprovals();

  const tiles = useMemo(() => projectTiles(projects, apps, vms), [projects, apps, vms]);
  const incidentsByProject = useMemo(() => {
    const counts = new Map<string, number>();
    for (const incident of incidentsQuery.data?.items ?? []) {
      if (incident.project_id) {
        counts.set(incident.project_id, (counts.get(incident.project_id) ?? 0) + 1);
      }
    }
    return counts;
  }, [incidentsQuery.data]);

  // One callout per pending approval, the one that expires first on top.
  const callouts = [...approvals].sort((left, right) =>
    left.expires_at.localeCompare(right.expires_at),
  );

  // A refused VM or incident list leaves those facts out; only the projects
  // and apps lists are needed to show the grid.
  const error = projectsQuery.error ?? appsQuery.error;
  const listError = error ? problemView(error, 'see projects') : null;
  const pending = projectsQuery.isPending || appsQuery.isPending;

  const newProject = canWrite ? (
    <Button variant="primary" onClick={() => setCreating(true)}>
      <Plus strokeWidth={1.7} aria-hidden="true" className="size-4" />
      New project
    </Button>
  ) : null;

  return (
    <main className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 sm:py-8">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="type-title">Projects</h1>
        {newProject}
      </div>

      {listError ? <ProblemBanner view={listError} className="mt-4" /> : null}

      {callouts.length > 0 ? (
        <div className="mt-5 flex flex-col gap-2">
          {callouts.map((approval) => (
            <ApprovalCallout
              key={approval.id}
              approval={approval}
              targetName={targetName(approval, { apps, vms, projects })}
            />
          ))}
        </div>
      ) : null}

      {pending ? (
        <div className="pgrid mt-5" aria-hidden="true">
          <div className="card ptile ptile-ghost" />
        </div>
      ) : tiles.length === 0 ? (
        listError ? null : (
          <EmptyState
            title="No projects yet"
            className="mt-10"
            action={newProject}
          >
            A project holds apps and VMs. Deploying an app also creates one.
          </EmptyState>
        )
      ) : (
        <div className="pgrid mt-5">
          {tiles.map((tile) => {
            const tileApps = tile.services.flatMap((service) =>
              service.kind === 'vm' ? [] : [service.app],
            );
            const tileVms = tile.services.flatMap((service) =>
              service.kind === 'vm' ? [service.vm] : [],
            );
            const summary = projectSummary({
              apps: tileApps,
              vms: tileVms,
              openIncidents: incidentsByProject.get(tile.projectId) ?? 0,
              lastBuildFailed: tileApps.some((app) => {
                const recent = builds[app.id];
                return recent ? lastBuildFailed(recent) : false;
              }),
            });
            return <ProjectTile key={tile.projectId} tile={tile} summary={summary} />;
          })}
        </div>
      )}

      <NewProjectDialog open={creating} onOpenChange={setCreating} />
    </main>
  );
}
