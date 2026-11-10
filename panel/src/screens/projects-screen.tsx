import { useQueries } from '@tanstack/react-query';
import { getRouteApi, useNavigate } from '@tanstack/react-router';
import { Plus, Search } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { useListApps } from '../api/generated/apps/apps';
import { getListAppDeploymentsQueryOptions } from '../api/generated/deployments/deployments';
import { useListIncidents } from '../api/generated/incidents/incidents';
import type { App, Approval, Project, Vm } from '../api/generated/model';
import { useListNodes } from '../api/generated/nodes/nodes';
import { useListProjects } from '../api/generated/projects/projects';
import { useListVms } from '../api/generated/vms/vms';
import { appsPollingInterval, relativeTime } from '../lib/app-status';
import { pendingByExpiry } from '../lib/approvals';
import { approvalProjectId, openFor, splitColumns, tileFoot, tileStatus } from '../lib/board';
import { canDecideApprovals } from '../lib/capabilities';
import { listIsConverging } from '../lib/lamp';
import { problemView } from '../lib/problem';
import { PROJECT_LIST_PARAMS, projectTiles, SERVICE_LIST_PARAMS, type ProjectTileData } from '../lib/projects';
import { lastBuildFailed, servicesCount } from '../lib/services';
import { useRecentBuilds } from '../lib/use-builds';
import { useNow } from '../lib/use-now';
import { useSession } from '../session';
import { NeedsYou, RailNodes, RecentActivity } from '../components/board-rail';
import { ClusterBand } from '../components/cluster-band';
import { CommandMenu, type JumpItem } from '../components/command-menu';
import { EmptyState } from '../components/empty-state';
import { LiveNodesProvider } from '../components/live-nodes';
import { NewProjectDialog } from '../components/new-project-dialog';
import { ProjectTile, type TileContext } from '../components/project-tile';
import { usePendingApprovals } from '../components/shell/shell-data';
import { Button } from '../components/ui/button';
import { ProblemBanner } from '../components/ui/problem-banner';
import { DeployScreen } from './deploy-screen';
import { SkeletonCards } from '../components/skeleton';

const projectsRoute = getRouteApi('/shell/projects');

const OPEN_INCIDENTS = { status: 'open', limit: 500 } as const;
/** The newest deployments of each app, for the tile's foot. */
const RECENT_DEPLOYMENTS = { limit: 5 } as const;

/**
 * `/projects`: the board. The cluster's load on top, one tile per project
 * with its services, and a rail with what waits on a person, the nodes and
 * the newest activity. `?add=app` opens the deploy work order that
 * `/apps/new` used to be.
 */
export function ProjectsScreen() {
  const search = projectsRoute.useSearch();
  if (search.add === 'app') return <DeployScreen />;
  return <ProjectsBoard />;
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

/** ⌘K on a Mac, Ctrl K elsewhere. */
const shortcutWords = (): string =>
  typeof navigator !== 'undefined' && /Mac|iPhone|iPad/.test(navigator.platform) ? '⌘K' : 'Ctrl K';

type Placed = { kind: 'tile'; tile: ProjectTileData } | { kind: 'new' };

function ProjectsBoard() {
  const { canWrite, me } = useSession();
  const navigate = useNavigate();
  const [creating, setCreating] = useState(false);
  const [searching, setSearching] = useState(false);
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
  const nodesQuery = useListNodes(undefined, { query: { refetchInterval: 10_000 } });
  const projects = useMemo(() => projectsQuery.data?.items ?? [], [projectsQuery.data]);
  const apps = useMemo(() => appsQuery.data?.items ?? [], [appsQuery.data]);
  const vms = useMemo(() => vmsQuery.data?.items ?? [], [vmsQuery.data]);
  const nodes = useMemo(() => nodesQuery.data?.items ?? [], [nodesQuery.data]);
  const incidents = useMemo(
    () => (incidentsQuery.data?.items ?? []).filter((incident) => incident.status === 'open'),
    [incidentsQuery.data],
  );
  const builds = useRecentBuilds(apps);
  const deploymentResults = useQueries({
    queries: apps.map((app) =>
      getListAppDeploymentsQueryOptions(app.id, RECENT_DEPLOYMENTS, {
        query: { staleTime: 30_000, refetchInterval: 30_000, retry: false },
      }),
    ),
  });
  const deployments = Object.fromEntries(
    apps.map((app, index) => [app.id, deploymentResults[index]?.data?.items]),
  );
  const pending = usePendingApprovals();
  const approvals = useMemo(() => pendingByExpiry(pending), [pending]);
  const now = useNow(true, 60_000);

  const tiles = useMemo(() => projectTiles(projects, apps, vms), [projects, apps, vms]);
  const context = useMemo<TileContext>(
    () => ({ nodeNames: Object.fromEntries(nodes.map((node) => [node.id, node.name])), vms }),
    [nodes, vms],
  );
  const nameOf = (approval: Approval) => targetName(approval, { apps, vms, projects });

  // A refused VM or incident list leaves those facts out; only the projects
  // and apps lists are needed to show the board.
  const error = projectsQuery.error ?? appsQuery.error;
  const listError = error ? problemView(error, 'see projects') : null;
  const loading = projectsQuery.isPending || appsQuery.isPending;

  const cards = tiles.map((tile) => {
    const tileApps = tile.services.flatMap((service) => (service.kind === 'vm' ? [] : [service.app]));
    const tileVms = tile.services.flatMap((service) => (service.kind === 'vm' ? [service.vm] : []));
    const tileIncidents = incidents.filter((incident) => incident.project_id === tile.projectId);
    const status = tileStatus({
      apps: tileApps,
      vms: tileVms,
      openIncidents: tileIncidents.length,
      lastBuildFailed: tileApps.some((app) => {
        const recent = builds[app.id];
        return recent ? lastBuildFailed(recent) : false;
      }),
    });
    const foot = tileFoot({
      incidents: tileIncidents,
      apps: tileApps.map((app) => ({ app, builds: builds[app.id], deployments: deployments[app.id] })),
    });
    const footWhen = foot
      ? foot.kind === 'incident'
        ? openFor(relativeTime(foot.at, now))
        : relativeTime(foot.at, now)
      : '';
    const asked = approvals.find(
      (approval) => approvalProjectId(approval, { apps, vms, projects }) === tile.projectId,
    );
    return {
      tile,
      status,
      foot,
      footWhen,
      ask: asked ? { approval: asked, targetName: nameOf(asked) } : undefined,
    };
  });
  const byProject = new Map(cards.map((card) => [card.tile.projectId, card]));

  const placed: Placed[] = [
    ...tiles.map((tile) => ({ kind: 'tile' as const, tile })),
    ...(canWrite && tiles.length > 0 ? [{ kind: 'new' as const }] : []),
  ];
  // Roughly how tall each tile is, in rows, so the two columns come out even.
  const columns = splitColumns(placed, (item) =>
    item.kind === 'new' ? 2 : 6 + item.tile.services.length + (byProject.get(item.tile.projectId)?.ask ? 2 : 0),
  );

  const jumps = useMemo<JumpItem[]>(() => {
    const names = new Map(projects.map((project) => [project.id, project.name]));
    return [
      ...tiles.map((tile) => ({
        key: `project:${tile.projectId}`,
        kind: 'project' as const,
        title: tile.project?.name ?? tile.projectId,
        detail: `Project · ${servicesCount(tile.services.length).toLowerCase()}`,
      })),
      ...tiles.flatMap((tile) =>
        tile.services.map((service) => ({
          key: `${tile.projectId}|${service.key}`,
          kind: service.kind,
          title: service.name,
          detail: `${service.kind === 'vm' ? 'VM' : 'App'} in ${names.get(tile.projectId) ?? 'a project'}`,
        })),
      ),
    ];
  }, [projects, tiles]);
  const jump = (key: string) => {
    if (key.startsWith('project:')) {
      void navigate({ to: '/projects/$projectId', params: { projectId: key.slice('project:'.length) } });
      return;
    }
    const [projectId, service] = key.split('|') as [string, string];
    const search = service.startsWith('vm:') ? { vm: service.slice(3) } : { app: service.slice(4) };
    void navigate({ to: '/projects/$projectId', params: { projectId }, search });
  };

  // Cmd+K (Ctrl+K elsewhere) opens the search while the board is in view.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key.toLowerCase() !== 'k' || !(event.metaKey || event.ctrlKey)) return;
      if (event.altKey || event.shiftKey) return;
      // Another dialog (New project, a Deny confirm) keeps the keys to itself.
      if (document.querySelector('[role="dialog"]:not(.cmd)')) return;
      event.preventDefault();
      setSearching((open) => !open);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  const newProject = canWrite ? (
    <Button variant="primary" onClick={() => setCreating(true)}>
      <Plus strokeWidth={1.7} aria-hidden="true" className="size-4" />
      New project
    </Button>
  ) : null;
  const keys = shortcutWords();

  return (
    <LiveNodesProvider nodes={nodes}>
      <main className="board-page">
        <div className="board-head">
          <h1 className="type-title">Projects</h1>
          <button
            type="button"
            className="psearch"
            onClick={() => setSearching(true)}
            aria-keyshortcuts="Meta+K Control+K"
          >
            <Search strokeWidth={1.7} aria-hidden="true" />
            <span>Search projects and apps</span>
            <kbd aria-hidden="true">{keys}</kbd>
          </button>
          {newProject}
        </div>

        {listError ? <ProblemBanner view={listError} className="mb-4" /> : null}

        <div className="board">
          <div className="board-main">
            <ClusterBand apps={apps} vms={vms} nodes={nodes} />
            {loading ? (
              <SkeletonCards label="Loading projects" height={222} lines={4} count={2} className="ptiles-grid" />
            ) : tiles.length === 0 ? (
              listError ? null : (
                <EmptyState title="No projects yet" className="mt-6" action={newProject}>
                  A project holds apps and VMs. Deploying an app also creates one.
                </EmptyState>
              )
            ) : (
              <div className="ptiles-grid">
                {columns.map((column, index) => (
                  <div key={index} className="ptiles-col">
                    {column.map((item) => {
                      if (item.kind === 'new') {
                        return (
                          <button key="new" type="button" className="pghost" onClick={() => setCreating(true)}>
                            <span className="pghost-plus">
                              <Plus strokeWidth={1.7} aria-hidden="true" />
                            </span>
                            <span>
                              <b>New project</b>
                              Group apps and VMs that ship together.
                            </span>
                          </button>
                        );
                      }
                      const card = byProject.get(item.tile.projectId)!;
                      return (
                        <ProjectTile
                          key={item.tile.projectId}
                          tile={card.tile}
                          status={card.status}
                          foot={card.foot}
                          footWhen={card.footWhen}
                          ask={card.ask}
                          context={context}
                        />
                      );
                    })}
                  </div>
                ))}
              </div>
            )}
          </div>

          <div className="board-rail">
            <NeedsYou
              approvals={approvals}
              incidents={incidents}
              targetName={nameOf}
              canDecide={canDecideApprovals(me)}
            />
            <RailNodes nodes={nodes} apps={apps} />
            <RecentActivity />
          </div>
        </div>

        <NewProjectDialog open={creating} onOpenChange={setCreating} />
        <CommandMenu open={searching} onOpenChange={setSearching} jumps={jumps} onJump={jump} />
      </main>
    </LiveNodesProvider>
  );
}
