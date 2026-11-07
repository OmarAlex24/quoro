import type { App, Project, Vm } from '../api/generated/model';
import type { LampTone } from './lamp';
import { isConverging } from './lamp';
import { projectServices, type ServiceItem } from './services';

/** Every project in one page, for the projects list and the switcher (the API allows 500). */
export const PROJECT_LIST_PARAMS = { limit: 500 } as const;

/** Every app or VM in one page, for the projects grid (the API allows 500). */
export const SERVICE_LIST_PARAMS = { limit: 500 } as const;

export interface ProjectGroup {
  projectId: string;
  /** Absent when the app names a project the list did not return. */
  project?: Project;
  apps: App[];
}

/**
 * The apps of each project, in the order the projects were listed; a project
 * with no apps still gets its group. Apps of a project the list did not
 * return come last, grouped by their project id, so none disappears.
 */
export const groupAppsByProject = (
  projects: readonly Project[],
  apps: readonly App[],
): ProjectGroup[] => {
  const groups = new Map<string, ProjectGroup>();
  for (const project of projects) {
    groups.set(project.id, { projectId: project.id, project, apps: [] });
  }
  for (const app of apps) {
    const group = groups.get(app.project_id);
    if (group) group.apps.push(app);
    else groups.set(app.project_id, { projectId: app.project_id, apps: [app] });
  }
  return [...groups.values()];
};

/** One tile of the projects grid: the project and its services (apps and standalone VMs). */
export interface ProjectTileData {
  projectId: string;
  project?: Project;
  services: ServiceItem[];
}

/** The tiles of the projects grid, in the order the projects were listed. */
export const projectTiles = (
  projects: readonly Project[],
  apps: readonly App[],
  vms: readonly Vm[],
): ProjectTileData[] =>
  groupAppsByProject(projects, apps).map((group) => ({
    projectId: group.projectId,
    project: group.project,
    services: projectServices(group.projectId, group.apps, vms),
  }));

/** What a project's footer says: a lamp and one line. */
export interface ProjectSummary {
  readonly tone: LampTone;
  readonly pulse: boolean;
  readonly text: string;
}

export interface ProjectSummaryInput {
  apps: ReadonlyArray<Pick<App, 'status'>>;
  /** The project's standalone VMs; an app's instances count through the app. */
  vms: ReadonlyArray<Pick<Vm, 'desired_status' | 'observed_status'>>;
  /** Open incidents of the project (`listIncidents`, `project_id`). */
  openIncidents: number;
  /** The newest build of one of the project's Git apps failed. */
  lastBuildFailed: boolean;
}

/** The order the counts are said in: the healthy count first, then what moves, then what is wrong. */
const COUNT_ORDER = [
  'running',
  'deploying',
  'starting',
  'stopping',
  'degraded',
  'down',
  'crashed',
  'stopped',
  'unknown',
] as const;

type CountWord = (typeof COUNT_ORDER)[number];

const vmWord = (vm: Pick<Vm, 'desired_status' | 'observed_status'>): CountWord => {
  switch (vm.observed_status) {
    case 'running':
      return isConverging(vm.desired_status, vm.observed_status) ? 'stopping' : 'running';
    case 'creating':
    case 'starting':
      return 'starting';
    case 'stopping':
      return 'stopping';
    case 'crashed':
      return 'crashed';
    case 'unknown':
      return 'unknown';
    case 'stopped':
    case 'deleted':
      return isConverging(vm.desired_status, vm.observed_status) ? 'starting' : 'stopped';
  }
};

/**
 * A project in one line, from the statuses `listApps` and `listVms` return:
 * "4 running · 1 deploying", "1 running · open incident",
 * "1 running · last build failed". The lamp takes the most urgent tone: fault
 * when something is down, crashed or failed to build; shift while something
 * moves or runs degraded; run when something runs; idle otherwise. It pulses
 * while something moves.
 */
export const projectSummary = (input: ProjectSummaryInput): ProjectSummary => {
  const counts: Record<CountWord, number> = Object.fromEntries(
    COUNT_ORDER.map((word) => [word, 0]),
  ) as Record<CountWord, number>;
  for (const app of input.apps) counts[app.status] += 1;
  for (const vm of input.vms) counts[vmWord(vm)] += 1;

  const parts = COUNT_ORDER.filter((word) => counts[word] > 0).map(
    (word) => `${counts[word]} ${word}`,
  );
  if (input.openIncidents === 1) parts.push('open incident');
  else if (input.openIncidents > 1) parts.push(`${input.openIncidents} open incidents`);
  if (input.lastBuildFailed) parts.push('last build failed');

  const moving = counts.deploying + counts.starting + counts.stopping;
  const tone: LampTone =
    counts.down + counts.crashed > 0 || input.lastBuildFailed
      ? 'fault'
      : moving + counts.degraded > 0
        ? 'shift'
        : counts.running > 0
          ? 'run'
          : 'idle';

  return {
    tone,
    pulse: moving > 0,
    text: parts.length === 0 ? 'No services yet' : parts.join(' · '),
  };
};
