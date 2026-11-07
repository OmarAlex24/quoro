import type { App, Project } from '../api/generated/model';

/** Every project in one page, for the projects list and the switcher (the API allows 500). */
export const PROJECT_LIST_PARAMS = { limit: 500 } as const;

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

export const servicesLine = (count: number): string =>
  count === 0 ? 'No apps yet' : `${count} ${count === 1 ? 'app' : 'apps'}`;
