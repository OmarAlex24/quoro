import { describe, expect, it } from 'vitest';
import type { App, Project } from '../api/generated/model';
import { groupAppsByProject, servicesLine } from './projects';

const project = (id: string, name: string) => ({ id, name, slug: name }) as Project;
const app = (id: string, projectId: string) => ({ id, name: id, project_id: projectId }) as App;

describe('groupAppsByProject', () => {
  it('keeps the listed order and gives an empty project its own group', () => {
    const groups = groupAppsByProject(
      [project('prj_b', 'blog'), project('prj_d', 'docs'), project('prj_e', 'empty')],
      [app('app_1', 'prj_d'), app('app_2', 'prj_b'), app('app_3', 'prj_d')],
    );
    expect(groups.map((group) => [group.project?.name, group.apps.map((a) => a.id)])).toEqual([
      ['blog', ['app_2']],
      ['docs', ['app_1', 'app_3']],
      ['empty', []],
    ]);
  });

  it('keeps an app whose project was not listed, in a group of its own', () => {
    const groups = groupAppsByProject([project('prj_b', 'blog')], [app('app_9', 'prj_x')]);
    expect(groups).toHaveLength(2);
    expect(groups[1]).toEqual({ projectId: 'prj_x', apps: [app('app_9', 'prj_x')] });
  });
});

describe('servicesLine', () => {
  it('counts apps in words', () => {
    expect(servicesLine(0)).toBe('No apps yet');
    expect(servicesLine(1)).toBe('1 app');
    expect(servicesLine(5)).toBe('5 apps');
  });
});
