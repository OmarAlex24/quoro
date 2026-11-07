import { describe, expect, it } from 'vitest';
import type { App, Project, Vm } from '../api/generated/model';
import { groupAppsByProject, projectSummary, projectTiles } from './projects';

const project = (id: string, name: string) => ({ id, name, slug: name }) as Project;
const app = (id: string, projectId: string, extra: Partial<App> = {}) =>
  ({ id, name: id, project_id: projectId, status: 'running', ...extra }) as App;
const vm = (id: string, projectId: string, extra: Partial<Vm> = {}) =>
  ({
    id,
    name: id,
    project_id: projectId,
    app_id: null,
    desired_status: 'running',
    observed_status: 'running',
    ...extra,
  }) as Vm;

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

describe('projectTiles', () => {
  it('counts apps and standalone VMs, never an app instance', () => {
    const tiles = projectTiles(
      [project('prj_a', 'atlas'), project('prj_b', 'blog')],
      [app('web', 'prj_a', { source: { kind: 'git', repo_url: 'https://x', ref: 'main' } }), app('blog', 'prj_b')],
      [vm('ci', 'prj_a'), vm('web-1', 'prj_a', { app_id: 'web' }), vm('other', 'prj_b')],
    );
    expect(tiles.map((tile) => [tile.projectId, tile.services.map((s) => `${s.kind}:${s.name}`)])).toEqual([
      ['prj_a', ['git:web', 'vm:ci']],
      ['prj_b', ['image:blog', 'vm:other']],
    ]);
  });
});

describe('projectSummary', () => {
  const none = { vms: [], openIncidents: 0, lastBuildFailed: false };

  it('says the healthy count first, then what moves, with a pulsing amber lamp', () => {
    expect(
      projectSummary({
        ...none,
        apps: [app('a', 'p'), app('b', 'p'), app('c', 'p'), app('d', 'p', { status: 'deploying' })],
        vms: [vm('v', 'p')],
      }),
    ).toEqual({ tone: 'shift', pulse: true, text: '4 running · 1 deploying' });
  });

  it('adds an open incident without changing the lamp', () => {
    expect(projectSummary({ ...none, apps: [app('a', 'p')], openIncidents: 1 })).toEqual({
      tone: 'run',
      pulse: false,
      text: '1 running · open incident',
    });
    expect(projectSummary({ ...none, apps: [app('a', 'p')], openIncidents: 3 }).text).toBe(
      '1 running · 3 open incidents',
    );
  });

  it('turns red for a failed build, a down app or a crashed VM', () => {
    expect(projectSummary({ ...none, apps: [app('a', 'p')], lastBuildFailed: true })).toEqual({
      tone: 'fault',
      pulse: false,
      text: '1 running · last build failed',
    });
    expect(projectSummary({ ...none, apps: [app('a', 'p', { status: 'down' })] })).toMatchObject({
      tone: 'fault',
      text: '1 down',
    });
    expect(
      projectSummary({ ...none, apps: [], vms: [vm('v', 'p', { observed_status: 'crashed' })] }),
    ).toMatchObject({ tone: 'fault', text: '1 crashed' });
  });

  it('says what a converging VM is doing', () => {
    expect(
      projectSummary({
        ...none,
        apps: [],
        vms: [
          vm('a', 'p', { observed_status: 'starting' }),
          vm('b', 'p', { desired_status: 'stopped', observed_status: 'running' }),
          vm('c', 'p', { desired_status: 'stopped', observed_status: 'stopped' }),
        ],
      }),
    ).toEqual({ tone: 'shift', pulse: true, text: '1 starting · 1 stopping · 1 stopped' });
  });

  it('is amber and still for a degraded app, idle when everything is stopped', () => {
    expect(projectSummary({ ...none, apps: [app('a', 'p', { status: 'degraded' })] })).toEqual({
      tone: 'shift',
      pulse: false,
      text: '1 degraded',
    });
    expect(projectSummary({ ...none, apps: [app('a', 'p', { status: 'stopped' })] })).toEqual({
      tone: 'idle',
      pulse: false,
      text: '1 stopped',
    });
  });

  it('has words for an empty project', () => {
    expect(projectSummary({ ...none, apps: [] })).toEqual({
      tone: 'idle',
      pulse: false,
      text: 'No services yet',
    });
  });
});
