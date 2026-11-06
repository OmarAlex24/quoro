import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import type { Node, Vm } from '../api/generated/model';
import { AppInstances, instanceStatusText } from './app-instances';

const instance = (overrides: Partial<Vm> = {}): Vm => ({
  id: 'vm_01HZZZZZZZZZZZZZZZZZZZZZZZ',
  project_id: 'prj_01HZZZZZZZZZZZZZZZZZZZZZZZ',
  app_id: 'app_01HZZZZZZZZZZZZZZZZZZZZZZZ',
  node_id: 'nod_01HZZZZZZZZZZZZZZZZZZZZZZZ',
  name: 'blog-1',
  vcpus: 1,
  memory_mb: 512,
  disk_gb: 2,
  image: 'docker.io/library/nginx:alpine',
  desired_status: 'running',
  observed_status: 'running',
  ip: '10.0.0.21',
  restart_generation: 0,
  restart_count: 0,
  last_exit_code: null,
  created_at: '2026-11-04T10:00:00Z',
  updated_at: '2026-11-04T10:00:00Z',
  ...overrides,
});

const render = (
  instances: readonly Vm[],
  nodeNames: Record<string, string> = {},
  hasSource = false,
  nodeStatuses: Record<string, Node['status']> = {},
): string =>
  renderToStaticMarkup(
    createElement(AppInstances, { instances, nodeNames, nodeStatuses, hasSource }),
  );

describe('AppInstances', () => {
  it('shows each instance with its observed status and node', () => {
    const html = render(
      [
        instance(),
        instance({
          id: 'vm_01HYYYYYYYYYYYYYYYYYYYYYYY',
          name: 'blog-2',
          node_id: 'nod_01HYYYYYYYYYYYYYYYYYYYYYYY',
          observed_status: 'creating',
          ip: null,
        }),
      ],
      {
        nod_01HZZZZZZZZZZZZZZZZZZZZZZZ: 'lab-3',
        nod_01HYYYYYYYYYYYYYYYYYYYYYYY: 'lab-4',
      },
    );

    expect(html).toContain('blog-1');
    expect(html).toContain('running');
    expect(html).toContain('node lab-3');
    expect(html).toContain('blog-2');
    expect(html).toContain('creating');
    expect(html).toContain('node lab-4');
  });

  it('says why an instance is unknown when its node is not connected', () => {
    const html = render(
      [
        instance({ observed_status: 'unknown' }),
        instance({
          id: 'vm_01HYYYYYYYYYYYYYYYYYYYYYYY',
          name: 'blog-2',
          node_id: 'nod_01HYYYYYYYYYYYYYYYYYYYYYYY',
        }),
      ],
      {
        nod_01HZZZZZZZZZZZZZZZZZZZZZZZ: 'lab-1',
        nod_01HYYYYYYYYYYYYYYYYYYYYYYY: 'lab-2',
      },
      false,
      {
        nod_01HZZZZZZZZZZZZZZZZZZZZZZZ: 'disconnected',
        nod_01HYYYYYYYYYYYYYYYYYYYYYYY: 'connected',
      },
    );

    expect(html).toContain('unknown (node disconnected)');
    expect(html).toContain('node lab-1');
    expect(html).toContain('running');
    expect(html).toContain('node lab-2');
  });

  it('shows the plain status when the node list does not explain it', () => {
    const vm = instance({ observed_status: 'unknown' });
    expect(instanceStatusText(vm, {})).toBe('unknown');
    expect(instanceStatusText(vm, { [vm.node_id]: 'connected' })).toBe('unknown');
    expect(instanceStatusText(vm, { [vm.node_id]: 'fenced' })).toBe('unknown (node fenced)');
    expect(instanceStatusText(instance(), { [vm.node_id]: 'disconnected' })).toBe('running');
  });

  it('keeps the empty message the screen already used', () => {
    expect(render([], {}, true)).toContain('A successful build deploys');
    expect(render([], {}, false)).toContain('Deploy the app to boot one.');
  });
});
