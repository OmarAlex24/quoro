import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import type { EdgeBackend } from '../api/generated/model';
import { AppEdgeBackends } from './app-edge';

const since = new Date(Date.now() - 30_000).toISOString();

const backend = (overrides: Partial<EdgeBackend> = {}): EdgeBackend => ({
  edge_node_id: 'nod_edge',
  vm_id: 'vm_01HZZZZZZZZZZZZZZZZZZZZZZZ',
  node_id: 'nod_01HZZZZZZZZZZZZZZZZZZZZZZZ',
  in_rotation: true,
  since,
  reason: null,
  last_error: null,
  ...overrides,
});

const render = (
  backends: readonly EdgeBackend[],
  nodeNames: Record<string, string> = {},
): string =>
  renderToStaticMarkup(
    createElement(AppEdgeBackends, { backends, nodeNames }),
  );

describe('AppEdgeBackends', () => {
  it('shows a backend that is in rotation with its node', () => {
    const html = render([backend()], {
      nod_01HZZZZZZZZZZZZZZZZZZZZZZZ: 'lab-3',
    });

    expect(html).toContain('Edge backends');
    expect(html).toContain('vm_01HZZZZ…');
    expect(html).toContain('node lab-3');
    expect(html).toContain('In rotation');
    expect(html).toContain('since just now');
    expect(html).not.toContain('Out of rotation');
  });

  it('shows the reason and the last error of a backend out of rotation', () => {
    const html = render(
      [
        backend({
          in_rotation: false,
          reason: 'health_check',
          last_error: 'GET http://10.0.0.31:80/: request canceled after 2s',
        }),
      ],
      { nod_01HZZZZZZZZZZZZZZZZZZZZZZZ: 'lab-4' },
    );

    expect(html).toContain('Out of rotation');
    expect(html).toContain('Health check failed');
    expect(html).toContain('request canceled after 2s');
  });

  it('falls back to the node id when the name is not known', () => {
    const html = render([backend()]);

    expect(html).toContain('node nod_01HZZZZZZZZZZZZZZZZZZZZZZZ');
  });

  it('renders nothing while no edge serves the app', () => {
    expect(render([])).toBe('');
  });
});
