import { createElement, type ReactElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import {
  createMemoryHistory,
  createRootRoute,
  createRoute,
  createRouter,
  RouterContextProvider,
} from '@tanstack/react-router';
import { describe, expect, it } from 'vitest';
import {
  agentTab,
  sheetTab,
  validateAgentsSearch,
  validateNodesSearch,
  validateProjectSearch,
  type AgentsSearch,
  type NodesSearch,
  type ProjectSearch,
} from '../lib/routes';
import { RailLinks } from '../components/shell/project-shell';
import { SheetTabLinks } from '../components/service-sheet';
import { AgentTabs } from './agent-screen';
import { AgentsTabs, NodesTabs } from './section-pages';

/*
 * Every tab row marks one link as the current page for a screen reader: the
 * tab the URL selects, which is also the one drawn with `on`. TanStack
 * Router's `Link` marks a link current when its search is a subset of the
 * URL's, so without exact matching the row's plain tab was current beside the
 * open one (#845).
 */

const root = createRootRoute();
const routeTree = root.addChildren([
  createRoute({ getParentRoute: () => root, path: '/nodes', validateSearch: validateNodesSearch }),
  createRoute({ getParentRoute: () => root, path: '/agents', validateSearch: validateAgentsSearch }),
  createRoute({ getParentRoute: () => root, path: '/projects/$projectId', validateSearch: validateProjectSearch }),
]);

/** Renders a row's links with the router at `url`, given that URL's parsed search. */
async function renderAt(url: string, row: (search: Record<string, unknown>) => ReactElement): Promise<string> {
  const router = createRouter({ routeTree, history: createMemoryHistory({ initialEntries: [url] }) });
  await router.load();
  const search = router.state.location.search as Record<string, unknown>;
  return renderToStaticMarkup(createElement(RouterContextProvider, { router } as never, row(search)));
}

interface Anchor {
  href: string;
  on: boolean;
  current: boolean;
}

const anchors = (html: string): Anchor[] =>
  Array.from(html.matchAll(/<a\b([^>]*)>/g), ([, attrs]) => ({
    href: (/\bhref="([^"]*)"/.exec(attrs)?.[1] ?? '').replaceAll('&amp;', '&'),
    on: /\bclass="(?:[^"]*\s)?on(?:\s[^"]*)?"/.test(attrs),
    current: /\baria-current="page"/.test(attrs),
  }));

/** The one link marked current, after checking it is the one drawn with `on`. */
function currentHref(html: string): string {
  const links = anchors(html);
  const current = links.filter((link) => link.current).map((link) => link.href);
  const on = links.filter((link) => link.on).map((link) => link.href);
  expect(current).toHaveLength(1);
  expect(current).toEqual(on);
  return current[0];
}

describe('the Nodes tabs', () => {
  const row = (search: Record<string, unknown>) =>
    createElement(NodesTabs, { tab: (search as NodesSearch).tab, vmCount: 3, securityCount: 2 });

  it.each([
    ['/nodes', '/nodes'],
    ['/nodes?tab=vms', '/nodes?tab=vms'],
    ['/nodes?tab=security', '/nodes?tab=security'],
    ['/nodes?tab=vms&node=nod_1', '/nodes?tab=vms'],
  ])('marks one tab current at %s', async (url, selected) => {
    expect(currentHref(await renderAt(url, row))).toBe(selected);
  });
});

describe('the Agents tabs', () => {
  // As `AgentsPage` reads it: `tab=grants` with an agent is that agent's tab.
  const row = (search: Record<string, unknown>) => {
    const { agent, tab } = search as AgentsSearch;
    return createElement(AgentsTabs, { grants: agent === undefined && tab === 'grants' });
  };

  it.each([
    ['/agents', '/agents'],
    ['/agents?tab=grants', '/agents?tab=grants'],
    ['/agents?tab=grants&status=active', '/agents?tab=grants'],
    ['/agents?agent=agt_1', '/agents'],
    ['/agents?agent=agt_1&tab=grants', '/agents'],
  ])('marks one tab current at %s', async (url, selected) => {
    expect(currentHref(await renderAt(url, row))).toBe(selected);
  });
});

describe("an agent's own tabs", () => {
  const row = (search: Record<string, unknown>) =>
    createElement(AgentTabs, { agentId: 'agt_1', tab: agentTab(search as AgentsSearch), activeCount: 2 });

  it.each([
    ['/agents?agent=agt_1', '/agents?agent=agt_1'],
    ['/agents?agent=agt_1&tab=grants', '/agents?agent=agt_1&tab=grants'],
    ['/agents?agent=agt_1&tab=keys', '/agents?agent=agt_1&tab=keys'],
    ['/agents?agent=agt_1&tab=settings', '/agents?agent=agt_1&tab=settings'],
  ])('marks one tab current at %s', async (url, selected) => {
    expect(currentHref(await renderAt(url, row))).toBe(selected);
  });
});

describe('the project rail', () => {
  const row = (search: Record<string, unknown>) =>
    createElement(RailLinks, { projectId: 'prj_1', view: (search as ProjectSearch).view ?? 'canvas' });

  it.each([
    ['/projects/prj_1', '/projects/prj_1'],
    ['/projects/prj_1?view=metrics', '/projects/prj_1?view=metrics'],
    ['/projects/prj_1?view=logs', '/projects/prj_1?view=logs'],
    ['/projects/prj_1?app=app_1&tab=logs', '/projects/prj_1'],
  ])('marks one view current at %s', async (url, selected) => {
    expect(currentHref(await renderAt(url, row))).toBe(selected);
  });
});

describe("an app sheet's tabs", () => {
  const row = (search: Record<string, unknown>) =>
    createElement(SheetTabLinks, {
      appId: 'app_1',
      projectId: 'prj_1',
      tab: sheetTab(search as ProjectSearch),
      openFindings: 4,
    });

  it.each([
    ['/projects/prj_1?app=app_1', '/projects/prj_1?app=app_1&tab=deployments'],
    ['/projects/prj_1?app=app_1&tab=logs', '/projects/prj_1?app=app_1&tab=logs'],
    ['/projects/prj_1?app=app_1&tab=security', '/projects/prj_1?app=app_1&tab=security'],
    ['/projects/prj_1?app=app_1&tab=deployments&build=bld_1', '/projects/prj_1?app=app_1&tab=deployments'],
  ])('marks one tab current at %s', async (url, selected) => {
    expect(currentHref(await renderAt(url, row))).toBe(selected);
  });
});
