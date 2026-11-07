import type { AuditRecordResult, GrantKind, GrantStatus } from '../api/generated/model';

/*
 * The panel's URLs, as pure data: what each route's search may hold, where
 * every URL from before the redesign lives now, and which sidebar section a
 * path belongs to. The router reads these; nothing here touches React.
 */

const pick = <T extends string>(value: unknown, allowed: readonly T[]): T | undefined =>
  typeof value === 'string' && (allowed as readonly string[]).includes(value)
    ? (value as T)
    : undefined;

const text = (value: unknown): string | undefined =>
  typeof value === 'string' && value.trim() !== '' ? value.trim() : undefined;

const positiveInt = (value: unknown): number | undefined => {
  const parsed = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : NaN;
  return Number.isInteger(parsed) && parsed >= 1 ? parsed : undefined;
};

/** Drops the keys whose value is undefined, so a URL carries only what is set. */
const compact = <T extends object>(value: T): T =>
  Object.fromEntries(
    Object.entries(value).filter(([, entry]) => entry !== undefined),
  ) as T;

/* ---------- search params ---------- */

/** The service sheet's tabs; the tab lives in the URL. */
export const PROJECT_TABS = [
  'deployments',
  'variables',
  'metrics',
  'logs',
  'storage',
  'security',
  'settings',
] as const;
export type ProjectTab = (typeof PROJECT_TABS)[number];

/** The project rail: canvas, metrics, logs and settings. No value means the canvas. */
export const PROJECT_VIEWS = ['canvas', 'metrics', 'logs', 'settings'] as const;
export type ProjectView = (typeof PROJECT_VIEWS)[number];

/** What "+ Add" can open over the canvas: a create sheet for each. */
export const ADD_KINDS = ['git', 'image', 'vm'] as const;
export type AddKind = (typeof ADD_KINDS)[number];

/** `/projects?add=app`: the projects page with the add flow open. */
export interface ProjectsSearch {
  add?: 'app';
}

/**
 * `/projects/$projectId?view=&app=&vm=&tab=&build=&add=`: a project, the rail
 * view, and the service open in it. `build` opens one build of `app`; `add`
 * opens the create sheet for a Git app, an image app or a VM.
 */
export interface ProjectSearch {
  view?: ProjectView;
  app?: string;
  vm?: string;
  tab?: ProjectTab;
  build?: string;
  add?: AddKind;
}

/** `/nodes?tab=vms`: the nodes, or the virtual machines of the whole cluster. */
export interface NodesSearch {
  tab?: 'vms';
}

/** The grants filters, shareable as a link. */
export interface GrantsSearch {
  agent_id?: string;
  status?: GrantStatus;
  kind?: GrantKind;
}

/** `/agents?agent=` opens one agent; `/agents?tab=grants` the grants with their filters. */
export interface AgentsSearch extends GrantsSearch {
  agent?: string;
  tab?: 'grants';
}

/** `/activity?actor_id=&target_id=&result=&incident_id=&seq=`: filters plus the open record. */
export interface AuditSearch {
  actor_id?: string;
  target_id?: string;
  result?: AuditRecordResult;
  incident_id?: string;
  seq?: number;
}

type Raw = Record<string, unknown>;

export const validateProjectsSearch = (search: Raw): ProjectsSearch =>
  compact({ add: pick(search.add, ['app'] as const) });

export const validateProjectSearch = (search: Raw): ProjectSearch =>
  compact({
    view: pick(search.view, PROJECT_VIEWS),
    app: text(search.app),
    vm: text(search.vm),
    tab: pick(search.tab, PROJECT_TABS),
    build: text(search.build),
    add: pick(search.add, ADD_KINDS),
  });

export const validateNodesSearch = (search: Raw): NodesSearch =>
  compact({ tab: pick(search.tab, ['vms'] as const) });

export const validateGrantsSearch = (search: Raw): GrantsSearch =>
  compact({
    agent_id: text(search.agent_id),
    status: pick(search.status, ['active', 'expired', 'revoked', 'exhausted'] as const),
    kind: pick(search.kind, ['standard', 'incident', 'approval'] as const),
  });

export const validateAgentsSearch = (search: Raw): AgentsSearch =>
  compact({
    agent: text(search.agent),
    tab: pick(search.tab, ['grants'] as const),
    ...validateGrantsSearch(search),
  });

export const validateAuditSearch = (search: Raw): AuditSearch =>
  compact({
    actor_id: text(search.actor_id),
    target_id: text(search.target_id),
    result: pick(search.result, ['ok', 'refused', 'error'] as const),
    incident_id: text(search.incident_id),
    seq: positiveInt(search.seq),
  });

/* ---------- old URLs and their new homes ---------- */

/**
 * Every URL the panel answered before the redesign that now lives somewhere
 * else. `/login`, `/setup`, `/nodes`, `/agents`, `/approvals`, the terminal
 * and the console keep their URL and are not here.
 */
export type LegacyUrl =
  | { path: '/' }
  | { path: '/apps/new' }
  | { path: '/apps/$appId'; appId: string; projectId: string }
  | { path: '/apps/$appId/builds/$buildId'; appId: string; buildId: string; projectId: string }
  | { path: '/vms' }
  | { path: '/agents/$agentId'; agentId: string }
  | { path: '/grants'; search: GrantsSearch }
  | { path: '/audit'; search: AuditSearch }
  | { path: '/webhooks' }
  | { path: '/webhooks/$webhookId'; webhookId: string }
  | { path: '/users' }
  | { path: '/account' };

export type LegacyPath = LegacyUrl['path'];

export const LEGACY_PATHS: readonly LegacyPath[] = [
  '/',
  '/apps/new',
  '/apps/$appId',
  '/apps/$appId/builds/$buildId',
  '/vms',
  '/agents/$agentId',
  '/grants',
  '/audit',
  '/webhooks',
  '/webhooks/$webhookId',
  '/users',
  '/account',
];

/** A place in the new panel, in the shape the router's `redirect` and `Navigate` take. */
export type NewHome =
  | { to: '/projects'; search: ProjectsSearch }
  | { to: '/projects/$projectId'; params: { projectId: string }; search: ProjectSearch }
  | { to: '/nodes'; search: NodesSearch }
  | { to: '/agents'; search: AgentsSearch }
  | { to: '/activity'; search: AuditSearch }
  | { to: '/settings/members' }
  | { to: '/settings/webhooks' }
  | { to: '/settings/webhooks/$webhookId'; params: { webhookId: string } }
  | { to: '/settings/account' };

/** An old app URL: the redirect needs the app's project from the API. */
export type LegacyAppUrl = Extract<LegacyUrl, { projectId: string }>;
export type ProjectHome = Extract<NewHome, { to: '/projects/$projectId' }>;

/** An app's old URL opens its project with the app on Deployments, and the build when one was named. */
export const appHome = (url: LegacyAppUrl): ProjectHome => ({
  to: '/projects/$projectId',
  params: { projectId: url.projectId },
  search: compact({
    app: url.appId,
    tab: 'deployments' as const,
    build: url.path === '/apps/$appId/builds/$buildId' ? url.buildId : undefined,
  }),
});

/** Where an old URL lives now, with the right tab open. */
export function newHome(url: LegacyUrl): NewHome {
  switch (url.path) {
    case '/':
      return { to: '/projects', search: {} };
    case '/apps/new':
      return { to: '/projects', search: { add: 'app' } };
    case '/apps/$appId':
    case '/apps/$appId/builds/$buildId':
      return appHome(url);
    case '/vms':
      return { to: '/nodes', search: { tab: 'vms' } };
    case '/agents/$agentId':
      return { to: '/agents', search: { agent: url.agentId } };
    case '/grants':
      return { to: '/agents', search: compact({ tab: 'grants', ...url.search }) };
    case '/audit':
      return { to: '/activity', search: compact({ ...url.search }) };
    case '/webhooks':
      return { to: '/settings/webhooks' };
    case '/webhooks/$webhookId':
      return { to: '/settings/webhooks/$webhookId', params: { webhookId: url.webhookId } };
    case '/users':
      return { to: '/settings/members' };
    case '/account':
      return { to: '/settings/account' };
  }
}

/**
 * The address a new home resolves to, for display and tests. Values are
 * written as text; the router writes the same URLs for ids and numbers.
 */
export function hrefOf(home: NewHome): string {
  let path: string = home.to;
  if ('params' in home) {
    for (const [key, value] of Object.entries(home.params)) {
      path = path.replace(`$${key}`, encodeURIComponent(value));
    }
  }
  const search = 'search' in home ? compact(home.search) : {};
  const query = new URLSearchParams(
    Object.entries(search).map(([key, value]) => [key, String(value)]),
  ).toString();
  return query === '' ? path : `${path}?${query}`;
}

/* ---------- the sidebar ---------- */

export const SECTIONS = ['projects', 'nodes', 'agents', 'approvals', 'activity', 'settings'] as const;
export type Section = (typeof SECTIONS)[number];

const under = (pathname: string, prefix: string) =>
  pathname === prefix || pathname.startsWith(`${prefix}/`);

/**
 * The sidebar section a path belongs to, old URLs included, so the right item
 * is lit while a redirect is on its way. Null for the full-screen routes.
 */
export function sectionOf(pathname: string): Section | null {
  if (/^\/apps\/[^/]+\/terminal$/.test(pathname)) return null;
  if (/^\/vms\/[^/]+\/console$/.test(pathname)) return null;
  if (pathname === '/' || under(pathname, '/projects') || under(pathname, '/apps')) {
    return 'projects';
  }
  if (under(pathname, '/nodes') || under(pathname, '/vms')) return 'nodes';
  if (under(pathname, '/agents') || under(pathname, '/grants')) return 'agents';
  if (under(pathname, '/approvals')) return 'approvals';
  if (under(pathname, '/activity') || under(pathname, '/audit')) return 'activity';
  if (
    under(pathname, '/settings') ||
    under(pathname, '/webhooks') ||
    under(pathname, '/users') ||
    under(pathname, '/account')
  ) {
    return 'settings';
  }
  return null;
}
