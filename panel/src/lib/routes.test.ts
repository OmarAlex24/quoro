import { describe, expect, it } from 'vitest';
import {
  agentTab,
  hrefOf,
  LEGACY_PATHS,
  newHome,
  sectionOf,
  sheetTab,
  validateAgentsSearch,
  validateApprovalsSearch,
  validateAuditSearch,
  validateGrantsSearch,
  validateNodesSearch,
  validateProjectSearch,
  validateProjectsSearch,
  vmSheetTab,
  type LegacyUrl,
} from './routes';

const VM = 'vm_01JD5Z8Q4XK3M9V2T7B6N1R0SG';
const NODE = 'nod_01JD5Z8Q4XK3M9V2T7B6N1R0SH';
const APP = 'app_01JD5Z8Q4XK3M9V2T7B6N1R0SA';
const PROJECT = 'prj_01JD5Z8Q4XK3M9V2T7B6N1R0SB';
const BUILD = 'bld_01JD5Z8Q4XK3M9V2T7B6N1R0SC';
const AGENT = 'agt_01JD5Z8Q4XK3M9V2T7B6N1R0SD';
const WEBHOOK = 'whk_01JD5Z8Q4XK3M9V2T7B6N1R0SE';
const APPROVAL = 'apr_01JD5Z8Q4XK3M9V2T7B6N1R0SF';

/** One example of every old URL, with the data a redirect needs. */
const EXAMPLES: Record<LegacyUrl['path'], LegacyUrl> = {
  '/': { path: '/' },
  '/apps/new': { path: '/apps/new' },
  '/apps/$appId': { path: '/apps/$appId', appId: APP, projectId: PROJECT },
  '/apps/$appId/builds/$buildId': {
    path: '/apps/$appId/builds/$buildId',
    appId: APP,
    buildId: BUILD,
    projectId: PROJECT,
  },
  '/vms': { path: '/vms' },
  '/agents/$agentId': { path: '/agents/$agentId', agentId: AGENT },
  '/grants': { path: '/grants', search: {} },
  '/audit': { path: '/audit', search: {} },
  '/webhooks': { path: '/webhooks' },
  '/webhooks/$webhookId': { path: '/webhooks/$webhookId', webhookId: WEBHOOK },
  '/users': { path: '/users' },
  '/account': { path: '/account' },
};

describe('newHome: every old URL has a new place', () => {
  it.each([
    ['/', '/projects'],
    ['/apps/new', '/projects?add=app'],
    ['/apps/$appId', `/projects/${PROJECT}?app=${APP}&tab=deployments`],
    [
      '/apps/$appId/builds/$buildId',
      `/projects/${PROJECT}?app=${APP}&tab=deployments&build=${BUILD}`,
    ],
    ['/vms', '/nodes?tab=vms'],
    ['/agents/$agentId', `/agents?agent=${AGENT}`],
    ['/grants', '/agents?tab=grants'],
    ['/audit', '/activity'],
    ['/webhooks', '/settings/webhooks'],
    ['/webhooks/$webhookId', `/settings/webhooks/${WEBHOOK}`],
    ['/users', '/settings/members'],
    ['/account', '/settings/account'],
  ] as const)('%s lands on %s', (path, href) => {
    expect(hrefOf(newHome(EXAMPLES[path]))).toBe(href);
  });

  it('covers every listed old URL, and nothing lands on another old URL', () => {
    expect(Object.keys(EXAMPLES).sort()).toEqual([...LEGACY_PATHS].sort());
    for (const path of LEGACY_PATHS) {
      const home = newHome(EXAMPLES[path]);
      expect(LEGACY_PATHS).not.toContain(home.to);
      expect(sectionOf(hrefOf(home).split('?')[0])).not.toBeNull();
    }
  });

  it('opens the Deployments tab of the app in its own project', () => {
    expect(newHome(EXAMPLES['/apps/$appId'])).toEqual({
      to: '/projects/$projectId',
      params: { projectId: PROJECT },
      search: { app: APP, tab: 'deployments' },
    });
  });

  it('carries every grants filter to the Grants tab of Agents', () => {
    expect(
      newHome({ path: '/grants', search: { agent_id: AGENT, status: 'active', kind: 'approval' } }),
    ).toEqual({
      to: '/agents',
      search: { tab: 'grants', agent_id: AGENT, status: 'active', kind: 'approval' },
    });
  });

  it('carries every audit filter and the open record to Activity', () => {
    const search = {
      actor_id: AGENT,
      target_id: APP,
      result: 'refused',
      incident_id: 'inc_01JD5Z8Q4XK3M9V2T7B6N1R0SF',
      seq: 7,
    } as const;
    const home = newHome({ path: '/audit', search });
    expect(home).toEqual({ to: '/activity', search });
    expect(hrefOf(home)).toBe(
      `/activity?actor_id=${AGENT}&target_id=${APP}&result=refused&incident_id=inc_01JD5Z8Q4XK3M9V2T7B6N1R0SF&seq=7`,
    );
  });

  it('writes no empty filter into the new URL', () => {
    expect(newHome({ path: '/grants', search: { agent_id: undefined, kind: 'standard' } })).toEqual({
      to: '/agents',
      search: { tab: 'grants', kind: 'standard' },
    });
  });

  it('round-trips through the validators of the new routes', () => {
    const build = newHome(EXAMPLES['/apps/$appId/builds/$buildId']);
    if (build.to !== '/projects/$projectId') throw new Error('unexpected home');
    expect(validateProjectSearch({ ...build.search })).toEqual(build.search);

    const grants = newHome({ path: '/grants', search: { agent_id: AGENT, status: 'revoked' } });
    if (grants.to !== '/agents') throw new Error('unexpected home');
    expect(validateAgentsSearch({ ...grants.search })).toEqual(grants.search);
  });
});

describe('search validators', () => {
  it('keeps a known project view, tab and ids, and drops the rest', () => {
    expect(
      validateProjectSearch({
        view: 'logs',
        app: ` ${APP} `,
        vm: '',
        tab: 'security',
        build: BUILD,
        extra: 'x',
      }),
    ).toEqual({ view: 'logs', app: APP, tab: 'security', build: BUILD });
    expect(validateProjectSearch({ view: 'canvas2', tab: 'Deployments' })).toEqual({});
  });

  it('accepts every sheet tab', () => {
    for (const tab of [
      'deployments',
      'variables',
      'metrics',
      'logs',
      'storage',
      'security',
      'settings',
    ]) {
      expect(validateProjectSearch({ tab }).tab).toBe(tab);
    }
  });

  it('opens a service on Deployments unless the URL names another tab', () => {
    expect(sheetTab(validateProjectSearch({ app: APP }))).toBe('deployments');
    expect(sheetTab(validateProjectSearch({ app: APP, tab: 'variables' }))).toBe('variables');
    expect(sheetTab(validateProjectSearch({ app: APP, tab: 'Variables' }))).toBe('deployments');
  });

  it('opens a create sheet for a Git app, an image app or a VM, and nothing else', () => {
    for (const add of ['git', 'image', 'vm']) {
      expect(validateProjectSearch({ add })).toEqual({ add });
    }
    expect(validateProjectSearch({ add: 'template' })).toEqual({});
    expect(validateProjectSearch({ add: 'app' })).toEqual({});
  });

  it('opens a VM on Overview, or on Settings when the URL names it', () => {
    expect(vmSheetTab(validateProjectSearch({ vm: VM }))).toBe('overview');
    expect(vmSheetTab(validateProjectSearch({ vm: VM, tab: 'settings' }))).toBe('settings');
    expect(vmSheetTab(validateProjectSearch({ vm: VM, tab: 'overview' }))).toBe('overview');
    // An app's tab on a VM falls back to Overview, and the other way round.
    expect(vmSheetTab(validateProjectSearch({ vm: VM, tab: 'deployments' }))).toBe('overview');
    expect(validateProjectSearch({ vm: VM, tab: 'overview' })).toEqual({ vm: VM, tab: 'overview' });
    expect(sheetTab(validateProjectSearch({ app: APP, tab: 'overview' }))).toBe('deployments');
    expect(sheetTab(validateProjectSearch({ app: APP, tab: 'settings' }))).toBe('settings');
  });

  it('reads the projects, nodes and agents switches', () => {
    expect(validateProjectsSearch({ add: 'app' })).toEqual({ add: 'app' });
    expect(validateProjectsSearch({ add: 'vm' })).toEqual({});
    expect(validateNodesSearch({ tab: 'vms' })).toEqual({ tab: 'vms' });
    expect(validateNodesSearch({ tab: 'security' })).toEqual({ tab: 'security' });
    expect(validateNodesSearch({ tab: 'nodes' })).toEqual({});
    expect(validateAgentsSearch({ agent: AGENT, tab: 'keys' })).toEqual({ agent: AGENT, tab: 'keys' });
    expect(validateAgentsSearch({ agent: AGENT, tab: 'grants' })).toEqual({ agent: AGENT, tab: 'grants' });
    expect(validateAgentsSearch({ agent: AGENT, tab: 'story' })).toEqual({ agent: AGENT });
    // Without an agent, only the cluster's grants are a tab.
    expect(validateAgentsSearch({ tab: 'keys' })).toEqual({});
    expect(validateAgentsSearch({ tab: 'settings' })).toEqual({});
    expect(validateAgentsSearch({ tab: 'grants' })).toEqual({ tab: 'grants' });
  });

  it('opens an agent on Activity unless the URL names a tab', () => {
    expect(agentTab(validateAgentsSearch({ agent: AGENT }))).toBe('activity');
    expect(agentTab(validateAgentsSearch({ agent: AGENT, tab: 'settings' }))).toBe('settings');
  });

  it('keeps an approval open in the inbox', () => {
    expect(validateApprovalsSearch({ approval: ` ${APPROVAL} ` })).toEqual({ approval: APPROVAL });
    expect(validateApprovalsSearch({ approval: '' })).toEqual({});
    expect(validateApprovalsSearch({})).toEqual({});
  });

  it('filters the VMs tab by node, and drops the node anywhere else', () => {
    expect(validateNodesSearch({ tab: 'vms', node: ` ${NODE} ` })).toEqual({ tab: 'vms', node: NODE });
    expect(validateNodesSearch({ tab: 'vms', node: '' })).toEqual({ tab: 'vms' });
    expect(validateNodesSearch({ node: NODE })).toEqual({});
    expect(validateNodesSearch({ tab: 'security', node: NODE })).toEqual({ tab: 'security' });
    expect(hrefOf({ to: '/nodes', search: { tab: 'vms', node: NODE } })).toBe(`/nodes?tab=vms&node=${NODE}`);
  });

  it('keeps the grants and audit filters the old routes accepted', () => {
    expect(validateGrantsSearch({ agent_id: AGENT, status: 'gone', kind: 'incident' })).toEqual({
      agent_id: AGENT,
      kind: 'incident',
    });
    expect(validateAuditSearch({ result: 'ok', seq: '12' })).toEqual({ result: 'ok', seq: 12 });
    expect(validateAuditSearch({ seq: '0' })).toEqual({});
    expect(validateAuditSearch({ seq: 2.5 })).toEqual({});
  });
});

describe('sectionOf', () => {
  it.each([
    ['/', 'projects'],
    ['/projects', 'projects'],
    [`/projects/${PROJECT}`, 'projects'],
    [`/apps/${APP}`, 'projects'],
    ['/apps/new', 'projects'],
    ['/nodes', 'nodes'],
    ['/vms', 'nodes'],
    ['/agents', 'agents'],
    [`/agents/${AGENT}`, 'agents'],
    ['/grants', 'agents'],
    ['/approvals', 'approvals'],
    ['/activity', 'activity'],
    ['/audit', 'activity'],
    ['/settings/members', 'settings'],
    [`/webhooks/${WEBHOOK}`, 'settings'],
    ['/users', 'settings'],
    ['/account', 'settings'],
  ] as const)('%s belongs to %s', (pathname, section) => {
    expect(sectionOf(pathname)).toBe(section);
  });

  it('leaves the full-screen routes and unknown paths out', () => {
    expect(sectionOf(`/apps/${APP}/terminal`)).toBeNull();
    expect(sectionOf('/vms/vm_1/console')).toBeNull();
    expect(sectionOf('/login')).toBeNull();
    expect(sectionOf('/projectsx')).toBeNull();
  });
});
