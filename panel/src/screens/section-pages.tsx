import { getRouteApi, Link, Outlet, useRouterState } from '@tanstack/react-router';
import { useListNodes } from '../api/generated/nodes/nodes';
import { useListVms } from '../api/generated/vms/vms';
import { useGetVulnerabilitySummary } from '../api/generated/vulnerabilities/vulnerabilities';
import { canManageProviders } from '../lib/capabilities';
import { SERVICE_LIST_PARAMS } from '../lib/projects';
import {
  SETTINGS_LABELS,
  SETTINGS_PAGES,
  settingsPageOf,
  type NodesTab,
  type SettingsPage,
} from '../lib/routes';
import { cn } from '../lib/utils';
import { nodeSecurityCount } from '../lib/vulnerabilities';
import { useSession } from '../session';
import { AddNodeButton } from '../components/add-node';
import { ApiTokens } from '../components/api-tokens';
import { ClusterVulnerabilities } from '../components/cluster-vulnerabilities';
import { SettingsPage as SettingsColumn } from '../components/settings-parts';
import { TabbedPage } from '../components/shell/tab-strip';
import { AgentScreen } from './agent-screen';
import { AgentsScreen } from './agents-screen';
import { GrantsScreen } from './grants-screen';
import { NodesScreen } from './nodes-screen';
import { VmsScreen } from './vms-screen';

/*
 * The sidebar sections that gather several of today's screens under tabs.
 * The redesigned Nodes, Agents and Settings replace the screens inside; the
 * tabs and their URLs stay.
 */

const nodesRoute = getRouteApi('/shell/nodes');
const agentsRoute = getRouteApi('/shell/agents');

/**
 * `/nodes`, `/nodes?tab=vms` and `/nodes?tab=security`: the machines with the
 * N+1 strip, every VM of the cluster (or of one node), and the vulnerability
 * summary of the cluster's own software.
 */
export function NodesPage() {
  const { tab, node } = nodesRoute.useSearch();
  const { user } = useSession();
  const vms = useListVms(SERVICE_LIST_PARAMS, { query: { enabled: user !== undefined } });
  const vmCount = vms.data?.items.length;
  const nodes = useListNodes(undefined, { query: { enabled: user !== undefined, staleTime: 10_000 } });
  const nodeNames = Object.fromEntries((nodes.data?.items ?? []).map((item) => [item.id, item.name]));
  // The same request the Security tab makes, so opening it reads the cache.
  const summary = useGetVulnerabilitySummary(
    { limit: 50 },
    { query: { enabled: user !== undefined, refetchInterval: 30_000 } },
  );
  const securityCount = nodeSecurityCount(summary.data);

  const tabLink = (value: NodesTab | undefined, words: string, count?: number) => (
    <Link
      to="/nodes"
      search={value ? { tab: value } : {}}
      className={cn(tab === value && 'on')}
      aria-current={tab === value ? 'page' : undefined}
    >
      {words}
      {count !== undefined ? (
        <>
          <span className="count count-soft" aria-hidden="true">
            {count}
          </span>
          <span className="sr-only">, {count}</span>
        </>
      ) : null}
    </Link>
  );

  return (
    <TabbedPage
      label="Nodes"
      title="Nodes"
      wide
      action={<AddNodeButton />}
      current={tab ?? 'nodes'}
      tabs={
        <>
          {tabLink(undefined, 'Nodes')}
          {tabLink('vms', 'Virtual machines', vmCount)}
          {tabLink('security', 'Security', securityCount ?? undefined)}
        </>
      }
    >
      {tab === 'vms' ? (
        <VmsScreen key={node ?? 'all'} nodeId={node} />
      ) : tab === 'security' ? (
        <main className="mx-auto w-full max-w-6xl px-4 pb-8 sm:px-6">
          <ClusterVulnerabilities nodeNames={nodeNames} />
        </main>
      ) : (
        <NodesScreen />
      )}
    </TabbedPage>
  );
}

/** `/agents`, `/agents?agent=` and `/agents?tab=grants`: who may act, and what they may do. */
export function AgentsPage() {
  const search = agentsRoute.useSearch();
  // `tab=grants` with an agent is that agent's Grants tab, not the cluster's.
  const grants = search.agent === undefined && search.tab === 'grants';

  return (
    <TabbedPage
      label="Agents"
      current={grants ? 'grants' : 'agents'}
      tabs={
        <>
          <Link
            to="/agents"
            search={{}}
            className={cn(!grants && 'on')}
            aria-current={!grants ? 'page' : undefined}
          >
            Agents
          </Link>
          <Link
            to="/agents"
            search={{ tab: 'grants' }}
            className={cn(grants && 'on')}
            aria-current={grants ? 'page' : undefined}
          >
            Grants
          </Link>
        </>
      }
    >
      {grants ? (
        <GrantsScreen />
      ) : search.agent ? (
        <AgentScreen key={search.agent} agentId={search.agent} />
      ) : (
        <AgentsScreen />
      )}
    </TabbedPage>
  );
}

const SETTINGS_TO = {
  members: '/settings/members',
  webhooks: '/settings/webhooks',
  backups: '/settings/backups',
  providers: '/settings/providers',
  tokens: '/settings/tokens',
  account: '/settings/account',
} as const satisfies Record<SettingsPage, string>;

/**
 * `/settings/...`: Members, Webhooks, Backups, Providers, API tokens and
 * Account, one sub-navigation on the left and the page on the right. The
 * entry of the page in view is lit, a webhook's own page lighting Webhooks.
 * Providers is left out for a role that cannot read them.
 */
export function SettingsLayout() {
  const { me } = useSession();
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const current = settingsPageOf(pathname);
  const pages = SETTINGS_PAGES.filter((page) => page !== 'providers' || canManageProviders(me));

  return (
    <div className="set-layout">
      <div className="set-side">
        <h1 className="set-title">Settings</h1>
        <nav className="subnav set-nav" aria-label="Settings">
          {pages.map((page) => (
            <Link
              key={page}
              to={SETTINGS_TO[page]}
              className={cn(current === page && 'on')}
              aria-current={current === page ? 'page' : undefined}
            >
              {SETTINGS_LABELS[page]}
            </Link>
          ))}
        </nav>
      </div>
      <div className="min-w-0">
        <Outlet />
      </div>
    </div>
  );
}

/** `/settings/tokens`: the signed-in person's API tokens. */
export function SettingsTokens() {
  return (
    <SettingsColumn label="API tokens">
      <ApiTokens />
    </SettingsColumn>
  );
}
