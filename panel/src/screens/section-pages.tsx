import { getRouteApi, Link, Outlet } from '@tanstack/react-router';
import { useListNodes } from '../api/generated/nodes/nodes';
import { useListVms } from '../api/generated/vms/vms';
import { useGetVulnerabilitySummary } from '../api/generated/vulnerabilities/vulnerabilities';
import { SERVICE_LIST_PARAMS } from '../lib/projects';
import type { NodesTab } from '../lib/routes';
import { cn } from '../lib/utils';
import { nodeSecurityCount } from '../lib/vulnerabilities';
import { useSession } from '../session';
import { ApiTokens } from '../components/api-tokens';
import { ClusterVulnerabilities } from '../components/cluster-vulnerabilities';
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

const SETTINGS = [
  { to: '/settings/members', label: 'Members' },
  { to: '/settings/webhooks', label: 'Webhooks' },
  { to: '/settings/tokens', label: 'API tokens' },
  { to: '/settings/account', label: 'Account' },
] as const;

/**
 * `/settings/...`: Members, Webhooks, API tokens and Account, each today's
 * screen. Backups and providers join this list when their screens exist.
 */
export function SettingsLayout() {
  return (
    <div className="mx-auto grid w-full max-w-6xl gap-x-8 gap-y-4 px-4 py-6 sm:px-6 sm:py-8 md:grid-cols-[170px_minmax(0,1fr)]">
      <div>
        <h1 className="type-display-light px-2.5 pb-3 text-[19px] leading-none">Settings</h1>
        <nav className="subnav max-md:flex max-md:flex-wrap" aria-label="Settings">
          {SETTINGS.map((item) => (
            <Link
              key={item.to}
              to={item.to}
              activeOptions={{ exact: false }}
              activeProps={{ className: 'on', 'aria-current': 'page' }}
            >
              {item.label}
            </Link>
          ))}
        </nav>
      </div>
      <div className="min-w-0 [&>main]:max-w-none [&>main]:p-0">
        <Outlet />
      </div>
    </div>
  );
}

/** `/settings/tokens`: the signed-in person's API tokens, also on their account. */
export function SettingsTokens() {
  return (
    <main className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 sm:py-8 [&>section]:mt-0">
      <ApiTokens />
    </main>
  );
}
