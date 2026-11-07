import { getRouteApi, Link, Outlet } from '@tanstack/react-router';
import { useListVms } from '../api/generated/vms/vms';
import { cn } from '../lib/utils';
import { useSession } from '../session';
import { ApiTokens } from '../components/api-tokens';
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

/** `/nodes` and `/nodes?tab=vms`: the machines, and every VM of the cluster. */
export function NodesPage() {
  const { tab } = nodesRoute.useSearch();
  const { user } = useSession();
  const vms = useListVms(undefined, { query: { enabled: user !== undefined } });
  const vmCount = vms.data?.items.length;

  return (
    <TabbedPage
      label="Nodes"
      tabs={
        <>
          <Link
            to="/nodes"
            search={{}}
            className={cn(tab === undefined && 'on')}
            aria-current={tab === undefined ? 'page' : undefined}
          >
            Nodes
          </Link>
          <Link
            to="/nodes"
            search={{ tab: 'vms' }}
            className={cn(tab === 'vms' && 'on')}
            aria-current={tab === 'vms' ? 'page' : undefined}
          >
            Virtual machines
            {vmCount !== undefined ? <span className="count count-soft">{vmCount}</span> : null}
          </Link>
        </>
      }
    >
      {tab === 'vms' ? <VmsScreen /> : <NodesScreen />}
    </TabbedPage>
  );
}

/** `/agents`, `/agents?agent=` and `/agents?tab=grants`: who may act, and what they may do. */
export function AgentsPage() {
  const search = agentsRoute.useSearch();
  const grants = search.tab === 'grants';

  return (
    <TabbedPage
      label="Agents"
      narrow={!grants && search.agent !== undefined}
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
