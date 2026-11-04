import {
  createRootRoute,
  createRoute,
  createRouter,
  Outlet,
} from '@tanstack/react-router';
import { ChassisHeader } from './components/chassis-header';
import type { AuditRecordResult, GrantKind, GrantStatus } from './api/generated/model';
import { AgentScreen } from './screens/agent-screen';
import { AgentsScreen } from './screens/agents-screen';
import { ApprovalsScreen } from './screens/approvals-screen';
import { AuditScreen } from './screens/audit-screen';
import { GrantsScreen } from './screens/grants-screen';
import { WebhookScreen } from './screens/webhook-screen';
import { WebhooksScreen } from './screens/webhooks-screen';
import { AppsScreen } from './screens/apps-screen';
import { AppScreen } from './screens/app-screen';
import { ConsoleScreen } from './screens/console-screen';
import { DeployScreen } from './screens/deploy-screen';
import { LoginScreen } from './screens/login-screen';
import { NodesScreen } from './screens/nodes-screen';
import { SetupScreen } from './screens/setup-screen';
import { UsersScreen } from './screens/users-screen';
import { TerminalScreen } from './screens/terminal-screen';
import { VmsScreen } from './screens/vms-screen';
import { SessionProvider } from './session';

function RootLayout() {
  return (
    <SessionProvider>
      <div className="min-h-dvh">
        <ChassisHeader />
        <Outlet />
      </div>
    </SessionProvider>
  );
}

const rootRoute = createRootRoute({ component: RootLayout });

const appsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  component: AppsScreen,
});

const appDeployRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/apps/new',
  component: DeployScreen,
});

const appDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/apps/$appId',
  component: AppScreen,
});

const vmsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/vms',
  component: VmsScreen,
});

const nodesRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/nodes',
  component: NodesScreen,
});

const terminalRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/apps/$appId/terminal',
  component: TerminalScreen,
});

const consoleRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/vms/$vmId/console',
  component: ConsoleScreen,
});

const agentsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/agents',
  component: AgentsScreen,
});

const agentDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/agents/$agentId',
  component: AgentScreen,
});

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

/** `/grants?agent_id=&status=&kind=`: the filters, shareable as a link. */
export interface GrantsSearch {
  agent_id?: string;
  status?: GrantStatus;
  kind?: GrantKind;
}

const grantsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/grants',
  component: GrantsScreen,
  validateSearch: (search: Record<string, unknown>): GrantsSearch => ({
    agent_id: text(search.agent_id),
    status: pick(search.status, ['active', 'expired', 'revoked', 'exhausted'] as const),
    kind: pick(search.kind, ['standard', 'incident', 'approval'] as const),
  }),
});

const approvalsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/approvals',
  component: ApprovalsScreen,
});

/** `/audit?actor_id=&target_id=&result=&incident_id=&seq=`: filters plus the open record. */
export interface AuditSearch {
  actor_id?: string;
  target_id?: string;
  result?: AuditRecordResult;
  incident_id?: string;
  seq?: number;
}

const auditRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/audit',
  component: AuditScreen,
  validateSearch: (search: Record<string, unknown>): AuditSearch => ({
    actor_id: text(search.actor_id),
    target_id: text(search.target_id),
    result: pick(search.result, ['ok', 'refused', 'error'] as const),
    incident_id: text(search.incident_id),
    seq: positiveInt(search.seq),
  }),
});

const webhooksRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/webhooks',
  component: WebhooksScreen,
});

const webhookDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/webhooks/$webhookId',
  component: WebhookScreen,
});

const usersRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/users',
  component: UsersScreen,
});

const loginRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/login',
  component: LoginScreen,
});

const setupRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/setup',
  component: SetupScreen,
});

const routeTree = rootRoute.addChildren([
  appsRoute,
  appDeployRoute,
  appDetailRoute,
  vmsRoute,
  nodesRoute,
  terminalRoute,
  consoleRoute,
  agentsRoute,
  agentDetailRoute,
  grantsRoute,
  approvalsRoute,
  auditRoute,
  webhooksRoute,
  webhookDetailRoute,
  usersRoute,
  loginRoute,
  setupRoute,
]);

export const router = createRouter({
  routeTree,
  defaultPreload: 'intent',
});

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}
