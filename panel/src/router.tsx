import {
  createRootRoute,
  createRoute,
  createRouter,
  getRouteApi,
  Outlet,
  redirect,
} from '@tanstack/react-router';
import {
  newHome,
  validateAgentsSearch,
  validateApprovalsSearch,
  validateAuditSearch,
  validateGrantsSearch,
  validateNodesSearch,
  validateProjectSearch,
  validateProjectsSearch,
  type LegacyUrl,
} from './lib/routes';
import { SideShell } from './components/shell/side-shell';
import { AccountScreen } from './screens/account-screen';
import { ApprovalsScreen } from './screens/approvals-screen';
import { AuditScreen } from './screens/audit-screen';
import { ConsoleScreen } from './screens/console-screen';
import { LegacyAppRedirect } from './screens/legacy-app-redirect';
import { LoginScreen } from './screens/login-screen';
import { ProjectScreen } from './screens/project-screen';
import { ProjectsScreen } from './screens/projects-screen';
import { AgentsPage, NodesPage, SettingsLayout, SettingsTokens } from './screens/section-pages';
import { SetupScreen } from './screens/setup-screen';
import { TerminalScreen } from './screens/terminal-screen';
import { UsersScreen } from './screens/users-screen';
import { WebhookScreen } from './screens/webhook-screen';
import { WebhooksScreen } from './screens/webhooks-screen';
import { SessionProvider } from './session';

/*
 * Three kinds of route:
 * - full screen, outside the shell: sign-in, setup, the terminal and the console;
 * - the sidebar shell (`/shell`): Projects, Nodes, Agents, Approvals, Activity, Settings;
 * - a project (`/projects/$projectId`), with its own top bar and rail.
 * Every URL from before the redesign is still a route, and redirects to its
 * new place (`lib/routes.ts` holds the table and its tests).
 */

function RootLayout() {
  return (
    <SessionProvider>
      <Outlet />
    </SessionProvider>
  );
}

const rootRoute = createRootRoute({ component: RootLayout });

/* ---------- full screen ---------- */

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

/* ---------- a project ---------- */

const projectRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/projects/$projectId',
  validateSearch: validateProjectSearch,
  component: ProjectScreen,
});

/* ---------- the sidebar shell ---------- */

const shellRoute = createRoute({
  getParentRoute: () => rootRoute,
  id: 'shell',
  component: SideShell,
});

const projectsRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: '/projects',
  validateSearch: validateProjectsSearch,
  component: ProjectsScreen,
});

const nodesRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: '/nodes',
  validateSearch: validateNodesSearch,
  component: NodesPage,
});

const agentsRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: '/agents',
  validateSearch: validateAgentsSearch,
  component: AgentsPage,
});

const approvalsRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: '/approvals',
  validateSearch: validateApprovalsSearch,
  component: ApprovalsScreen,
});

const activityRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: '/activity',
  validateSearch: validateAuditSearch,
  component: AuditScreen,
});

const settingsRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: '/settings',
  component: SettingsLayout,
});

const settingsIndexRoute = createRoute({
  getParentRoute: () => settingsRoute,
  path: '/',
  beforeLoad: () => {
    throw redirect({ to: '/settings/members', replace: true });
  },
});

const settingsMembersRoute = createRoute({
  getParentRoute: () => settingsRoute,
  path: '/members',
  component: UsersScreen,
});

const settingsWebhooksRoute = createRoute({
  getParentRoute: () => settingsRoute,
  path: '/webhooks',
  component: WebhooksScreen,
});

function SettingsWebhook() {
  const { webhookId } = getRouteApi('/shell/settings/webhooks/$webhookId').useParams();
  return <WebhookScreen key={webhookId} webhookId={webhookId} />;
}

const settingsWebhookRoute = createRoute({
  getParentRoute: () => settingsRoute,
  path: '/webhooks/$webhookId',
  component: SettingsWebhook,
});

const settingsTokensRoute = createRoute({
  getParentRoute: () => settingsRoute,
  path: '/tokens',
  component: SettingsTokens,
});

const settingsAccountRoute = createRoute({
  getParentRoute: () => settingsRoute,
  path: '/account',
  component: AccountScreen,
});

/* ---------- old URLs: each one redirects to its new place ---------- */

/** Replace the old URL with its new home, so Back skips it. */
const moveTo = (url: LegacyUrl) => {
  const home = newHome(url);
  throw redirect({ ...home, replace: true } as Parameters<typeof redirect>[0]);
};

const legacyRootRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: '/',
  beforeLoad: () => moveTo({ path: '/' }),
});

const legacyAppNewRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: '/apps/new',
  beforeLoad: () => moveTo({ path: '/apps/new' }),
});

function LegacyApp() {
  const { appId } = getRouteApi('/shell/apps/$appId').useParams();
  return <LegacyAppRedirect appId={appId} />;
}

const legacyAppRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: '/apps/$appId',
  component: LegacyApp,
});

function LegacyBuild() {
  const { appId, buildId } = getRouteApi('/shell/apps/$appId/builds/$buildId').useParams();
  return <LegacyAppRedirect appId={appId} buildId={buildId} />;
}

const legacyBuildRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: '/apps/$appId/builds/$buildId',
  component: LegacyBuild,
});

const legacyVmsRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: '/vms',
  beforeLoad: () => moveTo({ path: '/vms' }),
});

const legacyAgentRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: '/agents/$agentId',
  beforeLoad: ({ params }) => moveTo({ path: '/agents/$agentId', agentId: params.agentId }),
});

const legacyGrantsRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: '/grants',
  validateSearch: validateGrantsSearch,
  beforeLoad: ({ search }) => moveTo({ path: '/grants', search }),
});

const legacyAuditRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: '/audit',
  validateSearch: validateAuditSearch,
  beforeLoad: ({ search }) => moveTo({ path: '/audit', search }),
});

const legacyWebhooksRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: '/webhooks',
  beforeLoad: () => moveTo({ path: '/webhooks' }),
});

const legacyWebhookRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: '/webhooks/$webhookId',
  beforeLoad: ({ params }) =>
    moveTo({ path: '/webhooks/$webhookId', webhookId: params.webhookId }),
});

const legacyUsersRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: '/users',
  beforeLoad: () => moveTo({ path: '/users' }),
});

const legacyAccountRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: '/account',
  beforeLoad: () => moveTo({ path: '/account' }),
});

const routeTree = rootRoute.addChildren([
  loginRoute,
  setupRoute,
  terminalRoute,
  consoleRoute,
  projectRoute,
  shellRoute.addChildren([
    projectsRoute,
    nodesRoute,
    agentsRoute,
    approvalsRoute,
    activityRoute,
    settingsRoute.addChildren([
      settingsIndexRoute,
      settingsMembersRoute,
      settingsWebhooksRoute,
      settingsWebhookRoute,
      settingsTokensRoute,
      settingsAccountRoute,
    ]),
    legacyRootRoute,
    legacyAppNewRoute,
    legacyAppRoute,
    legacyBuildRoute,
    legacyVmsRoute,
    legacyAgentRoute,
    legacyGrantsRoute,
    legacyAuditRoute,
    legacyWebhooksRoute,
    legacyWebhookRoute,
    legacyUsersRoute,
    legacyAccountRoute,
  ]),
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
