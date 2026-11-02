import {
  createRootRoute,
  createRoute,
  createRouter,
  Outlet,
} from '@tanstack/react-router';
import { ChassisHeader } from './components/chassis-header';
import { LoginScreen } from './screens/login-screen';
import { SetupScreen } from './screens/setup-screen';
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

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  component: VmsScreen,
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

const routeTree = rootRoute.addChildren([indexRoute, loginRoute, setupRoute]);

export const router = createRouter({
  routeTree,
  defaultPreload: 'intent',
});

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}
