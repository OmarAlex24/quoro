import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Link, useNavigate, useRouterState } from '@tanstack/react-router';
import { logout } from '../api/generated/auth/auth';
import { useListNodes } from '../api/generated/nodes/nodes';
import { authEvents } from '../api/auth-store';
import { useSession } from '../session';
import { cn } from '../lib/utils';
import { Button } from './ui/button';

const roleLabel = (role: string) => role.charAt(0).toUpperCase() + role.slice(1);

/** Panel-colour header: the nameplate, the two sections and who is signed in. */
export function ChassisHeader() {
  const { user } = useSession();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const nodes = useListNodes(undefined, { query: { staleTime: 10_000 } });
  const pathname = useRouterState({ select: (state) => state.location.pathname });

  const logoutMutation = useMutation({
    mutationFn: () => logout(),
    onSuccess: () => {
      authEvents.endSession();
      queryClient.clear();
      void navigate({ to: '/login' });
    },
  });

  const nodesItems = nodes.data?.items;
  const primary =
    nodesItems?.find((node) => node.cp_status === 'primary') ?? nodesItems?.[0];

  const clusterLine = primary
    ? `${primary.name} · ${primary.cp_status ?? 'node'} · ${primary.status}`
    : nodes.isPending
      ? 'Checking the cluster…'
      : 'No node connected';

  if (!user) return null;

  const appsActive = pathname === '/' || pathname.startsWith('/apps');
  const vmsActive = pathname === '/vms' || pathname.startsWith('/vms/');

  return (
    <header className="border-b border-rail bg-panel">
      <div className="mx-auto flex min-h-14 w-full max-w-6xl flex-wrap items-center gap-x-3 gap-y-1 px-4 py-1.5 sm:gap-x-4 sm:px-6">
        <span className="type-display text-[18px] leading-none">Quoro</span>
        <span aria-hidden="true" className="hidden h-5 w-px bg-rail sm:block" />

        <nav aria-label="Sections" className="flex items-stretch gap-4 self-stretch">
          <Link
            to="/"
            className={cn(
              'flex items-center border-b-2 pt-0.5 text-[14px] underline-offset-4 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-cobalt',
              appsActive
                ? 'border-cobalt text-ink'
                : 'border-transparent text-ink-muted hover:border-rail hover:text-ink',
            )}
            aria-current={appsActive ? 'page' : undefined}
          >
            Apps
          </Link>
          <Link
            to="/vms"
            className={cn(
              'flex items-center border-b-2 pt-0.5 text-[14px] underline-offset-4 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-cobalt',
              vmsActive
                ? 'border-cobalt text-ink'
                : 'border-transparent text-ink-muted hover:border-rail hover:text-ink',
            )}
            aria-current={vmsActive ? 'page' : undefined}
          >
            Virtual machines
          </Link>
        </nav>

        <p className="type-data hidden min-w-0 truncate text-[13px] text-ink-muted md:block">
          {clusterLine}
        </p>
        <div className="ml-auto flex items-center gap-2 sm:gap-3">
          <span className="type-body-medium text-[13px] text-ink-muted">
            {roleLabel(user.role)}
          </span>
          <span aria-hidden="true" className="h-4 w-px bg-rail" />
          <Button
            variant="quiet"
            className="px-2.5"
            onClick={() => logoutMutation.mutate()}
            disabled={logoutMutation.isPending}
          >
            {logoutMutation.isPending ? 'Logging out…' : 'Log out'}
          </Button>
        </div>
      </div>
    </header>
  );
}
