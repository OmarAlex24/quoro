import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from '@tanstack/react-router';
import { logout } from '../api/generated/auth/auth';
import { useListNodes } from '../api/generated/nodes/nodes';
import { authEvents } from '../api/auth-store';
import { useSession } from '../session';
import { Button } from './ui/button';

const roleLabel = (role: string) => role.charAt(0).toUpperCase() + role.slice(1);

/** Panel-colour header: the nameplate, the cluster line and who is signed in. */
export function ChassisHeader() {
  const { user } = useSession();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const nodes = useListNodes(undefined, { query: { staleTime: 10_000 } });

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

  return (
    <header className="border-b border-rail bg-panel">
      <div className="mx-auto flex h-14 w-full max-w-6xl items-center gap-3 px-4 sm:gap-4 sm:px-6">
        <span className="type-display text-[18px] leading-none">Quoro</span>
        <span aria-hidden="true" className="hidden h-5 w-px bg-rail sm:block" />
        <p className="type-data hidden min-w-0 truncate text-[13px] text-ink-muted sm:block">
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
