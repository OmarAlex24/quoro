import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Link, useNavigate, useRouterState } from '@tanstack/react-router';
import { logout } from '../api/generated/auth/auth';
import { useListApprovals } from '../api/generated/approvals/approvals';
import { useListNodes } from '../api/generated/nodes/nodes';
import { authEvents } from '../api/auth-store';
import { useSession } from '../session';
import { cn } from '../lib/utils';
import { Avatar, QuoroMark } from './avatar';
import { Button } from './ui/button';

interface NavItem {
  to:
    | '/'
    | '/vms'
    | '/nodes'
    | '/agents'
    | '/grants'
    | '/approvals'
    | '/audit'
    | '/webhooks'
    | '/users';
  label: string;
  match: (pathname: string) => boolean;
}

const under = (prefix: string) => (pathname: string) =>
  pathname === prefix || pathname.startsWith(`${prefix}/`);

/** What runs: apps and their machines. */
const MACHINE_SECTIONS: NavItem[] = [
  { to: '/', label: 'Apps', match: (pathname) => pathname === '/' || under('/apps')(pathname) },
  { to: '/vms', label: 'Virtual machines', match: under('/vms') },
  { to: '/nodes', label: 'Nodes', match: under('/nodes') },
];

/** Who may act on it, and the record of what they did. */
const AGENCY_SECTIONS: NavItem[] = [
  { to: '/agents', label: 'Agents', match: under('/agents') },
  { to: '/grants', label: 'Grants', match: under('/grants') },
  { to: '/approvals', label: 'Approvals', match: under('/approvals') },
  { to: '/audit', label: 'Audit log', match: under('/audit') },
  { to: '/webhooks', label: 'Webhooks', match: under('/webhooks') },
  { to: '/users', label: 'Users', match: under('/users') },
];

const roleLabel = (role: string) => role.charAt(0).toUpperCase() + role.slice(1);

/** The header: the mark, the sections and who is signed in. */
export function ChassisHeader() {
  const { user } = useSession();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const nodes = useListNodes(undefined, { query: { staleTime: 10_000 } });
  const pending = useListApprovals(
    { status: 'pending' },
    { query: { enabled: user !== undefined, refetchInterval: 10_000, retry: false } },
  );
  const pendingCount = pending.data?.items.length ?? 0;
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

  const sectionLink = (item: NavItem) => {
    const active = item.match(pathname);
    return (
      <Link
        key={item.to}
        to={item.to}
        className={cn(
          'flex items-center gap-1.5 border-b-2 pt-0.5 text-[14px] underline-offset-4 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent',
          active
            ? 'border-ink text-ink'
            : 'border-transparent text-ink-2 hover:border-line-2 hover:text-ink',
        )}
        aria-current={active ? 'page' : undefined}
      >
        {item.label}
        {item.to === '/approvals' && pendingCount > 0 ? (
          <span className="type-data inline-grid h-[17px] min-w-[17px] place-items-center rounded-[4px] bg-fault px-1 text-[10.5px] font-[650] leading-none text-on-accent">
            {pendingCount}
            <span className="sr-only"> waiting</span>
          </span>
        ) : null}
      </Link>
    );
  };

  return (
    <header className="border-b border-line bg-ground">
      <div className="mx-auto flex min-h-14 w-full max-w-6xl flex-wrap items-center gap-x-3 gap-y-1 px-4 py-1.5 sm:gap-x-4 sm:px-6">
        <span className="flex items-center gap-2">
          <QuoroMark className="h-6 w-6" />
          <span className="type-display text-[18px] leading-none">Quoro</span>
        </span>
        <span aria-hidden="true" className="hidden h-5 w-px bg-line sm:block" />

        <nav
          aria-label="Sections"
          className="flex flex-wrap items-stretch gap-x-4 gap-y-1 self-stretch"
        >
          {MACHINE_SECTIONS.map(sectionLink)}
          <span aria-hidden="true" className="my-auto h-4 w-px bg-line" />
          {AGENCY_SECTIONS.map(sectionLink)}
        </nav>

        <p className="type-data hidden min-w-0 truncate text-[13px] text-ink-2 xl:block">
          {clusterLine}
        </p>
        <div className="ml-auto flex items-center gap-2 sm:gap-3">
          <Link
            to="/account"
            className="flex min-w-0 items-center gap-2 underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
          >
            <Avatar kind="human" name={user.name} />
            <span className="type-body-medium max-w-[14rem] truncate text-[13px] text-ink">
              {user.name}
            </span>
            <span className="type-body-medium text-[13px] text-ink-2">
              {roleLabel(user.role)}
            </span>
          </Link>
          <span aria-hidden="true" className="h-4 w-px bg-line" />
          <Button
            variant="ghost"
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
