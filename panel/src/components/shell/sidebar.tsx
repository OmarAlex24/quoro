import { Link, useRouterState } from '@tanstack/react-router';
import {
  Activity,
  Bell,
  Bot,
  LayoutGrid,
  Server,
  SlidersVertical,
  type LucideIcon,
} from 'lucide-react';
import { connectedLine, nodeReadout, primaryLine } from '../../lib/nodes';
import { sectionOf, type Section } from '../../lib/routes';
import { cn } from '../../lib/utils';
import { useSession } from '../../session';
import { QuoroMark } from '../avatar';
import { LampDot } from '../lamp';
import { AvatarMenu } from './avatar-menu';
import {
  roleLabel,
  useClusterSummary,
  usePendingApprovalCount,
  useWorkspaceName,
} from './shell-data';

interface NavItem {
  section: Section;
  to: '/projects' | '/nodes' | '/agents' | '/approvals' | '/activity' | '/settings/members';
  label: string;
  Icon: LucideIcon;
}

/** Six sections, down from nine links: Grants lives in Agents, Webhooks and Users in Settings. */
const NAV: readonly NavItem[] = [
  { section: 'projects', to: '/projects', label: 'Projects', Icon: LayoutGrid },
  { section: 'nodes', to: '/nodes', label: 'Nodes', Icon: Server },
  { section: 'agents', to: '/agents', label: 'Agents', Icon: Bot },
  { section: 'approvals', to: '/approvals', label: 'Approvals', Icon: Bell },
  { section: 'activity', to: '/activity', label: 'Activity', Icon: Activity },
  { section: 'settings', to: '/settings/members', label: 'Settings', Icon: SlidersVertical },
];

/** Pending approvals as a red count; nothing when none waits. */
export function PendingCount({ count }: { count: number }) {
  if (count === 0) return null;
  return (
    <span className="count">
      {count}
      <span className="sr-only"> waiting</span>
    </span>
  );
}

/**
 * The sidebar: the workspace (cluster name and role) with the avatar menu,
 * the sections, and the cluster's primary node at the foot. In the drawer the
 * avatar stays in the top bar instead.
 */
export function Sidebar({ drawer = false }: { drawer?: boolean }) {
  const { user } = useSession();
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const active = sectionOf(pathname);
  const pending = usePendingApprovalCount();
  const workspace = useWorkspaceName();

  return (
    <aside className={cn('side', drawer && 'side-drawer')} aria-label="Sidebar">
      <div className="ws">
        <QuoroMark className="logo" />
        <div className="min-w-0 flex-1">
          <b className="truncate">{workspace}</b>
          {user ? <small>{roleLabel(user.role)}</small> : null}
        </div>
        {drawer ? null : <AvatarMenu placement="sidebar" />}
      </div>

      <nav className="nav" aria-label="Sections">
        {NAV.map(({ section, to, label, Icon }) => (
          <Link
            key={section}
            to={to}
            className={cn(active === section && 'on')}
            aria-current={active === section ? 'page' : undefined}
          >
            <Icon strokeWidth={1.7} aria-hidden="true" />
            {label}
            {section === 'approvals' ? <PendingCount count={pending} /> : null}
          </Link>
        ))}
      </nav>

      <SideFoot />
    </aside>
  );
}

function SideFoot() {
  const { summary, pending } = useClusterSummary();
  const primary = summary.primary;
  const readout = primary ? nodeReadout(primary.status) : null;

  return (
    <div className="side-foot">
      {primary && readout ? (
        <span className="state">
          <LampDot
            ring={readout.tone}
            core={readout.tone}
            pulse={readout.pulse}
            size={15}
            label={readout.words}
          />
          <span className="truncate">{primaryLine(primary)}</span>
        </span>
      ) : (
        <span>{pending ? 'Checking the cluster…' : 'No node connected'}</span>
      )}
      {pending ? null : <span className="text-ink-3">{connectedLine(summary)}</span>}
    </div>
  );
}
