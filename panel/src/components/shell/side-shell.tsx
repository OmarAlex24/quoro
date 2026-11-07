import * as DialogPrimitive from '@radix-ui/react-dialog';
import { Link, Outlet, useRouterState } from '@tanstack/react-router';
import { Bell, Menu, X } from 'lucide-react';
import { useEffect, useState, type ReactNode } from 'react';
import { LoginScreen } from '../../screens/login-screen';
import { useSession } from '../../session';
import { QuoroMark } from '../avatar';
import { AvatarMenu } from './avatar-menu';
import { usePendingApprovalCount, useWorkspaceName } from './shell-data';
import { PendingCount, Sidebar } from './sidebar';

/** Shell pages need a person: sign-in shows full screen, without the shell. */
export function SessionGate({ children }: { children: ReactNode }) {
  const { user, loading, unauthenticated } = useSession();
  if (loading) {
    return (
      <div className="grid min-h-dvh place-items-center bg-ground">
        <p className="text-[14px] text-ink-3">Loading…</p>
      </div>
    );
  }
  if (unauthenticated || !user) return <LoginScreen />;
  return <>{children}</>;
}

/** Projects, Nodes, Agents, Approvals, Activity and Settings: the sidebar and the page. */
export function SideShell() {
  return (
    <SessionGate>
      <div className="shell">
        <div className="side-col">
          <Sidebar />
        </div>
        <div className="shell-content">
          <MobileBar />
          <Outlet />
        </div>
      </div>
    </SessionGate>
  );
}

const WIDE = '(min-width: 768px)';

/** Below 768 px: the sidebar folds into a menu button, the avatar sits on the right. */
function MobileBar() {
  const [open, setOpen] = useState(false);
  const href = useRouterState({ select: (state) => state.location.href });
  const workspace = useWorkspaceName();
  const pending = usePendingApprovalCount();

  // Close after a section is chosen, and when the window grows past the breakpoint.
  useEffect(() => setOpen(false), [href]);
  useEffect(() => {
    const query = window.matchMedia(WIDE);
    const onChange = () => {
      if (query.matches) setOpen(false);
    };
    query.addEventListener('change', onChange);
    return () => query.removeEventListener('change', onChange);
  }, []);

  return (
    <header className="top md:hidden">
      <DialogPrimitive.Root open={open} onOpenChange={setOpen}>
        <DialogPrimitive.Trigger className="iconbtn -ml-1.5" aria-label="Open the sections">
          <Menu strokeWidth={1.7} aria-hidden="true" />
        </DialogPrimitive.Trigger>
        <DialogPrimitive.Portal>
          <DialogPrimitive.Overlay className="dialog-overlay fixed inset-0 z-40 bg-scrim" />
          <DialogPrimitive.Content
            className="drawer fixed inset-y-0 left-0 z-50 w-[min(284px,86vw)] overflow-y-auto border-r border-line-2 bg-ground shadow-[var(--shadow)]"
            aria-describedby={undefined}
          >
            <DialogPrimitive.Title className="sr-only">Sections</DialogPrimitive.Title>
            <DialogPrimitive.Close
              className="iconbtn absolute right-2 top-2.5 z-10"
              aria-label="Close the sections"
            >
              <X strokeWidth={1.7} aria-hidden="true" />
            </DialogPrimitive.Close>
            <Sidebar drawer />
          </DialogPrimitive.Content>
        </DialogPrimitive.Portal>
      </DialogPrimitive.Root>

      <QuoroMark className="logo" />
      <span className="min-w-0 flex-1 truncate font-[600]">{workspace}</span>
      <Link
        to="/approvals"
        className="iconbtn"
        aria-label={pending > 0 ? `Approvals, ${pending} waiting` : 'Approvals'}
      >
        <Bell strokeWidth={1.7} aria-hidden="true" />
        <PendingCount count={pending} />
      </Link>
      <span className="vbar" aria-hidden="true" />
      <AvatarMenu placement="topbar" />
    </header>
  );
}
