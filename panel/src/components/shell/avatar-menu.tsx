import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Link, useNavigate } from '@tanstack/react-router';
import { LogOut, Monitor, Moon, Sun, UserRound, type LucideIcon } from 'lucide-react';
import { useId } from 'react';
import { logout } from '../../api/generated/auth/auth';
import { authEvents } from '../../api/auth-store';
import {
  setThemeChoice,
  THEME_CHOICES,
  useThemeChoice,
  type ThemeChoice,
} from '../../lib/use-theme';
import { cn } from '../../lib/utils';
import { useSession } from '../../session';
import { Avatar } from '../avatar';
import { Tag } from '../ui/tag';
import { MenuPanel, useMenu } from './menu';
import { roleLabel } from './shell-data';

export const THEME_WORDS: Record<ThemeChoice, { label: string; Icon: LucideIcon }> = {
  system: { label: 'System', Icon: Monitor },
  dark: { label: 'Dark', Icon: Moon },
  light: { label: 'Light', Icon: Sun },
};

/**
 * The signed-in person as an avatar; its menu holds who they are, the
 * account, the theme (stored per browser) and sign-out. In the sidebar the
 * menu opens under the whole workspace block; in a top bar, under the avatar.
 */
export function AvatarMenu({ placement }: { placement: 'sidebar' | 'topbar' }) {
  const { user } = useSession();
  const menu = useMenu();
  const choice = useThemeChoice();
  const themeLabelId = useId();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const signOut = useMutation({
    mutationFn: () => logout(),
    onSuccess: () => {
      authEvents.endSession();
      queryClient.clear();
      void navigate({ to: '/login' });
    },
  });

  if (!user) return null;

  return (
    <div ref={menu.rootRef} className={cn(placement === 'topbar' && 'relative')}>
      <button
        ref={menu.triggerRef}
        type="button"
        className="iconbtn"
        aria-label={`Account menu, ${user.name}`}
        {...menu.triggerProps}
      >
        <Avatar kind="human" name={user.name} />
      </button>
      <MenuPanel
        menu={menu}
        label="Account"
        align={placement === 'topbar' ? 'end' : 'start'}
        className={cn(placement === 'sidebar' && 'w-[260px]')}
      >
        <div className="flex items-center gap-3 px-2.5 pb-2.5 pt-1.5">
          <Avatar kind="human" name={user.name} size="lg" />
          <div className="min-w-0 flex-1">
            <p className="flex min-w-0 items-center gap-2">
              <span className="truncate text-[13.5px] font-[600] leading-tight text-ink">
                {user.name}
              </span>
              <Tag tone="plain">{roleLabel(user.role)}</Tag>
            </p>
            <p className="mt-0.5 truncate text-[12px] leading-snug text-ink-2" title={user.email}>
              {user.email}
            </p>
          </div>
        </div>
        <div className="menu-sep" role="separator" />
        <Link to="/settings/account" role="menuitem" className="menu-item">
          <UserRound strokeWidth={1.7} aria-hidden="true" />
          Account
        </Link>
        <div className="menu-sep" role="separator" />
        <p id={themeLabelId} className="menu-label">
          Theme
        </p>
        <div role="group" aria-labelledby={themeLabelId} className="seg mx-1 mb-1.5">
          {THEME_CHOICES.map((value) => {
            const { label, Icon } = THEME_WORDS[value];
            return (
              <button
                key={value}
                type="button"
                role="menuitemradio"
                aria-checked={choice === value}
                data-keep-open=""
                onClick={() => setThemeChoice(value)}
              >
                <Icon strokeWidth={1.7} aria-hidden="true" />
                {label}
              </button>
            );
          })}
        </div>
        <div className="menu-sep" role="separator" />
        <button
          type="button"
          role="menuitem"
          className="menu-item"
          data-keep-open=""
          disabled={signOut.isPending}
          onClick={() => signOut.mutate()}
        >
          <LogOut strokeWidth={1.7} aria-hidden="true" />
          {signOut.isPending ? 'Signing out…' : 'Sign out'}
        </button>
      </MenuPanel>
    </div>
  );
}
