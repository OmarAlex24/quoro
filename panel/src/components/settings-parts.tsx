import { MoreHorizontal, type LucideIcon } from 'lucide-react';
import type { ReactNode } from 'react';
import { cn } from '../lib/utils';
import { MenuPanel, useMenu } from './shell/menu';
import { SkeletonRows } from './skeleton';

/*
 * The one pattern every Settings page shares: a heading with its one action,
 * then a list in a box where each row is a thing, its state in one line and a
 * ⋯ menu with what can be done to it.
 */

/** A Settings page's column. */
export function SettingsPage({ label, children }: { label: string; children: ReactNode }) {
  return (
    <main className="set-page" aria-label={label}>
      {children}
    </main>
  );
}

/** The page's heading, the sentence under it, and its one action on the right. */
export function SettingsHead({
  title,
  lede,
  action,
}: {
  title: string;
  lede?: ReactNode;
  action?: ReactNode;
}) {
  return (
    <div className="set-page-h">
      <div className="min-w-0 flex-1">
        <h2>{title}</h2>
        {lede ? <p>{lede}</p> : null}
      </div>
      {action}
    </div>
  );
}

/** A line under the heading: what this role can and cannot do here. */
export function SettingsNote({ children }: { children: ReactNode }) {
  return <p className="set-note">{children}</p>;
}

/** A box while its list loads: two rows of the dither skeleton, so the page does not jump. */
export function SettingsLoading({ label }: { label: string }) {
  return <SkeletonRows label={label} rows={2} />;
}

/** The ⋯ menu at the end of a row. */
export function RowMenu({
  label,
  disabled = false,
  children,
}: {
  label: string;
  disabled?: boolean;
  children: ReactNode;
}) {
  const menu = useMenu();
  return (
    <div ref={menu.rootRef} className="relative row-menu">
      <button
        ref={menu.triggerRef}
        type="button"
        className="iconbtn"
        aria-label={label}
        disabled={disabled}
        {...menu.triggerProps}
      >
        <MoreHorizontal strokeWidth={1.7} aria-hidden="true" />
      </button>
      <MenuPanel menu={menu} label={label} align="end" className="min-w-[200px]">
        {children}
      </MenuPanel>
    </div>
  );
}

/** One item of a row's menu: an icon and a few words. `danger` asks before it acts. */
export function RowMenuItem({
  icon: Icon,
  danger = false,
  checked,
  onSelect,
  children,
}: {
  icon: LucideIcon;
  danger?: boolean;
  /** For a choice among several (a role): the one in effect is checked. */
  checked?: boolean;
  onSelect: () => void;
  children: ReactNode;
}) {
  return (
    <button
      type="button"
      role={checked === undefined ? 'menuitem' : 'menuitemradio'}
      aria-checked={checked}
      className={cn('menu-item', danger && 'menu-item-danger')}
      onClick={onSelect}
    >
      <Icon strokeWidth={1.7} aria-hidden="true" />
      <span className="min-w-0 flex-1">{children}</span>
    </button>
  );
}
