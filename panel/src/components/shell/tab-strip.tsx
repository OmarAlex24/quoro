import type { ReactNode } from 'react';
import { usePaneEnter, useTabInk } from '../../lib/use-tab-motion';
import { cn } from '../../lib/utils';

/**
 * A row of tabs over a section page (Nodes, Agents). The page under it keeps
 * its own column; this lines up with it and pulls it up to the strip. The
 * tabs are links (the tab is in the URL); `current` names the one shown, so
 * the bar can slide to it and the page under it can enter.
 */
export function TabbedPage({
  label,
  tabs,
  current,
  title,
  narrow = false,
  wide = false,
  action,
  children,
}: {
  label: string;
  tabs: ReactNode;
  /** The tab shown now. */
  current: string;
  /** The section's heading over the tabs, when the page under them has none. */
  title?: string;
  /** The page under it is a single record in the narrower column. */
  narrow?: boolean;
  /**
   * The section uses the whole width beside the sidebar, as the Projects
   * board does: the heading, the tabs and every tab's page share its gutters.
   */
  wide?: boolean;
  /** A control at the right of the heading, such as "Add a node". */
  action?: ReactNode;
  children: ReactNode;
}) {
  const { navRef, inkRef } = useTabInk(current);
  // The page is whatever follows the strip's own column.
  const pageRef = usePaneEnter<HTMLDivElement>(current, (root) => Array.from(root.children).slice(1));
  return (
    <div
      ref={pageRef}
      className={cn(
        wide
          ? 'tabbed-wide [&>main]:mx-0 [&>main]:max-w-none [&>main]:px-[14px] [&>main]:pt-4 md:[&>main]:px-[28px]'
          : '[&>main]:pt-5',
      )}
    >
      <div
        className={cn(
          wide
            ? 'tabbed-head w-full'
            : cn('mx-auto w-full px-4 pt-6 sm:px-6 sm:pt-8', narrow ? 'max-w-4xl' : 'max-w-6xl'),
        )}
      >
        {title && wide ? (
          <div className="board-head">
            <h1 className="type-title">{title}</h1>
            {action}
          </div>
        ) : title ? (
          <h1 className="type-title mb-5">{title}</h1>
        ) : null}
        <nav ref={navRef} className="tabs" aria-label={label}>
          {tabs}
          <span ref={inkRef} className="tab-ink" aria-hidden="true" />
        </nav>
      </div>
      {children}
    </div>
  );
}
