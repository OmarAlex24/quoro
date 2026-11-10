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
  children: ReactNode;
}) {
  const { navRef, inkRef } = useTabInk(current);
  // The page is whatever follows the strip's own column.
  const pageRef = usePaneEnter<HTMLDivElement>(current, (root) => Array.from(root.children).slice(1));
  return (
    <div ref={pageRef} className="[&>main]:pt-5">
      <div
        className={cn(
          'mx-auto w-full px-4 pt-6 sm:px-6 sm:pt-8',
          narrow ? 'max-w-4xl' : 'max-w-6xl',
        )}
      >
        {title ? <h1 className="type-title mb-5">{title}</h1> : null}
        <nav ref={navRef} className="tabs" aria-label={label}>
          {tabs}
          <span ref={inkRef} className="tab-ink" aria-hidden="true" />
        </nav>
      </div>
      {children}
    </div>
  );
}
