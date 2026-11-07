import type { ReactNode } from 'react';
import { cn } from '../../lib/utils';

/**
 * A row of tabs over a section page (Nodes, Agents). The page under it keeps
 * its own column; this lines up with it and pulls it up to the strip.
 */
export function TabbedPage({
  label,
  tabs,
  title,
  narrow = false,
  children,
}: {
  label: string;
  tabs: ReactNode;
  /** The section's heading over the tabs, when the page under them has none. */
  title?: string;
  /** The page under it is a single record in the narrower column. */
  narrow?: boolean;
  children: ReactNode;
}) {
  return (
    <div className="[&>main]:pt-5">
      <div
        className={cn(
          'mx-auto w-full px-4 pt-6 sm:px-6 sm:pt-8',
          narrow ? 'max-w-4xl' : 'max-w-6xl',
        )}
      >
        {title ? <h1 className="type-display mb-5 text-[24px] leading-none">{title}</h1> : null}
        <nav className="tabs" aria-label={label}>
          {tabs}
        </nav>
      </div>
      {children}
    </div>
  );
}
