import type { ReactNode } from 'react';
import { cn } from '../../lib/utils';

/**
 * A row of tabs over a section page (Nodes, Agents). The page under it keeps
 * its own column; this lines up with it and pulls it up to the strip.
 */
export function TabbedPage({
  label,
  tabs,
  narrow = false,
  children,
}: {
  label: string;
  tabs: ReactNode;
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
        <nav className="tabs" aria-label={label}>
          {tabs}
        </nav>
      </div>
      {children}
    </div>
  );
}
