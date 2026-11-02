import type { ReactNode } from 'react';
import { cn } from '../../lib/utils';
import { problemFacts, type ProblemView } from '../../lib/problem';

/**
 * A refusal or failure, said in plain words. Panel surface with a fault-red
 * edge; the machine facts (code, reason) sit in muted text below.
 */
export function ProblemBanner({
  view,
  className,
}: {
  view: ProblemView;
  className?: string;
}) {
  const facts = problemFacts(view);
  return (
    <div
      role="alert"
      className={cn(
        'rounded-[6px] border border-rail border-l-4 border-l-lamp-fault bg-panel px-3.5 py-2.5 shadow-[0_1px_0_rgb(20_32_48_/_0.04)]',
        className,
      )}
    >
      <p className="text-[15px] leading-snug text-ink">{view.message}</p>
      {facts ? (
        <p className="type-data mt-1 text-[13px] text-ink-muted">{facts}</p>
      ) : null}
    </div>
  );
}

/** A neutral confirmation line, e.g. "Stopped web-1". */
export function NoticeLine({ children }: { children: ReactNode }) {
  return (
    <p aria-live="polite" className="type-data text-[13px] text-ink-muted">
      {children}
    </p>
  );
}
