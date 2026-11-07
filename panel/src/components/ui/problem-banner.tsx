import type { ReactNode } from 'react';
import { cn } from '../../lib/utils';
import { problemFacts, type ProblemView } from '../../lib/problem';

/**
 * A refusal or failure, said in plain words, on a faint fault tint; the
 * machine facts (code, reason) sit below as code.
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
        'rounded-[8px] border border-[color-mix(in_oklab,var(--fault)_35%,var(--line))] bg-[color-mix(in_oklab,var(--fault)_8%,var(--surface))] px-3 py-2.5',
        className,
      )}
    >
      <p className="text-[13.5px] font-[600] leading-snug text-ink">{view.message}</p>
      {facts ? (
        <p className="type-code mt-1 text-ink-2">{facts}</p>
      ) : null}
    </div>
  );
}

/** A neutral confirmation line, e.g. "Stopped web-1". */
export function NoticeLine({ children }: { children: ReactNode }) {
  return (
    <p aria-live="polite" className="type-data text-[13px] text-ink-2">
      {children}
    </p>
  );
}
