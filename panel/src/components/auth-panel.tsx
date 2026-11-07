import type { ReactNode } from 'react';
import { cn } from '../lib/utils';
import { QuoroMark } from './avatar';

/** The centred plate used by setup and login: the mark, one line, one card. */
export function AuthPanel({
  children,
  footer,
  className,
}: {
  children: ReactNode;
  footer?: ReactNode;
  className?: string;
}) {
  return (
    <main className="flex min-h-dvh flex-col items-center justify-center px-4 py-10">
      <div className="w-full max-w-[400px]">
        <h1 className="flex items-center justify-center gap-3 text-ink">
          <QuoroMark className="h-9 w-9" />
          <span className="type-display text-[36px] leading-none">Quoro</span>
        </h1>
        <p className="mt-3 text-center text-[15px] leading-snug text-ink-2">
          One microVM per app, on your own metal.
        </p>
        <div
          className={cn(
            'mt-6 rounded-[14px] border border-line-2 bg-surface p-5 shadow-[var(--shadow)] card-sheen sm:p-7',
            className,
          )}
        >
          {children}
        </div>
        {footer ? <div className="mt-4 text-center">{footer}</div> : null}
      </div>
    </main>
  );
}
