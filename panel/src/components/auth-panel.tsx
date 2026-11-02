import type { ReactNode } from 'react';
import { cn } from '../lib/utils';

/** The centred plate used by setup and login: nameplate, one line, one panel. */
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
        <h1 className="type-display nameplate text-center text-[36px] leading-none text-ink">
          Quoro
        </h1>
        <p className="mt-3 text-center text-[15px] leading-snug text-ink-muted">
          One microVM per app, on your own metal.
        </p>
        <div
          className={cn(
            'mt-6 rounded-[6px] border border-rail bg-panel p-5 bay-shadow sm:p-6',
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
