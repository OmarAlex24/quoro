import type { ReactNode } from 'react';
import { cn } from '../../lib/utils';
import { Label } from './label';

export interface FieldProps {
  label: string;
  htmlFor: string;
  hint?: ReactNode;
  children: ReactNode;
  className?: string;
}

/** One labelled control with an optional muted hint underneath. */
export function Field({ label, htmlFor, hint, children, className }: FieldProps) {
  return (
    <div className={cn('flex flex-col gap-1.5', className)}>
      <Label htmlFor={htmlFor}>{label}</Label>
      {children}
      {hint ? (
        <p className="text-[13px] leading-snug text-ink-2">{hint}</p>
      ) : null}
    </div>
  );
}
