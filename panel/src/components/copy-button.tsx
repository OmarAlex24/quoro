import { Check, Copy } from 'lucide-react';
import { useState } from 'react';
import { cn } from '../lib/utils';
import { Button } from './ui/button';

const FACE = 'col-start-1 row-start-1 inline-flex items-center gap-1.5';

/**
 * Copy a value to the clipboard and say so for two seconds. Quiet (ghost) by
 * default; `default` gives it a face where copying is the thing to do, and
 * then it keeps its width while it says "Copied", so the text around it does
 * not reflow.
 */
export function CopyButton({
  value,
  label = 'Copy',
  variant = 'ghost',
  className,
}: {
  value: string;
  label?: string;
  variant?: 'ghost' | 'default';
  className?: string;
}) {
  const [copied, setCopied] = useState(false);
  return (
    <Button
      variant={variant}
      size="sm"
      className={className}
      aria-label={copied ? 'Copied' : label}
      onClick={() => {
        void navigator.clipboard?.writeText(value).then(() => {
          setCopied(true);
          window.setTimeout(() => setCopied(false), 2_000);
        });
      }}
    >
      {variant === 'default' ? (
        // Both faces share one grid cell, so the button is as wide as the wider one.
        <span className="grid justify-items-center">
          <span className={cn(FACE, copied && 'invisible')}>
            <Copy size={14} aria-hidden="true" />
            {label}
          </span>
          <span className={cn(FACE, !copied && 'invisible')}>
            <Check size={14} aria-hidden="true" />
            Copied
          </span>
        </span>
      ) : (
        <>
          {copied ? <Check size={14} aria-hidden="true" /> : <Copy size={14} aria-hidden="true" />}
          {copied ? 'Copied' : label}
        </>
      )}
    </Button>
  );
}
