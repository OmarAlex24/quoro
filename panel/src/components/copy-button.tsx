import { Check, Copy } from 'lucide-react';
import { useState } from 'react';
import { Button } from './ui/button';

/**
 * Copy a value to the clipboard and say so for two seconds. Quiet (ghost) by
 * default; `default` gives it a face where copying is the thing to do.
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
      {copied ? <Check size={14} aria-hidden="true" /> : <Copy size={14} aria-hidden="true" />}
      {copied ? 'Copied' : label}
    </Button>
  );
}
