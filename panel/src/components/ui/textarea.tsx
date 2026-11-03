import * as React from 'react';
import { cn } from '../../lib/utils';

/** A multi-line `field`: same edge and focus ring as the single-line input. */
export function Textarea({ className, rows = 3, ...props }: React.ComponentProps<'textarea'>) {
  return <textarea rows={rows} className={cn('field h-auto py-2 leading-snug', className)} {...props} />;
}
