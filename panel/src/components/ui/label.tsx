import * as React from 'react';
import { cn } from '../../lib/utils';

export function Label({ className, ...props }: React.ComponentProps<'label'>) {
  return (
    <label
      className={cn('type-body-medium text-[13px] text-ink-2', className)}
      {...props}
    />
  );
}
