import * as React from 'react';
import { Slot } from '@radix-ui/react-slot';
import { cva, type VariantProps } from 'class-variance-authority';
import { cn } from '../../lib/utils';

const buttonVariants = cva(
  'inline-flex h-9 shrink-0 select-none items-center justify-center gap-2 whitespace-nowrap rounded-[6px] px-3.5 text-[13px] font-medium transition-colors focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-cobalt disabled:pointer-events-none disabled:opacity-45',
  {
    variants: {
      variant: {
        primary: 'bg-cobalt text-white hover:bg-cobalt-deep',
        secondary:
          'border border-ink bg-transparent text-ink hover:bg-recess/70 active:bg-recess',
        danger:
          'border border-lamp-fault bg-transparent text-lamp-fault hover:bg-lamp-fault hover:text-white',
        quiet:
          'bg-transparent text-ink-muted hover:bg-recess/70 hover:text-ink',
      },
      size: {
        default: 'h-9 px-3.5',
        compact: 'h-9 px-2.5',
      },
    },
    defaultVariants: {
      variant: 'secondary',
      size: 'default',
    },
  },
);

export interface ButtonProps
  extends React.ComponentProps<'button'>,
    VariantProps<typeof buttonVariants> {
  asChild?: boolean;
}

export function Button({
  className,
  variant,
  size,
  asChild = false,
  type,
  ...props
}: ButtonProps) {
  const Component = asChild ? Slot : 'button';
  return (
    <Component
      className={cn(buttonVariants({ variant, size }), className)}
      {...(asChild ? {} : { type: type ?? 'button' })}
      {...props}
    />
  );
}

export { buttonVariants };
