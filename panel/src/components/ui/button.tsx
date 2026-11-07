import * as React from 'react';
import { Slot } from '@radix-ui/react-slot';
import { cva, type VariantProps } from 'class-variance-authority';
import { cn } from '../../lib/utils';

/**
 * Five variants, two heights (32 and 28 px), a 7 px radius. Never a pill.
 * `primary` is the one action a screen is for; `danger-solid` confirms a
 * destructive action already asked about; `danger` offers one.
 */
const buttonVariants = cva(
  'inline-flex shrink-0 select-none items-center justify-center gap-1.5 whitespace-nowrap rounded-[7px] border font-[540] transition-colors focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent disabled:pointer-events-none disabled:opacity-45',
  {
    variants: {
      variant: {
        primary:
          'border-transparent bg-accent-solid text-on-accent hover:bg-[color-mix(in_oklab,var(--accent-solid)_86%,var(--ink))]',
        default: 'border-line-2 bg-surface text-ink hover:border-ink-3',
        ghost: 'border-transparent bg-transparent text-ink-2 hover:bg-raised hover:text-ink',
        danger:
          'border-[color-mix(in_oklab,var(--fault)_45%,var(--line-2))] bg-surface text-fault hover:border-fault',
        'danger-solid':
          'border-transparent bg-fault text-on-accent hover:bg-[color-mix(in_oklab,var(--fault)_86%,var(--ink))]',
      },
      size: {
        default: 'h-[32px] px-3 text-[13px]',
        sm: 'h-[28px] px-[9px] text-[12.5px]',
        icon: 'h-[32px] w-[32px] p-0',
        'icon-sm': 'h-[28px] w-[28px] p-0',
      },
    },
    defaultVariants: {
      variant: 'default',
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
