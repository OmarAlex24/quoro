import * as React from 'react';
import { Slot } from '@radix-ui/react-slot';
import { cva, type VariantProps } from 'class-variance-authority';
import { cn } from '../../lib/utils';

/**
 * Five variants, two heights (32 and 28 px), a 7 px radius. Never a pill.
 * `primary` is the one action a screen is for; `danger-solid` confirms a
 * destructive action already asked about; `danger` offers one. The face, the
 * lit edge, the press and the busy sweep are the `.btn` classes in styles.css.
 */
const buttonVariants = cva(
  'btn inline-flex shrink-0 select-none items-center justify-center gap-1.5 whitespace-nowrap focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent',
  {
    variants: {
      variant: {
        primary: 'btn-primary',
        default: '',
        ghost: 'btn-ghost',
        danger: 'btn-danger',
        'danger-solid': 'btn-danger-solid',
      },
      size: {
        default: 'h-[32px] px-3 text-[13px]',
        sm: 'h-[28px] px-[10px] text-[12.5px]',
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
  /**
   * The action is on its way (a mutation is pending): the button keeps its
   * colour and a dither band sweeps across it. Pair it with `disabled`.
   */
  busy?: boolean;
}

export function Button({
  className,
  variant,
  size,
  asChild = false,
  busy = false,
  type,
  ...props
}: ButtonProps) {
  const Component = asChild ? Slot : 'button';
  return (
    <Component
      className={cn(buttonVariants({ variant, size }), className)}
      {...(asChild ? {} : { type: type ?? 'button' })}
      {...(busy ? { 'data-busy': '', 'aria-busy': true } : {})}
      {...props}
    />
  );
}

export { buttonVariants };
