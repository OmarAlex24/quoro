import * as SelectPrimitive from '@radix-ui/react-select';
import { Check, ChevronDown } from 'lucide-react';
import type { ComponentProps } from 'react';
import { cn } from '../../lib/utils';

export const Select = SelectPrimitive.Root;
export const SelectValue = SelectPrimitive.Value;

export function SelectTrigger({
  className,
  children,
  ...props
}: ComponentProps<typeof SelectPrimitive.Trigger>) {
  return (
    <SelectPrimitive.Trigger
      className={cn(
        'field flex items-center justify-between gap-2 text-left data-[placeholder]:text-ink-3 focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-accent',
        className,
      )}
      {...props}
    >
      {children}
      <SelectPrimitive.Icon asChild>
        <ChevronDown size={15} className="shrink-0 text-ink-2" />
      </SelectPrimitive.Icon>
    </SelectPrimitive.Trigger>
  );
}

export function SelectContent({
  children,
  className,
  ...props
}: ComponentProps<typeof SelectPrimitive.Content>) {
  return (
    <SelectPrimitive.Portal>
      <SelectPrimitive.Content
        position="popper"
        sideOffset={4}
        className={cn(
          'z-[60] max-h-72 min-w-[var(--radix-select-trigger-width)] overflow-hidden rounded-[10px] border border-line-2 bg-raised p-1 shadow-[var(--shadow)]',
          className,
        )}
        {...props}
      >
        <SelectPrimitive.Viewport>{children}</SelectPrimitive.Viewport>
      </SelectPrimitive.Content>
    </SelectPrimitive.Portal>
  );
}

export function SelectItem({
  className,
  children,
  ...props
}: ComponentProps<typeof SelectPrimitive.Item>) {
  return (
    <SelectPrimitive.Item
      className={cn(
        'relative flex cursor-default select-none items-center gap-2 rounded-[7px] py-1.5 pl-2.5 pr-8 text-[13px] outline-none data-[highlighted]:bg-surface data-[disabled]:opacity-45',
        className,
      )}
      {...props}
    >
      <SelectPrimitive.ItemText>{children}</SelectPrimitive.ItemText>
      <SelectPrimitive.ItemIndicator className="absolute right-2">
        <Check size={14} className="text-accent" />
      </SelectPrimitive.ItemIndicator>
    </SelectPrimitive.Item>
  );
}
