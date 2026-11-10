import * as DialogPrimitive from '@radix-ui/react-dialog';
import { X } from 'lucide-react';
import type { ReactNode } from 'react';
import { cn } from '../../lib/utils';

export const Dialog = DialogPrimitive.Root;
export const DialogTrigger = DialogPrimitive.Trigger;
export const DialogClose = DialogPrimitive.Close;

export function DialogContent({
  children,
  className,
  title,
  description,
  onCloseAutoFocus,
}: {
  children: ReactNode;
  className?: string;
  title: string;
  description?: string;
  /** Where focus goes on close, for a dialog opened without a `DialogTrigger`. */
  onCloseAutoFocus?: (event: Event) => void;
}) {
  return (
    <DialogPrimitive.Portal>
      <DialogPrimitive.Overlay className="dialog-overlay fixed inset-0 z-40 bg-scrim" />
      <DialogPrimitive.Content
        className={cn(
          'dialog-content fixed left-1/2 top-1/2 z-50 flex max-h-[calc(100dvh-32px)] w-[min(92vw,560px)] -translate-x-1/2 -translate-y-1/2 flex-col overflow-y-auto border border-line-2 bg-surface p-5 sm:p-6',
          className,
        )}
        onCloseAutoFocus={onCloseAutoFocus}
      >
        <div className="mb-4 flex items-start justify-between gap-4 border-b border-line pb-3">
          <div>
            <DialogPrimitive.Title className="type-display text-[19px] leading-tight">
              {title}
            </DialogPrimitive.Title>
            {description ? (
              <DialogPrimitive.Description className="mt-1 text-[13px] text-ink-2">
                {description}
              </DialogPrimitive.Description>
            ) : null}
          </div>
          <DialogPrimitive.Close
            aria-label="Close"
            className="-mr-1 -mt-1 rounded-[7px] p-1.5 text-ink-2 transition-colors hover:bg-raised hover:text-ink focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
          >
            <X size={16} strokeWidth={2} />
          </DialogPrimitive.Close>
        </div>
        {children}
      </DialogPrimitive.Content>
    </DialogPrimitive.Portal>
  );
}
