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
}: {
  children: ReactNode;
  className?: string;
  title: string;
  description?: string;
}) {
  return (
    <DialogPrimitive.Portal>
      <DialogPrimitive.Overlay className="dialog-overlay fixed inset-0 z-40 bg-ink/40" />
      <DialogPrimitive.Content
        className={cn(
          'dialog-content fixed left-1/2 top-1/2 z-50 flex max-h-[calc(100dvh-32px)] w-[min(92vw,560px)] -translate-x-1/2 -translate-y-1/2 flex-col overflow-y-auto rounded-[6px] border border-rail bg-panel p-5 shadow-[0_24px_60px_-24px_rgb(20_32_48_/_0.55)] sm:p-6',
          className,
        )}
      >
        <div className="mb-4 flex items-start justify-between gap-4 border-b border-rail pb-3">
          <div>
            <DialogPrimitive.Title className="type-display text-[18px] leading-tight">
              {title}
            </DialogPrimitive.Title>
            {description ? (
              <DialogPrimitive.Description className="mt-1 text-[13px] text-ink-muted">
                {description}
              </DialogPrimitive.Description>
            ) : null}
          </div>
          <DialogPrimitive.Close
            aria-label="Close"
            className="-mr-1 -mt-1 rounded-[6px] p-1.5 text-ink-muted transition-colors hover:bg-recess hover:text-ink focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-cobalt"
          >
            <X size={16} strokeWidth={2} />
          </DialogPrimitive.Close>
        </div>
        {children}
      </DialogPrimitive.Content>
    </DialogPrimitive.Portal>
  );
}
