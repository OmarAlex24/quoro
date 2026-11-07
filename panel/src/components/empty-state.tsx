import type { ReactNode } from 'react';
import { drawCube, type DitherSurface, type TokenReader } from '../lib/dither-draw';
import { cn } from '../lib/utils';
import { DitherCanvas } from './dither-canvas';

const paintCube = (surface: DitherSurface, read: TokenReader) => drawCube(surface, read);

/** Nothing here yet: a dithered cube, one phrase, a line under it and at most one action. */
export function EmptyState({
  title,
  children,
  action,
  className,
}: {
  title: string;
  children?: ReactNode;
  action?: ReactNode;
  className?: string;
}) {
  return (
    <div className={cn('empty-state', className)}>
      <DitherCanvas paint={paintCube} className="empty-cube" />
      <p className="empty-title">{title}</p>
      {children ? <p className="empty-text">{children}</p> : null}
      {action ? <div className="mt-3">{action}</div> : null}
    </div>
  );
}
