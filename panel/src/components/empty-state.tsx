import type { ReactNode } from 'react';
import { drawCube, type DitherSurface, type TokenReader } from '../lib/dither-draw';
import { cn } from '../lib/utils';
import { DitherCanvas } from './dither-canvas';

const paintCube = (surface: DitherSurface, read: TokenReader) => drawCube(surface, read);

/**
 * Nothing here yet, in a dashed box: a dithered cube, what is missing (the
 * title), what adding it does (one sentence) and at most one action. The
 * panel's one empty state, for every list or section that has nothing in it.
 * `glyph={false}` drops the cube where the box says why a page is closed
 * rather than what is missing.
 */
export function EmptyState({
  title,
  children,
  action,
  glyph = true,
  className,
}: {
  title: string;
  children?: ReactNode;
  action?: ReactNode;
  glyph?: boolean;
  className?: string;
}) {
  return (
    <div className={cn('empty-state', className)}>
      {glyph ? <DitherCanvas paint={paintCube} className="empty-cube" /> : null}
      <p className="empty-title">{title}</p>
      {children ? <p className="empty-text">{children}</p> : null}
      {action ? <div className="empty-action">{action}</div> : null}
    </div>
  );
}
