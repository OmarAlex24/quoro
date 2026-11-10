// SPDX-License-Identifier: AGPL-3.0-only
import type { CSSProperties, ReactNode } from 'react';
import { lineWidths } from '../lib/skeleton';
import { cn } from '../lib/utils';

/*
 * While data loads: a dither field in the shape of what is coming, instead of
 * the word "Loading". The sweep runs only while the field is on screen, that
 * is while data loads; under reduced motion it stands still in the middle.
 * Each field is one status region with a name for screen readers; the bars
 * themselves are hidden from them.
 */

/** One bar of the field. Width and height default to a 12 px line across. */
export function SkelBar({
  width,
  height,
  className,
}: {
  width?: number | string;
  height?: number;
  className?: string;
}) {
  const style: CSSProperties = {};
  if (width !== undefined) style.width = typeof width === 'number' ? `${width}%` : width;
  if (height !== undefined) style.height = `${height}px`;
  return <span className={cn('skel', className)} style={style} />;
}

/** The status region a skeleton sits in: `label` is what screen readers hear. */
export function Skeleton({
  label,
  className,
  style,
  children,
}: {
  label: string;
  className?: string;
  style?: CSSProperties;
  children: ReactNode;
}) {
  return (
    <div role="status" className={cn('skel-field', className)} style={style}>
      <span className="sr-only">{label}</span>
      <div aria-hidden="true" className="contents">
        {children}
      </div>
    </div>
  );
}

/** A heading and a few lines of text: the proposal's skeleton. */
export function SkeletonLines({
  label,
  lines = 3,
  heading = true,
  className,
}: {
  label: string;
  lines?: number;
  heading?: boolean;
  className?: string;
}) {
  return (
    <Skeleton label={label} className={className}>
      {heading ? <SkelBar width={46} height={16} /> : null}
      {lineWidths(lines).map((width, index) => (
        <SkelBar key={index} width={width} />
      ))}
    </Skeleton>
  );
}

/** A list in a box: each row a glyph and two lines, the rows split by 1 px rules. */
export function SkeletonRows({
  label,
  rows = 3,
  glyph = true,
  className,
}: {
  label: string;
  rows?: number;
  glyph?: boolean;
  className?: string;
}) {
  const widths = lineWidths(rows * 2);
  return (
    <Skeleton label={label} className={cn('box skel-rows', className)}>
      {Array.from({ length: rows }, (_, index) => (
        <div key={index} className="skel-row">
          {glyph ? <SkelBar className="skel-glyph" /> : null}
          <div className="skel-row-t">
            <SkelBar width={(widths[index * 2] ?? 80) / 2} />
            <SkelBar width={widths[index * 2 + 1]} height={10} />
          </div>
        </div>
      ))}
    </Skeleton>
  );
}

/** Cards on their way: each as tall as the card it stands in for, a title and lines in it. */
export function SkeletonCards({
  label,
  count = 1,
  height,
  lines = 3,
  className,
  cardClassName,
}: {
  label: string;
  count?: number;
  height?: number;
  lines?: number;
  className?: string;
  /** The class of the grid's cards, so the field keeps the page's layout. */
  cardClassName?: string;
}) {
  return (
    <Skeleton label={label} className={className}>
      {Array.from({ length: count }, (_, index) => (
        <div
          key={index}
          className={cn('box skel-card', cardClassName)}
          style={height !== undefined ? { minHeight: `${height}px` } : undefined}
        >
          <SkelBar width={40} height={14} />
          {lineWidths(lines, index).map((width, line) => (
            <SkelBar key={line} width={width} />
          ))}
        </div>
      ))}
    </Skeleton>
  );
}

/** A whole page while the session is read: a title, a line under it and a box of rows. */
export function PageSkeleton({ label = 'Loading' }: { label?: string }) {
  return (
    <Skeleton label={label} className="skel-page">
      <SkelBar width="min(180px, 60%)" height={20} />
      <SkelBar width={42} />
      <div className="box skel-rows">
        {lineWidths(3).map((width, index) => (
          <div key={index} className="skel-row">
            <SkelBar className="skel-glyph" />
            <div className="skel-row-t">
              <SkelBar width={width / 2} />
              <SkelBar width={width} height={10} />
            </div>
          </div>
        ))}
      </div>
    </Skeleton>
  );
}
