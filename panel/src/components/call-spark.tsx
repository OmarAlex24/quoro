// SPDX-License-Identifier: AGPL-3.0-only
import { useCallback } from 'react';
import { drawSpark, type DitherSurface, type TokenReader } from '../lib/dither-draw';
import { DitherCanvas } from './dither-canvas';

/** An agent's calls per hour over the last day, as dithered bars, with the total. */
export function CallSpark({ buckets, total }: { buckets: readonly number[]; total: number }) {
  const paint = useCallback(
    (surface: DitherSurface, read: TokenReader) => drawSpark(surface, buckets, read),
    [buckets],
  );
  return (
    <div className="spark">
      <DitherCanvas
        paint={paint}
        label={total === 1 ? '1 call in the last 24 hours' : `${total} calls in the last 24 hours`}
      />
      <small>
        <span>Calls, last 24 h</span>
        <span>{total}</span>
      </small>
    </div>
  );
}
