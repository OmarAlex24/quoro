// SPDX-License-Identifier: AGPL-3.0-only
import { useCallback } from 'react';
import { drawMeter, type DitherSurface, type MeterTone, type TokenReader } from '../lib/dither-draw';
import { useTween } from '../lib/use-tween';
import { DitherCanvas } from './dither-canvas';

/**
 * A dithered meter with its heading: what it measures on the left, the
 * reading on the right, and the bar filled to `share`. Accent for a grant's
 * time, amber for a request that waits on a person. A new share eases in
 * (at once under reduced motion); `mark`, when given, is a thin rule at what
 * is in use right now.
 */
export function DitherMeter({
  label,
  words,
  share,
  tone = 'accent',
  mark = null,
  spoken,
}: {
  label: string;
  words: string;
  share: number;
  tone?: MeterTone;
  /** The share in use right now (0..1), drawn as a rule over the bar. */
  mark?: number | null;
  /** What a screen reader hears for the bar; the label and words by default. */
  spoken?: string;
}) {
  const shown = useTween(share);
  const paint = useCallback(
    (surface: DitherSurface, read: TokenReader) => drawMeter(surface, shown, read, tone, mark),
    [shown, tone, mark],
  );
  return (
    <div className="meter">
      <div className="meter-h">
        <b>{label}</b>
        <span>{words}</span>
      </div>
      <DitherCanvas paint={paint} label={spoken ?? `${label}: ${words}`} />
    </div>
  );
}
