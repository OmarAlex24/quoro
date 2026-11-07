// SPDX-License-Identifier: AGPL-3.0-only
import { useCallback } from 'react';
import { drawMeter, type DitherSurface, type MeterTone, type TokenReader } from '../lib/dither-draw';
import { DitherCanvas } from './dither-canvas';

/**
 * A dithered meter with its heading: what it measures on the left, the
 * reading on the right, and the bar filled to `share`. Accent for a grant's
 * time, amber for a request that waits on a person.
 */
export function DitherMeter({
  label,
  words,
  share,
  tone = 'accent',
  spoken,
}: {
  label: string;
  words: string;
  share: number;
  tone?: MeterTone;
  /** What a screen reader hears for the bar; the label and words by default. */
  spoken?: string;
}) {
  const paint = useCallback(
    (surface: DitherSurface, read: TokenReader) => drawMeter(surface, share, read, tone),
    [share, tone],
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
