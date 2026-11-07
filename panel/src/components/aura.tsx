import { useCallback } from 'react';
import type { AuraTone } from '../lib/canvas-aura';
import { drawAura, type DitherSurface, type TokenReader } from '../lib/dither-draw';
import { cn } from '../lib/utils';
import { DitherCanvas } from './dither-canvas';

/**
 * What needs a person radiates: a dithered glow just outside the box it sits
 * in, amber while something deploys or waits on someone, red when it failed.
 * Place it inside a positioned box the size of the card; it reaches `margin`
 * px past every edge and sits behind the card. It breathes only when told to
 * (one per screen), and never under `prefers-reduced-motion`.
 */
export function Aura({
  tone,
  breathe = false,
  margin = 40,
  className,
}: {
  tone: AuraTone;
  breathe?: boolean;
  margin?: number;
  className?: string;
}) {
  const token = tone === 'fault' ? '--fault' : '--shift';
  const paint = useCallback(
    (surface: DitherSurface, read: TokenReader) => drawAura(surface, margin, token, read),
    [margin, token],
  );
  return (
    <span
      aria-hidden="true"
      className={cn('aura', breathe && 'aura-breathe', className)}
      style={{ inset: -margin }}
    >
      <DitherCanvas paint={paint} />
    </span>
  );
}
