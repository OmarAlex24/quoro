import { useCallback, useLayoutEffect, useRef } from 'react';
import { prepCanvas, type DitherSurface, type TokenReader } from '../lib/dither-draw';
import { readToken, useResolvedTheme } from '../lib/use-theme';

export type DitherPainter = (surface: DitherSurface, read: TokenReader) => void;

/**
 * A canvas painted by the dither kit. It paints at a device pixel ratio of at
 * least 2, repaints when its box changes size and when the theme changes, and
 * reads every colour from the tokens at paint time. Keep `paint` stable
 * (module-level or `useCallback`) so it does not repaint on every render.
 */
export function DitherCanvas({
  paint,
  label,
  className,
}: {
  paint: DitherPainter;
  /** Spoken name; without one the canvas is decoration and hidden from assistive tech. */
  label?: string;
  className?: string;
}) {
  const ref = useRef<HTMLCanvasElement | null>(null);
  const painter = useRef(paint);
  const theme = useResolvedTheme();

  const draw = useCallback(() => {
    const canvas = ref.current;
    if (!canvas) return;
    const surface = prepCanvas(canvas);
    if (surface) painter.current(surface, readToken);
  }, []);

  // A new painter (a new value, or each frame of a tween) is one draw, nothing more.
  useLayoutEffect(() => {
    painter.current = paint;
    draw();
  }, [paint, draw]);

  // A canvas with no box yet (first paint, hidden tab) draws once it gets one;
  // a theme change redraws with the new tokens.
  useLayoutEffect(() => {
    const canvas = ref.current;
    if (!canvas) return;
    const observer = new ResizeObserver(draw);
    observer.observe(canvas);
    return () => observer.disconnect();
  }, [theme, draw]);

  return label ? (
    <canvas ref={ref} role="img" aria-label={label} className={className} />
  ) : (
    <canvas ref={ref} aria-hidden="true" className={className} />
  );
}
