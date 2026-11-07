import { useLayoutEffect, useRef } from 'react';
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
  const theme = useResolvedTheme();

  useLayoutEffect(() => {
    const canvas = ref.current;
    if (!canvas) return;
    const draw = () => {
      const surface = prepCanvas(canvas);
      if (surface) paint(surface, readToken);
    };
    draw();
    // A canvas with no box yet (first paint, hidden tab) draws once it gets one.
    const observer = new ResizeObserver(draw);
    observer.observe(canvas);
    return () => observer.disconnect();
  }, [paint, theme]);

  return label ? (
    <canvas ref={ref} role="img" aria-label={label} className={className} />
  ) : (
    <canvas ref={ref} aria-hidden="true" className={className} />
  );
}
