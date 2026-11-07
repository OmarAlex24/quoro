import { useEffect, useRef } from 'react';
import {
  FIELD_CELL,
  fieldFrameDue,
  fieldGrid,
  fieldMotion,
  fieldPixels,
  fieldTones,
  nextFieldPhase,
  parseRgb,
  type Rgb,
} from '../lib/dither-field';
import { readToken, useResolvedTheme } from '../lib/use-theme';

const REDUCED_MOTION = '(prefers-reduced-motion: reduce)';

/**
 * The animated two-tone field behind sign-in and setup. One frame is computed
 * at one pixel per cell, put on a small hidden canvas and scaled up without
 * smoothing onto a backing store sized by the device pixel ratio, so every dot
 * stays crisp and a frame costs one pass over the cells.
 *
 * It drifts on requestAnimationFrame at about nine frames a second, stops
 * while the tab is hidden, and holds still under `prefers-reduced-motion`. It
 * measures itself only when its box changes (ResizeObserver), never per frame.
 * Decoration only: hidden from assistive technology.
 */
export function DitherField({ className }: { className?: string }) {
  const ref = useRef<HTMLCanvasElement | null>(null);
  const phase = useRef(0);
  const theme = useResolvedTheme();

  useEffect(() => {
    const canvas = ref.current;
    const ctx = canvas?.getContext('2d');
    const cells = document.createElement('canvas');
    const cellCtx = cells.getContext('2d');
    if (!canvas || !ctx || !cellCtx) return;

    const first: Rgb | null = parseRgb(readToken('--accent'));
    const second: Rgb | null = parseRgb(readToken('--ion'));
    const reduced = window.matchMedia(REDUCED_MOTION);
    let width = 0;
    let height = 0;
    let ratio = 1;
    let tones: Uint8Array | undefined;
    let image: ImageData | null = null;
    let frame = 0;
    let last = Number.NEGATIVE_INFINITY;

    const draw = () => {
      if (width === 0 || height === 0 || !first || !second) return;
      const { cols, rows } = fieldGrid(width, height);
      tones = fieldTones(width, height, phase.current, FIELD_CELL, tones);
      if (!image || image.width !== cols || image.height !== rows) {
        cells.width = cols;
        cells.height = rows;
        image = cellCtx.createImageData(cols, rows);
      }
      fieldPixels(tones, first, second, image.data);
      cellCtx.putImageData(image, 0, 0);
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.imageSmoothingEnabled = false;
      ctx.drawImage(cells, 0, 0, cols * FIELD_CELL * ratio, rows * FIELD_CELL * ratio);
    };

    const motion = () =>
      fieldMotion({ reducedMotion: reduced.matches, hidden: document.visibilityState === 'hidden' });

    const tick = (now: number) => {
      frame = 0;
      if (motion() !== 'running') return;
      if (fieldFrameDue(now, last)) {
        last = now;
        phase.current = nextFieldPhase(phase.current);
        draw();
      }
      frame = requestAnimationFrame(tick);
    };

    const update = () => {
      if (motion() === 'running') {
        if (frame === 0) frame = requestAnimationFrame(tick);
      } else if (frame !== 0) {
        cancelAnimationFrame(frame);
        frame = 0;
      }
    };

    const observer = new ResizeObserver((entries) => {
      const box = entries[entries.length - 1]?.contentRect;
      if (!box) return;
      width = box.width;
      height = box.height;
      ratio = Math.max(1, Math.round(window.devicePixelRatio || 1));
      canvas.width = Math.round(width * ratio);
      canvas.height = Math.round(height * ratio);
      draw();
    });
    observer.observe(canvas);
    document.addEventListener('visibilitychange', update);
    reduced.addEventListener('change', update);
    update();

    return () => {
      observer.disconnect();
      document.removeEventListener('visibilitychange', update);
      reduced.removeEventListener('change', update);
      if (frame !== 0) cancelAnimationFrame(frame);
    };
  }, [theme]);

  return <canvas ref={ref} aria-hidden="true" className={className} />;
}
