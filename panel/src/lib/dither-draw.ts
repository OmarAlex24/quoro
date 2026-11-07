import {
  AVATAR_GRID,
  avatarCellAt,
  avatarCells,
  avatarPair,
  clamp,
  isLit,
  takesSecondTone,
} from './dither';
import { readToken } from './use-theme';

/**
 * The canvas half of the dither kit. A canvas is drawn at a device pixel ratio
 * of at least 2, with integer cells, so every dot lands on whole device pixels.
 * Colours are read from the theme tokens at draw time; callers redraw when the
 * theme changes (`DitherCanvas` does).
 */

export interface DitherSurface {
  readonly ctx: CanvasRenderingContext2D;
  /** Width in CSS pixels. */
  readonly w: number;
  /** Height in CSS pixels. */
  readonly h: number;
}

/** A density (or mix) at the centre of a cell, in CSS pixels. */
export type DensityField = (x: number, y: number) => number;


/**
 * Sizes the backing store to the element at a whole ratio of at least 2 and
 * clears it. Null while the element has no box yet (hidden, not laid out).
 */
export function prepCanvas(canvas: HTMLCanvasElement): DitherSurface | null {
  const w = canvas.offsetWidth;
  const h = canvas.offsetHeight;
  if (!w || !h) return null;
  const ratio = Math.max(2, Math.round(window.devicePixelRatio || 1));
  canvas.width = Math.round(w * ratio);
  canvas.height = Math.round(h * ratio);
  const ctx = canvas.getContext('2d');
  if (!ctx) return null;
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
  ctx.clearRect(0, 0, w, h);
  return { ctx, w, h };
}

/** One-tone ordered dither: `density` decides which cells light, in one colour. */
export function dither(
  ctx: CanvasRenderingContext2D,
  w: number,
  h: number,
  cell: number,
  color: string,
  density: DensityField,
  x0 = 0,
  y0 = 0,
): void {
  const path = new Path2D();
  for (let y = y0, j = 0; y < h; y += cell, j += 1) {
    for (let x = x0, i = 0; x < w; x += cell, i += 1) {
      if (isLit(density(x + cell / 2, y + cell / 2), i, j)) path.rect(x, y, cell, cell);
    }
  }
  ctx.fillStyle = color;
  ctx.fill(path);
}

/**
 * Two-tone ordered dither: `density` decides which cells light and `mix` the
 * share of lit cells drawn in the second colour (see `BAYER_2`).
 */
export function dither2(
  ctx: CanvasRenderingContext2D,
  w: number,
  h: number,
  cell: number,
  colorA: string,
  colorB: string,
  density: DensityField,
  mix: DensityField,
  x0 = 0,
  y0 = 0,
): void {
  const first = new Path2D();
  const second = new Path2D();
  for (let y = y0, j = 0; y < h; y += cell, j += 1) {
    for (let x = x0, i = 0; x < w; x += cell, i += 1) {
      const cx = x + cell / 2;
      const cy = y + cell / 2;
      if (!isLit(density(cx, cy), i, j)) continue;
      (takesSecondTone(mix(cx, cy), i, j) ? second : first).rect(x, y, cell, cell);
    }
  }
  ctx.fillStyle = colorA;
  ctx.fill(first);
  ctx.fillStyle = colorB;
  ctx.fill(second);
}

export type TokenReader = (name: string) => string;

/**
 * An agent's avatar: a seeded, mirrored 5×5 grid. Full cells in the first tone,
 * half cells as a checker in the second, a little of the second tone deeper down.
 */
export function drawAvatar({ ctx, w, h }: DitherSurface, seed: string, read: TokenReader = readToken): void {
  const cells = avatarCells(seed);
  const [a, b] = avatarPair(seed);
  const pad = Math.round(w * 0.18);
  const span = w - pad * 2;
  const value = (x: number, y: number): number => {
    const u = (x - pad) / span;
    const v = (y - pad) / span;
    if (u < 0 || u >= 1 || v < 0 || v >= 1) return 0;
    return avatarCellAt(cells, Math.floor(u * AVATAR_GRID), Math.floor(v * AVATAR_GRID));
  };
  dither2(ctx, w, h, 1, read(a), read(b), value, (x, y) =>
    value(x, y) === 0.5 ? 1 : 0.22 * clamp((y - pad) / span),
  );
}

/** The Quoro mark: a dithered ring, accent turning to ion around the circle. */
export function drawMark({ ctx, w, h }: DitherSurface, read: TokenReader = readToken): void {
  const cx = w / 2;
  const cy = h / 2;
  const r = Math.min(w, h) / 2;
  const cell = w > 20 ? 2 : 1;
  const angle = (x: number, y: number) =>
    ((Math.atan2(y - cy, x - cx) + Math.PI) / (2 * Math.PI) + 0.25) % 1;
  dither2(
    ctx,
    w,
    h,
    cell,
    read('--accent'),
    read('--ion'),
    (x, y) => {
      const d = Math.hypot(x - cx, y - cy) / r;
      if (d > 0.98 || d < 0.5) return 0;
      return 0.3 + 0.7 * angle(x, y);
    },
    (x, y) => clamp((angle(x, y) - 0.45) * 1.6),
  );
}

/** A left-to-right ramp in the two dither tones: the kit's own specimen. */
export function drawRamp({ ctx, w, h }: DitherSurface, read: TokenReader = readToken): void {
  dither2(ctx, w, h, 2, read('--accent'), read('--ion'), (x) => x / w, (x) => clamp((x / w - 0.3) * 1.1));
}
