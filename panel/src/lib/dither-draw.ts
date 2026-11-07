import {
  AVATAR_GRID,
  avatarCellAt,
  avatarCells,
  avatarPair,
  clamp,
  isLit,
  takesSecondTone,
} from './dither';
import type { AreaPlot } from './metric-area';
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

/**
 * A project's preview: a two-tone radial glow behind its service tiles,
 * wider the more services it has.
 */
export function drawPreview({ ctx, w, h }: DitherSurface, services: number, read: TokenReader = readToken): void {
  const rx = Math.min(0.46, 0.16 + services * 0.065) * w;
  const ry = 0.5 * h;
  const distance = (x: number, y: number) => Math.hypot((x - w / 2) / rx, (y - h / 2) / ry);
  dither2(
    ctx,
    w,
    h,
    2,
    read('--accent'),
    read('--ion'),
    (x, y) => {
      const d = distance(x, y);
      return d < 1 ? 0.26 * Math.pow(1 - d, 1.7) : 0;
    },
    (x, y) => clamp(0.2 + 0.55 * (1 - distance(x, y))),
  );
}

/** The corner radius of the card an aura surrounds; `.svc` uses the same. */
export const AURA_RADIUS = 11;

/**
 * An aura: a one-tone dithered glow outside a rounded rectangle that sits
 * `margin` px inside the canvas on every side, fading out over the margin.
 */
export function drawAura(
  { ctx, w, h }: DitherSurface,
  margin: number,
  tone: string,
  read: TokenReader = readToken,
): void {
  const r = AURA_RADIUS;
  const hx = (w - 2 * margin) / 2 - r;
  const hy = (h - 2 * margin) / 2 - r;
  const outside = (x: number, y: number) => {
    const qx = Math.abs(x - w / 2) - hx;
    const qy = Math.abs(y - h / 2) - hy;
    return Math.hypot(Math.max(qx, 0), Math.max(qy, 0)) + Math.min(Math.max(qx, qy), 0) - r;
  };
  dither(ctx, w, h, 2, read(tone), (x, y) => {
    const d = outside(x, y);
    return d <= 0 || d >= margin ? 0 : 0.5 * Math.pow(1 - d / margin, 2.4);
  });
}

/** The empty-state drawing: a cube in three dithered faces. */
export function drawCube({ ctx, w, h }: DitherSurface, read: TokenReader = readToken): void {
  const s = Math.min(h * 0.46, w * 0.3);
  const cx = w / 2;
  const cy = h / 2 + s * 0.02;
  const k = 0.866;
  type Polygon = ReadonlyArray<readonly [number, number]>;
  const inside = (x: number, y: number, points: Polygon) => {
    let hit = false;
    for (let i = 0, j = points.length - 1; i < points.length; j = i, i += 1) {
      const [xi, yi] = points[i]!;
      const [xj, yj] = points[j]!;
      if (yi > y !== yj > y && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) hit = !hit;
    }
    return hit;
  };
  const top: Polygon = [
    [cx, cy - s],
    [cx + k * s, cy - s / 2],
    [cx, cy],
    [cx - k * s, cy - s / 2],
  ];
  const left: Polygon = [
    [cx - k * s, cy - s / 2],
    [cx, cy],
    [cx, cy + s],
    [cx - k * s, cy + s / 2],
  ];
  const right: Polygon = [
    [cx, cy],
    [cx + k * s, cy - s / 2],
    [cx + k * s, cy + s / 2],
    [cx, cy + s],
  ];
  const face = (x: number, y: number) =>
    inside(x, y, top) ? 1 : inside(x, y, left) ? 2 : inside(x, y, right) ? 3 : 0;
  const density = [0, 0.22, 0.55, 0.92] as const;
  const mix = [0, 0.85, 0.1, 0.3] as const;
  dither2(
    ctx,
    w,
    h,
    2,
    read('--accent'),
    read('--ion'),
    (x, y) => density[face(x, y)]!,
    (x, y) => mix[face(x, y)]!,
  );
}

/** How a tape segment or a step bar is drawn: by what happened in it. */
export type TapeTone = 'done' | 'live' | 'failed' | 'stopped';

/**
 * One phase of the deploy tape. Done phases share one accent-to-ion run
 * across the whole tape: `from` and `to` are where this segment starts and
 * ends on it (0..1), so neighbours meet in the same mix. The running phase
 * fills in amber towards now; a broken one is solid fault red.
 */
export function drawTapeSegment(
  { ctx, w, h }: DitherSurface,
  tone: TapeTone,
  from: number,
  to: number,
  read: TokenReader = readToken,
): void {
  if (tone === 'live') {
    dither(ctx, w, h, 2, read('--shift'), (x) => 0.3 + (0.7 * x) / w);
    return;
  }
  if (tone === 'failed') {
    dither(ctx, w, h, 2, read('--fault'), () => 0.92);
    return;
  }
  if (tone === 'stopped') {
    dither(ctx, w, h, 2, read('--idle'), () => 0.6);
    return;
  }
  dither2(ctx, w, h, 2, read('--accent'), read('--ion'), () => 0.92, (x) =>
    clamp(0.1 + 0.7 * (from + ((to - from) * x) / w)),
  );
}

/**
 * A BuildKit step's bar: a faint dotted track, filled to the step's share of
 * the build. The share is drawn at least 4 px wide so a short step still shows.
 */
export function drawStepBar(
  { ctx, w, h }: DitherSurface,
  share: number,
  tone: TapeTone,
  read: TokenReader = readToken,
): void {
  dither(ctx, w, h, 2, read('--line-2'), () => 0.2);
  if (share <= 0) return;
  const filled = Math.min(w, Math.max(4, share * w));
  ctx.clearRect(0, 0, filled, h);
  if (tone === 'failed') {
    dither(ctx, filled, h, 2, read('--fault'), () => 0.96);
  } else if (tone === 'live') {
    dither(ctx, filled, h, 2, read('--shift'), (x) => 0.3 + (0.7 * x) / filled);
  } else {
    dither2(ctx, filled, h, 2, read('--accent'), read('--ion'), () => 0.96, (x) =>
      clamp(0.15 + (0.6 * x) / filled),
    );
  }
}

/**
 * A meter: a dithered bar filled to `share` (0..1) over a faint dotted track,
 * its leading edge thinning out so the reading point shows. The fill runs from
 * the accent towards ion as it grows.
 */
export function drawMeter({ ctx, w, h }: DitherSurface, share: number, read: TokenReader = readToken): void {
  const edge = clamp(share) * w;
  const fade = Math.min(10, edge);
  const density = (x: number) =>
    x >= edge ? 0.14 : x > edge - fade ? 0.45 + (0.55 * (edge - x)) / fade : 1;
  dither2(ctx, w, h, 2, read('--accent'), read('--ion'), density, (x) =>
    x >= edge ? 0 : 0.78 * Math.pow(x / Math.max(1, edge), 1.8),
  );
}

const CHART_FONT = '500 11px "Archivo Variable", Archivo, ui-sans-serif, system-ui, sans-serif';

/**
 * DitherArea: a metric over the last ten minutes. A faint grid with its
 * labels, the area under the line in two-tone ordered dither (ion close to
 * the line, accent deeper down, more ion in the recent minutes), the line
 * itself running from accent to ion, the configured limit as a thin line and
 * a dot on the newest sample. Each run of samples is drawn on its own, so a
 * gap stays empty.
 */
export function drawMetricArea({ ctx, w, h }: DitherSurface, plot: AreaPlot, read: TokenReader = readToken): void {
  const { box } = plot;
  const accent = read('--accent');
  const ion = read('--ion');
  const faint = read('--ink-3');

  ctx.font = CHART_FONT;
  ctx.textBaseline = 'middle';
  ctx.textAlign = 'right';
  for (const tick of plot.ticks) {
    ctx.fillStyle = read('--line');
    ctx.fillRect(box.left, Math.round(tick.y), box.right - box.left, 1);
    ctx.fillStyle = faint;
    ctx.fillText(tick.label, box.left - 8, tick.y);
  }
  ctx.fillStyle = faint;
  ctx.textAlign = 'left';
  ctx.fillText('−10 min', box.left, h - 7);
  ctx.textAlign = 'center';
  ctx.fillText('−5 min', (box.left + box.right) / 2, h - 7);
  ctx.textAlign = 'right';
  ctx.fillText('now', box.right, h - 7);

  // The line's height at every whole pixel; NaN where no run covers it.
  const lineAt = new Float32Array(Math.ceil(w) + 2).fill(Number.NaN);
  for (const run of plot.runs) {
    if (run.length === 1) {
      const { x, y } = run[0]!;
      for (let px = Math.floor(x) - 1; px <= Math.ceil(x) + 1; px += 1) if (px >= 0) lineAt[px] = y;
      continue;
    }
    for (let k = 1; k < run.length; k += 1) {
      const a = run[k - 1]!;
      const b = run[k]!;
      const span = Math.max(1e-6, b.x - a.x);
      for (let px = Math.max(0, Math.floor(a.x)); px <= Math.ceil(b.x) && px < lineAt.length; px += 1) {
        lineAt[px] = a.y + ((b.y - a.y) * clamp((px - a.x) / span));
      }
    }
  }
  const depth = (x: number, y: number): number | null => {
    const line = lineAt[Math.floor(x)];
    if (line === undefined || Number.isNaN(line) || y < line) return null;
    return (y - line) / Math.max(1, box.bottom - line);
  };
  const width = Math.max(1, box.right - box.left);
  dither2(
    ctx,
    box.right,
    box.bottom,
    2,
    accent,
    ion,
    (x, y) => {
      if (x < box.left) return 0;
      const t = depth(x, y);
      return t === null ? 0 : 0.62 * Math.pow(1 - t, 1.5) + 0.05;
    },
    (x, y) => {
      const t = depth(x, y) ?? 1;
      return clamp((1 - t * 2.2) * (0.45 + (0.55 * (x - box.left)) / width));
    },
    box.left,
    0,
  );

  if (plot.limit) {
    ctx.fillStyle = faint;
    ctx.fillRect(box.left, Math.round(plot.limit.y), width, 1);
    ctx.textAlign = 'right';
    ctx.textBaseline = 'bottom';
    ctx.fillText(plot.limit.label, box.right, plot.limit.y - 3);
    ctx.textBaseline = 'middle';
  }

  const gradient = ctx.createLinearGradient(box.left, 0, box.right, 0);
  gradient.addColorStop(0, accent);
  gradient.addColorStop(0.55, accent);
  gradient.addColorStop(1, ion);
  ctx.strokeStyle = gradient;
  ctx.fillStyle = gradient;
  ctx.lineWidth = 2;
  ctx.lineJoin = 'round';
  ctx.lineCap = 'round';
  for (const run of plot.runs) {
    if (run.length === 1) {
      ctx.beginPath();
      ctx.arc(run[0]!.x, run[0]!.y, 1.6, 0, Math.PI * 2);
      ctx.fill();
      continue;
    }
    ctx.beginPath();
    run.forEach((point, index) => (index === 0 ? ctx.moveTo(point.x, point.y) : ctx.lineTo(point.x, point.y)));
    ctx.stroke();
  }

  if (plot.newest) {
    ctx.fillStyle = read('--surface');
    ctx.beginPath();
    ctx.arc(plot.newest.x, plot.newest.y, 6, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = ion;
    ctx.beginPath();
    ctx.arc(plot.newest.x, plot.newest.y, 4, 0, Math.PI * 2);
    ctx.fill();
  }
}
