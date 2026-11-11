import {
  AVATAR_GRID,
  avatarCellAt,
  avatarCells,
  avatarPair,
  clamp,
  isLit,
  meterDensity,
  meterEdge,
  meterMarkX,
  meterMix,
  noise,
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

/** The face the wordmark is set in: Archivo at its widest and heaviest. */
export const WORDMARK_FONT = '"Archivo Variable", Archivo, ui-sans-serif, system-ui, sans-serif';

/**
 * The wordmark: "quoro" set in Archivo, widened and heavy, then dithered in
 * the two tones, accent at the left turning to ion at the right. The letters
 * are drawn to a hidden canvas first and sampled cell by cell. Draw it once
 * the face has loaded, or it dithers the fallback font.
 */
export function drawWordmark({ ctx, w, h }: DitherSurface, read: TokenReader = readToken): void {
  const width = Math.ceil(w);
  const height = Math.ceil(h);
  const scratch = document.createElement('canvas');
  scratch.width = width;
  scratch.height = height;
  const letters = scratch.getContext('2d', { willReadFrequently: true });
  if (!letters) return;
  let size = height;
  const setFont = () => {
    letters.font = `800 ${size}px ${WORDMARK_FONT}`;
    letters.fontStretch = 'expanded';
  };
  setFont();
  let metrics = letters.measureText('quoro');
  const fitHeight = metrics.actualBoundingBoxAscent + metrics.actualBoundingBoxDescent || size * 0.75;
  size = (size * height * 0.86) / fitHeight;
  setFont();
  metrics = letters.measureText('quoro');
  if (metrics.width > width) {
    size *= (width / metrics.width) * 0.99;
    setFont();
    metrics = letters.measureText('quoro');
  }
  const ascent = metrics.actualBoundingBoxAscent;
  const descent = metrics.actualBoundingBoxDescent;
  letters.fillStyle = '#000';
  letters.fillText('quoro', 0, (height - (ascent + descent)) / 2 + ascent);
  const alpha = letters.getImageData(0, 0, width, height).data;
  const ink = (x: number, y: number) =>
    alpha[(Math.min(height - 1, y | 0) * width + Math.min(width - 1, x | 0)) * 4 + 3]! / 255;
  const grain = noise('mark-a', 64);
  const tint = noise('mark-c', 18);
  dither2(
    ctx,
    w,
    h,
    2,
    read('--accent'),
    read('--ion'),
    (x, y) => (ink(x, y) > 0.45 ? 0.5 + 0.5 * grain(x / w) : 0),
    (x, y) =>
      clamp(0.08 + 0.72 * Math.pow(x / w, 1.15) + 0.3 * (tint(x / w) - 0.5) - 0.18 * (y / h - 0.5)),
  );
}

/** A left-to-right ramp in the two dither tones: the kit's own specimen. */
export function drawRamp({ ctx, w, h }: DitherSurface, read: TokenReader = readToken): void {
  dither2(ctx, w, h, 2, read('--accent'), read('--ion'), (x) => x / w, (x) => clamp((x / w - 0.3) * 1.1));
}

/** The tone of a project's preview: the two dither tones, or amber or red when the project needs a look. */
export type PreviewTone = 'accent' | 'shift' | 'fault';

/**
 * A project's field: a coarse two-tone glow behind its service tiles, wider
 * the more services it has, accent at its heart and ion at its edge. An open
 * incident draws it in amber and a failed build in red, one colour each.
 */
export function drawPreview(
  { ctx, w, h }: DitherSurface,
  services: number,
  read: TokenReader = readToken,
  tone: PreviewTone = 'accent',
): void {
  const rx = Math.min(w * 0.46, 46 + Math.max(1, services) * 26);
  const ry = h * 0.56;
  const distance = (x: number, y: number) => Math.hypot((x - w / 2) / rx, (y - h / 2) / ry);
  const state = tone === 'accent' ? null : read(tone === 'fault' ? '--fault' : '--shift');
  dither2(
    ctx,
    w,
    h,
    4,
    state ?? read('--ion'),
    state ?? read('--accent'),
    (x, y) => {
      const d = distance(x, y);
      return d < 1 ? 0.5 * Math.pow(1 - d, 1.2) : 0;
    },
    (x, y) => clamp(1.2 - 1.6 * distance(x, y)),
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
 * is an even amber checker; a broken one is solid fault red.
 */
export function drawTapeSegment(
  { ctx, w, h }: DitherSurface,
  tone: TapeTone,
  from: number,
  to: number,
  read: TokenReader = readToken,
): void {
  if (tone === 'live') {
    // An even checker, as the proposal draws the step under way: no ramp, so
    // its end never reads as a finished, solid bar.
    dither(ctx, w, h, 2, read('--shift'), () => 0.5);
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

/** How a meter is drawn: in the two dither tones, or in one state colour. */
export type MeterTone = 'accent' | 'shift' | 'fault';

/**
 * A meter: a dithered bar filled to `share` (0..1) over a faint dotted track,
 * its leading edge thinning out so the reading point shows. The fill runs from
 * the accent towards ion as it grows; a `fault` meter, one that is nearly
 * full, is drawn fill and track in the fault colour, so it reads without
 * reading the number. An optional `mark` (0..1) draws a thin rule at what is
 * in use right now, against the share reserved.
 */
export function drawMeter(
  { ctx, w, h }: DitherSurface,
  share: number,
  read: TokenReader = readToken,
  tone: MeterTone = 'accent',
  mark: number | null = null,
): void {
  const edge = meterEdge(share, w);
  const density = (x: number) => meterDensity(x, edge);
  if (tone !== 'accent') {
    // A state meter (amber while it waits on a person, red when nearly full) is one colour.
    dither(ctx, w, h, 2, read(tone === 'fault' ? '--fault' : '--shift'), density);
  } else {
    dither2(ctx, w, h, 2, read('--accent'), read('--ion'), density, (x) => meterMix(x, edge));
  }
  // The live-use mark: a 2 px rule in ink with a 1.5 px gap cleared on each side.
  const x = meterMarkX(mark, w);
  if (x === null) return;
  ctx.clearRect(x - 1.5, 0, 5, h);
  ctx.fillStyle = read('--ink');
  ctx.fillRect(x, 0, 2, h);
}

/**
 * A sparkline of counts: one dithered bar per bucket over a hairline base,
 * scaled to the busiest bucket. Each bar is solid, accent at its foot and ion
 * towards its top; an empty bucket draws nothing above the base.
 */
export function drawSpark({ ctx, w, h }: DitherSurface, values: readonly number[], read: TokenReader = readToken): void {
  const count = values.length;
  if (count === 0) return;
  const most = Math.max(1, ...values);
  const width = w / count;
  const base = h - 2;
  ctx.fillStyle = read('--line-2');
  ctx.fillRect(0, h - 1, w, 1);
  const bucket = (x: number) => Math.min(count - 1, Math.floor(x / width));
  const top = (k: number) => {
    const value = values[k] ?? 0;
    return value > 0 ? base - Math.max(2, (base * value) / most) : base;
  };
  dither2(
    ctx,
    w,
    base,
    2,
    read('--accent'),
    read('--ion'),
    (x, y) => {
      const k = bucket(x);
      if (!values[k] || x - k * width > width - 2) return 0;
      return y >= top(k) ? 1 : 0;
    },
    (x, y) => clamp(1 - (y - top(bucket(x))) / 10),
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
/**
 * How much of the chart is drawn: the full chart has its grid, labels and the
 * newest dot; a bare one (the cluster band, a spark in a row) is the area and
 * the line alone, with or without the dot.
 */
export interface MetricAreaLook {
  labels?: boolean;
  dot?: boolean;
  lineWidth?: number;
  /** The first of the two tones: the accent, or the run green for memory in use on a node card. */
  tone?: 'accent' | 'run';
}

export function drawMetricArea(
  { ctx, w, h }: DitherSurface,
  plot: AreaPlot,
  read: TokenReader = readToken,
  look: MetricAreaLook = {},
): void {
  const { box } = plot;
  const { labels = true, dot = true, lineWidth = 2, tone = 'accent' } = look;
  const accent = read(tone === 'run' ? '--run' : '--accent');
  const ion = read('--ion');
  const faint = read('--ink-3');

  if (labels) {
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
  }

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

  if (plot.limit && !labels) {
    // A chart without labels (the service sheet's) marks the limit with a
    // dashed rule alone; its figure is in the heading above.
    ctx.fillStyle = faint;
    const y = Math.round(plot.limit.y);
    for (let x = box.left; x < box.right; x += 6) ctx.fillRect(x, y, Math.min(3, box.right - x), 1);
  } else if (plot.limit) {
    ctx.fillStyle = faint;
    ctx.fillRect(box.left, Math.round(plot.limit.y), width, 1);
    ctx.textAlign = 'right';
    ctx.textBaseline = plot.limit.below ? 'top' : 'bottom';
    ctx.fillText(plot.limit.label, box.right, plot.limit.below ? plot.limit.y + 4 : plot.limit.y - 3);
    ctx.textBaseline = 'middle';
  }

  const gradient = ctx.createLinearGradient(box.left, 0, box.right, 0);
  gradient.addColorStop(0, accent);
  gradient.addColorStop(0.55, accent);
  gradient.addColorStop(1, ion);
  ctx.strokeStyle = gradient;
  ctx.fillStyle = gradient;
  ctx.lineWidth = lineWidth;
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

  if (dot && plot.newest) {
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
