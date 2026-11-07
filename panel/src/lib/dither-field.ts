import { clamp, isLit, noise, takesSecondTone } from './dither';

/*
 * The field behind sign-in and setup, as pure numbers: a two-tone dither that
 * rises from the bottom edge like a skyline and drifts sideways. Each frame is
 * a grid of cells, each empty, in the first tone or in the second; the canvas
 * half (`DitherField`) only turns that grid into pixels.
 *
 * The density is a column term times a row term, and so is the mix, so a frame
 * costs one multiply and two threshold lookups per cell: the column terms are
 * computed once per frame and the row terms once per size.
 */

/** A cell is 2 CSS px, the grain of every other dithered surface. */
export const FIELD_CELL = 2;

/** One step every 110 ms: a slow drift, about nine frames a second. */
export const FIELD_FRAME_MS = 110;

/** How far the field drifts per frame, as a share of its width. */
export const FIELD_STEP = 0.0025;

/** The phase after one more frame, wrapping at 1. */
export const nextFieldPhase = (phase: number): number => (phase + FIELD_STEP) % 1;

/** Is a new frame due at `now`, the last one having been drawn at `last` (ms)? */
export const fieldFrameDue = (now: number, last: number): boolean => now - last >= FIELD_FRAME_MS;

/**
 * What the field does: `still` under `prefers-reduced-motion` (one frame,
 * drawn once), `paused` while the tab is hidden, `running` otherwise.
 */
export type FieldMotion = 'still' | 'paused' | 'running';

export const fieldMotion = ({
  reducedMotion,
  hidden,
}: {
  reducedMotion: boolean;
  hidden: boolean;
}): FieldMotion => (reducedMotion ? 'still' : hidden ? 'paused' : 'running');

/** How many cells a `width` × `height` CSS px field holds. */
export const fieldGrid = (
  width: number,
  height: number,
  cell: number = FIELD_CELL,
): { cols: number; rows: number } => ({
  cols: Math.max(0, Math.ceil(width / cell)),
  rows: Math.max(0, Math.ceil(height / cell)),
});

let shapes: { tall: (x: number) => number; tint: (x: number) => number } | null = null;

/** The two seeded noises the field is made of, built once. */
const fieldShapes = () => {
  shapes ??= { tall: noise('login', 36), tint: noise('login-2', 12) };
  return shapes;
};

/** A cell holds nothing (0), the first tone (1) or the second tone (2). */
export type FieldTone = 0 | 1 | 2;

/**
 * One frame of the field at `phase` (0..1), row by row. `out` is reused when it
 * already has the right length, so a running field allocates nothing per frame.
 */
export function fieldTones(
  width: number,
  height: number,
  phase: number,
  cell: number = FIELD_CELL,
  out?: Uint8Array,
): Uint8Array {
  const { cols, rows } = fieldGrid(width, height, cell);
  const tones = out && out.length === cols * rows ? out : new Uint8Array(cols * rows);
  if (cols === 0 || rows === 0) return tones;
  const { tall, tint } = fieldShapes();

  const colDensity = new Float32Array(cols);
  const colMix = new Float32Array(cols);
  for (let i = 0; i < cols; i += 1) {
    const x = (i * cell + cell / 2) / width;
    const drift = tint((x + phase * 0.5) % 1);
    colDensity[i] = 0.08 + 0.62 * tall((x + phase) % 1) * (0.4 + 0.6 * drift);
    colMix[i] = 0.15 + 0.7 * drift;
  }

  for (let j = 0; j < rows; j += 1) {
    const y = (j * cell + cell / 2) / height;
    const rowDensity = Math.pow(Math.max(0, 1 - y), 1.6);
    const rowMix = 0.3 * y;
    const base = j * cols;
    for (let i = 0; i < cols; i += 1) {
      tones[base + i] = isLit(colDensity[i]! * rowDensity, i, j)
        ? takesSecondTone(clamp(colMix[i]! - rowMix), i, j)
          ? 2
          : 1
        : 0;
    }
  }
  return tones;
}

export type Rgb = readonly [number, number, number];

/**
 * A token colour as red, green and blue: `#rgb`, `#rrggbb`, `rgb(r g b)` or
 * `rgb(r, g, b)`. Null for anything else, so the caller can skip a frame
 * instead of painting black.
 */
export function parseRgb(value: string): Rgb | null {
  const text = value.trim().toLowerCase();
  const hex = /^#([0-9a-f]{3}|[0-9a-f]{6})$/.exec(text);
  if (hex) {
    const digits = hex[1]!;
    const full =
      digits.length === 3
        ? digits
            .split('')
            .map((digit) => digit + digit)
            .join('')
        : digits;
    return [
      parseInt(full.slice(0, 2), 16),
      parseInt(full.slice(2, 4), 16),
      parseInt(full.slice(4, 6), 16),
    ];
  }
  const rgb = /^rgba?\(\s*(\d{1,3})[\s,]+(\d{1,3})[\s,]+(\d{1,3})/.exec(text);
  if (rgb) {
    const channel = (part: string) => Math.min(255, Number(part));
    return [channel(rgb[1]!), channel(rgb[2]!), channel(rgb[3]!)];
  }
  return null;
}

/**
 * Writes a frame into RGBA pixels, one pixel per cell: transparent where the
 * cell is empty, opaque in the tone's colour where it is lit.
 */
export function fieldPixels(
  tones: Uint8Array,
  first: Rgb,
  second: Rgb,
  pixels: Uint8ClampedArray,
): Uint8ClampedArray {
  for (let k = 0; k < tones.length; k += 1) {
    const tone = tones[k];
    const at = k * 4;
    if (tone === 0) {
      pixels[at + 3] = 0;
      continue;
    }
    const color = tone === 1 ? first : second;
    pixels[at] = color[0];
    pixels[at + 1] = color[1];
    pixels[at + 2] = color[2];
    pixels[at + 3] = 255;
  }
  return pixels;
}
