/**
 * The pure half of the dither kit: the ordered Bayer thresholds, the seeded
 * noise and the agent avatar pattern. Nothing here touches the DOM; the canvas
 * half lives in `dither-draw.ts`.
 *
 * Every dithered surface in the panel is two-tone: a density picks which cells
 * of the 8×8 grid light up, and a mix picks how many of those lit cells take
 * the second colour. Each dot is one colour or the other, never a blend.
 */

/** The 8×8 ordered (Bayer) threshold matrix: every value 0..63 once. */
export const BAYER: readonly (readonly number[])[] = [
  [0, 32, 8, 40, 2, 34, 10, 42],
  [48, 16, 56, 24, 50, 18, 58, 26],
  [12, 44, 4, 36, 14, 46, 6, 38],
  [60, 28, 52, 20, 62, 30, 54, 22],
  [3, 35, 11, 43, 1, 33, 9, 41],
  [51, 19, 59, 27, 49, 17, 57, 25],
  [15, 47, 7, 39, 13, 45, 5, 37],
  [63, 31, 55, 23, 61, 29, 53, 21],
];

/** Reverses the three bits of a 0..7 index. */
export const rev3 = (x: number): number => ((x & 1) << 2) | (x & 2) | ((x >> 2) & 1);

/**
 * The second threshold, for picking the colour of a lit cell: `BAYER` with its
 * row and column bits reversed. Over the cells `BAYER` lights at any density
 * these thresholds stay spread out, so a 30 % mix is close to 30 % of the lit
 * cells. Reusing `BAYER` itself would light the second tone on exactly the
 * cells the density lit first, so the mix would collapse to all or nothing.
 */
export const BAYER_2: readonly (readonly number[])[] = BAYER.map((row, j) =>
  row.map((_, i) => BAYER[rev3(j)]![rev3(i)]!),
);

const at = (matrix: readonly (readonly number[])[], i: number, j: number): number =>
  matrix[j & 7]![i & 7]!;

/** Is cell (i, j) lit at density `v` (0..1)? */
export const isLit = (v: number, i: number, j: number): boolean =>
  v > 0 && v * 64 > at(BAYER, i, j) + 0.5;

/** Does lit cell (i, j) take the second tone at mix `m` (0..1)? */
export const takesSecondTone = (m: number, i: number, j: number): boolean =>
  m * 64 > at(BAYER_2, i, j) + 0.5;

export const clamp = (v: number, lo = 0, hi = 1): number => Math.max(lo, Math.min(hi, v));

/*
 * A meter's fill, as pure numbers so it can be tested without a canvas: the
 * bar is lit solid up to the reading point, thins out over the last 10 px so
 * the point shows, and leaves a faint dotted track past it. The second tone
 * gathers towards the reading point.
 */

/** Where the reading point of a meter `width` px wide filled to `share` sits, in px. */
export const meterEdge = (share: number, width: number): number =>
  Number.isFinite(share) ? clamp(share) * width : 0;

/** The density at `x` of a meter whose reading point is at `edge`. */
export const meterDensity = (x: number, edge: number): number => {
  const fade = Math.min(10, edge);
  if (x >= edge) return 0.14;
  if (x > edge - fade) return 0.45 + (0.55 * (edge - x)) / fade;
  return 1;
};

/** The share of lit cells in the second tone at `x`: none on the track. */
export const meterMix = (x: number, edge: number): number =>
  x >= edge ? 0 : 0.78 * Math.pow(x / Math.max(1, edge), 1.8);

/**
 * A seeded generator (mulberry32 over a string hash), so a seed draws the same
 * picture on every render, in every browser.
 */
export function rng(seed: string): () => number {
  let a = 0;
  for (const ch of seed) a = (a * 31 + ch.charCodeAt(0)) | 0;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Smooth seeded 1-D noise over 0..1, from `n` control points. */
export function noise(seed: string, n = 40): (x: number) => number {
  const next = rng(seed);
  const points = Array.from({ length: n }, () => next());
  return (x) => {
    const p = clamp(x, 0, 0.9999) * (n - 1);
    const i = Math.floor(p);
    const t = p - i;
    const s = t * t * (3 - 2 * t);
    return points[i]! * (1 - s) + points[Math.min(n - 1, i + 1)]! * s;
  };
}

/** The colour pairs an agent avatar can take, as token names. */
export const AVATAR_PAIRS = [
  ['--accent', '--ion'],
  ['--violet', '--k-vm'],
  ['--k-image', '--accent'],
] as const;

export type AvatarPair = (typeof AVATAR_PAIRS)[number];

/** Which pair a seed draws in: a string hash that starts at 5. */
export const avatarPairIndex = (seed: string): number => {
  let hash = 5;
  for (const ch of seed) hash = (hash * 31 + ch.charCodeAt(0)) | 0;
  return Math.abs(hash) % AVATAR_PAIRS.length;
};

export const avatarPair = (seed: string): AvatarPair => AVATAR_PAIRS[avatarPairIndex(seed)]!;

/** The avatar grid is 5×5 and mirrored, so only three columns are drawn from the seed. */
export const AVATAR_GRID = 5;

export type AvatarCell = 0 | 0.5 | 1;

/**
 * The left three columns of a 5×5 avatar, row by row: empty, half (a checker
 * in the second tone) or full (the first tone). At least one cell is full.
 */
export function avatarCells(seed: string): AvatarCell[] {
  const next = rng(seed);
  const cells: AvatarCell[] = [];
  for (let k = 0; k < AVATAR_GRID * 3; k += 1) {
    const q = next();
    cells.push(q < 0.38 ? 0 : q < 0.66 ? 0.5 : 1);
  }
  if (!cells.includes(1)) cells[7] = 1;
  return cells;
}

/** The cell value at grid column `col` (0..4) and row `row`, mirroring the right half. */
export const avatarCellAt = (cells: readonly AvatarCell[], col: number, row: number): AvatarCell => {
  const mirrored = col > 2 ? AVATAR_GRID - 1 - col : col;
  return cells[row * 3 + mirrored] ?? 0;
};
