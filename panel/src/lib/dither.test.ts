import { describe, expect, it } from 'vitest';
import {
  AVATAR_PAIRS,
  BAYER,
  BAYER_2,
  avatarCellAt,
  avatarCells,
  avatarPair,
  avatarPairIndex,
  isLit,
  noise,
  rev3,
  rng,
  takesSecondTone,
} from './dither';

const cells = (): Array<[number, number]> => {
  const all: Array<[number, number]> = [];
  for (let j = 0; j < 8; j += 1) for (let i = 0; i < 8; i += 1) all.push([i, j]);
  return all;
};

/** The cells `BAYER` lights at density k/64. */
const litAt = (k: number) => cells().filter(([i, j]) => isLit(k / 64, i, j));

/**
 * The worst gap, over every density and every mix, between how many lit cells
 * take the second tone and how many should (mix × lit), for a second threshold.
 */
const worstMixGap = (second: (m: number, i: number, j: number) => boolean): number => {
  let worst = 0;
  for (let k = 1; k <= 64; k += 1) {
    const lit = litAt(k);
    for (let p = 0; p <= 64; p += 1) {
      const got = lit.filter(([i, j]) => second(p / 64, i, j)).length;
      worst = Math.max(worst, Math.abs(got - (p / 64) * k));
    }
  }
  return worst;
};

describe('the Bayer threshold matrix', () => {
  it('holds every threshold from 0 to 63 exactly once', () => {
    expect(BAYER).toHaveLength(8);
    for (const row of BAYER) expect(row).toHaveLength(8);
    expect([...BAYER.flat()].sort((a, b) => a - b)).toEqual(Array.from({ length: 64 }, (_, k) => k));
  });

  it('lights exactly k cells of the 8×8 tile at density k/64', () => {
    for (let k = 0; k <= 64; k += 1) expect(litAt(k)).toHaveLength(k);
  });

  it('lights nothing at zero or below and everything at one', () => {
    expect(litAt(0)).toHaveLength(0);
    expect(cells().filter(([i, j]) => isLit(-0.2, i, j))).toHaveLength(0);
    expect(cells().filter(([i, j]) => isLit(1, i, j))).toHaveLength(64);
  });

  it('repeats every 8 cells in both directions', () => {
    for (const [i, j] of cells()) {
      expect(isLit(0.3, i + 8, j)).toBe(isLit(0.3, i, j));
      expect(isLit(0.3, i, j + 16)).toBe(isLit(0.3, i, j));
    }
  });

  it('spreads a 50 % density as a checker of 2×2 blocks', () => {
    const half = litAt(32);
    // Each 2×2 block of the tile holds exactly two lit cells.
    for (let by = 0; by < 8; by += 2) {
      for (let bx = 0; bx < 8; bx += 2) {
        const inBlock = half.filter(([i, j]) => i >= bx && i < bx + 2 && j >= by && j < by + 2);
        expect(inBlock).toHaveLength(2);
      }
    }
  });
});

describe('the second threshold (B2)', () => {
  it('reverses the bits of a 0..7 index', () => {
    expect([0, 1, 2, 3, 4, 5, 6, 7].map(rev3)).toEqual([0, 4, 2, 6, 1, 5, 3, 7]);
  });

  it('is the Bayer matrix with its row and column bits reversed', () => {
    for (const [i, j] of cells()) expect(BAYER_2[j]![i]).toBe(BAYER[rev3(j)]![rev3(i)]);
    expect([...BAYER_2.flat()].sort((a, b) => a - b)).toEqual(Array.from({ length: 64 }, (_, k) => k));
  });

  it('keeps the mix uniform over the cells B lights, within 2.6 cells at every density', () => {
    expect(worstMixGap(takesSecondTone)).toBeLessThanOrEqual(2.6);
  });

  it('gives the exact share once every cell is lit', () => {
    const all = litAt(64);
    for (let p = 0; p <= 64; p += 1) {
      expect(all.filter(([i, j]) => takesSecondTone(p / 64, i, j))).toHaveLength(p);
    }
  });

  it('is why B2 exists: reusing B as the second threshold misses by 16 cells', () => {
    const sameMatrix = (m: number, i: number, j: number) => isLit(m, i, j);
    // At 50 % density and a 50 % mix, B picks all 32 lit cells instead of 16.
    expect(worstMixGap(sameMatrix)).toBeGreaterThanOrEqual(16);
    const half = litAt(32);
    expect(half.filter(([i, j]) => sameMatrix(0.5, i, j))).toHaveLength(32);
    expect(half.filter(([i, j]) => takesSecondTone(0.5, i, j))).toHaveLength(16);
  });
});

describe('seeded noise', () => {
  it('repeats for the same seed and differs between seeds', () => {
    const a = rng('seed');
    const b = rng('seed');
    const c = rng('other');
    const first = [a(), a(), a()];
    expect([b(), b(), b()]).toEqual(first);
    expect([c(), c(), c()]).not.toEqual(first);
    for (const v of first) {
      expect(v).toBeGreaterThanOrEqual(0);
      expect(v).toBeLessThan(1);
    }
  });

  it('matches the reference generator', () => {
    const r = rng('seed');
    expect(r()).toBeCloseTo(0.41314317425712943, 15);
    expect(r()).toBeCloseTo(0.02951375348493457, 15);
  });

  it('is smooth and stays within 0..1', () => {
    const n = noise('bayer', 24);
    for (let x = -0.5; x <= 1.5; x += 0.01) {
      const v = n(x);
      expect(v).toBeGreaterThanOrEqual(0);
      expect(v).toBeLessThan(1);
    }
    expect(Math.abs(n(0.5) - n(0.501))).toBeLessThan(0.05);
  });
});

describe('agent avatars', () => {
  it('picks the colour pair from a hash that starts at 5', () => {
    // Expected values come from running the reference script on the same seeds.
    expect(avatarPairIndex('on-call-restarter')).toBe(1);
    expect(avatarPairIndex('weekly-reporter')).toBe(0);
    expect(avatarPairIndex('x')).toBe(2);
    expect(avatarPairIndex('a')).toBe(0);
    expect(avatarPairIndex('b')).toBe(1);
    expect(avatarPairIndex('c')).toBe(2);
    // The empty seed still lands on a pair: 5 % 3.
    expect(avatarPairIndex('')).toBe(2);
  });

  it('draws in two token colours', () => {
    expect(avatarPair('on-call-restarter')).toEqual(['--violet', '--k-vm']);
    expect(avatarPair('weekly-reporter')).toEqual(['--accent', '--ion']);
    expect(avatarPair('x')).toEqual(['--k-image', '--accent']);
    for (const pair of AVATAR_PAIRS) for (const name of pair) expect(name).toMatch(/^--/);
  });

  it('draws the same cells as the reference for a seed', () => {
    expect(avatarCells('on-call-restarter')).toEqual([0, 1, 0, 1, 0, 0, 0, 1, 0.5, 1, 1, 0.5, 1, 1, 1]);
    expect(avatarCells('x')).toEqual([1, 0, 0.5, 0, 1, 0.5, 0, 0.5, 1, 1, 0.5, 0, 0.5, 0, 0]);
  });

  it('always has at least one full cell', () => {
    for (let k = 0; k < 500; k += 1) expect(avatarCells(`agent-${k}`)).toContain(1);
  });

  it('mirrors the right half onto the left', () => {
    const grid = avatarCells('on-call-restarter');
    for (let row = 0; row < 5; row += 1) {
      expect(avatarCellAt(grid, 4, row)).toBe(avatarCellAt(grid, 0, row));
      expect(avatarCellAt(grid, 3, row)).toBe(avatarCellAt(grid, 1, row));
    }
  });
});
