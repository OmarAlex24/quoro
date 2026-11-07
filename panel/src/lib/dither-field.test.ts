import { describe, expect, it } from 'vitest';
import {
  FIELD_CELL,
  FIELD_FRAME_MS,
  FIELD_STEP,
  fieldFrameDue,
  fieldGrid,
  fieldMotion,
  fieldPixels,
  fieldTones,
  nextFieldPhase,
  parseRgb,
} from './dither-field';

describe('the field clock', () => {
  it('steps the phase a little each frame and wraps at 1', () => {
    expect(nextFieldPhase(0)).toBeCloseTo(FIELD_STEP);
    expect(nextFieldPhase(0.5)).toBeCloseTo(0.5 + FIELD_STEP);
    expect(nextFieldPhase(1 - FIELD_STEP / 2)).toBeCloseTo(FIELD_STEP / 2);
    let phase = 0;
    for (let frame = 0; frame < 1_000; frame += 1) phase = nextFieldPhase(phase);
    expect(phase).toBeGreaterThanOrEqual(0);
    expect(phase).toBeLessThan(1);
  });

  it('draws a frame only once the interval has passed', () => {
    expect(fieldFrameDue(1_000, 1_000)).toBe(false);
    expect(fieldFrameDue(1_000 + FIELD_FRAME_MS - 1, 1_000)).toBe(false);
    expect(fieldFrameDue(1_000 + FIELD_FRAME_MS, 1_000)).toBe(true);
  });

  it('holds still under reduced motion and pauses while the tab is hidden', () => {
    expect(fieldMotion({ reducedMotion: true, hidden: false })).toBe('still');
    expect(fieldMotion({ reducedMotion: true, hidden: true })).toBe('still');
    expect(fieldMotion({ reducedMotion: false, hidden: true })).toBe('paused');
    expect(fieldMotion({ reducedMotion: false, hidden: false })).toBe('running');
  });
});

describe('fieldTones', () => {
  it('has one cell per 2 px, rounding a partial cell up', () => {
    expect(FIELD_CELL).toBe(2);
    expect(fieldGrid(1440, 900)).toEqual({ cols: 720, rows: 450 });
    expect(fieldGrid(391, 845)).toEqual({ cols: 196, rows: 423 });
    expect(fieldGrid(0, 900)).toEqual({ cols: 0, rows: 450 });
    expect(fieldTones(0, 900, 0)).toHaveLength(0);
  });

  it('draws the same frame for the same phase, and a different one as it drifts', () => {
    const first = fieldTones(240, 160, 0.3);
    expect(fieldTones(240, 160, 0.3)).toEqual(first);
    expect(fieldTones(240, 160, 0.3 + FIELD_STEP * 40)).not.toEqual(first);
  });

  it('only holds empty cells, the first tone and the second tone', () => {
    const tones = fieldTones(240, 160, 0.1);
    const seen = new Set(tones);
    expect([...seen].every((tone) => tone === 0 || tone === 1 || tone === 2)).toBe(true);
    expect(seen.has(1)).toBe(true);
    expect(seen.has(2)).toBe(true);
  });

  it('rises from the bottom: the top rows are busier than the bottom ones', () => {
    const width = 400;
    const height = 300;
    const tones = fieldTones(width, height, 0);
    const { cols, rows } = fieldGrid(width, height);
    const lit = (from: number, to: number) => {
      let count = 0;
      for (let j = from; j < to; j += 1) {
        for (let i = 0; i < cols; i += 1) if (tones[j * cols + i] !== 0) count += 1;
      }
      return count;
    };
    const quarter = Math.floor(rows / 4);
    expect(lit(0, quarter)).toBeGreaterThan(lit(rows - quarter, rows) * 3);
    // The last row sits at the bottom edge: nothing is lit there.
    expect(lit(rows - 1, rows)).toBe(0);
  });

  it('reuses the buffer it is given when the size matches', () => {
    const buffer = new Uint8Array(120 * 80);
    const tones = fieldTones(240, 160, 0.2, FIELD_CELL, buffer);
    expect(tones).toBe(buffer);
    const other = fieldTones(240, 160, 0.2, FIELD_CELL, new Uint8Array(3));
    expect(other).toHaveLength(120 * 80);
  });
});

describe('parseRgb', () => {
  it('reads the token forms the theme uses', () => {
    expect(parseRgb('#8196ff')).toEqual([0x81, 0x96, 0xff]);
    expect(parseRgb(' #5ED0F2 ')).toEqual([0x5e, 0xd0, 0xf2]);
    expect(parseRgb('#abc')).toEqual([0xaa, 0xbb, 0xcc]);
    expect(parseRgb('rgb(6 8 13 / 0.55)')).toEqual([6, 8, 13]);
    expect(parseRgb('rgb(38, 67, 181)')).toEqual([38, 67, 181]);
  });

  it('refuses what it cannot read', () => {
    expect(parseRgb('')).toBeNull();
    expect(parseRgb('cobalt')).toBeNull();
    expect(parseRgb('#12345')).toBeNull();
  });
});

describe('fieldPixels', () => {
  it('paints lit cells opaque in their tone and leaves empty cells clear', () => {
    const pixels = fieldPixels(
      Uint8Array.from([0, 1, 2]),
      [10, 20, 30],
      [40, 50, 60],
      new Uint8ClampedArray(12).fill(99),
    );
    expect(Array.from(pixels.slice(3, 4))).toEqual([0]);
    expect(Array.from(pixels.slice(4, 8))).toEqual([10, 20, 30, 255]);
    expect(Array.from(pixels.slice(8, 12))).toEqual([40, 50, 60, 255]);
  });
});
