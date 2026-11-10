import { describe, expect, it } from 'vitest';
import { lineWidths } from './skeleton';

describe('lineWidths', () => {
  it('gives uneven widths that repeat', () => {
    expect(lineWidths(7)).toEqual([88, 72, 80, 64, 76, 88, 72]);
  });

  it('starts further into the pattern with an offset', () => {
    expect(lineWidths(2, 3)).toEqual([64, 76]);
  });

  it('is stable from render to render', () => {
    expect(lineWidths(4)).toEqual(lineWidths(4));
  });

  it('gives nothing for no lines or a broken count', () => {
    expect(lineWidths(0)).toEqual([]);
    expect(lineWidths(-3)).toEqual([]);
    expect(lineWidths(2.9)).toEqual([88, 72]);
  });
});
