import { describe, expect, it } from 'vitest';
import {
  autoLayout,
  bounds,
  CARD_W,
  CELL_H,
  CELL_W,
  fitView,
  layoutFrame,
  moved,
  positionsKey,
  readingOrder,
  readPositions,
  writePositions,
  zoomIn,
  zoomOut,
  ZOOM_MAX,
  ZOOM_MIN,
} from './canvas-layout';

/** A Storage stand-in: a map, or one that refuses every call. */
const memory = (initial: Record<string, string> = {}) => {
  const data = new Map(Object.entries(initial));
  return {
    getItem: (key: string) => data.get(key) ?? null,
    setItem: (key: string, value: string) => void data.set(key, value),
    data,
  };
};
const refusing = {
  getItem: () => {
    throw new Error('SecurityError');
  },
  setItem: () => {
    throw new Error('QuotaExceededError');
  },
};

const frame = { columns: 3, origin: { x: 48, y: 80 } };

describe('layoutFrame', () => {
  it('fits up to four columns and leaves room for the tools', () => {
    expect(layoutFrame(390)).toEqual({ columns: 1, origin: { x: 16, y: 72 } });
    expect(layoutFrame(1384).columns).toBe(4);
    expect(layoutFrame(3000).columns).toBe(4);
    expect(layoutFrame(900)).toEqual({ columns: 2, origin: { x: 48, y: 80 } });
  });
});

describe('autoLayout', () => {
  const items = [
    { key: 'app:web', kind: 'git' as const },
    { key: 'app:api', kind: 'git' as const },
    { key: 'app:pg', kind: 'image' as const },
    { key: 'app:worker', kind: 'image' as const },
    { key: 'vm:ci', kind: 'vm' as const },
  ];

  it('gives each kind its own band of rows', () => {
    expect(autoLayout(items, {}, frame)).toEqual({
      'app:web': { x: 48, y: 80 },
      'app:api': { x: 48 + CELL_W, y: 80 },
      'app:pg': { x: 48, y: 80 + CELL_H },
      'app:worker': { x: 48 + CELL_W, y: 80 + CELL_H },
      'vm:ci': { x: 48, y: 80 + 2 * CELL_H },
    });
  });

  it('wraps a kind that does not fit in one row', () => {
    const one = { columns: 1, origin: { x: 16, y: 72 } };
    const layout = autoLayout(items.slice(0, 3), {}, one);
    expect(layout['app:api']).toEqual({ x: 16, y: 72 + CELL_H });
    expect(layout['app:pg']).toEqual({ x: 16, y: 72 + 2 * CELL_H });
  });

  it('keeps a saved position and never lays a new card over it', () => {
    const saved = { 'app:api': { x: 48, y: 80 } };
    const layout = autoLayout(items.slice(0, 2), saved, frame);
    expect(layout['app:api']).toEqual({ x: 48, y: 80 });
    expect(layout['app:web']).toEqual({ x: 48 + CELL_W, y: 80 });
  });

  it('ignores a saved position for a service that no longer exists', () => {
    const layout = autoLayout(items.slice(0, 1), { 'app:gone': { x: 48, y: 80 } }, frame);
    expect(layout).toEqual({ 'app:web': { x: 48, y: 80 } });
  });
});

describe('moved', () => {
  it('follows the drag but stays inside the canvas', () => {
    expect(moved({ x: 100, y: 100 }, 20.4, -30.6)).toEqual({ x: 120, y: 69 });
    expect(moved({ x: 10, y: 10 }, -50, -50)).toEqual({ x: 0, y: 0 });
  });
});

describe('readingOrder', () => {
  it('goes row by row, then left to right', () => {
    const positions = {
      a: { x: 400, y: 80 },
      b: { x: 48, y: 90 },
      c: { x: 48, y: 400 },
      d: { x: 700, y: 75 },
    };
    expect(readingOrder(positions, ['a', 'b', 'c', 'd'])).toEqual(['b', 'a', 'd', 'c']);
  });
});

describe('position storage', () => {
  it('round-trips the positions of one project', () => {
    const storage = memory();
    writePositions(storage, 'prj_1', { 'app:a': { x: 10.4, y: 20 } }, ['app:a']);
    expect(storage.data.get(positionsKey('prj_1'))).toBe('{"app:a":{"x":10,"y":20}}');
    expect(readPositions(storage, 'prj_1')).toEqual({ 'app:a': { x: 10, y: 20 } });
    expect(readPositions(storage, 'prj_2')).toEqual({});
  });

  it('forgets services that no longer exist when it saves', () => {
    const storage = memory();
    writePositions(storage, 'p', { 'app:a': { x: 1, y: 2 }, 'app:gone': { x: 3, y: 4 } }, ['app:a']);
    expect(readPositions(storage, 'p')).toEqual({ 'app:a': { x: 1, y: 2 } });
  });

  it('reads damaged or foreign content as nothing saved', () => {
    expect(readPositions(memory({ [positionsKey('p')]: '{not json' }), 'p')).toEqual({});
    expect(readPositions(memory({ [positionsKey('p')]: '[1,2]' }), 'p')).toEqual({});
    expect(
      readPositions(
        memory({ [positionsKey('p')]: '{"app:a":{"x":"1","y":2},"app:b":{"x":-5,"y":3}}' }),
        'p',
      ),
    ).toEqual({ 'app:b': { x: 0, y: 3 } });
  });

  it('works without storage and when storage refuses', () => {
    expect(readPositions(null, 'p')).toEqual({});
    expect(readPositions(refusing, 'p')).toEqual({});
    expect(() => writePositions(refusing, 'p', { a: { x: 1, y: 1 } }, ['a'])).not.toThrow();
    expect(() => writePositions(null, 'p', {}, [])).not.toThrow();
  });
});

describe('zoom and fit', () => {
  it('steps by a tenth between the limits', () => {
    expect(zoomIn(1)).toBe(1.1);
    expect(zoomOut(1)).toBe(0.9);
    expect(zoomIn(ZOOM_MAX)).toBe(ZOOM_MAX);
    expect(zoomOut(ZOOM_MIN)).toBe(ZOOM_MIN);
  });

  it('boxes every card', () => {
    expect(bounds({})).toBeNull();
    expect(bounds({ a: { x: 48, y: 80 }, b: { x: 368, y: 320 } }, { b: { w: CARD_W, h: 150 } })).toEqual({
      x: 48,
      y: 80,
      w: 320 + CARD_W,
      h: 240 + 150,
    });
  });

  it('never zooms past 1 to fit, and shrinks what does not fit', () => {
    const small = fitView({ x: 48, y: 80, w: 584, h: 390 }, { w: 1200, h: 800 });
    expect(small.zoom).toBe(1);
    const large = fitView({ x: 0, y: 0, w: 2400, h: 1000 }, { w: 1200, h: 800 });
    expect(large.zoom).toBeLessThan(0.5);
    expect(large.zoom).toBeGreaterThanOrEqual(ZOOM_MIN);
  });
});
