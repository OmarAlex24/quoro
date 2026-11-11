import { beforeAll, describe, expect, it } from 'vitest';
import { drawMetricArea, type DitherSurface } from './dither-draw';
import type { AreaPlot } from './metric-area';

/** A canvas context that records what is drawn, enough for drawMetricArea. */
function recorder() {
  const texts: string[] = [];
  const rects: Array<[number, number, number, number]> = [];
  const gradient = { addColorStop: () => undefined };
  const ctx = new Proxy(
    {
      fillText: (text: string) => texts.push(text),
      fillRect: (x: number, y: number, w: number, h: number) => rects.push([x, y, w, h]),
      createLinearGradient: () => gradient,
      createImageData: (w: number, h: number) => ({ width: w, height: h, data: new Uint8ClampedArray(w * h * 4) }),
      getImageData: (_x: number, _y: number, w: number, h: number) => ({
        width: w,
        height: h,
        data: new Uint8ClampedArray(w * h * 4),
      }),
    } as Record<string, unknown>,
    {
      get: (target, key: string) => (key in target ? target[key] : () => undefined),
      set: (target, key: string, value) => {
        target[key] = value;
        return true;
      },
    },
  ) as unknown as CanvasRenderingContext2D;
  return { ctx, texts, rects };
}

const plot: AreaPlot = {
  box: { left: 0, right: 120, top: 6, bottom: 60 },
  ticks: [],
  runs: [
    [
      { x: 0, y: 40 },
      { x: 120, y: 30 },
    ],
  ],
  limit: { y: 20, label: 'limit 512 MiB', below: false },
  newest: { x: 120, y: 30 },
};

const read = () => '#808080';

beforeAll(() => {
  // The dither fills its cells through a Path2D, which Node does not have.
  if (typeof globalThis.Path2D === 'undefined') {
    (globalThis as Record<string, unknown>).Path2D = class {
      rect() {}
      moveTo() {}
      lineTo() {}
    };
  }
});

describe('drawMetricArea', () => {
  it('marks the limit with a dashed rule and no words on a chart without labels', () => {
    const { ctx, texts, rects } = recorder();
    const surface = { ctx, w: 120, h: 60 } as DitherSurface;
    drawMetricArea(surface, plot, read, { labels: false, dot: true, lineWidth: 1.5 });
    expect(texts).toEqual([]);
    const dashes = rects.filter(([, y, , h]) => y === 20 && h === 1);
    expect(dashes.length).toBe(20);
    expect(dashes.every(([, , w]) => w === 3)).toBe(true);
  });

  it('keeps the labelled limit on a full chart', () => {
    const { ctx, texts } = recorder();
    const surface = { ctx, w: 120, h: 60 } as DitherSurface;
    drawMetricArea(surface, plot, read);
    expect(texts).toContain('limit 512 MiB');
  });
});
