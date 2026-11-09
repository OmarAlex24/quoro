import { describe, expect, it } from 'vitest';
import {
  agoWords,
  areaPlot,
  capacityAxis,
  clockTime,
  cpuAxis,
  memoryFigures,
  nearestSample,
  niceAxis,
  percentTick,
  plotBox,
  readingWords,
  secondsWords,
  seriesSummary,
  splitRuns,
  stepIndex,
  streamReadout,
  timeForX,
  toSeries,
  vcpuShareWords,
  whenWords,
  xForTime,
  yForValue,
} from './metric-area';
import { formatPercent } from './metrics';

const INTERVAL = 5_000;
const END = Date.parse('2026-11-03T12:10:00.000Z');
const WINDOW = 10 * 60 * 1_000;

/** Times every 5 s ending at END, oldest first. */
const every5s = (count: number, end = END): number[] =>
  Array.from({ length: count }, (_, index) => end - (count - 1 - index) * INTERVAL);

describe('plotBox', () => {
  it('leaves room for the tick labels at the left and the time labels below', () => {
    expect(plotBox(600, 150)).toEqual({ left: 44, right: 590, top: 8, bottom: 130 });
  });

  it('never collapses to a negative width on a canvas with no box yet', () => {
    const box = plotBox(0, 0);
    expect(box.right).toBeGreaterThan(box.left);
    expect(box.bottom).toBeGreaterThan(box.top);
  });
});

describe('axes', () => {
  it('draws CPU from 0 to 100 % in quarters', () => {
    expect(cpuAxis(22.8)).toEqual({ max: 100, ticks: [0, 25, 50, 75, 100] });
  });

  it('grows the CPU axis in quarters when the VMM pushes a VM above 100 %', () => {
    expect(cpuAxis(108)).toEqual({ max: 125, ticks: [0, 25, 50, 75, 100, 125] });
    expect(cpuAxis(Number.NaN).max).toBe(100);
  });

  it('puts the memory axis a round step above the configured memory', () => {
    expect(niceAxis(512)).toEqual({ max: 600, ticks: [0, 200, 400, 600] });
    expect(niceAxis(1024)).toEqual({ max: 1200, ticks: [0, 400, 800, 1200] });
    expect(niceAxis(256).max).toBeGreaterThan(256);
  });

  it('gives an empty series an axis that still has a top', () => {
    expect(niceAxis(0).max).toBeGreaterThan(0);
  });

  // Issue 639: a 3922 MiB host drew its memory on an axis to 6000, so the line sat in the bottom third.
  it('ends a host memory axis at the host’s memory, with round grid lines under it', () => {
    expect(niceAxis(3922).max).toBe(6000);
    expect(capacityAxis(3922)).toEqual({ max: 3922, ticks: [0, 1000, 2000, 3000] });
    expect(capacityAxis(2048)).toEqual({ max: 2048, ticks: [0, 500, 1000, 1500, 2000] });
    expect(capacityAxis(16_384)).toEqual({ max: 16_384, ticks: [0, 5000, 10_000, 15_000] });
    expect(capacityAxis(1024)).toEqual({ max: 1024, ticks: [0, 250, 500, 750, 1000] });
    for (const capacity of [100, 3922, 7_812, 65_536, 257_000]) {
      const axis = capacityAxis(capacity);
      expect(axis.max).toBe(capacity);
      expect(axis.ticks.length).toBeGreaterThanOrEqual(4);
      expect(axis.ticks[axis.ticks.length - 1]!).toBeLessThanOrEqual(capacity);
    }
  });

  it('falls back to a round axis without a capacity', () => {
    expect(capacityAxis(0)).toEqual(niceAxis(0));
    expect(capacityAxis(Number.NaN).max).toBeGreaterThan(0);
  });

  it('labels the CPU floor without a unit, the way the reference does', () => {
    expect([0, 25, 100].map(percentTick)).toEqual(['0', '25%', '100%']);
  });
});

describe('scaling', () => {
  const box = plotBox(600, 150);

  it('puts the newest sample at the right edge and ten minutes before it at the left', () => {
    expect(xForTime(END, END, box)).toBe(box.right);
    expect(xForTime(END - WINDOW, END, box)).toBe(box.left);
    expect(xForTime(END - WINDOW / 2, END, box)).toBe((box.left + box.right) / 2);
  });

  it('clamps a time outside the window to the edge', () => {
    expect(xForTime(END - 2 * WINDOW, END, box)).toBe(box.left);
    expect(xForTime(END + 5_000, END, box)).toBe(box.right);
  });

  it('turns an x back into the time it shows', () => {
    const time = END - 123_000;
    expect(timeForX(xForTime(time, END, box), END, box)).toBeCloseTo(time, 0);
  });

  it('puts 0 on the bottom edge and the axis top on the top edge', () => {
    expect(yForValue(0, 100, box)).toBe(box.bottom);
    expect(yForValue(100, 100, box)).toBe(box.top);
    expect(yForValue(50, 100, box)).toBe((box.top + box.bottom) / 2);
    expect(yForValue(140, 100, box)).toBe(box.top);
  });
});

describe('toSeries', () => {
  it('keeps readable samples, oldest first', () => {
    const series = toSeries(
      [
        { time: '2026-11-03T12:00:10Z', v: 2 },
        { time: 'not a time', v: 9 },
        { time: '2026-11-03T12:00:05Z', v: 1 },
        { time: '2026-11-03T12:00:15Z', v: Number.NaN },
      ],
      (sample) => sample.v,
    );
    expect(series.values).toEqual([1, 2]);
    expect(series.times).toEqual([Date.parse('2026-11-03T12:00:05Z'), Date.parse('2026-11-03T12:00:10Z')]);
  });
});

describe('gaps', () => {
  it('keeps samples one interval apart in one run', () => {
    expect(splitRuns(every5s(10), INTERVAL)).toEqual([[0, 9]]);
  });

  it('breaks the run where a sample is missing', () => {
    const times = [...every5s(4, END - 60_000), ...every5s(3)];
    expect(splitRuns(times, INTERVAL)).toEqual([
      [0, 3],
      [4, 6],
    ]);
  });

  it('tolerates live samples that arrive faster than the interval', () => {
    expect(splitRuns([0, 2_000, 4_000, 9_000], INTERVAL)).toEqual([[0, 3]]);
  });

  it('has no run without samples', () => {
    expect(splitRuns([], INTERVAL)).toEqual([]);
  });

  it('draws each run on its own and leaves the gap empty', () => {
    const times = [...every5s(4, END - 60_000), ...every5s(3)];
    const plot = areaPlot({
      series: { times, values: times.map(() => 50) },
      end: END,
      width: 600,
      height: 150,
      axis: cpuAxis(50),
      tickLabel: percentTick,
      intervalMs: INTERVAL,
    });
    expect(plot.runs.map((run) => run.length)).toEqual([4, 3]);
    expect(plot.runs[0]![3]!.x).toBeLessThan(plot.runs[1]![0]!.x);
    expect(plot.newest).toEqual({ x: plot.box.right, y: (plot.box.top + plot.box.bottom) / 2 });
  });

  it('places the limit line and the grid on the axis', () => {
    const plot = areaPlot({
      series: { times: [END], values: [406] },
      end: END,
      width: 600,
      height: 150,
      axis: niceAxis(512),
      tickLabel: String,
      intervalMs: INTERVAL,
      limit: { value: 512, label: 'limit 512 MiB' },
    });
    expect(plot.ticks.map((tick) => tick.label)).toEqual(['0', '200', '400', '600']);
    expect(plot.ticks[0]!.y).toBe(plot.box.bottom);
    expect(plot.limit?.label).toBe('limit 512 MiB');
    expect(plot.limit!.y).toBeCloseTo(yForValue(512, 600, plot.box));
    expect(plot.limit!.below).toBe(false);
  });

  it('puts the limit’s label under its line when the line runs along the top', () => {
    const plot = areaPlot({
      series: { times: [END], values: [611] },
      end: END,
      width: 600,
      height: 150,
      axis: capacityAxis(3922),
      tickLabel: String,
      intervalMs: INTERVAL,
      limit: { value: 3922, label: 'total 3.8 GiB' },
    });
    expect(plot.limit!.y).toBe(plot.box.top);
    expect(plot.limit!.below).toBe(true);
    expect(plot.ticks.map((tick) => tick.label)).toEqual(['0', '1000', '2000', '3000']);
  });

  it('draws nothing for an empty series', () => {
    const plot = areaPlot({
      series: { times: [], values: [] },
      end: END,
      width: 600,
      height: 150,
      axis: cpuAxis(0),
      tickLabel: percentTick,
      intervalMs: INTERVAL,
    });
    expect(plot.runs).toEqual([]);
    expect(plot.newest).toBeNull();
  });
});

describe('nearestSample', () => {
  const times = every5s(10);

  it('reads the closest sample', () => {
    expect(nearestSample(times, END, INTERVAL)).toBe(9);
    expect(nearestSample(times, END - 6_000, INTERVAL)).toBe(8);
    expect(nearestSample(times, END - 8_000, INTERVAL)).toBe(7);
  });

  it('prefers the older sample at an exact midpoint', () => {
    expect(nearestSample(times, END - 2_500, INTERVAL)).toBe(8);
  });

  it('finds no sample in a gap', () => {
    const gapped = [...every5s(4, END - 60_000), ...every5s(3)];
    expect(nearestSample(gapped, END - 35_000, INTERVAL)).toBeNull();
    // Close to either edge of the gap, the edge sample still reads.
    expect(nearestSample(gapped, END - 58_000, INTERVAL)).toBe(3);
  });

  it('finds no sample before the first one', () => {
    expect(nearestSample(times, END - 5 * 60_000, INTERVAL)).toBeNull();
  });

  it('finds nothing in an empty series', () => {
    expect(nearestSample([], END, INTERVAL)).toBeNull();
  });
});

describe('stepIndex', () => {
  it('starts at the newest sample', () => {
    expect(stepIndex(null, 'ArrowLeft', 10)).toBe(9);
    expect(stepIndex(null, 'ArrowRight', 10)).toBe(9);
    expect(stepIndex(null, 'Home', 10)).toBe(0);
  });

  it('moves one sample with the arrows and stops at the ends', () => {
    expect(stepIndex(5, 'ArrowLeft', 10)).toBe(4);
    expect(stepIndex(5, 'ArrowRight', 10)).toBe(6);
    expect(stepIndex(0, 'ArrowLeft', 10)).toBe(0);
    expect(stepIndex(9, 'ArrowRight', 10)).toBe(9);
  });

  it('moves a minute with PageUp and PageDown', () => {
    expect(stepIndex(30, 'PageUp', 120)).toBe(18);
    expect(stepIndex(30, 'PageDown', 120)).toBe(42);
    expect(stepIndex(5, 'PageUp', 120)).toBe(0);
  });

  it('ignores other keys and empty series', () => {
    expect(stepIndex(3, 'Enter', 10)).toBeNull();
    expect(stepIndex(null, 'ArrowLeft', 0)).toBeNull();
  });
});

describe('words', () => {
  it('says a span down to the second', () => {
    expect(secondsWords(45)).toBe('45 s');
    expect(secondsWords(200)).toBe('3 min 20 s');
    expect(secondsWords(120)).toBe('2 min');
    expect(secondsWords(3_840)).toBe('1 h 4 min');
  });

  it('counts back from the newest sample', () => {
    expect(agoWords(END, END)).toBe('now');
    expect(agoWords(END - 200_000, END)).toBe('3 min 20 s ago');
  });

  it('reads the clock in local time', () => {
    const local = new Date(2026, 10, 3, 14, 2, 5).getTime();
    expect(clockTime(local)).toBe('14:02:05');
    expect(whenWords(local, local + 45_000)).toBe('14:02:05 · 45 s ago');
    expect(readingWords('CPU', '22.8%', local, local)).toBe('CPU 22.8% at 14:02:05, now');
  });

  it('says what 100 % of CPU means for the VM', () => {
    expect(vcpuShareWords(1)).toBe('share of 1 vCPU');
    expect(vcpuShareWords(4)).toBe('share of 4 vCPUs');
  });

  it('splits memory into used and configured in one unit', () => {
    const mib = 1024 * 1024;
    expect(memoryFigures(406 * mib, 512 * mib)).toEqual({ used: '406', of: '/ 512 MiB' });
    expect(memoryFigures(6.2 * 1024 * mib, 8 * 1024 * mib)).toEqual({ used: '6.2', of: '/ 8.0 GiB' });
  });
});

describe('seriesSummary', () => {
  it('says the newest, lowest and highest values', () => {
    expect(seriesSummary({ label: 'CPU', values: [0.3, 0.061, 0.48, 0.228], format: formatPercent })).toBe(
      'CPU over the last 10 minutes: now 22.8%, lowest 6.1%, highest 48.0%.',
    );
  });

  it('says when samples are missing', () => {
    expect(seriesSummary({ label: 'CPU', values: [0.2, 0.3], format: formatPercent, runs: 2 })).toBe(
      'CPU over the last 10 minutes: now 30.0%, lowest 20.0%, highest 30.0%. Samples are missing for part of the window.',
    );
  });

  it('says so when there is nothing yet', () => {
    expect(seriesSummary({ label: 'Memory', values: [], format: String })).toBe('Memory: no samples yet.');
  });
});

describe('streamReadout', () => {
  it('reads live as the ten-minute window the node keeps', () => {
    expect(streamReadout('live')).toEqual({ tone: 'run', words: 'Live · last 10 minutes', pulse: false });
    expect(streamReadout('unreachable').tone).toBe('fault');
    expect(streamReadout('connecting').pulse).toBe(true);
  });
});
