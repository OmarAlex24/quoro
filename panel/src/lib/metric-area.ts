// SPDX-License-Identifier: AGPL-3.0-only
/**
 * The pure half of DitherArea, the metrics chart: the plot box, the axes, how
 * a time and a value land on the canvas, where the samples break into runs
 * (a gap is a stretch with no sample), which sample a crosshair reads, and
 * the words a screen reader gets. No DOM here; `dither-draw.ts` paints what
 * `areaPlot` returns and `dither-area.tsx` handles the pointer and the keys.
 */
import { METRICS_WINDOW_MS } from './metrics';
import type { MetricsStreamStatus } from './use-metrics-stream';
import type { LampTone } from './lamp';

/** The plot inside the canvas, in CSS pixels: room for the tick labels at the left and the time labels below. */
export interface PlotBox {
  left: number;
  right: number;
  top: number;
  bottom: number;
}

export const PLOT_PAD = { left: 44, right: 10, top: 8, bottom: 20 } as const;

export const plotBox = (width: number, height: number): PlotBox => ({
  left: PLOT_PAD.left,
  right: Math.max(PLOT_PAD.left + 1, width - PLOT_PAD.right),
  top: PLOT_PAD.top,
  bottom: Math.max(PLOT_PAD.top + 1, height - PLOT_PAD.bottom),
});

/** A y axis from 0 to `max`, with its grid lines. Values are in the axis unit (percent, MiB). */
export interface Axis {
  max: number;
  ticks: number[];
}

const ticksTo = (max: number, step: number): number[] => {
  const ticks: number[] = [];
  for (let value = 0; value <= max + step / 1000; value += step) ticks.push(Math.round(value * 1000) / 1000);
  return ticks;
};

/**
 * CPU in percent of the VM's vCPUs. 0 to 100 % in quarters; the VMM's own
 * threads can push a VM a little above 100 % (openapi.yaml `VmMetricSample`),
 * and then the axis grows in quarters to hold the peak.
 */
export const cpuAxis = (peakPercent: number): Axis => {
  const peak = Number.isFinite(peakPercent) ? peakPercent : 0;
  const max = peak <= 100 ? 100 : Math.ceil(peak / 25) * 25;
  return { max, ticks: ticksTo(max, max > 150 ? 50 : 25) };
};

const NICE_STEPS = [1, 2, 2.5, 4, 5, 10] as const;

/**
 * An axis a little above `top`, in about three round steps: 512 MiB of
 * memory gets 0, 200, 400, 600; 1024 gets 0, 400, 800, 1200.
 */
export const niceAxis = (top: number, steps = 3): Axis => {
  const value = Number.isFinite(top) && top > 0 ? top * 1.08 : 1;
  const raw = value / steps;
  const magnitude = 10 ** Math.floor(Math.log10(raw));
  const step = (NICE_STEPS.find((candidate) => candidate * magnitude >= raw) ?? 10) * magnitude;
  const max = Math.ceil(value / step) * step;
  return { max, ticks: ticksTo(max, step) };
};

/** Where a time lands: the window's start at the left edge, its end (the newest sample) at the right. */
export const xForTime = (time: number, end: number, box: PlotBox, windowMs = METRICS_WINDOW_MS): number =>
  box.left + (box.right - box.left) * clamp01((time - (end - windowMs)) / windowMs);

/** The inverse of `xForTime`, clamped to the window. */
export const timeForX = (x: number, end: number, box: PlotBox, windowMs = METRICS_WINDOW_MS): number =>
  end - windowMs + windowMs * clamp01((x - box.left) / Math.max(1, box.right - box.left));

/** Where a value lands: 0 on the bottom edge, `max` on the top. */
export const yForValue = (value: number, max: number, box: PlotBox): number =>
  box.bottom - (box.bottom - box.top) * clamp01(max > 0 ? value / max : 0);

/** A series in the chart's terms: times in milliseconds, oldest first, and values in the axis unit. */
export interface Series {
  times: number[];
  values: number[];
}

/** Keeps the samples with a readable time and a finite value, oldest first. */
export const toSeries = <T extends { time: string }>(samples: readonly T[], value: (sample: T) => number): Series => {
  const rows = samples
    .map((sample) => ({ time: Date.parse(sample.time), value: value(sample) }))
    .filter((row) => Number.isFinite(row.time) && Number.isFinite(row.value))
    .sort((a, b) => a.time - b.time);
  return { times: rows.map((row) => row.time), values: rows.map((row) => row.value) };
};

/**
 * Two samples further apart than one and a half intervals have a gap between
 * them: at least one sample the node did not take or the panel never got.
 */
export const GAP_FACTOR = 1.5;

/** The index ranges, `[first, last]` inclusive, that have no gap inside them. */
export const splitRuns = (times: readonly number[], intervalMs: number): Array<[number, number]> => {
  if (times.length === 0) return [];
  const runs: Array<[number, number]> = [];
  let start = 0;
  for (let index = 1; index < times.length; index += 1) {
    if (times[index]! - times[index - 1]! > intervalMs * GAP_FACTOR) {
      runs.push([start, index - 1]);
      start = index;
    }
  }
  runs.push([start, times.length - 1]);
  return runs;
};

/**
 * The sample a crosshair at `time` reads: the nearest one, as long as it is
 * within three quarters of an interval. Inside a run the nearest sample is
 * always that close; in a gap, or before the first sample, there is none.
 */
export const nearestSample = (times: readonly number[], time: number, intervalMs: number): number | null => {
  if (times.length === 0 || !Number.isFinite(time)) return null;
  let low = 0;
  let high = times.length - 1;
  while (low < high) {
    const middle = (low + high) >> 1;
    if (times[middle]! < time) low = middle + 1;
    else high = middle;
  }
  let best = low;
  if (low > 0 && Math.abs(times[low - 1]! - time) <= Math.abs(times[low]! - time)) best = low - 1;
  return Math.abs(times[best]! - time) <= intervalMs * 0.75 ? best : null;
};

/** The keys that move the crosshair from the keyboard. */
export const STEP_KEYS: ReadonlySet<string> = new Set([
  'ArrowLeft',
  'ArrowRight',
  'Home',
  'End',
  'PageUp',
  'PageDown',
]);

/** PageUp and PageDown move a minute at the node's 5 s interval. */
export const PAGE_STEP = 12;

/**
 * The sample a key moves the crosshair to. With no crosshair yet, any key
 * but Home starts at the newest sample, where an operator looks first. Null
 * when there is nothing to read or the key does not move the crosshair.
 */
export const stepIndex = (current: number | null, key: string, length: number): number | null => {
  if (length === 0 || !STEP_KEYS.has(key)) return null;
  const last = length - 1;
  if (key === 'Home') return 0;
  if (key === 'End') return last;
  if (current === null) return last;
  const from = Math.min(last, Math.max(0, current));
  switch (key) {
    case 'ArrowLeft':
      return Math.max(0, from - 1);
    case 'ArrowRight':
      return Math.min(last, from + 1);
    case 'PageUp':
      return Math.max(0, from - PAGE_STEP);
    default:
      return Math.min(last, from + PAGE_STEP);
  }
};

/** One grid line: where it sits and what it says. */
export interface AxisTick {
  y: number;
  label: string;
}

/** Everything the painter draws, already in CSS pixels. */
export interface AreaPlot {
  box: PlotBox;
  ticks: AxisTick[];
  /** Each run is drawn on its own, so a gap stays empty. */
  runs: Array<Array<{ x: number; y: number }>>;
  /** The configured limit, drawn as a thin line with its label. */
  limit: { y: number; label: string } | null;
  /** The newest sample, which gets the dot at the right edge. */
  newest: { x: number; y: number } | null;
}

export const areaPlot = ({
  series,
  end,
  width,
  height,
  axis,
  tickLabel,
  intervalMs,
  limit = null,
  windowMs = METRICS_WINDOW_MS,
}: {
  series: Series;
  end: number;
  width: number;
  height: number;
  axis: Axis;
  tickLabel: (value: number) => string;
  intervalMs: number;
  limit?: { value: number; label: string } | null;
  windowMs?: number;
}): AreaPlot => {
  const box = plotBox(width, height);
  const point = (index: number) => ({
    x: xForTime(series.times[index]!, end, box, windowMs),
    y: yForValue(series.values[index]!, axis.max, box),
  });
  const start = end - windowMs;
  const runs = splitRuns(series.times, intervalMs)
    .map(([first, last]) => {
      const points: Array<{ x: number; y: number }> = [];
      for (let index = first; index <= last; index += 1) {
        if (series.times[index]! >= start) points.push(point(index));
      }
      return points;
    })
    .filter((run) => run.length > 0);
  const lastIndex = series.times.length - 1;
  return {
    box,
    ticks: axis.ticks.map((value) => ({ y: yForValue(value, axis.max, box), label: tickLabel(value) })),
    runs,
    limit: limit === null ? null : { y: yForValue(limit.value, axis.max, box), label: limit.label },
    newest: lastIndex >= 0 ? point(lastIndex) : null,
  };
};

/** `0`, `25%`, `100%`: the CPU axis says its unit on every line but the floor. */
export const percentTick = (value: number): string => (value === 0 ? '0' : `${value}%`);

/** The memory axis is in MiB and says so in its heading, so its lines are bare numbers. */
export const plainTick = (value: number): string => `${value}`;

const MIB = 1024 * 1024;
const GIB = 1024 * MIB;

/**
 * The memory figures of the stats row: used and configured in one unit,
 * MiB below 4 GiB of configured memory and GiB from there.
 */
export const memoryFigures = (usedBytes: number, totalBytes: number): { used: string; of: string } => {
  if (totalBytes >= 4 * GIB) {
    return { used: (usedBytes / GIB).toFixed(1), of: `/ ${(totalBytes / GIB).toFixed(1)} GiB` };
  }
  return { used: `${Math.round(usedBytes / MIB)}`, of: `/ ${Math.round(totalBytes / MIB)} MiB` };
};

/** "share of 1 vCPU", "share of 4 vCPUs": what 100 % means for this VM. */
export const vcpuShareWords = (vcpus: number): string => `share of ${vcpus} ${vcpus === 1 ? 'vCPU' : 'vCPUs'}`;

/** A span in plain words, down to the second: `45 s`, `3 min 20 s`, `2 min`, `1 h 4 min`. */
export const secondsWords = (seconds: number): string => {
  const total = Math.max(0, Math.round(seconds));
  if (total < 60) return `${total} s`;
  if (total < 3_600) {
    const minutes = Math.floor(total / 60);
    const rest = total % 60;
    return rest ? `${minutes} min ${rest} s` : `${minutes} min`;
  }
  const hours = Math.floor(total / 3_600);
  const minutes = Math.floor((total % 3_600) / 60);
  return minutes ? `${hours} h ${minutes} min` : `${hours} h`;
};

/** How long before the newest sample a sample was taken: `now`, `45 s ago`, `3 min 20 s ago`. */
export const agoWords = (time: number, newest: number): string => {
  const seconds = Math.round((newest - time) / 1_000);
  return seconds <= 0 ? 'now' : `${secondsWords(seconds)} ago`;
};

const two = (value: number): string => String(value).padStart(2, '0');

/** The browser's local clock time, to the second: `14:32:05`. */
export const clockTime = (time: number): string => {
  const date = new Date(time);
  return `${two(date.getHours())}:${two(date.getMinutes())}:${two(date.getSeconds())}`;
};

/** What the crosshair says under the value: `14:32:05 · 3 min 20 s ago`. */
export const whenWords = (time: number, newest: number): string =>
  `${clockTime(time)} · ${agoWords(time, newest)}`;

/** What a keyboard move announces: `CPU 22.8% at 14:32:05, 3 min 20 s ago`. */
export const readingWords = (label: string, value: string, time: number, newest: number): string =>
  `${label} ${value} at ${clockTime(time)}, ${agoWords(time, newest)}`;

/**
 * The chart said as text, for a screen reader and for anyone who reads
 * numbers better than shapes: the newest value, the lowest and the highest
 * over the window, and whether samples are missing.
 */
export const seriesSummary = ({
  label,
  values,
  format,
  runs = 1,
}: {
  label: string;
  values: readonly number[];
  format: (value: number) => string;
  runs?: number;
}): string => {
  if (values.length === 0) return `${label}: no samples yet.`;
  let lowest = values[0]!;
  let highest = values[0]!;
  for (const value of values) {
    if (value < lowest) lowest = value;
    if (value > highest) highest = value;
  }
  const now = values[values.length - 1]!;
  const gaps = runs > 1 ? ' Samples are missing for part of the window.' : '';
  return `${label} over the last 10 minutes: now ${format(now)}, lowest ${format(lowest)}, highest ${format(highest)}.${gaps}`;
};

/** How the live readout reads for each state of the stream. */
export interface StreamReadout {
  tone: LampTone;
  words: string;
  pulse: boolean;
}

const STREAM_READOUT: Record<MetricsStreamStatus, StreamReadout> = {
  connecting: { tone: 'shift', words: 'Connecting', pulse: true },
  live: { tone: 'run', words: 'Live · last 10 minutes', pulse: false },
  reconnecting: { tone: 'shift', words: 'Reconnecting', pulse: true },
  unreachable: { tone: 'fault', words: 'Node unreachable', pulse: false },
  error: { tone: 'fault', words: 'Stream stopped', pulse: false },
};

export const streamReadout = (status: MetricsStreamStatus): StreamReadout => STREAM_READOUT[status];

const clamp01 = (value: number): number => Math.min(1, Math.max(0, value));
