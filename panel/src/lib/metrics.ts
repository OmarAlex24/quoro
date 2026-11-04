/**
 * The pure half of the metrics bays: merging history with live samples,
 * formatting the values an operator reads, and turning a sample window into
 * SVG coordinates. No React in here; the components only draw what this says.
 */

/** The window the node keeps and the panel shows: the last ten minutes. */
export const METRICS_WINDOW_MS = 10 * 60 * 1000;

export interface TimedSample {
  time: string;
}

export interface MetricDomain {
  min: number;
  max: number;
}

export interface ChartGeometry {
  width: number;
  height: number;
  padTop?: number;
  padRight?: number;
  padBottom?: number;
  padLeft?: number;
}

export interface ChartPoint {
  x: number;
  y: number;
}

const timeOf = (sample: TimedSample): number => Date.parse(sample.time);

/**
 * Merge a history read with the samples that arrived live: keep the last ten
 * minutes, drop anything outside it, dedupe by `keyOf` (by default `time`) and
 * sort oldest first. The incoming sample wins when both carry the same key, so
 * a fresh history read can replace a replayed live sample.
 */
export const mergeSamples = <T extends TimedSample>(
  current: readonly T[],
  incoming: readonly T[],
  nowMs: number = Date.now(),
  keyOf: (sample: T) => string = (sample) => sample.time,
  windowMs: number = METRICS_WINDOW_MS,
): T[] => {
  // The window is anchored on the newest sample, not on the browser clock, so
  // a node whose clock differs from the browser's still shows its samples.
  let newest = Number.NEGATIVE_INFINITY;
  for (const sample of [...current, ...incoming]) {
    const time = timeOf(sample);
    if (Number.isFinite(time) && time > newest) newest = time;
  }
  const cutoff = (Number.isFinite(newest) ? newest : nowMs) - windowMs;
  const byKey = new Map<string, T>();

  for (const sample of current) {
    const time = timeOf(sample);
    if (!Number.isFinite(time) || time < cutoff) continue;
    byKey.set(keyOf(sample), sample);
  }
  for (const sample of incoming) {
    const time = timeOf(sample);
    if (!Number.isFinite(time) || time < cutoff) continue;
    byKey.set(keyOf(sample), sample);
  }

  return [...byKey.values()].sort((a, b) => timeOf(a) - timeOf(b));
};

/** A fraction of capacity as a percentage with one decimal: `0.124` → `12.4%`. */
export const formatPercent = (fraction: number): string => {
  const value = Number.isFinite(fraction) ? fraction : 0;
  return `${(value * 100).toFixed(1)}%`;
};

const MIB = 1024 * 1024;
const GIB = 1024 * MIB;

/**
 * Bytes in the unit an operator reads them in: GiB above a gibibyte with one
 * decimal, whole MiB from 100 MiB up, one decimal below that.
 */
export const formatBytes = (bytes: number): string => {
  if (!Number.isFinite(bytes) || bytes <= 0) return '0 MiB';
  if (bytes >= GIB) return `${(bytes / GIB).toFixed(1)} GiB`;
  const mib = bytes / MIB;
  return mib >= 100 ? `${Math.round(mib)} MiB` : `${mib.toFixed(1)} MiB`;
};

/**
 * The y-domain of a CPU series: a node's usage is a fraction of all host CPUs
 * and never leaves 0..1; a VM's fraction of its vCPUs can reach slightly above
 * 1, so the domain grows to the peak but never shrinks below 0..1.
 */
export const cpuDomain = (
  samples: readonly { cpu_usage: number }[],
  vm = false,
): MetricDomain => {
  if (!vm) return { min: 0, max: 1 };
  const peak = samples.reduce(
    (highest, sample) => Math.max(highest, sample.cpu_usage),
    0,
  );
  return { min: 0, max: Math.max(1, peak) };
};

/** The y-domain of a memory series: zero to the configured total. */
export const memoryDomain = (totalBytes: number): MetricDomain => ({
  min: 0,
  max: Math.max(1, Number.isFinite(totalBytes) ? totalBytes : 0),
});

/**
 * Map each sample onto the chart: x is its place in the last ten minutes with
 * now at the right edge, y is its value inside the fixed domain.
 */
export const chartPoints = <T extends TimedSample>(
  samples: readonly T[],
  value: (sample: T) => number,
  domain: MetricDomain,
  geometry: ChartGeometry,
  nowMs: number = Date.now(),
  windowMs: number = METRICS_WINDOW_MS,
): ChartPoint[] => {
  const { width, height } = geometry;
  const padTop = geometry.padTop ?? 10;
  const padRight = geometry.padRight ?? 8;
  const padBottom = geometry.padBottom ?? 18;
  const padLeft = geometry.padLeft ?? 44;
  const innerWidth = Math.max(0, width - padLeft - padRight);
  const innerHeight = Math.max(0, height - padTop - padBottom);
  const start = nowMs - windowMs;
  const span = Math.max(Number.EPSILON, domain.max - domain.min);

  return samples.map((sample) => {
    const time = timeOf(sample);
    const xRatio = Number.isFinite(time)
      ? clamp((time - start) / windowMs, 0, 1)
      : 0;
    const yRatio = clamp((value(sample) - domain.min) / span, 0, 1);
    return {
      x: padLeft + xRatio * innerWidth,
      y: padTop + (1 - yRatio) * innerHeight,
    };
  });
};

/** The line through the points, in SVG path syntax. */
export const linePath = (points: readonly ChartPoint[]): string =>
  points
    .map(
      (point, index) =>
        `${index === 0 ? 'M' : 'L'}${point.x.toFixed(2)} ${point.y.toFixed(2)}`,
    )
    .join(' ');

/** The filled area under the line, down to the chart's baseline. */
export const areaPath = (
  points: readonly ChartPoint[],
  baselineY: number,
): string => {
  if (points.length === 0) return '';
  const first = points[0];
  const last = points[points.length - 1];
  if (first === undefined || last === undefined) return '';
  return `${linePath(points)} L${last.x.toFixed(2)} ${baselineY.toFixed(2)} L${first.x.toFixed(2)} ${baselineY.toFixed(2)} Z`;
};

const clamp = (value: number, min: number, max: number): number =>
  Math.min(max, Math.max(min, value));
