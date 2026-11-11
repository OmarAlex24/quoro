// SPDX-License-Identifier: AGPL-3.0-only
import { useCallback, useEffect, useId, useLayoutEffect, useRef, useState, type KeyboardEvent, type PointerEvent, type ReactNode } from 'react';
import { drawMetricArea, type DitherSurface, type MetricAreaLook, type TokenReader } from '../lib/dither-draw';
import {
  areaPlot,
  clockTime,
  nearestSample,
  plotBox,
  readingWords,
  seriesSummary,
  splitRuns,
  stepIndex,
  STEP_KEYS,
  timeForX,
  toSeries,
  whenWords,
  xForTime,
  yForValue,
  type Axis,
  type PlotPad,
} from '../lib/metric-area';
import { METRICS_WINDOW_MS } from '../lib/metrics';
import { cn } from '../lib/utils';
import { DitherCanvas } from './dither-canvas';

export interface DitherAreaProps<T extends { time: string }> {
  title: string;
  /** What the axis measures, beside the title: "share of 1 vCPU", "VMM resident memory, MiB". */
  subtitle: string;
  samples: readonly T[];
  /** The sample's value in the axis unit (percent, MiB). */
  value: (sample: T) => number;
  axis: Axis;
  tickLabel: (value: number) => string;
  /** The exact value an operator reads, from the sample itself: "22.8%", "406 MiB". */
  formatValue: (value: number) => string;
  /** Seconds between samples; decides where a gap is. */
  intervalS: number;
  /** The configured limit, drawn as a thin line: memory's configured size. */
  limit?: { value: number; label: string } | null;
  emptyText: string;
  /**
   * A small chart (the node cards): no tick or time labels, the plot the
   * whole canvas, and `reading` (the newest value) at the right of the
   * heading. The crosshair and its exact reading work as on a full chart.
   */
  compact?: boolean;
  /** The newest value, set at the right of the heading. */
  reading?: ReactNode;
  /** The area's first tone (compact charts only): the accent, or run green. */
  tone?: 'accent' | 'run';
  /**
   * The time under a compact chart in words, `−10 min`, `−5 min`, `now`, as
   * the service sheet draws it. The node cards leave it out.
   */
  timeAxis?: boolean;
  className?: string;
}

/** A compact chart's plot: the whole canvas but headroom for the line and room for the newest dot. */
const COMPACT_PAD: PlotPad = { left: 0, right: 4, top: 6, bottom: 0 };
const COMPACT_LOOK: MetricAreaLook = { labels: false, dot: true, lineWidth: 1.5 };
const COMPACT_RUN_LOOK: MetricAreaLook = { ...COMPACT_LOOK, tone: 'run' };

/**
 * A metric over the last ten minutes as a dithered area. The picture is a
 * canvas painted by the dither kit; the crosshair and its reading sit on top
 * as elements, so moving the pointer never repaints the dither. The plot takes
 * focus: the arrow keys move the crosshair a sample at a time (PageUp and
 * PageDown a minute, Home and End the ends), each move is announced, and the
 * chart's summary (now, lowest, highest) is its description.
 */
export function DitherArea<T extends { time: string }>({
  title,
  subtitle,
  samples,
  value,
  axis,
  tickLabel,
  formatValue,
  intervalS,
  limit = null,
  emptyText,
  compact = false,
  reading: newestReading,
  tone = 'accent',
  timeAxis = false,
  className,
}: DitherAreaProps<T>) {
  const pad = compact ? COMPACT_PAD : undefined;
  const id = useId();
  const plotRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);
  const [height, setHeight] = useState(0);
  // Where the crosshair is, as a time on the chart; null when it is hidden.
  const [cursor, setCursor] = useState<number | null>(null);
  const [announcement, setAnnouncement] = useState('');
  const [fontsReady, setFontsReady] = useState(false);

  const intervalMs = Math.max(1, intervalS) * 1_000;
  const series = toSeries(samples, value);
  const { times, values } = series;
  const newest = times.length > 0 ? times[times.length - 1]! : null;
  // The right edge is the newest sample, so a node clock that differs from
  // the browser's does not push the area off the chart.
  const end = newest ?? 0;
  const runs = splitRuns(times, intervalMs).length;

  // The overlay needs the plot's size in CSS pixels; the canvas measures itself.
  useLayoutEffect(() => {
    const element = plotRef.current;
    if (!element) return;
    const measure = () => {
      setWidth(element.clientWidth);
      setHeight(element.clientHeight);
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  // The tick labels are drawn in Archivo; repaint once it has loaded.
  useEffect(() => {
    let live = true;
    void document.fonts?.ready.then(() => {
      if (live) setFontsReady(true);
    });
    return () => {
      live = false;
    };
  }, []);

  // One key for everything the picture depends on, so the canvas repaints
  // when a sample arrives and not when the crosshair moves.
  const dataKey = `${times.join(',')}|${values.join(',')}|${axis.max}|${limit?.value ?? ''}|${fontsReady}`;
  const paint = useCallback(
    (surface: DitherSurface, read: TokenReader) => {
      drawMetricArea(
        surface,
        areaPlot({
          series,
          end,
          width: surface.w,
          height: surface.h,
          axis,
          tickLabel,
          intervalMs,
          limit: times.length > 0 ? limit : null,
          pad,
        }),
        read,
        compact ? (tone === 'run' ? COMPACT_RUN_LOOK : COMPACT_LOOK) : undefined,
      );
    },
    // `dataKey` stands for the series, the axis and the limit.
    [dataKey, intervalMs, compact, tone],
  );

  const box = plotBox(width, height, pad);
  const index = cursor === null ? null : nearestSample(times, cursor, intervalMs);
  // A crosshair in a gap still shows where it is, and reads "No sample" there.
  // One whose sample has left the window disappears.
  const cursorTime = index !== null ? times[index]! : cursor;
  const showCursor = cursorTime !== null && newest !== null && cursorTime >= end - METRICS_WINDOW_MS;
  const cursorX = showCursor ? xForTime(cursorTime, end, box) : 0;

  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.key === 'Escape') {
      if (cursor !== null) {
        setCursor(null);
        setAnnouncement('');
      }
      return;
    }
    if (!STEP_KEYS.has(event.key)) return;
    event.preventDefault();
    const next = stepIndex(index, event.key, times.length);
    if (next === null) return;
    setCursor(times[next]!);
    setAnnouncement(readingWords(title, formatValue(values[next]!), times[next]!, end));
  };

  const fromPointer = (event: PointerEvent<HTMLDivElement>) => {
    if (newest === null) return;
    const rect = event.currentTarget.getBoundingClientRect();
    const x = event.clientX - rect.left;
    if (x < box.left || x > box.right) {
      if (event.pointerType !== 'touch') setCursor(null);
      return;
    }
    setCursor(timeForX(x, end, box));
  };

  const summary = seriesSummary({ label: title, values, format: formatValue, runs });
  const reading =
    showCursor && index !== null
      ? { value: formatValue(values[index]!), when: whenWords(times[index]!, end) }
      : showCursor
        ? { value: 'No sample', when: clockTime(cursorTime) }
        : null;
  // Keep the reading inside the chart: to the right of the line, or to its left near the right edge.
  const tipLeft = cursorX + 10;
  const tipOnLeft = tipLeft + 170 > width;

  return (
    <figure className={cn('chart', compact && 'chart-sm', className)}>
      <figcaption className="chart-h">
        <b id={`${id}-title`}>{title}</b>
        <span>{subtitle}</span>
        {newestReading !== undefined ? <span className="chart-v">{newestReading}</span> : null}
      </figcaption>
      <div
        ref={plotRef}
        className="chart-plot"
        tabIndex={0}
        role="img"
        aria-labelledby={`${id}-title`}
        aria-describedby={`${id}-summary ${id}-keys`}
        data-crosshair={showCursor ? '' : undefined}
        onKeyDown={onKeyDown}
        onPointerDown={fromPointer}
        onPointerMove={fromPointer}
        onPointerLeave={(event) => {
          if (event.pointerType !== 'touch') setCursor(null);
        }}
        // A tap leaves its reading up until focus moves on; a hover comes back
        // with the next pointer move.
        onBlur={() => setCursor(null)}
      >
        <DitherCanvas paint={paint} />
        {times.length === 0 ? <p className="chart-empty">{emptyText}</p> : null}
        {showCursor ? (
          <>
            <span
              className="xh"
              aria-hidden="true"
              style={{ left: cursorX, top: box.top, height: Math.max(0, box.bottom - box.top) }}
            />
            {index !== null ? (
              <span
                className="xh-dot"
                aria-hidden="true"
                style={{ left: cursorX, top: yForValue(values[index]!, axis.max, box) }}
              />
            ) : null}
          </>
        ) : null}
        {reading ? (
          <span
            className="tip"
            aria-hidden="true"
            style={
              tipOnLeft
                ? { right: Math.max(6, width - cursorX + 10) }
                : { left: Math.max(6, tipLeft) }
            }
          >
            <b>{reading.value}</b>
            <span>{reading.when}</span>
          </span>
        ) : null}
      </div>
      {timeAxis ? (
        <div className="chart-ax" aria-hidden="true">
          <span>−10 min</span>
          <span>−5 min</span>
          <span>now</span>
        </div>
      ) : null}
      <p id={`${id}-summary`} className="sr-only">
        {summary}
      </p>
      <p id={`${id}-keys`} className="sr-only">
        Arrow keys read one sample at a time, Page Up and Page Down a minute, Home and End the oldest and newest.
      </p>
      <p className="sr-only" aria-live="polite" aria-atomic="true">
        {announcement}
      </p>
    </figure>
  );
}
