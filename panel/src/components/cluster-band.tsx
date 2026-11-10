// SPDX-License-Identifier: AGPL-3.0-only
import { useCallback, useMemo } from 'react';
import type { App, Vm } from '../api/generated/model';
import { bandAxisMax, bandFacts, gibOne, gibTotal, sumNodeSeries } from '../lib/board';
import { drawMetricArea, type DitherSurface, type TokenReader } from '../lib/dither-draw';
import { areaPlot, BARE_PAD, seriesSummary, streamReadout, toSeries } from '../lib/metric-area';
import { formatPercent } from '../lib/metrics';
import type { MetricsStreamStatus } from '../lib/use-metrics-stream';
import { DitherCanvas } from './dither-canvas';
import { LampDot } from './lamp';
import { useLiveNodes } from './live-nodes';

const INTERVAL_MS = 5_000;
const LOOK = { labels: false, dot: true, lineWidth: 1.5 } as const;

/** The worst state among the streams: one that stopped beats one that retries beats one that opens. */
const RANK: Record<MetricsStreamStatus, number> = { live: 0, connecting: 1, reconnecting: 2, unreachable: 3, error: 4 };

/**
 * The cluster's load on top of the board: CPU busy now and over the last ten
 * minutes, summed from each node's own samples, behind the apps running, the
 * VMs and the memory in use. Each new sample redraws the area at once.
 */
export function ClusterBand({ apps, vms }: { apps: readonly App[]; vms: readonly Vm[] }) {
  const feeds = useLiveNodes();
  const states = useMemo(() => Object.values(feeds), [feeds]);
  const samples = useMemo(() => sumNodeSeries(states.map((feed) => feed.samples)), [states]);
  const newest = samples[samples.length - 1];
  const facts = bandFacts(apps, vms);
  const status = states.reduce<MetricsStreamStatus | null>(
    (worst, feed) => (worst === null || RANK[feed.status] > RANK[worst] ? feed.status : worst),
    null,
  );
  const readout = status ? streamReadout(status) : null;

  const series = useMemo(() => toSeries(samples, (sample) => sample.cpu_usage * 100), [samples]);
  const peak = series.values.reduce((top, value) => Math.max(top, value), 0);
  const max = bandAxisMax(peak);
  const end = series.times[series.times.length - 1] ?? 0;
  const paint = useCallback(
    (surface: DitherSurface, read: TokenReader) =>
      drawMetricArea(
        surface,
        areaPlot({
          series,
          end,
          width: surface.w,
          height: surface.h,
          axis: { max, ticks: [] },
          tickLabel: String,
          intervalMs: INTERVAL_MS,
          pad: BARE_PAD,
        }),
        read,
        LOOK,
      ),
    [series, end, max],
  );
  const summary = seriesSummary({
    label: 'Cluster CPU busy',
    values: series.values,
    format: (value) => formatPercent(value / 100),
  });

  return (
    <section className="card band" aria-label="Cluster load">
      <DitherCanvas paint={paint} label={summary} className="band-chart" />
      <div className="band-in">
        <div className="band-l">
          <div>
            <p className="band-k">Cluster load, last 10 minutes</p>
            <p className="band-big">
              <span>{newest ? `${Math.round(newest.cpu_usage * 100)}%` : '—'}</span>
              <small>CPU busy</small>
            </p>
          </div>
          <p className="band-facts">
            <span>
              <b>{facts.running}</b> of {facts.apps} {facts.apps === 1 ? 'app' : 'apps'} running
            </span>
            <span>
              <b>{facts.vms}</b> {facts.vms === 1 ? 'VM' : 'VMs'}
            </span>
            {newest ? (
              <span>
                <b>{gibOne(newest.memory_used_bytes)}</b> of {gibTotal(newest.memory_total_bytes)} GiB in use
              </span>
            ) : null}
          </p>
        </div>
        {readout ? (
          <span className="band-live">
            <LampDot ring={readout.tone} core={readout.tone} pulse={readout.pulse} size={11} />
            {status === 'live' ? 'Live' : readout.words}
          </span>
        ) : null}
      </div>
    </section>
  );
}
