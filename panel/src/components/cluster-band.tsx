// SPDX-License-Identifier: AGPL-3.0-only
import { useCallback, useMemo } from 'react';
import type { App, Node, Vm } from '../api/generated/model';
import { bandAxisMax, bandFacts, bandReadout, gibOne, gibTotal, sumNodeSeries, worstStream } from '../lib/board';
import { drawMetricArea, type DitherSurface, type TokenReader } from '../lib/dither-draw';
import { areaPlot, BARE_PAD, seriesSummary, toSeries } from '../lib/metric-area';
import { formatPercent } from '../lib/metrics';
import { DitherCanvas } from './dither-canvas';
import { LampDot } from './lamp';
import { SkelBar, Skeleton } from './skeleton';
import { useLiveNodes } from './live-nodes';

/** Room at the right for the newest sample's dot. */
const BAND_PAD = { ...BARE_PAD, right: 6 };
const INTERVAL_MS = 5_000;
const LOOK = { labels: false, dot: true, lineWidth: 1.5 } as const;

/**
 * The cluster's load on top of the board: CPU busy now and over the last ten
 * minutes, summed from each node's own samples, behind the apps running, the
 * VMs and the memory in use. Each new sample redraws the area at once. It says
 * Live only while every listed node is in the sums, and how many are otherwise.
 * Until the lists and the first samples arrive, it is a skeleton of itself.
 */
export function ClusterBand({
  apps,
  vms,
  nodes,
  loading,
}: {
  apps: readonly App[];
  vms: readonly Vm[];
  nodes: readonly Node[];
  /** The node, app or VM list has not come back yet. */
  loading: boolean;
}) {
  const feeds = useLiveNodes();
  const states = useMemo(() => Object.values(feeds), [feeds]);
  const samples = useMemo(() => sumNodeSeries(states.map((feed) => feed.samples)), [states]);
  const newest = samples[samples.length - 1];
  const facts = bandFacts(apps, vms);
  const readout = bandReadout({
    status: worstStream(states.map((feed) => feed.status)),
    covered: newest?.nodes ?? 0,
    listed: nodes.filter((node) => node.status !== 'removed').length,
  });

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
          pad: BAND_PAD,
        }),
        read,
        LOOK,
      ),
    [series, end, max],
  );
  // A connected node whose stream has not reported, or still opens, means the first samples are on their way.
  const connected = nodes.filter((node) => node.status === 'connected').length;
  const opening = states.length < connected || states.some((feed) => feed.status === 'connecting');
  if (loading || (!newest && opening)) return <BandSkeleton />;

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
            {readout.words}
          </span>
        ) : null}
      </div>
    </section>
  );
}

/** The band while its data loads: the number, the facts line and the area as dither bars, at the band's own height. */
function BandSkeleton() {
  return (
    <section className="card band band-skel" aria-label="Cluster load">
      <Skeleton label="Loading the cluster’s load">
        <div className="band-in">
          <div className="band-l">
            <div>
              <p className="band-k">Cluster load, last 10 minutes</p>
              <span className="band-skel-big">
                <SkelBar width="128px" height={22} />
              </span>
            </div>
            <span className="band-skel-facts">
              <SkelBar width="min(300px, 100%)" />
            </span>
          </div>
        </div>
        <SkelBar className="band-skel-chart" />
      </Skeleton>
    </section>
  );
}
