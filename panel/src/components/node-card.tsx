// SPDX-License-Identifier: AGPL-3.0-only
import * as DialogPrimitive from '@radix-ui/react-dialog';
import { Link } from '@tanstack/react-router';
import { Cpu, Globe, X } from 'lucide-react';
import { useCallback, useState } from 'react';
import type { Node, NodeMetricSample } from '../api/generated/model';
import type { DitherSurface, TokenReader } from '../lib/dither-draw';
import { drawMeter } from '../lib/dither-draw';
import { capacityAxis, cpuAxis, memoryFigures, percentTick, plainTick, streamReadout } from '../lib/metric-area';
import { formatBytes, formatPercent } from '../lib/metrics';
import { liveWords, loadWords, meterTone, nodeMeters, nodeTags, type NodeLoad, type NodeMeter } from '../lib/node-capacity';
import { nodeReadout } from '../lib/nodes';
import { useNodeMetricsStream, type MetricsStreamStatus } from '../lib/use-metrics-stream';
import { DitherArea } from './dither-area';
import { DitherCanvas } from './dither-canvas';
import { LampDot } from './lamp';
import { ToneLamp } from './tone-lamp';
import { Tag } from './ui/tag';

const MIB = 1024 * 1024;
const COLLECTING = 'Collecting samples — the first one arrives within 5 s';

/** One dithered meter: CPU, memory or disk reserved against the node's capacity. */
function Meter({ meter }: { meter: NodeMeter }) {
  const share = meter.share ?? 0;
  const tone = meterTone(meter.share);
  const paint = useCallback(
    (surface: DitherSurface, read: TokenReader) => drawMeter(surface, share, read, tone),
    [share, tone],
  );
  return (
    <div className="meter">
      <div className="meter-h">
        <b>{meter.label}</b>
        <span>{meter.words}</span>
      </div>
      {meter.unknown !== null ? (
        <p className="meter-unknown">{meter.unknown}</p>
      ) : (
        <DitherCanvas paint={paint} label={meter.spoken} />
      )}
    </div>
  );
}

/**
 * One node: its name, what it is, and a lamp with one word for its session;
 * CPU, memory and disk as reserved against capacity; the node's own live
 * reading of its host; and what it runs. The live reading opens the last ten
 * minutes the node keeps.
 */
export function NodeCard({ node, load }: { node: Node; load: NodeLoad }) {
  const readout = nodeReadout(node.status);
  const connected = node.status === 'connected';
  const runsVms = node.roles.includes('node');
  const { samples, status, problem } = useNodeMetricsStream(node.id, connected);
  const [charts, setCharts] = useState(false);
  const live = liveWords(samples[samples.length - 1]);
  const address = node.address ?? (node.behind_nat ? 'Behind NAT' : null);

  return (
    <article className="node" aria-label={node.name}>
      <div className="node-h">
        <ToneLamp tone={readout.tone} pulse={readout.pulse} size={15} label={readout.words} />
        <b>{node.name}</b>
        {nodeTags(node).map((tag) => (
          <Tag key={tag} tone="plain">
            {tag}
          </Tag>
        ))}
        <span className="node-word">{readout.words}</span>
      </div>

      <div className="node-b">
        {runsVms ? (
          nodeMeters(node).map((meter) => <Meter key={meter.key} meter={meter} />)
        ) : (
          <p className="node-note">Runs no VMs: this machine is {node.roles.join(' and ')} only.</p>
        )}

        <div className="node-live">
          {connected ? (
            <>
              <LiveState status={status} />
              <span className="node-live-t">{live ?? (problem ? problem.message : 'Waiting for the first sample')}</span>
              <button type="button" className="node-live-go" onClick={() => setCharts(true)}>
                Last 10 minutes
              </button>
            </>
          ) : (
            <span className="node-live-t">The node's own reading returns when it connects again.</span>
          )}
        </div>
      </div>

      <div className="svc-foot node-foot">
        {runsVms ? <Cpu strokeWidth={1.7} aria-hidden="true" /> : <Globe strokeWidth={1.7} aria-hidden="true" />}
        {runsVms ? (
          <Link to="/nodes" search={{ tab: 'vms', node: node.id }} className="node-foot-link">
            {loadWords(load)}
          </Link>
        ) : (
          <span>{node.roles.join(' · ')}</span>
        )}
        <span className="dot-sep" aria-hidden="true" />
        <span className={node.kvm.available ? undefined : 'text-fault'}>{node.kvm.available ? 'KVM' : 'No KVM'}</span>
        {address ? (
          <>
            <span className="dot-sep" aria-hidden="true" />
            <span className="min-w-0 truncate">{address}</span>
          </>
        ) : null}
      </div>

      {charts ? (
        <NodeMetricsSheet
          node={node}
          samples={samples}
          status={status}
          onClose={() => setCharts(false)}
        />
      ) : null}
    </article>
  );
}

function LiveState({ status }: { status: MetricsStreamStatus }) {
  const readout = streamReadout(status);
  return (
    <LampDot
      ring={readout.tone}
      core={readout.tone}
      pulse={readout.pulse}
      size={11}
      label={status === 'live' ? 'Live' : readout.words}
    />
  );
}

/**
 * The node's CPU and memory over the ten minutes it keeps (D-0129), as the
 * app's Metrics tab draws them, in a sheet over the Nodes page. The samples
 * are the card's own stream, so opening it starts no second one.
 */
function NodeMetricsSheet({
  node,
  samples,
  status,
  onClose,
}: {
  node: Node;
  samples: readonly NodeMetricSample[];
  status: MetricsStreamStatus;
  onClose: () => void;
}) {
  const last = samples[samples.length - 1];
  const cpuPeak = samples.reduce((peak, sample) => Math.max(peak, sample.cpu_usage * 100), 0);
  const total = samples.reduce((top, sample) => Math.max(top, sample.memory_total_bytes), 0);
  const memory = last ? memoryFigures(last.memory_used_bytes, last.memory_total_bytes) : null;
  const readout = streamReadout(status);

  return (
    <DialogPrimitive.Root open onOpenChange={(open) => (open ? undefined : onClose())}>
      <DialogPrimitive.Portal>
        <DialogPrimitive.Overlay className="dialog-overlay fixed inset-0 z-40 bg-scrim" />
        <DialogPrimitive.Content className="sheet" aria-describedby={undefined}>
          <div className="sheet-head">
            <span className="glyph glyph-neutral glyph-lg" aria-hidden="true">
              <Cpu strokeWidth={1.7} />
            </span>
            <DialogPrimitive.Title className="sheet-title">{node.name}</DialogPrimitive.Title>
            <span className="flex-1" />
            <DialogPrimitive.Close className="iconbtn" aria-label="Close">
              <X strokeWidth={1.7} aria-hidden="true" />
            </DialogPrimitive.Close>
          </div>
          <div className="sheet-body">
            <div className="grid gap-4 px-5 pb-6">
              <div className="mtop">
                <span className="mtop-one">The host, as the node reads it</span>
                <span className="flex-1" />
                <span className="mtop-state">
                  <LampDot ring={readout.tone} core={readout.tone} pulse={readout.pulse} size={12} />
                  {readout.words}
                </span>
              </div>
              <dl className="stats stats-2">
                <div>
                  <dt>CPU</dt>
                  <dd>{last ? formatPercent(last.cpu_usage) : '—'}</dd>
                </div>
                <div>
                  <dt>Memory</dt>
                  <dd>
                    {memory ? (
                      <>
                        {memory.used}
                        <em>{memory.of}</em>
                      </>
                    ) : (
                      '—'
                    )}
                  </dd>
                </div>
              </dl>
              <DitherArea
                title="CPU"
                subtitle={last ? `share of ${last.cpu_count} host CPUs` : 'share of the host CPUs'}
                samples={samples}
                value={cpuValue}
                axis={cpuAxis(cpuPeak)}
                tickLabel={percentTick}
                formatValue={formatCpu}
                intervalS={5}
                emptyText={COLLECTING}
              />
              <DitherArea
                title="Memory"
                subtitle="host memory in use, MiB"
                samples={samples}
                value={memoryValue}
                axis={capacityAxis(total / MIB)}
                tickLabel={plainTick}
                formatValue={formatMemory}
                intervalS={5}
                limit={total > 0 ? { value: total / MIB, label: `total ${formatBytes(total)}` } : null}
                emptyText={COLLECTING}
              />
            </div>
          </div>
        </DialogPrimitive.Content>
      </DialogPrimitive.Portal>
    </DialogPrimitive.Root>
  );
}

const cpuValue = (sample: NodeMetricSample): number => sample.cpu_usage * 100;
const formatCpu = (percent: number): string => formatPercent(percent / 100);
const memoryValue = (sample: NodeMetricSample): number => sample.memory_used_bytes / MIB;
const formatMemory = (mib: number): string => formatBytes(mib * MIB);
