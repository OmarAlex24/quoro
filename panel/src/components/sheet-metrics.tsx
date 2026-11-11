// SPDX-License-Identifier: AGPL-3.0-only
import { useState, type KeyboardEvent } from 'react';
import type { AppDetail, VmMetricSample } from '../api/generated/model';
import {
  cpuAxis,
  memoryFigures,
  niceAxis,
  percentTick,
  plainTick,
  streamReadout,
  vcpuShareWords,
} from '../lib/metric-area';
import { formatBytes, formatPercent } from '../lib/metrics';
import { useAppMetricsStream, type AppInstanceSeries, type MetricsStreamStatus } from '../lib/use-metrics-stream';
import type { ProblemView } from '../lib/problem';
import { DitherArea } from './dither-area';
import { LampDot } from './lamp';
import { EmptyState } from './empty-state';
import { SkeletonCards } from './skeleton';

const MIB = 1024 * 1024;

/** The node samples every 5 s; used until the history read says otherwise. */
const DEFAULT_INTERVAL_S = 5;

const COLLECTING = 'Collecting samples — the first one arrives within 5 s';

/** The short form of a vm id, for an instance the app no longer lists. */
const shortVmId = (id: string): string => (id.length > 12 ? `${id.slice(0, 10)}…` : id);

const lastOf = <T,>(items: readonly T[]): T | undefined => items[items.length - 1];

/**
 * The Metrics tab: one instance at a time, picked above the charts, with its
 * figures above two charts, CPU and memory as dithered areas over the ten
 * minutes its node keeps. Each chart reads its exact value under the pointer
 * or the arrow keys, as the node cards' do. History comes from
 * `getAppMetrics` and new samples from its stream, each one redrawn as it
 * lands; there is no range to pick because there is no more data.
 */
export function MetricsTab({ app, nodeNames }: { app: AppDetail; nodeNames: Record<string, string> }) {
  const { instances, intervalS, status, problem } = useAppMetricsStream(app.id, app.id !== '');
  const [picked, setPicked] = useState<string | null>(null);
  const current = instances.find((instance) => instance.vm_id === picked) ?? instances[0];
  const vmOf = (vmId: string) => app.instances.find((vm) => vm.id === vmId);
  const labelOf = (instance: AppInstanceSeries) => {
    const name = vmOf(instance.vm_id)?.name ?? shortVmId(instance.vm_id);
    return `${name} · ${nodeNames[instance.node_id] ?? 'node'}`;
  };

  return (
    <>
      <div className="mtop">
        {instances.length > 1 ? (
          <InstancePicker
            instances={instances}
            current={current?.vm_id ?? null}
            labelOf={labelOf}
            onPick={setPicked}
          />
        ) : current ? (
          <span className="mtop-one">{labelOf(current)}</span>
        ) : null}
        <span className="flex-1" />
        <StreamState status={status} />
      </div>
      <StreamNotice status={status} problem={problem} />

      {current === undefined && status === 'connecting' ? (
        <SkeletonCards label="Loading the app’s instances" count={2} height={150} />
      ) : current === undefined ? (
        <EmptyState title="No instance is reporting metrics">
          CPU and memory of each instance appear here when the app has a running instance.
        </EmptyState>
      ) : (
        <InstanceCharts
          key={current.vm_id}
          samples={current.samples}
          intervalS={intervalS ?? DEFAULT_INTERVAL_S}
          restarts={vmOf(current.vm_id)?.restart_count ?? null}
          node={nodeNames[current.node_id] ?? 'Unknown node'}
        />
      )}
    </>
  );
}

function StreamState({ status }: { status: MetricsStreamStatus }) {
  const readout = streamReadout(status);
  return (
    <span className="live-chip">
      <LampDot ring={readout.tone} core={readout.tone} pulse={readout.pulse} size={9} />
      {readout.words}
    </span>
  );
}

function StreamNotice({ status, problem }: { status: MetricsStreamStatus; problem: ProblemView | null }) {
  if (status === 'live' || status === 'connecting') return null;
  const words = problem?.message ?? (status === 'unreachable' ? 'The node is not reachable right now.' : null);
  if (!words) return null;
  return <p className="text-[12.5px] leading-snug text-fault">{words}</p>;
}

/**
 * Instances as a segmented choice: one radio per instance, the arrow keys
 * moving between them as in any radio group.
 */
function InstancePicker({
  instances,
  current,
  labelOf,
  onPick,
}: {
  instances: readonly AppInstanceSeries[];
  current: string | null;
  labelOf: (instance: AppInstanceSeries) => string;
  onPick: (vmId: string) => void;
}) {
  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    const step = event.key === 'ArrowRight' || event.key === 'ArrowDown' ? 1 : event.key === 'ArrowLeft' || event.key === 'ArrowUp' ? -1 : 0;
    if (step === 0) return;
    event.preventDefault();
    const at = instances.findIndex((instance) => instance.vm_id === current);
    const next = instances[(at + step + instances.length) % instances.length];
    if (!next) return;
    onPick(next.vm_id);
    event.currentTarget.querySelector<HTMLButtonElement>(`[data-vm="${next.vm_id}"]`)?.focus();
  };
  return (
    <div role="radiogroup" aria-label="Instance" className="seg seg-wrap" onKeyDown={onKeyDown}>
      {instances.map((instance) => {
        const on = instance.vm_id === current;
        return (
          <button
            key={instance.vm_id}
            type="button"
            role="radio"
            aria-checked={on}
            tabIndex={on ? 0 : -1}
            data-vm={instance.vm_id}
            onClick={() => onPick(instance.vm_id)}
          >
            {labelOf(instance)}
          </button>
        );
      })}
    </div>
  );
}

function InstanceCharts({
  samples,
  intervalS,
  restarts,
  node,
}: {
  samples: readonly VmMetricSample[];
  intervalS: number;
  restarts: number | null;
  node: string;
}) {
  const last = lastOf(samples);
  const cpuPeak = samples.reduce((peak, sample) => Math.max(peak, sample.cpu_usage * 100), 0);
  const memoryTotal = samples.reduce((top, sample) => Math.max(top, sample.memory_bytes), 0);
  const memoryPeak = samples.reduce((top, sample) => Math.max(top, sample.memory_rss_bytes), 0);
  const memory = last ? memoryFigures(last.memory_rss_bytes, last.memory_bytes) : null;

  return (
    <>
      <dl className="stats">
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
        <div>
          <dt>Restarts</dt>
          <dd>{restarts ?? '—'}</dd>
        </div>
        <div>
          <dt>Node</dt>
          <dd title={node}>{node}</dd>
        </div>
      </dl>

      <DitherArea
        compact
        timeAxis
        className="chart-sheet"
        title="CPU"
        subtitle={last ? vcpuShareWords(last.vcpus) : 'share of the VM’s vCPUs'}
        reading={last ? formatCpu(cpuValue(last)) : '—'}
        samples={samples}
        value={cpuValue}
        axis={cpuAxis(cpuPeak)}
        tickLabel={percentTick}
        formatValue={formatCpu}
        intervalS={intervalS}
        emptyText={COLLECTING}
      />
      <DitherArea
        compact
        timeAxis
        className="chart-sheet"
        title="Memory"
        subtitle="VMM resident memory, MiB"
        reading={last ? formatMemory(memoryValue(last)) : '—'}
        samples={samples}
        value={memoryValue}
        axis={niceAxis(Math.max(memoryTotal, memoryPeak) / MIB)}
        tickLabel={plainTick}
        formatValue={formatMemory}
        intervalS={intervalS}
        limit={memoryTotal > 0 ? { value: memoryTotal / MIB, label: `limit ${formatBytes(memoryTotal)}` } : null}
        emptyText={COLLECTING}
      />
    </>
  );
}

const cpuValue = (sample: VmMetricSample): number => sample.cpu_usage * 100;
const formatCpu = (percent: number): string => formatPercent(percent / 100);
const memoryValue = (sample: VmMetricSample): number => sample.memory_rss_bytes / MIB;
const formatMemory = (mib: number): string => formatBytes(mib * MIB);
