import type { ReactNode } from 'react';
import type { VmMetricSample } from '../api/generated/model';
import type { LampTone } from '../lib/lamp';
import {
  cpuDomain,
  formatBytes,
  formatPercent,
  memoryDomain,
} from '../lib/metrics';
import {
  useAppMetricsStream,
  useNodeMetricsStream,
  type MetricsStreamStatus,
} from '../lib/use-metrics-stream';
import { cn } from '../lib/utils';
import { MetricChart } from './metric-chart';
import { ToneLamp } from './tone-lamp';

interface StatusReadout {
  tone: LampTone;
  words: string;
  pulse: boolean;
}

const STATUS_READOUT: Record<MetricsStreamStatus, StatusReadout> = {
  connecting: { tone: 'shift', words: 'Connecting', pulse: true },
  live: { tone: 'run', words: 'Live', pulse: false },
  reconnecting: { tone: 'shift', words: 'Reconnecting', pulse: true },
  unreachable: { tone: 'fault', words: 'Node unreachable', pulse: false },
  error: { tone: 'fault', words: 'Stream stopped', pulse: false },
};

const COLLECTING = 'Collecting samples — first one within 5 s';

/** The short form of a vm id, enough to tell two instances apart. */
const shortVmId = (id: string): string =>
  id.length > 12 ? `${id.slice(0, 10)}…` : id;

function MetricsBayCard({
  status,
  className,
  children,
}: {
  status: MetricsStreamStatus;
  className?: string;
  children: ReactNode;
}) {
  const readout = STATUS_READOUT[status];
  return (
    <section
      className={cn(
        'rounded-[6px] border border-rail bg-panel px-4 py-4 sm:px-5',
        className,
      )}
    >
      <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
        <h2 className="type-bay-title text-[15px] leading-none text-ink">Metrics</h2>
        <span className="type-data text-[12px] text-ink-muted">Last 10 minutes</span>
        <div className="ml-auto flex items-center gap-2">
          <ToneLamp
            tone={readout.tone}
            pulse={readout.pulse}
            size={14}
            label={readout.words}
          />
          <span className="type-body-medium text-[13px] text-ink">{readout.words}</span>
        </div>
      </div>
      {children}
    </section>
  );
}

const noticeFor = (
  status: MetricsStreamStatus,
  problem: { message: string } | null,
): string | null => {
  if (status === 'live') return null;
  if (problem) return problem.message;
  if (status === 'unreachable') return 'The node is not reachable right now.';
  return null;
};

const lastOf = <T,>(samples: readonly T[]): T | undefined =>
  samples.length === 0 ? undefined : samples[samples.length - 1];

/** A node's CPU and memory, live from the node itself. */
export function NodeMetricsBay({
  nodeId,
  enabled = true,
  className,
}: {
  nodeId: string;
  enabled?: boolean;
  className?: string;
}) {
  const { samples, status, problem } = useNodeMetricsStream(nodeId, enabled);
  const last = lastOf(samples);
  const total = samples.reduce(
    (highest, sample) => Math.max(highest, sample.memory_total_bytes),
    0,
  );
  const notice = noticeFor(status, problem);

  return (
    <MetricsBayCard status={status} className={className}>
      <div className="mt-3 grid gap-x-6 gap-y-4 sm:grid-cols-2">
        <MetricChart
          title="CPU"
          currentText={last ? formatPercent(last.cpu_usage) : '—'}
          samples={samples}
          value={(sample) => sample.cpu_usage}
          domain={cpuDomain(samples)}
          formatValue={formatPercent}
          tone="cobalt"
          emptyText={COLLECTING}
          notice={notice}
        />
        <MetricChart
          title="Memory"
          currentText={
            last
              ? `${formatBytes(last.memory_used_bytes)} of ${formatBytes(last.memory_total_bytes)}`
              : '—'
          }
          samples={samples}
          value={(sample) => sample.memory_used_bytes}
          domain={memoryDomain(total)}
          formatValue={formatBytes}
          tone="ink"
          emptyText={COLLECTING}
          notice={notice}
        />
      </div>
    </MetricsBayCard>
  );
}

/** An app's instances, each with its own pair of charts. */
export function AppMetricsBay({
  appId,
  nodeNames,
  className,
}: {
  appId: string;
  nodeNames?: Record<string, string>;
  className?: string;
}) {
  const { instances, status, problem } = useAppMetricsStream(appId, appId !== '');
  const notice = noticeFor(status, problem);

  return (
    <MetricsBayCard status={status} className={className}>
      {instances.length === 0 ? (
        <p className="mt-3 text-[13px] leading-snug text-ink-muted">
          No instance is reporting metrics. They appear when the app has a
          running instance.
        </p>
      ) : (
        <div className="mt-3 flex flex-col">
          {instances.map((instance, index) => {
            const last = lastOf(instance.samples);
            const total = instance.samples.reduce(
              (highest: number, sample: VmMetricSample) =>
                Math.max(highest, sample.memory_bytes),
              0,
            );
            return (
              <div
                key={instance.vm_id}
                className={cn(index > 0 && 'mt-4 border-t border-rail pt-4')}
              >
                <div className="flex flex-wrap items-baseline gap-x-2">
                  <h3 className="type-data text-[13px] text-ink">
                    {shortVmId(instance.vm_id)}
                  </h3>
                  <span className="text-[12px] text-ink-muted">
                    {nodeNames?.[instance.node_id] ?? 'node'}
                  </span>
                </div>
                <div className="mt-2 grid gap-x-6 gap-y-4 sm:grid-cols-2">
                  <MetricChart
                    title="CPU"
                    currentText={last ? formatPercent(last.cpu_usage) : '—'}
                    samples={instance.samples}
                    value={(sample) => sample.cpu_usage}
                    domain={cpuDomain(instance.samples, true)}
                    formatValue={formatPercent}
                    tone="cobalt"
                    emptyText={COLLECTING}
                    notice={notice}
                  />
                  <MetricChart
                    title="Memory"
                    currentText={
                      last
                        ? `${formatBytes(last.memory_rss_bytes)} of ${formatBytes(last.memory_bytes)}`
                        : '—'
                    }
                    samples={instance.samples}
                    value={(sample) => sample.memory_rss_bytes}
                    domain={memoryDomain(total)}
                    formatValue={formatBytes}
                    tone="ink"
                    emptyText={COLLECTING}
                    notice={notice}
                  />
                </div>
              </div>
            );
          })}
        </div>
      )}
    </MetricsBayCard>
  );
}
