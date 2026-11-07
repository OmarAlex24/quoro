import { useEffect, useRef, useState } from 'react';
import type {
  AppInstanceMetrics,
  NodeMetricSample,
  NodeMetrics,
  VmMetricSample,
} from '../api/generated/model';
import { getAppMetrics, getNodeMetrics } from '../api/generated/metrics/metrics';
import { isApiProblem } from '../api/fetcher';
import { isTransientProblem, problemViewFromEvent } from './log-buffer';
import { mergeSamples } from './metrics';
import { problemView, type ProblemView } from './problem';

/** How the live bay reads: a socket that is opening, live, or retrying. */
export type MetricsStreamStatus =
  | 'connecting'
  | 'live'
  | 'reconnecting'
  | 'unreachable'
  | 'error';

interface MetricsCoreOptions<S extends { time: string }, M> {
  url: string;
  enabled: boolean;
  /** Read the history a reconnect has to merge the live samples back into. */
  loadHistory: () => Promise<{ samples: S[]; meta: M | null }>;
  /** The identity of a sample inside the window; defaults to its time. */
  keyOf: (sample: S) => string;
}

interface MetricsCore<S, M> {
  samples: S[];
  meta: M | null;
  status: MetricsStreamStatus;
  problem: ProblemView | null;
}

/**
 * One EventSource plus the history read it is merged into. The browser retries
 * a dropped stream by itself; when it gives up for good (a restarted control
 * plane, a 504 from an offline node) this opens a new source with its own
 * backoff and re-reads the history, because the stream's events carry no `id:`
 * and cannot be resumed.
 */
function useMetricsCore<S extends { time: string }, M>(
  options: MetricsCoreOptions<S, M>,
): MetricsCore<S, M> {
  const optionsRef = useRef(options);
  optionsRef.current = options;
  const { url, enabled } = options;

  const [samples, setSamples] = useState<S[]>([]);
  const [meta, setMeta] = useState<M | null>(null);
  const [status, setStatus] = useState<MetricsStreamStatus>('connecting');
  const [problem, setProblem] = useState<ProblemView | null>(null);

  useEffect(() => {
    setSamples([]);
    setMeta(null);
    setProblem(null);
    setStatus('connecting');

    if (!enabled) return;

    let source: EventSource | null = null;
    let retryTimer: ReturnType<typeof setTimeout> | undefined;
    let retryDelay = 1_000;
    let disposed = false;

    const refreshHistory = async () => {
      try {
        const history = await optionsRef.current.loadHistory();
        if (disposed) return;
        setSamples((current) =>
          mergeSamples(current, history.samples, Date.now(), optionsRef.current.keyOf),
        );
        setMeta(history.meta);
      } catch (error) {
        if (disposed) return;
        const view = problemView(error, 'read metrics');
        setProblem(view);
        setStatus(
          isApiProblem(error) && error.code === 'node_unreachable'
            ? 'unreachable'
            : 'error',
        );
      }
    };

    const onOpen = () => {
      retryDelay = 1_000;
      setStatus('live');
      setProblem(null);
    };

    const onSample = (event: Event) => {
      const message = event as MessageEvent<string>;
      try {
        const sample = JSON.parse(message.data) as S;
        setSamples((current) =>
          mergeSamples(current, [sample], Date.now(), optionsRef.current.keyOf),
        );
        setStatus('live');
        setProblem(null);
      } catch {
        // A malformed frame is dropped; the stream itself keeps going.
      }
    };

    const close = () => {
      if (!source) return;
      source.removeEventListener('open', onOpen);
      source.removeEventListener('sample', onSample);
      source.removeEventListener('error', onError);
      source.close();
      source = null;
    };

    const scheduleConnect = () => {
      retryTimer = setTimeout(() => {
        void refreshHistory();
        connect();
      }, retryDelay);
      retryDelay = Math.min(retryDelay * 2, 15_000);
    };

    function onError(event: Event) {
      if (event instanceof MessageEvent && typeof event.data === 'string' && event.data !== '') {
        const view = problemViewFromEvent(event.data);
        setProblem(view);
        setStatus(view.code === 'node_unreachable' ? 'unreachable' : 'error');
        if (!isTransientProblem(view)) {
          close();
          return;
        }
      }

      if (disposed) return;
      setStatus((current) => (current === 'unreachable' ? current : 'reconnecting'));
      if (source && source.readyState === EventSource.CLOSED) {
        close();
        scheduleConnect();
      }
    }

    function connect() {
      if (disposed) return;
      source = new EventSource(url, { withCredentials: true });
      source.addEventListener('open', onOpen);
      source.addEventListener('sample', onSample);
      source.addEventListener('error', onError);
    }

    void refreshHistory();
    connect();

    return () => {
      disposed = true;
      clearTimeout(retryTimer);
      close();
    };
  }, [url, enabled]);

  return { samples, meta, status, problem };
}

/** A node's recent CPU and memory, live. Disabled nodes never open a stream. */
export function useNodeMetricsStream(
  nodeId: string,
  enabled = true,
): {
  samples: NodeMetricSample[];
  status: MetricsStreamStatus;
  problem: ProblemView | null;
} {
  const core = useMetricsCore<NodeMetricSample, null>({
    url: `/api/v1/nodes/${nodeId}/metrics/stream`,
    enabled: enabled && nodeId !== '',
    loadHistory: async () => {
      const data: NodeMetrics = await getNodeMetrics(nodeId);
      return { samples: data.samples, meta: null };
    },
    keyOf: (sample) => sample.time,
  });

  return { samples: core.samples, status: core.status, problem: core.problem };
}

interface AppStreamSample extends VmMetricSample {
  vm_id: string;
  node_id: string;
}

export interface AppInstanceSeries {
  vm_id: string;
  node_id: string;
  samples: VmMetricSample[];
}

/**
 * The app's instances, each with its own sample window. The stream sends one
 * sample per instance per interval, so all of them ride one EventSource and
 * are grouped by `vm_id` here.
 */
export function useAppMetricsStream(
  appId: string,
  enabled = true,
): {
  instances: AppInstanceSeries[];
  /** Seconds between two samples, as the history read said (`AppMetrics.interval_s`); null before it. */
  intervalS: number | null;
  status: MetricsStreamStatus;
  problem: ProblemView | null;
} {
  const core = useMetricsCore<
    AppStreamSample,
    { instances: AppInstanceMetrics[]; interval_s: number }
  >({
    url: `/api/v1/apps/${appId}/metrics/stream`,
    enabled: enabled && appId !== '',
    loadHistory: async () => {
      const data = await getAppMetrics(appId);
      return {
        samples: data.instances.flatMap((instance) =>
          instance.samples.map((sample) => ({
            ...sample,
            vm_id: instance.vm_id,
            node_id: instance.node_id,
          })),
        ),
        meta: { instances: data.instances, interval_s: data.interval_s },
      };
    },
    keyOf: (sample) => `${sample.vm_id}:${sample.time}`,
  });

  const samplesByVm = new Map<string, VmMetricSample[]>();
  for (const sample of core.samples) {
    const list = samplesByVm.get(sample.vm_id);
    if (list) {
      list.push(sample);
    } else {
      samplesByVm.set(sample.vm_id, [sample]);
    }
  }

  const order: Array<Pick<AppInstanceMetrics, 'vm_id' | 'node_id'>> = [];
  const seen = new Set<string>();
  for (const instance of core.meta?.instances ?? []) {
    if (seen.has(instance.vm_id)) continue;
    seen.add(instance.vm_id);
    order.push({ vm_id: instance.vm_id, node_id: instance.node_id });
  }
  for (const sample of core.samples) {
    if (seen.has(sample.vm_id)) continue;
    seen.add(sample.vm_id);
    order.push({ vm_id: sample.vm_id, node_id: sample.node_id });
  }

  return {
    instances: order.map((instance) => ({
      ...instance,
      samples: samplesByVm.get(instance.vm_id) ?? [],
    })),
    intervalS: core.meta?.interval_s ?? null,
    status: core.status,
    problem: core.problem,
  };
}
