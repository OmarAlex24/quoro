import { HttpResponse, http } from 'msw';
import type {
  AppMetrics,
  Node,
  NodeMetricSample,
  NodeMetrics,
  Vm,
  VmMetricSample,
} from '../api/generated/model';
import { formatSseFrame } from '../lib/log-buffer';
import { db, findApp, now } from './db';
import { json, problem, signedIn, unauthorized } from './http';

/** Mock-mode CPU and memory: a readable shape, no real host behind it. */

const MIB = 1024 * 1024;
const INTERVAL_MS = 5_000;
const HISTORY_COUNT = 20;

const clamp = (value: number, min: number, max: number): number =>
  Math.min(max, Math.max(min, value));

const nodeMemoryTotal = (node: Node): number => node.capacity.memory_mb * MIB;

const nodeSample = (
  node: Node,
  time: string,
  cpu: number,
  used: number,
): NodeMetricSample => ({
  time,
  cpu_usage: clamp(cpu, 0, 1),
  cpu_count: node.capacity.cpus,
  memory_total_bytes: nodeMemoryTotal(node),
  memory_used_bytes: Math.round(clamp(used, 0, nodeMemoryTotal(node))),
});

const nodeHistory = (node: Node): NodeMetricSample[] => {
  const total = nodeMemoryTotal(node);
  return Array.from({ length: HISTORY_COUNT }, (_, index) => {
    const time = new Date(Date.now() - (HISTORY_COUNT - 1 - index) * INTERVAL_MS).toISOString();
    const wave = Math.sin(index / 2.4);
    return nodeSample(
      node,
      time,
      0.26 + wave * 0.11 + (Math.random() - 0.5) * 0.02,
      total * (0.61 + wave * 0.025),
    );
  });
};

interface LiveNodeState {
  cpu: number;
  memoryFraction: number;
}

const liveNodes = new Map<string, LiveNodeState>();

const nextNodeSample = (node: Node): NodeMetricSample => {
  const state = liveNodes.get(node.id) ?? { cpu: 0.27, memoryFraction: 0.61 };
  state.cpu = clamp(state.cpu + (Math.random() - 0.5) * 0.05, 0.03, 0.92);
  state.memoryFraction = clamp(
    state.memoryFraction + (Math.random() - 0.5) * 0.004,
    0.48,
    0.78,
  );
  liveNodes.set(node.id, state);
  return nodeSample(node, now(), state.cpu, nodeMemoryTotal(node) * state.memoryFraction);
};

const vmMemoryBytes = (vm: Vm): number => vm.memory_mb * MIB;

const vmSample = (vm: Vm, time: string, cpu: number, rss: number): VmMetricSample => ({
  time,
  cpu_usage: clamp(cpu, 0, 1.4),
  vcpus: vm.vcpus,
  memory_rss_bytes: Math.round(clamp(rss, 0, vmMemoryBytes(vm))),
  memory_bytes: vmMemoryBytes(vm),
});

const vmHistory = (vm: Vm): VmMetricSample[] =>
  Array.from({ length: HISTORY_COUNT }, (_, index) => {
    const time = new Date(Date.now() - (HISTORY_COUNT - 1 - index) * INTERVAL_MS).toISOString();
    const wave = Math.sin(index / 3.1);
    return vmSample(
      vm,
      time,
      0.3 + wave * 0.12 + (Math.random() - 0.5) * 0.03,
      vmMemoryBytes(vm) * (0.8 + wave * 0.02),
    );
  });

interface AppStreamSample extends VmMetricSample {
  vm_id: string;
  node_id: string;
}

const runningInstances = (appId: string): Vm[] =>
  db.vms.filter(
    (vm) => vm.app_id === appId && vm.observed_status === 'running',
  );

const nextAppSamples = (appId: string): AppStreamSample[] =>
  runningInstances(appId).map((vm, index) => {
    const wave = Math.sin(Date.now() / 9_000 + index * 1.7);
    return {
      ...vmSample(
        vm,
        now(),
        0.32 + wave * 0.11 + (Math.random() - 0.5) * 0.04,
        vmMemoryBytes(vm) * (0.81 + wave * 0.02),
      ),
      vm_id: vm.id,
      node_id: vm.node_id,
    };
  });

/** A `text/event-stream` that writes one `sample` event per interval. */
const sampleStream = (
  next: () => unknown[],
  intervalMs = 2_000,
): Response => {
  const encoder = new TextEncoder();
  let timer: ReturnType<typeof setInterval> | undefined;

  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      const emit = () => {
        for (const sample of next()) {
          controller.enqueue(
            encoder.encode(
              formatSseFrame({ event: 'sample', data: JSON.stringify(sample) }),
            ),
          );
        }
      };
      emit();
      timer = setInterval(emit, intervalMs);
    },
    cancel() {
      if (timer !== undefined) clearInterval(timer);
    },
  });

  return new HttpResponse(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
    },
  });
};

const nodeUnreachable = () =>
  problem(504, 'node_unreachable', {
    detail: 'The node is not connected, so its metrics are unavailable.',
  });

const nodeKnown = (nodeId: string): Node | undefined => {
  if (nodeId === db.node.id) return db.node;
  return db.extraNodes.find((node) => node.id === nodeId);
};

export const metricsHandlers = [
  http.get('*/api/v1/nodes/:nodeId/metrics', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const node = nodeKnown(String(params.nodeId));
    if (!node) {
      return problem(404, 'not_found', { detail: 'That node no longer exists.' });
    }
    if (node.status !== 'connected') return nodeUnreachable();
    const body: NodeMetrics = {
      node_id: node.id,
      interval_s: 5,
      samples: nodeHistory(node),
    };
    return json(body);
  }),

  http.get('*/api/v1/nodes/:nodeId/metrics/stream', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const node = nodeKnown(String(params.nodeId));
    if (!node) {
      return problem(404, 'not_found', { detail: 'That node no longer exists.' });
    }
    if (node.status !== 'connected') return nodeUnreachable();
    return sampleStream(() => [nextNodeSample(node)]);
  }),

  http.get('*/api/v1/apps/:appId/metrics', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }
    if (db.node.status !== 'connected') return nodeUnreachable();

    const body: AppMetrics = {
      app_id: app.id,
      interval_s: 5,
      instances: db.vms
        .filter((vm) => vm.app_id === app.id)
        .map((vm) => ({
          vm_id: vm.id,
          node_id: vm.node_id,
          samples: vm.observed_status === 'running' ? vmHistory(vm) : [],
        })),
    };
    return json(body);
  }),

  http.get('*/api/v1/apps/:appId/metrics/stream', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }
    if (db.node.status !== 'connected') return nodeUnreachable();
    return sampleStream(() => nextAppSamples(app.id));
  }),
];
