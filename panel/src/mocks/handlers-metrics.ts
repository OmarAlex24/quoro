import { HttpResponse, http } from 'msw';
import type {
  AppMetrics,
  Node,
  NodeMetricSample,
  NodeMetrics,
  Vm,
  VmMetricSample,
} from '../api/generated/model';
import { noise, rng } from '../lib/dither';
import { formatSseFrame } from '../lib/log-buffer';
import { db, findApp, now } from './db';
import { json, problem, signedIn, unauthorized } from './http';

/** Mock-mode CPU and memory: a readable shape, no real host behind it. */

const MIB = 1024 * 1024;
const INTERVAL_MS = 5_000;
/** The node keeps ten minutes at 5 s (D-0129): 120 samples. */
const HISTORY_COUNT = 120;

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

interface LiveVmState {
  cpu: number;
  memoryFraction: number;
}

/** Where each VM's live samples carry on from: the end of its history. */
const liveVms = new Map<string, LiveVmState>();

/**
 * Ten minutes of a VM: CPU wandering around a quarter of its vCPUs with a
 * busier stretch near the end, and resident memory creeping up the way a
 * VMM's does (it never gives touched pages back). A VM named `…-2` lost half
 * a minute of samples, so the chart has a gap to show.
 */
const vmHistory = (vm: Vm): VmMetricSample[] => {
  const wobble = noise(`cpu-${vm.id}`, 26);
  const jitter = rng(`jitter-${vm.id}`);
  const creep = noise(`mem-${vm.id}`, 12);
  const end = Date.now();
  const samples: VmMetricSample[] = [];
  let cpu = 0.24;
  let memoryFraction = 0.73;
  for (let index = 0; index < HISTORY_COUNT; index += 1) {
    const at = index / (HISTORY_COUNT - 1);
    const busy = index > 78 && index < 92 ? 0.16 : 0;
    cpu = clamp(0.1 + 0.3 * wobble(at) + busy + (jitter() - 0.5) * 0.05, 0.04, 0.95);
    memoryFraction = clamp(0.72 + 0.07 * at + 0.012 * (creep(at) - 0.5), 0.5, 0.95);
    if (vm.name.endsWith('-2') && index >= 64 && index < 70) continue;
    const time = new Date(end - (HISTORY_COUNT - 1 - index) * INTERVAL_MS).toISOString();
    samples.push(vmSample(vm, time, cpu, vmMemoryBytes(vm) * memoryFraction));
  }
  liveVms.set(vm.id, { cpu, memoryFraction });
  return samples;
};

interface AppStreamSample extends VmMetricSample {
  vm_id: string;
  node_id: string;
}

const runningInstances = (appId: string): Vm[] =>
  db.vms.filter(
    (vm) => vm.app_id === appId && vm.observed_status === 'running',
  );

const nextAppSamples = (appId: string): AppStreamSample[] =>
  runningInstances(appId).map((vm) => {
    const state = liveVms.get(vm.id) ?? { cpu: 0.24, memoryFraction: 0.79 };
    state.cpu = clamp(state.cpu + (Math.random() - 0.5) * 0.05 + (0.24 - state.cpu) * 0.08, 0.04, 0.95);
    state.memoryFraction = clamp(state.memoryFraction + (Math.random() - 0.4) * 0.002, 0.5, 0.95);
    liveVms.set(vm.id, state);
    return {
      ...vmSample(vm, now(), state.cpu, vmMemoryBytes(vm) * state.memoryFraction),
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
