// SPDX-License-Identifier: AGPL-3.0-only
import type {
  App,
  Backup,
  CapacityShortfall,
  ClusterCapacity,
  ClusterCapacityApp,
  Node,
  NodeCapacity,
  Vm,
  Volume,
} from '../api/generated/model';

/*
 * Mock mode only: what the primary answers to `getClusterCapacity`, worked
 * out the way openapi.yaml describes it, from the mock cluster's own state.
 * Pure, so the "fits" and "does not fit" answers the screenshots show can be
 * tested.
 */

/** What a node has given its VMs: the sum of the VMs placed on it. */
export const allocatedFrom = (nodeId: string, vms: readonly Vm[]): NodeCapacity =>
  vms
    .filter((vm) => vm.node_id === nodeId && vm.observed_status !== 'deleted')
    .reduce(
      (sum, vm) => ({
        cpus: sum.cpus + vm.vcpus,
        memory_mb: sum.memory_mb + vm.memory_mb,
        disk_gb: sum.disk_gb + vm.disk_gb,
      }),
      { cpus: 0, memory_mb: 0, disk_gb: 0 },
    );

const ACTIVE_VOLUME: ReadonlySet<Volume['state']> = new Set(['creating', 'ready', 'restoring']);

interface Need {
  app: App;
  action: Exclude<ClusterCapacityApp['action'], 'keep'>;
  /** The only node it may go to: a warm app's warm node. */
  only?: string;
}

/**
 * Simulate the loss of `lostId`: every app with an instance meant to run
 * there, or a volume there. A stateless app that keeps another instance
 * loses one replica; one whose only instance was there needs a new one; a
 * `warm` app starts on its warm node; a stateful `none` or `cold` app needs a
 * restore of its latest succeeded backup. They are placed greedily, by memory,
 * then vCPUs, then disk, largest first, against each remaining connected node
 * with the `node` role, within the app's `placement.node_ids`, on the node with
 * the most memory left.
 */
export function simulateLoss({
  lostId,
  nodes,
  apps,
  vms,
  volumes,
  backups,
  now,
}: {
  lostId: string;
  /** Nodes with `allocated` as it stands. */
  nodes: readonly Node[];
  apps: readonly App[];
  vms: readonly Vm[];
  volumes: readonly Volume[];
  backups: readonly Backup[];
  now: string;
}): ClusterCapacity {
  const remaining = nodes.filter(
    (node) => node.id !== lostId && node.status === 'connected' && node.roles.includes('node'),
  );
  const free = new Map(
    remaining.map((node) => [
      node.id,
      {
        cpus: node.capacity.cpus - node.allocated.cpus,
        memory_mb: node.capacity.memory_mb - node.allocated.memory_mb,
        disk_gb: node.capacity.disk_gb - node.allocated.disk_gb,
      },
    ]),
  );

  const kept: ClusterCapacityApp[] = [];
  const settled: ClusterCapacityApp[] = [];
  const needs: Need[] = [];

  for (const app of apps) {
    const instances = vms.filter((vm) => vm.app_id === app.id && vm.observed_status !== 'deleted');
    const onLost = instances.some((vm) => vm.node_id === lostId);
    const volumeThere = volumes.some(
      (volume) => volume.app_id === app.id && volume.node_id === lostId && ACTIVE_VOLUME.has(volume.state),
    );
    if (!onLost && !volumeThere) continue;

    // An app whose data is not on the lost node, with an instance elsewhere,
    // only loses a replica.
    const elsewhere = instances.find((vm) => vm.node_id !== lostId);
    if (!app.stateful || !volumeThere) {
      if (elsewhere) {
        kept.push({ app_id: app.id, policy: app.ha_policy, action: 'keep', node_id: elsewhere.node_id, reason: null });
        continue;
      }
      if (!app.stateful) {
        needs.push({ app, action: 'start' });
        continue;
      }
    }
    if (app.ha_policy === 'warm') {
      const warm = app.warm?.node_id ?? null;
      if (warm === null || app.warm?.last_snapshot_seq == null) {
        settled.push({ app_id: app.id, policy: 'warm', action: 'failover', node_id: null, reason: 'no_warm_copy' });
      } else {
        needs.push({ app, action: 'failover', only: warm });
      }
    } else if (!backups.some((backup) => backup.app_id === app.id && backup.status === 'succeeded')) {
      settled.push({ app_id: app.id, policy: app.ha_policy, action: 'restore', node_id: null, reason: 'no_backup' });
    } else {
      needs.push({ app, action: 'restore' });
    }
  }

  needs.sort(
    (left, right) =>
      right.app.resources.memory_mb - left.app.resources.memory_mb ||
      right.app.resources.vcpus - left.app.resources.vcpus ||
      right.app.resources.disk_gb - left.app.resources.disk_gb,
  );

  const missing: CapacityShortfall = { cpus: 0, memory_mb: 0, disk_gb: 0 };
  const placed: ClusterCapacityApp[] = [];
  for (const need of needs) {
    const { resources, placement } = need.app;
    // A warm copy already holds the disk on its node; it needs memory and vCPUs there.
    const disk = need.action === 'failover' ? 0 : resources.disk_gb;
    const allowed = remaining.filter((node) =>
      need.only !== undefined
        ? node.id === need.only
        : placement.node_ids.length === 0 || placement.node_ids.includes(node.id),
    );
    const fitting = allowed
      .map((node) => ({ node, room: free.get(node.id)! }))
      .filter(
        ({ room }) => room.cpus >= resources.vcpus && room.memory_mb >= resources.memory_mb && room.disk_gb >= disk,
      )
      .sort((left, right) => right.room.memory_mb - left.room.memory_mb)[0];
    if (!fitting) {
      missing.cpus += resources.vcpus;
      missing.memory_mb += resources.memory_mb;
      missing.disk_gb += disk;
      placed.push({ app_id: need.app.id, policy: need.app.ha_policy, action: need.action, node_id: null, reason: 'no_capacity' });
      continue;
    }
    fitting.room.cpus -= resources.vcpus;
    fitting.room.memory_mb -= resources.memory_mb;
    fitting.room.disk_gb -= disk;
    placed.push({ app_id: need.app.id, policy: need.app.ha_policy, action: need.action, node_id: fitting.node.id, reason: null });
  }

  const fits = !placed.some((entry) => entry.reason === 'no_capacity');
  return {
    lost_node_id: lostId,
    fits,
    missing: fits ? { cpus: 0, memory_mb: 0, disk_gb: 0 } : missing,
    apps: [...kept, ...placed, ...settled],
    computed_at: now,
  };
}
