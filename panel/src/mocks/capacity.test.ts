// SPDX-License-Identifier: AGPL-3.0-only
import { describe, expect, it } from 'vitest';
import { allocatedFrom, simulateLoss } from './capacity';
import { db } from './db';

/** The seeded cluster as the nodes handler reports it. */
const nodes = () => [db.node, ...db.extraNodes].map((node) => ({ ...node, allocated: allocatedFrom(node.id, db.vms) }));

const lose = (name: string) => {
  const all = nodes();
  const lost = all.find((node) => node.name === name)!;
  return simulateLoss({
    lostId: lost.id,
    nodes: all,
    apps: db.apps,
    vms: db.vms,
    volumes: db.volumes,
    backups: db.backups,
    now: '2026-11-07T00:00:00Z',
  });
};

const nameOf = (appId: string) => db.apps.find((app) => app.id === appId)?.name;

describe('the mock N+1 answer', () => {
  it('reports what the VMs placed on a node add up to', () => {
    const lab3 = nodes().find((node) => node.name === 'lab-3')!;
    const expected = db.vms
      .filter((vm) => vm.node_id === lab3.id)
      .reduce((sum, vm) => sum + vm.memory_mb, 0);
    expect(lab3.allocated.memory_mb).toBe(expected);
    expect(lab3.allocated.memory_mb).toBeGreaterThan(0);
  });

  it('losing lab-3 fits: replicas keep running, five apps move to lab-4 and three have no complete warm copy', () => {
    const answer = lose('lab-3');
    const lab4 = db.extraNodes[0]!.id;
    expect(answer.fits).toBe(true);
    expect(answer.missing).toEqual({ cpus: 0, memory_mb: 0, disk_gb: 0 });
    const moved = answer.apps.filter((app) => app.action !== 'keep' && app.reason === null);
    expect(moved.map((app) => nameOf(app.app_id)).sort()).toEqual([
      'analytics',
      'blog',
      'docs-site',
      'media',
      'orders',
    ]);
    expect(moved.every((app) => app.node_id === lab4)).toBe(true);
    // The Harbor warm apps whose copy is complete fail over to it on lab-4.
    expect(
      moved.filter((app) => app.action === 'failover').map((app) => nameOf(app.app_id)).sort(),
    ).toEqual(['analytics', 'media', 'orders']);
    // The ones with no complete copy yet have nowhere to start.
    const stuck = answer.apps.filter((app) => app.reason === 'no_warm_copy');
    expect(stuck.map((app) => nameOf(app.app_id)).sort()).toEqual(['billing', 'inventory', 'search']);
    expect(stuck.every((app) => app.action === 'failover' && app.node_id === null)).toBe(true);
    expect(answer.apps.filter((app) => app.action === 'keep').map((app) => nameOf(app.app_id)).sort()).toEqual([
      'api',
      'web',
    ]);
  });

  it('losing lab-4 does not fit: the apps pinned to it have nowhere to go', () => {
    const answer = lose('lab-4');
    expect(answer.fits).toBe(false);
    const stranded = answer.apps.filter((app) => app.reason === 'no_capacity');
    expect(stranded.map((app) => nameOf(app.app_id)).sort()).toEqual(['postgres', 'worker']);
    expect(answer.missing).toEqual({ cpus: 2, memory_mb: 1536, disk_gb: 4 });
  });
});
