// SPDX-License-Identifier: AGPL-3.0-only
import { describe, expect, it } from 'vitest';
import type { ClusterCapacity, ClusterCapacityApp, Node } from '../api/generated/model';
import { meterDensity, meterEdge, meterMix } from './dither';
import {
  biggestNode,
  capacityShare,
  gibWords,
  loadWords,
  lossCandidates,
  meterTone,
  n1Readout,
  n1Text,
  nodeLoad,
  nodeMeters,
  nodeTags,
  shortfallWords,
} from './node-capacity';

const node = (
  name: string,
  allocated: Node['allocated'],
  capacity: Node['capacity'] = { cpus: 8, memory_mb: 8192, disk_gb: 100 },
  extra: Partial<Node> = {},
): Node => ({
  id: `nod_${name}`,
  name,
  roles: ['node'],
  cp_status: null,
  status: 'connected',
  public_key: 'k',
  address: null,
  behind_nat: false,
  labels: {},
  capacity,
  allocated,
  kvm: { available: true, kernel: '6.12' },
  desired_rev: 1,
  applied_rev: 1,
  log_seq: 1,
  fence_failure: null,
  tunnel: null,
  lost_at: null,
  created_at: '2026-11-07T00:00:00Z',
  ...extra,
});

const app = (id: string, memory_mb: number) => ({ id, resources: { vcpus: 1, memory_mb, disk_gb: 2 } });

const answer = (lost: Node, apps: ClusterCapacityApp[], missing = { cpus: 0, memory_mb: 0, disk_gb: 0 }): ClusterCapacity => ({
  lost_node_id: lost.id,
  fits: !apps.some((entry) => entry.reason === 'no_capacity'),
  missing,
  apps,
  computed_at: '2026-11-07T00:00:00Z',
});

const placed = (
  app_id: string,
  action: ClusterCapacityApp['action'],
  node_id: string | null,
  reason: ClusterCapacityApp['reason'] = null,
): ClusterCapacityApp => ({ app_id, policy: 'none', action, node_id, reason });

const lab1 = node('lab-1', { cpus: 3, memory_mb: 5120, disk_gb: 20 });
const lab2 = node('lab-2', { cpus: 4, memory_mb: 6144, disk_gb: 30 }, undefined, { cp_status: 'primary' });
const lab3 = node('lab-3', { cpus: 1, memory_mb: 1024, disk_gb: 10 }, { cpus: 4, memory_mb: 4096, disk_gb: 60 });

describe('meter fill', () => {
  it('puts the reading point at the share of the width, and clamps it', () => {
    expect(meterEdge(0.25, 200)).toBe(50);
    expect(meterEdge(1.4, 200)).toBe(200);
    expect(meterEdge(-1, 200)).toBe(0);
    expect(meterEdge(Number.NaN, 200)).toBe(0);
  });

  it('is solid up to the last 10 px, thins out to the point, and leaves a faint track', () => {
    const edge = meterEdge(0.5, 200);
    expect(meterDensity(10, edge)).toBe(1);
    expect(meterDensity(89, edge)).toBe(1);
    expect(meterDensity(95, edge)).toBeCloseTo(0.725);
    expect(meterDensity(99.9, edge)).toBeCloseTo(0.4555, 3);
    expect(meterDensity(100, edge)).toBe(0.14);
    expect(meterDensity(180, edge)).toBe(0.14);
  });

  it('draws only the track for an empty meter, and no second tone on it', () => {
    expect(meterDensity(0, 0)).toBe(0.14);
    expect(meterDensity(120, 0)).toBe(0.14);
    expect(meterMix(120, 100)).toBe(0);
  });

  it('gathers the second tone towards the reading point', () => {
    expect(meterMix(0, 100)).toBe(0);
    expect(meterMix(50, 100)).toBeLessThan(meterMix(90, 100));
    expect(meterMix(99, 100)).toBeLessThanOrEqual(0.78);
  });

  it('turns the meter to the fault colour once it is nearly full', () => {
    expect(meterTone(0.89)).toBe('accent');
    expect(meterTone(0.9)).toBe('fault');
    expect(meterTone(1.3)).toBe('fault');
    expect(meterTone(null)).toBe('accent');
  });
});

describe('node meters', () => {
  it('reads reserved over capacity for CPU, memory and disk', () => {
    const meters = nodeMeters({
      capacity: { cpus: 12, memory_mb: 24_576, disk_gb: 200 },
      allocated: { cpus: 8, memory_mb: 10_240, disk_gb: 48 },
    });
    expect(meters.map((meter) => meter.words)).toEqual([
      '8 of 12 reserved',
      '10 of 24 GiB reserved',
      '48 of 200 GiB reserved',
    ]);
    expect(meters[0]!.share).toBeCloseTo(8 / 12);
    expect(meters[1]!.spoken).toBe('Memory: 42% reserved, 10 of 24 GiB reserved');
  });

  it('keeps an overcommitted share above 1, and has no share without a capacity', () => {
    expect(capacityShare(10, 8)).toBe(1.25);
    expect(capacityShare(3, 0)).toBeNull();
    const [, memory] = nodeMeters({
      capacity: { cpus: 1, memory_mb: 0, disk_gb: 0 },
      allocated: { cpus: 0, memory_mb: 0, disk_gb: 0 },
    });
    expect(memory!.share).toBeNull();
    expect(memory!.spoken).toBe('Memory: 0 GiB reserved, no capacity reported');
  });

  // Issue 639: a node that reports a disk capacity of 0 read "2 of 0 GiB reserved" over an empty meter.
  it('says what is reserved, and that the capacity is unknown, when a node reports none', () => {
    const [cpu, memory, disk] = nodeMeters({
      capacity: { cpus: 2, memory_mb: 3922, disk_gb: 0 },
      allocated: { cpus: 2, memory_mb: 512, disk_gb: 2 },
    });
    expect(disk).toMatchObject({
      share: null,
      words: '2 GiB reserved',
      unknown: 'The node reports no disk capacity',
      spoken: 'Disk: 2 GiB reserved, no capacity reported',
    });
    expect(disk!.words).not.toMatch(/of 0/);
    expect(cpu).toMatchObject({ share: 1, words: '2 of 2 reserved', unknown: null });
    expect(memory!.unknown).toBeNull();
    expect(memory!.words).toBe('0.5 of 3.8 GiB reserved');
  });

  it('says gibibytes with as few digits as needed', () => {
    expect(gibWords(512)).toBe('0.5');
    expect(gibWords(3379)).toBe('3.3');
    expect(gibWords(4096)).toBe('4');
    expect(gibWords(65_536)).toBe('64');
  });


  it('counts the VMs on a node and the apps they belong to', () => {
    const vms = [
      { node_id: 'a', app_id: 'app_1', observed_status: 'running' as const },
      { node_id: 'a', app_id: 'app_1', observed_status: 'running' as const },
      { node_id: 'a', app_id: null, observed_status: 'stopped' as const },
      { node_id: 'a', app_id: 'app_2', observed_status: 'deleted' as const },
      { node_id: 'b', app_id: 'app_3', observed_status: 'running' as const },
    ];
    expect(nodeLoad('a', vms)).toEqual({ vms: 3, apps: 1 });
    expect(loadWords(nodeLoad('a', vms))).toBe('3 VMs · 1 app');
    expect(loadWords({ vms: 1, apps: 0 })).toBe('1 VM');
    expect(loadWords(nodeLoad('c', vms))).toBe('No VMs');
  });

  it('tags primary and replica only when the node says so', () => {
    expect(nodeTags({ cp_status: 'primary', roles: ['node'] })).toEqual(['primary', 'node']);
    expect(nodeTags({ cp_status: null, roles: ['node', 'edge'] })).toEqual(['node', 'edge']);
  });
});

describe('the biggest node', () => {
  it('is the one carrying the most memory, then vCPUs, then disk, then by name', () => {
    expect(biggestNode([lab1, lab2, lab3])?.name).toBe('lab-2');
    const twin = node('lab-0', lab2.allocated);
    expect(biggestNode([lab2, twin])?.name).toBe('lab-0');
    const moreCpu = node('lab-9', { ...lab2.allocated, cpus: 5 });
    expect(biggestNode([lab2, moreCpu])?.name).toBe('lab-9');
  });

  it('leaves out removed nodes and nodes without the node role', () => {
    const removed = node('gone', { cpus: 9, memory_mb: 99_999, disk_gb: 9 }, undefined, { status: 'removed' });
    const edge = node('edge', { cpus: 9, memory_mb: 99_999, disk_gb: 9 }, undefined, { roles: ['edge'] });
    expect(lossCandidates([lab1, removed, edge])).toEqual([lab1]);
    expect(biggestNode([removed, edge])).toBeUndefined();
  });
});

describe('the N+1 strip', () => {
  const nodes = [lab1, lab2, lab3];

  it('fits: names where the apps go and how full that node would be', () => {
    const readout = n1Readout({
      lost: lab2,
      nodes,
      apps: [app('app_a', 1024), app('app_b', 512)],
      answer: answer(lab2, [placed('app_a', 'start', lab1.id), placed('app_b', 'restore', lab1.id)]),
    });
    expect(readout?.tone).toBe('run');
    // (5120 + 1024 + 512) / 8192 = 81.25 %
    expect(n1Text(readout!)).toBe('If lab-2 goes down, its 2 apps fit on lab-1, using 81% of its memory.');
    expect(readout!.segments).toContainEqual({ strong: 'lab-1' });
  });

  it('fits on two nodes: names both and the fuller one', () => {
    const readout = n1Readout({
      lost: lab2,
      nodes,
      apps: [app('app_a', 1024), app('app_b', 2048), app('app_c', 512)],
      answer: answer(lab2, [
        placed('app_a', 'start', lab1.id),
        placed('app_b', 'start', lab3.id),
        placed('app_c', 'keep', lab1.id),
      ]),
    });
    // lab-1: (5120 + 1024) / 8192 = 75 %; lab-3: (1024 + 2048) / 4096 = 75 % → the first stays.
    expect(n1Text(readout!)).toBe(
      'If lab-2 goes down, the 2 apps that need a new node fit on lab-1 and lab-3, with lab-1 the fullest at 75% of its memory.',
    );
  });

  it('fits with nothing to move', () => {
    const kept = n1Readout({
      lost: lab2,
      nodes,
      apps: [],
      answer: answer(lab2, [placed('app_a', 'keep', lab1.id), placed('app_b', 'keep', lab3.id)]),
    });
    expect(n1Text(kept!)).toBe('If lab-2 goes down, its 2 apps keep running on their other replicas.');
    const empty = n1Readout({ lost: lab2, nodes, apps: [], answer: answer(lab2, []) });
    expect(n1Text(empty!)).toBe('If lab-2 goes down, no app has to move.');
    expect(empty!.tone).toBe('run');
  });

  it('fits, but says which apps have nothing to start from', () => {
    const readout = n1Readout({
      lost: lab2,
      nodes,
      apps: [app('app_a', 1024)],
      answer: answer(lab2, [placed('app_a', 'start', lab1.id), placed('app_b', 'restore', null, 'no_backup')]),
    });
    expect(readout?.tone).toBe('shift');
    expect(n1Text(readout!)).toBe(
      'If lab-2 goes down, its 1 app fits on lab-1, using 75% of its memory, but 1 app has no backup to restore.',
    );
  });

  it('does not fit: how many apps are left without room and what is missing', () => {
    const readout = n1Readout({
      lost: lab2,
      nodes,
      apps: [],
      answer: answer(
        lab2,
        [placed('app_a', 'start', null, 'no_capacity'), placed('app_b', 'restore', null, 'no_capacity'), placed('app_c', 'keep', lab1.id)],
        { cpus: 2, memory_mb: 1536, disk_gb: 4 },
      ),
    });
    expect(readout?.tone).toBe('fault');
    expect(n1Text(readout!)).toBe(
      'Losing lab-2 would leave 2 apps without room: 2 vCPUs, 1.5 GiB of memory and 4 GiB of disk short.',
    );
  });

  it('a single node: there is nothing to fail over to, without asking the primary', () => {
    const readout = n1Readout({ lost: lab1, nodes: [lab1], apps: [], answer: undefined });
    expect(readout?.tone).toBe('fault');
    expect(n1Text(readout!)).toBe('lab-1 is the only node: if it goes down, no other node can take its apps.');
    // An edge-only node is not one to fail over to.
    const edge = node('edge', lab3.allocated, lab3.capacity, { roles: ['edge'] });
    expect(n1Text(n1Readout({ lost: lab1, nodes: [lab1, edge], apps: [], answer: undefined })!)).toContain(
      'is the only node',
    );
  });

  it('missing data: says nothing rather than guess', () => {
    // No answer yet, or an answer about another node.
    expect(n1Readout({ lost: lab2, nodes, apps: [], answer: undefined })).toBeNull();
    expect(n1Readout({ lost: lab2, nodes, apps: [], answer: answer(lab1, []) })).toBeNull();
    // A moving app the list does not hold: the verdict, without the share.
    const unknownApp = n1Readout({
      lost: lab2,
      nodes,
      apps: [],
      answer: answer(lab2, [placed('app_x', 'start', lab1.id)]),
    });
    expect(n1Text(unknownApp!)).toBe('If lab-2 goes down, its 1 app fits on lab-1.');
    // A receiving node the list does not hold: no name for it.
    const unknownNode = n1Readout({
      lost: lab2,
      nodes,
      apps: [app('app_a', 512)],
      answer: answer(lab2, [placed('app_a', 'start', 'nod_unknown')]),
    });
    expect(n1Text(unknownNode!)).toBe('If lab-2 goes down, its 1 app fits on the other nodes.');
  });

  it('says only the missing resources that are short', () => {
    expect(shortfallWords({ cpus: 1, memory_mb: 0, disk_gb: 0 })).toBe('1 vCPU');
    expect(shortfallWords({ cpus: 0, memory_mb: 512, disk_gb: 10 })).toBe('0.5 GiB of memory and 10 GiB of disk');
    expect(shortfallWords({ cpus: 0, memory_mb: 0, disk_gb: 0 })).toBeNull();
  });
});
