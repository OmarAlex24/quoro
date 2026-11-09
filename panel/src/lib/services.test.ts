import { describe, expect, it } from 'vitest';
import type { App, Backup, Build } from '../api/generated/model';
import {
  appAddress,
  appIdOfKey,
  appKey,
  commitLine,
  lastBuildFailed,
  memoryWords,
  nextBackupLine,
  nodesLine,
  replicasMark,
  servicesCount,
  vmSize,
  volumeLine,
} from './services';

const NOW = Date.parse('2026-11-06T12:00:00Z');
const hoursAgo = (hours: number) => new Date(NOW - hours * 3_600_000).toISOString();

const build = (status: Build['status'], commit: string | null, ref = 'main') =>
  ({ status, commit, source: { kind: 'git', repo_url: 'https://x', ref } }) as Build;

describe('service lines', () => {
  it('counts services', () => {
    expect(servicesCount(0)).toBe('No services');
    expect(servicesCount(1)).toBe('1 service');
    expect(servicesCount(5)).toBe('5 services');
  });

  it('marks replicas only when there is more than one', () => {
    expect(replicasMark({ replicas: 1 })).toBeNull();
    expect(replicasMark({ replicas: 2 })).toBe('×2');
  });

  it('says where an app answers', () => {
    expect(appAddress({ port: 3000 }, 'atlas.example.com')).toBe('atlas.example.com');
    expect(appAddress({ port: 3000 }, undefined)).toBe('Private, port 3000');
  });

  it('sizes a VM in the unit that says it exactly', () => {
    expect(memoryWords(512)).toBe('512 MiB');
    expect(memoryWords(8192)).toBe('8 GiB');
    expect(memoryWords(1536)).toBe('1.5 GiB');
    expect(vmSize({ vcpus: 4, memory_mb: 8192 })).toBe('VM · 4 vCPU · 8 GiB');
  });
});

describe('footer lines', () => {
  it('shows the commit of the newest successful build, with its ref', () => {
    const builds = [
      build('failed', '9a8b7c6d5e4f3021'),
      build('succeeded', '4f2a9c1b2c3d4e5f', 'main'),
    ];
    expect(commitLine(builds)).toBe('4f2a9c1 on main');
    expect(commitLine([build('failed', 'abc')])).toBeNull();
    expect(lastBuildFailed(builds)).toBe(true);
    expect(lastBuildFailed(builds.slice(1))).toBe(false);
    expect(lastBuildFailed([])).toBe(false);
  });

  it('shows the volume in use, not one being deleted', () => {
    expect(
      volumeLine([
        { state: 'deleting', mount_path: '/old', size_gb: 5 },
        { state: 'ready', mount_path: '/var/lib/postgresql/data', size_gb: 20 },
      ]),
    ).toBe('/var/lib/postgresql/data · 20 GiB');
    expect(volumeLine([])).toBeNull();
  });

  it('counts the next backup from the last succeeded one, for a cold app only', () => {
    const cold = { ha_policy: 'cold', backup_schedule: { interval_h: 6, keep: 7 } } as App;
    const backups = [
      { status: 'failed', created_at: hoursAgo(1), finished_at: hoursAgo(1) },
      { status: 'succeeded', created_at: hoursAgo(4), finished_at: hoursAgo(3) },
    ] as Backup[];
    expect(nextBackupLine(cold, backups, NOW)).toBe('Backup in 3 h');
    expect(nextBackupLine({ ...cold, ha_policy: 'none' }, backups, NOW)).toBeNull();
    expect(nextBackupLine(cold, [], NOW)).toBe('No backup yet');
    expect(
      nextBackupLine(cold, [{ status: 'succeeded', created_at: hoursAgo(9), finished_at: hoursAgo(9) }] as Backup[], NOW),
    ).toBe('Backup due now');
  });

  it('names the nodes once each, sorted, and skips unknown ones', () => {
    expect(nodesLine(['n2', 'n1', 'n2', 'n9'], { n1: 'lab-1', n2: 'lab-2' })).toBe('lab-1 · lab-2');
    expect(nodesLine(['n9'], {})).toBeNull();
  });
});

describe('appIdOfKey', () => {
  it('reads the app id back from a service key, and nothing from a VM key', () => {
    expect(appIdOfKey(appKey('app_1'))).toBe('app_1');
    expect(appIdOfKey('vm:vm_1')).toBeUndefined();
    expect(appIdOfKey(undefined)).toBeUndefined();
  });
});
