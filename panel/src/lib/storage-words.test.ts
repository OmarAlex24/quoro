import { describe, expect, it } from 'vitest';
import type { Backup } from '../api/generated/model';
import {
  repeatWords,
  scheduleSummary,
  backupLine,
  haSentence,
  isActiveVolume,
  orderVolumes,
  scheduleWords,
  volumeShare,
  volumeTag,
  volumeUsedWords,
} from './storage-words';

const NOW = Date.parse('2026-11-03T12:00:00.000Z');
const hoursAgo = (hours: number): string => new Date(NOW - hours * 3_600_000).toISOString();
const GIB = 1024 ** 3;

const backup = (overrides: Partial<Backup>): Backup => ({
  id: 'bkp_01JC9Z6Q5W7X8Y9Z0A1B2C3D4E',
  repository_id: 'brp_01JC9Z6Q5W7X8Y9Z0A1B2C3D4E',
  source_kind: 'volume',
  app_id: 'app_01JC9Z6Q5W7X8Y9Z0A1B2C3D4E',
  volume_id: 'vol_01JC9Z6Q5W7X8Y9Z0A1B2C3D4E',
  node_id: 'nod_01JC9Z6Q5W7X8Y9Z0A1B2C3D4E',
  status: 'succeeded',
  trigger: 'schedule',
  snapshot_method: 'reflink',
  pause_ms: 0,
  logical_bytes: 4 * GIB,
  uploaded_bytes: 412 * 1024 ** 2,
  chunk_count: 120,
  error: null,
  created_at: hoursAgo(6),
  started_at: hoursAgo(6),
  finished_at: hoursAgo(6),
  pruned_at: null,
  ...overrides,
});

describe('volumes', () => {
  it('tags each state with its tone', () => {
    expect(volumeTag('ready')).toEqual({ tone: 'run', words: 'Ready' });
    expect(volumeTag('failed').tone).toBe('fault');
    expect(volumeTag('detached').tone).toBe('plain');
  });

  it('puts the volume in use before a detached one', () => {
    const ordered = orderVolumes([
      { id: 'old', state: 'detached' as const },
      { id: 'new', state: 'restoring' as const },
    ]);
    expect(ordered.map((volume) => volume.id)).toEqual(['new', 'old']);
    expect(isActiveVolume({ state: 'creating' })).toBe(true);
    expect(isActiveVolume({ state: 'deleting' })).toBe(false);
  });

  it('says how full the sparse file is', () => {
    expect(volumeShare({ allocated_bytes: 5 * GIB, size_gb: 20 })).toBe(0.25);
    expect(volumeUsedWords({ allocated_bytes: 4.2 * GIB, size_gb: 20 })).toBe('4.2 of 20 GiB');
  });

  it('says so before the node first reports the file', () => {
    expect(volumeShare({ allocated_bytes: null, size_gb: 20 })).toBeNull();
    expect(volumeUsedWords({ allocated_bytes: null, size_gb: 20 })).toBe('Not reported yet');
  });
});

describe('backupLine', () => {
  it('says what a succeeded backup uploaded and paused', () => {
    expect(backupLine(backup({}), NOW)).toEqual({
      tone: 'run',
      pulse: false,
      words: 'Succeeded',
      detail: '6 h ago, scheduled · 412 MiB uploaded · no pause',
    });
  });

  it('says how long a pause_copy snapshot paused the app', () => {
    const line = backupLine(backup({ snapshot_method: 'pause_copy', pause_ms: 840, trigger: 'api' }), NOW);
    expect(line.detail).toBe('6 h ago, on demand · 412 MiB uploaded · paused 840 ms');
  });

  it('marks a pruned backup, which can no longer be restored', () => {
    const line = backupLine(backup({ pruned_at: hoursAgo(1) }), NOW);
    expect(line.tone).toBe('idle');
    expect(line.detail).toContain('pruned 1 h ago');
  });

  it('gives a failed backup its error', () => {
    const line = backupLine(
      backup({ status: 'failed', error: 'the node could not read the volume snapshot', uploaded_bytes: null }),
      NOW,
    );
    expect(line).toMatchObject({ tone: 'fault', words: 'Failed' });
    expect(line.detail).toBe('6 h ago · the node could not read the volume snapshot');
  });

  it('pulses while a backup runs or waits', () => {
    expect(backupLine(backup({ status: 'running', started_at: hoursAgo(0.05) }), NOW)).toMatchObject({
      words: 'Running',
      pulse: true,
      detail: 'started 3 min ago, scheduled',
    });
    expect(backupLine(backup({ status: 'queued' }), NOW).words).toBe('Queued');
  });
});

describe('scheduleWords', () => {
  const cold = { ha_policy: 'cold' as const, backup_schedule: { interval_h: 6, keep: 7 } };

  it('counts the next backup from the last succeeded one', () => {
    const backups = [
      backup({ status: 'failed', created_at: hoursAgo(1), finished_at: hoursAgo(1) }),
      backup({ finished_at: new Date(NOW - (3 * 60 + 46) * 60_000).toISOString() }),
    ];
    expect(scheduleWords(cold, backups, NOW)).toBe('Every 6 hours · next in 2 h 14 min · keeps 7');
  });

  it('says when the next one is already due', () => {
    expect(scheduleWords(cold, [backup({ finished_at: hoursAgo(7) })], NOW)).toBe(
      'Every 6 hours · next due now · keeps 7',
    );
  });

  it('has nothing to count from before the first backup succeeds', () => {
    expect(scheduleWords({ ...cold, backup_schedule: { interval_h: 1, keep: 3 } }, [], NOW)).toBe(
      'Every hour · none has succeeded yet · keeps 3',
    );
  });

  it('runs only under the Cold policy', () => {
    expect(scheduleWords({ ...cold, ha_policy: 'none' }, [backup({})], NOW)).toBe(
      'Not scheduled: backups run on a schedule under the Cold policy',
    );
  });
});

describe('haSentence', () => {
  it('gives cold its restore estimate', () => {
    expect(haSentence('cold', { restore_estimate_s: 70 })).toBe(
      'Cold: restores the last backup on another node. Estimate 1 min 10 s.',
    );
  });

  it('leaves the estimate out when the contract has none', () => {
    expect(haSentence('cold', { restore_estimate_s: null })).toBe('Cold: restores the last backup on another node.');
    expect(haSentence('none', { restore_estimate_s: null })).toMatch(/^None:/);
    expect(haSentence('hot', undefined)).toBe('Hot: not in this release.');
  });
});

describe('repeatWords', () => {
  const NOW = Date.parse('2026-11-07T12:00:00Z');
  const hoursAgo = (hours: number) => new Date(NOW - hours * 3_600_000).toISOString();

  it('counts the next run from the last succeeded backup', () => {
    expect(
      repeatWords(
        { interval_h: 6, keep: 7 },
        [
          { status: 'failed', created_at: hoursAgo(1), finished_at: hoursAgo(1), pruned_at: null },
          { status: 'succeeded', created_at: hoursAgo(4), finished_at: hoursAgo(4), pruned_at: null },
        ],
        NOW,
      ),
    ).toBe('Every 6 hours · next in 2 h · keeps 7');
  });

  it('says when none has succeeded yet, and an hourly schedule in words', () => {
    expect(repeatWords({ interval_h: 1, keep: 3 }, [], NOW)).toBe(
      'Every hour · none has succeeded yet · keeps 3',
    );
  });
});

describe('scheduleSummary', () => {
  it('says a schedule with the words repeatWords uses, without the next run', () => {
    expect(scheduleSummary({ interval_h: 6, keep: 7 })).toBe('Every 6 hours · keeps 7');
    expect(scheduleSummary({ interval_h: 1, keep: 1 })).toBe('Every hour · keeps 1');
  });
});
