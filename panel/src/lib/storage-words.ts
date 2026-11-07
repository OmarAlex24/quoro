// SPDX-License-Identifier: AGPL-3.0-only
import type {
  App,
  Backup,
  BackupSchedule,
  HaPolicyCost,
  HaPolicyCostPolicy,
  Volume,
} from '../api/generated/model';
import { relativeTime } from './app-status';
import { formatBytes } from './ha-cost';
import type { LampTone } from './lamp';
import { secondsWords } from './metric-area';
import type { TagTone } from '../components/ui/tag';

/*
 * The Storage tab's words, from the contract's volume, backup and HA fields
 * (openapi.yaml `Volume`, `Backup`, `BackupSchedule`, `HaPolicyCost`). Pure,
 * so each sentence can be tested without a DOM.
 */

const VOLUME_TAG: Record<Volume['state'], { tone: TagTone; words: string }> = {
  creating: { tone: 'shift', words: 'Creating' },
  ready: { tone: 'run', words: 'Ready' },
  restoring: { tone: 'shift', words: 'Restoring' },
  failed: { tone: 'fault', words: 'Failed' },
  detached: { tone: 'plain', words: 'Detached' },
  deleting: { tone: 'shift', words: 'Deleting' },
};

/** A volume's state as a tag: the tone and the word. */
export const volumeTag = (state: Volume['state']): { tone: TagTone; words: string } => VOLUME_TAG[state];

/** The volume in use: `creating`, `ready` or `restoring` (one per app in M5a). */
export const isActiveVolume = (volume: Pick<Volume, 'state'>): boolean =>
  volume.state === 'creating' || volume.state === 'ready' || volume.state === 'restoring';

/**
 * The app's volumes, the one in use first, then the rest newest first as
 * `listAppVolumes` returns them (a restore leaves the replaced one `detached`).
 */
export const orderVolumes = <V extends Pick<Volume, 'state'>>(volumes: readonly V[]): V[] => [
  ...volumes.filter(isActiveVolume),
  ...volumes.filter((volume) => !isActiveVolume(volume)),
];

/** How full the sparse file is, 0..1, or null before the node first reports it. */
export const volumeShare = (volume: Pick<Volume, 'allocated_bytes' | 'size_gb'>): number | null => {
  if (volume.allocated_bytes === null || volume.allocated_bytes === undefined) return null;
  const size = volume.size_gb * 1024 ** 3;
  return size > 0 ? Math.min(1, Math.max(0, volume.allocated_bytes / size)) : null;
};

/** "4.2 of 20 GiB", or "Not reported yet" before the node's first report. */
export const volumeUsedWords = (volume: Pick<Volume, 'allocated_bytes' | 'size_gb'>): string => {
  if (volume.allocated_bytes === null || volume.allocated_bytes === undefined) return 'Not reported yet';
  const gib = volume.allocated_bytes / 1024 ** 3;
  const used = gib >= 100 ? `${Math.round(gib)}` : `${Math.round(gib * 10) / 10}`;
  return `${used} of ${volume.size_gb} GiB`;
};

export interface BackupLine {
  tone: LampTone;
  pulse: boolean;
  words: string;
  detail: string;
}

const triggerWords: Record<Backup['trigger'], string> = {
  schedule: 'scheduled',
  api: 'on demand',
  upgrade: 'before an upgrade',
};

/** The pause a backup cost the app: a reflink snapshot costs none. */
const pauseWords = (backup: Pick<Backup, 'pause_ms' | 'snapshot_method'>): string | null => {
  if (backup.pause_ms === null || backup.pause_ms === undefined) return null;
  if (backup.pause_ms <= 0) return 'no pause';
  return `paused ${backup.pause_ms < 1_000 ? `${backup.pause_ms} ms` : secondsWords(backup.pause_ms / 1_000)}`;
};

/**
 * One backup as a row: a lamp, a word, and one line of what happened. A
 * succeeded backup says what it uploaded and what it paused; a failed one its
 * error; a pruned one that it can no longer be restored.
 */
export const backupLine = (backup: Backup, now: number = Date.now()): BackupLine => {
  const when = relativeTime(backup.created_at, now);
  const trigger = triggerWords[backup.trigger];
  switch (backup.status) {
    case 'succeeded': {
      const parts = [`${when}, ${trigger}`];
      if (backup.uploaded_bytes !== null && backup.uploaded_bytes !== undefined) {
        parts.push(`${formatBytes(backup.uploaded_bytes)} uploaded`);
      }
      const pause = pauseWords(backup);
      if (pause) parts.push(pause);
      if (backup.pruned_at) parts.push(`pruned ${relativeTime(backup.pruned_at, now)}`);
      return { tone: backup.pruned_at ? 'idle' : 'run', pulse: false, words: 'Succeeded', detail: parts.join(' · ') };
    }
    case 'failed':
      return {
        tone: 'fault',
        pulse: false,
        words: 'Failed',
        detail: backup.error ? `${when} · ${backup.error}` : `${when}, ${trigger}`,
      };
    case 'running':
      return {
        tone: 'shift',
        pulse: true,
        words: 'Running',
        detail: `started ${relativeTime(backup.started_at ?? backup.created_at, now)}, ${trigger}`,
      };
    default:
      return { tone: 'shift', pulse: true, words: 'Queued', detail: `${when}, ${trigger}` };
  }
};

const hoursWords = (hours: number): string => (hours === 1 ? 'Every hour' : `Every ${hours} hours`);

/**
 * The app's backup schedule in one line: `Every 6 hours · next in 2 h 14 min
 * · keeps 7`. The schedule runs only under the `cold` policy and counts from
 * the last succeeded backup (openapi.yaml `BackupSchedule`), so with no
 * succeeded backup there is nothing to count from yet. Backups are newest
 * first, as `listAppBackups` returns them.
 */
export const scheduleWords = (
  app: Pick<App, 'ha_policy' | 'backup_schedule'>,
  backups: ReadonlyArray<Pick<Backup, 'status' | 'created_at' | 'finished_at' | 'pruned_at'>>,
  now: number = Date.now(),
): string => {
  if (app.ha_policy !== 'cold') return 'Not scheduled: backups run on a schedule under the Cold policy';
  return repeatWords(app.backup_schedule, backups, now);
};

/**
 * A schedule in one line, counted from the last succeeded backup in `backups`
 * (newest first): `Every 6 hours · next in 2 h 14 min · keeps 7`. The
 * control-plane schedule of the backup repository reads the same way.
 */
export const repeatWords = (
  schedule: Pick<BackupSchedule, 'interval_h' | 'keep'>,
  backups: ReadonlyArray<Pick<Backup, 'status' | 'created_at' | 'finished_at' | 'pruned_at'>>,
  now: number = Date.now(),
): string => {
  const { interval_h: interval, keep } = schedule;
  const last = backups.find((backup) => backup.status === 'succeeded');
  let next: string;
  if (!last) {
    next = 'none has succeeded yet';
  } else {
    const due = Date.parse(last.finished_at ?? last.created_at) + interval * 3_600_000;
    const seconds = Math.round((due - now) / 1_000);
    next = seconds < 60 ? 'next due now' : `next in ${secondsWords(Math.floor(seconds / 60) * 60)}`;
  }
  return `${hoursWords(interval)} · ${next} · keeps ${keep}`;
};

const POLICY_SENTENCE: Record<HaPolicyCostPolicy, string> = {
  none: 'None: the data lives on this node and in its backups; nothing is kept elsewhere.',
  cold: 'Cold: restores the last backup on another node.',
  warm: 'Warm: keeps a copy of the volume on another node, ready to start.',
  hot: 'Hot: not in this release.',
};

/**
 * What the chosen policy does, and the estimate the contract gives for it:
 * the restore time for `cold`, the start time of the copy for `warm`.
 */
export const haSentence = (
  policy: HaPolicyCostPolicy,
  cost: Pick<HaPolicyCost, 'restore_estimate_s'> | undefined,
): string => {
  const sentence = POLICY_SENTENCE[policy];
  const estimate = cost?.restore_estimate_s;
  if ((policy === 'cold' || policy === 'warm') && estimate !== null && estimate !== undefined) {
    return `${sentence} Estimate ${secondsWords(estimate)}.`;
  }
  return sentence;
};
