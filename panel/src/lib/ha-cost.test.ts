import { describe, expect, it } from 'vitest';
import type { HaPolicyCost } from '../api/generated/model';
import {
  formatBytes,
  formatDisk,
  formatMemory,
  formatPause,
  formatRestoreEstimate,
  formatVcpus,
  haPolicyWords,
  HA_POLICIES,
  mountPathProblem,
  policyAvailability,
  snapshotMethodWords,
  unavailableReasonWords,
  volumeSizeProblem,
} from './ha-cost';

const cost = (patch: Partial<HaPolicyCost> = {}): HaPolicyCost => ({
  policy: 'cold',
  available: true,
  reserved: { vcpus: 0, memory_mb: 0, disk_gb: 0 },
  storage_bytes: 0,
  restore_estimate_s: null,
  pause_estimate_ms: null,
  ...patch,
});

describe('HA_POLICIES', () => {
  it('lists the four policies in the contract order', () => {
    expect([...HA_POLICIES]).toEqual(['none', 'cold', 'warm', 'hot']);
    expect(haPolicyWords.cold).toBe('Cold');
  });
});

describe('formatBytes', () => {
  it('says nothing measured as an em dash', () => {
    expect(formatBytes(null)).toBe('—');
    expect(formatBytes(undefined)).toBe('—');
  });

  it('uses binary units and trims the decimals', () => {
    expect(formatBytes(0)).toBe('0 B');
    expect(formatBytes(512)).toBe('512 B');
    expect(formatBytes(512 * 1024 ** 2)).toBe('512 MiB');
    expect(formatBytes(10 * 1024 ** 3)).toBe('10 GiB');
    expect(formatBytes(Math.round(3.4 * 1024 ** 3))).toBe('3.4 GiB');
    expect(formatBytes(2 * 1024 ** 4)).toBe('2 TiB');
  });
});

describe('formatRestoreEstimate', () => {
  it('says nothing measured as an em dash', () => {
    expect(formatRestoreEstimate(null)).toBe('—');
    expect(formatRestoreEstimate(undefined)).toBe('—');
  });

  it('formats m:ss under an hour and h:mm:ss above it', () => {
    expect(formatRestoreEstimate(0)).toBe('0:00');
    expect(formatRestoreEstimate(45)).toBe('0:45');
    expect(formatRestoreEstimate(90)).toBe('1:30');
    expect(formatRestoreEstimate(3_661)).toBe('1:01:01');
  });
});

describe('formatPause', () => {
  it('says a reflink snapshot costs no pause', () => {
    expect(formatPause(0, 'reflink')).toBe('no pause');
  });

  it('keeps the milliseconds for a copy, and an em dash when unknown', () => {
    expect(formatPause(0, 'pause_copy')).toBe('0 ms');
    expect(formatPause(120, 'pause_copy')).toBe('120 ms');
    expect(formatPause(null, 'reflink')).toBe('—');
    expect(formatPause(undefined, null)).toBe('—');
  });
});

describe('unavailableReasonWords', () => {
  it('says each reason in plain words', () => {
    expect(unavailableReasonWords('stateless')).toBe('needs a volume');
    expect(unavailableReasonWords('no_repository')).toBe('needs a backup repository');
    expect(unavailableReasonWords('not_in_this_release')).toBe('not in this release');
    expect(unavailableReasonWords('no_warm_node')).toBe('needs another eligible node');
    expect(unavailableReasonWords(null)).toBeNull();
    expect(unavailableReasonWords(undefined)).toBeNull();
  });
});

describe('policyAvailability', () => {
  it('is available when the cost says so and gives no reason', () => {
    expect(policyAvailability(cost())).toEqual({ available: true, reason: null });
  });

  it('is unavailable with the reason in words', () => {
    expect(
      policyAvailability(
        cost({ available: false, unavailable_reason: 'not_in_this_release' }),
      ),
    ).toEqual({ available: false, reason: 'not in this release' });
  });

  it('stays unavailable when a reason arrives without the flag', () => {
    expect(
      policyAvailability(cost({ available: true, unavailable_reason: 'stateless' })),
    ).toEqual({ available: false, reason: 'needs a volume' });
  });
});

describe('snapshotMethodWords', () => {
  it('says what the method does to the app', () => {
    expect(snapshotMethodWords('reflink')).toBe('snapshots without pausing the app');
    expect(snapshotMethodWords('pause_copy')).toBe(
      'pauses the app while it copies the volume',
    );
    expect(snapshotMethodWords(null)).toBeNull();
  });
});

describe('reserved capacity', () => {
  it('formats each column', () => {
    const capacity = { vcpus: 2, memory_mb: 4_096, disk_gb: 10 };
    expect(formatVcpus(capacity.vcpus)).toBe('2');
    expect(formatMemory(capacity)).toBe('4 GiB');
    expect(formatDisk(capacity)).toBe('10 GiB');
  });
});

describe('volumeSizeProblem', () => {
  it('accepts a whole size from 1 to 1024 GiB', () => {
    expect(volumeSizeProblem('10')).toBeNull();
    expect(volumeSizeProblem('1')).toBeNull();
    expect(volumeSizeProblem('1024')).toBeNull();
  });

  it('refuses an empty, fractional or out-of-range size', () => {
    expect(volumeSizeProblem('')).toBe('Enter a size in GiB.');
    expect(volumeSizeProblem('1.5')).toBe('Use a whole number of GiB.');
    expect(volumeSizeProblem('0')).toBe('Use a size from 1 to 1024 GiB.');
    expect(volumeSizeProblem('2048')).toBe('Use a size from 1 to 1024 GiB.');
  });
});

describe('mountPathProblem', () => {
  it('accepts an absolute path with plain segments', () => {
    expect(mountPathProblem('/data')).toBeNull();
    expect(mountPathProblem('/var/lib/app-data')).toBeNull();
  });

  it('refuses the paths the contract refuses', () => {
    expect(mountPathProblem('data')).toBe('Use an absolute path like /data.');
    expect(mountPathProblem('/data/')).toBe('Do not use / or a trailing slash.');
    expect(mountPathProblem('/')).toBe('Do not use / or a trailing slash.');
    expect(mountPathProblem('/data/../etc')).toBe('Remove empty, "." or ".." segments.');
    expect(mountPathProblem('/proc/data')).toBe('Do not mount under /proc, /sys or /dev.');
  });
});
