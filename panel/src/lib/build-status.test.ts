// SPDX-License-Identifier: AGPL-3.0-only
import { describe, expect, it } from 'vitest';
import type { Build, BuildStatus } from '../api/generated/model';
import {
  buildDurationLabel,
  buildDurationSeconds,
  buildLampState,
  buildRail,
  builtImageBuildId,
  builtImageLabel,
  BUILD_PHASES,
  formatBuildDuration,
  isBuildLive,
  isBuildTerminal,
  lastBuiltCommit,
  shortCommit,
} from './build-status';

const rail = (status: BuildStatus, failedPhase: Build['failed_phase'] = null) =>
  buildRail({ status, failed_phase: failedPhase });

describe('buildRail', () => {
  it('starts at the queue with every later station dark', () => {
    const result = rail('queued');
    expect(result.stations.map((station) => station.state)).toEqual([
      'current',
      'not-reached',
      'not-reached',
      'not-reached',
      'not-reached',
      'not-reached',
    ]);
    expect(result.caption).toBe('Waiting for the node');
    expect(result.live).toBe(true);
  });

  it('walks the stations in order while the node works', () => {
    expect(rail('preparing').stations.map((station) => station.state)).toEqual([
      'done',
      'current',
      'not-reached',
      'not-reached',
      'not-reached',
      'not-reached',
    ]);
    expect(rail('building').stations.map((station) => station.state)).toEqual([
      'done',
      'done',
      'current',
      'not-reached',
      'not-reached',
      'not-reached',
    ]);
    expect(rail('importing').stations.map((station) => station.state)).toEqual([
      'done',
      'done',
      'done',
      'done',
      'current',
      'not-reached',
    ]);
  });

  it('turns every station green once the image is ready', () => {
    const result = rail('succeeded');
    expect(result.stations.every((station) => station.state === 'done')).toBe(true);
    expect(result.live).toBe(false);
  });

  it('reddens the phase the build failed in and leaves the rest dark', () => {
    expect(rail('failed', 'building').stations.map((station) => station.state)).toEqual([
      'done',
      'done',
      'failed',
      'not-reached',
      'not-reached',
      'not-reached',
    ]);
    expect(rail('failed', 'preparing').stations.map((station) => station.state)).toEqual([
      'done',
      'failed',
      'not-reached',
      'not-reached',
      'not-reached',
      'not-reached',
    ]);
    expect(rail('failed', 'building').live).toBe(false);
  });

  it('says so when a failure named no phase', () => {
    const result = rail('failed', null);
    expect(result.stations.every((station) => station.state === 'not-reached')).toBe(true);
    expect(result.caption).toBe('The build failed before it named a phase.');
  });

  it('keeps a cancelled build dark with a plain caption', () => {
    const result = rail('cancelled');
    expect(result.stations.every((station) => station.state === 'not-reached')).toBe(true);
    expect(result.caption).toBe('Cancelled before the build finished.');
  });

  it('names every station in the real order', () => {
    expect(BUILD_PHASES).toEqual([
      'queued',
      'preparing',
      'building',
      'exporting',
      'importing',
      'succeeded',
    ]);
  });
});

describe('build state', () => {
  it('knows which statuses are terminal', () => {
    expect(isBuildTerminal('succeeded')).toBe(true);
    expect(isBuildTerminal('failed')).toBe(true);
    expect(isBuildTerminal('cancelled')).toBe(true);
    expect(isBuildTerminal('queued')).toBe(false);
    expect(isBuildLive('building')).toBe(true);
  });

  it('maps every status to a lamp with words, not colour alone', () => {
    expect(buildLampState('building').tone).toBe('shift');
    expect(buildLampState('building').pulse).toBe(true);
    expect(buildLampState('succeeded').tone).toBe('run');
    expect(buildLampState('failed').tone).toBe('fault');
    expect(buildLampState('cancelled').tone).toBe('idle');
    expect(buildLampState('failed').statusText).toBe('Failed');
  });
});

describe('build timing', () => {
  const timing = {
    created_at: '2026-11-03T10:00:00.000Z',
    started_at: '2026-11-03T10:00:01.000Z',
    finished_at: '2026-11-03T10:00:05.250Z',
  };

  it('measures from the node starting the build to its terminal state', () => {
    expect(buildDurationSeconds(timing)).toBeCloseTo(4.25, 3);
    expect(
      buildDurationSeconds({ created_at: timing.created_at, started_at: null, finished_at: null }, Date.parse('2026-11-03T10:00:03.000Z')),
    ).toBe(3);
  });

  it('says a short duration with one decimal and a long one in minutes', () => {
    expect(formatBuildDuration(4.25)).toBe('4.3 s');
    expect(formatBuildDuration(38.4)).toBe('38 s');
    expect(formatBuildDuration(125)).toBe('2 min 5 s');
    expect(formatBuildDuration(120)).toBe('2 min');
    expect(buildDurationLabel(timing)).toBe('4.3 s');
  });
});

describe('build facts', () => {
  it('shortens a commit and says there is none yet', () => {
    expect(shortCommit('4f2c9a1b2c3d4e5f60718293a4b5c6d7e8f90123')).toBe('4f2c9a1');
    expect(shortCommit(null)).toBe('—');
  });

  it('shows a built image as built image and build id, never a pullable name', () => {
    expect(builtImageLabel('build.quoro.invalid/app_ABC:bld_01JXYZ')).toBe(
      'built image bld_01JXYZ…',
    );
    expect(builtImageLabel('docker.io/library/nginx:alpine')).toBeNull();
    expect(builtImageLabel('')).toBeNull();
  });

  it('takes the short build id out of a built image reference', () => {
    expect(builtImageBuildId('build.quoro.invalid/app_ABC:bld_01JXYZ')).toBe('bld_01JXYZ…');
    expect(builtImageBuildId('build.quoro.invalid/app_ABC:')).toBe('');
    expect(builtImageBuildId('docker.io/library/nginx:alpine')).toBeNull();
    expect(builtImageBuildId('')).toBeNull();
  });

  it('finds the newest successful commit', () => {
    expect(
      lastBuiltCommit([
        { status: 'failed', commit: 'a'.repeat(40) },
        { status: 'succeeded', commit: 'b'.repeat(40) },
        { status: 'succeeded', commit: 'c'.repeat(40) },
      ]),
    ).toBe('b'.repeat(40));
    expect(lastBuiltCommit([])).toBeNull();
  });
});
