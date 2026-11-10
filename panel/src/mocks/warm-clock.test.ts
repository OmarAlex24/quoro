// SPDX-License-Identifier: AGPL-3.0-only
import { describe, expect, it } from 'vitest';
import type { WarmCopy } from '../api/generated/model';
import { warmCopyAt } from './warm-clock';

const SEEDED = Date.parse('2026-11-10T12:00:00.000Z');
const at = (seconds: number): number => SEEDED + seconds * 1_000;
const iso = (ms: number): string => new Date(ms).toISOString();

const copy = (overrides: Partial<WarmCopy>): WarmCopy => ({
  node_id: 'nod_01JC9Z6Q5W7X8Y9Z0A1B2C3D4E',
  node_pinned: false,
  interval_s: 30,
  state: 'in_sync',
  last_snapshot_seq: 4182,
  // Taken 12 s before the seed, shipped 5 s after it was taken.
  last_shipped_at: iso(at(-7)),
  lag_s: 12,
  extent_count: 312,
  error: null,
  ...overrides,
});

/** The copy's age at `now`, in seconds. */
const age = (warm: WarmCopy, now: number): number => (now - Date.parse(warm.last_shipped_at!)) / 1_000;

describe('warmCopyAt', () => {
  it('answers the seeded copy at the seed time', () => {
    expect(warmCopyAt(copy({}), SEEDED, SEEDED)).toEqual(copy({}));
  });

  it('moves an in-sync copy on by one snapshot each interval', () => {
    // 10 min later: 20 intervals, the newest taken 588 s after the seeded one.
    const later = warmCopyAt(copy({}), SEEDED, at(600));
    expect(later.last_snapshot_seq).toBe(4202);
    expect(later.lag_s).toBe(12);
    expect(later.last_shipped_at).toBe(iso(at(593)));
    // Just before the next copy completes, the lag is the interval plus the ship time.
    expect(warmCopyAt(copy({}), SEEDED, at(622.9)).lag_s).toBe(34);
    expect(warmCopyAt(copy({}), SEEDED, at(623)).lag_s).toBe(5);
  });

  it('keeps an in-sync copy within twice the interval and younger than its lag, at any time', () => {
    for (const interval_s of [5, 30, 60]) {
      const seeded = copy({ interval_s, lag_s: interval_s, last_shipped_at: iso(at(-interval_s + 2)) });
      for (let second = 0; second < 4 * 3_600; second += 7.3) {
        const warm = warmCopyAt(seeded, SEEDED, at(second));
        expect(warm.state).toBe('in_sync');
        expect(warm.lag_s!).toBeLessThanOrEqual(2 * interval_s);
        expect(age(warm, at(second))).toBeLessThanOrEqual(warm.lag_s!);
      }
    }
  });

  it('lets the lag of a lagging or failed copy grow, its snapshot staying', () => {
    for (const state of ['lagging', 'failed'] as const) {
      const seeded = copy({
        state,
        interval_s: 60,
        last_snapshot_seq: 977,
        last_shipped_at: iso(at(-381)),
        lag_s: 412,
        error: state === 'failed' ? 'the warm node is not connected' : null,
      });
      for (const seconds of [600, 7_200]) {
        const warm = warmCopyAt(seeded, SEEDED, at(seconds));
        expect(warm.last_snapshot_seq).toBe(977);
        expect(warm.last_shipped_at).toBe(seeded.last_shipped_at);
        expect(warm.lag_s).toBe(412 + seconds);
        expect(age(warm, at(seconds))).toBeLessThanOrEqual(warm.lag_s!);
      }
    }
  });

  it('leaves a copy without progress as it is', () => {
    const syncing = copy({ state: 'syncing', last_snapshot_seq: null, last_shipped_at: null, lag_s: null });
    expect(warmCopyAt(syncing, SEEDED, at(3_600))).toEqual(syncing);
    const lagging = copy({ state: 'lagging', lag_s: null });
    expect(warmCopyAt(lagging, SEEDED, at(3_600))).toEqual(lagging);
    const none = copy({
      state: 'failed',
      node_id: null,
      last_snapshot_seq: null,
      last_shipped_at: null,
      lag_s: null,
      error: 'no node is eligible as the warm node',
    });
    expect(warmCopyAt(none, SEEDED, at(3_600))).toEqual(none);
  });
});
