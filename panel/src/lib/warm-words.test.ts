// SPDX-License-Identifier: AGPL-3.0-only
import { describe, expect, it } from 'vitest';
import type { WarmCopy } from '../api/generated/model';
import { localTime } from './audit';
import {
  warmCardLine,
  warmExtentWords,
  warmIntervalWords,
  warmLagSentence,
  warmLagShort,
  warmNodeLine,
  warmShippedLine,
  warmTag,
} from './warm-words';

const NOW = Date.parse('2026-11-10T12:00:00.000Z');
const secondsAgo = (seconds: number): string => new Date(NOW - seconds * 1_000).toISOString();

const LAB4 = 'nod_01JC9Z6Q5W7X8Y9Z0A1B2C3D4E';
const GONE = 'nod_01JC9Z6Q5W7X8Y9Z0A1B2C3D4F';
const names = { [LAB4]: 'lab-4' };

const copy = (overrides: Partial<WarmCopy>): WarmCopy => ({
  node_id: LAB4,
  node_pinned: false,
  interval_s: 30,
  state: 'in_sync',
  last_snapshot_seq: 4182,
  last_shipped_at: secondsAgo(150),
  lag_s: 12,
  extent_count: 312,
  error: null,
  ...overrides,
});

const STATES: ReadonlyArray<WarmCopy['state']> = ['syncing', 'in_sync', 'lagging', 'failed'];

describe('warmTag', () => {
  it('gives each state its word, tone and pulse', () => {
    expect(warmTag({ state: 'syncing' })).toEqual({ tone: 'shift', words: 'Syncing', pulse: true });
    expect(warmTag({ state: 'in_sync' })).toEqual({ tone: 'run', words: 'In sync', pulse: false });
    expect(warmTag({ state: 'lagging' })).toEqual({ tone: 'shift', words: 'Lagging', pulse: false });
    expect(warmTag({ state: 'failed' })).toEqual({ tone: 'fault', words: 'Failed', pulse: false });
  });

  it('takes the state as given, never from the lag against the interval', () => {
    // A lag far above twice the interval, but the primary says in sync.
    expect(warmTag(copy({ state: 'in_sync', lag_s: 900, interval_s: 30 })).words).toBe('In sync');
    expect(warmTag(copy({ state: 'lagging', lag_s: 1, interval_s: 30 })).words).toBe('Lagging');
  });
});

describe('warmNodeLine', () => {
  it('names the warm node and who chose it', () => {
    expect(warmNodeLine(copy({}), names)).toEqual({
      node: { id: LAB4, name: 'lab-4' },
      text: 'On lab-4 · picked by the scheduler',
      how: 'picked by the scheduler',
    });
    expect(warmNodeLine(copy({ node_pinned: true }), names).text).toBe('On lab-4 · pinned');
  });

  it('says an unknown node, with nothing to link to, when the list does not hold it', () => {
    expect(warmNodeLine(copy({ node_id: GONE }), names)).toEqual({
      node: null,
      text: 'On an unknown node · picked by the scheduler',
      how: 'picked by the scheduler',
    });
    expect(warmNodeLine(copy({ node_id: LAB4 }), { [LAB4]: '' }).text).toBe(
      'On an unknown node · picked by the scheduler',
    );
  });

  it('says there is no eligible node when the node is null', () => {
    for (const state of STATES) {
      expect(warmNodeLine(copy({ state, node_id: null }), names)).toEqual({
        node: null,
        text: 'No eligible node',
        how: null,
      });
    }
  });
});

describe('warmShippedLine', () => {
  it('gives the snapshot and when, with the exact time for the title', () => {
    expect(warmShippedLine(copy({ last_shipped_at: secondsAgo(150) }), NOW)).toEqual({
      text: 'Snapshot 4182, 3 min ago',
      title: localTime(secondsAgo(150)),
    });
    expect(warmShippedLine(copy({ last_shipped_at: secondsAgo(4) }), NOW).text).toBe('Snapshot 4182, just now');
  });

  it('says no complete copy yet when no snapshot is reported, whatever the state', () => {
    for (const state of STATES) {
      expect(
        warmShippedLine(copy({ state, last_snapshot_seq: null, last_shipped_at: null, lag_s: null }), NOW),
      ).toEqual({ text: 'No complete copy reported yet', title: null });
    }
  });

  it('keeps the snapshot of a failed copy', () => {
    expect(warmShippedLine(copy({ state: 'failed', error: 'warm node: disk full' }), NOW).text).toBe(
      'Snapshot 4182, 3 min ago',
    );
  });

  it('gives the snapshot alone when its time is missing', () => {
    expect(warmShippedLine(copy({ last_shipped_at: null }), NOW)).toEqual({ text: 'Snapshot 4182', title: null });
  });
});

describe('warmLagSentence', () => {
  it('says what a failover now would lose, from lag_s only', () => {
    expect(warmLagSentence({ lag_s: 12 })).toBe('A failover now would lose up to 12 s of writes');
    expect(warmLagSentence({ lag_s: 190 })).toBe('A failover now would lose up to 3 min 10 s of writes');
    expect(warmLagSentence({ lag_s: 7_380 })).toBe('A failover now would lose up to 2 h 3 min of writes');
  });

  it('reads sensibly at 0', () => {
    expect(warmLagSentence({ lag_s: 0 })).toBe('A failover now would lose under a second of writes');
  });

  it('is null without a lag, in every state', () => {
    for (const state of STATES) {
      expect(warmLagSentence(copy({ state, lag_s: null }))).toBeNull();
    }
  });

  it('never reads the shipped time against the clock', () => {
    // Shipped a day ago by the browser's clock, but the primary says 12 s.
    expect(warmLagSentence(copy({ last_shipped_at: secondsAgo(86_400), lag_s: 12 }))).toBe(
      'A failover now would lose up to 12 s of writes',
    );
  });
});

describe('warmIntervalWords', () => {
  it('says the snapshot interval, in minutes from a minute on', () => {
    expect(warmIntervalWords({ interval_s: 30 })).toBe('A snapshot every 30 s');
    expect(warmIntervalWords({ interval_s: 5 })).toBe('A snapshot every 5 s');
    expect(warmIntervalWords({ interval_s: 300 })).toBe('A snapshot every 5 min');
    expect(warmIntervalWords({ interval_s: 90 })).toBe('A snapshot every 1 min 30 s');
    expect(warmIntervalWords({ interval_s: 3_600 })).toBe('A snapshot every 1 h');
  });
});

describe('warmExtentWords', () => {
  it('counts the extents, singular and plural', () => {
    expect(warmExtentWords({ extent_count: 1 })).toBe('1 extent in the newest snapshot');
    expect(warmExtentWords({ extent_count: 0 })).toBe('0 extents in the newest snapshot');
    expect(warmExtentWords({ extent_count: 18_940 })).toBe('18,940 extents in the newest snapshot');
  });

  it('is null when not reported', () => {
    expect(warmExtentWords({ extent_count: null })).toBeNull();
  });

  it('can be set before any copy exists', () => {
    expect(warmExtentWords(copy({ state: 'syncing', last_snapshot_seq: null, extent_count: 640 }))).toBe(
      '640 extents in the newest snapshot',
    );
  });
});

describe('warmLagShort', () => {
  it('rounds up to one unit at the edges, never understating the lag', () => {
    expect(warmLagShort(0)).toBe('under 1 s');
    expect(warmLagShort(59)).toBe('59 s');
    expect(warmLagShort(60)).toBe('1 min');
    expect(warmLagShort(61)).toBe('2 min');
    expect(warmLagShort(417)).toBe('7 min');
    expect(warmLagShort(3_599)).toBe('60 min');
    expect(warmLagShort(3_600)).toBe('1 h');
    expect(warmLagShort(3_601)).toBe('2 h');
  });
});

describe('warmCardLine', () => {
  it('reads each state as the issue writes it', () => {
    expect(warmCardLine(copy({ state: 'in_sync', lag_s: 12 }), names)).toBe('Warm copy on lab-4, 12 s behind');
    expect(warmCardLine(copy({ state: 'syncing', last_snapshot_seq: null, lag_s: null }), names)).toBe(
      'Warm copy syncing to lab-4',
    );
    expect(warmCardLine(copy({ state: 'lagging', lag_s: 190 }), names)).toBe('Warm copy on lab-4, 4 min behind');
    expect(warmCardLine(copy({ state: 'failed', error: 'the warm node is not connected' }), names)).toBe(
      'Warm copy failed: the warm node is not connected',
    );
  });

  it('drops the lag part when lag_s is null', () => {
    expect(warmCardLine(copy({ state: 'in_sync', lag_s: null }), names)).toBe('Warm copy on lab-4');
    expect(warmCardLine(copy({ state: 'lagging', lag_s: null }), names)).toBe('Warm copy on lab-4, lagging');
    expect(warmCardLine(copy({ state: 'syncing', lag_s: null }), names)).toBe('Warm copy syncing to lab-4');
  });

  it('rounds the lag up on the card, while the Storage tab gives it exactly', () => {
    expect(warmCardLine(copy({ state: 'lagging', lag_s: 412 }), names)).toBe('Warm copy on lab-4, 7 min behind');
    expect(warmLagSentence({ lag_s: 412 })).toBe('A failover now would lose up to 6 min 52 s of writes');
    expect(warmCardLine(copy({ state: 'in_sync', lag_s: 60 }), names)).toBe('Warm copy on lab-4, 1 min behind');
  });

  it('reads a lag of 0 sensibly', () => {
    expect(warmCardLine(copy({ state: 'in_sync', lag_s: 0 }), names)).toBe('Warm copy on lab-4, under 1 s behind');
  });

  it('names the state instead of a node when the node is null', () => {
    expect(warmCardLine(copy({ state: 'syncing', node_id: null }), names)).toBe('Warm copy syncing');
    expect(warmCardLine(copy({ state: 'in_sync', node_id: null, lag_s: 12 }), names)).toBe(
      'Warm copy in sync, 12 s behind',
    );
    expect(warmCardLine(copy({ state: 'in_sync', node_id: null, lag_s: null }), names)).toBe('Warm copy in sync');
    expect(warmCardLine(copy({ state: 'lagging', node_id: null, lag_s: 190 }), names)).toBe(
      'Warm copy lagging, 4 min behind',
    );
    expect(warmCardLine(copy({ state: 'lagging', node_id: null, lag_s: null }), names)).toBe('Warm copy lagging');
    expect(
      warmCardLine(
        copy({ state: 'failed', node_id: null, error: 'no node is eligible as the warm node' }),
        names,
      ),
    ).toBe('Warm copy failed: no node is eligible as the warm node');
    expect(warmCardLine(copy({ state: 'failed', node_id: null, error: null }), names)).toBe('Warm copy failed');
  });

  it('says an unknown node when the list does not hold it', () => {
    expect(warmCardLine(copy({ state: 'in_sync', node_id: GONE }), names)).toBe(
      'Warm copy on an unknown node, 12 s behind',
    );
    expect(warmCardLine(copy({ state: 'syncing', node_id: GONE }), names)).toBe(
      'Warm copy syncing to an unknown node',
    );
    expect(warmCardLine(copy({ state: 'lagging', node_id: GONE, lag_s: null }), names)).toBe(
      'Warm copy on an unknown node, lagging',
    );
    expect(warmCardLine(copy({ state: 'failed', node_id: GONE, error: 'the warm node is removed' }), names)).toBe(
      'Warm copy failed: the warm node is removed',
    );
  });

  it('says failed alone when a failed copy has no error', () => {
    expect(warmCardLine(copy({ state: 'failed', error: null }), names)).toBe('Warm copy failed');
    expect(warmCardLine(copy({ state: 'failed', error: '  \n ' }), names)).toBe('Warm copy failed');
  });

  it('keeps the first line of a long error and marks the cut', () => {
    const error = 'warm node: receiving snapshot 2211 failed\nno space left on device (os error 28)\nwhile writing';
    expect(warmCardLine(copy({ state: 'failed', error }), names)).toBe(
      'Warm copy failed: warm node: receiving snapshot 2211 failed…',
    );
    expect(warmCardLine(copy({ state: 'failed', error: '\r\nsource: the volume is busy\r\n' }), names)).toBe(
      'Warm copy failed: source: the volume is busy',
    );
  });

  it('leaves a long single line whole, for the row to truncate', () => {
    const error = `source: ${'x'.repeat(480)}`;
    expect(warmCardLine(copy({ state: 'failed', error }), names)).toBe(`Warm copy failed: ${error}`);
  });

  it('never mentions the lag of a failed copy that keeps its progress', () => {
    expect(warmCardLine(copy({ state: 'failed', lag_s: 940, error: 'the source node is not connected' }), names)).toBe(
      'Warm copy failed: the source node is not connected',
    );
  });
});
