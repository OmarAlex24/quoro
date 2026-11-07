import { describe, expect, it } from 'vitest';
import { ribbonLayout, ribbonSpan, type RibbonInput } from './ribbon';

const T0 = Date.UTC(2026, 10, 7, 17, 17, 0);
const at = (minutes: number) => new Date(T0 + minutes * 60_000).toISOString();

/** The reference incident: opened 17:17, now 17:35, a 1 h incident grant. */
const reference = (patch: Partial<RibbonInput> = {}): RibbonInput => ({
  openedAt: at(0.2),
  now: T0 + 18 * 60_000,
  grants: [{ id: 'grt_incident', created_at: at(0.5), expires_at: at(60.5) }],
  calls: [
    { seq: 1, time: at(0.2), refused: false, other: true },
    { seq: 3, time: at(3), refused: false, other: false },
    { seq: 4, time: at(5), refused: false, other: false },
    { seq: 5, time: at(6), refused: true, other: false },
  ],
  waits: [{ id: 'apr_pending', from: at(7), to: null }],
  ...patch,
});

describe('ribbonSpan', () => {
  it('picks the first span that keeps now at most 90 % across', () => {
    expect(ribbonSpan(18 * 60_000)).toBe(20);
    expect(ribbonSpan(19 * 60_000)).toBe(30);
    expect(ribbonSpan(0)).toBe(10);
    expect(ribbonSpan(50 * 60_000)).toBe(60);
  });

  it('stops at its longest span', () => {
    expect(ribbonSpan(90 * 86_400_000)).toBe(10_080);
  });
});

describe('ribbonLayout', () => {
  it('starts on the minute the incident opened and spans 20 min for 18 min elapsed', () => {
    const layout = ribbonLayout(reference());
    expect(layout.start).toBe(T0);
    expect(layout.end).toBe(T0 + 20 * 60_000);
    expect(layout.now).toBeCloseTo(90);
  });

  it('places quarter ticks and keeps them clear of the now label', () => {
    const layout = ribbonLayout(reference());
    expect(layout.ticks.map((tick) => tick.left)).toEqual([0, 25, 50, 75]);
    expect(layout.ticks.every((tick) => !tick.hidden)).toBe(true);
    // Now at 78 %: the 75 % tick hides under the now label.
    const close = ribbonLayout(reference({ now: T0 + 15.6 * 60_000 }));
    expect(close.now).toBeCloseTo(78);
    expect(close.ticks[3]!.hidden).toBe(true);
    expect(close.ticks[0]!.hidden).toBe(false);
  });

  it('puts each call at its own time, with no duration', () => {
    const { points } = ribbonLayout(reference());
    expect(points.map((point) => point.left)).toEqual([1, 15, 25, 30]);
    expect(points.map((point) => point.tone)).toEqual(['other', 'call', 'call', 'refused']);
  });

  it('draws the grant window from its issue, running on past the right edge', () => {
    const [window] = ribbonLayout(reference()).windows;
    expect(window!.left).toBeCloseTo(2.5);
    expect(window!.left + window!.width).toBe(100);
    expect(window!.open).toBe(true);
  });

  it('ends a revoked grant where it was revoked', () => {
    const layout = ribbonLayout(
      reference({ grants: [{ id: 'g', created_at: at(0.5), expires_at: at(60.5), revoked_at: at(10) }] }),
    );
    expect(layout.windows[0]!.left + layout.windows[0]!.width).toBeCloseTo(50);
    expect(layout.windows[0]!.open).toBe(false);
  });

  it('marks calls outside every grant window', () => {
    const { points } = ribbonLayout(reference());
    // The opening record comes before the grant was issued.
    expect(points[0]!.inGrant).toBe(false);
    expect(points.slice(1).every((point) => point.inGrant)).toBe(true);
    const lapsed = ribbonLayout(
      reference({ grants: [{ id: 'g', created_at: at(0.5), expires_at: at(4) }] }),
    );
    expect(lapsed.points.map((point) => point.inGrant)).toEqual([false, true, false, false]);
  });

  it('opens the lane earlier when a record of the incident is older than the incident', () => {
    const layout = ribbonLayout(
      reference({ calls: [{ seq: 1, time: at(-4), refused: false, other: false }] }),
    );
    expect(layout.start).toBe(T0 - 4 * 60_000);
    expect(layout.points[0]!.left).toBe(0);
    expect(layout.points[0]!.pinned).toBe(false);
  });

  it('runs the waiting band up to now while the request waits', () => {
    const [band] = ribbonLayout(reference()).waits;
    expect(band!.left).toBeCloseTo(35);
    expect(band!.left + band!.width).toBeCloseTo(90);
    expect(band!.live).toBe(true);
  });

  it('ends the waiting band when a person decided', () => {
    const [band] = ribbonLayout(reference({ waits: [{ id: 'a', from: at(7), to: at(9) }] })).waits;
    expect(band!.width).toBeCloseTo(10);
    expect(band!.live).toBe(false);
  });

  it('stops at the resolution and draws no now line', () => {
    const layout = ribbonLayout(reference({ resolvedAt: at(12), now: T0 + 300 * 60_000 }));
    expect(layout.now).toBeNull();
    expect(layout.end).toBe(T0 + 15 * 60_000);
    // The still-open request ends at the resolution, not at the clock.
    expect(layout.waits[0]!.left + layout.waits[0]!.width).toBeCloseTo(80);
  });

  it('skips grants and calls it cannot read', () => {
    const layout = ribbonLayout(
      reference({
        grants: [{ id: 'g', created_at: 'soon', expires_at: at(10) }],
        calls: [{ seq: 9, time: 'never', refused: false, other: false }],
      }),
    );
    expect(layout.windows).toEqual([]);
    expect(layout.points).toEqual([]);
  });
});
