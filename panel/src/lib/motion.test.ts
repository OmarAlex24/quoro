import { describe, expect, it } from 'vitest';
import { cubicBezier, ease, MOTION_CHANGE_MS, shouldTween, tweenAt, TWEEN_MIN_STEP } from './motion';

describe('cubicBezier', () => {
  it('is the identity for a linear curve', () => {
    const linear = cubicBezier(0, 0, 1, 1);
    for (const t of [0, 0.1, 0.25, 0.5, 0.9, 1]) expect(linear(t)).toBeCloseTo(t, 5);
  });

  it('matches the CSS keyword ease at its known midpoint', () => {
    // cubic-bezier(0.25, 0.1, 0.25, 1) at t = 0.5 is about 0.8024.
    expect(cubicBezier(0.25, 0.1, 0.25, 1)(0.5)).toBeCloseTo(0.8024, 3);
  });

  it('pins the ends and clamps outside them', () => {
    expect(ease(0)).toBe(0);
    expect(ease(1)).toBe(1);
    expect(ease(-1)).toBe(0);
    expect(ease(2)).toBe(1);
  });
});

describe('ease (the panel curve)', () => {
  it('rises fast and settles: past 80 % of the way by half time', () => {
    expect(ease(0.5)).toBeGreaterThan(0.8);
    expect(ease(0.5)).toBeLessThan(1);
  });

  it('never goes backwards', () => {
    let last = 0;
    for (let i = 1; i <= 100; i += 1) {
      const v = ease(i / 100);
      expect(v).toBeGreaterThanOrEqual(last - 1e-9);
      last = v;
    }
  });
});

describe('tweenAt', () => {
  it('starts at from and ends at to', () => {
    expect(tweenAt(0.2, 0.8, 0)).toBeCloseTo(0.2);
    expect(tweenAt(0.2, 0.8, MOTION_CHANGE_MS)).toBe(0.8);
    expect(tweenAt(0.2, 0.8, MOTION_CHANGE_MS * 3)).toBe(0.8);
  });

  it('runs in either direction', () => {
    const mid = tweenAt(0.9, 0.1, MOTION_CHANGE_MS / 2);
    expect(mid).toBeLessThan(0.9);
    expect(mid).toBeGreaterThan(0.1);
  });

  it('jumps when the duration is zero', () => {
    expect(tweenAt(0, 1, 0, 0)).toBe(1);
  });

  it('keeps the change of state within 300 ms', () => {
    expect(MOTION_CHANGE_MS).toBeLessThanOrEqual(300);
  });
});

describe('shouldTween', () => {
  it('animates a visible change', () => {
    expect(shouldTween(0.2, 0.6, false)).toBe(true);
  });

  it('never animates under reduced motion', () => {
    expect(shouldTween(0.2, 0.6, true)).toBe(false);
  });

  it('draws a tiny step at once, so a draining meter does not animate every tick', () => {
    expect(shouldTween(0.5, 0.5 + TWEEN_MIN_STEP / 2, false)).toBe(false);
    expect(shouldTween(0.5, 0.5, false)).toBe(false);
  });

  it('draws a value that is not a number at once', () => {
    expect(shouldTween(0.5, Number.NaN, false)).toBe(false);
    expect(shouldTween(Number.POSITIVE_INFINITY, 0.5, false)).toBe(false);
  });
});
