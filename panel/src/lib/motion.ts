// SPDX-License-Identifier: AGPL-3.0-only
/**
 * Motion in script, for what CSS cannot animate (a canvas meter). It follows
 * the same rules as the tokens in styles.css (ADR 0318): one curve for the
 * whole panel, at most 300 ms for a change of state, nothing under
 * `prefers-reduced-motion`.
 */

/** The media query for reduced motion. */
export const REDUCED_MOTION = '(prefers-reduced-motion: reduce)';

/** How long a change of state takes; keep in step with `--motion-change`. */
export const MOTION_CHANGE_MS = 280;

/** A smaller change than this is drawn at once: a draining meter does not animate every tick. */
export const TWEEN_MIN_STEP = 0.002;

/**
 * A CSS `cubic-bezier(x1, y1, x2, y2)` timing function: progress in time
 * (0..1) to progress in value. Solves x(s) = t by Newton's method with a
 * bisection fallback, then returns y(s).
 */
export function cubicBezier(x1: number, y1: number, x2: number, y2: number): (t: number) => number {
  const cx = 3 * x1;
  const bx = 3 * (x2 - x1) - cx;
  const ax = 1 - cx - bx;
  const cy = 3 * y1;
  const by = 3 * (y2 - y1) - cy;
  const ay = 1 - cy - by;
  const x = (s: number) => ((ax * s + bx) * s + cx) * s;
  const y = (s: number) => ((ay * s + by) * s + cy) * s;
  const dx = (s: number) => (3 * ax * s + 2 * bx) * s + cx;

  const solve = (t: number): number => {
    let s = t;
    for (let i = 0; i < 8; i += 1) {
      const err = x(s) - t;
      if (Math.abs(err) < 1e-6) return s;
      const slope = dx(s);
      if (Math.abs(slope) < 1e-6) break;
      s -= err / slope;
    }
    let lo = 0;
    let hi = 1;
    s = t;
    for (let i = 0; i < 30; i += 1) {
      const v = x(s);
      if (Math.abs(v - t) < 1e-6) return s;
      if (v < t) lo = s;
      else hi = s;
      s = (lo + hi) / 2;
    }
    return s;
  };

  return (t: number) => {
    if (t <= 0) return 0;
    if (t >= 1) return 1;
    return y(solve(t));
  };
}

/** The panel's one curve, `--ease`: cubic-bezier(0.2, 0.8, 0.2, 1). */
export const ease = cubicBezier(0.2, 0.8, 0.2, 1);

/** The value `elapsed` ms into a tween from `from` to `to` that lasts `duration` ms. */
export function tweenAt(from: number, to: number, elapsed: number, duration = MOTION_CHANGE_MS): number {
  if (duration <= 0 || elapsed >= duration) return to;
  return from + (to - from) * ease(Math.max(0, elapsed) / duration);
}

/** Whether a change from `from` to `to` animates, or is drawn at once. */
export function shouldTween(from: number, to: number, reducedMotion: boolean): boolean {
  if (reducedMotion) return false;
  if (!Number.isFinite(from) || !Number.isFinite(to)) return false;
  return Math.abs(to - from) >= TWEEN_MIN_STEP;
}
