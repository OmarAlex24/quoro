// SPDX-License-Identifier: AGPL-3.0-only
import { useEffect, useRef, useState } from 'react';
import { MOTION_CHANGE_MS, REDUCED_MOTION, shouldTween, tweenAt } from './motion';

/**
 * A number that eases to each new `target` on the panel's curve instead of
 * jumping. The first value is shown as it is (nothing fills on mount); a change
 * that arrives mid-way starts from what is on screen; a tiny change, or any
 * change under `prefers-reduced-motion`, is shown at once.
 */
export function useTween(target: number, duration = MOTION_CHANGE_MS): number {
  const [shown, setShown] = useState(target);
  const current = useRef(target);

  useEffect(() => {
    const from = current.current;
    const reduced = typeof window.matchMedia === 'function' && window.matchMedia(REDUCED_MOTION).matches;
    if (!shouldTween(from, target, reduced)) {
      current.current = target;
      setShown(target);
      return;
    }
    const start = performance.now();
    let frame = requestAnimationFrame(function step(now: number) {
      const elapsed = now - start;
      const value = tweenAt(from, target, elapsed, duration);
      current.current = value;
      setShown(value);
      if (elapsed < duration) frame = requestAnimationFrame(step);
    });
    return () => cancelAnimationFrame(frame);
  }, [target, duration]);

  return shown;
}
