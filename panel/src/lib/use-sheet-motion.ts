// SPDX-License-Identifier: AGPL-3.0-only
import { useEffect, useLayoutEffect, useRef, type RefObject } from 'react';
import { MOTION_CHANGE_MS, REDUCED_MOTION } from './motion';
import { SHEET_FADE_MS, sheetOffset } from './sheet-presence';
import { EASE_CSS } from './tab-motion';

/** The service sheet's entrance and exit; `presence` decides when each happens. */
export interface SheetMotion {
  /** Slide in when the sheet mounts. */
  enter: boolean;
  /** Slide out; `onLeft` runs once it is out. */
  leaving: boolean;
  onLeft: () => void;
}

const matches = (query: string) =>
  typeof window !== 'undefined' && typeof window.matchMedia === 'function' && window.matchMedia(query).matches;

/** Below 768 px the sheet takes the screen and rises from below. */
const NARROW = '(max-width: 767.98px)';

/**
 * Slides the sheet in from the right (up from below on a phone) on the
 * panel's curve when it mounts with `enter`, and back out when `leaving`
 * turns on: 280 ms for the slide, 200 ms for the fade. Opened again before
 * it is out, it slides back. Nothing moves under reduced motion, and nothing
 * is left on the element once a slide ends.
 */
export function useSheetMotion(ref: RefObject<HTMLElement | null>, motion: SheetMotion | undefined) {
  const out = useRef<Animation[]>([]);
  const onLeft = useRef(motion?.onLeft);
  onLeft.current = motion?.onLeft;

  // The entrance, once, when the sheet mounts.
  useLayoutEffect(() => {
    const element = ref.current;
    if (!element || !motion?.enter || matches(REDUCED_MOTION) || typeof element.animate !== 'function') return;
    const from = sheetOffset(matches(NARROW));
    const slide = element.animate([{ transform: from }, { transform: 'none' }], {
      duration: MOTION_CHANGE_MS,
      easing: EASE_CSS,
    });
    const fade = element.animate([{ opacity: 0 }, { opacity: 1 }], { duration: SHEET_FADE_MS, easing: EASE_CSS });
    return () => {
      slide.cancel();
      fade.cancel();
    };
    // Read once, at mount, on purpose.
  }, []);

  const leaving = motion?.leaving ?? false;
  useEffect(() => {
    const element = ref.current;
    if (!leaving) {
      // Opened again before it was out: back to its place along the same path.
      for (const animation of out.current) {
        animation.onfinish = () => animation.cancel();
        animation.reverse();
      }
      out.current = [];
      return;
    }
    const done = () => onLeft.current?.();
    if (!element || matches(REDUCED_MOTION) || typeof element.animate !== 'function') {
      done();
      return;
    }
    const to = sheetOffset(matches(NARROW));
    const slide = element.animate([{ transform: 'none' }, { transform: to }], {
      duration: MOTION_CHANGE_MS,
      easing: EASE_CSS,
      fill: 'forwards',
    });
    const fade = element.animate([{ opacity: 1 }, { opacity: 0 }], {
      duration: SHEET_FADE_MS,
      easing: EASE_CSS,
      fill: 'forwards',
    });
    out.current = [slide, fade];
    slide.onfinish = done;
    // A hidden browser tab may hold the animation back; the sheet still leaves.
    const fallback = window.setTimeout(done, MOTION_CHANGE_MS + 120);
    return () => window.clearTimeout(fallback);
  }, [leaving]);
}
