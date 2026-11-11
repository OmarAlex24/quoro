// SPDX-License-Identifier: AGPL-3.0-only
import { useLayoutEffect, useRef } from 'react';
import { MOTION_CHANGE_MS, REDUCED_MOTION } from './motion';
import { EASE_CSS, inkGeometry, PANE_ENTER_FRAMES, tabChangeAnimates } from './tab-motion';

const reducedMotion = () =>
  typeof window !== 'undefined' && typeof window.matchMedia === 'function' && window.matchMedia(REDUCED_MOTION).matches;

/**
 * The active bar of a tab row. Put `navRef` on the row (it must be
 * positioned) and `inkRef` on a `.tab-ink` span inside it. The bar sits under
 * the tab with the class `on`. When `tab` changes it slides there on
 * the panel's curve; on mount, on a resize, on a font load or when a count
 * widens a tab it moves at once, so nothing slides on an idle screen.
 */
export function useTabInk<T extends HTMLElement = HTMLElement>(tab: string) {
  const navRef = useRef<T>(null);
  const inkRef = useRef<HTMLSpanElement>(null);
  const shown = useRef<string | null>(null);
  const settle = useRef<number | undefined>(undefined);

  const place = () => {
    const nav = navRef.current;
    const ink = inkRef.current;
    if (!nav || !ink) return;
    // The strips mark their active tab with `on`, the same tab that carries
    // aria-current (the links match their search exactly, `EXACT_LINK`).
    const active = nav.querySelector<HTMLElement>(':scope > .on');
    const at = inkGeometry(active ? { left: active.offsetLeft, width: active.offsetWidth } : null);
    if (at === null) {
      ink.style.opacity = '0';
      return;
    }
    ink.style.opacity = '';
    ink.style.width = `${at.width}px`;
    ink.style.transform = `translateX(${at.x}px)`;
  };

  // A change of tab: slide (the bar carries `data-glide` for one change), or
  // jump under reduced motion and on the first tab shown.
  useLayoutEffect(() => {
    const ink = inkRef.current;
    if (ink && tabChangeAnimates(shown.current, tab, reducedMotion())) {
      ink.setAttribute('data-glide', '');
      window.clearTimeout(settle.current);
      settle.current = window.setTimeout(() => ink.removeAttribute('data-glide'), MOTION_CHANGE_MS + 40);
    }
    shown.current = tab;
    place();
  }, [tab]);

  // Sizes that change without a change of tab. While a slide runs the bar
  // keeps sliding, to the corrected place.
  useLayoutEffect(() => {
    const nav = navRef.current;
    if (!nav) return;
    const observer = new ResizeObserver(() => place());
    observer.observe(nav);
    for (const child of Array.from(nav.children)) observer.observe(child);
    let live = true;
    void document.fonts?.ready.then(() => {
      if (live) place();
    });
    return () => {
      live = false;
      observer.disconnect();
      window.clearTimeout(settle.current);
    };
  }, []);

  return { navRef, inkRef };
}

/**
 * A tab's page entering: when `tab` changes, the elements `pick` returns rise
 * 6 px into place and fade in, on the panel's curve. Not on mount, not under
 * reduced motion, and nothing is left on the elements once it ends.
 */
export function usePaneEnter<T extends HTMLElement = HTMLElement>(
  tab: string,
  pick: (root: T) => Element[] = (root) => [root],
) {
  const ref = useRef<T>(null);
  const shown = useRef<string | null>(null);

  useLayoutEffect(() => {
    const root = ref.current;
    const animates = tabChangeAnimates(shown.current, tab, reducedMotion());
    shown.current = tab;
    if (!root || !animates) return;
    for (const element of pick(root)) {
      if (typeof element.animate !== 'function') continue;
      element.animate(PANE_ENTER_FRAMES, { duration: MOTION_CHANGE_MS, easing: EASE_CSS });
    }
    // `pick` is read once per change of tab, on purpose.
  }, [tab]);

  return ref;
}
