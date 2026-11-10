// SPDX-License-Identifier: AGPL-3.0-only
/**
 * The geometry and timing of a tab row (ADR 0318): the active bar slides to
 * the new tab on the panel's curve and the page under it enters 6 px below
 * where it settles. Both happen only when the tab changes, never on load, on
 * a resize or under `prefers-reduced-motion`.
 */

/** A tab's horizontal padding: the bar runs under its words, not its padding. */
export const TAB_INSET = 8;

/** How far below its place a tab's page starts when it enters. */
export const PANE_ENTER_OFFSET = 6;

/** The panel's one curve as CSS, for Web Animations; keep in step with `--ease`. */
export const EASE_CSS = 'cubic-bezier(0.2, 0.8, 0.2, 1)';

/** A page entering: from 6 px low and transparent to where it sits. */
export const PANE_ENTER_FRAMES: Keyframe[] = [
  { opacity: 0, transform: `translateY(${PANE_ENTER_OFFSET}px)` },
  { opacity: 1, transform: 'none' },
];

/**
 * Where the bar sits under the active tab, measured from the row's own left
 * edge: across the tab less its padding on both sides. No tab, no bar.
 */
export function inkGeometry(tab: { left: number; width: number } | null): { x: number; width: number } | null {
  if (tab === null || !Number.isFinite(tab.left) || !Number.isFinite(tab.width)) return null;
  const inset = Math.min(TAB_INSET, tab.width / 2);
  return { x: tab.left + inset, width: Math.max(0, tab.width - inset * 2) };
}

/**
 * Whether a change of tab animates: only a real change from a tab already on
 * screen, and never under reduced motion. The first tab shown is drawn in
 * place.
 */
export function tabChangeAnimates(previous: string | null, next: string, reducedMotion: boolean): boolean {
  return !reducedMotion && previous !== null && previous !== next;
}
