// SPDX-License-Identifier: AGPL-3.0-only
/**
 * Which service sheet is on screen, and whether it is on its way out (ADR
 * 0318). The URL says which app or VM is open; the sheet stays on screen for
 * one change of state after the URL closes it, so it can slide out, and only
 * then leaves the page. Under `prefers-reduced-motion` it leaves at once.
 */

/** What a sheet shows: the app or VM, and the tab and build it holds while it leaves. */
export interface SheetTarget {
  kind: 'app' | 'vm';
  id: string;
  /** The app's tab; a VM sheet has one pane and ignores it. */
  tab: string;
  build?: string;
}

export interface SheetPresence {
  shown: SheetTarget | null;
  /** The URL closed it; it slides out and is inert until it leaves. */
  leaving: boolean;
  /**
   * It slides in when it mounts: true only for a sheet opened over a canvas
   * with none on it. Another app or a VM opened from an open sheet takes its
   * place without a second entrance.
   */
  enter: boolean;
}

export const SHEET_CLOSED: SheetPresence = { shown: null, leaving: false, enter: false };

const sameTarget = (left: SheetTarget | null, right: SheetTarget | null): boolean =>
  left === right ||
  (left !== null &&
    right !== null &&
    left.kind === right.kind &&
    left.id === right.id &&
    left.tab === right.tab &&
    left.build === right.build);

/**
 * The next presence for what the URL asks for. Calling it again with its own
 * answer returns that answer unchanged, so a screen can derive it while it
 * renders.
 */
export function sheetPresence(
  previous: SheetPresence,
  wanted: SheetTarget | null,
  reducedMotion: boolean,
): SheetPresence {
  if (wanted !== null) {
    if (previous.shown === null) return { shown: wanted, leaving: false, enter: !reducedMotion };
    // Already showing it: unchanged, so the screen does not render again.
    if (!previous.leaving && sameTarget(previous.shown, wanted)) return previous;
    // Another tab, app or VM, or the same one opened again while it leaves:
    // it stays where it is (or slides back) and never enters a second time.
    return { shown: wanted, leaving: false, enter: false };
  }
  if (previous.shown === null || previous.leaving) return previous;
  if (reducedMotion) return SHEET_CLOSED;
  return { ...previous, leaving: true };
}

/** The sheet finished sliding out. A sheet opened again meanwhile stays. */
export function sheetLeft(previous: SheetPresence, left: SheetTarget | null): SheetPresence {
  return previous.leaving && sameTarget(previous.shown, left) ? SHEET_CLOSED : previous;
}

/** How far a sheet slides: from the right on a wide screen, up from below on a phone. */
export const SHEET_SLIDE_X = 36;
export const SHEET_SLIDE_Y = 20;

/** Its words fade faster than it slides, as the proposal draws it. */
export const SHEET_FADE_MS = 200;

/** Where a sheet slides from (and back to when it leaves). */
export const sheetOffset = (narrow: boolean): string =>
  narrow ? `translateY(${SHEET_SLIDE_Y}px)` : `translateX(${SHEET_SLIDE_X}px)`;
