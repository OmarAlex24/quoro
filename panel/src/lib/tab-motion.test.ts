import { describe, expect, it } from 'vitest';
import { inkGeometry, PANE_ENTER_FRAMES, PANE_ENTER_OFFSET, TAB_INSET, tabChangeAnimates } from './tab-motion';

describe('inkGeometry', () => {
  it('runs under the words, inside the tab padding', () => {
    expect(inkGeometry({ left: 120, width: 96 })).toEqual({ x: 120 + TAB_INSET, width: 96 - 2 * TAB_INSET });
  });

  it('starts at the padding for the first tab', () => {
    expect(inkGeometry({ left: 0, width: 60 })).toEqual({ x: 8, width: 44 });
  });

  it('never goes negative for a tab narrower than its padding', () => {
    expect(inkGeometry({ left: 10, width: 10 })).toEqual({ x: 15, width: 0 });
    expect(inkGeometry({ left: 10, width: 0 })).toEqual({ x: 10, width: 0 });
  });

  it('has no bar without an active tab or with a broken measure', () => {
    expect(inkGeometry(null)).toBeNull();
    expect(inkGeometry({ left: Number.NaN, width: 40 })).toBeNull();
  });
});

describe('tabChangeAnimates', () => {
  it('animates a change from a tab on screen', () => {
    expect(tabChangeAnimates('deployments', 'logs', false)).toBe(true);
  });

  it('draws the first tab in place', () => {
    expect(tabChangeAnimates(null, 'deployments', false)).toBe(false);
  });

  it('does not animate the same tab again', () => {
    expect(tabChangeAnimates('logs', 'logs', false)).toBe(false);
  });

  it('never animates under reduced motion', () => {
    expect(tabChangeAnimates('deployments', 'logs', true)).toBe(false);
  });
});

describe('PANE_ENTER_FRAMES', () => {
  it('enters from 6 px below, transparent, to its place', () => {
    expect(PANE_ENTER_OFFSET).toBe(6);
    expect(PANE_ENTER_FRAMES[0]).toEqual({ opacity: 0, transform: 'translateY(6px)' });
    expect(PANE_ENTER_FRAMES.at(-1)).toEqual({ opacity: 1, transform: 'none' });
  });
});
