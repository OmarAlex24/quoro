import { describe, expect, it } from 'vitest';
import { SHEET_CLOSED, sheetLeft, sheetOffset, sheetPresence, type SheetTarget } from './sheet-presence';

const api: SheetTarget = { kind: 'app', id: 'app_api', tab: 'deployments' };
const web: SheetTarget = { kind: 'app', id: 'app_web', tab: 'deployments' };
const runner: SheetTarget = { kind: 'vm', id: 'vm_runner', tab: 'deployments' };

describe('sheetPresence', () => {
  it('enters when it opens over an empty canvas', () => {
    expect(sheetPresence(SHEET_CLOSED, api, false)).toEqual({ shown: api, leaving: false, enter: true });
  });

  it('opens in place under reduced motion', () => {
    expect(sheetPresence(SHEET_CLOSED, api, true)).toEqual({ shown: api, leaving: false, enter: false });
  });

  it('returns its own answer unchanged, so a screen can derive it while rendering', () => {
    const open = sheetPresence(SHEET_CLOSED, api, false);
    expect(sheetPresence(open, { ...api }, false)).toBe(open);
    const leaving = sheetPresence(open, null, false);
    expect(sheetPresence(leaving, null, false)).toBe(leaving);
    expect(sheetPresence(SHEET_CLOSED, null, false)).toBe(SHEET_CLOSED);
  });

  it('does not enter again for another tab, another app or a VM', () => {
    const open = sheetPresence(SHEET_CLOSED, api, false);
    expect(sheetPresence(open, { ...api, tab: 'logs' }, false)).toEqual({
      shown: { ...api, tab: 'logs' },
      leaving: false,
      enter: false,
    });
    expect(sheetPresence(open, web, false).enter).toBe(false);
    expect(sheetPresence(open, runner, false)).toEqual({ shown: runner, leaving: false, enter: false });
  });

  it('keeps what it showed while it leaves', () => {
    const open = sheetPresence(SHEET_CLOSED, { ...api, tab: 'metrics' }, false);
    expect(sheetPresence(open, null, false)).toEqual({
      shown: { ...api, tab: 'metrics' },
      leaving: true,
      enter: true,
    });
  });

  it('leaves at once under reduced motion', () => {
    const open = sheetPresence(SHEET_CLOSED, api, true);
    expect(sheetPresence(open, null, true)).toBe(SHEET_CLOSED);
  });

  it('comes back without a second entrance when opened again while it leaves', () => {
    const leaving = sheetPresence(sheetPresence(SHEET_CLOSED, api, false), null, false);
    expect(sheetPresence(leaving, api, false)).toEqual({ shown: api, leaving: false, enter: false });
    expect(sheetPresence(leaving, web, false)).toEqual({ shown: web, leaving: false, enter: false });
  });
});

describe('sheetLeft', () => {
  it('closes a sheet that finished leaving', () => {
    const leaving = sheetPresence(sheetPresence(SHEET_CLOSED, api, false), null, false);
    expect(sheetLeft(leaving, api)).toBe(SHEET_CLOSED);
  });

  it('keeps a sheet opened again before its exit ended', () => {
    const leaving = sheetPresence(sheetPresence(SHEET_CLOSED, api, false), null, false);
    const back = sheetPresence(leaving, api, false);
    expect(sheetLeft(back, api)).toBe(back);
    const other = sheetPresence(leaving, web, false);
    expect(sheetLeft(other, api)).toBe(other);
  });

  it('ignores the end of an exit for another sheet', () => {
    const leaving = sheetPresence(sheetPresence(SHEET_CLOSED, api, false), null, false);
    expect(sheetLeft(leaving, web)).toBe(leaving);
  });
});

describe('sheetOffset', () => {
  it('slides in from the right, and up from below on a phone', () => {
    expect(sheetOffset(false)).toBe('translateX(36px)');
    expect(sheetOffset(true)).toBe('translateY(20px)');
  });
});
