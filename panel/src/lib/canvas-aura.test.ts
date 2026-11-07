import { describe, expect, it } from 'vitest';
import type { Approval } from '../api/generated/model';
import { appFailed, chooseAuras, vmFailed, waitsOnApproval } from './canvas-aura';

const quiet = { waiting: false, deploying: false, failed: false };

describe('chooseAuras', () => {
  it('gives no aura to a card that needs nobody', () => {
    expect(chooseAuras([{ key: 'a', ...quiet }])).toEqual({});
  });

  it('marks deploying amber and breathing, failed red and still', () => {
    expect(
      chooseAuras([
        { key: 'api', ...quiet, deploying: true },
        { key: 'docs', ...quiet, failed: true },
      ]),
    ).toEqual({
      api: { reason: 'deploying', tone: 'shift', breathe: true },
      docs: { reason: 'failed', tone: 'fault', breathe: false },
    });
  });

  it('lets only one aura breathe, and a person waiting goes first', () => {
    const auras = chooseAuras([
      { key: 'a', ...quiet, deploying: true },
      { key: 'b', ...quiet, deploying: true },
      { key: 'c', ...quiet, waiting: true },
    ]);
    expect(Object.values(auras).filter((aura) => aura.breathe)).toHaveLength(1);
    expect(auras.c).toEqual({ reason: 'waiting', tone: 'shift', breathe: true });
    expect(auras.a).toEqual({ reason: 'deploying', tone: 'shift', breathe: false });
    expect(auras.b?.breathe).toBe(false);
  });

  it('picks the first deploying card in the order given', () => {
    const auras = chooseAuras([
      { key: 'x', ...quiet, deploying: true },
      { key: 'y', ...quiet, deploying: true },
    ]);
    expect(auras.x?.breathe).toBe(true);
    expect(auras.y?.breathe).toBe(false);
  });

  it('per card, waiting beats deploying and deploying beats failed', () => {
    const auras = chooseAuras([
      { key: 'a', waiting: true, deploying: true, failed: true },
      { key: 'b', waiting: false, deploying: true, failed: true },
    ]);
    expect(auras.a?.reason).toBe('waiting');
    expect(auras.b?.reason).toBe('deploying');
  });

  it('never lets a red aura breathe, even alone', () => {
    expect(chooseAuras([{ key: 'a', ...quiet, failed: true }]).a?.breathe).toBe(false);
  });
});

describe('aura signals', () => {
  const approval = (status: Approval['status'], type: string, id: string) =>
    ({ status, target: { type, id } }) as Approval;

  it('waits only on a pending approval for that same target', () => {
    const approvals = [approval('pending', 'app', 'app_1'), approval('denied', 'app', 'app_2')];
    expect(waitsOnApproval(approvals, { type: 'app', id: 'app_1' })).toBe(true);
    expect(waitsOnApproval(approvals, { type: 'app', id: 'app_2' })).toBe(false);
    expect(waitsOnApproval(approvals, { type: 'vm', id: 'app_1' })).toBe(false);
  });

  it('calls a down app, a failed newest build and a crashed VM failed', () => {
    expect(appFailed({ status: 'down' }, false)).toBe(true);
    expect(appFailed({ status: 'running' }, true)).toBe(true);
    expect(appFailed({ status: 'degraded' }, false)).toBe(false);
    expect(vmFailed({ observed_status: 'crashed' })).toBe(true);
    expect(vmFailed({ observed_status: 'stopped' })).toBe(false);
  });
});
