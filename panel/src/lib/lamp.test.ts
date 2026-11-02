import { describe, expect, it } from 'vitest';
import type { VmDesiredStatus, VmObservedStatus } from '../api/generated/model';
import {
  countVms,
  isConverging,
  LAMP_COLORS,
  lampState,
  listIsConverging,
} from './lamp';

const vm = (desired: VmDesiredStatus, observed: VmObservedStatus) => ({
  desired_status: desired,
  observed_status: observed,
});

describe('lampState', () => {
  it('burns one still colour when desired and observed agree', () => {
    const state = lampState(vm('running', 'running'));
    expect(state.ringTone).toBe('run');
    expect(state.coreTone).toBe('run');
    expect(state.ringColor).toBe(LAMP_COLORS.run);
    expect(state.coreColor).toBe(LAMP_COLORS.run);
    expect(state.pulse).toBe(false);
    expect(state.converging).toBe(false);
    expect(state.text).toBe('Running');
  });

  it('burns idle and still when a stopped VM is meant to be stopped', () => {
    const state = lampState(vm('stopped', 'stopped'));
    expect(state.ringColor).toBe(LAMP_COLORS.idle);
    expect(state.coreColor).toBe(LAMP_COLORS.idle);
    expect(state.pulse).toBe(false);
    expect(state.text).toBe('Stopped');
  });

  it('shows the target on the ring and the observed step in the core while creating', () => {
    const state = lampState(vm('running', 'creating'));
    expect(state.ringTone).toBe('run');
    expect(state.coreTone).toBe('shift');
    expect(state.ringColor).toBe(LAMP_COLORS.run);
    expect(state.coreColor).toBe(LAMP_COLORS.shift);
    expect(state.converging).toBe(true);
    expect(state.pulse).toBe(true);
    expect(state.text).toBe('Creating, will be running');
  });

  it('says what starting and stopping will end at', () => {
    expect(lampState(vm('running', 'starting')).text).toBe('Starting, will be running');
    expect(lampState(vm('stopped', 'stopping')).text).toBe('Stopping, will be stopped');
    expect(lampState(vm('stopped', 'running')).text).toBe('Running, will be stopped');
    expect(lampState(vm('running', 'stopped')).text).toBe('Stopped, will be running');
  });

  it('is fault-red end to end and never pulses for a crashed VM', () => {
    const state = lampState(vm('running', 'crashed'));
    expect(state.ringTone).toBe('fault');
    expect(state.coreTone).toBe('fault');
    expect(state.ringColor).toBe(LAMP_COLORS.fault);
    expect(state.coreColor).toBe(LAMP_COLORS.fault);
    expect(state.pulse).toBe(false);
    expect(state.converging).toBe(false);
    expect(state.text).toBe('Crashed. Start it to recover.');
    expect(state.statusText).toBe('Crashed');
  });

  it('keeps the target on the ring when the node has not reported yet', () => {
    const state = lampState(vm('running', 'unknown'));
    expect(state.ringColor).toBe(LAMP_COLORS.run);
    expect(state.coreColor).toBe(LAMP_COLORS.unknown);
    expect(state.pulse).toBe(true);
    expect(state.text).toBe('Unknown, will be running');
  });
});

describe('isConverging', () => {
  it('follows the refresh rule: desired running is settled at running', () => {
    expect(isConverging('running', 'running')).toBe(false);
    expect(isConverging('running', 'creating')).toBe(true);
    expect(isConverging('running', 'starting')).toBe(true);
    expect(isConverging('running', 'unknown')).toBe(true);
  });

  it('follows the refresh rule: desired stopped is settled at stopped', () => {
    expect(isConverging('stopped', 'stopped')).toBe(false);
    expect(isConverging('stopped', 'stopping')).toBe(true);
    expect(isConverging('stopped', 'running')).toBe(true);
  });

  it('treats a crash as settled, so it does not keep the list on a 1 s clock', () => {
    expect(isConverging('running', 'crashed')).toBe(false);
    expect(isConverging('stopped', 'crashed')).toBe(false);
  });

  it('detects one converging VM among many', () => {
    expect(listIsConverging([vm('running', 'running'), vm('running', 'starting')])).toBe(
      true,
    );
    expect(listIsConverging([vm('running', 'running'), vm('stopped', 'stopped')])).toBe(
      false,
    );
  });
});

describe('countVms', () => {
  it('counts running, stopped and crashed from the observed state', () => {
    const counts = countVms([
      vm('running', 'running'),
      vm('running', 'running'),
      vm('stopped', 'stopped'),
      vm('running', 'crashed'),
      vm('running', 'starting'),
    ]);
    expect(counts).toEqual({ running: 2, stopped: 1, crashed: 1 });
  });
});
