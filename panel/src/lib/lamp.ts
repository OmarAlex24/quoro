import type { Vm, VmDesiredStatus, VmObservedStatus } from '../api/generated/model';

/** The five colours a lamp can take, each naming a token in `styles.css`. */
export type LampTone = 'run' | 'shift' | 'fault' | 'idle' | 'unknown';

export const LAMP_COLORS: Record<LampTone, string> = {
  run: 'var(--color-lamp-run)',
  shift: 'var(--color-lamp-shift)',
  fault: 'var(--color-lamp-fault)',
  idle: 'var(--color-lamp-idle)',
  unknown: 'var(--color-lamp-unknown)',
};

/** Colour of the observed (core) state. */
export const observedTone = (status: VmObservedStatus): LampTone => {
  switch (status) {
    case 'running':
      return 'run';
    case 'creating':
    case 'starting':
    case 'stopping':
      return 'shift';
    case 'crashed':
      return 'fault';
    case 'stopped':
    case 'deleted':
      return 'idle';
    case 'unknown':
      return 'unknown';
  }
};

/** Colour of the desired (outer ring) state. */
export const desiredTone = (status: VmDesiredStatus): LampTone =>
  status === 'running' ? 'run' : 'idle';

/**
 * Is the VM still on its way from observed to desired? This is the exact rule
 * the list uses to pick a 1 s refresh instead of 10 s. A crash and an unknown
 * state are settled: nothing moves until someone acts or the VM's node, which
 * is disconnected, reports again.
 */
export const isConverging = (
  desired: VmDesiredStatus,
  observed: VmObservedStatus,
): boolean => {
  if (observed === 'crashed' || observed === 'unknown') return false;
  if (desired === 'running') return observed !== 'running';
  return observed !== 'stopped';
};

const observedLabel: Record<VmObservedStatus, string> = {
  creating: 'Creating',
  starting: 'Starting',
  running: 'Running',
  stopping: 'Stopping',
  stopped: 'Stopped',
  crashed: 'Crashed',
  deleted: 'Deleted',
  unknown: 'Unknown',
};

const lampText = (
  desired: VmDesiredStatus,
  observed: VmObservedStatus,
): string => {
  const target = desired === 'running' ? 'running' : 'stopped';

  if (observed === 'crashed') return 'Crashed. Start it to recover.';
  if (!isConverging(desired, observed)) return observedLabel[observed];
  return `${observedLabel[observed]}, will be ${target}`;
};

export interface LampState {
  /** Tone of the outer ring: what the control plane wants. */
  readonly ringTone: LampTone;
  /** Tone of the core: what the node reports. */
  readonly coreTone: LampTone;
  readonly ringColor: string;
  readonly coreColor: string;
  /** True only while the observed state is catching up with the desired one. */
  readonly converging: boolean;
  /** The ring pulses while converging; a crashed lamp never pulses. */
  readonly pulse: boolean;
  /** Plain words for what is happening, e.g. "Starting, will be running". */
  readonly text: string;
  /** Just the observed state, title case, for the status column. */
  readonly statusText: string;
}

/** Everything the lamp and its caption need, derived from one VM. */
export const lampState = (
  vm: Pick<Vm, 'desired_status' | 'observed_status'>,
): LampState => {
  const converging = isConverging(vm.desired_status, vm.observed_status);
  const coreTone = observedTone(vm.observed_status);
  // A crashed lamp is fault-red end to end: the ring must not advertise a
  // target it is not working towards.
  const ringTone =
    coreTone === 'fault' ? 'fault' : desiredTone(vm.desired_status);

  return {
    ringTone,
    coreTone,
    ringColor: LAMP_COLORS[ringTone],
    coreColor: LAMP_COLORS[coreTone],
    converging,
    pulse: converging,
    text: lampText(vm.desired_status, vm.observed_status),
    statusText: observedLabel[vm.observed_status],
  };
};

export const listIsConverging = (
  vms: ReadonlyArray<Pick<Vm, 'desired_status' | 'observed_status'>>,
): boolean =>
  vms.some((vm) => isConverging(vm.desired_status, vm.observed_status));

export interface VmCounts {
  running: number;
  stopped: number;
  crashed: number;
}

export const countVms = (vms: ReadonlyArray<Pick<Vm, 'observed_status'>>): VmCounts => {
  const counts: VmCounts = { running: 0, stopped: 0, crashed: 0 };
  for (const vm of vms) {
    if (vm.observed_status === 'running') counts.running += 1;
    else if (vm.observed_status === 'stopped') counts.stopped += 1;
    else if (vm.observed_status === 'crashed') counts.crashed += 1;
  }
  return counts;
};
