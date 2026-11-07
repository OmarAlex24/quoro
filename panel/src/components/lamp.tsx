import type { CSSProperties } from 'react';
import type { Vm } from '../api/generated/model';
import { LAMP_COLORS, lampState, type LampTone } from '../lib/lamp';
import { cn } from '../lib/utils';

/** The core is this share of the lamp's box; the gap and the ring take the rest. */
const CORE_SHARE = 8 / 15;

/**
 * The lamp itself: a core in the observed tone inside a ring in the desired
 * tone, in a square box of `size` px. Still when they agree, a slow pulse while
 * converging (not under `prefers-reduced-motion`). An unknown core is hollow.
 * Without a label it is decoration and hidden from assistive technology.
 */
export function LampDot({
  ring,
  core,
  pulse = false,
  size = 15,
  label,
  className,
}: {
  ring: LampTone;
  core: LampTone;
  pulse?: boolean;
  size?: number;
  label?: string;
  className?: string;
}) {
  const dotStyle = {
    '--lamp-size': `${Math.max(5, Math.round(size * CORE_SHARE))}px`,
    '--lamp-core': LAMP_COLORS[core],
    '--lamp-ring': LAMP_COLORS[ring],
  } as CSSProperties;

  return (
    <span
      role={label ? "img" : undefined}
      aria-label={label}
      aria-hidden={label ? undefined : true}
      className={cn('inline-flex shrink-0 items-center justify-center', className)}
      style={{ width: size, height: size }}
    >
      <span
        aria-hidden="true"
        className={cn('lamp', core === 'unknown' && 'lamp-hollow', pulse && 'lamp-pulse')}
        style={dotStyle}
      />
    </span>
  );
}

/**
 * The signature of the panel for a VM: ring = desired state, core = observed
 * state. It is still when they agree and pulses while the VM converges.
 */
export function Lamp({
  vm,
  size,
  className,
}: {
  vm: Pick<Vm, 'desired_status' | 'observed_status'>;
  size?: number;
  className?: string;
}) {
  const lamp = lampState(vm);
  return (
    <LampDot
      ring={lamp.ringTone}
      core={lamp.coreTone}
      pulse={lamp.pulse}
      size={size}
      label={lamp.text}
      className={className}
    />
  );
}
