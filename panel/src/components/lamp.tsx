import type { CSSProperties } from 'react';
import type { Vm } from '../api/generated/model';
import { lampState } from '../lib/lamp';
import { cn } from '../lib/utils';

/**
 * The signature of the panel: outer ring = desired state, core = observed
 * state. It is still when they agree and pulses while the VM converges.
 */
export function Lamp({
  vm,
  size = 22,
  className,
}: {
  vm: Pick<Vm, 'desired_status' | 'observed_status'>;
  size?: number;
  className?: string;
}) {
  const lamp = lampState(vm);
  const ringStyle = {
    borderColor: lamp.ringColor,
    '--lamp-ring': lamp.ringColor,
  } as CSSProperties;

  return (
    <span
      role="img"
      aria-label={lamp.text}
      className={cn('relative inline-flex shrink-0 items-center justify-center', className)}
      style={{ width: size, height: size }}
    >
      <span
        aria-hidden="true"
        className={cn('absolute inset-0 rounded-full border-2', lamp.pulse && 'lamp-pulse')}
        style={ringStyle}
      />
      <span
        aria-hidden="true"
        className="block rounded-full"
        style={{
          width: Math.round(size * 0.42),
          height: Math.round(size * 0.42),
          backgroundColor: lamp.coreColor,
          boxShadow: `0 0 5px -1px ${lamp.coreColor}`,
        }}
      />
    </span>
  );
}
