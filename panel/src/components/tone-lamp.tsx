import type { CSSProperties } from 'react';
import { LAMP_COLORS, type LampTone } from '../lib/lamp';
import { cn } from '../lib/utils';

/**
 * A lamp for state that is not a VM pair: one colour, optional slow pulse.
 * Same well-known shape as the VM lamp so the chassis reads as one machine.
 */
export function ToneLamp({
  tone,
  pulse = false,
  size = 22,
  label,
  className,
}: {
  tone: LampTone;
  pulse?: boolean;
  size?: number;
  label: string;
  className?: string;
}) {
  const color = LAMP_COLORS[tone];
  return (
    <span
      role="img"
      aria-label={label}
      className={cn('relative inline-flex shrink-0 items-center justify-center', className)}
      style={{ width: size, height: size }}
    >
      <span
        aria-hidden="true"
        className={cn('absolute inset-0 rounded-full border-2', pulse && 'lamp-pulse')}
        style={{ borderColor: color, '--lamp-ring': color } as CSSProperties}
      />
      <span
        aria-hidden="true"
        className="block rounded-full"
        style={{
          width: Math.round(size * 0.42),
          height: Math.round(size * 0.42),
          backgroundColor: color,
          boxShadow: `0 0 5px -1px ${color}`,
        }}
      />
    </span>
  );
}
