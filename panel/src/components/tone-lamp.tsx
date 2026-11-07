import type { LampTone } from '../lib/lamp';
import { LampDot } from './lamp';

/**
 * A lamp for state that is not a VM pair: ring and core in one tone, with an
 * optional slow pulse. Same shape as the VM lamp so the panel reads as one.
 */
export function ToneLamp({
  tone,
  pulse = false,
  size,
  label,
  className,
}: {
  tone: LampTone;
  pulse?: boolean;
  size?: number;
  label: string;
  className?: string;
}) {
  return (
    <LampDot ring={tone} core={tone} pulse={pulse} size={size} label={label} className={className} />
  );
}
