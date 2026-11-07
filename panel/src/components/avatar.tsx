import { useCallback } from 'react';
import { drawAvatar, drawMark, type DitherSurface, type TokenReader } from '../lib/dither-draw';
import { initials } from '../lib/agency-format';
import { cn } from '../lib/utils';
import { DitherCanvas } from './dither-canvas';

const paintMark = (surface: DitherSurface, read: TokenReader) => drawMark(surface, read);

/**
 * Who did it, at a glance: a human is round with initials, an agent is square
 * with its own seeded two-tone dither, the system wears the Quoro mark.
 * Decorative by default: the name is always written next to it.
 */
export type AvatarProps =
  | { kind: 'human'; name: string; size?: 'md' | 'lg'; className?: string }
  | { kind: 'agent'; seed: string; size?: 'md' | 'lg'; className?: string }
  | { kind: 'system'; size?: 'md' | 'lg'; className?: string };

export function Avatar(props: AvatarProps) {
  const box = cn(
    'avatar',
    props.kind === 'human' && 'avatar-human',
    props.kind === 'agent' && 'avatar-agent',
    props.kind === 'system' && 'avatar-system',
    props.size === 'lg' && 'avatar-lg',
    props.className,
  );

  if (props.kind === 'human') {
    return (
      <span aria-hidden="true" className={box}>
        {initials(props.name)}
      </span>
    );
  }
  if (props.kind === 'agent') return <AgentAvatar seed={props.seed} className={box} />;
  return (
    <span aria-hidden="true" className={box}>
      <DitherCanvas paint={paintMark} />
    </span>
  );
}

function AgentAvatar({ seed, className }: { seed: string; className: string }) {
  const paint = useCallback(
    (surface: DitherSurface, read: TokenReader) => drawAvatar(surface, seed, read),
    [seed],
  );
  return (
    <span aria-hidden="true" className={className}>
      <DitherCanvas paint={paint} />
    </span>
  );
}

/** The Quoro mark on its own, for the header and the sign-in plate. */
export function QuoroMark({ className, label }: { className?: string; label?: string }) {
  return <DitherCanvas paint={paintMark} label={label} className={className} />;
}
