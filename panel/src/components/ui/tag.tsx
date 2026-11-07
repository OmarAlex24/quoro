import type { ComponentProps } from 'react';
import { cn } from '../../lib/utils';

/**
 * A short state, role or scope word on a tinted 4 px chip. `run`, `shift`,
 * `fault` and `accent` take their tone's tint; `plain` is neutral; `code` sets
 * a code value (a scope, a key) in Martian Mono. No tone means idle.
 */
export type TagTone = 'idle' | 'run' | 'shift' | 'fault' | 'accent' | 'plain' | 'code';

export function Tag({
  tone = 'idle',
  className,
  ...props
}: ComponentProps<'span'> & { tone?: TagTone }) {
  return <span className={cn('tag', tone !== 'idle' && `tag-${tone}`, className)} {...props} />;
}
