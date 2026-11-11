import { Box, Cpu, Database, GitBranch, type LucideIcon } from 'lucide-react';
import { cn } from '../lib/utils';

/**
 * What a thing is, as a tinted icon tile: a Git source, an image, a template
 * or a virtual machine. Kind colours say what something is and never how it
 * is doing; state belongs to the lamp.
 */
export type GlyphKind = 'git' | 'image' | 'template' | 'vm';

const ICONS: Record<GlyphKind, LucideIcon> = {
  git: GitBranch,
  image: Box,
  template: Database,
  vm: Cpu,
};

const WORDS: Record<GlyphKind, string> = {
  git: 'Git repository',
  image: 'Image',
  template: 'Template',
  vm: 'Virtual machine',
};

export function GlyphTile({
  kind,
  size = 'md',
  label,
  className,
}: {
  kind: GlyphKind;
  size?: 'sm' | 'md' | 'lg';
  /** Spoken name; without one the tile is decoration next to written words. */
  label?: boolean;
  className?: string;
}) {
  const Icon = ICONS[kind];
  return (
    <span
      className={cn('glyph', `glyph-${kind}`, size === 'lg' && 'glyph-lg', size === 'sm' && 'glyph-sm', className)}
      {...(label ? { role: 'img', 'aria-label': WORDS[kind] } : { 'aria-hidden': true })}
    >
      <Icon strokeWidth={1.7} aria-hidden="true" />
    </span>
  );
}
