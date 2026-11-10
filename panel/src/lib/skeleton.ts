// SPDX-License-Identifier: AGPL-3.0-only
/**
 * The widths of a skeleton's text lines, in percent of the line they stand
 * in for. Uneven on purpose, so a block reads as text and not as stripes, and
 * the same every render so nothing shifts while data loads.
 */
const PATTERN = [88, 72, 80, 64, 76] as const;

export function lineWidths(count: number, offset = 0): number[] {
  const n = Math.max(0, Math.floor(count));
  const start = Math.max(0, Math.floor(offset));
  return Array.from({ length: n }, (_, i) => PATTERN[(start + i) % PATTERN.length]!);
}
