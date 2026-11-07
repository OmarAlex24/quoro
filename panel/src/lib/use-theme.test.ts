import { describe, expect, it } from 'vitest';
import { resolveTheme } from './use-theme';

describe('resolveTheme', () => {
  it('follows the system when no attribute is set, dark by default', () => {
    expect(resolveTheme(null, false)).toBe('dark');
    expect(resolveTheme(null, true)).toBe('light');
  });

  it('lets data-theme on <html> override the system either way', () => {
    expect(resolveTheme('dark', true)).toBe('dark');
    expect(resolveTheme('light', false)).toBe('light');
  });

  it('ignores a value it does not know', () => {
    expect(resolveTheme('sepia', true)).toBe('light');
    expect(resolveTheme('', false)).toBe('dark');
  });
});
