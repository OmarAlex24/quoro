import { describe, expect, it } from 'vitest';
import { describeSelector, expiryWords, formatDuration, shortHash, shortId } from './agency-format';

describe('agency format', () => {
  it('formats durations in the largest units', () => {
    expect(formatDuration(42)).toBe('42 s');
    expect(formatDuration(600)).toBe('10 min');
    expect(formatDuration(3_600)).toBe('1 h');
    expect(formatDuration(7_500)).toBe('2 h 5 min');
    expect(formatDuration(90_000)).toBe('1 d 1 h');
  });

  it('says expiry relative to now', () => {
    const now = Date.parse('2026-11-02T12:00:00Z');
    expect(expiryWords('2026-11-02T12:45:00Z', now)).toBe('in 45 min');
    expect(expiryWords('2026-11-02T11:48:00Z', now)).toBe('12 min ago');
    expect(expiryWords('2026-11-02T12:00:10Z', now)).toBe('now');
  });

  it('describes selectors', () => {
    expect(describeSelector({ all: true })).toBe('Every resource');
    expect(describeSelector({})).toBe('Nothing');
    expect(describeSelector({ projects: ['blog'] })).toBe('Project blog');
    expect(
      describeSelector({
        projects: ['blog', 'shop'],
        apps: ['app_01JC9Z6Q5W8N3K4T7R2M1XHB0F'],
      }),
    ).toBe('Projects blog, shop and 1 app');
  });

  it('shortens ids and hashes', () => {
    expect(shortId('agt_01JC9Z6Q5W8N3K4T7R2M1XHB0D')).toBe('agt_01JC9Z…');
    expect(shortHash('a'.repeat(56) + 'bbbbbbbb')).toBe('aaaaaaaa…bbbbbbbb');
  });
});
