import { describe, expect, it } from 'vitest';
import { tokenExpiresAt, tokenLamp, tokenNameProblem, tokenState } from './tokens';

const NOW = Date.parse('2026-11-06T12:00:00.000Z');

describe('tokenExpiresAt', () => {
  it('adds the chosen days to now', () => {
    expect(tokenExpiresAt('30d', NOW)).toBe('2026-12-06T12:00:00.000Z');
    expect(tokenExpiresAt('7d', NOW)).toBe('2026-11-13T12:00:00.000Z');
  });

  it('sends no expiry for never', () => {
    expect(tokenExpiresAt('never', NOW)).toBeNull();
  });
});

describe('tokenNameProblem', () => {
  it('needs a label of 1 to 200 characters after trimming', () => {
    expect(tokenNameProblem('   ')).not.toBeNull();
    expect(tokenNameProblem('n'.repeat(201))).not.toBeNull();
    expect(tokenNameProblem('  laptop  ')).toBeNull();
    expect(tokenNameProblem('n'.repeat(200))).toBeNull();
  });
});

describe('tokenState', () => {
  it('is active until it expires or is revoked', () => {
    expect(tokenState({ expires_at: null, revoked_at: null }, NOW)).toBe('active');
    expect(tokenState({ expires_at: '2026-11-07T00:00:00Z', revoked_at: null }, NOW)).toBe(
      'active',
    );
    expect(tokenState({ expires_at: '2026-11-06T11:59:59Z', revoked_at: null }, NOW)).toBe(
      'expired',
    );
  });

  it('says revoked even when the token has also expired', () => {
    expect(
      tokenState({ expires_at: '2026-01-01T00:00:00Z', revoked_at: '2025-12-01T00:00:00Z' }, NOW),
    ).toBe('revoked');
  });

  it('lights the lamp only for an active token', () => {
    expect(tokenLamp('active')).toBe('run');
    expect(tokenLamp('expired')).toBe('idle');
    expect(tokenLamp('revoked')).toBe('idle');
  });
});
