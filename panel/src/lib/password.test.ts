import { describe, expect, it } from 'vitest';
import {
  PASSWORD_MIN_LENGTH,
  passwordDetailField,
  passwordDraftProblem,
  type PasswordChangeDraft,
} from './password';

const draft = (patch: Partial<PasswordChangeDraft> = {}): PasswordChangeDraft => ({
  current: 'old-password-123',
  next: 'new-password-456',
  repeat: 'new-password-456',
  ...patch,
});

describe('passwordDraftProblem', () => {
  it('accepts a filled draft with a long, repeated, different password', () => {
    expect(passwordDraftProblem(draft())).toBeNull();
  });

  it('names each empty field', () => {
    expect(passwordDraftProblem(draft({ current: '' }))).toEqual({
      field: 'current',
      message: 'Enter your current password.',
    });
    expect(passwordDraftProblem(draft({ next: '' }))).toEqual({
      field: 'next',
      message: 'Enter a new password.',
    });
    expect(passwordDraftProblem(draft({ repeat: '' }))).toEqual({
      field: 'repeat',
      message: 'Repeat the new password.',
    });
  });

  it('requires at least twelve characters in the new password', () => {
    const short = 'a'.repeat(PASSWORD_MIN_LENGTH - 1);
    expect(passwordDraftProblem(draft({ next: short, repeat: short }))).toEqual({
      field: 'next',
      message: `Use at least ${PASSWORD_MIN_LENGTH} characters.`,
    });
    const exact = 'a'.repeat(PASSWORD_MIN_LENGTH);
    expect(passwordDraftProblem(draft({ next: exact, repeat: exact }))).toBeNull();
  });

  it('puts a mismatch on the repeat field', () => {
    expect(passwordDraftProblem(draft({ repeat: 'new-password-457' }))).toEqual({
      field: 'repeat',
      message: 'The passwords do not match.',
    });
  });

  it('refuses a new password equal to the current one', () => {
    const same = 'same-password-123';
    expect(
      passwordDraftProblem(draft({ current: same, next: same, repeat: same })),
    ).toEqual({
      field: 'next',
      message: 'The new password must differ from the current one.',
    });
  });
});

describe('passwordDetailField', () => {
  it('sends the wrong-current-password detail to the current field', () => {
    expect(passwordDetailField('the current password is wrong')).toBe('current');
  });

  it('sends every other detail to the new password field', () => {
    expect(
      passwordDetailField('the new password must be at least 12 characters'),
    ).toBe('next');
    expect(
      passwordDetailField(
        'the new password must be different from the current password',
      ),
    ).toBe('next');
  });
});
