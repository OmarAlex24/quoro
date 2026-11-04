/**
 * What the change-password form checks before it sends anything. The rules
 * mirror the contract: every field filled, the new password at least 12
 * characters, the repeat identical, and the new password different from the
 * current one. The server repeats the last three, so this only saves a round
 * trip; it never replaces the server's answer.
 */

/** The shortest password the contract accepts. */
export const PASSWORD_MIN_LENGTH = 12;

export interface PasswordChangeDraft {
  current: string;
  next: string;
  repeat: string;
}

export type PasswordField = keyof PasswordChangeDraft;

export interface PasswordFieldProblem {
  field: PasswordField;
  message: string;
}

/** The first local problem with the draft, said plainly; `null` when it is fine. */
export const passwordDraftProblem = (
  draft: PasswordChangeDraft,
): PasswordFieldProblem | null => {
  if (draft.current === '') {
    return { field: 'current', message: 'Enter your current password.' };
  }
  if (draft.next === '') {
    return { field: 'next', message: 'Enter a new password.' };
  }
  if (draft.repeat === '') {
    return { field: 'repeat', message: 'Repeat the new password.' };
  }
  if (draft.next.length < PASSWORD_MIN_LENGTH) {
    return {
      field: 'next',
      message: `Use at least ${PASSWORD_MIN_LENGTH} characters.`,
    };
  }
  if (draft.next !== draft.repeat) {
    return { field: 'repeat', message: 'The passwords do not match.' };
  }
  if (draft.next === draft.current) {
    return {
      field: 'next',
      message: 'The new password must differ from the current one.',
    };
  }
  return null;
};

/**
 * Where a `validation_failed` detail belongs on the form. The server says
 * "the current password is wrong" for a bad current password and names the
 * new password otherwise; the repeat is checked only in the browser.
 */
export const passwordDetailField = (
  detail: string,
): Exclude<PasswordField, 'repeat'> =>
  /current password is wrong/i.test(detail) ? 'current' : 'next';
