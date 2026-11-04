import type { User, UserRole } from '../api/generated/model';
import type { LampTone } from './lamp';

/**
 * The alphabet for generated initial passwords: upper- and lower-case letters
 * and digits with the look-alikes `I`, `l`, `O` and `0` left out, so a password
 * read aloud, typed from a note or copied by hand does not turn into a support
 * call.
 */
export const PASSWORD_ALPHABET =
  'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789';

/** Generated passwords are 24 characters; the contract demands at least 12. */
export const PASSWORD_LENGTH = 24;
export const PASSWORD_MIN_LENGTH = 12;

/** The longest name the contract accepts for a user. */
export const USER_NAME_MAX = 200;

/**
 * A fresh password from `crypto.getRandomValues`, rejecting the tail of the
 * byte range so every character is equally likely (no modulo bias).
 */
export const generatePassword = (length: number = PASSWORD_LENGTH): string => {
  const size = PASSWORD_ALPHABET.length;
  const limit = 256 - (256 % size);
  let password = '';
  while (password.length < length) {
    const bytes = new Uint8Array(Math.max(length, 8));
    crypto.getRandomValues(bytes);
    for (const byte of bytes) {
      if (byte >= limit) continue;
      password += PASSWORD_ALPHABET[byte % size];
      if (password.length === length) break;
    }
  }
  return password;
};

/** What is wrong with a password, said plainly; `null` when it is fine. */
export const passwordProblem = (password: string): string | null =>
  password.length < PASSWORD_MIN_LENGTH
    ? `Use at least ${PASSWORD_MIN_LENGTH} characters.`
    : null;

/** What is wrong with an email, said plainly; `null` when it is fine. */
export const emailProblem = (email: string): string | null => {
  const value = email.trim();
  if (value === '') return 'Enter the email the person will sign in with.';
  if (value.length > 320 || /\s/.test(value)) return 'Enter a valid email address.';
  const [local, domain, ...rest] = value.split('@');
  if (rest.length > 0 || !local || !domain || !domain.includes('.')) {
    return 'Enter a valid email address.';
  }
  return null;
};

/** What is wrong with a name, said plainly; `null` when it is fine. */
export const userNameProblem = (name: string): string | null => {
  const value = name.trim();
  if (value === '') return 'Give the person a name.';
  if (value.length > USER_NAME_MAX) return 'Keep the name under 200 characters.';
  return null;
};

/** The role bundle in a sentence, for the select and the invite card. */
export const ROLE_DESCRIPTIONS: Record<UserRole, string> = {
  viewer: 'Read-only: every list and log, no actions.',
  operator: 'Runs the cluster: every scope except users and roles.',
  owner: 'Everything, including users and roles.',
};

export const roleLabel = (role: UserRole): string =>
  role.charAt(0).toUpperCase() + role.slice(1);

/**
 * The text an owner hands over beside the password. It names the panel, the
 * email and the role, and never includes the password itself.
 */
export const inviteText = ({
  panelUrl,
  email,
  role,
}: {
  panelUrl: string;
  email: string;
  role: UserRole;
}): string =>
  [
    'You have a Quoro account.',
    '',
    `Panel: ${panelUrl}`,
    `Email: ${email}`,
    `Role: ${roleLabel(role)}`,
    '',
    'Sign in with the password shared separately.',
  ].join('\n');

/** A disabled user cannot sign in, so the status lamp is off. */
export const userLamp = (user: Pick<User, 'disabled'>): {
  tone: LampTone;
  statusText: string;
} =>
  user.disabled
    ? { tone: 'idle', statusText: 'Disabled' }
    : { tone: 'run', statusText: 'Enabled' };

/** The line under the invite dialog's password field. */
export const GENERATED_PASSWORD_NOTE =
  'Share the password over a separate channel. It is not shown again; the user cannot change it themselves yet, so you can reset it from this screen.';
