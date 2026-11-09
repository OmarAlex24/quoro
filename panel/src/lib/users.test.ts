import { describe, expect, it } from 'vitest';
import {
  personLabel,
  emailProblem,
  generatePassword,
  inviteText,
  PASSWORD_ALPHABET,
  PASSWORD_LENGTH,
  PASSWORD_MIN_LENGTH,
  passwordProblem,
  roleLabel,
  ROLE_DESCRIPTIONS,
  userNameProblem,
  userLamp,
} from './users';

describe('generated passwords', () => {
  it('are 24 characters drawn only from the unambiguous alphabet', () => {
    for (let round = 0; round < 20; round += 1) {
      const password = generatePassword();
      expect(password).toHaveLength(PASSWORD_LENGTH);
      for (const character of password) {
        expect(PASSWORD_ALPHABET).toContain(character);
      }
    }
  });

  it('leaves out the look-alikes I, l, O and 0', () => {
    for (const character of 'I l O 0'.split(' ')) {
      expect(PASSWORD_ALPHABET).not.toContain(character);
    }
  });

  it('differs from one call to the next', () => {
    expect(generatePassword()).not.toBe(generatePassword());
  });

  it('honours a shorter or longer length', () => {
    expect(generatePassword(16)).toHaveLength(16);
    expect(generatePassword(64)).toHaveLength(64);
  });

  it('demands at least 12 characters', () => {
    expect(passwordProblem('short')).toBe('Use at least 12 characters.');
    expect(passwordProblem('a'.repeat(PASSWORD_MIN_LENGTH))).toBeNull();
  });
});

describe('email and name', () => {
  it('accepts a normal address and refuses a malformed one', () => {
    expect(emailProblem('ada@example.com')).toBeNull();
    expect(emailProblem('ada@example')).toBe('Enter a valid email address.');
    expect(emailProblem('ada example.com')).toBe('Enter a valid email address.');
    expect(emailProblem('')).toBe('Enter the email the person will sign in with.');
    expect(emailProblem('a@b@c.com')).toBe('Enter a valid email address.');
  });

  it('requires a name under 200 characters', () => {
    expect(userNameProblem('Ada')).toBeNull();
    expect(userNameProblem('  ')).toBe('Give the person a name.');
    expect(userNameProblem('a'.repeat(201))).toBe('Keep the name under 200 characters.');
  });
});

describe('roles and invite text', () => {
  it('names each fixed scope bundle', () => {
    expect(roleLabel('operator')).toBe('Operator');
    expect(Object.keys(ROLE_DESCRIPTIONS)).toEqual(['viewer', 'operator', 'owner']);
  });

  it('puts the panel, email and role in the invite text without a password', () => {
    const text = inviteText({
      panelUrl: 'https://panel.example.com',
      email: 'ada@example.com',
      role: 'operator',
    });
    expect(text).toContain('https://panel.example.com');
    expect(text).toContain('ada@example.com');
    expect(text).toContain('Role: Operator');
    expect(text).not.toContain('password:');
  });

  it('lights a disabled user idle and an enabled one run', () => {
    expect(userLamp({ disabled: true })).toEqual({ tone: 'idle', statusText: 'Disabled' });
    expect(userLamp({ disabled: false })).toEqual({ tone: 'run', statusText: 'Enabled' });
  });
});

describe('personLabel', () => {
  it('names a person the way the deployment card does, with (you) for the one signed in', () => {
    expect(personLabel('usr_01M70BF53B541C2D4X0EWYCFXF', 'Lab owner', true)).toBe('Lab owner (you)');
    expect(personLabel('usr_01M70BF53B541C2D4X0EWYCFXF', 'Ada Moreno')).toBe('Ada Moreno');
  });

  it('shows a short id, never a raw one, until a name is known', () => {
    expect(personLabel('usr_01M70BF53B541C2D4X0EWYCFXF', null)).toBe('usr_01M70B…');
  });
});
