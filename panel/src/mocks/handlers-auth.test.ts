import { setupServer } from 'msw/node';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { ChangePasswordRequest, Problem, SetupState } from '../api/generated/model';
import { db, mockId, now } from './db';
import { handlers } from './handlers';

const server = setupServer(...handlers);

const URL = 'http://panel.test/api/v1/auth/password';

const post = (body: ChangePasswordRequest): Promise<Response> =>
  fetch(URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

beforeAll(() => server.listen());
afterAll(() => server.close());

beforeEach(() => {
  if (!db.owner) {
    db.owner = {
      id: mockId('usr'),
      email: 'owner@example.com',
      name: 'Owner',
      role: 'owner',
      disabled: false,
      created_at: now(),
    };
    db.users = [db.owner];
  }
  db.setupDone = true;
  db.password = 'current-password-1';
  db.sessionUserId = db.owner.id;
});

describe('change password mock', () => {
  it('answers 401 without a session', async () => {
    db.sessionUserId = null;
    const response = await post({
      current_password: 'current-password-1',
      new_password: 'another-password-2',
    });
    expect(response.status).toBe(401);
    const body = (await response.json()) as Problem;
    expect(body.code).toBe('unauthenticated');
  });

  it('refuses a wrong current password with a detail naming it', async () => {
    const response = await post({
      current_password: 'not-the-password',
      new_password: 'another-password-2',
    });
    expect(response.status).toBe(422);
    const body = (await response.json()) as Problem;
    expect(body.code).toBe('validation_failed');
    expect(body.detail).toBe('the current password is wrong');
    expect(db.password).toBe('current-password-1');
  });

  it('refuses a new password shorter than twelve characters', async () => {
    const response = await post({
      current_password: 'current-password-1',
      new_password: 'short',
    });
    expect(response.status).toBe(422);
    const body = (await response.json()) as Problem;
    expect(body.code).toBe('validation_failed');
    expect(body.detail).toContain('at least 12');
    expect(db.password).toBe('current-password-1');
  });

  it('refuses a new password equal to the current one', async () => {
    const response = await post({
      current_password: 'current-password-1',
      new_password: 'current-password-1',
    });
    expect(response.status).toBe(422);
    const body = (await response.json()) as Problem;
    expect(body.code).toBe('validation_failed');
    expect(body.detail).toContain('different');
    expect(db.password).toBe('current-password-1');
  });

  it('answers 204, keeps the session and signs in with the new password', async () => {
    const response = await post({
      current_password: 'current-password-1',
      new_password: 'another-password-2',
    });
    expect(response.status).toBe(204);
    expect(db.password).toBe('another-password-2');
    expect(db.sessionUserId).toBe(db.owner?.id);

    const login = await fetch('http://panel.test/api/v1/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        email: 'owner@example.com',
        password: 'another-password-2',
      }),
    });
    expect(login.status).toBe(200);
  });
});

// ADR 0293: the mock answers `getSetup` both ways, from its own setup state.
describe('getSetup mock', () => {
  const SETUP_URL = 'http://panel.test/api/v1/setup';

  const read = async (): Promise<SetupState> => {
    const response = await fetch(SETUP_URL);
    expect(response.status).toBe(200);
    return (await response.json()) as SetupState;
  };

  it('answers required: true while the cluster has no owner, and nothing else', async () => {
    db.setupDone = false;
    expect(await read()).toEqual({ required: true });
  });

  it('answers required: false once the owner exists', async () => {
    expect(await read()).toEqual({ required: false });
  });

  it('closes after a setup, which then answers conflict', async () => {
    db.setupDone = false;
    const created = await fetch(SETUP_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Quoro-Setup-Token': 'mock' },
      body: JSON.stringify({ email: 'owner@example.com', name: 'Owner', password: 'current-password-1' }),
    });
    expect(created.status).toBe(201);
    expect(await read()).toEqual({ required: false });

    const again = await fetch(SETUP_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Quoro-Setup-Token': 'mock' },
      body: JSON.stringify({ email: 'owner@example.com', name: 'Owner', password: 'current-password-1' }),
    });
    expect(again.status).toBe(409);
    expect(((await again.json()) as Problem).code).toBe('conflict');
  });
});
