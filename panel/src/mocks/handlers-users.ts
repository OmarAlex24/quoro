import { http, type HttpHandler } from 'msw';
import type {
  CreateUserRequest,
  UpdateUserRequest,
  User,
  UserList,
  UserRole,
} from '../api/generated/model';
import { db, mockId, now } from './db';
import {
  conflict,
  json,
  noContent,
  notFound,
  readJson,
  signedIn,
  unauthorized,
  validationFailed,
  writeRefusal,
} from './http';

const ROLES: ReadonlySet<string> = new Set(['owner', 'operator', 'viewer']);
const NAME_MAX = 200;
const PASSWORD_MIN = 12;

const isRole = (value: unknown): value is UserRole =>
  typeof value === 'string' && ROLES.has(value);

/** Only an owner holds `user:write`; a viewer or an operator is refused. */
const userWriter = (): Response | null => {
  const user = signedIn();
  if (!user) return unauthorized();
  if (user.role !== 'owner') return writeRefusal('user:write');
  return null;
};

const findUser = (userId: string): User | undefined =>
  db.users.find((candidate) => candidate.id === userId);

const emailIsValid = (email: string): boolean => {
  const value = email.trim();
  if (value === '' || value.length > 320 || /\s/.test(value)) return false;
  const [local, domain, ...rest] = value.split('@');
  return rest.length === 0 && !!local && !!domain && domain.includes('.');
};

/** One page of users in creation order, with an offset cursor while more remain. */
const pageUsers = (items: User[], request: Request): UserList => {
  const url = new URL(request.url);
  const limitParam = Number(url.searchParams.get('limit') ?? '50');
  const limit = Number.isInteger(limitParam) && limitParam > 0 ? limitParam : 50;
  const cursorParam = Number(url.searchParams.get('cursor') ?? '0');
  const start = Number.isInteger(cursorParam) && cursorParam > 0 ? cursorParam : 0;
  const pageItems = items.slice(start, start + limit);
  const next = start + pageItems.length;
  return {
    items: pageItems,
    next_cursor: next < items.length ? String(next) : null,
  };
};

/** Would leaving this user out leave the cluster with no enabled owner? */
const orphansOwners = (
  target: User,
  next: Pick<User, 'role' | 'disabled'>,
): boolean => {
  const otherEnabledOwners = db.users.filter(
    (candidate) =>
      candidate.id !== target.id &&
      candidate.role === 'owner' &&
      !candidate.disabled,
  );
  const targetStaysEnabledOwner = next.role === 'owner' && !next.disabled;
  return otherEnabledOwners.length === 0 && !targetStaysEnabledOwner;
};

/** Stateful mock handlers for the users screen. Mock mode only. */
export const usersHandlers: HttpHandler[] = [
  http.get('*/api/v1/users', ({ request }) => {
    if (!signedIn()) return unauthorized();
    return json(pageUsers(db.users, request));
  }),

  http.post('*/api/v1/users', async ({ request }) => {
    const refusal = userWriter();
    if (refusal) return refusal;

    const body = await readJson<CreateUserRequest>(request);
    if (!body) return validationFailed('The form could not be read. Try again.');

    const email = (body.email ?? '').trim().toLowerCase();
    const name = (body.name ?? '').trim();
    const password = body.password ?? '';

    if (!emailIsValid(email)) return validationFailed('Enter a valid email address.');
    if (name === '' || name.length > NAME_MAX) {
      return validationFailed('User name must be 1-200 characters.');
    }
    if (!isRole(body.role)) return validationFailed('Choose a role.');
    if (password.length < PASSWORD_MIN) {
      return validationFailed('Password must be at least 12 characters.');
    }
    if (db.users.some((user) => user.email.toLowerCase() === email)) {
      return conflict('A user with that email already exists.');
    }

    const user: User = {
      id: mockId('usr'),
      email,
      name,
      role: body.role,
      disabled: body.disabled ?? false,
      created_at: now(),
    };
    db.users.push(user);

    // No password is stored: the mock has no login for invited users.
    return json(user, { status: 201 });
  }),

  http.patch('*/api/v1/users/:userId', async ({ request, params }) => {
    const refusal = userWriter();
    if (refusal) return refusal;

    const user = findUser(String(params.userId));
    if (!user) return notFound('That user');

    const body = await readJson<UpdateUserRequest>(request);
    if (!body) return validationFailed('The form could not be read. Try again.');

    if (body.email !== undefined) {
      const email = body.email.trim().toLowerCase();
      if (!emailIsValid(email)) return validationFailed('Enter a valid email address.');
      if (
        db.users.some(
          (candidate) =>
            candidate.id !== user.id && candidate.email.toLowerCase() === email,
        )
      ) {
        return conflict('A user with that email already exists.');
      }
    }
    if (body.name !== undefined) {
      const name = body.name.trim();
      if (name === '' || name.length > NAME_MAX) {
        return validationFailed('User name must be 1-200 characters.');
      }
    }
    if (body.role !== undefined && !isRole(body.role)) {
      return validationFailed('Choose a role.');
    }
    if (body.password !== undefined && body.password.length < PASSWORD_MIN) {
      return validationFailed('Password must be at least 12 characters.');
    }
    if (body.disabled !== undefined && typeof body.disabled !== 'boolean') {
      return validationFailed('Disabled must be true or false.');
    }

    const next = {
      role: body.role ?? user.role,
      disabled: body.disabled ?? user.disabled,
    };
    if (orphansOwners(user, next)) {
      return conflict(
        'That would leave no enabled owner. Make another user an owner first.',
      );
    }

    if (body.email !== undefined) user.email = body.email.trim().toLowerCase();
    if (body.name !== undefined) user.name = body.name.trim();
    if (body.role !== undefined) user.role = body.role;
    if (body.disabled !== undefined) user.disabled = body.disabled;

    return json(user);
  }),

  http.delete('*/api/v1/users/:userId', ({ params }) => {
    const refusal = userWriter();
    if (refusal) return refusal;

    const userId = String(params.userId);
    const user = findUser(userId);
    if (!user) return notFound('That user');

    // The owner created at setup owns the seeded agents and grants: the mock
    // keeps it undeletable, as a real referenced user would be.
    if (db.owner && userId === db.owner.id) {
      return conflict(
        'This user is referenced by agents or grants. Disable it instead.',
      );
    }
    if (orphansOwners(user, { role: 'viewer', disabled: true })) {
      return conflict(
        'That would leave no enabled owner. Make another user an owner first.',
      );
    }

    db.users = db.users.filter((candidate) => candidate.id !== userId);
    return noContent();
  }),
];
