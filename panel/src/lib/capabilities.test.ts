import { describe, expect, it } from 'vitest';
import type { MeResponse, User } from '../api/generated/model';
import {
  canDecideApprovals,
  canListUsers,
  canManageBackups,
  canManageProviders,
  canManageAgents,
  canManageGrants,
  canResolveIncidents,
  canManageUsers,
  canManageWebhooks,
  isHumanSession,
} from './capabilities';

const user = (role: User['role'], disabled = false): MeResponse => ({
  type: 'user',
  user: {
    id: 'usr_01JC9Z6Q5W8N3K4T7R2M1XHB0E',
    email: 'owner@example.com',
    name: 'Owner',
    role,
    disabled,
    created_at: '2026-11-02T00:00:00Z',
  },
});

const agentSession: MeResponse = {
  type: 'agent',
  agent: {
    id: 'agt_01JC9Z6Q5W8N3K4T7R2M1XHB0D',
    name: 'restarter',
    model: 'example-model-id',
    owner_user_id: 'usr_01JC9Z6Q5W8N3K4T7R2M1XHB0E',
    disabled: false,
    created_at: '2026-11-02T00:00:00Z',
    updated_at: '2026-11-02T00:00:00Z',
  },
  // Even an agent whose grants list the decide scope must not get the control.
  grants: [],
};

const writers = [
  canDecideApprovals,
  canManageAgents,
  canManageGrants,
  canManageWebhooks,
  canResolveIncidents,
  canManageProviders,
];

describe('capabilities', () => {
  it('lets an owner and an operator decide and manage', () => {
    for (const check of writers) {
      expect(check(user('owner'))).toBe(true);
      expect(check(user('operator'))).toBe(true);
    }
  });

  it('never lets a viewer decide or manage', () => {
    for (const check of writers) expect(check(user('viewer'))).toBe(false);
  });

  it('never lets an agent session decide or manage, whatever its grants', () => {
    expect(isHumanSession(agentSession)).toBe(false);
    for (const check of writers) expect(check(agentSession)).toBe(false);
    expect(canListUsers(agentSession)).toBe(false);
  });

  it('refuses a disabled human and a missing session', () => {
    for (const check of writers) {
      expect(check(user('owner', true))).toBe(false);
      expect(check(undefined)).toBe(false);
    }
  });

  it('lets every human role list users', () => {
    expect(canListUsers(user('owner'))).toBe(true);
    expect(canListUsers(user('operator'))).toBe(true);
    expect(canListUsers(user('viewer'))).toBe(true);
  });

  it('refuses a disabled human the user list', () => {
    expect(canListUsers(user('operator', true))).toBe(false);
    expect(canListUsers(undefined)).toBe(false);
  });

  it('lets only an owner change users', () => {
    expect(canManageUsers(user('owner'))).toBe(true);
    expect(canManageUsers(user('operator'))).toBe(false);
    expect(canManageUsers(user('viewer'))).toBe(false);
    expect(canManageUsers(user('owner', true))).toBe(false);
    expect(canManageUsers(agentSession)).toBe(false);
    expect(canManageUsers(undefined)).toBe(false);
  });

  it('lets only an owner manage the backup repository and its recovery key', () => {
    expect(canManageBackups(user('owner'))).toBe(true);
    expect(canManageBackups(user('operator'))).toBe(false);
    expect(canManageBackups(user('viewer'))).toBe(false);
    expect(canManageBackups(user('owner', true))).toBe(false);
    expect(canManageBackups(agentSession)).toBe(false);
    expect(canManageBackups(undefined)).toBe(false);
  });
});
