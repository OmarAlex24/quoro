import { MutationObserver, QueryClient } from '@tanstack/react-query';
import { describe, expect, it } from 'vitest';
import type { NodeJoinTokenCreated, NodeJoinTokenRequest } from '../api/generated/model';
import { serveMockApi } from '../mocks/node-server';
import { createJoinToken, joinTokenMutation, nodeInstallerCommand, type JoinTokenCreator } from './join-token';

const { requests } = serveMockApi();

const CREATED: NodeJoinTokenCreated = {
  id: 'njt_01JCA0A1B2C3D4E5F6G7H8J9KM',
  token: 'njt_secret',
  expires_at: '2026-11-12T19:00:00Z',
  join_command: 'quoro join https://10.0.0.3:8443 --token njt_secret --fingerprint 4f1c',
};

/** A stand-in for the generated client that remembers what each call sent. */
const recorder = () => {
  const calls: { body: NodeJoinTokenRequest | undefined; headers: unknown }[] = [];
  const create: JoinTokenCreator = (body, options) => {
    calls.push({ body, headers: options?.headers });
    return Promise.resolve(CREATED);
  };
  return { calls, create };
};

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

describe('createJoinToken', () => {
  it('sends an empty body with the Idempotency-Key header and returns the token', async () => {
    const { calls, create } = recorder();
    await expect(createJoinToken({ create, idempotencyKey: 'key-1' })).resolves.toBe(CREATED);
    expect(calls).toEqual([{ body: {}, headers: { 'Idempotency-Key': 'key-1' } }]);
  });

  it('uses a fresh UUID for every call, so a second token is a new one', async () => {
    const { calls, create } = recorder();
    await createJoinToken({ create });
    await createJoinToken({ create });
    const keys = calls.map((call) => (call.headers as Record<string, string>)['Idempotency-Key']);
    expect(keys).toHaveLength(2);
    for (const key of keys) expect(key).toMatch(UUID);
    expect(keys[0]).not.toBe(keys[1]);
  });
});

describe('joinTokenMutation', () => {
  it('asks the API for a new token on every mutate, each with its own Idempotency-Key', async () => {
    const observer = new MutationObserver(new QueryClient(), joinTokenMutation());
    const first = await observer.mutate();
    const second = await observer.mutate();

    const sent = requests.filter((request) => request.path === '/api/v1/nodes/join-tokens');
    expect(sent.map((request) => request.method)).toEqual(['POST', 'POST']);
    const keys = sent.map((request) => request.headers.get('Idempotency-Key'));
    for (const key of keys) expect(key).toMatch(UUID);
    expect(keys[0]).not.toBe(keys[1]);
    expect(await Promise.all(sent.map((request) => request.body))).toEqual(['{}', '{}']);
    expect(first.token).not.toBe(second.token);
    expect(observer.getCurrentResult().data).toBe(second);
  });
});

describe('nodeInstallerCommand', () => {
  it('is the README\'s node install, with the cluster\'s version and placeholders for the join values', () => {
    expect(nodeInstallerCommand('0.1.0')).toBe(
      'curl -fsSL https://raw.githubusercontent.com/OmarAlex24/quoro/main/installer/install.sh | sudo QUORO_ROLE=node QUORO_VERSION=0.1.0 QUORO_JOIN_URL=<url> QUORO_JOIN_TOKEN=<token> QUORO_JOIN_FINGERPRINT=<fingerprint> sh',
    );
  });

  it('leaves the version a placeholder while it is unknown', () => {
    expect(nodeInstallerCommand(undefined)).toContain(' QUORO_VERSION=<version> ');
    expect(nodeInstallerCommand('')).toContain(' QUORO_VERSION=<version> ');
  });
});
