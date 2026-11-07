import { describe, expect, it } from 'vitest';
import type { Agent } from '../api/generated/model';
import {
  agentDraftProblem,
  agentPatchIsEmpty,
  agentDisabledPatch,
  agentStatus,
  buildAgentPatch,
  callBuckets,
  isAgentKeyId,
} from './agents';

const agent = (patch: Partial<Agent> = {}): Agent => ({
  id: 'agt_01JC9Z6Q5W7X8Y9Z0A1B2C3D4E',
  name: 'on-call-restarter',
  model: 'claude-sonnet-5-5',
  owner_user_id: 'usr_01JC9Z6Q5W7X8Y9Z0A1B2C3D4E',
  description: 'Restarts an app when it goes down.',
  disabled: false,
  created_at: '2026-11-02T10:00:00Z',
  updated_at: '2026-11-02T10:00:00Z',
  ...patch,
});

const draft = (patch: Partial<{ name: string; model: string; description: string }> = {}) => ({
  name: 'on-call-restarter',
  model: 'claude-sonnet-5-5',
  description: 'Restarts an app when it goes down.',
  ...patch,
});

describe('agentDraftProblem', () => {
  it('accepts a complete draft', () => {
    expect(agentDraftProblem(draft())).toBeNull();
  });

  it('requires a name and a model', () => {
    expect(agentDraftProblem(draft({ name: '   ' }))).toBe('Give the agent a name.');
    expect(agentDraftProblem(draft({ model: ' ' }))).toBe(
      'Enter the model identifier the agent runs.',
    );
  });

  it('keeps the name under 200 and the description under 2000 characters', () => {
    expect(agentDraftProblem(draft({ name: 'a'.repeat(201) }))).toBe(
      'Keep the name under 200 characters.',
    );
    expect(agentDraftProblem(draft({ description: 'a'.repeat(2_001) }))).toBe(
      'Keep the description under 2000 characters.',
    );
  });
});

describe('buildAgentPatch', () => {
  it('sends nothing when the draft matches the agent', () => {
    const patch = buildAgentPatch(agent(), draft());
    expect(patch).toEqual({});
    expect(agentPatchIsEmpty(patch)).toBe(true);
  });

  it('sends only the fields that changed', () => {
    expect(buildAgentPatch(agent(), draft({ model: 'claude-haiku-5-5' }))).toEqual({
      model: 'claude-haiku-5-5',
    });
    expect(
      buildAgentPatch(agent(), draft({ name: 'restarter', description: 'Only restarts.' })),
    ).toEqual({ name: 'restarter', description: 'Only restarts.' });
  });

  it('trims the values it sends', () => {
    expect(buildAgentPatch(agent(), draft({ name: '  restarter  ' }))).toEqual({
      name: 'restarter',
    });
  });

  it('sends an empty description to clear it', () => {
    const patch = buildAgentPatch(agent({ description: 'Old text' }), draft({ description: '' }));
    expect(patch).toEqual({ description: '' });
  });

  it('treats a missing stored description as empty', () => {
    expect(
      buildAgentPatch(agent({ description: null }), draft({ description: '' })),
    ).toEqual({});
  });

  it('builds the disabled body', () => {
    expect(agentDisabledPatch(true)).toEqual({ disabled: true });
    expect(agentDisabledPatch(false)).toEqual({ disabled: false });
  });
});

describe('isAgentKeyId', () => {
  it('accepts akey_ plus 26 Crockford characters', () => {
    expect(isAgentKeyId('akey_01JC9Z6Q5W7X8Y9Z0A1B2C3D4E')).toBe(true);
    expect(isAgentKeyId('  akey_01JC9Z6Q5W7X8Y9Z0A1B2C3D4E  ')).toBe(true);
  });

  it('rejects the wrong prefix, length or alphabet', () => {
    expect(isAgentKeyId('grt_01JC9Z6Q5W7X8Y9Z0A1B2C3D4E')).toBe(false);
    expect(isAgentKeyId('akey_01JC9Z6Q5W7X8Y9Z0A1B2C3D4')).toBe(false);
    expect(isAgentKeyId('akey_01JC9Z6Q5W7X8Y9Z0A1B2C3D4E5')).toBe(false);
    expect(isAgentKeyId('akey_01JC9Z6Q5W7X8Y9Z0A1B2C3D4O')).toBe(false);
    expect(isAgentKeyId('')).toBe(false);
  });
});

describe('agentStatus', () => {
  const ask = { scope: 'app:delete' as const, target: { type: 'app', id: 'app_01JC9Z6Q5W7X8Y9Z0A1B2C3D4E' } };

  it('puts a disabled agent first, with its grants counted', () => {
    expect(agentStatus({ agent: { disabled: true }, activeGrants: 0, pending: [ask] })).toEqual({
      tone: 'idle',
      pulse: false,
      attention: false,
      phrase: 'Disabled · no active grants',
    });
    expect(agentStatus({ agent: { disabled: true }, activeGrants: 2, pending: [] }).phrase).toBe(
      'Disabled · 2 active grants',
    );
  });

  it('says what a waiting request asks, and breathes', () => {
    const status = agentStatus({ agent: { disabled: false }, activeGrants: 1, pending: [ask], pendingTargetName: 'blog' });
    expect(status).toEqual({ tone: 'shift', pulse: true, attention: true, phrase: 'Waiting on you · delete blog' });
    expect(
      agentStatus({ agent: { disabled: false }, activeGrants: 1, pending: [ask, ask], pendingTargetName: 'blog' }).phrase,
    ).toBe('Waiting on you · 2 requests');
    expect(agentStatus({ agent: { disabled: false }, activeGrants: 1, pending: [ask] }).phrase).toBe(
      'Waiting on you · delete app',
    );
  });

  it('names the incident it works when nothing waits', () => {
    expect(
      agentStatus({ agent: { disabled: false }, activeGrants: 2, pending: [], incident: { title: 'blog is down' } }),
    ).toEqual({ tone: 'shift', pulse: false, attention: false, phrase: 'On an incident · blog is down' });
  });

  it('counts its grants otherwise', () => {
    expect(agentStatus({ agent: { disabled: false }, activeGrants: 1, pending: [] }).phrase).toBe('Ready · 1 active grant');
    expect(agentStatus({ agent: { disabled: false }, activeGrants: 0, pending: [] })).toMatchObject({
      tone: 'idle',
      phrase: 'Enabled · no active grants',
    });
  });
});

describe('callBuckets', () => {
  const now = Date.UTC(2026, 10, 7, 18, 0);
  const ago = (hours: number) => new Date(now - hours * 3_600_000).toISOString();

  it('counts calls per hour, oldest hour first', () => {
    const { buckets, total } = callBuckets([ago(0.1), ago(0.9), ago(1.5), ago(23.5)], now);
    expect(buckets).toHaveLength(24);
    expect(buckets[23]).toBe(2);
    expect(buckets[22]).toBe(1);
    expect(buckets[0]).toBe(1);
    expect(total).toBe(4);
  });

  it('leaves out what is older than the window or unreadable', () => {
    const { buckets, total } = callBuckets([ago(24), ago(30), 'yesterday'], now);
    expect(total).toBe(0);
    expect(buckets.every((count) => count === 0)).toBe(true);
  });

  it('counts a time slightly ahead of this clock in the last hour', () => {
    expect(callBuckets([new Date(now + 20_000).toISOString()], now).buckets[23]).toBe(1);
  });

  it('takes another window length', () => {
    expect(callBuckets([ago(2.5)], now, 6).buckets).toEqual([0, 0, 0, 1, 0, 0]);
  });
});
