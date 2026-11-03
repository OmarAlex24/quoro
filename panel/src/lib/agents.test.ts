import { describe, expect, it } from 'vitest';
import type { Agent } from '../api/generated/model';
import {
  agentDraftProblem,
  agentLamp,
  agentPatchIsEmpty,
  agentDisabledPatch,
  buildAgentPatch,
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

describe('agentLamp', () => {
  it('lights run and says Enabled for a live agent', () => {
    expect(agentLamp({ disabled: false })).toEqual({ tone: 'run', statusText: 'Enabled' });
  });

  it('lights idle and says Disabled for a disabled agent', () => {
    expect(agentLamp({ disabled: true })).toEqual({ tone: 'idle', statusText: 'Disabled' });
  });
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
