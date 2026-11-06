import { describe, expect, it } from 'vitest';
import permissionModel from '../../../docs/contracts/permission-model.md?raw';
import {
  ALL_SCOPES,
  DESTRUCTIVE_SCOPES,
  HUMAN_ONLY_SCOPES,
  grantableScopes,
  grantableScopesByResource,
} from './scopes';

/**
 * The scopes the contract's sentence "N scopes are <class>: ..." names, in
 * order (permission-model.md, sections 4.1 and 4.2).
 */
const contractScopes = (scopeClass: 'human-only' | 'destructive'): string[] => {
  const sentence = new RegExp(`scopes are ${scopeClass}: ([^]*?)\\(README\\.md`).exec(
    permissionModel,
  );
  expect(sentence, `permission-model.md lists the ${scopeClass} scopes`).not.toBeNull();
  return [...(sentence?.[1] ?? '').matchAll(/`([a-z_]+:[a-z_]+)`/g)].map((match) => match[1] ?? '');
};

describe('scopes', () => {
  it('matches the human-only scopes of permission-model.md, section 4.2', () => {
    const contract = contractScopes('human-only');
    expect(contract).toHaveLength(16);
    expect([...HUMAN_ONLY_SCOPES]).toEqual(contract);
  });

  it('matches the destructive scopes of permission-model.md, section 4.1', () => {
    const contract = contractScopes('destructive');
    expect(contract).toHaveLength(7);
    expect([...DESTRUCTIVE_SCOPES]).toEqual(contract);
  });

  it('names only scopes the API knows', () => {
    for (const scope of [...HUMAN_ONLY_SCOPES, ...DESTRUCTIVE_SCOPES]) {
      expect(ALL_SCOPES).toContain(scope);
    }
  });

  it('never offers the terminal, the backup key, the signing key or a restore for a grant', () => {
    const offered = grantableScopes();
    expect(offered).not.toContain('app:terminal');
    expect(offered).not.toContain('backup:manage');
    expect(offered).not.toContain('cluster:manage');
    expect(offered).not.toContain('backup:restore');
    expect(offered).toContain('backup:read');
  });

  it('never offers a human-only or destructive scope for a grant', () => {
    const offered = grantableScopes();
    for (const scope of offered) {
      expect(HUMAN_ONLY_SCOPES.has(scope)).toBe(false);
      expect(DESTRUCTIVE_SCOPES.has(scope)).toBe(false);
    }
    expect(offered).toContain('app:restart');
    expect(offered).not.toContain('app:delete');
    expect(offered).not.toContain('approval:decide');
  });

  it('keeps every other scope offerable', () => {
    const blocked = new Set([...HUMAN_ONLY_SCOPES, ...DESTRUCTIVE_SCOPES]);
    expect(grantableScopes().length).toBe(ALL_SCOPES.length - blocked.size);
  });

  it('groups by resource in contract order', () => {
    const groups = grantableScopesByResource();
    expect(groups[0]?.resource).toBe('cluster');
    const app = groups.find((group) => group.resource === 'app');
    expect(app?.scopes[0]).toBe('app:read');
  });
});
