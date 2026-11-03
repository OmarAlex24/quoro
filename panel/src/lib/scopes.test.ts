import { describe, expect, it } from 'vitest';
import {
  ALL_SCOPES,
  DESTRUCTIVE_SCOPES,
  HUMAN_ONLY_SCOPES,
  grantableScopes,
  grantableScopesByResource,
} from './scopes';

describe('scopes', () => {
  it('names nine human-only and four destructive scopes', () => {
    expect(HUMAN_ONLY_SCOPES.size).toBe(9);
    expect(DESTRUCTIVE_SCOPES.size).toBe(4);
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
