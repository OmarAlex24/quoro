import { describe, expect, it } from 'vitest';
import { accessCovers, accessPhrase, approvalAsk } from './access';

describe('accessPhrase', () => {
  it('says what the scopes let the agent do here', () => {
    expect(accessPhrase(['app:read', 'app:restart'])).toBe('read and restart apps');
    expect(accessPhrase(['app:read', 'app:logs'])).toBe('read apps');
    expect(accessPhrase(['app:logs'])).toBe('read the logs of apps');
    expect(accessPhrase(['app:read', 'app:redeploy', 'vm:read', 'vm:start'])).toBe(
      'read and redeploy apps and read and start VMs',
    );
  });

  it('leaves out scopes that do not act inside a project', () => {
    expect(accessPhrase(['audit:read', 'event:read'])).toBeNull();
    expect(accessPhrase(['app:read', 'incident:resolve', 'approval:create'])).toBe('read apps');
  });
});

describe('accessCovers', () => {
  it('is true only when the plain phrase says every scope', () => {
    expect(accessCovers(['app:read', 'app:logs', 'app:restart', 'vm:start'])).toBe(true);
    expect(accessCovers(['app:read', 'approval:create', 'approval:read'])).toBe(false);
    expect(accessCovers(['app:terminal'])).toBe(false);
  });
});

describe('approval callout', () => {
  it('says what the agent asks, by the target name when known', () => {
    expect(
      approvalAsk({ scope: 'app:delete', target: { type: 'app', id: 'app_1' } }, 'blog'),
    ).toEqual({ wants: 'wants to delete', target: 'blog' });
    expect(
      approvalAsk(
        { scope: 'app:failover', target: { type: 'app', id: 'app_01JD5Z8Q4XK3M9V2T7B6N1R0SA' } },
        null,
      ),
    ).toEqual({ wants: 'wants to fail over', target: 'app app_01JD5Z…' });
  });
});
