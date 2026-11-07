import { describe, expect, it } from 'vitest';
import type { Grant } from '../api/generated/model';
import {
  accessPhrase,
  approvalAsk,
  approvalExpiry,
  grantCoversProject,
  projectAccess,
  timeLeft,
} from './access';

const NOW = Date.parse('2026-11-06T12:00:00Z');
const later = (minutes: number) => new Date(NOW + minutes * 60_000).toISOString();

const grant = (extra: Partial<Grant>): Grant =>
  ({
    id: 'grt_1',
    agent_id: 'agt_1',
    status: 'active',
    scopes: ['app:read', 'app:restart'],
    selector: { projects: ['blog'] },
    expires_at: later(60),
    ...extra,
  }) as Grant;

const blog = { slug: 'blog' };

describe('grantCoversProject', () => {
  it('covers the project named by slug, or every resource', () => {
    expect(grantCoversProject(grant({}), blog, NOW)).toBe(true);
    expect(grantCoversProject(grant({ selector: { all: true } }), blog, NOW)).toBe(true);
    expect(grantCoversProject(grant({ selector: { projects: ['docs'] } }), blog, NOW)).toBe(false);
  });

  it('does not count a grant limited to some apps as access to the project', () => {
    expect(grantCoversProject(grant({ selector: { apps: ['app_1'] } }), blog, NOW)).toBe(false);
    expect(grantCoversProject(grant({ selector: {} }), blog, NOW)).toBe(false);
  });

  it('ignores a grant that is no longer active or already expired', () => {
    expect(grantCoversProject(grant({ status: 'revoked' }), blog, NOW)).toBe(false);
    expect(grantCoversProject(grant({ expires_at: later(-1) }), blog, NOW)).toBe(false);
  });
});

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

describe('timeLeft', () => {
  it('says the largest whole unit', () => {
    expect(timeLeft(later(6 * 1440 + 300), NOW)).toBe('6 d left');
    expect(timeLeft(later(20 * 60 + 5), NOW)).toBe('20 h left');
    expect(timeLeft(later(45), NOW)).toBe('45 min left');
    expect(timeLeft(later(0.2), NOW)).toBe('1 min left');
  });
});

describe('projectAccess', () => {
  it('lists the grants that act here, soonest expiry first', () => {
    const access = projectAccess(
      [
        grant({ id: 'late', expires_at: later(600) }),
        grant({ id: 'soon', expires_at: later(30), scopes: ['vm:read'] }),
        grant({ id: 'audit', scopes: ['audit:read'] }),
        grant({ id: 'apps', selector: { apps: ['app_1'] } }),
      ],
      blog,
      NOW,
    );
    expect(access.map((entry) => [entry.grant.id, entry.phrase])).toEqual([
      ['soon', 'read VMs'],
      ['late', 'read and restart apps'],
    ]);
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

  it('says when the request expires', () => {
    expect(approvalExpiry(later(49), NOW)).toBe('Expires in 49 min.');
    expect(approvalExpiry(later(-2), NOW)).toBe('Expired 2 min ago.');
  });
});
