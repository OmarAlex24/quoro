import { describe, expect, it } from 'vitest';
import type { GrantStatus, Scope, Selector } from '../api/generated/model';
import {
  buildCreateGrant,
  expirySentence,
  grantLamp,
  grantMeter,
  grantRequestHeaders,
  grantSummary,
  TTL_PRESETS,
  ttlFromCustom,
  ttlFromPreset,
  meterGrant,
  usesWords,
  type GrantDraft,
} from './grants';

describe('grantLamp', () => {
  it('burns run while active and fault when revoked', () => {
    expect(grantLamp('active')).toBe('run');
    expect(grantLamp('revoked')).toBe('fault');
  });

  it('settles to idle when expired or exhausted', () => {
    expect(grantLamp('expired')).toBe('idle');
    expect(grantLamp('exhausted')).toBe('idle');
  });

  it('covers every status', () => {
    const statuses: GrantStatus[] = ['active', 'expired', 'revoked', 'exhausted'];
    for (const status of statuses) expect(grantLamp(status)).toBeTruthy();
  });
});

describe('usesWords', () => {
  it('says how many of the cap were used', () => {
    expect(usesWords({ uses: 4, max_uses: 10 })).toBe('4 of 10 uses');
    expect(usesWords({ uses: 1, max_uses: 1 })).toBe('1 of 1 uses');
  });

  it('says there is no cap when max_uses is null or absent', () => {
    expect(usesWords({ uses: 4, max_uses: null })).toBe('4 uses, no cap');
    expect(usesWords({ uses: 1 })).toBe('1 use, no cap');
    expect(usesWords({ uses: 0, max_uses: undefined })).toBe('0 uses, no cap');
  });
});

describe('expirySentence', () => {
  const now = Date.parse('2026-11-02T12:00:00Z');

  it('says when an active grant expires', () => {
    expect(expirySentence('2026-11-02T12:45:00Z', now)).toBe('Expires in 45 min');
  });

  it('says how long ago a grant expired', () => {
    expect(expirySentence('2026-11-02T09:00:00Z', now)).toBe('Expired 3 h ago');
  });

  it('does not invent a time for a broken timestamp', () => {
    expect(expirySentence('not-a-date', now)).toBe('Unknown expiry');
  });
});

describe('lifetime conversion', () => {
  it('maps every preset to its seconds', () => {
    for (const preset of TTL_PRESETS) {
      expect(ttlFromPreset(preset.key)).toBe(preset.seconds);
    }
    expect(ttlFromPreset('15m')).toBe(900);
    expect(ttlFromPreset('1h')).toBe(3_600);
    expect(ttlFromPreset('8h')).toBe(28_800);
    expect(ttlFromPreset('24h')).toBe(86_400);
    expect(ttlFromPreset('7d')).toBe(604_800);
  });

  it('leaves Custom to the number and unit', () => {
    expect(ttlFromPreset('custom')).toBeNull();
  });

  it('converts a custom number in minutes, hours or days', () => {
    expect(ttlFromCustom('15', 'minutes')).toBe(900);
    expect(ttlFromCustom('3', 'hours')).toBe(10_800);
    expect(ttlFromCustom('2', 'days')).toBe(172_800);
  });

  it('refuses a custom number that is not a positive integer', () => {
    expect(ttlFromCustom('', 'minutes')).toBeNull();
    expect(ttlFromCustom('0', 'minutes')).toBeNull();
    expect(ttlFromCustom('-5', 'hours')).toBeNull();
    expect(ttlFromCustom('1.5', 'days')).toBeNull();
    expect(ttlFromCustom('soon', 'days')).toBeNull();
  });
});

const draft = (patch: Partial<GrantDraft> = {}): GrantDraft => ({
  agentId: 'agt_01JC9Z6Q5W8N3K4T7R2M1XHB0D',
  scopes: ['app:read', 'app:restart'] as Scope[],
  selector: { projects: ['blog'] } as Selector,
  ttlSeconds: 3_600,
  maxUses: '',
  reason: 'Restart the blog during the on-call shift',
  ...patch,
});

describe('buildCreateGrant', () => {
  it('builds the body the spec defines, without an incident field', () => {
    const result = buildCreateGrant(draft({ maxUses: '5' }));
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.body).toEqual({
      agent_id: 'agt_01JC9Z6Q5W8N3K4T7R2M1XHB0D',
      scopes: ['app:read', 'app:restart'],
      selector: { projects: ['blog'] },
      ttl_s: 3_600,
      max_uses: 5,
      reason: 'Restart the blog during the on-call shift',
    });
    expect('incident_id' in result.body).toBe(false);
    expect('kind' in result.body).toBe(false);
  });

  it('omits max_uses when the field is empty', () => {
    const result = buildCreateGrant(draft());
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.body.max_uses).toBeUndefined();
  });

  it('accepts every resource', () => {
    const result = buildCreateGrant(draft({ selector: { all: true } }));
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.body.selector).toEqual({ all: true });
  });

  it('refuses an agent that was not chosen', () => {
    const result = buildCreateGrant(draft({ agentId: '  ' }));
    expect(result).toEqual({ ok: false, message: 'Choose an agent.' });
  });

  it('refuses no scopes', () => {
    const result = buildCreateGrant(draft({ scopes: [] }));
    expect(result).toEqual({ ok: false, message: 'Choose at least one scope.' });
  });

  it('refuses a destructive scope and says an approval is needed', () => {
    const result = buildCreateGrant(draft({ scopes: ['app:delete'] }));
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.message).toBe(
      "app:delete can't be granted: it is destructive and needs an approval.",
    );
  });

  it('refuses a human-only scope', () => {
    const result = buildCreateGrant(draft({ scopes: ['grant:write'] }));
    expect(result).toEqual({
      ok: false,
      message: "grant:write can't be granted: it is human-only.",
    });
  });

  it('refuses an empty selector', () => {
    const result = buildCreateGrant(draft({ selector: {} }));
    expect(result).toEqual({
      ok: false,
      message: 'Choose at least one resource, or every resource.',
    });
  });

  it('refuses a missing or bad lifetime', () => {
    const missing = buildCreateGrant(draft({ ttlSeconds: null }));
    expect(missing.ok).toBe(false);
    if (missing.ok) return;
    expect(missing.message).toContain('how long the grant lasts');

    for (const ttlSeconds of [0, -1, 1.5]) {
      expect(buildCreateGrant(draft({ ttlSeconds })).ok).toBe(false);
    }
  });

  it('refuses bad max uses', () => {
    for (const maxUses of ['0', '-2', '1.5', 'many']) {
      const result = buildCreateGrant(draft({ maxUses }));
      expect(result.ok).toBe(false);
      if (result.ok) continue;
      expect(result.message).toContain('Max uses');
    }
  });

  it('refuses an empty reason', () => {
    const result = buildCreateGrant(draft({ reason: '   ' }));
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.message).toContain('Enter a reason');
  });
});

describe('grantRequestHeaders', () => {
  it('always carries a fresh idempotency key', () => {
    expect(grantRequestHeaders(undefined, 'key-1')).toEqual({
      'Idempotency-Key': 'key-1',
    });
  });

  it('sends the incident in the header, not the body', () => {
    expect(
      grantRequestHeaders('inc_01JC9Z6Q5W8N3K4T7R2M1XHB0D', 'key-2'),
    ).toEqual({
      'Idempotency-Key': 'key-2',
      'Quoro-Incident': 'inc_01JC9Z6Q5W8N3K4T7R2M1XHB0D',
    });
  });
});

describe('grantSummary', () => {
  it('reads as one sentence before submitting', () => {
    expect(
      grantSummary({
        agentName: 'on-call-restarter',
        scopes: ['app:read', 'app:restart'],
        selector: { projects: ['blog'] },
        ttlSeconds: 3_600,
        maxUses: '5',
      }),
    ).toBe(
      'on-call-restarter may app:read, app:restart on project blog for 1 h, up to 5 uses.',
    );
  });

  it('says every resource and leaves the cap out when there is none', () => {
    expect(
      grantSummary({
        agentName: 'weekly-reporter',
        scopes: ['audit:read'],
        selector: { all: true },
        ttlSeconds: 604_800,
        maxUses: '',
      }),
    ).toBe('weekly-reporter may audit:read on every resource for 7 d.');
  });

  it('names the missing pieces instead of guessing', () => {
    expect(
      grantSummary({
        agentName: '',
        scopes: [],
        selector: {},
        ttlSeconds: null,
        maxUses: '0',
      }),
    ).toBe('The agent may no scopes on nothing for an unchosen lifetime.');
  });
});

describe('grantMeter', () => {
  const now = Date.UTC(2026, 10, 7, 17, 35);
  const at = (minutes: number) => new Date(now + minutes * 60_000).toISOString();

  it('says the time left of the lifetime, in minutes up to two hours', () => {
    const meter = grantMeter({ created_at: at(-18), expires_at: at(42), status: 'active' }, now);
    expect(meter).toEqual({ share: 0.7, words: '42 of 60 min left', over: false });
  });

  it('counts a day-long grant in hours and a week in days', () => {
    expect(grantMeter({ created_at: at(-4 * 60), expires_at: at(20 * 60), status: 'active' }, now)?.words).toBe(
      '20 of 24 h left',
    );
    expect(grantMeter({ created_at: at(-24 * 60), expires_at: at(6 * 24 * 60), status: 'active' }, now)?.words).toBe(
      '6 of 7 d left',
    );
  });

  it('counts the last unit in minutes', () => {
    expect(grantMeter({ created_at: at(-23.5 * 60), expires_at: at(30), status: 'active' }, now)?.words).toBe(
      '30 min of 24 h left',
    );
    expect(grantMeter({ created_at: at(-59.5), expires_at: at(0.5), status: 'active' }, now)?.words).toBe(
      '1 of 60 min left',
    );
  });

  it('is empty once the grant expired, was revoked or used up', () => {
    expect(grantMeter({ created_at: at(-70), expires_at: at(-10), status: 'active' }, now)).toEqual({
      share: 0,
      words: 'Expired',
      over: true,
    });
    expect(grantMeter({ created_at: at(-10), expires_at: at(50), status: 'revoked' }, now)?.words).toBe('Revoked');
    expect(grantMeter({ created_at: at(-10), expires_at: at(50), status: 'exhausted' }, now)?.words).toBe('Exhausted');
  });

  it('draws nothing when there is no readable expiry', () => {
    expect(grantMeter({ created_at: at(-10), expires_at: '', status: 'active' }, now)).toBeNull();
    expect(grantMeter({ created_at: 'then', expires_at: at(10), status: 'active' }, now)).toBeNull();
    expect(grantMeter({ created_at: at(10), expires_at: at(10), status: 'active' }, now)).toBeNull();
  });
});

describe('meterGrant', () => {
  it('shows an incident grant before the one that ends first', () => {
    const grants = [
      { id: 'a', kind: 'standard' as const, status: 'active' as const, expires_at: '2026-11-07T18:00:00Z' },
      { id: 'b', kind: 'incident' as const, status: 'active' as const, expires_at: '2026-11-07T19:00:00Z' },
      { id: 'c', kind: 'incident' as const, status: 'expired' as const, expires_at: '2026-11-07T17:00:00Z' },
    ];
    expect(meterGrant(grants)?.id).toBe('b');
    expect(meterGrant(grants.filter((grant) => grant.id !== 'b'))?.id).toBe('a');
    expect(meterGrant([])).toBeUndefined();
  });
});
