import { describe, expect, it } from 'vitest';
import type { AppStatus } from '../api/generated/model';
import {
  appLampState,
  appsPollingInterval,
  certTone,
  certWords,
  pickDefaultNode,
  relativeTime,
  servingTone,
  servingWords,
  stationTone,
  tlsWords,
} from './app-status';

describe('appLampState', () => {
  const cases: Array<[AppStatus, string, boolean]> = [
    ['running', 'run', false],
    ['deploying', 'shift', true],
    ['degraded', 'shift', false],
    ['down', 'fault', false],
    ['stopped', 'idle', false],
  ];

  it.each(cases)('maps %s to the %s lamp', (status, tone, pulse) => {
    const state = appLampState(status);
    expect(state.tone).toBe(tone);
    expect(state.pulse).toBe(pulse);
  });

  it('says down in words an operator can act on', () => {
    expect(appLampState('down').caption).toBe(
      "Down: the app's VM crashed or its health check fails",
    );
  });
});

describe('appsPollingInterval', () => {
  it('polls every 2 s while any app deploys', () => {
    expect(appsPollingInterval([{ status: 'running' }, { status: 'deploying' }])).toBe(
      2_000,
    );
  });

  it('polls every 10 s otherwise', () => {
    expect(appsPollingInterval([{ status: 'running' }, { status: 'stopped' }])).toBe(
      10_000,
    );
    expect(appsPollingInterval([])).toBe(10_000);
  });
});

describe('stationTone', () => {
  it('maps done, current, failed and not reached to the lamp palette', () => {
    expect(stationTone('done')).toBe('run');
    expect(stationTone('current')).toBe('shift');
    expect(stationTone('failed')).toBe('fault');
    expect(stationTone('not-reached')).toBe('idle');
  });
});

describe('serving words', () => {
  it('names every serving status of the contract and maps it to a lamp', () => {
    expect(servingWords.serving).toBe('Serving');
    expect(servingWords.unserved).toBe('Not served');
    expect(servingWords.pending).toBe('Waiting for an instance');
    expect(servingTone('serving')).toBe('run');
    expect(servingTone('unserved')).toBe('fault');
    expect(servingTone('pending')).toBe('idle');
  });
});

describe('certificate words', () => {
  it('names certificate states and TLS modes in plain words', () => {
    expect(certWords.pending).toBe('Pending');
    expect(certWords.not_applicable).toBe('Not applicable');
    expect(tlsWords.tunnel).toBe('Cloudflare Tunnel');
    expect(tlsWords.acme).toBe('ACME certificate');
  });

  it('maps certificate states to lamps', () => {
    expect(certTone('issued')).toBe('run');
    expect(certTone('pending')).toBe('shift');
    expect(certTone('renewing')).toBe('shift');
    expect(certTone('failed')).toBe('fault');
    expect(certTone('not_applicable')).toBe('idle');
  });
});

describe('relativeTime', () => {
  const now = Date.parse('2026-11-02T12:00:00Z');

  it('says just now for a recent change', () => {
    expect(relativeTime('2026-11-02T11:59:40Z', now)).toBe('just now');
  });

  it('counts minutes, hours and days', () => {
    expect(relativeTime('2026-11-02T11:55:00Z', now)).toBe('5 min ago');
    expect(relativeTime('2026-11-02T09:00:00Z', now)).toBe('3 h ago');
    expect(relativeTime('2026-10-30T12:00:00Z', now)).toBe('3 d ago');
  });
});

describe('pickDefaultNode', () => {
  it('prefers a connected replica over the primary', () => {
    expect(
      pickDefaultNode([
        { id: 'nod_primary', status: 'connected', cp_status: 'primary' },
        { id: 'nod_replica', status: 'connected', cp_status: 'replica' },
      ]),
    ).toBe('nod_replica');
  });

  it('falls back to the first connected node', () => {
    expect(
      pickDefaultNode([
        { id: 'nod_offline', status: 'disconnected', cp_status: 'replica' },
        { id: 'nod_primary', status: 'connected', cp_status: 'primary' },
      ]),
    ).toBe('nod_primary');
  });

  it('returns an empty id when no node is listed', () => {
    expect(pickDefaultNode([])).toBe('');
  });
});
