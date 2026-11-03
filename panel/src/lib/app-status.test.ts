import { describe, expect, it } from 'vitest';
import type { AppStatus, DeploymentStatus } from '../api/generated/model';
import {
  appLampState,
  appsPollingInterval,
  certTone,
  certWords,
  deploymentRail,
  pickDefaultNode,
  relativeTime,
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

describe('deploymentRail', () => {
  const rail = (status: DeploymentStatus, phase: Parameters<typeof deploymentRail>[0]['phase']) =>
    deploymentRail({ status, phase });

  it('starts with every station dark while pending', () => {
    expect(rail('pending', null).stations.map((station) => station.state)).toEqual([
      'not-reached',
      'not-reached',
      'not-reached',
      'not-reached',
    ]);
    expect(rail('pending', null).live).toBe(true);
  });

  it('lights the pull station while preparing the rootfs', () => {
    expect(rail('preparing', 'pull').stations.map((station) => station.state)).toEqual([
      'current',
      'not-reached',
      'not-reached',
      'not-reached',
    ]);
    expect(rail('preparing', 'rootfs').stations.map((station) => station.state)).toEqual([
      'done',
      'current',
      'not-reached',
      'not-reached',
    ]);
  });

  it('walks through boot and the health check while starting', () => {
    expect(rail('starting', 'boot').stations.map((station) => station.state)).toEqual([
      'done',
      'done',
      'current',
      'not-reached',
    ]);
    expect(rail('starting', 'health').stations.map((station) => station.state)).toEqual([
      'done',
      'done',
      'done',
      'current',
    ]);
  });

  it('turns every station green once the deployment succeeded', () => {
    const result = rail('succeeded', 'health');
    expect(result.stations.every((station) => station.state === 'done')).toBe(true);
    expect(result.live).toBe(false);
  });

  it('reddens only the failing station and leaves the rest dark', () => {
    expect(rail('failed', 'boot').stations.map((station) => station.state)).toEqual([
      'done',
      'done',
      'failed',
      'not-reached',
    ]);
  });

  it('keeps a superseded deployment dark with a plain caption', () => {
    const result = rail('superseded', null);
    expect(result.stations.every((station) => station.state === 'not-reached')).toBe(true);
    expect(result.caption).toBe('Superseded by a newer deployment');
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
