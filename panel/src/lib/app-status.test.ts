import { describe, expect, it } from 'vitest';
import type { App, AppDetail, AppStatus } from '../api/generated/model';
import {
  appLampState,
  appsPollingInterval,
  newestBuildLive,
  serviceLampState,
  withOpenApp,
  certTone,
  certWords,
  pickDefaultNode,
  relativeTime,
  servingTone,
  servingWords,
  stationTone,
  tlsWords,
  warmListInterval,
  warmReadInterval,
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

describe('warmReadInterval', () => {
  const warm = (interval_s: number): Pick<App, 'warm'> => ({
    warm: {
      node_id: null,
      node_pinned: false,
      interval_s,
      state: 'failed',
      last_snapshot_seq: null,
      last_shipped_at: null,
      lag_s: null,
      extent_count: null,
      error: 'no node is eligible as the warm node',
    },
  });

  it('keeps the base for an app without a warm copy', () => {
    expect(warmReadInterval(10_000, { warm: null })).toBe(10_000);
    expect(warmReadInterval(10_000, {})).toBe(10_000);
    expect(warmReadInterval(10_000, undefined)).toBe(10_000);
  });

  it('reads a warm app at its snapshot interval when that is sooner than the base', () => {
    expect(warmReadInterval(10_000, warm(8))).toBe(8_000);
    expect(warmReadInterval(60_000, warm(30))).toBe(30_000);
  });

  it('never reads more often than every 5 s for the copy', () => {
    expect(warmReadInterval(10_000, warm(5))).toBe(5_000);
    expect(warmReadInterval(10_000, warm(1))).toBe(5_000);
  });

  it('keeps a faster base, such as the 2 s poll while deploying', () => {
    expect(warmReadInterval(2_000, warm(5))).toBe(2_000);
    expect(warmReadInterval(10_000, warm(30))).toBe(10_000);
  });

  it('takes the fastest app of a list', () => {
    expect(warmListInterval(10_000, [{ warm: null }, warm(30), warm(6)])).toBe(6_000);
    expect(warmListInterval(10_000, [])).toBe(10_000);
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

describe('issue 637: the card and the sheet say the same state', () => {
  it('says Building while the newest build runs, on a first build too', () => {
    expect(serviceLampState('stopped', [{ status: 'preparing' }]).statusText).toBe('Building');
    expect(serviceLampState('running', [{ status: 'exporting' }, { status: 'succeeded' }]).statusText).toBe('Building');
    expect(serviceLampState('stopped', [{ status: 'preparing' }]).pulse).toBe(true);
  });

  it('says the app status once the build ended or the deployment started', () => {
    expect(serviceLampState('deploying', [{ status: 'importing' }]).statusText).toBe('Deploying');
    expect(serviceLampState('stopped', [{ status: 'failed' }]).statusText).toBe('Stopped');
    expect(serviceLampState('running', [{ status: 'succeeded' }, { status: 'building' }]).statusText).toBe('Running');
    expect(serviceLampState('running').statusText).toBe('Running');
    expect(newestBuildLive([])).toBe(false);
    expect(newestBuildLive(undefined)).toBe(false);
  });

  it('polls every 2 s while a build runs, even with no app deploying', () => {
    expect(appsPollingInterval([{ status: 'stopped' }], true)).toBe(2_000);
    expect(appsPollingInterval([{ status: 'stopped' }], false)).toBe(10_000);
  });
});

describe("issue 684: the sheet's header follows the tab's run", () => {
  it('says what the newest run does, whatever the app read last said', () => {
    expect(serviceLampState('running', [], 'building').statusText).toBe('Building');
    expect(serviceLampState('stopped', [{ status: 'succeeded' }], 'building').pulse).toBe(true);
    expect(serviceLampState('running', [{ status: 'exporting' }], 'deploying').statusText).toBe('Deploying');
    expect(serviceLampState('running', [], 'deploying').pulse).toBe(true);
  });

  it('lets the app speak once the run settled, whatever the shared page of builds says', () => {
    expect(serviceLampState('running', [{ status: 'building' }], null).statusText).toBe('Running');
    expect(serviceLampState('deploying', [], null).statusText).toBe('Deploying');
    expect(serviceLampState('down', [], null).statusText).toBe('Down');
  });

  it('keeps the rule from issue 637 without the lists', () => {
    expect(serviceLampState('running', [{ status: 'building' }], undefined).statusText).toBe('Building');
    expect(serviceLampState('running', [{ status: 'succeeded' }], undefined).statusText).toBe('Running');
  });

  it('draws the open app from the sheet’s read, keeping the other cards as listed', () => {
    const listed = [
      { id: 'app_web', name: 'web', status: 'stopped' },
      { id: 'app_whoami', name: 'whoami', status: 'stopped' },
    ] as App[];
    const open = { id: 'app_whoami', name: 'whoami', status: 'running', instances: [], domains: [] } as unknown as AppDetail;
    const merged = withOpenApp(listed, open);
    expect(merged.map((app) => app.status)).toEqual(['stopped', 'running']);
    expect('instances' in merged[1]!).toBe(false);
    expect(withOpenApp(listed, undefined)).toEqual(listed);
  });
});
