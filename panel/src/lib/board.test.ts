import { describe, expect, it } from 'vitest';
import type { App, Approval, AuditRecord, Build, NodeMetricSample, VmMetricSample } from '../api/generated/model';
import { segmentsText } from './activity';
import {
  alignSeries,
  appCpuSeries,
  approvalProjectId,
  bandAxisMax,
  bandFacts,
  gibOne,
  gibTotal,
  openFor,
  railSentence,
  splitColumns,
  sumNodeSeries,
  tileFoot,
  tileStatus,
} from './board';

const at = (seconds: number) => new Date(Date.UTC(2026, 10, 9, 12, 0, 0) + seconds * 1_000).toISOString();
const GIB = 1024 ** 3;

const node = (seconds: number, cpu: number, count: number, used = 1 * GIB, total = 4 * GIB): NodeMetricSample => ({
  time: at(seconds),
  cpu_usage: cpu,
  cpu_count: count,
  memory_used_bytes: used,
  memory_total_bytes: total,
});

describe('alignSeries', () => {
  it('puts the samples of each series in shared 5 s buckets', () => {
    const buckets = alignSeries([
      [{ time: at(0) }, { time: at(5) }],
      [{ time: at(1) }, { time: at(6) }],
    ]);
    expect(buckets.map((bucket) => bucket.samples.map((sample) => sample.time))).toEqual([
      [at(0), at(1)],
      [at(5), at(6)],
    ]);
  });

  it('carries a series that reported a moment later forward instead of dropping it', () => {
    const buckets = alignSeries([
      [{ time: at(0) }, { time: at(5) }, { time: at(10) }],
      [{ time: at(1) }, { time: at(6) }],
    ]);
    expect(buckets).toHaveLength(3);
    expect(buckets[2]!.samples.map((sample) => sample.time)).toEqual([at(10), at(6)]);
  });

  it('waits until every series has a sample before the first bucket', () => {
    const buckets = alignSeries([
      [{ time: at(0) }, { time: at(5) }, { time: at(10) }],
      [{ time: at(10) }],
    ]);
    expect(buckets.map((bucket) => bucket.time)).toEqual([Date.parse(at(10))]);
  });

  it('leaves out a series that stopped reporting, so the others keep going', () => {
    const buckets = alignSeries([
      [{ time: at(0) }, { time: at(30) }, { time: at(35) }],
      [{ time: at(0) }],
    ]);
    expect(buckets.map((bucket) => bucket.samples.length)).toEqual([1, 1, 1]);
  });

  it('skips empty series and unreadable times', () => {
    expect(alignSeries([[], [{ time: 'not a time' }]])).toEqual([]);
  });
});

describe('sumNodeSeries', () => {
  it('weights each node by its CPU count and adds up memory', () => {
    const [sample] = sumNodeSeries([
      [node(0, 0.5, 12, 10 * GIB, 24 * GIB)],
      [node(1, 0.25, 8, 3 * GIB, 8 * GIB)],
    ]);
    expect(sample!.cpu_usage).toBeCloseTo((0.5 * 12 + 0.25 * 8) / 20);
    expect(sample!.cpus).toBe(20);
    expect(sample!.memory_used_bytes).toBe(13 * GIB);
    expect(sample!.memory_total_bytes).toBe(32 * GIB);
  });

  it('keeps the total steady when the nodes take turns reporting', () => {
    const totals = sumNodeSeries([
      [node(0, 0.2, 4), node(5, 0.2, 4), node(10, 0.2, 4)],
      [node(2, 0.4, 4), node(7, 0.4, 4)],
    ]).map((sample) => sample.memory_total_bytes);
    expect(new Set(totals)).toEqual(new Set([8 * GIB]));
  });
});

describe('appCpuSeries', () => {
  const vm = (seconds: number, cpu: number, vcpus: number): VmMetricSample => ({
    time: at(seconds),
    cpu_usage: cpu,
    vcpus,
    memory_rss_bytes: 0,
    memory_bytes: 0,
  });

  it('is the busy share of all the instances’ vCPUs', () => {
    const series = appCpuSeries([{ samples: [vm(0, 0.5, 1)] }, { samples: [vm(1, 0.2, 3)] }]);
    expect(series).toHaveLength(1);
    expect(series[0]!.cpu_usage).toBeCloseTo((0.5 + 0.6) / 4);
  });

  it('is empty for an app with no instances', () => {
    expect(appCpuSeries([])).toEqual([]);
  });
});

describe('bandAxisMax', () => {
  it('fills the band from the peak, in round tens between 20 and 100', () => {
    expect(bandAxisMax(40)).toBe(60);
    expect(bandAxisMax(2)).toBe(20);
    expect(bandAxisMax(90)).toBe(100);
    expect(bandAxisMax(Number.NaN)).toBe(20);
  });
});

describe('bandFacts and GiB words', () => {
  it('counts the apps running and every VM that is not deleted', () => {
    expect(
      bandFacts(
        [{ status: 'running' }, { status: 'down' }, { status: 'running' }],
        [{ observed_status: 'running' }, { observed_status: 'deleted' }, { observed_status: 'stopped' }],
      ),
    ).toEqual({ running: 2, apps: 3, vms: 2 });
  });

  it('says GiB in use with one decimal and the total plainly', () => {
    expect(gibOne(19.94 * GIB)).toBe('19.9');
    expect(gibTotal(32 * GIB)).toBe('32');
    expect(gibTotal(7.5 * GIB)).toBe('7.5');
  });
});

describe('tileStatus', () => {
  const running = { status: 'running' } as App;
  const base = { apps: [running, running], vms: [], openIncidents: 0, lastBuildFailed: false };

  it('says how many run when all is well', () => {
    expect(tileStatus(base)).toMatchObject({ tone: 'run', words: '2 running', field: 'accent', warn: false });
  });

  it('puts an open incident first, in amber, with the edge', () => {
    expect(tileStatus({ ...base, openIncidents: 1, lastBuildFailed: true })).toMatchObject({
      tone: 'shift',
      words: 'Open incident',
      field: 'shift',
      warn: true,
    });
    expect(tileStatus({ ...base, openIncidents: 2 }).words).toBe('2 open incidents');
  });

  it('says the last build failed in red', () => {
    expect(tileStatus({ ...base, lastBuildFailed: true })).toMatchObject({
      tone: 'fault',
      words: 'Last build failed',
      field: 'fault',
    });
  });

  it('says deploying, pulsing, while an app deploys', () => {
    expect(tileStatus({ ...base, apps: [running, { status: 'deploying' } as App] })).toMatchObject({
      tone: 'shift',
      pulse: true,
      words: 'Deploying',
    });
  });

  it('keeps the counts when not everything runs', () => {
    expect(tileStatus({ ...base, apps: [running, { status: 'stopped' } as App] }).words).toBe('1 running · 1 stopped');
  });
});

describe('tileFoot', () => {
  const build = (extra: Partial<Build>): Build =>
    ({
      status: 'succeeded',
      commit: null,
      failed_step: null,
      failed_phase: null,
      error: null,
      deployment_id: null,
      created_at: at(0),
      finished_at: at(60),
      ...extra,
    }) as Build;
  const web = { name: 'web' };

  it('says the open incident before anything else', () => {
    expect(
      tileFoot({
        incidents: [
          { title: 'old', opened_at: at(0) },
          { title: 'blog is down', opened_at: at(100) },
        ],
        apps: [{ app: web, builds: [build({ status: 'failed' })] }],
      }),
    ).toEqual({ kind: 'incident', segments: [{ text: 'blog is down' }], at: at(100) });
  });

  it('says the exact step a failed build stopped at, with its commit', () => {
    const failed = build({
      status: 'failed',
      commit: '9a8b7c6d5e4f30211223344556677889900aabbc',
      failed_step: { vertex: 7, name: '[3/4] RUN ls /does-not-exist', error: 'exit 2', dockerfile_line: 4, excerpt: [] },
      finished_at: at(200),
    });
    expect(tileFoot({ incidents: [], apps: [{ app: web, builds: [failed] }] })).toEqual({
      kind: 'failed',
      segments: [{ text: 'Build of ' }, { text: '9a8b7c6', code: true }, { text: ' failed at [3/4] RUN ls /does-not-exist' }],
      at: at(200),
    });
  });

  it('says the phase when no step is known, and ignores a failure an app has since built past', () => {
    const phase = build({ status: 'failed', failed_phase: 'preparing' });
    expect(tileFoot({ incidents: [], apps: [{ app: web, builds: [phase] }] })!.segments).toEqual([
      { text: 'Build failed preparing the source' },
    ]);
    expect(tileFoot({ incidents: [], apps: [{ app: web, builds: [build({}), phase] }] })).toBeNull();
  });

  it('says the newest deployment with the commit its build built', () => {
    const built = build({ commit: '7d41e0a00000000000000000000000000000000f', deployment_id: 'dep_2' });
    expect(
      tileFoot({
        incidents: [],
        apps: [
          {
            app: web,
            builds: [built],
            deployments: [
              { id: 'dep_2', status: 'succeeded', image: 'build.quoro.invalid/app:bld', created_at: at(300), finished_at: at(320) },
              { id: 'dep_3', status: 'failed', image: 'build.quoro.invalid/app:bld2', created_at: at(400), finished_at: at(420) },
            ],
          },
          {
            app: { name: 'postgres' },
            deployments: [{ id: 'dep_1', status: 'succeeded', image: 'postgres:17', created_at: at(10), finished_at: at(20) }],
          },
        ],
      }),
    ).toEqual({
      kind: 'deployed',
      segments: [{ text: 'web deployed from ' }, { text: '7d41e0a', code: true }],
      at: at(320),
    });
  });

  it('says the image an image app deployed, and nothing when nothing is known', () => {
    expect(
      tileFoot({
        incidents: [],
        apps: [
          {
            app: { name: 'postgres' },
            deployments: [{ id: 'dep_1', status: 'succeeded', image: 'postgres:17', created_at: at(10), finished_at: null }],
          },
        ],
      })!.segments,
    ).toEqual([{ text: 'postgres deployed from ' }, { text: 'postgres:17', code: true }]);
    expect(tileFoot({ incidents: [], apps: [{ app: web }] })).toBeNull();
  });

  it('drops "ago" for how long an incident has been open', () => {
    expect(openFor('16 min ago')).toBe('16 min');
    expect(openFor('just now')).toBe('just now');
  });
});

describe('approvalProjectId', () => {
  const lists = {
    apps: [{ id: 'app_1', project_id: 'prj_b' }],
    vms: [{ id: 'vm_1', project_id: 'prj_a' }],
    projects: [{ id: 'prj_a' }, { id: 'prj_b' }],
  };
  const ask = (type: string, id: string) => ({ target: { type, id } }) as Pick<Approval, 'target'>;

  it('finds the project of an app, a VM or the project itself', () => {
    expect(approvalProjectId(ask('app', 'app_1'), lists)).toBe('prj_b');
    expect(approvalProjectId(ask('vm', 'vm_1'), lists)).toBe('prj_a');
    expect(approvalProjectId(ask('project', 'prj_a'), lists)).toBe('prj_a');
  });

  it('belongs to no tile for another target or one not listed', () => {
    expect(approvalProjectId(ask('app', 'app_9'), lists)).toBeUndefined();
    expect(approvalProjectId(ask('node', 'nod_1'), lists)).toBeUndefined();
    expect(approvalProjectId(ask('project', 'prj_x'), lists)).toBeUndefined();
  });
});

describe('splitColumns', () => {
  it('puts each tile under the shorter column, ties to the left', () => {
    const weights: Record<string, number> = { atlas: 9, blog: 6, docs: 5, new: 2 };
    expect(splitColumns(['atlas', 'blog', 'docs', 'new'], (name) => weights[name]!)).toEqual([
      ['atlas', 'new'],
      ['blog', 'docs'],
    ]);
  });

  it('gives one column everything, in order', () => {
    expect(splitColumns([1, 2, 3], () => 1, 1)).toEqual([[1, 2, 3]]);
  });
});

describe('railSentence', () => {
  const record = (extra: Partial<AuditRecord>) =>
    ({
      seq: 1,
      time: at(0),
      actor_type: 'agent',
      actor_id: 'agt_1',
      channel: 'mcp',
      operation: 'restart_app',
      target_type: 'app',
      target_id: 'app_1',
      result: 'ok',
      ...extra,
    }) as AuditRecord;
  const ctx = { name: (_type: string, id: string) => ({ agt_1: 'on-call-restarter', app_1: 'blog' })[id] };

  it('says who did what without the channel', () => {
    const text = segmentsText(railSentence(record({}), ctx));
    expect(text).toContain('on-call-restarter');
    expect(text).toContain('blog');
    expect(text).not.toMatch(/via/);
  });

  it('says when it was refused or failed', () => {
    expect(segmentsText(railSentence(record({ result: 'refused' }), ctx))).toMatch(/ · refused$/);
    expect(segmentsText(railSentence(record({ result: 'error' }), ctx))).toMatch(/ · failed$/);
  });
});
