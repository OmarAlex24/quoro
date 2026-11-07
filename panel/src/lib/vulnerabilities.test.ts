import { describe, expect, it } from 'vitest';
import type {
  ImageInventoryStatus,
  VulnerabilityCounts,
  VulnerabilityDataSource,
  VulnerabilityFinding,
  VulnerabilitySeverity,
} from '../api/generated/model';
import {
  dataAgeSentence,
  guestKernelFindings,
  guestKernelIsClear,
  groupFindings,
  guestKernelVersion,
  inventorySentence,
  nodeSecurityCount,
  openFindingCount,
  parsePins,
  resolvedReasonWords,
  securityParams,
  securityTabCount,
  severityLabel,
  severityRank,
  severityTone,
} from './vulnerabilities';

const PINS = `# Pinned artifacts for the Cloud Hypervisor engine. The single source of truth:
# read by crates/engine/src/pins.rs (include_str!) and by
# crates/engine/scripts/fetch-pinned.sh.
#
# name version arch file url sha256
cloud-hypervisor v54.0 x86_64 cloud-hypervisor-static https://example.test/cloud-hypervisor v54sha
guest-kernel 7.2.15-q1 x86_64 vmlinux-x86_64 https://example.test/vmlinux-x86_64 kernel-sha
`;

const finding = (overrides: Partial<VulnerabilityFinding> = {}): VulnerabilityFinding => ({
  id: 'vln_01JC9Z6Q5W7X8Y9Z0A1B2C3D4E',
  subject_kind: 'app',
  vuln_id: 'CVE-2025-1111',
  aliases: [],
  ecosystem: 'Debian:13',
  package: 'openssl',
  installed_version: '3.0.16-1',
  fixed_version: '3.0.16-2',
  severity: 'critical',
  cvss_score: 9.8,
  kev: true,
  epss: null,
  first_seen_at: '2026-01-01T00:00:00Z',
  resolved_at: null,
  resolved_reason: null,
  ...overrides,
});

const inventory = (
  overrides: Partial<ImageInventoryStatus> = {},
): ImageInventoryStatus => ({ status: 'inventoried', ...overrides });

const counts = (overrides: Partial<VulnerabilityCounts> = {}): VulnerabilityCounts => ({
  critical: 0,
  high: 0,
  medium: 0,
  low: 0,
  unknown: 0,
  kev: 0,
  ...overrides,
});

describe('parsePins', () => {
  it('reads the data lines and skips comments and blank lines', () => {
    const artifacts = parsePins(PINS);
    expect(artifacts).toHaveLength(2);
    expect(artifacts[0]).toEqual({
      name: 'cloud-hypervisor',
      version: 'v54.0',
      arch: 'x86_64',
      file: 'cloud-hypervisor-static',
      url: 'https://example.test/cloud-hypervisor',
      sha256: 'v54sha',
    });
  });

  it('skips a line that does not carry all six fields', () => {
    expect(parsePins('guest-kernel 7.2.15-q1 x86_64\n')).toEqual([]);
  });
});

describe('guestKernelVersion', () => {
  it('names the pinned guest kernel', () => {
    expect(guestKernelVersion(PINS)).toBe('7.2.15-q1');
  });

  it('is null when the file names no guest kernel', () => {
    const withoutGuest = PINS.split('\n')
      .filter((line) => !line.startsWith('guest-kernel'))
      .join('\n');
    expect(guestKernelVersion(withoutGuest)).toBeNull();
  });

  it('is null for an empty file', () => {
    expect(guestKernelVersion('')).toBeNull();
  });
});

describe('severity', () => {
  it('ranks critical above high above medium above low above unknown', () => {
    const order: VulnerabilitySeverity[] = [
      'critical',
      'high',
      'medium',
      'low',
      'unknown',
    ];
    const ranks = order.map(severityRank);
    expect(ranks).toEqual([5, 4, 3, 2, 1]);
  });

  it.each([
    ['critical', 'Critical', 'fault'],
    ['high', 'High', 'shift'],
    ['medium', 'Medium', 'shift'],
    ['low', 'Low', 'idle'],
    ['unknown', 'Unknown', 'unknown'],
  ] as const)('labels and tones %s', (severity, label, tone) => {
    expect(severityLabel(severity)).toBe(label);
    expect(severityTone(severity)).toBe(tone);
  });
});

describe('inventorySentence', () => {
  it('says when no image is deployed', () => {
    expect(inventorySentence(inventory({ status: 'no_image' }))).toBe(
      'No image deployed yet.',
    );
  });

  it('says the node has not listed the packages yet', () => {
    expect(inventorySentence(inventory({ status: 'pending' }))).toBe(
      "Waiting for the node to list this image's packages.",
    );
  });

  it('says the image has no package database', () => {
    expect(inventorySentence(inventory({ status: 'not_inventoried' }))).toBe(
      'No package database was found in this image, so it cannot be checked.',
    );
  });

  it('names the ecosystems whose data is still coming', () => {
    expect(
      inventorySentence(
        inventory({ status: 'pending_data', ecosystems: ['Debian:13', 'npm'] }),
      ),
    ).toBe('Fetching vulnerability data for Debian:13, npm.');
  });

  it('falls back when pending_data names no ecosystem', () => {
    expect(inventorySentence(inventory({ status: 'pending_data' }))).toBe(
      'Fetching vulnerability data for this image.',
    );
  });

  it('counts the packages and names the OS', () => {
    expect(
      inventorySentence(
        inventory({ status: 'inventoried', package_count: 214, os: 'debian 13' }),
      ),
    ).toBe('214 packages checked (debian 13).');
  });

  it('counts the packages when the OS is unknown', () => {
    expect(
      inventorySentence(inventory({ status: 'inventoried', package_count: 0 })),
    ).toBe('0 packages checked.');
  });
});

describe('dataAgeSentence', () => {
  const source = (overrides: Partial<VulnerabilityDataSource> = {}): VulnerabilityDataSource => ({
    ecosystem: 'Debian:13',
    source: 'download',
    data_modified_at: '2026-01-01T00:00:00Z',
    refreshed_at: '2026-01-01T00:00:00Z',
    last_error: null,
    ...overrides,
  });

  it('dates the data from its newest record', () => {
    const now = Date.parse('2026-01-01T05:00:00Z');
    expect(dataAgeSentence(source(), now)).toBe('Debian:13: data from 5 h ago');
  });

  it('says a source has no data yet', () => {
    expect(dataAgeSentence(source({ data_modified_at: null }))).toBe(
      'Debian:13: no data yet',
    );
  });
});

describe('resolvedReasonWords', () => {
  it.each([
    ['image_changed', 'the app runs another image'],
    ['data_changed', 'the data no longer matches'],
    ['app_deleted', 'the app was deleted'],
    ['node_removed', 'the node was removed'],
    ['upgraded', 'the package was upgraded'],
    [null, 'it no longer applies'],
  ] as const)('says %s in plain words', (reason, words) => {
    expect(resolvedReasonWords(reason)).toBe(words);
  });
});

describe('guest kernel findings', () => {
  it('picks the guest-kernel component out of the release findings', () => {
    const items = [
      finding({ id: 'vln_1', component: 'cloud-hypervisor' }),
      finding({ id: 'vln_2', component: 'guest-kernel' }),
    ];
    expect(guestKernelFindings(items).map((item) => item.id)).toEqual(['vln_2']);
    expect(guestKernelIsClear(items)).toBe(false);
  });

  it('is clear when no finding names the guest kernel', () => {
    expect(guestKernelIsClear([finding({ component: 'h2' })])).toBe(true);
    expect(guestKernelIsClear([])).toBe(true);
  });
});

describe('openFindingCount', () => {
  it('adds every severity and leaves the KEV count out', () => {
    expect(
      openFindingCount(counts({ critical: 1, high: 2, medium: 3, low: 4, unknown: 5, kev: 6 })),
    ).toBe(15);
  });
});

describe('groupFindings', () => {
  const finding = (
    vuln_id: string,
    severity: VulnerabilityFinding['severity'],
    resolved = false,
  ): VulnerabilityFinding => ({
    id: `vln_${vuln_id}`,
    subject_kind: 'app',
    vuln_id,
    aliases: [],
    ecosystem: 'npm',
    package: 'tar',
    installed_version: '6.2.0',
    fixed_version: null,
    severity,
    cvss_score: null,
    kev: false,
    epss: null,
    first_seen_at: '2026-11-01T00:00:00Z',
    resolved_at: resolved ? '2026-11-02T00:00:00Z' : null,
    resolved_reason: resolved ? 'upgraded' : null,
  });

  it('groups open findings by severity, most severe first, and the resolved last', () => {
    const groups = groupFindings([
      finding('A', 'high'),
      finding('B', 'low'),
      finding('C', 'critical'),
      finding('D', 'high'),
      finding('E', 'medium', true),
    ]);
    expect(groups.map((group) => [group.label, group.items.map((item) => item.vuln_id)])).toEqual([
      ['Critical', ['C']],
      ['High', ['A', 'D']],
      ['Low', ['B']],
      ['Resolved', ['E']],
    ]);
  });

  it('has no group for an empty report', () => {
    expect(groupFindings([])).toEqual([]);
  });
});

describe('securityTabCount', () => {
  const counts = { critical: 0, high: 1, medium: 2, low: 0, unknown: 0, kev: 1 };

  it('counts the open findings', () => {
    expect(securityTabCount({ counts })).toBe(3);
  });

  it('shows nothing for zero or for a report that has not arrived', () => {
    expect(securityTabCount({ counts: { ...counts, high: 0, medium: 0 } })).toBeNull();
    expect(securityTabCount(undefined)).toBeNull();
  });
});

describe('securityParams', () => {
  it('asks for open findings of every severity by default, with no undefined keys', () => {
    expect(securityParams()).toEqual({ limit: 50 });
    expect(Object.keys(securityParams())).toEqual(['limit']);
  });

  it('maps each filter to the report query', () => {
    expect(securityParams('high', true, true)).toEqual({
      severity: 'high',
      kev: true,
      include_resolved: true,
      limit: 50,
    });
    expect(securityParams('critical')).toEqual({ severity: 'critical', limit: 50 });
  });
});

describe('nodeSecurityCount', () => {
  const counts = (high: number, low = 0) => ({ critical: 0, high, medium: 0, low, unknown: 0, kev: 0 });

  it('adds the release and every host kernel, and nothing of the apps', () => {
    expect(
      nodeSecurityCount({
        quoro: { version: '0.1.0', counts: counts(1), items: [] },
        host_kernels: [
          { node_id: 'nod_a', kernel: '6.12', counts: counts(1, 2), items: [] },
          { node_id: 'nod_b', kernel: '6.12', counts: counts(0), items: [] },
        ],
      }),
    ).toBe(4);
  });

  it('is null when the summary has no node data or is not there', () => {
    expect(nodeSecurityCount({})).toBeNull();
    expect(nodeSecurityCount(undefined)).toBeNull();
    expect(nodeSecurityCount({ host_kernels: [] })).toBe(0);
  });
});
