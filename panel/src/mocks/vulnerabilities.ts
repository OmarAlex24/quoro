// SPDX-License-Identifier: AGPL-3.0-only
/**
 * Vulnerability fixtures for mock mode (ADR 0150). The Git-built mock app has
 * a critical KEV finding in openssl, the running release names no guest-kernel
 * finding, and the primary's host kernel has one upstream-version finding.
 * A rebuild with `refresh_base` clears the app's findings for the rest of the
 * session, as a patched base image would. Mock mode only.
 */
import type {
  App,
  AppVulnerabilitySummary,
  ImageInventoryStatus,
  NodeKernelVulnerabilities,
  QuoroVulnerabilities,
  VulnerabilityCounts,
  VulnerabilityDataSource,
  VulnerabilityFinding,
  VulnerabilitySeverity,
  VulnerabilitySummary,
} from '../api/generated/model';
import { db, mockId } from './db';

const ago = (hours: number): string =>
  new Date(Date.now() - hours * 3_600_000).toISOString();

const emptyCounts = (): VulnerabilityCounts => ({
  critical: 0,
  high: 0,
  medium: 0,
  low: 0,
  unknown: 0,
  kev: 0,
});

/** Open findings by severity, with the KEV count, as the contract defines it. */
export const countsOf = (
  findings: readonly VulnerabilityFinding[],
): VulnerabilityCounts => {
  const counts = emptyCounts();
  for (const finding of findings) {
    if (finding.resolved_at !== null) continue;
    counts[finding.severity] += 1;
    if (finding.kev) counts.kev += 1;
  }
  return counts;
};

export const dataSources = (ecosystems: readonly string[]): VulnerabilityDataSource[] =>
  ecosystems.map((ecosystem, index) => ({
    ecosystem,
    source: index === 0 ? 'download' : 'import',
    data_modified_at: index === 0 ? ago(5) : ago(50),
    refreshed_at: ago(5),
    last_error:
      index === 0 ? null : 'the last refresh failed: 503 from the OSV mirror',
  }));

const vulnerabilityData: VulnerabilityDataSource[] = [
  {
    ecosystem: 'Debian:13',
    source: 'download',
    data_modified_at: ago(5),
    refreshed_at: ago(5),
    last_error: null,
  },
  {
    ecosystem: 'Alpine:v3.22',
    source: 'import',
    data_modified_at: ago(50),
    refreshed_at: ago(50),
    last_error: null,
  },
  {
    ecosystem: 'npm',
    source: 'download',
    data_modified_at: ago(9),
    refreshed_at: ago(9),
    last_error: null,
  },
];

export const summaryData = (): VulnerabilityDataSource[] => vulnerabilityData;

const baseFinding = (
  overrides: Partial<VulnerabilityFinding> & Pick<VulnerabilityFinding, 'vuln_id'>,
): VulnerabilityFinding => ({
  id: mockId('vln'),
  subject_kind: 'app',
  aliases: [],
  ecosystem: 'Debian:13',
  package: 'openssl',
  installed_version: '3.0.16-1~deb13u1',
  fixed_version: null,
  severity: 'medium',
  cvss_score: null,
  kev: false,
  epss: null,
  first_seen_at: ago(24),
  resolved_at: null,
  resolved_reason: null,
  ...overrides,
});

const docsSiteFindings = (app: App): VulnerabilityFinding[] => [
  baseFinding({
    vuln_id: 'CVE-2025-15467',
    app_id: app.id,
    package: 'openssl',
    source_package: 'openssl',
    installed_version: '3.0.16-1~deb13u1',
    fixed_version: '3.0.16-1~deb13u2',
    severity: 'critical',
    cvss_score: 9.8,
    kev: true,
    summary: 'A buffer overflow in the X.509 certificate parser.',
    first_seen_at: ago(30),
  }),
  baseFinding({
    vuln_id: 'CVE-2024-45337',
    app_id: app.id,
    package: 'libssl3t64',
    source_package: 'openssl',
    installed_version: '3.0.16-1~deb13u1',
    fixed_version: '3.0.16-1~deb13u2',
    severity: 'high',
    cvss_score: 7.4,
    summary: 'A verification bypass in the certificate chain check.',
    first_seen_at: ago(28),
  }),
  baseFinding({
    vuln_id: 'GHSA-952p-6rrq-rcjv',
    app_id: app.id,
    ecosystem: 'npm',
    package: 'tar',
    installed_version: '6.2.0',
    fixed_version: '6.2.1',
    severity: 'medium',
    cvss_score: 5.3,
    summary: 'A malicious archive can write outside the extraction directory.',
    first_seen_at: ago(20),
  }),
  baseFinding({
    vuln_id: 'CVE-2023-0464',
    app_id: app.id,
    package: 'zlib1g',
    installed_version: '1:1.3.dfsg-2',
    fixed_version: '1:1.3.dfsg-2+deb13u1',
    severity: 'low',
    cvss_score: 3.7,
    resolved_at: ago(6),
    resolved_reason: 'image_changed',
    first_seen_at: ago(72),
  }),
];

const blogFindings = (app: App): VulnerabilityFinding[] => [
  baseFinding({
    vuln_id: 'CVE-2025-31498',
    app_id: app.id,
    ecosystem: 'Alpine:v3.22',
    package: 'musl',
    installed_version: '1.2.5-r9',
    fixed_version: '1.2.5-r10',
    severity: 'medium',
    cvss_score: 5.9,
    summary: 'A crafted DNS response can overflow a stack buffer in the resolver.',
    first_seen_at: ago(26),
  }),
];

/** Atlas's api: four open findings in its Debian base and npm tree, and one resolved. */
const atlasApiFindings = (app: App): VulnerabilityFinding[] => [
  baseFinding({
    vuln_id: 'CVE-2025-27587',
    app_id: app.id,
    package: 'libssl3t64',
    source_package: 'openssl',
    installed_version: '3.5.1-1',
    fixed_version: '3.5.1-1+deb13u1',
    severity: 'high',
    cvss_score: 7.5,
    kev: true,
    summary: 'A timing side channel in the elliptic-curve code leaks key bits.',
    first_seen_at: ago(20),
  }),
  baseFinding({
    vuln_id: 'GHSA-3xgq-45jj-v275',
    app_id: app.id,
    ecosystem: 'npm',
    package: 'cross-spawn',
    installed_version: '7.0.3',
    fixed_version: '7.0.5',
    severity: 'high',
    cvss_score: 7.5,
    summary: 'A regular expression in argument escaping can take exponential time.',
    first_seen_at: ago(44),
  }),
  baseFinding({
    vuln_id: 'GHSA-952p-6rrq-rcjv',
    app_id: app.id,
    ecosystem: 'npm',
    package: 'micromatch',
    installed_version: '4.0.5',
    fixed_version: '4.0.8',
    severity: 'medium',
    cvss_score: 5.3,
    summary: 'A crafted pattern can make matching take a long time.',
    first_seen_at: ago(44),
  }),
  baseFinding({
    vuln_id: 'CVE-2023-4641',
    app_id: app.id,
    package: 'login',
    source_package: 'shadow',
    installed_version: '1:4.17.4-2',
    fixed_version: null,
    severity: 'medium',
    cvss_score: 5.5,
    summary: 'A password entered at a failed prompt can stay in memory.',
    first_seen_at: ago(96),
  }),
  baseFinding({
    vuln_id: 'CVE-2024-56171',
    app_id: app.id,
    package: 'libxml2',
    installed_version: '2.12.7+dfsg-1',
    fixed_version: '2.12.7+dfsg+really2.9.14-1',
    severity: 'high',
    cvss_score: 7.8,
    resolved_at: ago(30),
    resolved_reason: 'upgraded',
    first_seen_at: ago(96),
  }),
];

const findingsCache = new Map<string, VulnerabilityFinding[]>();
const clearedApps = new Set<string>();

const seedFor = (app: App): VulnerabilityFinding[] => {
  if (app.name === 'docs-site') return docsSiteFindings(app);
  if (app.name === 'blog') return blogFindings(app);
  if (app.name === 'api' && db.projects.some((project) => project.id === app.project_id && project.slug === 'atlas')) {
    return atlasApiFindings(app);
  }
  return [];
};

/** The app's findings; empty once a refreshed-base rebuild has replaced them. */
export const appFindings = (app: App): VulnerabilityFinding[] => {
  if (clearedApps.has(app.id)) return [];
  const cached = findingsCache.get(app.id);
  if (cached) return cached;
  const findings = seedFor(app);
  findingsCache.set(app.id, findings);
  return findings;
};

/** A rebuild with `refresh_base` picks up a patched base and clears the list. */
export const clearAppFindings = (appId: string): void => {
  clearedApps.add(appId);
};

export const inventoryFor = (app: App): ImageInventoryStatus => {
  if (app.image === '') return { status: 'no_image' };
  const alpine = app.name === 'blog';
  return {
    status: 'inventoried',
    image_digest:
      alpine
        ? 'sha256:6f8d0b7d7e5d4f0c9a1e2d3c4b5a69788796a5b4c3d2e1f0a9b8c7d6e5f4a3b2'
        : 'sha256:2a7d1c4e8f0b3d6a9c2e5f8b1d4a7c0e3f6b9d2a5c8e1f4b7d0a3c6e9f2b5d8a',
    os: alpine ? 'alpine 3.22' : 'debian 13',
    package_count: alpine ? 47 : 214,
    ecosystems: alpine ? ['Alpine:v3.22'] : ['Debian:13', 'npm'],
    inventoried_at: ago(0.3),
  };
};

export const reportDataFor = (app: App): VulnerabilityDataSource[] =>
  dataSources(inventoryFor(app).ecosystems ?? []);

const inventoryStatusOf = (app: App): AppVulnerabilitySummary['inventory_status'] =>
  app.image === '' ? 'no_image' : 'inventoried';

export const appSummary = (app: App): AppVulnerabilitySummary => ({
  app_id: app.id,
  project_id: app.project_id,
  name: app.name,
  inventory_status: inventoryStatusOf(app),
  counts: countsOf(appFindings(app)),
});

const quoroFindings = (): VulnerabilityFinding[] => [
  {
    id: mockId('vln'),
    subject_kind: 'quoro',
    component: 'cloud-hypervisor',
    vuln_id: 'CVE-2025-2222',
    aliases: [],
    ecosystem: 'crates.io',
    package: 'vm-memory',
    installed_version: '0.16.1',
    fixed_version: '0.17.0',
    severity: 'high',
    cvss_score: 7.5,
    kev: false,
    epss: null,
    first_seen_at: ago(16),
    resolved_at: null,
    resolved_reason: null,
  },
];

export const quoroPart = (): QuoroVulnerabilities => {
  const items = quoroFindings();
  return { version: '0.1.0', counts: countsOf(items), items };
};

const hostKernelFindings = (): VulnerabilityFinding[] => [
  {
    id: mockId('vln'),
    subject_kind: 'host_kernel',
    node_id: db.node.id,
    vuln_id: 'CVE-2025-38211',
    aliases: [],
    summary: 'A use-after-free in the netfilter conntrack table.',
    ecosystem: 'Linux',
    package: 'linux',
    installed_version: db.node.kvm.kernel,
    fixed_version: '6.12.5',
    severity: 'high',
    cvss_score: 7.8,
    kev: false,
    epss: null,
    upstream_version_only: true,
    first_seen_at: ago(12),
    resolved_at: null,
    resolved_reason: null,
  },
];

export const hostKernelParts = (): NodeKernelVulnerabilities[] => {
  const items = hostKernelFindings();
  return [
    {
      node_id: db.node.id,
      kernel: db.node.kvm.kernel,
      counts: countsOf(items),
      items,
    },
  ];
};

const severityWeight = (severity: VulnerabilitySeverity): number => {
  switch (severity) {
    case 'critical':
      return 5;
    case 'high':
      return 4;
    case 'medium':
      return 3;
    case 'low':
      return 2;
    case 'unknown':
      return 1;
  }
};

/** Open first, then most severe, KEV first within a severity, then vuln_id. */
export const orderFindings = (
  findings: readonly VulnerabilityFinding[],
): VulnerabilityFinding[] =>
  [...findings].sort((left, right) => {
    const leftOpen = left.resolved_at === null ? 0 : 1;
    const rightOpen = right.resolved_at === null ? 0 : 1;
    if (leftOpen !== rightOpen) return leftOpen - rightOpen;
    const severity =
      severityWeight(right.severity) - severityWeight(left.severity);
    if (severity !== 0) return severity;
    if (left.kev !== right.kev) return left.kev ? -1 : 1;
    return left.vuln_id.localeCompare(right.vuln_id);
  });

/** Apps with a critical or KEV finding first, then by name. */
export const buildSummary = (projectId: string | null): VulnerabilitySummary => {
  const apps = db.apps
    .filter((app) => projectId === null || app.project_id === projectId)
    .map(appSummary)
    .sort((left, right) => {
      const leftFirst = left.counts.critical > 0 || left.counts.kev > 0 ? 1 : 0;
      const rightFirst =
        right.counts.critical > 0 || right.counts.kev > 0 ? 1 : 0;
      if (leftFirst !== rightFirst) return rightFirst - leftFirst;
      return left.name.localeCompare(right.name);
    });

  return {
    data: summaryData(),
    apps,
    quoro: quoroPart(),
    host_kernels: hostKernelParts(),
    next_cursor: null,
  };
};
