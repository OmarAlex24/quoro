import type {
  AppVulnerabilitySummaryInventoryStatus,
  GetAppVulnerabilitiesParams,
  ImageInventoryStatus,
  ImageInventoryStatusStatus,
  VulnerabilityCounts,
  VulnerabilityDataSource,
  VulnerabilityFinding,
  VulnerabilityFindingResolvedReason,
  VulnerabilitySeverity,
  VulnerabilitySummary,
} from '../api/generated/model';
import { relativeTime } from './app-status';
import type { LampTone } from './lamp';

/**
 * One line of `crates/engine/pins.txt`: `name version arch file url sha256`,
 * with `#` comment lines and blank lines around it. The panel imports the same
 * file, so the guest kernel it names is the one this release runs.
 */
export interface PinnedArtifact {
  name: string;
  version: string;
  arch: string;
  file: string;
  url: string;
  sha256: string;
}

/** Parse the pinned-artifact file into its data lines, skipping comments. */
export const parsePins = (raw: string): PinnedArtifact[] => {
  const artifacts: PinnedArtifact[] = [];
  for (const rawLine of raw.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (line === '' || line.startsWith('#')) continue;
    const parts = line.split(/\s+/);
    if (parts.length < 6) continue;
    const [name, version, arch, file, url, sha256] = parts;
    artifacts.push({ name, version, arch, file, url, sha256 });
  }
  return artifacts;
};

/** The guest kernel version the release pins, or null when it names none. */
export const guestKernelVersion = (raw: string): string | null =>
  parsePins(raw).find((artifact) => artifact.name === 'guest-kernel')?.version ?? null;

/** Severity from least to most severe; a higher rank is more severe. */
export const SEVERITY_ORDER: readonly VulnerabilitySeverity[] = [
  'unknown',
  'low',
  'medium',
  'high',
  'critical',
];

const SEVERITY_RANK: Record<VulnerabilitySeverity, number> = {
  unknown: 1,
  low: 2,
  medium: 3,
  high: 4,
  critical: 5,
};

export const severityRank = (severity: VulnerabilitySeverity): number =>
  SEVERITY_RANK[severity];

const SEVERITY_LABEL: Record<VulnerabilitySeverity, string> = {
  critical: 'Critical',
  high: 'High',
  medium: 'Medium',
  low: 'Low',
  unknown: 'Unknown',
};

export const severityLabel = (severity: VulnerabilitySeverity): string =>
  SEVERITY_LABEL[severity];

const SEVERITY_TONE: Record<VulnerabilitySeverity, LampTone> = {
  critical: 'fault',
  high: 'shift',
  medium: 'shift',
  low: 'idle',
  unknown: 'unknown',
};

/** The lamp tone a severity takes in the panel's existing five colours. */
export const severityTone = (severity: VulnerabilitySeverity): LampTone =>
  SEVERITY_TONE[severity];

/**
 * The one status line of an image's package inventory. `pending_data` names
 * the ecosystems whose data has not arrived yet.
 */
export const inventorySentence = (inventory: ImageInventoryStatus): string => {
  switch (inventory.status) {
    case 'no_image':
      return 'No image deployed yet.';
    case 'pending':
      return "Waiting for the node to list this image's packages.";
    case 'not_inventoried':
      return 'No package database was found in this image, so it cannot be checked.';
    case 'pending_data': {
      const ecosystems = inventory.ecosystems ?? [];
      return ecosystems.length === 0
        ? 'Fetching vulnerability data for this image.'
        : `Fetching vulnerability data for ${ecosystems.join(', ')}.`;
    }
    case 'inventoried': {
      const packages = `${inventory.package_count ?? 0} packages checked`;
      return inventory.os ? `${packages} (${inventory.os}).` : `${packages}.`;
    }
  }
};

const INVENTORY_STATUS_WORDS: Record<ImageInventoryStatusStatus, string> = {
  inventoried: 'Packages checked',
  pending: 'Waiting for packages',
  not_inventoried: 'No package database',
  no_image: 'No image yet',
  pending_data: 'Fetching data',
};

/** The short form of an inventory status, for a table cell. */
export const inventoryStatusWords = (
  status: ImageInventoryStatusStatus | AppVulnerabilitySummaryInventoryStatus,
): string => INVENTORY_STATUS_WORDS[status];

/**
 * "Debian:13: data from 5 h ago". A source that never carried a record says so
 * rather than dating nothing.
 */
export const dataAgeSentence = (
  source: VulnerabilityDataSource,
  now: number = Date.now(),
): string =>
  source.data_modified_at === null
    ? `${source.ecosystem}: no data yet`
    : `${source.ecosystem}: data from ${relativeTime(source.data_modified_at, now)}`;

/** `Debian:13` as a person writes it: `Debian 13`. */
const ecosystemWords = (ecosystem: string): string => ecosystem.replace(':', ' ');

/**
 * What a clean report was checked against, for its empty state: the data of
 * each ecosystem the image's inventory covers and how old it is, for example
 * `Checked against Debian 13 data from 5 h ago.` Null when no data source
 * covers the image, so the empty state says nothing it cannot back.
 */
export const checkedAgainstSentence = (
  inventory: ImageInventoryStatus,
  data: readonly VulnerabilityDataSource[],
  now: number = Date.now(),
): string | null => {
  const covered = inventory.ecosystems;
  const sources = data.filter(
    (source) => source.data_modified_at !== null && (covered === undefined || covered.includes(source.ecosystem)),
  );
  if (sources.length === 0) return null;
  const parts = sources.map(
    (source) => `${ecosystemWords(source.ecosystem)} data from ${relativeTime(source.data_modified_at!, now)}`,
  );
  const list = parts.length === 1 ? parts[0] : `${parts.slice(0, -1).join(', ')} and ${parts[parts.length - 1]}`;
  return `Checked against ${list}.`;
};

/** A resolved reason in words, as the app screen shows it. */
export const resolvedReasonWords = (
  reason: VulnerabilityFindingResolvedReason,
): string => {
  switch (reason) {
    case 'image_changed':
      return 'the app runs another image';
    case 'data_changed':
      return 'the data no longer matches';
    case 'app_deleted':
      return 'the app was deleted';
    case 'node_removed':
      return 'the node was removed';
    case 'upgraded':
      return 'the package was upgraded';
    case null:
      return 'it no longer applies';
  }
};

/** A finding is open while it has no resolution. */
export const isOpenFinding = (finding: VulnerabilityFinding): boolean =>
  finding.resolved_at === null;

/** The total number of open findings a counts object names. */
export const openFindingCount = (counts: VulnerabilityCounts): number =>
  counts.critical + counts.high + counts.medium + counts.low + counts.unknown;

/**
 * The open findings of the cluster's own machines, for the count on the Nodes
 * section's Security tab: the running Quoro release and every node's host
 * kernel. Null when the summary carries neither, which is how it answers a
 * caller without `node:read`.
 */
export const nodeSecurityCount = (
  summary: Pick<VulnerabilitySummary, 'quoro' | 'host_kernels'> | undefined,
): number | null => {
  if (!summary || (summary.quoro === undefined && summary.host_kernels === undefined)) return null;
  const quoro = summary.quoro ? openFindingCount(summary.quoro.counts) : 0;
  const hosts = (summary.host_kernels ?? []).reduce((sum, host) => sum + openFindingCount(host.counts), 0);
  return quoro + hosts;
};

/** The Quoro findings that belong to the guest kernel. */
export const guestKernelFindings = (
  findings: readonly VulnerabilityFinding[],
): VulnerabilityFinding[] =>
  findings.filter((finding) => finding.component === 'guest-kernel');

/** True when the running release's guest kernel has no open finding. */
export const guestKernelIsClear = (
  findings: readonly VulnerabilityFinding[],
): boolean => guestKernelFindings(findings).length === 0;

/** A group of findings in the Security tab: one per severity, then the resolved ones. */
export interface FindingGroup {
  key: VulnerabilitySeverity | 'resolved';
  label: string;
  items: VulnerabilityFinding[];
}

/**
 * Findings grouped the way the Security tab reads them: open ones by
 * severity, most severe first, then the resolved ones. Within a group the
 * report's own order stays (KEV first within a severity, then `vuln_id`).
 * Empty groups are left out.
 */
export const groupFindings = (findings: readonly VulnerabilityFinding[]): FindingGroup[] => {
  const groups: FindingGroup[] = [...SEVERITY_ORDER]
    .reverse()
    .map((severity) => ({ key: severity, label: severityLabel(severity), items: [] }));
  const resolved: FindingGroup = { key: 'resolved', label: 'Resolved', items: [] };
  for (const finding of findings) {
    if (!isOpenFinding(finding)) {
      resolved.items.push(finding);
      continue;
    }
    groups.find((group) => group.key === finding.severity)?.items.push(finding);
  }
  return [...groups, resolved].filter((group) => group.items.length > 0);
};

/**
 * The open findings count the Security tab carries, or null when there is
 * none to show: no report yet, or zero (an image that could not be checked
 * has no open findings).
 */
export const securityTabCount = (report: { counts: VulnerabilityCounts } | undefined): number | null => {
  if (!report) return null;
  const count = openFindingCount(report.counts);
  return count > 0 ? count : null;
};

/** The Security tab's severity filter: everything, high and above, or critical only. */
export type SeverityFilter = 'all' | 'high' | 'critical';

const SEVERITY_PARAM: Record<SeverityFilter, VulnerabilitySeverity | undefined> = {
  all: undefined,
  high: 'high',
  critical: 'critical',
};

/** One page of the Security tab, the size the app page used. */
export const SECURITY_PAGE_SIZE = 50;

/**
 * The report's query for the Security tab: open findings only unless asked,
 * every severity unless asked. With no filter it is the same query the tab's
 * count reads, so both share one request (no key carries `undefined`).
 */
export const securityParams = (
  severity: SeverityFilter = 'all',
  kevOnly = false,
  includeResolved = false,
): GetAppVulnerabilitiesParams => {
  const at = SEVERITY_PARAM[severity];
  return {
    ...(at ? { severity: at } : {}),
    ...(kevOnly ? { kev: true } : {}),
    ...(includeResolved ? { include_resolved: true } : {}),
    limit: SECURITY_PAGE_SIZE,
  };
};
