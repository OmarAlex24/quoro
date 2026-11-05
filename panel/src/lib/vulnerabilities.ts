import type {
  AppVulnerabilitySummaryInventoryStatus,
  ImageInventoryStatus,
  ImageInventoryStatusStatus,
  VulnerabilityCounts,
  VulnerabilityDataSource,
  VulnerabilityFinding,
  VulnerabilityFindingResolvedReason,
  VulnerabilitySeverity,
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

/** The Quoro findings that belong to the guest kernel. */
export const guestKernelFindings = (
  findings: readonly VulnerabilityFinding[],
): VulnerabilityFinding[] =>
  findings.filter((finding) => finding.component === 'guest-kernel');

/** True when the running release's guest kernel has no open finding. */
export const guestKernelIsClear = (
  findings: readonly VulnerabilityFinding[],
): boolean => guestKernelFindings(findings).length === 0;
