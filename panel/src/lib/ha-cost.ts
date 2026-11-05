import type {
  AppHaCostSnapshotMethod,
  HaCapacity,
  HaPolicyCost,
  HaPolicyCostPolicy,
  HaPolicyCostUnavailableReason,
} from '../api/generated/model';

/**
 * The storage section's pure words and numbers (openapi.yaml `HaPolicyCost`,
 * `AppHaCost`; audit-log-format.md section 8.2 for the copy's strengths).
 * Everything here is formatting, so it can be tested without a DOM.
 */

/** One row per policy, in the order the contract fixes. */
export const HA_POLICIES: readonly HaPolicyCostPolicy[] = [
  'none',
  'cold',
  'warm',
  'hot',
];

/** The policy's name as an operator reads it. */
export const haPolicyWords: Record<HaPolicyCostPolicy, string> = {
  none: 'None',
  cold: 'Cold',
  warm: 'Warm',
  hot: 'Hot',
};

const UNITS: ReadonlyArray<readonly [string, number]> = [
  ['TiB', 1024 ** 4],
  ['GiB', 1024 ** 3],
  ['MiB', 1024 ** 2],
  ['KiB', 1024],
];

const number = (value: number): string =>
  value >= 100 ? `${Math.round(value)}` : `${Math.round(value * 10) / 10}`;

/**
 * Bytes in binary units, the units the contract counts volumes in (openapi.yaml
 * `Volume.size_gb`, `HaPolicyCost.storage_bytes`). A missing value is an em dash.
 */
export const formatBytes = (bytes: number | null | undefined): string => {
  if (bytes === null || bytes === undefined || !Number.isFinite(bytes)) return '—';
  if (bytes <= 0) return '0 B';
  for (const [suffix, size] of UNITS) {
    if (bytes >= size) return `${number(bytes / size)} ${suffix}`;
  }
  return `${Math.round(bytes)} B`;
};

/** An estimate in seconds as `m:ss`, or `h:mm:ss` from an hour on. */
export const formatRestoreEstimate = (seconds: number | null | undefined): string => {
  if (seconds === null || seconds === undefined || !Number.isFinite(seconds)) {
    return '—';
  }
  const total = Math.max(0, Math.round(seconds));
  const hours = Math.floor(total / 3_600);
  const minutes = Math.floor((total % 3_600) / 60);
  const rest = total % 60;
  const two = (value: number): string => String(value).padStart(2, '0');
  return hours > 0 ? `${hours}:${two(minutes)}:${two(rest)}` : `${minutes}:${two(rest)}`;
};

/**
 * The pause a backup costs the app: a `reflink` snapshot costs none, so 0 is
 * said in words rather than as a number (openapi.yaml `HaPolicyCost.pause_estimate_ms`).
 */
export const formatPause = (
  milliseconds: number | null | undefined,
  snapshotMethod: AppHaCostSnapshotMethod | undefined,
): string => {
  if (milliseconds === null || milliseconds === undefined) return '—';
  if (milliseconds <= 0 && snapshotMethod === 'reflink') return 'no pause';
  return `${Math.round(milliseconds)} ms`;
};

/** Why a policy is unavailable, in plain words; null when it is available. */
export const unavailableReasonWords = (
  reason: HaPolicyCostUnavailableReason | undefined,
): string | null => {
  switch (reason) {
    case 'stateless':
      return 'needs a volume';
    case 'no_repository':
      return 'needs a backup repository';
    case 'not_in_this_release':
      return 'not in this release';
    default:
      return null;
  }
};

/** Whether a policy may be picked, and the reason in words when it may not. */
export const policyAvailability = (
  cost: HaPolicyCost,
): { available: boolean; reason: string | null } => {
  const reason = unavailableReasonWords(cost.unavailable_reason);
  return { available: cost.available && reason === null, reason };
};

/**
 * How the volume's node snapshots it (openapi.yaml `AppHaCost.snapshot_method`,
 * ADR 0146); null for a stateless app.
 */
export const snapshotMethodWords = (
  method: AppHaCostSnapshotMethod | undefined,
): string | null => {
  switch (method) {
    case 'reflink':
      return 'snapshots without pausing the app';
    case 'pause_copy':
      return 'pauses the app while it copies the volume';
    default:
      return null;
  }
};

/** Reserved vCPUs as the table's plain count. */
export const formatVcpus = (vcpus: number): string => `${vcpus}`;

/** Reserved memory, a mebibyte count as human units. */
export const formatMemory = (capacity: HaCapacity): string =>
  formatBytes(capacity.memory_mb * 1024 ** 2);

/** Reserved disk, a gibibyte count as human units. */
export const formatDisk = (capacity: HaCapacity): string =>
  formatBytes(capacity.disk_gb * 1024 ** 3);

/**
 * The volume size the form may send: a whole number of gibibytes, 1 to 1024
 * (openapi.yaml `CreateVolumeRequest.size_gb`).
 */
export const volumeSizeProblem = (text: string): string | null => {
  const value = Number(text.trim());
  if (text.trim() === '' || !Number.isFinite(value)) return 'Enter a size in GiB.';
  if (!Number.isInteger(value)) return 'Use a whole number of GiB.';
  if (value < 1 || value > 1024) return 'Use a size from 1 to 1024 GiB.';
  return null;
};

/**
 * The mount path the form may send: absolute, no empty `.` or `..` segment, no
 * trailing slash, and not `/` nor under `/proc`, `/sys` or `/dev`
 * (openapi.yaml `CreateVolumeRequest.mount_path`, ADR 0145).
 */
export const mountPathProblem = (text: string): string | null => {
  const value = text.trim();
  if (!value.startsWith('/')) {
    return 'Use an absolute path like /data.';
  }
  if (value === '/' || value.endsWith('/')) {
    return 'Do not use / or a trailing slash.';
  }
  if (!/^\/[A-Za-z0-9._/-]+$/.test(value)) {
    return 'Use only letters, digits, dots, dashes and slashes.';
  }
  if (
    value
      .slice(1)
      .split('/')
      .some((segment) => segment === '' || segment === '.' || segment === '..')
  ) {
    return 'Remove empty, "." or ".." segments.';
  }
  if (/^\/(proc|sys|dev)(\/|$)/.test(value)) {
    return 'Do not mount under /proc, /sys or /dev.';
  }
  return null;
};

