import type { App, Backup, Build, Vm, Volume } from '../api/generated/model';
import { formatDuration } from './agency-format';
import { lastBuiltCommit } from './build-status';

/*
 * A project's services, as pure data: its apps and the VMs that belong to no
 * app. The projects grid and the canvas both read them from here, so the two
 * always count and order the same things.
 */

/** What a service is: an app built from Git, an app run from an image, or a standalone VM. */
export type ServiceKind = 'git' | 'image' | 'vm';

export type ServiceItem =
  | { key: string; kind: 'git' | 'image'; name: string; app: App }
  | { key: string; kind: 'vm'; name: string; vm: Vm };

/** The kinds in the order the canvas lays them out and the preview shows them. */
export const SERVICE_KINDS: readonly ServiceKind[] = ['git', 'image', 'vm'];

export const appKind = (app: Pick<App, 'source'>): 'git' | 'image' =>
  app.source != null ? 'git' : 'image';

export const appKey = (appId: string): string => `app:${appId}`;
/** The app id in a service key, or undefined for a VM's key or none. */
export const appIdOfKey = (key: string | undefined): string | undefined =>
  key?.startsWith('app:') ? key.slice('app:'.length) : undefined;
export const vmKey = (vmId: string): string => `vm:${vmId}`;

/** A VM with no app is a service of its own; an app's instances are part of the app. */
export const isStandaloneVm = (vm: Pick<Vm, 'app_id'>): boolean => vm.app_id === null;

/**
 * The services of one project: its apps and its standalone VMs, ordered by
 * kind (Git, image, VM) and then by name.
 */
export const projectServices = (
  projectId: string,
  apps: readonly App[],
  vms: readonly Vm[],
): ServiceItem[] => {
  const items: ServiceItem[] = [
    ...apps
      .filter((app) => app.project_id === projectId)
      .map((app) => ({ key: appKey(app.id), kind: appKind(app), name: app.name, app })),
    ...vms
      .filter((vm) => vm.project_id === projectId && isStandaloneVm(vm))
      .map((vm) => ({ key: vmKey(vm.id), kind: 'vm' as const, name: vm.name, vm })),
  ];
  return items.sort(
    (left, right) =>
      SERVICE_KINDS.indexOf(left.kind) - SERVICE_KINDS.indexOf(right.kind) ||
      left.name.localeCompare(right.name),
  );
};

/** "1 service", "5 services", "No services". */
export const servicesCount = (count: number): string =>
  count === 0 ? 'No services' : `${count} ${count === 1 ? 'service' : 'services'}`;

/** `×2` for an app with more than one replica; nothing for one. */
export const replicasMark = (app: Pick<App, 'replicas'>): string | null =>
  app.replicas > 1 ? `×${app.replicas}` : null;

/** Where an app answers: its first domain, or the port it listens on inside the project. */
export const appAddress = (app: Pick<App, 'port'>, hostname?: string | null): string =>
  hostname ? hostname : `Private, port ${app.port}`;

/** Memory in the larger unit that says it exactly: `8 GiB`, `1.5 GiB`, `512 MiB`. */
export const memoryWords = (mb: number): string => {
  if (mb < 1024) return `${mb} MiB`;
  const gib = mb / 1024;
  return Number.isInteger(gib) ? `${gib} GiB` : `${gib.toFixed(1)} GiB`;
};

/**
 * An app's resources on one line: `1 vCPU, 256 MiB, 1 GiB disk`. The
 * contract's `disk_gb` counts gibibytes, as Storage and the VM views say.
 */
export const resourcesLine = (resources: Pick<App['resources'], 'vcpus' | 'memory_mb' | 'disk_gb'>): string =>
  `${resources.vcpus} vCPU, ${memoryWords(resources.memory_mb)}, ${resources.disk_gb} GiB disk`;

/** A VM's size on one line: `VM · 4 vCPU · 8 GiB`. */
export const vmSize = (vm: Pick<Vm, 'vcpus' | 'memory_mb'>): string =>
  `VM · ${vm.vcpus} vCPU · ${memoryWords(vm.memory_mb)}`;

/** The commit an app's newest successful build built, as `4f2a9c1 on main`. */
export const commitLine = (
  builds: ReadonlyArray<Pick<Build, 'status' | 'commit' | 'source'>>,
): string | null => {
  const commit = lastBuiltCommit(builds);
  if (commit === null) return null;
  const built = builds.find((build) => build.status === 'succeeded' && build.commit === commit);
  const ref = built?.source.ref;
  return ref ? `${commit.slice(0, 7)} on ${ref}` : commit.slice(0, 7);
};

/** True when the newest build of an app (builds newest first) failed. */
export const lastBuildFailed = (builds: ReadonlyArray<Pick<Build, 'status'>>): boolean =>
  builds[0]?.status === 'failed';

/** The volume an app runs on, if it has one in use: `/data · 10 GiB`. */
export const volumeLine = (
  volumes: ReadonlyArray<Pick<Volume, 'state' | 'mount_path' | 'size_gb'>>,
): string | null => {
  const volume = volumes.find(
    (candidate) =>
      candidate.state === 'ready' ||
      candidate.state === 'creating' ||
      candidate.state === 'restoring',
  );
  return volume ? `${volume.mount_path} · ${volume.size_gb} GiB` : null;
};

/**
 * When the next scheduled backup is due. The schedule applies only while the
 * app's HA policy is `cold`, and it counts from the last succeeded backup
 * (openapi.yaml `BackupSchedule`). Null when no schedule applies; "No backup
 * yet" when none has succeeded, since the contract gives nothing to count from.
 * Backups are newest first, as `listAppBackups` returns them.
 */
export const nextBackupLine = (
  app: Pick<App, 'ha_policy' | 'backup_schedule'>,
  backups: ReadonlyArray<Pick<Backup, 'status' | 'created_at' | 'finished_at'>>,
  now: number = Date.now(),
): string | null => {
  if (app.ha_policy !== 'cold') return null;
  const last = backups.find((backup) => backup.status === 'succeeded');
  if (!last) return 'No backup yet';
  const from = Date.parse(last.finished_at ?? last.created_at);
  if (Number.isNaN(from)) return null;
  const due = from + app.backup_schedule.interval_h * 3_600_000;
  const seconds = Math.round((due - now) / 1_000);
  return seconds < 60 ? 'Backup due now' : `Backup in ${formatDuration(seconds)}`;
};

/** The nodes a service runs on, by name, sorted: `lab-1 · lab-2`. Unknown nodes are left out. */
export const nodesLine = (
  nodeIds: readonly string[],
  names: Readonly<Record<string, string>>,
): string | null => {
  const known = [...new Set(nodeIds)]
    .map((id) => names[id])
    .filter((name): name is string => name !== undefined && name !== '');
  return known.length === 0 ? null : known.sort().join(' · ');
};

/**
 * Where a scrolling tab row should rest so its selected tab shows whole, with
 * `pad` pixels to spare, or null when it already does. Offsets are from the
 * row's own left edge; the answer stays within what the row can scroll.
 */
export const keepInView = ({
  left,
  width,
  scrollLeft,
  clientWidth,
  scrollWidth,
  pad = 32,
}: {
  left: number;
  width: number;
  scrollLeft: number;
  clientWidth: number;
  scrollWidth: number;
  pad?: number;
}): number | null => {
  const most = Math.max(0, scrollWidth - clientWidth);
  const right = left + width;
  let next: number | null = null;
  if (left - pad < scrollLeft) next = left - pad;
  else if (right + pad > scrollLeft + clientWidth) next = right + pad - clientWidth;
  if (next === null) return null;
  const clamped = Math.min(most, Math.max(0, next));
  return Math.abs(clamped - scrollLeft) < 1 ? null : clamped;
};
