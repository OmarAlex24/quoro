import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import { getGetAppQueryKey, updateApp } from '../api/generated/apps/apps';
import {
  createBackup,
  getListAppBackupsQueryKey,
  getListBackupRepositoriesQueryKey,
  useListAppBackups,
  useListBackupRepositories,
} from '../api/generated/backups/backups';
import type {
  App,
  AppHaPolicy,
  AppHaCostSnapshotMethod,
  Backup,
  HaPolicyCost,
  Volume,
} from '../api/generated/model';
import { shortId } from '../lib/agency-format';
import {
  createVolume,
  getListAppVolumesQueryKey,
  useListAppVolumes,
} from '../api/generated/volumes/volumes';
import { relativeTime } from '../lib/app-status';
import {
  formatBytes,
  formatDisk,
  formatMemory,
  formatPause,
  formatRestoreEstimate,
  formatVcpus,
  HA_POLICIES,
  haPolicyWords,
  mountPathProblem,
  policyAvailability,
  snapshotMethodWords,
  volumeSizeProblem,
} from '../lib/ha-cost';
import { problemView, type ProblemView } from '../lib/problem';
import { cn } from '../lib/utils';
import { ToneLamp } from './tone-lamp';
import { Button } from './ui/button';
import { Field } from './ui/field';
import { Input } from './ui/input';
import { ProblemBanner } from './ui/problem-banner';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from './ui/select';

/** A backup's lamp: the same tones the rest of the panel uses for state (lamp.ts). */
const backupLamp = (
  status: Backup['status'],
): { tone: 'run' | 'shift' | 'fault' | 'idle'; pulse: boolean; words: string } => {
  switch (status) {
    case 'succeeded':
      return { tone: 'run', pulse: false, words: 'Succeeded' };
    case 'failed':
      return { tone: 'fault', pulse: false, words: 'Failed' };
    case 'running':
      return { tone: 'shift', pulse: true, words: 'Running' };
    default:
      return { tone: 'shift', pulse: true, words: 'Queued' };
  }
};

/** A failed backup that retention pruned stays in the list but cannot be restored. */
function VolumeLine({
  volume,
  nodeNames,
}: {
  volume: Volume;
  nodeNames: Record<string, string>;
}) {
  return (
    <dl className="type-data grid grid-cols-1 gap-x-6 gap-y-1 text-[13px] sm:grid-cols-2">
      <div className="flex gap-2">
        <dt className="text-ink-2">Size</dt>
        <dd className="text-ink">{volume.size_gb} GiB</dd>
      </div>
      <div className="flex min-w-0 gap-2">
        <dt className="text-ink-2">Mount path</dt>
        <dd className="type-code min-w-0 truncate text-ink" title={volume.mount_path}>
          {volume.mount_path}
        </dd>
      </div>
      <div className="flex gap-2">
        <dt className="text-ink-2">Node</dt>
        <dd className="text-ink">{nodeNames[volume.node_id] ?? 'an unknown node'}</dd>
      </div>
      <div className="flex gap-2">
        <dt className="text-ink-2">State</dt>
        <dd className="text-ink">{volume.state}</dd>
      </div>
      <div className="flex gap-2">
        <dt className="text-ink-2">Allocated</dt>
        <dd className="text-ink">{formatBytes(volume.allocated_bytes)}</dd>
      </div>
      {volume.restored_from_backup_id ? (
        <div className="flex gap-2">
          <dt className="text-ink-2">Restored from</dt>
          <dd className="min-w-0 truncate text-ink" title={volume.restored_from_backup_id}>
            {volume.restored_from_backup_id}
          </dd>
        </div>
      ) : null}
      {volume.error ? (
        <div className="text-[13px] leading-snug text-fault sm:col-span-2">
          {volume.error}
        </div>
      ) : null}
    </dl>
  );
}

/** Add the first volume of a stateless app: size in GiB and its mount path. */
function AddVolumeForm({
  appId,
  canWrite,
  onChanged,
}: {
  appId: string;
  canWrite: boolean;
  onChanged: () => void;
}) {
  const [size, setSize] = useState('10');
  const [mountPath, setMountPath] = useState('/data');
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});

  const add = useMutation({
    mutationFn: (body: { size_gb: number; mount_path: string }) =>
      createVolume(appId, body, {
        headers: { 'Idempotency-Key': crypto.randomUUID() },
      }),
    onSuccess: () => {
      setProblem(null);
      setFieldErrors({});
      onChanged();
    },
    onError: (error) => setProblem(problemView(error, 'add volumes')),
  });

  const submit = () => {
    setProblem(null);
    const errors: Record<string, string> = {};
    const sizeProblem = volumeSizeProblem(size);
    if (sizeProblem) errors.size = sizeProblem;
    const pathProblem = mountPathProblem(mountPath);
    if (pathProblem) errors.mount = pathProblem;
    setFieldErrors(errors);
    if (Object.keys(errors).length > 0) return;
    add.mutate({ size_gb: Number(size), mount_path: mountPath });
  };

  if (!canWrite) {
    return (
      <p className="mt-2 text-[13px] leading-snug text-ink-2">
        Your role can read storage but not add a volume.
      </p>
    );
  }

  return (
    <div className="mt-3 flex flex-col gap-3 border-t border-line pt-3 sm:flex-row sm:items-start">
      <Field label="Size (GiB)" htmlFor="volume-size" className="sm:w-32">
        <Input
          id="volume-size"
          type="number"
          min={1}
          max={1024}
          step={1}
          inputMode="numeric"
          value={size}
          onChange={(event) => setSize(event.target.value)}
          aria-invalid={fieldErrors.size !== undefined}
        />
        {fieldErrors.size ? (
          <p className="text-[13px] text-fault">{fieldErrors.size}</p>
        ) : null}
      </Field>
      <Field
        label="Mount path"
        htmlFor="volume-mount"
        hint="Where the guest init mounts it; the volume attaches at the next start."
        className="flex-1"
      >
        <Input
          id="volume-mount"
          value={mountPath}
          onChange={(event) => setMountPath(event.target.value)}
          spellCheck={false}
          aria-invalid={fieldErrors.mount !== undefined}
        />
        {fieldErrors.mount ? (
          <p className="text-[13px] text-fault">{fieldErrors.mount}</p>
        ) : null}
      </Field>
      <div className="flex flex-col gap-1.5 sm:pt-[22px]">
        <Button variant="primary" disabled={add.isPending} onClick={submit}>
          {add.isPending ? 'Adding…' : 'Add a volume'}
        </Button>
      </div>
      {problem ? <ProblemBanner view={problem} className="sm:w-full" /> : null}
    </div>
  );
}

/** The four policies; an unavailable one says why and cannot be picked. */
function HaPolicyControl({
  app,
  canWrite,
  onSaved,
}: {
  app: App;
  canWrite: boolean;
  onSaved: () => void;
}) {
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const change = useMutation({
    mutationFn: (policy: AppHaPolicy) => updateApp(app.id, { ha_policy: policy }),
    onSuccess: () => {
      setProblem(null);
      onSaved();
    },
    onError: (error) => setProblem(problemView(error, 'change the HA policy')),
  });

  return (
    <div>
      <div className="mt-2 flex flex-wrap items-end gap-x-3 gap-y-2">
        <Field label="Policy" htmlFor="ha-policy" className="w-64">
          <Select
            value={app.ha_policy}
            onValueChange={(value) => change.mutate(value as AppHaPolicy)}
            disabled={!canWrite || change.isPending}
          >
            <SelectTrigger id="ha-policy" aria-label="HA policy">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {HA_POLICIES.map((policy) => {
                const row = app.ha_cost?.policies.find(
                  (candidate) => candidate.policy === policy,
                );
                const availability = row
                  ? policyAvailability(row)
                  : { available: policy === 'none', reason: null };
                return (
                  <SelectItem
                    key={policy}
                    value={policy}
                    disabled={!availability.available}
                  >
                    {haPolicyWords[policy]}
                    {availability.reason ? ` — ${availability.reason}` : ''}
                  </SelectItem>
                );
              })}
            </SelectContent>
          </Select>
        </Field>
        <p className="max-w-md text-[13px] leading-snug text-ink-2">
          {change.isPending
            ? 'Saving the policy…'
            : 'Cold backups run on the app’s schedule. Warm and hot keep a copy ready on another node, which this release does not offer.'}
        </p>
      </div>
      {!canWrite ? (
        <p className="mt-1 text-[13px] leading-snug text-ink-2">
          Your role can read the policy but not change it.
        </p>
      ) : null}
      {problem ? <ProblemBanner view={problem} className="mt-3" /> : null}
    </div>
  );
}

/** One row per policy, with the current policy on the sunk tone. */
function HaCostTable({
  policies,
  current,
  snapshotMethod,
}: {
  policies: readonly HaPolicyCost[];
  current: AppHaPolicy;
  snapshotMethod: AppHaCostSnapshotMethod;
}) {
  return (
    <div className="mt-3">
      <div className="overflow-x-auto">
        <table className="type-data w-full min-w-[640px] text-[13px]">
          <thead>
            <tr className="border-b border-line text-left text-ink-2">
              <th className="py-1.5 pr-3 font-normal">Policy</th>
              <th className="py-1.5 pr-3 font-normal">Reserved vCPU</th>
              <th className="py-1.5 pr-3 font-normal">Reserved memory</th>
              <th className="py-1.5 pr-3 font-normal">Reserved disk</th>
              <th className="py-1.5 pr-3 font-normal">Storage</th>
              <th className="py-1.5 pr-3 font-normal">Restore estimate</th>
              <th className="py-1.5 font-normal">Pause per backup</th>
            </tr>
          </thead>
          <tbody>
            {policies.map((row) => {
              const active = row.policy === current;
              return (
                <tr
                  key={row.policy}
                  aria-current={active ? 'true' : undefined}
                  className={cn(
                    'border-b border-line/70 last:border-b-0',
                    active && 'bg-sunk/45',
                  )}
                >
                  <td className="py-2 pr-3">
                    <span className={cn(active ? 'type-body-medium text-ink' : 'text-ink')}>
                      {haPolicyWords[row.policy]}
                    </span>
                    {row.available ? null : (
                      <span className="ml-1.5 text-ink-2">
                        {policyAvailability(row).reason ?? 'unavailable'}
                      </span>
                    )}
                  </td>
                  <td className="py-2 pr-3 text-ink">{formatVcpus(row.reserved.vcpus)}</td>
                  <td className="py-2 pr-3 text-ink">{formatMemory(row.reserved)}</td>
                  <td className="py-2 pr-3 text-ink">{formatDisk(row.reserved)}</td>
                  <td className="py-2 pr-3 text-ink">{formatBytes(row.storage_bytes)}</td>
                  <td className="py-2 pr-3 text-ink">
                    {formatRestoreEstimate(row.restore_estimate_s)}
                  </td>
                  <td className="py-2 text-ink">
                    {formatPause(row.pause_estimate_ms, snapshotMethod)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {snapshotMethodWords(snapshotMethod) ? (
        <p className="mt-2 text-[13px] leading-snug text-ink-2">
          Snapshot method: {snapshotMethodWords(snapshotMethod)}.
        </p>
      ) : (
        <p className="mt-2 text-[13px] leading-snug text-ink-2">
          This app has no volume, so there is nothing to snapshot.
        </p>
      )}
      <p className="mt-1 text-[13px] leading-snug text-ink-2">
        These numbers are estimates, not reservations.
      </p>
    </div>
  );
}

/** `interval_h` and `keep`, editable only while the policy runs the schedule. */
function BackupScheduleForm({
  app,
  canWrite,
  onSaved,
}: {
  app: App;
  canWrite: boolean;
  onSaved: () => void;
}) {
  const [interval, setIntervalHours] = useState(String(app.backup_schedule.interval_h));
  const [keep, setKeep] = useState(String(app.backup_schedule.keep));
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});

  const save = useMutation({
    mutationFn: (body: { interval_h: number; keep: number }) =>
      updateApp(app.id, { backup_schedule: body }),
    onSuccess: (saved) => {
      setProblem(null);
      setFieldErrors({});
      setIntervalHours(String(saved.backup_schedule.interval_h));
      setKeep(String(saved.backup_schedule.keep));
      onSaved();
    },
    onError: (error) => setProblem(problemView(error, 'change the backup schedule')),
  });

  const submit = () => {
    setProblem(null);
    const errors: Record<string, string> = {};
    const intervalValue = Number(interval);
    if (!Number.isInteger(intervalValue) || intervalValue < 1 || intervalValue > 168) {
      errors.interval = 'Use a whole number of hours, 1 to 168.';
    }
    const keepValue = Number(keep);
    if (!Number.isInteger(keepValue) || keepValue < 1 || keepValue > 100) {
      errors.keep = 'Keep a whole number of backups, 1 to 100.';
    }
    setFieldErrors(errors);
    if (Object.keys(errors).length > 0) return;
    save.mutate({ interval_h: intervalValue, keep: keepValue });
  };

  return (
    <div>
      <div className="mt-2 flex flex-col gap-3 sm:flex-row sm:items-start">
        <Field
          label="Every (hours)"
          htmlFor="backup-interval"
          hint="Counted from the last succeeded backup."
          className="sm:w-36"
        >
          <Input
            id="backup-interval"
            type="number"
            min={1}
            max={168}
            step={1}
            inputMode="numeric"
            value={interval}
            disabled={!canWrite}
            onChange={(event) => setIntervalHours(event.target.value)}
            aria-invalid={fieldErrors.interval !== undefined}
          />
          {fieldErrors.interval ? (
            <p className="text-[13px] text-fault">{fieldErrors.interval}</p>
          ) : null}
        </Field>
        <Field
          label="Keep"
          htmlFor="backup-keep"
          hint="Succeeded backups kept; older ones are pruned."
          className="sm:w-36"
        >
          <Input
            id="backup-keep"
            type="number"
            min={1}
            max={100}
            step={1}
            inputMode="numeric"
            value={keep}
            disabled={!canWrite}
            onChange={(event) => setKeep(event.target.value)}
            aria-invalid={fieldErrors.keep !== undefined}
          />
          {fieldErrors.keep ? (
            <p className="text-[13px] text-fault">{fieldErrors.keep}</p>
          ) : null}
        </Field>
        {canWrite ? (
          <div className="flex flex-col gap-1.5 sm:pt-[22px]">
            <Button variant="default" disabled={save.isPending} onClick={submit}>
              {save.isPending ? 'Saving…' : 'Save schedule'}
            </Button>
          </div>
        ) : null}
      </div>
      {problem ? <ProblemBanner view={problem} className="mt-3" /> : null}
    </div>
  );
}

/** The app's backups, newest first, and the manual "Back up now" action. */
function BackupsList({
  app,
  canWrite,
  onChanged,
}: {
  app: App;
  canWrite: boolean;
  onChanged: () => void;
}) {
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const backupsQuery = useListAppBackups(
    app.id,
    { limit: 50 },
    {
      query: {
        refetchInterval: (query) =>
          query.state.data?.items.some(
            (backup) => backup.status === 'queued' || backup.status === 'running',
          )
            ? 2_000
            : 10_000,
      },
    },
  );

  const repositoriesQuery = useListBackupRepositories({
    query: { staleTime: 30_000 },
  });

  const volumesQuery = useListAppVolumes(app.id);

  const backupNow = useMutation({
    mutationFn: () =>
      createBackup(app.id, { headers: { 'Idempotency-Key': crypto.randomUUID() } }),
    onSuccess: () => {
      setProblem(null);
      onChanged();
    },
    onError: (error) => setProblem(problemView(error, 'back up the app')),
  });

  const repository = repositoriesQuery.data?.items[0];
  const readyVolume = volumesQuery.data?.items.find(
    (volume) => volume.state === 'ready',
  );
  const backups = [...(backupsQuery.data?.items ?? [])].sort(
    (left, right) => Date.parse(right.created_at) - Date.parse(left.created_at),
  );

  const disabledReason = !canWrite
    ? 'Your role can read backups but not take one.'
    : repositoriesQuery.isPending
      ? 'Reading the backup repository…'
      : repository === undefined
        ? 'A backup needs a backup repository, and none is configured.'
        : volumesQuery.isPending
          ? 'Reading the volume…'
          : readyVolume === undefined
            ? 'A backup needs the volume to be ready.'
            : null;

  return (
    <div>
      <div className="mt-2 flex flex-wrap items-end justify-between gap-x-3 gap-y-2">
        <p className="max-w-md text-[13px] leading-snug text-ink-2">
          {repositoriesQuery.isPending
            ? 'Reading the backup repository…'
            : repository
              ? `Backups are stored in ${repository.name} (${repository.prefix}).`
              : 'No backup repository is configured, so scheduled and manual backups are unavailable.'}
        </p>
        <div className="flex flex-col gap-1.5">
          <Button
            variant="default"
            disabled={disabledReason !== null || backupNow.isPending}
            onClick={() => {
              setProblem(null);
              backupNow.mutate();
            }}
          >
            {backupNow.isPending ? 'Starting…' : 'Back up now'}
          </Button>
          {disabledReason ? (
            <p className="max-w-[260px] text-[13px] leading-snug text-ink-2">
              {disabledReason}
            </p>
          ) : null}
        </div>
      </div>

      {problem ? <ProblemBanner view={problem} className="mt-3" /> : null}

      {backupsQuery.error !== null && backupsQuery.error !== undefined ? (
        <ProblemBanner
          view={problemView(backupsQuery.error, 'see backups')}
          className="mt-3"
        />
      ) : backupsQuery.isPending ? (
        <p className="mt-3 text-[13px] text-ink-2">Reading backups…</p>
      ) : backups.length === 0 ? (
        <p className="mt-3 text-[13px] text-ink-2">
          No backups yet. A cold policy takes them on its schedule; Back up now
          takes one outside it.
        </p>
      ) : (
        <ul className="mt-3 flex flex-col divide-y divide-line border-y border-line">
          {backups.map((backup) => {
            const lamp = backupLamp(backup.status);
            return (
              <li
                key={backup.id}
                className="flex flex-wrap items-center gap-x-3 gap-y-1.5 py-2.5"
              >
                <ToneLamp
                  tone={lamp.tone}
                  pulse={lamp.pulse}
                  size={18}
                  label={lamp.words}
                />
                <span className="type-body-medium min-w-[76px] text-[14px] text-ink">
                  {lamp.words}
                </span>
                <span className="type-data text-[13px] text-ink-2">
                  {backup.trigger}
                </span>
                <span
                  className="type-data text-[13px] text-ink-2"
                  title={backup.created_at}
                >
                  created {relativeTime(backup.created_at)}
                </span>
                <span className="type-data text-[13px] text-ink-2">
                  logical {formatBytes(backup.logical_bytes)}
                </span>
                <span className="type-data text-[13px] text-ink-2">
                  uploaded {formatBytes(backup.uploaded_bytes)}
                </span>
                <span className="type-data text-[13px] text-ink-2">
                  pause {formatPause(backup.pause_ms, backup.snapshot_method)}
                </span>
                <span
                  className="type-code ml-auto text-[12px] text-ink-2"
                  title={backup.id}
                >
                  {shortId(backup.id)}
                </span>
                {backup.error ? (
                  <p className="w-full text-[13px] leading-snug text-fault">
                    {backup.error}
                  </p>
                ) : null}
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}

/**
 * The app's storage: its volume, the HA policies with their estimated cost,
 * the backup schedule while `cold` runs it, and the backups so far (ADR 0020,
 * ADR 0145, ADR 0147). Restore stays API-only in M5a.
 */
export function AppStorage({
  app,
  canWrite,
  nodeNames,
  onChanged,
}: {
  app: App;
  canWrite: boolean;
  nodeNames: Record<string, string>;
  onChanged: () => void;
}) {
  const queryClient = useQueryClient();

  const refresh = () => {
    void queryClient.invalidateQueries({
      queryKey: getListAppVolumesQueryKey(app.id),
    });
    void queryClient.invalidateQueries({
      queryKey: getListAppBackupsQueryKey(app.id),
    });
    void queryClient.invalidateQueries({
      queryKey: getListBackupRepositoriesQueryKey(),
    });
    void queryClient.invalidateQueries({ queryKey: getGetAppQueryKey(app.id) });
    onChanged();
  };

  const volumesQuery = useListAppVolumes(app.id, {
    query: { enabled: app.stateful },
  });
  const volume = volumesQuery.data?.items[0];

  return (
    <section className="mt-5 rounded-[10px] border border-line bg-surface px-4 py-4 sm:px-5">
      <h2 className="type-display-light text-[15px] leading-none text-ink">
        Storage and HA
      </h2>

      {!app.stateful ? (
        <>
          <p className="mt-2 text-[13px] leading-snug text-ink-2">
            This app keeps no data across redeploys. Add a volume to pin it to one
            node and keep its files.
          </p>
          <AddVolumeForm appId={app.id} canWrite={canWrite} onChanged={refresh} />
        </>
      ) : (
        <>
          <div className="mt-3">
            {volumesQuery.error !== null && volumesQuery.error !== undefined ? (
              <ProblemBanner
                view={problemView(volumesQuery.error, 'see the volume')}
              />
            ) : volumesQuery.isPending ? (
              <p className="text-[13px] text-ink-2">Reading the volume…</p>
            ) : volume === undefined ? (
              <p className="text-[13px] text-ink-2">
                No volume is attached to this app right now.
              </p>
            ) : (
              <VolumeLine volume={volume} nodeNames={nodeNames} />
            )}
          </div>

          <div className="mt-4 border-t border-line pt-3">
            <h3 className="type-bay-title text-[14px] leading-tight text-ink">
              HA policy
            </h3>
            <HaPolicyControl app={app} canWrite={canWrite} onSaved={refresh} />
            {app.ha_cost === undefined || app.ha_cost === null ? (
              <p className="mt-3 text-[13px] leading-snug text-ink-2">
                What each policy would cost this app is not computed yet.
              </p>
            ) : (
              <HaCostTable
                policies={app.ha_cost.policies}
                current={app.ha_policy}
                snapshotMethod={app.ha_cost.snapshot_method}
              />
            )}
          </div>

          {app.ha_policy === 'cold' ? (
            <div className="mt-4 border-t border-line pt-3">
              <h3 className="type-bay-title text-[14px] leading-tight text-ink">
                Backup schedule
              </h3>
              <BackupScheduleForm
                app={app}
                canWrite={canWrite}
                onSaved={refresh}
              />
            </div>
          ) : null}

          <div className="mt-4 border-t border-line pt-3">
            <h3 className="type-bay-title text-[14px] leading-tight text-ink">
              Backups
            </h3>
            <BackupsList app={app} canWrite={canWrite} onChanged={refresh} />
          </div>
        </>
      )}
    </section>
  );
}
