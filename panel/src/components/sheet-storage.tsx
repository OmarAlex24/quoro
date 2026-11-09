// SPDX-License-Identifier: AGPL-3.0-only
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { CalendarClock, HardDrive } from 'lucide-react';
import { useCallback, useState, type KeyboardEvent } from 'react';
import { getGetAppQueryKey, updateApp } from '../api/generated/apps/apps';
import {
  createBackup,
  getListAppBackupsQueryKey,
  getListBackupRepositoriesQueryKey,
  useListAppBackups,
  useListBackupRepositories,
} from '../api/generated/backups/backups';
import type { AppDetail, AppHaPolicy, BackupSchedule, HaPolicyCost, Volume } from '../api/generated/model';
import { readSchedule, scheduleForm, type ScheduleErrors, type ScheduleForm } from '../lib/backup-forms';
import { createVolume, getListAppVolumesQueryKey, useListAppVolumes } from '../api/generated/volumes/volumes';
import { relativeTime } from '../lib/app-status';
import { drawMeter, type DitherSurface, type TokenReader } from '../lib/dither-draw';
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
import {
  backupLine,
  haSentence,
  orderVolumes,
  scheduleWords,
  volumeShare,
  volumeTag,
  volumeUsedWords,
} from '../lib/storage-words';
import { DitherCanvas } from './dither-canvas';
import { LampDot } from './lamp';
import { ScheduleFields } from './schedule-fields';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';
import { Field } from './ui/field';
import { Input } from './ui/input';
import { ProblemBanner } from './ui/problem-banner';
import { Tag } from './ui/tag';

/**
 * The Storage tab: the app's volume and how full it is, its backups with the
 * schedule and the next one due, and its HA policy as a choice of four with
 * one sentence for the chosen one. The seven-column cost table opens from
 * Compare. Every action the app page had stays: add a volume, change the
 * policy, edit the schedule and back up now.
 */
export function StorageTab({
  app,
  canWrite,
  nodeNames,
  onChanged,
}: {
  app: AppDetail;
  canWrite: boolean;
  nodeNames: Record<string, string>;
  onChanged: () => void;
}) {
  const queryClient = useQueryClient();
  const refresh = () => {
    void queryClient.invalidateQueries({ queryKey: getListAppVolumesQueryKey(app.id) });
    void queryClient.invalidateQueries({ queryKey: getListAppBackupsQueryKey(app.id) });
    void queryClient.invalidateQueries({ queryKey: getListBackupRepositoriesQueryKey() });
    void queryClient.invalidateQueries({ queryKey: getGetAppQueryKey(app.id) });
    onChanged();
  };

  const volumesQuery = useListAppVolumes(app.id, { query: { enabled: app.stateful } });
  const volumes = orderVolumes(volumesQuery.data?.items ?? []);

  if (!app.stateful) {
    return <AddVolume appId={app.id} canWrite={canWrite} onChanged={refresh} />;
  }

  return (
    <>
      {volumesQuery.error ? (
        <ProblemBanner view={problemView(volumesQuery.error, 'see the volume')} />
      ) : volumesQuery.isPending ? (
        <div className="box h-[104px] opacity-60" aria-hidden="true" />
      ) : volumes.length === 0 ? (
        <p className="box px-4 py-4 text-[13px] text-ink-2">No volume is attached to this app right now.</p>
      ) : (
        volumes.map((volume) => (
          <VolumeBox key={volume.id} volume={volume} node={nodeNames[volume.node_id] ?? 'an unknown node'} />
        ))
      )}
      <BackupsBox app={app} volumes={volumes} canWrite={canWrite} onChanged={refresh} />
      <HaBox app={app} canWrite={canWrite} onSaved={refresh} />
    </>
  );
}

/** One volume: its mount path and node, its state, and how full the sparse file is. */
function VolumeBox({ volume, node }: { volume: Volume; node: string }) {
  const tag = volumeTag(volume.state);
  const share = volumeShare(volume);
  const paint = useCallback(
    (surface: DitherSurface, read: TokenReader) => drawMeter(surface, share ?? 0, read),
    [share],
  );
  return (
    <section className="box" aria-label={`Volume at ${volume.mount_path}`}>
      <div className="box-h">
        <span className="glyph glyph-neutral" aria-hidden="true">
          <HardDrive strokeWidth={1.7} />
        </span>
        <div className="box-t">
          <b>Volume</b>
          <small>
            <span className="type-code text-ink-2" title={volume.mount_path}>
              {volume.mount_path}
            </span>
            <span aria-hidden="true">·</span>
            <span>{node}</span>
          </small>
        </div>
        <Tag tone={tag.tone}>{tag.words}</Tag>
      </div>
      <div className="box-b">
        <div className="meter">
          <div className="meter-h">
            <b>Used</b>
            <span>{volumeUsedWords(volume)}</span>
          </div>
          <DitherCanvas
            paint={paint}
            label={share === null ? undefined : `${Math.round(share * 100)}% of the volume used`}
          />
        </div>
        {volume.restored_from_backup_id ? (
          <p className="mt-2.5 text-[12px] text-ink-2">
            Restored from backup{' '}
            <span className="type-code" title={volume.restored_from_backup_id}>
              {volume.restored_from_backup_id}
            </span>
          </p>
        ) : null}
        {volume.error ? <p className="mt-2.5 text-[12.5px] leading-snug text-fault">{volume.error}</p> : null}
      </div>
    </section>
  );
}

/** A stateless app: what a volume does, and the form that adds one. */
function AddVolume({ appId, canWrite, onChanged }: { appId: string; canWrite: boolean; onChanged: () => void }) {
  const [size, setSize] = useState('10');
  const [mountPath, setMountPath] = useState('/data');
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [errors, setErrors] = useState<Record<string, string>>({});

  const add = useMutation({
    mutationFn: (body: { size_gb: number; mount_path: string }) =>
      createVolume(appId, body, { headers: { 'Idempotency-Key': crypto.randomUUID() } }),
    onSuccess: () => {
      setProblem(null);
      setErrors({});
      onChanged();
    },
    onError: (error) => setProblem(problemView(error, 'add volumes')),
  });

  const submit = () => {
    setProblem(null);
    const next: Record<string, string> = {};
    const sizeProblem = volumeSizeProblem(size);
    if (sizeProblem) next.size = sizeProblem;
    const pathProblem = mountPathProblem(mountPath);
    if (pathProblem) next.mount = pathProblem;
    setErrors(next);
    if (Object.keys(next).length > 0) return;
    add.mutate({ size_gb: Number(size), mount_path: mountPath });
  };

  return (
    <section className="box" aria-label="Volume">
      <div className="box-h">
        <span className="glyph glyph-neutral" aria-hidden="true">
          <HardDrive strokeWidth={1.7} />
        </span>
        <div className="box-t">
          <b>No volume</b>
          <small>This app keeps no data across redeploys. A volume pins it to one node and keeps its files.</small>
        </div>
      </div>
      <div className="box-b">
        {canWrite ? (
          <div className="flex flex-col gap-3 sm:flex-row sm:items-start">
            <Field label="Size (GiB)" htmlFor="volume-size" className="sm:w-28">
              <Input
                id="volume-size"
                type="number"
                min={1}
                max={1024}
                step={1}
                inputMode="numeric"
                value={size}
                onChange={(event) => setSize(event.target.value)}
                aria-invalid={errors.size !== undefined}
              />
              {errors.size ? <p className="text-[12.5px] text-fault">{errors.size}</p> : null}
            </Field>
            <Field
              label="Mount path"
              htmlFor="volume-mount"
              hint="Where the guest mounts it; it attaches at the next start."
              className="min-w-0 flex-1"
            >
              <Input
                id="volume-mount"
                value={mountPath}
                onChange={(event) => setMountPath(event.target.value)}
                spellCheck={false}
                aria-invalid={errors.mount !== undefined}
              />
              {errors.mount ? <p className="text-[12.5px] text-fault">{errors.mount}</p> : null}
            </Field>
            <div className="sm:pt-[22px]">
              <Button variant="primary" disabled={add.isPending} onClick={submit}>
                {add.isPending ? 'Adding…' : 'Add a volume'}
              </Button>
            </div>
          </div>
        ) : (
          <p className="text-[13px] text-ink-2">Your role can read storage but not add a volume.</p>
        )}
        {problem ? <ProblemBanner view={problem} className="mt-3" /> : null}
      </div>
    </section>
  );
}

/** The app's backups, newest first, with the schedule in the heading and Back up now. */
function BackupsBox({
  app,
  volumes,
  canWrite,
  onChanged,
}: {
  app: AppDetail;
  volumes: readonly Volume[];
  canWrite: boolean;
  onChanged: () => void;
}) {
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [scheduleOpen, setScheduleOpen] = useState(false);
  const backupsQuery = useListAppBackups(
    app.id,
    { limit: 50 },
    {
      query: {
        refetchInterval: (query) =>
          query.state.data?.items.some((backup) => backup.status === 'queued' || backup.status === 'running')
            ? 2_000
            : 10_000,
      },
    },
  );
  const repositoriesQuery = useListBackupRepositories({ query: { staleTime: 30_000 } });
  const backupNow = useMutation({
    mutationFn: () => createBackup(app.id, { headers: { 'Idempotency-Key': crypto.randomUUID() } }),
    onSuccess: () => {
      setProblem(null);
      onChanged();
    },
    onError: (error) => setProblem(problemView(error, 'back up the app')),
  });

  const repository = repositoriesQuery.data?.items[0];
  const ready = volumes.some((volume) => volume.state === 'ready');
  const backups = [...(backupsQuery.data?.items ?? [])].sort(
    (left, right) => Date.parse(right.created_at) - Date.parse(left.created_at),
  );
  const disabledReason = !canWrite
    ? 'Your role can read backups but not take one.'
    : repositoriesQuery.isPending
      ? null
      : repository === undefined
        ? 'A backup needs a backup repository, and none is configured.'
        : !ready
          ? 'A backup needs the volume to be ready.'
          : null;

  return (
    <section className="box" aria-labelledby="backups-title">
      <div className="box-h box-h-wrap">
        <div className="box-t">
          <b id="backups-title">Backups</b>
          <small>
            <CalendarClock className="size-3.5 text-ink-3" strokeWidth={1.7} aria-hidden="true" />
            {backupsQuery.data ? scheduleWords(app, backups) : 'Reading the schedule…'}
          </small>
        </div>
        <div className="flex items-center gap-1.5">
          {canWrite && app.ha_policy === 'cold' ? (
            <Button size="sm" variant="ghost" onClick={() => setScheduleOpen(true)}>
              Edit schedule
            </Button>
          ) : null}
          <Button
            size="sm"
            disabled={!canWrite || repositoriesQuery.isPending || disabledReason !== null || backupNow.isPending}
            onClick={() => {
              setProblem(null);
              backupNow.mutate();
            }}
          >
            {backupNow.isPending ? 'Starting…' : 'Back up now'}
          </Button>
        </div>
      </div>
      {disabledReason ? <p className="box-note">{disabledReason}</p> : null}
      {problem ? <ProblemBanner view={problem} className="mx-3.5 mb-3" /> : null}
      {backupsQuery.error ? (
        <ProblemBanner view={problemView(backupsQuery.error, 'see backups')} className="mx-3.5 mb-3" />
      ) : backupsQuery.isPending ? (
        <p className="box-row text-[12.5px] text-ink-2">Reading backups…</p>
      ) : backups.length === 0 ? (
        <p className="box-row text-[12.5px] text-ink-2">
          No backups yet. The Cold policy takes them on its schedule; Back up now takes one outside it.
        </p>
      ) : (
        <ul className="box-list" aria-label="Backups">
          {backups.map((backup) => {
            const line = backupLine(backup);
            return (
              <li key={backup.id} className="box-row">
                <LampDot ring={line.tone} core={line.tone} pulse={line.pulse} size={14} />
                <div className="box-t">
                  <b>{line.words}</b>
                  <small title={backup.created_at}>{line.detail}</small>
                </div>
              </li>
            );
          })}
        </ul>
      )}
      {repository ? (
        <p className="box-foot">
          Stored in {repository.name} <span className="type-code">{repository.prefix}</span>
        </p>
      ) : null}
      {scheduleOpen ? (
        <ScheduleDialog app={app} canWrite={canWrite} onClose={() => setScheduleOpen(false)} onSaved={onChanged} />
      ) : null}
    </section>
  );
}

/** `interval_h` and `keep`, as the app page's form had them, in a dialog. */
function ScheduleDialog({
  app,
  canWrite,
  onClose,
  onSaved,
}: {
  app: AppDetail;
  canWrite: boolean;
  onClose: () => void;
  onSaved: () => void;
}) {
  const [schedule, setSchedule] = useState<ScheduleForm>(() => scheduleForm(app.backup_schedule));
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [errors, setErrors] = useState<ScheduleErrors>({});

  const save = useMutation({
    mutationFn: (body: BackupSchedule) => updateApp(app.id, { backup_schedule: body }),
    onSuccess: () => {
      setProblem(null);
      onSaved();
      onClose();
    },
    onError: (error) => setProblem(problemView(error, 'change the backup schedule')),
  });

  const submit = () => {
    setProblem(null);
    const read = readSchedule(schedule);
    setErrors(read.errors);
    if (!read.schedule) return;
    save.mutate(read.schedule);
  };

  return (
    <Dialog open onOpenChange={(open) => (open ? undefined : onClose())}>
      <DialogContent title="Backup schedule" description="Scheduled backups run while the HA policy is Cold.">
        <ScheduleFields
          idPrefix="backup"
          value={schedule}
          errors={errors}
          disabled={!canWrite}
          onChange={setSchedule}
        />
        {problem ? <ProblemBanner view={problem} className="mt-3" /> : null}
        <div className="mt-5 flex justify-end gap-2">
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="primary" disabled={save.isPending} onClick={submit}>
            {save.isPending ? 'Saving…' : 'Save schedule'}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}

/**
 * The HA policy as four choices and one sentence for the chosen one, with its
 * estimate. A policy the app cannot take is disabled and says why; Compare
 * opens what each one would cost.
 */
function HaBox({ app, canWrite, onSaved }: { app: AppDetail; canWrite: boolean; onSaved: () => void }) {
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [comparing, setComparing] = useState(false);
  const change = useMutation({
    mutationFn: (policy: AppHaPolicy) => updateApp(app.id, { ha_policy: policy }),
    onSuccess: () => {
      setProblem(null);
      onSaved();
    },
    onError: (error) => setProblem(problemView(error, 'change the HA policy')),
  });
  const costs = app.ha_cost?.policies ?? [];
  const costOf = (policy: AppHaPolicy) => costs.find((row) => row.policy === policy);
  const availability = (policy: AppHaPolicy) => {
    const row = costOf(policy);
    return row ? policyAvailability(row) : { available: policy === 'none', reason: null };
  };
  const pick = (policy: AppHaPolicy) => {
    if (policy === app.ha_policy || !availability(policy).available) return;
    change.mutate(policy);
  };
  const choices = HA_POLICIES.filter((policy) => policy === app.ha_policy || availability(policy).available);
  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    const step = event.key === 'ArrowRight' || event.key === 'ArrowDown' ? 1 : event.key === 'ArrowLeft' || event.key === 'ArrowUp' ? -1 : 0;
    if (step === 0 || !canWrite || change.isPending) return;
    event.preventDefault();
    const at = choices.indexOf(app.ha_policy);
    const next = choices[(at + step + choices.length) % choices.length];
    if (next) {
      pick(next);
      event.currentTarget.querySelector<HTMLButtonElement>(`[data-policy="${next}"]`)?.focus();
    }
  };

  return (
    <section className="box" aria-labelledby="ha-title">
      <div className="box-h box-h-wrap">
        <div className="box-t">
          <b id="ha-title">High availability</b>
          <small className="box-t-wrap">
            {change.isPending ? 'Saving the policy…' : haSentence(app.ha_policy, costOf(app.ha_policy))}
          </small>
        </div>
        <div role="radiogroup" aria-labelledby="ha-title" className="seg seg-fit" onKeyDown={onKeyDown}>
          {HA_POLICIES.map((policy) => {
            const { available, reason } = availability(policy);
            const on = policy === app.ha_policy;
            return (
              <button
                key={policy}
                type="button"
                role="radio"
                aria-checked={on}
                data-policy={policy}
                tabIndex={on ? 0 : -1}
                disabled={!canWrite || change.isPending || (!on && !available)}
                title={reason ? `${haPolicyWords[policy]}: ${reason}` : undefined}
                onClick={() => pick(policy)}
              >
                {haPolicyWords[policy]}
              </button>
            );
          })}
        </div>
      </div>
      <div className="box-foot box-foot-row">
        <span>
          {!canWrite
            ? 'Your role can read the policy but not change it.'
            : app.ha_cost
              ? 'Costs are estimates, not reservations.'
              : 'What each policy would cost this app is not computed yet.'}
        </span>
        {app.ha_cost ? (
          <Button size="sm" variant="ghost" onClick={() => setComparing(true)}>
            Compare
          </Button>
        ) : null}
      </div>
      {problem ? <ProblemBanner view={problem} className="mx-3.5 mb-3" /> : null}
      {comparing && app.ha_cost ? (
        <CompareDialog
          policies={app.ha_cost.policies}
          current={app.ha_policy}
          snapshotMethod={app.ha_cost.snapshot_method}
          computedAt={app.ha_cost.computed_at}
          onClose={() => setComparing(false)}
        />
      ) : null}
    </section>
  );
}

/**
 * What each policy costs this app, policies across and costs down, so the
 * table fits a phone. The chosen policy's column is marked.
 */
function CompareDialog({
  policies,
  current,
  snapshotMethod,
  computedAt,
  onClose,
}: {
  policies: readonly HaPolicyCost[];
  current: AppHaPolicy;
  snapshotMethod: NonNullable<AppDetail['ha_cost']>['snapshot_method'];
  computedAt: string;
  onClose: () => void;
}) {
  const rows: ReadonlyArray<readonly [string, (row: HaPolicyCost) => string]> = [
    ['Reserved vCPU', (row) => formatVcpus(row.reserved.vcpus)],
    ['Reserved memory', (row) => formatMemory(row.reserved)],
    ['Reserved disk', (row) => formatDisk(row.reserved)],
    ['Storage', (row) => formatBytes(row.storage_bytes)],
    ['Restore estimate', (row) => formatRestoreEstimate(row.restore_estimate_s)],
    ['Pause per backup', (row) => formatPause(row.pause_estimate_ms, snapshotMethod)],
  ];
  const method = snapshotMethodWords(snapshotMethod);
  return (
    <Dialog open onOpenChange={(open) => (open ? undefined : onClose())}>
      <DialogContent
        title="What each policy costs"
        description={`Estimates, not reservations, computed ${relativeTime(computedAt)}.`}
      >
        <table className="cmp">
          <thead>
            <tr>
              <th scope="col">
                <span className="sr-only">Cost</span>
              </th>
              {policies.map((row) => (
                <th key={row.policy} scope="col" aria-current={row.policy === current ? 'true' : undefined}>
                  {haPolicyWords[row.policy]}
                  {policyAvailability(row).reason ? (
                    <small>{policyAvailability(row).reason}</small>
                  ) : row.policy === current ? (
                    <small>current</small>
                  ) : null}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map(([label, value]) => (
              <tr key={label}>
                <th scope="row">{label}</th>
                {policies.map((row) => (
                  <td key={row.policy} aria-current={row.policy === current ? 'true' : undefined}>
                    {value(row)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
        <p className="mt-3 text-[12.5px] leading-snug text-ink-2">
          {method ? `The volume's node ${method}.` : 'This app has no volume, so there is nothing to snapshot.'}
        </p>
      </DialogContent>
    </Dialog>
  );
}
