import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';
import { useState, type FormEvent } from 'react';
import {
  createBackupRepository,
  getListBackupRepositoriesQueryKey,
  updateBackupRepository,
} from '../api/generated/backups/backups';
import type {
  AuditExport,
  BackupRepository,
  CreateBackupRepositoryRequest,
  Provider,
  UpdateBackupRepositoryRequest,
} from '../api/generated/model';
import {
  bucketWords,
  newRepositoryForm,
  readNewRepository,
  readRepositorySettings,
  readSchedule,
  repositoryProviderChoices,
  repositorySettingsForm,
  settingsChange,
  type RepositoryErrors,
  type RepositoryForm,
  type RepositorySettingsErrors,
  type RepositorySettingsForm,
  type ScheduleErrors,
  type ScheduleForm,
} from '../lib/backup-forms';
import { problemView, type ProblemView } from '../lib/problem';
import { scheduleSummary } from '../lib/storage-words';
import { ScheduleFields } from './schedule-fields';
import { SecretOnce } from './secret-once';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';
import { Field } from './ui/field';
import { Input } from './ui/input';
import { ProblemBanner } from './ui/problem-banner';

const NEW_KEY_NOTE =
  'Copy it now and keep it away from this cluster, in a password manager or on paper. Quoro shows it only once. With it, the backups can be opened even if every node is lost.';

const CONTROL_PLANE_LINE =
  'The cluster database, copied on the primary on this schedule and before every upgrade.';

/** A field's error under its control, as the schedule fields show theirs. */
function FieldError({ text }: { text: string | undefined }) {
  return text ? <p className="text-[12.5px] text-fault">{text}</p> : null;
}

/** The control-plane schedule: what it copies, the two numbers, and the schedule in words once it reads. */
function ControlPlaneSchedule({
  idPrefix,
  value,
  errors,
  disabled,
  onChange,
}: {
  idPrefix: string;
  value: ScheduleForm;
  errors: ScheduleErrors;
  disabled: boolean;
  onChange: (value: ScheduleForm) => void;
}) {
  const { schedule } = readSchedule(value);
  return (
    <fieldset className="rounded-[10px] border border-line bg-sunk/40 px-3.5 pb-3.5 pt-2">
      <legend className="type-body-medium px-1 text-[13px] text-ink-2">Control-plane backups</legend>
      <p className="mb-3 text-[13px] leading-snug text-ink-2">{CONTROL_PLANE_LINE}</p>
      <ScheduleFields
        idPrefix={idPrefix}
        value={value}
        errors={errors}
        disabled={disabled}
        onChange={onChange}
      />
      <p className="mt-3 border-t border-line pt-2.5 text-[13px] text-ink" aria-live="polite">
        {schedule ? scheduleSummary(schedule) : 'Fix the numbers above to see the schedule.'}
      </p>
    </fieldset>
  );
}

/**
 * Create the cluster's backup repository (`createBackupRepository`): a name,
 * the object storage provider whose bucket holds it, a prefix in that bucket
 * and the control-plane schedule. The answer carries the recovery key once;
 * the dialog then shows it the way replacing the key does, and drops it with
 * the mutation when it closes. Mount it only while it is open.
 */
export function CreateRepositoryDialog({
  providers,
  auditExports,
  loading,
  onClose,
  onCreated,
}: {
  providers: readonly Provider[];
  auditExports: ReadonlyArray<Pick<AuditExport, 'provider_id'>>;
  loading: boolean;
  onClose: () => void;
  onCreated: (repository: BackupRepository) => void;
}) {
  const queryClient = useQueryClient();
  const choices = repositoryProviderChoices(providers, auditExports);
  const [form, setForm] = useState<RepositoryForm>(() => newRepositoryForm(choices));
  const [errors, setErrors] = useState<RepositoryErrors>({});
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const create = useMutation({
    mutationFn: (body: CreateBackupRepositoryRequest) =>
      createBackupRepository(body, { headers: { 'Idempotency-Key': crypto.randomUUID() } }),
    // The answer holds the recovery key: keep the mutation no longer than this dialog.
    gcTime: 0,
    onSuccess: () => {
      setProblem(null);
      void queryClient.invalidateQueries({ queryKey: getListBackupRepositoriesQueryKey() });
    },
    onError: (error) => setProblem(problemView(error, 'create the backup repository')),
  });
  const created = create.data;

  // The providers may arrive after the dialog opens: fall back to the first usable one.
  const providerId =
    form.providerId !== ''
      ? form.providerId
      : (choices.find((choice) => choice.refusal === null)?.provider.id ?? '');
  const chosen = choices.find((choice) => choice.provider.id === providerId);
  const usable = choices.some((choice) => choice.refusal === null);

  const close = () => {
    if (create.isPending) return;
    if (created) onCreated(created);
    // Drop the key with the dialog: it lives in the mutation result only.
    create.reset();
    onClose();
  };

  const update = (patch: Partial<RepositoryForm>) => setForm((current) => ({ ...current, ...patch }));

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setProblem(null);
    const read = readNewRepository({ ...form, providerId }, choices);
    setErrors(read.errors);
    if (!read.body) return;
    create.mutate(read.body);
  };

  return (
    <Dialog open onOpenChange={(open) => (open ? undefined : close())}>
      {created ? (
        <DialogContent
          title="New recovery key"
          description={`For ${created.name}. Fingerprint ${created.recovery_key_fingerprint}.`}
        >
          <div className="flex flex-col gap-4">
            <SecretOnce label="Recovery key" secret={created.recovery_key} note={NEW_KEY_NOTE} />
            <div className="flex flex-wrap justify-end gap-2 border-t border-line pt-4">
              <Button variant="primary" onClick={close}>
                I have kept it
              </Button>
            </div>
          </div>
        </DialogContent>
      ) : (
        <DialogContent
          title="Create the backup repository"
          description="Where app volumes and the cluster database are backed up, encrypted. Quoro shows its recovery key once, right after this."
        >
          <form className="flex flex-col gap-4" onSubmit={submit} noValidate>
            {problem ? <ProblemBanner view={problem} /> : null}

            <Field label="Name" htmlFor="repository-name">
              <Input
                id="repository-name"
                value={form.name}
                maxLength={200}
                placeholder="cluster-backups"
                autoComplete="off"
                spellCheck={false}
                disabled={create.isPending}
                onChange={(event) => update({ name: event.target.value })}
                aria-invalid={errors.name !== undefined}
              />
              <FieldError text={errors.name} />
            </Field>

            {choices.length === 0 && !loading ? (
              <div className="rounded-[10px] border border-line bg-sunk/40 px-3.5 py-3 text-[13.5px] leading-snug text-ink">
                <p className="type-body-medium">No object storage provider yet.</p>
                <p className="mt-1 text-[13px] text-ink-2">
                  The repository lives in a bucket. Add an object storage provider in{' '}
                  <Link to="/settings/providers" className="set-inline-link">
                    Providers
                  </Link>{' '}
                  first, then come back.
                </p>
              </div>
            ) : (
              <Field
                label="Object storage provider"
                htmlFor="repository-provider"
                hint={
                  chosen
                    ? (bucketWords(chosen.provider.config) ?? 'Its bucket holds every backup.')
                    : 'Its bucket holds every backup.'
                }
              >
                <select
                  id="repository-provider"
                  className="field"
                  value={providerId}
                  disabled={create.isPending || loading}
                  onChange={(event) => update({ providerId: event.target.value })}
                  aria-invalid={errors.providerId !== undefined}
                >
                  {loading ? <option value="">Loading providers…</option> : null}
                  {!loading && !usable ? <option value="">No usable provider</option> : null}
                  {choices.map((choice) => (
                    <option key={choice.provider.id} value={choice.provider.id} disabled={choice.refusal !== null}>
                      {choice.refusal
                        ? `${choice.provider.name} (${choice.refusal.toLowerCase()})`
                        : choice.provider.name}
                    </option>
                  ))}
                </select>
                <FieldError text={errors.providerId} />
              </Field>
            )}

            <Field
              label="Prefix"
              htmlFor="repository-prefix"
              hint="The folder inside the bucket. It must not hold a repository already."
            >
              <Input
                id="repository-prefix"
                className="type-code"
                value={form.prefix}
                maxLength={200}
                autoComplete="off"
                spellCheck={false}
                disabled={create.isPending}
                onChange={(event) => update({ prefix: event.target.value })}
                aria-invalid={errors.prefix !== undefined}
              />
              <FieldError text={errors.prefix} />
            </Field>

            <ControlPlaneSchedule
              idPrefix="repository-schedule"
              value={form.schedule}
              errors={errors}
              disabled={create.isPending}
              onChange={(schedule) => update({ schedule })}
            />

            <div className="flex flex-wrap justify-end gap-2 border-t border-line pt-4">
              <Button variant="default" type="button" disabled={create.isPending} onClick={close}>
                Cancel
              </Button>
              <Button
                variant="primary"
                type="submit"
                disabled={create.isPending || (!loading && !usable)}
              >
                {create.isPending ? 'Creating…' : 'Create repository'}
              </Button>
            </div>
          </form>
        </DialogContent>
      )}
    </Dialog>
  );
}

/**
 * A repository's settings (`updateBackupRepository`): its name and the
 * control-plane schedule. Only what changed is sent; the provider and the
 * prefix cannot change. Mount it only while it is open.
 */
export function RepositorySettingsDialog({
  repository,
  onClose,
  onSaved,
}: {
  repository: BackupRepository;
  onClose: () => void;
  onSaved: (repository: BackupRepository) => void;
}) {
  const queryClient = useQueryClient();
  const [form, setForm] = useState<RepositorySettingsForm>(() => repositorySettingsForm(repository));
  const [errors, setErrors] = useState<RepositorySettingsErrors>({});
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const save = useMutation({
    mutationFn: (body: UpdateBackupRepositoryRequest) => updateBackupRepository(repository.id, body),
    onSuccess: (updated) => {
      setProblem(null);
      void queryClient.invalidateQueries({ queryKey: getListBackupRepositoriesQueryKey() });
      onSaved(updated);
    },
    onError: (error) => setProblem(problemView(error, 'change the backup repository')),
  });

  const close = () => {
    if (save.isPending) return;
    onClose();
  };

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setProblem(null);
    const read = readRepositorySettings(repository, form);
    setErrors(read.errors);
    if (!read.body) return;
    if (!settingsChange(read.body)) {
      setProblem({ message: 'Nothing has changed yet. Change a field, then save.' });
      return;
    }
    save.mutate(read.body);
  };

  return (
    <Dialog open onOpenChange={(open) => (open ? undefined : close())}>
      <DialogContent
        title="Repository settings"
        description={`The name of ${repository.name} and when the cluster database is backed up. The provider and the prefix stay as they are.`}
      >
        <form className="flex flex-col gap-4" onSubmit={submit} noValidate>
          {problem ? <ProblemBanner view={problem} /> : null}
          <Field label="Name" htmlFor="repository-settings-name">
            <Input
              id="repository-settings-name"
              value={form.name}
              maxLength={200}
              autoComplete="off"
              spellCheck={false}
              disabled={save.isPending}
              onChange={(event) => setForm((current) => ({ ...current, name: event.target.value }))}
              aria-invalid={errors.name !== undefined}
            />
            <FieldError text={errors.name} />
          </Field>
          <ControlPlaneSchedule
            idPrefix="repository-settings"
            value={form.schedule}
            errors={errors}
            disabled={save.isPending}
            onChange={(schedule) => setForm((current) => ({ ...current, schedule }))}
          />
          <div className="flex flex-wrap justify-end gap-2 border-t border-line pt-4">
            <Button variant="default" type="button" disabled={save.isPending} onClick={close}>
              Cancel
            </Button>
            <Button variant="primary" type="submit" disabled={save.isPending}>
              {save.isPending ? 'Saving…' : 'Save settings'}
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
}
