// SPDX-License-Identifier: AGPL-3.0-only
import type {
  AuditExport,
  BackupRepository,
  BackupSchedule,
  CreateBackupRepositoryRequest,
  Provider,
  ProviderConfig,
  UpdateBackupRepositoryRequest,
} from '../api/generated/model';

/*
 * The backup forms: a schedule (an app's and the control plane's take the same
 * two numbers, openapi.yaml `BackupSchedule`), a new backup repository
 * (`CreateBackupRepositoryRequest`) and a repository's settings
 * (`UpdateBackupRepositoryRequest`). Pure, so every rule and every body the
 * panel sends can be tested without a DOM. The server checks the same rules
 * and stays the final word.
 */

/** The schedule a new repository's control-plane backups start with (`BackupSchedule`). */
export const CONTROL_PLANE_SCHEDULE: BackupSchedule = { interval_h: 6, keep: 7 };

/** The contract's default key prefix inside the bucket. */
export const DEFAULT_PREFIX = 'quoro-backups';

/** A schedule as typed: two text fields, read into numbers on submit. */
export interface ScheduleForm {
  interval: string;
  keep: string;
}

export type ScheduleErrors = Partial<Record<keyof ScheduleForm, string>>;

export const scheduleForm = (schedule: Pick<BackupSchedule, 'interval_h' | 'keep'>): ScheduleForm => ({
  interval: String(schedule.interval_h),
  keep: String(schedule.keep),
});

const wholeNumberIn = (text: string, minimum: number, maximum: number): number | null => {
  if (text.trim() === '') return null;
  const value = Number(text);
  return Number.isInteger(value) && value >= minimum && value <= maximum ? value : null;
};

/**
 * The schedule the form holds, or why it cannot be sent: `interval_h` is 1 to
 * 168 hours and `keep` 1 to 100 backups.
 */
export const readSchedule = (
  form: ScheduleForm,
): { schedule: BackupSchedule | null; errors: ScheduleErrors } => {
  const interval = wholeNumberIn(form.interval, 1, 168);
  const keep = wholeNumberIn(form.keep, 1, 100);
  const errors: ScheduleErrors = {};
  if (interval === null) errors.interval = 'Use a whole number of hours, 1 to 168.';
  if (keep === null) errors.keep = 'Keep a whole number of backups, 1 to 100.';
  return {
    schedule: interval !== null && keep !== null ? { interval_h: interval, keep } : null,
    errors,
  };
};

/** Why a repository name cannot be sent, or null: 1 to 200 characters once trimmed. */
export const repositoryNameProblem = (name: string): string | null => {
  const length = [...name.trim()].length;
  if (length === 0) return 'Name the repository.';
  if (length > 200) return 'Use at most 200 characters.';
  return null;
};

const PREFIX = /^[A-Za-z0-9._-]+(\/[A-Za-z0-9._-]+)*$/;

/** Why a key prefix cannot be sent, or null (`CreateBackupRepositoryRequest.prefix`). */
export const prefixProblem = (prefix: string): string | null => {
  const value = prefix.trim();
  if (value.length === 0) return `Give a prefix, such as ${DEFAULT_PREFIX}.`;
  if (value.length > 200) return 'Use at most 200 characters.';
  if (!PREFIX.test(value)) {
    return 'Use letters, digits, dots, dashes and underscores, in parts separated by a single slash.';
  }
  return null;
};

/** An object storage provider the repository could live on, and why it cannot when it cannot. */
export interface ProviderChoice {
  provider: Provider;
  refusal: string | null;
}

/**
 * The providers a new repository can pick from: object storage only, by name.
 * The provider of the off-site audit copy is listed but refused, since backup
 * and audit credentials are separate entries (ADR 0147).
 */
export const repositoryProviderChoices = (
  providers: readonly Provider[],
  auditExports: ReadonlyArray<Pick<AuditExport, 'provider_id'>>,
): ProviderChoice[] =>
  providers
    .filter((provider) => provider.kind === 'object_store')
    .sort((left, right) => left.name.localeCompare(right.name))
    .map((provider) => ({
      provider,
      refusal: auditExports.some((copy) => copy.provider_id === provider.id)
        ? 'Holds the off-site copy of the audit log'
        : null,
    }));

/** Where a provider's bucket is, in a few words: `Bucket lab at x.r2.cloudflarestorage.com`. */
export const bucketWords = (config: ProviderConfig): string | null => {
  const bucket = typeof config.bucket === 'string' && config.bucket !== '' ? config.bucket : null;
  let host: string | null = null;
  if (typeof config.endpoint === 'string') {
    try {
      host = new URL(config.endpoint).host || null;
    } catch {
      host = null;
    }
  }
  if (bucket && host) return `Bucket ${bucket} at ${host}`;
  if (bucket) return `Bucket ${bucket}`;
  return null;
};

/** A new repository as typed. */
export interface RepositoryForm {
  name: string;
  providerId: string;
  prefix: string;
  schedule: ScheduleForm;
}

export type RepositoryErrors = Partial<Record<'name' | 'providerId' | 'prefix' | keyof ScheduleForm, string>>;

/** The empty form: the first provider that can hold it, the default prefix and schedule. */
export const newRepositoryForm = (choices: readonly ProviderChoice[]): RepositoryForm => ({
  name: '',
  providerId: choices.find((choice) => choice.refusal === null)?.provider.id ?? '',
  prefix: DEFAULT_PREFIX,
  schedule: scheduleForm(CONTROL_PLANE_SCHEDULE),
});

/** The body `createBackupRepository` takes, or the reason each field cannot be sent. */
export const readNewRepository = (
  form: RepositoryForm,
  choices: readonly ProviderChoice[],
): { body: CreateBackupRepositoryRequest | null; errors: RepositoryErrors } => {
  const errors: RepositoryErrors = {};
  const name = repositoryNameProblem(form.name);
  if (name) errors.name = name;
  const choice = choices.find((item) => item.provider.id === form.providerId);
  if (!choice) errors.providerId = 'Choose an object storage provider.';
  else if (choice.refusal) errors.providerId = `${choice.refusal}; choose another provider.`;
  const prefix = prefixProblem(form.prefix);
  if (prefix) errors.prefix = prefix;
  const schedule = readSchedule(form.schedule);
  Object.assign(errors, schedule.errors);
  if (Object.keys(errors).length > 0 || !schedule.schedule) return { body: null, errors };
  return {
    body: {
      name: form.name.trim(),
      provider_id: form.providerId,
      prefix: form.prefix.trim(),
      control_plane_schedule: schedule.schedule,
    },
    errors,
  };
};

/** A repository's settings as typed: what `updateBackupRepository` can change. */
export interface RepositorySettingsForm {
  name: string;
  schedule: ScheduleForm;
}

export type RepositorySettingsErrors = Partial<Record<'name' | keyof ScheduleForm, string>>;

export const repositorySettingsForm = (
  repository: Pick<BackupRepository, 'name' | 'control_plane_schedule'>,
): RepositorySettingsForm => ({
  name: repository.name,
  schedule: scheduleForm(repository.control_plane_schedule),
});

/**
 * The body `updateBackupRepository` takes, with only the fields that changed
 * (absent fields stay as they are), or the reason each field cannot be sent.
 * An empty body means nothing changed.
 */
export const readRepositorySettings = (
  repository: Pick<BackupRepository, 'name' | 'control_plane_schedule'>,
  form: RepositorySettingsForm,
): { body: UpdateBackupRepositoryRequest | null; errors: RepositorySettingsErrors } => {
  const errors: RepositorySettingsErrors = {};
  const name = repositoryNameProblem(form.name);
  if (name) errors.name = name;
  const schedule = readSchedule(form.schedule);
  Object.assign(errors, schedule.errors);
  if (Object.keys(errors).length > 0 || !schedule.schedule) return { body: null, errors };

  const body: UpdateBackupRepositoryRequest = {};
  const trimmed = form.name.trim();
  if (trimmed !== repository.name) body.name = trimmed;
  const current = repository.control_plane_schedule;
  if (
    schedule.schedule.interval_h !== current.interval_h ||
    schedule.schedule.keep !== current.keep
  ) {
    body.control_plane_schedule = schedule.schedule;
  }
  return { body, errors };
};

/** Whether a settings body changes anything. */
export const settingsChange = (body: UpdateBackupRepositoryRequest): boolean =>
  body.name !== undefined || body.control_plane_schedule !== undefined;
