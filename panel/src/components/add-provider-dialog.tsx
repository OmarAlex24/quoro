import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Archive, Cable, Globe, LockKeyhole, type LucideIcon } from 'lucide-react';
import { useState, type FormEvent, type KeyboardEvent } from 'react';
import type { CreateProviderRequest, Provider } from '../api/generated/model';
import { createProvider, getListProvidersQueryKey } from '../api/generated/providers/providers';
import { problemView, type ProblemView } from '../lib/problem';
import {
  buildCreateProviderBody,
  CREATABLE_KINDS,
  newProviderForm,
  PROVIDER_CONFIG_FIELDS,
  PROVIDER_KIND_USE,
  PROVIDER_KIND_WORDS,
  PROVIDER_SECRET_WORDS,
  providerFormProblems,
  type CreatableKind,
  type ProviderErrors,
  type ProviderForm,
} from '../lib/providers';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';
import { Field } from './ui/field';
import { Input } from './ui/input';
import { ProblemBanner } from './ui/problem-banner';

const KIND_ICON: Record<CreatableKind, LucideIcon> = {
  dns: Globe,
  tunnel: Cable,
  object_store: Archive,
};

const NAME_PLACEHOLDER: Record<CreatableKind, string> = {
  dns: 'cloudflare-dns',
  tunnel: 'cloudflare-tunnel',
  object_store: 'r2-backups',
};

/** The id each error key belongs to, so the first one can take the focus. */
const fieldId = (key: string): string =>
  key.startsWith('config.') ? `provider-config-${key.slice('config.'.length)}` : `provider-${key}`;

/** A field's error under its control, as the schedule fields show theirs. */
function FieldError({ text }: { text: string | undefined }) {
  return text ? <p className="text-[12.5px] text-fault">{text}</p> : null;
}

/** The three kinds as one choice; the arrow keys move it, as the HA policy choice does. */
function KindPicker({
  value,
  disabled,
  onChange,
}: {
  value: CreatableKind;
  disabled: boolean;
  onChange: (kind: CreatableKind) => void;
}) {
  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    const step =
      event.key === 'ArrowRight' || event.key === 'ArrowDown'
        ? 1
        : event.key === 'ArrowLeft' || event.key === 'ArrowUp'
          ? -1
          : 0;
    if (step === 0 || disabled) return;
    event.preventDefault();
    const at = CREATABLE_KINDS.indexOf(value);
    const next = CREATABLE_KINDS[(at + step + CREATABLE_KINDS.length) % CREATABLE_KINDS.length];
    if (next) {
      onChange(next);
      event.currentTarget.querySelector<HTMLButtonElement>(`[data-kind="${next}"]`)?.focus();
    }
  };

  return (
    <div
      role="radiogroup"
      aria-labelledby="provider-kind"
      // On a phone the three kinds stack, so none has to wrap its name.
      className="seg max-sm:flex-col max-sm:[&>button]:justify-start max-sm:[&>button]:py-1.5"
      onKeyDown={onKeyDown}
    >
      {CREATABLE_KINDS.map((kind) => {
        const Icon = KIND_ICON[kind];
        const on = kind === value;
        return (
          <button
            key={kind}
            type="button"
            role="radio"
            aria-checked={on}
            data-kind={kind}
            tabIndex={on ? 0 : -1}
            disabled={disabled}
            onClick={() => onChange(kind)}
          >
            <Icon strokeWidth={1.7} aria-hidden="true" />
            {PROVIDER_KIND_WORDS[kind]}
          </button>
        );
      })}
    </div>
  );
}

/**
 * Add a provider (`createProvider`): its kind, a name, the non-secret settings
 * that kind needs, and the secret. The secret is write-only (D-0051): it lives
 * in this form until it is sent, the mutation is dropped as soon as the dialog
 * closes, and nothing ever shows it again. Mount it only while it is open, so
 * closing it forgets everything typed.
 */
export function AddProviderDialog({
  onClose,
  onAdded,
}: {
  onClose: () => void;
  onAdded: (provider: Provider) => void;
}) {
  const queryClient = useQueryClient();
  const [form, setForm] = useState<ProviderForm>(() => newProviderForm());
  const [errors, setErrors] = useState<ProviderErrors>({});
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const add = useMutation({
    mutationFn: (body: CreateProviderRequest) =>
      createProvider(body, { headers: { 'Idempotency-Key': crypto.randomUUID() } }),
    // The variables hold the secret: keep the mutation no longer than this dialog.
    gcTime: 0,
    onSuccess: (provider) => {
      void queryClient.invalidateQueries({ queryKey: getListProvidersQueryKey() });
      onAdded(provider);
    },
    onError: (error) => setProblem(problemView(error, 'add providers')),
  });

  const close = () => {
    if (add.isPending) return;
    add.reset();
    onClose();
  };

  const update = (patch: Partial<ProviderForm>) => setForm((current) => ({ ...current, ...patch }));
  const setConfig = (key: string, value: string) =>
    setForm((current) => ({ ...current, config: { ...current.config, [key]: value } }));
  const clearError = (key: string) =>
    setErrors((current) => {
      if (!(key in current)) return current;
      const { [key]: _gone, ...rest } = current;
      return rest;
    });

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setProblem(null);
    const found = providerFormProblems(form);
    setErrors(found);
    const first = Object.keys(found)[0];
    if (first) {
      document.getElementById(fieldId(first))?.focus();
      return;
    }
    add.mutate(buildCreateProviderBody(form));
  };

  const fields = PROVIDER_CONFIG_FIELDS[form.kind];
  const secretWords = PROVIDER_SECRET_WORDS[form.kind];

  return (
    <Dialog open onOpenChange={(open) => (open ? undefined : close())}>
      <DialogContent
        title="Add a provider"
        description="An outside account Quoro uses for DNS records, tunnels or backups."
      >
        <form className="flex flex-col gap-4" onSubmit={submit} noValidate>
          {problem ? <ProblemBanner view={problem} /> : null}

          <div className="flex flex-col gap-1.5">
            <span id="provider-kind" className="type-body-medium text-[13px] text-ink-2">
              Kind
            </span>
            <KindPicker
              value={form.kind}
              disabled={add.isPending}
              onChange={(kind) => {
                update({ kind });
                setErrors({});
                setProblem(null);
              }}
            />
            <p className="text-[13px] leading-snug text-ink-2">{PROVIDER_KIND_USE[form.kind]}</p>
          </div>

          <Field label="Name" htmlFor="provider-name" hint="Unique in the cluster.">
            <Input
              id="provider-name"
              value={form.name}
              maxLength={200}
              placeholder={NAME_PLACEHOLDER[form.kind]}
              autoComplete="off"
              spellCheck={false}
              disabled={add.isPending}
              onChange={(event) => {
                update({ name: event.target.value });
                clearError('name');
              }}
              aria-invalid={errors.name !== undefined}
            />
            <FieldError text={errors.name} />
          </Field>

          <div className="grid gap-4 sm:grid-cols-2">
            {fields.map((field) => {
              const key = `config.${field.key}`;
              const wide = fields.length === 1 || field.https === true;
              return (
                <Field
                  key={`${form.kind}-${field.key}`}
                  label={field.label}
                  htmlFor={fieldId(key)}
                  hint={field.hint}
                  className={wide ? 'sm:col-span-2' : undefined}
                >
                  <Input
                    id={fieldId(key)}
                    className="type-code"
                    value={form.config[field.key] ?? ''}
                    placeholder={field.placeholder}
                    inputMode={field.https ? 'url' : undefined}
                    autoComplete="off"
                    spellCheck={false}
                    disabled={add.isPending}
                    onChange={(event) => {
                      setConfig(field.key, event.target.value);
                      clearError(key);
                    }}
                    aria-invalid={errors[key] !== undefined}
                  />
                  <FieldError text={errors[key]} />
                </Field>
              );
            })}
          </div>

          <fieldset className="rounded-[10px] border border-line bg-sunk/40 px-3.5 pb-3.5 pt-2">
            <legend className="type-body-medium flex items-center gap-1.5 px-1 text-[13px] text-ink-2">
              <LockKeyhole size={13} strokeWidth={1.8} aria-hidden="true" />
              Write-only
            </legend>
            <Field label={secretWords.label} htmlFor="provider-secret" hint={secretWords.hint}>
              <Input
                id="provider-secret"
                type="password"
                className="type-code"
                value={form.secret}
                autoComplete="off"
                spellCheck={false}
                disabled={add.isPending}
                onChange={(event) => {
                  update({ secret: event.target.value });
                  clearError('secret');
                }}
                aria-invalid={errors.secret !== undefined}
              />
              <FieldError text={errors.secret} />
            </Field>
            <p className="mt-2.5 border-t border-line pt-2.5 text-[12.5px] leading-snug text-ink-2">
              Quoro encrypts it with the cluster key and never shows it again, here or anywhere. To
              change it later, replace it.
            </p>
          </fieldset>

          <div className="flex flex-wrap justify-end gap-2 border-t border-line pt-4">
            <Button variant="default" type="button" disabled={add.isPending} onClick={close}>
              Cancel
            </Button>
            <Button variant="primary" type="submit" disabled={add.isPending}>
              {add.isPending ? 'Adding…' : 'Add provider'}
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
}
