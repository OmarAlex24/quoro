import { useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Archive,
  Cable,
  Globe,
  KeyRound,
  Plus,
  Split,
  Trash2,
  type LucideIcon,
} from 'lucide-react';
import { useState, type FormEvent } from 'react';
import { useListAuditExports } from '../api/generated/audit/audit';
import { useListBackupRepositories } from '../api/generated/backups/backups';
import type { Provider, ProviderKind } from '../api/generated/model';
import {
  deleteProvider,
  getListProvidersQueryKey,
  rotateProviderSecret,
  useListProviders,
} from '../api/generated/providers/providers';
import { canManageProviders } from '../lib/capabilities';
import { problemView, type ProblemView } from '../lib/problem';
import {
  configSummary,
  PROVIDER_KIND_USE,
  PROVIDER_KIND_WORDS,
  PROVIDER_KINDS,
  providerUses,
  secretLine,
  secretProblem,
  sortProviders,
} from '../lib/providers';
import { useSession } from '../session';
import {
  RowMenu,
  RowMenuItem,
  SettingsEmpty,
  SettingsHead,
  SettingsLoading,
  SettingsPage,
} from '../components/settings-parts';
import { AddProviderDialog } from '../components/add-provider-dialog';
import { ToneLamp } from '../components/tone-lamp';
import { Button } from '../components/ui/button';
import { Dialog, DialogContent } from '../components/ui/dialog';
import { Field } from '../components/ui/field';
import { Input } from '../components/ui/input';
import { NoticeLine, ProblemBanner } from '../components/ui/problem-banner';
import { LoginScreen } from './login-screen';

const KIND_ICON: Record<ProviderKind, LucideIcon> = {
  dns: Globe,
  tunnel: Cable,
  lb: Split,
  object_store: Archive,
};

/** One provider: its name, its non-secret config, whether a secret is stored, and what holds it. */
function ProviderRow({
  provider,
  uses,
  onReplace,
  onDelete,
}: {
  provider: Provider;
  uses: string[];
  onReplace: () => void;
  onDelete: () => void;
}) {
  const Icon = KIND_ICON[provider.kind];
  const { shown, more } = configSummary(provider.config);
  const secret = secretLine(provider);

  return (
    <li className="srow">
      <span className="glyph glyph-neutral" aria-hidden="true">
        <Icon strokeWidth={1.7} />
      </span>
      <div className="srow-t">
        <b>
          <span className="truncate">{provider.name}</span>
        </b>
        {shown.length > 0 ? (
          <span
            className="srow-code type-code"
            title={shown.map((entry) => `${entry.key}=${entry.text}`).join('  ')}
          >
            {shown.map((entry) => `${entry.key}=${entry.text}`).join('  ')}
            {more > 0 ? `  +${more}` : ''}
          </span>
        ) : null}
        <small>
          <ToneLamp tone={provider.has_secret ? 'run' : 'idle'} size={11} label={secret} />
          <span aria-hidden="true">{secret}</span>
          {uses.map((use) => (
            <span key={use} className="contents">
              <span className="dot-sep" aria-hidden="true" />
              <span>{use}</span>
            </span>
          ))}
        </small>
      </div>
      <RowMenu label={`Actions for ${provider.name}`}>
        <RowMenuItem icon={KeyRound} onSelect={onReplace}>
          Replace the secret…
        </RowMenuItem>
        <div className="menu-sep" role="separator" />
        <RowMenuItem icon={Trash2} danger onSelect={onDelete}>
          Delete…
        </RowMenuItem>
      </RowMenu>
    </li>
  );
}

/** The new secret, typed once and sent; it never comes back (D-0051). */
function ReplaceSecretDialog({
  provider,
  onClose,
  onReplaced,
}: {
  provider: Provider | null;
  onClose: () => void;
  onReplaced: (provider: Provider) => void;
}) {
  const queryClient = useQueryClient();
  const [secret, setSecret] = useState('');
  const [fieldError, setFieldError] = useState<string | null>(null);
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const replace = useMutation({
    mutationFn: (input: { id: string; secret: string }) =>
      rotateProviderSecret(input.id, { secret: input.secret }),
    onSuccess: (updated) => {
      setSecret('');
      setProblem(null);
      void queryClient.invalidateQueries({ queryKey: getListProvidersQueryKey() });
      onReplaced(updated);
    },
    onError: (error) => setProblem(problemView(error, 'replace provider secrets')),
  });

  const close = () => {
    if (replace.isPending) return;
    setSecret('');
    setFieldError(null);
    setProblem(null);
    onClose();
  };

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!provider) return;
    const invalid = secretProblem(secret);
    if (invalid) {
      setFieldError(invalid);
      return;
    }
    setFieldError(null);
    replace.mutate({ id: provider.id, secret });
  };

  return (
    <Dialog open={provider !== null} onOpenChange={(open) => (open ? undefined : close())}>
      {provider ? (
        <DialogContent
          title={`Replace the secret of ${provider.name}?`}
          description="The old secret stops being used as soon as this is saved. The new one is write-only: Quoro never shows it again, here or anywhere."
        >
          <form className="flex flex-col gap-4" onSubmit={submit} noValidate>
            {problem ? <ProblemBanner view={problem} /> : null}
            <Field
              label="New secret"
              htmlFor="provider-secret"
              hint={
                fieldError ? (
                  <span className="text-fault">{fieldError}</span>
                ) : (
                  'The API token, tunnel token or access key secret.'
                )
              }
            >
              <Input
                id="provider-secret"
                type="password"
                className="type-code"
                value={secret}
                onChange={(event) => {
                  setSecret(event.target.value);
                  setFieldError(null);
                }}
                autoComplete="off"
                spellCheck={false}
                aria-invalid={fieldError ? true : undefined}
              />
            </Field>
            <div className="flex flex-wrap justify-end gap-2 border-t border-line pt-4">
              <Button variant="default" type="button" disabled={replace.isPending} onClick={close}>
                Keep the current secret
              </Button>
              <Button busy={replace.isPending} variant="primary" type="submit" disabled={replace.isPending}>
                {replace.isPending ? 'Replacing…' : 'Replace the secret'}
              </Button>
            </div>
          </form>
        </DialogContent>
      ) : null}
    </Dialog>
  );
}

/**
 * Settings › Providers: the credentials Quoro holds for DNS, tunnels, load
 * balancers and object storage, grouped by kind. Managing them is human-only
 * and needs `provider:manage` (owner or operator): add one, replace its
 * secret, delete it. A secret is write-only.
 */
export function ProvidersScreen() {
  const { me, user, loading, unauthenticated } = useSession();
  const queryClient = useQueryClient();
  const manager = canManageProviders(me);
  const [replacing, setReplacing] = useState<Provider | null>(null);
  const [deleting, setDeleting] = useState<Provider | null>(null);
  const [adding, setAdding] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const enabled = user !== undefined && manager;
  const providers = useListProviders({ limit: 200 }, { query: { enabled, refetchInterval: 30_000 } });
  const repositories = useListBackupRepositories({ query: { enabled, staleTime: 30_000 } });
  const auditExports = useListAuditExports({ query: { enabled, staleTime: 30_000 } });

  const remove = useMutation({
    mutationFn: (provider: Provider) => deleteProvider(provider.id),
    onSuccess: (_data, provider) => {
      setProblem(null);
      setDeleting(null);
      setNotice(`Deleted ${provider.name} and its secret.`);
      void queryClient.invalidateQueries({ queryKey: getListProvidersQueryKey() });
    },
    onError: (error) => setProblem(problemView(error, 'delete providers')),
  });

  if (loading) {
    return (
      <SettingsPage label="Providers">
        <SettingsHead title="Providers" />
        <SettingsLoading label="Loading providers" />
      </SettingsPage>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  const add = manager ? (
    <Button
      size="sm"
      onClick={() => {
        setNotice(null);
        setProblem(null);
        setAdding(true);
      }}
    >
      <Plus size={14} strokeWidth={1.8} aria-hidden="true" />
      Add provider
    </Button>
  ) : null;

  const head = (
    <SettingsHead
      title="Providers"
      lede="The accounts Quoro uses outside the cluster: DNS, tunnels, load balancers and object storage. A secret is write-only; the panel never sees one."
      action={add}
    />
  );

  if (!manager) {
    return (
      <SettingsPage label="Providers">
        {head}
        <SettingsEmpty title="Owners and operators manage providers.">
          Your role cannot read them: a provider holds the credentials of an outside account.
        </SettingsEmpty>
      </SettingsPage>
    );
  }

  const items = sortProviders(providers.data?.items ?? []);
  const repositoryItems = repositories.data?.items ?? [];
  const exportItems = auditExports.data?.items ?? [];
  const listError =
    providers.error !== null && providers.error !== undefined
      ? problemView(providers.error, 'see providers')
      : null;
  const deletingUses = deleting ? providerUses(deleting.id, repositoryItems, exportItems) : [];

  return (
    <SettingsPage label="Providers">
      {head}
      {notice ? <NoticeLine>{notice}</NoticeLine> : null}
      {problem && !deleting ? <ProblemBanner view={problem} /> : null}
      {listError ? <ProblemBanner view={listError} /> : null}

      {providers.isPending ? (
        <SettingsLoading label="Loading providers" />
      ) : items.length === 0 ? (
        <SettingsEmpty title="No providers yet." action={add}>
          Add one to give the cluster a DNS zone, a tunnel or a bucket.
        </SettingsEmpty>
      ) : (
        PROVIDER_KINDS.filter((kind) => items.some((item) => item.kind === kind)).map((kind) => (
          <section key={kind} className="grid gap-2.5" aria-labelledby={`providers-${kind}`}>
            <div>
              <h3 id={`providers-${kind}`} className="set-sub">
                {PROVIDER_KIND_WORDS[kind]}
              </h3>
              <p className="set-sub-x">{PROVIDER_KIND_USE[kind]}</p>
            </div>
            <ul className="box">
              {items
                .filter((item) => item.kind === kind)
                .map((provider) => (
                  <ProviderRow
                    key={provider.id}
                    provider={provider}
                    uses={providerUses(provider.id, repositoryItems, exportItems)}
                    onReplace={() => {
                      setNotice(null);
                      setProblem(null);
                      setReplacing(provider);
                    }}
                    onDelete={() => {
                      setNotice(null);
                      setProblem(null);
                      remove.reset();
                      setDeleting(provider);
                    }}
                  />
                ))}
            </ul>
          </section>
        ))
      )}

      {adding ? (
        <AddProviderDialog
          onClose={() => setAdding(false)}
          onAdded={(provider) => {
            setAdding(false);
            setNotice(`Added ${provider.name}. Its secret is stored encrypted and never shown again.`);
          }}
        />
      ) : null}

      <ReplaceSecretDialog
        provider={replacing}
        onClose={() => setReplacing(null)}
        onReplaced={(provider) => {
          setReplacing(null);
          setNotice(`Replaced the secret of ${provider.name}. The old one is no longer used.`);
        }}
      />

      <Dialog
        open={deleting !== null}
        onOpenChange={(open) => {
          if (open || remove.isPending) return;
          setDeleting(null);
          setProblem(null);
        }}
      >
        {deleting ? (
          <DialogContent
            title={`Delete ${deleting.name}?`}
            description="Deletes the entry and its stored secret. A provider that a domain, a node's tunnel or the off-site audit copy still uses cannot be deleted."
          >
            <div className="flex flex-col gap-4">
              {problem ? <ProblemBanner view={problem} /> : null}
              {deletingUses.length > 0 ? (
                <ul className="grid gap-1 text-[13.5px] leading-snug text-ink">
                  {deletingUses.map((use) => (
                    <li key={use}>{use}, so Quoro will refuse.</li>
                  ))}
                </ul>
              ) : null}
              <div className="flex flex-wrap justify-end gap-2 border-t border-line pt-4">
                <Button
                  variant="default"
                  disabled={remove.isPending}
                  onClick={() => {
                    setDeleting(null);
                    setProblem(null);
                  }}
                >
                  Keep the provider
                </Button>
                <Button
                  busy={remove.isPending}
                  variant="danger-solid"
                  disabled={remove.isPending}
                  onClick={() => remove.mutate(deleting)}
                >
                  {remove.isPending ? 'Deleting…' : 'Delete provider'}
                </Button>
              </div>
            </div>
          </DialogContent>
        ) : null}
      </Dialog>
    </SettingsPage>
  );
}
