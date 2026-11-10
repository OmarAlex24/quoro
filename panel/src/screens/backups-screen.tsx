import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Archive, KeyRound, Plus, Settings2 } from 'lucide-react';
import { useState } from 'react';
import type { Backup, BackupRepository } from '../api/generated/model';
import {
  getListBackupRepositoriesQueryKey,
  rotateBackupRecoveryKey,
  useListBackupRepositories,
  useListBackups,
} from '../api/generated/backups/backups';
import { useListAuditExports } from '../api/generated/audit/audit';
import { useListProviders } from '../api/generated/providers/providers';
import { relativeTime } from '../lib/app-status';
import { canManageBackups, canManageProviders } from '../lib/capabilities';
import { formatBytes } from '../lib/ha-cost';
import { problemView, type ProblemView } from '../lib/problem';
import { PROVIDER_KIND_WORDS } from '../lib/providers';
import { backupLine, repeatWords } from '../lib/storage-words';
import { useSession } from '../session';
import {
  CreateRepositoryDialog,
  RepositorySettingsDialog,
} from '../components/backup-repository-dialogs';
import { UserName } from '../components/identity-name';
import { SecretOnce } from '../components/secret-once';
import {
  SettingsHead,
  SettingsLoading,
  SettingsNote,
  SettingsPage,
} from '../components/settings-parts';
import { ToneLamp } from '../components/tone-lamp';
import { Button } from '../components/ui/button';
import { Dialog, DialogContent } from '../components/ui/dialog';
import { NoticeLine, ProblemBanner } from '../components/ui/problem-banner';
import { LoginScreen } from './login-screen';
import { EmptyState } from '../components/empty-state';

const RECOVERY_NOTE =
  'Copy it now and keep it away from this cluster, in a password manager or on paper. Quoro shows it only once. With it, the backups can be opened even if every node is lost; the old key no longer opens them.';

/** How many control-plane backups the page lists, newest first. */
const CONTROL_PLANE_SHOWN = 5;

/** One control-plane backup: the lamp, the word and one line of what happened. */
function ControlPlaneRow({ backup }: { backup: Backup }) {
  const line = backupLine(backup);
  return (
    <li className="srow srow-tight">
      <ToneLamp tone={line.tone} pulse={line.pulse} size={11} label={line.words} className="srow-lamp" />
      <div className="srow-t">
        <b>{line.words}</b>
        <small>{line.detail}</small>
      </div>
      {backup.logical_bytes !== null && backup.logical_bytes !== undefined ? (
        <span className="srow-end" title="Size of the database copy">
          {formatBytes(backup.logical_bytes)}
        </span>
      ) : null}
    </li>
  );
}

/** One backup repository: where it lives, the key that opens it, and the control-plane schedule. */
function RepositoryBox({
  repository,
  providerName,
  controlPlane,
  owner,
  onEdit,
  onReplaceKey,
}: {
  repository: BackupRepository;
  providerName: string | null;
  controlPlane: Backup[];
  owner: boolean;
  onEdit: () => void;
  onReplaceKey: () => void;
}) {
  return (
    <section className="box" aria-label={`Backup repository ${repository.name}`}>
      <div className="box-h box-h-wrap">
        <span className="glyph glyph-neutral" aria-hidden="true">
          <Archive strokeWidth={1.7} />
        </span>
        <div className="box-t">
          <b>{repository.name}</b>
          <small>
            {providerName ? `On ${providerName}` : 'On an object storage provider'}
            <span className="dot-sep" aria-hidden="true" />
            Created {relativeTime(repository.created_at)} by <UserName id={repository.created_by} />
          </small>
        </div>
        {owner ? (
          <div className="flex flex-wrap gap-2">
            <Button size="sm" onClick={onEdit}>
              <Settings2 size={14} strokeWidth={1.8} aria-hidden="true" />
              Edit settings…
            </Button>
            <Button size="sm" onClick={onReplaceKey}>
              <KeyRound size={14} strokeWidth={1.8} aria-hidden="true" />
              Replace the recovery key…
            </Button>
          </div>
        ) : null}
      </div>
      <dl className="set-rows border-t border-line">
        <div className="set-row">
          <dt>Prefix</dt>
          <dd>
            <span className="type-code">{repository.prefix}</span>
          </dd>
        </div>
        <div className="set-row">
          <dt>Recovery key</dt>
          <dd className="grid gap-0.5">
            <span>
              Fingerprint <span className="type-code">{repository.recovery_key_fingerprint}</span>
            </span>
            <span className="text-ink-2">
              {repository.recovery_key_rotated_at
                ? `Replaced ${relativeTime(repository.recovery_key_rotated_at)}`
                : 'Never replaced: the key shown when the repository was created'}
            </span>
          </dd>
        </div>
        <div className="set-row">
          <dt>Control plane</dt>
          <dd className="grid gap-0.5">
            <span>{repeatWords(repository.control_plane_schedule, controlPlane)}</span>
            <span className="text-ink-2">
              The cluster database, copied on the primary on this schedule and before every upgrade.
            </span>
          </dd>
        </div>
      </dl>
    </section>
  );
}

/**
 * Settings › Backups: the cluster's backup repository (at most one in M5a),
 * which an owner creates, renames and reschedules, the recovery key an owner
 * can replace, and the control-plane database's own backups. App volume
 * backups stay on each app's Storage tab.
 */
export function BackupsScreen() {
  const { me, user, loading, unauthenticated } = useSession();
  const queryClient = useQueryClient();
  const owner = canManageBackups(me);
  const [target, setTarget] = useState<BackupRepository | null>(null);
  const [creating, setCreating] = useState(false);
  const [editing, setEditing] = useState<BackupRepository | null>(null);
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const repositories = useListBackupRepositories({ query: { enabled: user !== undefined } });
  const controlPlane = useListBackups(
    { source_kind: 'control_plane', limit: 50 },
    { query: { enabled: user !== undefined, refetchInterval: 30_000 } },
  );
  // Provider names need provider:manage; other roles see the kind only.
  const providers = useListProviders(
    { limit: 200 },
    { query: { enabled: user !== undefined && canManageProviders(me), staleTime: 30_000 } },
  );
  // A new repository cannot share the audit copy's provider; only an owner creates one.
  const auditExports = useListAuditExports({
    query: { enabled: user !== undefined && owner, staleTime: 30_000 },
  });

  const rotate = useMutation({
    mutationFn: (repository: BackupRepository) => rotateBackupRecoveryKey(repository.id),
    // The answer holds the recovery key: keep the mutation no longer than the dialog.
    gcTime: 0,
    onSuccess: () => {
      setProblem(null);
      void queryClient.invalidateQueries({ queryKey: getListBackupRepositoriesQueryKey() });
    },
    onError: (error) => setProblem(problemView(error, 'replace the recovery key')),
  });

  const close = () => {
    if (rotate.isPending) return;
    if (rotate.data) setNotice(`Replaced the recovery key of ${rotate.data.name}.`);
    // Drop the key with the dialog: it lives in the mutation result only.
    rotate.reset();
    setProblem(null);
    setTarget(null);
  };

  if (loading) {
    return (
      <SettingsPage label="Backups">
        <SettingsHead title="Backups" />
        <SettingsLoading label="Loading backups" />
      </SettingsPage>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  const items = repositories.data?.items ?? [];
  const cpBackups = controlPlane.data?.items ?? [];
  const listError =
    repositories.error !== null && repositories.error !== undefined
      ? problemView(repositories.error, 'see backup repositories')
      : null;
  const providerName = (id: string): string | null => {
    const provider = providers.data?.items.find((item) => item.id === id);
    return provider ? `${provider.name} (${PROVIDER_KIND_WORDS[provider.kind].toLowerCase()})` : null;
  };
  const created = rotate.data;
  const create =
    owner && repositories.isSuccess && items.length === 0 ? (
      <Button
        size="sm"
        onClick={() => {
          setNotice(null);
          setCreating(true);
        }}
      >
        <Plus size={14} strokeWidth={1.8} aria-hidden="true" />
        Create repository
      </Button>
    ) : null;

  return (
    <SettingsPage label="Backups">
      <SettingsHead
        title="Backups"
        lede="Where backups are kept, and the key that opens them without this cluster. Each app's volume backups are on its Storage tab."
        action={create}
      />
      {!owner ? (
        <SettingsNote>
          Only an owner can create the repository, change its settings or replace its recovery key.
        </SettingsNote>
      ) : null}
      {notice ? <NoticeLine>{notice}</NoticeLine> : null}
      {listError ? <ProblemBanner view={listError} /> : null}

      {repositories.isPending ? (
        <SettingsLoading label="Loading backup repositories" />
      ) : items.length === 0 ? (
        <EmptyState title="No backup repository yet" action={create}>
          {owner
            ? 'Create one in the bucket of an object storage provider. Until then, nothing is backed up.'
            : 'An owner creates one in the bucket of an object storage provider. Until then, nothing is backed up.'}
        </EmptyState>
      ) : (
        items.map((repository) => (
          <RepositoryBox
            key={repository.id}
            repository={repository}
            providerName={providerName(repository.provider_id)}
            controlPlane={cpBackups.filter((backup) => backup.repository_id === repository.id)}
            owner={owner}
            onEdit={() => {
              setNotice(null);
              setEditing(repository);
            }}
            onReplaceKey={() => {
              setNotice(null);
              setProblem(null);
              rotate.reset();
              setTarget(repository);
            }}
          />
        ))
      )}

      {items.length > 0 ? (
        <section className="grid gap-2.5" aria-labelledby="control-plane-backups">
          <h3 id="control-plane-backups" className="set-sub">
            Control-plane backups
          </h3>
          {controlPlane.isPending ? (
            <SettingsLoading label="Loading control-plane backups" />
          ) : cpBackups.length === 0 ? (
            <EmptyState title="No control-plane backup yet">
              A backup keeps the cluster&rsquo;s own state in the repository. The first one runs on the schedule above.
            </EmptyState>
          ) : (
            <ul className="box">
              {cpBackups.slice(0, CONTROL_PLANE_SHOWN).map((backup) => (
                <ControlPlaneRow key={backup.id} backup={backup} />
              ))}
            </ul>
          )}
        </section>
      ) : null}

      {creating ? (
        <CreateRepositoryDialog
          providers={providers.data?.items ?? []}
          auditExports={auditExports.data?.items ?? []}
          loading={providers.isPending || auditExports.isPending}
          onClose={() => setCreating(false)}
          onCreated={(repository) =>
            setNotice(
              `Created the backup repository ${repository.name}. Control-plane backups run on its schedule.`,
            )
          }
        />
      ) : null}

      {editing ? (
        <RepositorySettingsDialog
          repository={editing}
          onClose={() => setEditing(null)}
          onSaved={(repository) => {
            setEditing(null);
            setNotice(`Saved the settings of ${repository.name}.`);
          }}
        />
      ) : null}

      <Dialog open={target !== null} onOpenChange={(open) => (open ? undefined : close())}>
        {target ? (
          <DialogContent
            title={created ? 'New recovery key' : 'Replace the recovery key?'}
            description={
              created
                ? `For ${created.name}. Fingerprint ${created.recovery_key_fingerprint}.`
                : `Quoro makes a new key for ${target.name} and shows it once. Every backup stays as it is; the current key stops opening the repository.`
            }
          >
            <div className="flex flex-col gap-4">
              {problem ? <ProblemBanner view={problem} /> : null}
              {created ? (
                <SecretOnce label="Recovery key" secret={created.recovery_key} note={RECOVERY_NOTE} />
              ) : (
                <p className="text-[13.5px] leading-snug text-ink">
                  Use it when the key is lost, or when it may have leaked. Have somewhere safe to keep
                  the new one before you go on.
                </p>
              )}
              <div className="flex flex-wrap justify-end gap-2 border-t border-line pt-4">
                {created ? (
                  <Button variant="primary" onClick={close}>
                    I have kept it
                  </Button>
                ) : (
                  <>
                    <Button variant="default" disabled={rotate.isPending} onClick={close}>
                      Keep the current key
                    </Button>
                    <Button
                      busy={rotate.isPending}
                      variant="primary"
                      disabled={rotate.isPending}
                      onClick={() => rotate.mutate(target)}
                    >
                      {rotate.isPending ? 'Replacing…' : 'Replace the key'}
                    </Button>
                  </>
                )}
              </div>
            </div>
          </DialogContent>
        ) : null}
      </Dialog>
    </SettingsPage>
  );
}
