import { Plus } from 'lucide-react';
import { useState } from 'react';
import type { Webhook } from '../api/generated/model';
import { useListWebhooks } from '../api/generated/webhooks/webhooks';
import { canManageWebhooks } from '../lib/capabilities';
import { problemView, type ProblemView } from '../lib/problem';
import { useSession } from '../session';
import {
  SettingsHead,
  SettingsLoading,
  SettingsNote,
  SettingsPage,
} from '../components/settings-parts';
import { Button } from '../components/ui/button';
import { NoticeLine, ProblemBanner } from '../components/ui/problem-banner';
import {
  DeleteWebhookDialog,
  useWebhookActions,
  WebhookActionsMenu,
} from '../components/webhook-actions';
import { WebhookDialog } from '../components/webhook-dialog';
import { WebhookRow } from '../components/webhook-row';
import { LoginScreen } from './login-screen';
import { EmptyState } from '../components/empty-state';

/** Settings › Webhooks: where events go, the latest delivery of each, and the agent each wakes. */
export function WebhooksScreen() {
  const { me, user, loading, unauthenticated } = useSession();
  const [createOpen, setCreateOpen] = useState(false);
  const [editing, setEditing] = useState<Webhook | null>(null);
  const [deleting, setDeleting] = useState<Webhook | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const writer = canManageWebhooks(me);

  const webhooksQuery = useListWebhooks(undefined, { query: { refetchInterval: 15_000 } });
  const actions = useWebhookActions({
    onNotice: (words) => {
      setProblem(null);
      setNotice(words);
    },
    onProblem: (view) => {
      setNotice(null);
      setProblem(view);
    },
    onDeleted: () => setDeleting(null),
  });

  if (loading) {
    return (
      <SettingsPage label="Webhooks">
        <SettingsHead title="Webhooks" />
        <SettingsLoading label="Loading webhooks" />
      </SettingsPage>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  const webhooks = webhooksQuery.data?.items ?? [];
  const listError =
    webhooksQuery.error !== null && webhooksQuery.error !== undefined
      ? problemView(webhooksQuery.error, 'see webhooks')
      : null;

  const add = writer ? (
    <Button size="sm" onClick={() => setCreateOpen(true)}>
      <Plus size={14} strokeWidth={1.8} aria-hidden="true" />
      Add webhook
    </Button>
  ) : null;

  return (
    <SettingsPage label="Webhooks">
      <SettingsHead title="Webhooks" action={add} />

      {!writer ? <SettingsNote>Your role can read webhooks but not change them.</SettingsNote> : null}
      {notice ? <NoticeLine>{notice}</NoticeLine> : null}
      {problem ? <ProblemBanner view={problem} /> : null}
      {listError ? <ProblemBanner view={listError} /> : null}

      {webhooksQuery.isPending ? (
        <SettingsLoading label="Loading webhooks" />
      ) : webhooks.length === 0 ? (
        <EmptyState title="No webhooks yet" action={add}>
          {writer
            ? 'A webhook sends the events you pick to a receiver you run, as they happen.'
            : 'A webhook sends the events you pick to a receiver. An owner or operator can add one.'}
        </EmptyState>
      ) : (
        <ul className="box">
          {webhooks.map((webhook) => (
            <WebhookRow
              key={webhook.id}
              webhook={webhook}
              actions={
                writer ? (
                  <WebhookActionsMenu
                    webhook={webhook}
                    busy={actions.busy}
                    open
                    onEdit={() => setEditing(webhook)}
                    onToggle={() => actions.toggle.mutate(webhook)}
                    onTest={() => actions.test.mutate(webhook)}
                    onDelete={() => setDeleting(webhook)}
                  />
                ) : undefined
              }
            />
          ))}
        </ul>
      )}

      <WebhookDialog mode="create" open={createOpen} onOpenChange={setCreateOpen} />
      <WebhookDialog
        mode="edit"
        open={editing !== null}
        onOpenChange={(open) => (open ? undefined : setEditing(null))}
        webhook={editing ?? undefined}
        onSaved={() => {
          setProblem(null);
          setNotice(editing ? `Saved ${editing.name}` : 'Saved changes');
        }}
      />
      <DeleteWebhookDialog
        webhook={deleting}
        busy={actions.remove.isPending}
        onKeep={() => setDeleting(null)}
        onDelete={(webhook) => actions.remove.mutate(webhook)}
      />
    </SettingsPage>
  );
}
