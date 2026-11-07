import { Link, useNavigate } from '@tanstack/react-router';
import { ChevronLeft, FlaskConical, Send } from 'lucide-react';
import { useState } from 'react';
import { useGetWebhook } from '../api/generated/webhooks/webhooks';
import { canManageWebhooks } from '../lib/capabilities';
import { problemView, type ProblemView } from '../lib/problem';
import { useSession } from '../session';
import { SettingsLoading, SettingsNote, SettingsPage } from '../components/settings-parts';
import { Button } from '../components/ui/button';
import { NoticeLine, ProblemBanner } from '../components/ui/problem-banner';
import { Tag } from '../components/ui/tag';
import {
  DeleteWebhookDialog,
  useWebhookActions,
  WebhookActionsMenu,
} from '../components/webhook-actions';
import { WebhookDeliveries } from '../components/webhook-deliveries';
import { WebhookDialog } from '../components/webhook-dialog';
import { WebhookPlate } from '../components/webhook-plate';
import { LoginScreen } from './login-screen';

/** Settings › Webhooks › one webhook: every field, then its deliveries, newest first. */
export function WebhookScreen({ webhookId }: { webhookId: string }) {
  const { me, user, loading, unauthenticated } = useSession();
  const navigate = useNavigate();
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [editOpen, setEditOpen] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);
  const writer = canManageWebhooks(me);

  const webhookQuery = useGetWebhook(webhookId, { query: { retry: false } });
  const webhook = webhookQuery.data;

  const actions = useWebhookActions({
    onNotice: (words) => {
      setProblem(null);
      setNotice(words);
    },
    onProblem: (view) => {
      setNotice(null);
      setProblem(view);
    },
    onDeleted: () => {
      setDeleteOpen(false);
      void navigate({ to: '/settings/webhooks' });
    },
  });

  const back = (
    <Link to="/settings/webhooks" className="set-back">
      <ChevronLeft strokeWidth={1.8} aria-hidden="true" />
      All webhooks
    </Link>
  );

  if (loading) {
    return (
      <SettingsPage label="Webhook">
        {back}
        <SettingsLoading label="Loading the webhook" />
      </SettingsPage>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  const loadError =
    webhookQuery.error !== null && webhookQuery.error !== undefined
      ? problemView(webhookQuery.error, 'see this webhook')
      : null;

  return (
    <SettingsPage label={webhook?.name ?? 'Webhook'}>
      {back}
      {loadError ? <ProblemBanner view={loadError} /> : null}

      {webhookQuery.isPending ? (
        <SettingsLoading label="Loading the webhook" />
      ) : webhook ? (
        <>
          <div className="set-page-h set-page-h-thing">
            <span className="glyph glyph-neutral" aria-hidden="true">
              <Send strokeWidth={1.7} />
            </span>
            <div className="min-w-0 flex-1 max-md:basis-[calc(100%-40px)]">
              <h2>
                <span className="truncate">{webhook.name}</span>
                {webhook.enabled ? null : <Tag tone="plain">Paused</Tag>}
              </h2>
              <p className="set-page-code type-code" title={webhook.url}>
                {webhook.url}
              </p>
            </div>
            {writer ? (
              <div className="flex flex-none items-center gap-1.5 max-md:ml-10">
                <Button
                  size="sm"
                  disabled={actions.busy}
                  onClick={() => actions.test.mutate(webhook)}
                >
                  <FlaskConical size={14} strokeWidth={1.8} aria-hidden="true" />
                  {actions.test.isPending ? 'Sending…' : 'Send a test delivery'}
                </Button>
                <WebhookActionsMenu
                  webhook={webhook}
                  busy={actions.busy}
                  onEdit={() => setEditOpen(true)}
                  onToggle={() => actions.toggle.mutate(webhook)}
                  onTest={() => actions.test.mutate(webhook)}
                  onDelete={() => setDeleteOpen(true)}
                />
              </div>
            ) : null}
          </div>

          {!writer ? (
            <SettingsNote>Your role can read this webhook but not change it.</SettingsNote>
          ) : null}
          {notice ? <NoticeLine>{notice}</NoticeLine> : null}
          {problem ? <ProblemBanner view={problem} /> : null}

          <WebhookPlate webhook={webhook} />
          <WebhookDeliveries webhookId={webhook.id} />

          <WebhookDialog
            mode="edit"
            open={editOpen}
            onOpenChange={setEditOpen}
            webhook={webhook}
            onSaved={() => {
              setProblem(null);
              setNotice('Saved changes');
            }}
          />
          <DeleteWebhookDialog
            webhook={deleteOpen ? webhook : null}
            busy={actions.remove.isPending}
            onKeep={() => setDeleteOpen(false)}
            onDelete={(target) => actions.remove.mutate(target)}
          />
        </>
      ) : null}
    </SettingsPage>
  );
}
