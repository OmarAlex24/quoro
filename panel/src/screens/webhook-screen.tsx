import { useMutation, useQueryClient } from '@tanstack/react-query';
import { getRouteApi, Link, useNavigate } from '@tanstack/react-router';
import { useState } from 'react';
import {
  deleteWebhook,
  getGetWebhookQueryKey,
  getListWebhookDeliveriesQueryKey,
  getListWebhooksQueryKey,
  testWebhook,
  updateWebhook,
  useGetWebhook,
} from '../api/generated/webhooks/webhooks';
import { canManageWebhooks } from '../lib/capabilities';
import { problemView, type ProblemView } from '../lib/problem';
import { useSession } from '../session';
import { Button } from '../components/ui/button';
import { Dialog, DialogContent } from '../components/ui/dialog';
import { NoticeLine, ProblemBanner } from '../components/ui/problem-banner';
import { WebhookDeliveries } from '../components/webhook-deliveries';
import { WebhookDialog } from '../components/webhook-dialog';
import { WebhookPlate } from '../components/webhook-plate';
import { LoginScreen } from './login-screen';

export function WebhookScreen() {
  const { webhookId } = getRouteApi('/webhooks/$webhookId').useParams();
  const { me, user, loading, unauthenticated } = useSession();
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [editOpen, setEditOpen] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);
  const writer = canManageWebhooks(me);

  const webhookQuery = useGetWebhook(webhookId, { query: { retry: false } });
  const webhook = webhookQuery.data;

  const toggleEnabled = useMutation({
    mutationFn: (webhook: { id: string; enabled: boolean }) =>
      updateWebhook(webhook.id, { enabled: !webhook.enabled }),
    onSuccess: (_updated, variables) => {
      setProblem(null);
      setNotice(
        variables.enabled ? 'Paused deliveries' : 'Resumed deliveries',
      );
      void queryClient.invalidateQueries({
        queryKey: getGetWebhookQueryKey(webhookId),
      });
      void queryClient.invalidateQueries({ queryKey: getListWebhooksQueryKey() });
    },
    onError: (error) =>
      setProblem(
        problemView(error, 'change this webhook'),
      ),
  });

  const sendTest = useMutation({
    mutationFn: () => testWebhook(webhookId),
    onSuccess: () => {
      setProblem(null);
      setNotice('Sent a test delivery; watch it below.');
      void queryClient.invalidateQueries({
        queryKey: getListWebhookDeliveriesQueryKey(webhookId),
      });
    },
    onError: (error) => setProblem(problemView(error, 'send a test delivery')),
  });

  const remove = useMutation({
    mutationFn: () => deleteWebhook(webhookId),
    onSuccess: () => {
      setProblem(null);
      void queryClient.invalidateQueries({ queryKey: getListWebhooksQueryKey() });
      setDeleteOpen(false);
      void navigate({ to: '/webhooks' });
    },
    onError: (error) => setProblem(problemView(error, 'delete this webhook')),
  });

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-6xl px-4 py-8 sm:px-6">
        <p className="text-[15px] text-ink-2">Loading…</p>
      </main>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  const loadError =
    webhookQuery.error !== null && webhookQuery.error !== undefined
      ? problemView(webhookQuery.error, 'see this webhook')
      : null;

  return (
    <main className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 sm:py-8">
      <Link
        to="/webhooks"
        className="text-[13px] text-ink-2 underline-offset-4 hover:text-ink hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
      >
        All webhooks
      </Link>

      {loadError ? <ProblemBanner view={loadError} className="mt-4" /> : null}

      {webhookQuery.isPending ? (
        <div className="mt-5 h-[240px] rounded-[10px] border border-line bg-surface/60" />
      ) : webhook ? (
        <>
          <div className="mt-3 flex flex-wrap items-start justify-between gap-3">
            <div className="min-w-0">
              <h1 className="type-display text-[24px] leading-none">{webhook.name}</h1>
              <p className="mt-2 text-[13px] text-ink-2">
                {webhook.enabled
                  ? 'Deliveries are attempted for matching events.'
                  : 'Paused: no deliveries are attempted.'}
              </p>
            </div>

            {writer ? (
              <div className="flex flex-wrap gap-2">
                <Button variant="default" onClick={() => setEditOpen(true)}>
                  Edit
                </Button>
                <Button
                  variant="default"
                  disabled={toggleEnabled.isPending}
                  onClick={() => {
                    setNotice(null);
                    toggleEnabled.mutate({ id: webhook.id, enabled: webhook.enabled });
                  }}
                >
                  {webhook.enabled ? 'Pause deliveries' : 'Resume deliveries'}
                </Button>
                <Button
                  variant="default"
                  disabled={sendTest.isPending}
                  onClick={() => {
                    setNotice(null);
                    sendTest.mutate();
                  }}
                >
                  {sendTest.isPending ? 'Sending…' : 'Send a test delivery'}
                </Button>
                <Button variant="danger" onClick={() => setDeleteOpen(true)}>
                  Delete webhook
                </Button>
              </div>
            ) : (
              <p className="text-[13px] text-ink-2">
                Your role can read this webhook but not change it.
              </p>
            )}
          </div>

          {notice ? (
            <div className="mt-3">
              <NoticeLine>{notice}</NoticeLine>
            </div>
          ) : null}

          {problem ? <ProblemBanner view={problem} className="mt-3" /> : null}

          <div className="mt-5">
            <WebhookPlate webhook={webhook} />
          </div>

          <div className="mt-7">
            <WebhookDeliveries webhookId={webhook.id} />
          </div>

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

          <Dialog open={deleteOpen} onOpenChange={setDeleteOpen}>
            <DialogContent
              title="Delete webhook"
              description="Deliveries already attempted stay in the history. New matching events will not reach the receiver."
            >
              <div className="flex flex-wrap justify-end gap-2 border-t border-line pt-4">
                <Button variant="default" onClick={() => setDeleteOpen(false)}>
                  Cancel
                </Button>
                <Button
                  variant="danger-solid"
                  disabled={remove.isPending}
                  onClick={() => remove.mutate()}
                >
                  {remove.isPending ? 'Deleting…' : 'Delete webhook'}
                </Button>
              </div>
            </DialogContent>
          </Dialog>
        </>
      ) : null}
    </main>
  );
}
