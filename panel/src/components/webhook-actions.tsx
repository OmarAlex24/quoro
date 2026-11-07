import { useMutation, useQueryClient } from '@tanstack/react-query';
import { ExternalLink, FlaskConical, Pause, Pencil, Play, Trash2 } from 'lucide-react';
import { useNavigate } from '@tanstack/react-router';
import type { Webhook } from '../api/generated/model';
import {
  deleteWebhook,
  getGetWebhookQueryKey,
  getListWebhookDeliveriesQueryKey,
  getListWebhooksQueryKey,
  testWebhook,
  updateWebhook,
} from '../api/generated/webhooks/webhooks';
import { problemView, type ProblemView } from '../lib/problem';
import { RowMenu, RowMenuItem } from './settings-parts';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';

/*
 * What can be done to a webhook, the same from its row in the list and from
 * its own page: pause or resume deliveries, send a test delivery, edit (the
 * existing dialog) and delete after a confirmation.
 */

export function useWebhookActions({
  onNotice,
  onProblem,
  onDeleted,
}: {
  onNotice: (notice: string) => void;
  onProblem: (problem: ProblemView) => void;
  onDeleted?: () => void;
}) {
  const queryClient = useQueryClient();
  const refresh = (id: string) => {
    void queryClient.invalidateQueries({ queryKey: getGetWebhookQueryKey(id) });
    void queryClient.invalidateQueries({ queryKey: getListWebhooksQueryKey() });
  };

  const toggle = useMutation({
    mutationFn: (webhook: Webhook) => updateWebhook(webhook.id, { enabled: !webhook.enabled }),
    onSuccess: (_updated, webhook) => {
      onNotice(webhook.enabled ? `Paused deliveries to ${webhook.name}` : `Resumed deliveries to ${webhook.name}`);
      refresh(webhook.id);
    },
    onError: (error) => onProblem(problemView(error, 'change this webhook')),
  });

  const test = useMutation({
    mutationFn: (webhook: Webhook) => testWebhook(webhook.id),
    onSuccess: (_delivery, webhook) => {
      onNotice(`Sent a test delivery to ${webhook.name}; it shows in its deliveries.`);
      void queryClient.invalidateQueries({ queryKey: getListWebhookDeliveriesQueryKey(webhook.id) });
    },
    onError: (error) => onProblem(problemView(error, 'send a test delivery')),
  });

  const remove = useMutation({
    mutationFn: (webhook: Webhook) => deleteWebhook(webhook.id),
    onSuccess: (_data, webhook) => {
      onNotice(`Deleted ${webhook.name}`);
      void queryClient.invalidateQueries({ queryKey: getListWebhooksQueryKey() });
      onDeleted?.();
    },
    onError: (error) => onProblem(problemView(error, 'delete this webhook')),
  });

  return { toggle, test, remove, busy: toggle.isPending || test.isPending || remove.isPending };
}

/** The ⋯ menu of a webhook. `open` is offered from the list, where the row is not the page. */
export function WebhookActionsMenu({
  webhook,
  busy,
  open = false,
  onEdit,
  onToggle,
  onTest,
  onDelete,
}: {
  webhook: Webhook;
  busy: boolean;
  open?: boolean;
  onEdit: () => void;
  onToggle: () => void;
  onTest: () => void;
  onDelete: () => void;
}) {
  const navigate = useNavigate();
  return (
    <RowMenu label={`Actions for ${webhook.name}`} disabled={busy}>
      {open ? (
        <RowMenuItem
          icon={ExternalLink}
          onSelect={() =>
            void navigate({ to: '/settings/webhooks/$webhookId', params: { webhookId: webhook.id } })
          }
        >
          Open deliveries
        </RowMenuItem>
      ) : null}
      <RowMenuItem icon={Pencil} onSelect={onEdit}>
        Edit…
      </RowMenuItem>
      <RowMenuItem icon={webhook.enabled ? Pause : Play} onSelect={onToggle}>
        {webhook.enabled ? 'Pause deliveries' : 'Resume deliveries'}
      </RowMenuItem>
      <RowMenuItem icon={FlaskConical} onSelect={onTest}>
        Send a test delivery
      </RowMenuItem>
      <div className="menu-sep" role="separator" />
      <RowMenuItem icon={Trash2} danger onSelect={onDelete}>
        Delete…
      </RowMenuItem>
    </RowMenu>
  );
}

/** The question before a webhook is deleted: its history stays, new events stop. */
export function DeleteWebhookDialog({
  webhook,
  busy,
  onKeep,
  onDelete,
}: {
  webhook: Webhook | null;
  busy: boolean;
  onKeep: () => void;
  onDelete: (webhook: Webhook) => void;
}) {
  return (
    <Dialog open={webhook !== null} onOpenChange={(open) => (open || busy ? undefined : onKeep())}>
      {webhook ? (
        <DialogContent
          title={`Delete ${webhook.name}?`}
          description="Deliveries already attempted stay in the history. New matching events will not reach the receiver."
        >
          <div className="flex flex-wrap justify-end gap-2 border-t border-line pt-4">
            <Button variant="default" disabled={busy} onClick={onKeep}>
              Keep the webhook
            </Button>
            <Button variant="danger-solid" disabled={busy} onClick={() => onDelete(webhook)}>
              {busy ? 'Deleting…' : 'Delete webhook'}
            </Button>
          </div>
        </DialogContent>
      ) : null}
    </Dialog>
  );
}
