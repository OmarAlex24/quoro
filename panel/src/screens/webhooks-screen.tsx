import { useState } from 'react';
import { useListWebhooks } from '../api/generated/webhooks/webhooks';
import { canManageWebhooks } from '../lib/capabilities';
import { problemView } from '../lib/problem';
import { useSession } from '../session';
import { Button } from '../components/ui/button';
import { ProblemBanner } from '../components/ui/problem-banner';
import { WebhookBay } from '../components/webhook-bay';
import { WebhookDialog } from '../components/webhook-dialog';
import { LoginScreen } from './login-screen';

export function WebhooksScreen() {
  const { me, user, loading, unauthenticated } = useSession();
  const [dialogOpen, setDialogOpen] = useState(false);
  const writer = canManageWebhooks(me);

  const webhooksQuery = useListWebhooks(undefined, {
    query: { refetchInterval: 15_000 },
  });

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-6xl px-4 py-8 sm:px-6">
        <p className="text-[15px] text-ink-2">Loading…</p>
      </main>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  const webhooks = webhooksQuery.data?.items ?? [];
  const listError =
    webhooksQuery.error !== null && webhooksQuery.error !== undefined
      ? problemView(webhooksQuery.error, 'see webhooks')
      : null;

  return (
    <main className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 sm:py-8">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="type-display text-[24px] leading-none">Webhooks</h1>
          <p className="mt-2 max-w-2xl text-[15px] leading-snug text-ink-2">
            Send events to a receiver, and optionally wake an agent with a short grant
            for the incident.
          </p>
        </div>
        {writer ? (
          <Button variant="primary" onClick={() => setDialogOpen(true)}>
            Add a webhook
          </Button>
        ) : null}
      </div>

      {!writer ? (
        <p className="mt-3 text-[13px] text-ink-2">
          Your role can read webhooks but not change them.
        </p>
      ) : null}

      {listError ? <ProblemBanner view={listError} className="mt-4" /> : null}

      <div className="mt-5 flex flex-col gap-2">
        {webhooksQuery.isPending ? (
          <div className="h-[104px] rounded-[10px] border border-line bg-surface/60" />
        ) : webhooks.length === 0 ? (
          <div className="rounded-[10px] border border-dashed border-line bg-surface/50 px-4 py-10 text-center">
            <p className="text-[15px] text-ink">
              {writer
                ? 'No webhooks yet. Add one to send events to a receiver.'
                : 'No webhooks yet.'}
            </p>
            {writer ? (
              <Button
                variant="primary"
                className="mt-4"
                onClick={() => setDialogOpen(true)}
              >
                Add a webhook
              </Button>
            ) : null}
          </div>
        ) : (
          webhooks.map((webhook) => <WebhookBay key={webhook.id} webhook={webhook} />)
        )}
      </div>

      <WebhookDialog mode="create" open={dialogOpen} onOpenChange={setDialogOpen} />
    </main>
  );
}
