import { useListWebhookDeliveries } from '../api/generated/webhooks/webhooks';
import { expiryWords, shortId } from '../lib/agency-format';
import { relativeTime } from '../lib/app-status';
import { deliveryLamp, deliveryPollingInterval } from '../lib/webhooks';
import { CopyButton } from './copy-button';
import { ToneLamp } from './tone-lamp';

/** The delivery history of one webhook, newest first. */
export function WebhookDeliveries({ webhookId }: { webhookId: string }) {
  const deliveries = useListWebhookDeliveries(webhookId, undefined, {
    query: {
      refetchInterval: (query) =>
        deliveryPollingInterval(query.state.data?.items ?? []),
    },
  });
  const items = deliveries.data?.items ?? [];

  return (
    <section>
      <h2 className="type-display-light text-[15px] leading-none text-ink">Deliveries</h2>

      {items.length === 0 ? (
        <p className="mt-3 border-y border-rail py-3 text-[13px] text-ink-muted">
          No deliveries yet. Send a test delivery to check the receiver.
        </p>
      ) : (
        <ul className="mt-3 flex flex-col divide-y divide-rail border-y border-rail">
          {items.map((delivery) => {
            const lamp = deliveryLamp(delivery.status);
            return (
              <li
                key={delivery.id}
                className="grid grid-cols-[22px_minmax(0,1fr)] gap-x-3 gap-y-1 py-3"
              >
                <ToneLamp
                  tone={lamp.tone}
                  pulse={lamp.pulse}
                  size={20}
                  label={lamp.word}
                  className="mt-0.5"
                />

                <div className="min-w-0">
                  <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
                    <span className="type-body-medium text-[14px] text-ink">{lamp.word}</span>
                    <span className="type-data text-[13px] text-ink-muted">
                      Attempt {delivery.attempt}
                    </span>
                    <span className="type-data text-[13px] text-ink-muted">
                      {delivery.response_code === null ||
                      delivery.response_code === undefined
                        ? 'No response'
                        : `HTTP ${delivery.response_code}`}
                    </span>
                    <span className="type-data text-[13px] text-ink-muted">
                      {relativeTime(delivery.created_at)}
                    </span>
                  </div>

                  {delivery.error ? (
                    <p className="mt-1 text-[13px] leading-snug text-ink">
                      {delivery.error}
                    </p>
                  ) : null}

                  <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-[13px] text-ink-muted">
                    <span className="inline-flex min-w-0 items-center gap-1">
                      <span>Event</span>
                      <span className="type-data truncate" title={delivery.event_id}>
                        {shortId(delivery.event_id)}
                      </span>
                      <CopyButton value={delivery.event_id} label="Copy event id" />
                    </span>
                    {delivery.next_attempt_at ? (
                      <span>Next attempt {expiryWords(delivery.next_attempt_at)}</span>
                    ) : null}
                  </div>
                </div>
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}
