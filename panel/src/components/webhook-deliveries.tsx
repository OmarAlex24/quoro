import { FlaskConical } from 'lucide-react';
import { useListWebhookDeliveries } from '../api/generated/webhooks/webhooks';
import { expiryWords, shortId } from '../lib/agency-format';
import { relativeTime } from '../lib/app-status';
import { cn } from '../lib/utils';
import {
  deliveryAttemptWords,
  deliveryLamp,
  deliveryPollingInterval,
} from '../lib/webhooks';
import { CopyButton } from './copy-button';
import { ToneLamp } from './tone-lamp';

/** The delivery history of one webhook, newest first, one row per attempt. */
export function WebhookDeliveries({ webhookId }: { webhookId: string }) {
  const deliveries = useListWebhookDeliveries(webhookId, undefined, {
    query: {
      refetchInterval: (query) => deliveryPollingInterval(query.state.data?.items ?? []),
    },
  });
  const items = deliveries.data?.items ?? [];

  return (
    <section className="grid gap-2.5" aria-labelledby="webhook-deliveries">
      <h3 id="webhook-deliveries" className="set-sub">
        Deliveries
      </h3>

      {deliveries.isPending ? (
        <div className="box set-loading" role="status">
          <span className="sr-only">Loading deliveries</span>
        </div>
      ) : items.length === 0 ? (
        <div className="set-empty">
          <p className="set-empty-t">No deliveries yet.</p>
          <p className="set-empty-x">Send a test delivery to check the receiver.</p>
        </div>
      ) : (
        <ul className="box">
          {items.map((delivery) => {
            const lamp = deliveryLamp(delivery.status);
            const failed = lamp.tone === 'fault';
            return (
              <li key={delivery.id} className="srow srow-tight">
                <ToneLamp
                  tone={lamp.tone}
                  pulse={lamp.pulse}
                  size={11}
                  label={lamp.word}
                  className="srow-lamp"
                />
                <div className="srow-t">
                  <b>
                    <span className={cn(failed && 'text-fault')}>{lamp.word}</span>
                    {delivery.test ? (
                      <span className="srow-test">
                        <FlaskConical aria-hidden="true" size={13} strokeWidth={1.8} />
                        {deliveryAttemptWords(delivery)}
                      </span>
                    ) : null}
                  </b>
                  {delivery.error ? <span className="srow-sub">{delivery.error}</span> : null}
                  <small>
                    {delivery.test ? null : (
                      <>
                        <span>{deliveryAttemptWords(delivery)}</span>
                        <span className="dot-sep" aria-hidden="true" />
                      </>
                    )}
                    <span>
                      {delivery.response_code === null || delivery.response_code === undefined
                        ? 'No response'
                        : `HTTP ${delivery.response_code}`}
                    </span>
                    <span className="dot-sep" aria-hidden="true" />
                    <span title={delivery.created_at}>{relativeTime(delivery.created_at)}</span>
                    {delivery.next_attempt_at ? (
                      <>
                        <span className="dot-sep" aria-hidden="true" />
                        <span>Next attempt {expiryWords(delivery.next_attempt_at)}</span>
                      </>
                    ) : null}
                  </small>
                </div>
                <span className="srow-end">
                  <span className="text-ink-3">Event</span>
                  <span className="type-code" title={delivery.event_id}>
                    {shortId(delivery.event_id)}
                  </span>
                  <CopyButton value={delivery.event_id} label="Copy id" />
                </span>
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}
