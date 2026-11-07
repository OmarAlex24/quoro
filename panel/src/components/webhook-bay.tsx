import { Link } from '@tanstack/react-router';
import { useListAgents } from '../api/generated/agents/agents';
import type { Webhook } from '../api/generated/model';
import { useListWebhookDeliveries } from '../api/generated/webhooks/webhooks';
import { describeSelector, shortId } from '../lib/agency-format';
import { relativeTime } from '../lib/app-status';
import { deliveryLamp, describeEventTypes, wakeSummary } from '../lib/webhooks';
import { ToneLamp } from './tone-lamp';

/** One webhook as a rack bay: lamp well on the left, subscription plate on the right. */
export function WebhookBay({ webhook }: { webhook: Webhook }) {
  const agents = useListAgents({ limit: 200 }, { query: { staleTime: 30_000 } });
  const agentName =
    agents.data?.items.find((agent) => agent.id === webhook.wake?.agent_id)?.name ??
    (webhook.wake ? shortId(webhook.wake.agent_id) : '');

  const deliveries = useListWebhookDeliveries(
    webhook.id,
    { limit: 1 },
    { query: { staleTime: 5_000 } },
  );
  const latest = deliveries.data?.items[0];

  return (
    <article className="grid grid-cols-[56px_minmax(0,1fr)] overflow-hidden rounded-[10px] border border-line bg-surface card-sheen">
      <div className="flex items-center justify-center border-r border-line bg-sunk">
        <ToneLamp
          tone={webhook.enabled ? 'run' : 'idle'}
          label={webhook.enabled ? 'Delivering' : 'Paused'}
        />
      </div>

      <div className="min-w-0 px-4 py-3">
        <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
          <Link
            to="/settings/webhooks/$webhookId"
            params={{ webhookId: webhook.id }}
            className="type-bay-title truncate text-[18px] leading-tight text-ink underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
          >
            {webhook.name}
          </Link>
          <span className="type-data text-[13px] text-ink-2">
            {webhook.enabled ? 'Delivering' : 'Paused'}
          </span>
        </div>

        <p className="type-code mt-1.5 truncate text-[13px] text-ink" title={webhook.url}>
          {webhook.url}
        </p>

        <div className="mt-1.5 flex flex-wrap gap-x-4 gap-y-1 text-[13px] text-ink-2">
          <span>{describeEventTypes(webhook.event_types)}</span>
          <span>{describeSelector(webhook.selector)}</span>
          <span>{wakeSummary(webhook.wake, agentName)}</span>
        </div>

        {latest ? (
          <p className="mt-1.5 inline-flex flex-wrap items-center gap-1.5 text-[13px] text-ink-2">
            <ToneLamp
              tone={deliveryLamp(latest.status).tone}
              pulse={deliveryLamp(latest.status).pulse}
              size={13}
              label={deliveryLamp(latest.status).word}
            />
            <span>
              Last delivery {deliveryLamp(latest.status).word.toLowerCase()}
              {latest.response_code ? ` (${latest.response_code})` : ''} ·{' '}
              {relativeTime(latest.created_at)}
            </span>
          </p>
        ) : null}
      </div>
    </article>
  );
}
