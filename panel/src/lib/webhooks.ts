import {
  EventType,
  type CreateWebhookRequest,
  type Selector,
  type UpdateWebhookRequest,
  type Webhook,
  type WebhookDelivery,
  type WebhookDeliveryStatus,
  type WebhookWake,
} from '../api/generated/model';
import { formatDuration } from './agency-format';
import { relativeTime } from './app-status';
import type { LampTone } from './lamp';

export const allEventTypes = (): EventType[] => Object.values(EventType);

/** `domain.unserved` -> `domain`. */
const eventSubject = (type: EventType): string => type.split('.')[0] ?? type;

/**
 * Every `EventType` grouped by its subject prefix, in the order the contract
 * lists them (`events.schema.json`). The groups come from the generated enum,
 * so a new event family shows up without a panel change. The add dialog
 * paints one group per subject.
 */
export const eventTypesBySubject = (): Array<{ subject: string; types: EventType[] }> => {
  const groups: Array<{ subject: string; types: EventType[] }> = [];
  for (const type of allEventTypes()) {
    const subject = eventSubject(type);
    const group = groups.find((candidate) => candidate.subject === subject);
    if (group) group.types.push(type);
    else groups.push({ subject, types: [type] });
  }
  return groups;
};

/** "Every event", the list when it is short, or a count when it is long. */
export const describeEventTypes = (types: readonly EventType[]): string => {
  if (types.length === 0) return 'Every event';
  if (types.length <= 3) return types.join(', ');
  return `${types.length} event types`;
};

/**
 * A receiver URL: HTTPS anywhere, or plain HTTP only to localhost for testing
 * (`Webhook.url` in the contract). Spaces and non-URL text are refused before
 * the request goes out.
 */
export const isValidWebhookUrl = (value: string): boolean => {
  const trimmed = value.trim();
  if (trimmed === '' || /\s/.test(trimmed)) return false;
  let parsed: URL;
  try {
    parsed = new URL(trimmed);
  } catch {
    return false;
  }
  if (parsed.protocol === 'https:') return parsed.hostname !== '';
  if (parsed.protocol !== 'http:') return false;
  return parsed.hostname === 'localhost';
};

export interface DeliveryLamp {
  readonly tone: LampTone;
  readonly pulse: boolean;
  readonly word: string;
}

const DELIVERY_LAMPS: Record<WebhookDeliveryStatus, DeliveryLamp> = {
  pending: { tone: 'shift', pulse: true, word: 'Pending' },
  delivered: { tone: 'run', pulse: false, word: 'Delivered' },
  failed: { tone: 'fault', pulse: false, word: 'Failed' },
};

/** The lamp and word for one delivery: delivered runs, pending shifts, failed faults. */
export const deliveryLamp = (status: WebhookDeliveryStatus): DeliveryLamp =>
  DELIVERY_LAMPS[status];

/**
 * What the attempt slot of a delivery row says. A test delivery is always
 * attempt 1 and never retried, so it says "Test delivery" instead of a number.
 */
export const deliveryAttemptWords = (
  delivery: Pick<WebhookDelivery, 'attempt' | 'test'>,
): string => (delivery.test ? 'Test delivery' : `Attempt ${delivery.attempt}`);

/**
 * The latest delivery in one line, for a webhook's row: "Delivered 200 · 12 min
 * ago", "Last delivery failed with 503 · 5 min ago", or "No deliveries yet".
 * `fault` turns the whole line the fault colour, as a failure should read.
 */
export const lastDeliveryLine = (
  delivery: Pick<WebhookDelivery, 'status' | 'response_code' | 'created_at' | 'test'> | undefined,
  now: number = Date.now(),
): { tone: LampTone; pulse: boolean; text: string; fault: boolean } => {
  if (!delivery) return { tone: 'idle', pulse: false, text: 'No deliveries yet', fault: false };
  const when = relativeTime(delivery.created_at, now);
  const code = delivery.response_code ?? null;
  const test = delivery.test ? 'Test delivery' : null;
  switch (delivery.status) {
    case 'delivered':
      return {
        tone: 'run',
        pulse: false,
        text: `${test ? `${test} delivered` : 'Delivered'}${code !== null ? ` ${code}` : ''} · ${when}`,
        fault: false,
      };
    case 'failed':
      return {
        tone: 'fault',
        pulse: false,
        text: `${test ? `${test} failed` : 'Last delivery failed'}${code !== null ? ` with ${code}` : ', no response'} · ${when}`,
        fault: true,
      };
    case 'pending':
      return { tone: 'shift', pulse: true, text: `Delivering · ${when}`, fault: false };
  }
};

/** Every 2 s while an attempt is still pending, otherwise every 15 s. */
export const deliveryPollingInterval = (
  deliveries: ReadonlyArray<Pick<WebhookDelivery, 'status'>>,
): number => (deliveries.some((delivery) => delivery.status === 'pending') ? 2_000 : 15_000);

/** The wake configuration with the contract defaults filled in. */
export const normalizeWake = (wake: WebhookWake): WebhookWake => ({
  agent_id: wake.agent_id,
  grant_scopes: wake.grant_scopes ?? [],
  grant_ttl_s: wake.grant_ttl_s ?? 3_600,
  grant_max_uses: wake.grant_max_uses ?? null,
});

/**
 * One sentence for a list bay: "Wakes on-call with app:read, app:logs for 1 h",
 * or "Notifies only" when the webhook has no wake configuration.
 */
export const wakeSummary = (wake: WebhookWake | undefined, agentName: string): string => {
  if (!wake) return 'Notifies only';
  const scopes = wake.grant_scopes ?? [];
  const ttl = wake.grant_ttl_s ?? 3_600;
  const withScopes = scopes.length > 0 ? ` with ${scopes.join(', ')}` : '';
  return `Wakes ${agentName}${withScopes} for ${formatDuration(ttl)}`;
};

/** What the add and edit forms hold, before it becomes a request body. */
export interface WebhookForm {
  name: string;
  url: string;
  event_types: EventType[];
  selector: Selector;
  enabled: boolean;
  wake?: WebhookWake;
}

/** The form the edit dialog starts from. `wake` is copied, never aliased. */
export const formFromWebhook = (webhook: Webhook): WebhookForm => ({
  name: webhook.name,
  url: webhook.url,
  event_types: [...webhook.event_types],
  selector: { ...webhook.selector },
  enabled: webhook.enabled,
  ...(webhook.wake ? { wake: normalizeWake(webhook.wake) } : {}),
});

const normalizeSelector = (selector: Selector) => ({
  all: selector.all === true,
  projects: selector.projects ?? [],
  apps: selector.apps ?? [],
  nodes: selector.nodes ?? [],
});

const sameSelector = (left: Selector, right: Selector): boolean =>
  JSON.stringify(normalizeSelector(left)) === JSON.stringify(normalizeSelector(right));

const sameEventTypes = (left: readonly EventType[], right: readonly EventType[]): boolean =>
  left.length === right.length && left.every((type, index) => type === right[index]);

/**
 * The create body: an empty `event_types` list means every event, and the wake
 * block is absent unless the form is waking an agent.
 */
export const buildCreateWebhookBody = (form: WebhookForm): CreateWebhookRequest => {
  const body: CreateWebhookRequest = {
    name: form.name.trim(),
    url: form.url.trim(),
    event_types: [...form.event_types],
    selector: form.selector,
    enabled: form.enabled,
  };
  if (form.wake?.agent_id) body.wake = normalizeWake(form.wake);
  return body;
};

/**
 * The PATCH body: only fields whose value changed. An absent field stays as it
 * is on the server, which is why the wake block is never sent empty.
 */
export const buildUpdateWebhookBody = (
  current: Webhook,
  form: WebhookForm,
): UpdateWebhookRequest => {
  const body: UpdateWebhookRequest = {};
  const name = form.name.trim();
  if (name !== current.name) body.name = name;
  const url = form.url.trim();
  if (url !== current.url) body.url = url;
  if (!sameEventTypes(form.event_types, current.event_types)) {
    body.event_types = [...form.event_types];
  }
  if (!sameSelector(form.selector, current.selector)) body.selector = form.selector;
  if (form.enabled !== current.enabled) body.enabled = form.enabled;
  if (form.wake?.agent_id) {
    const next = normalizeWake(form.wake);
    const before = current.wake ? normalizeWake(current.wake) : null;
    if (JSON.stringify(next) !== JSON.stringify(before)) body.wake = next;
  }
  return body;
};

export const updateHasChanges = (body: UpdateWebhookRequest): boolean =>
  Object.keys(body).length > 0;
