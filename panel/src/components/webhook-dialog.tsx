import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from '@tanstack/react-router';
import { useEffect, useState } from 'react';
import { useListAgents } from '../api/generated/agents/agents';
import type {
  Scope,
  UpdateWebhookRequest,
  Webhook,
  WebhookWake,
} from '../api/generated/model';
import {
  createWebhook,
  getGetWebhookQueryKey,
  getListWebhooksQueryKey,
  updateWebhook,
} from '../api/generated/webhooks/webhooks';
import { problemView, type ProblemView } from '../lib/problem';
import {
  buildCreateWebhookBody,
  buildUpdateWebhookBody,
  formFromWebhook,
  isValidWebhookUrl,
  updateHasChanges,
  type WebhookForm,
} from '../lib/webhooks';
import { selectorIsEmpty, SelectorEditor } from './selector-editor';
import { ScopePicker } from './scope-picker';
import { SecretOnce } from './secret-once';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';
import { Field } from './ui/field';
import { Input } from './ui/input';
import { ProblemBanner } from './ui/problem-banner';
import { WebhookEventPicker } from './webhook-event-picker';

const DEFAULT_FORM: WebhookForm = {
  name: '',
  url: '',
  event_types: [],
  selector: { all: true },
  enabled: true,
};

const TTL_PRESETS: ReadonlyArray<{ seconds: number; label: string }> = [
  { seconds: 900, label: '15 min' },
  { seconds: 3_600, label: '1 h' },
  { seconds: 14_400, label: '4 h' },
  { seconds: 86_400, label: '24 h' },
];

const SECRET_NOTE =
  'Copy it now. Quoro shows it only once. Deliveries carry Quoro-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256 of "<t>.<raw body>">; reject a signature older than 5 minutes.';

/**
 * Add or edit a webhook. In add mode the plate switches to the signing secret
 * once the API returns it; the secret lives in the mutation result only, never
 * in the query cache, the URL or storage.
 */
export function WebhookDialog({
  mode,
  open,
  onOpenChange,
  webhook,
  onSaved,
}: {
  mode: 'create' | 'edit';
  open: boolean;
  onOpenChange: (open: boolean) => void;
  webhook?: Webhook;
  onSaved?: () => void;
}) {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const [form, setForm] = useState<WebhookForm>(DEFAULT_FORM);
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const agents = useListAgents({ limit: 200 }, { query: { staleTime: 30_000 } });
  const agentOptions = (agents.data?.items ?? []).filter(
    (agent) => !agent.disabled || agent.id === form.wake?.agent_id,
  );

  useEffect(() => {
    if (!open) return;
    setForm(webhook ? formFromWebhook(webhook) : DEFAULT_FORM);
    setProblem(null);
  }, [open, webhook]);

  const saveCreate = useMutation({
    mutationFn: (body: WebhookForm) =>
      createWebhook(buildCreateWebhookBody(body), {
        headers: { 'Idempotency-Key': crypto.randomUUID() },
      }),
    onSuccess: () => {
      setProblem(null);
      void queryClient.invalidateQueries({ queryKey: getListWebhooksQueryKey() });
    },
    onError: (error) => setProblem(problemView(error, 'add webhooks')),
  });

  const saveEdit = useMutation({
    mutationFn: (input: { id: string; body: UpdateWebhookRequest }) =>
      updateWebhook(input.id, input.body),
    onSuccess: () => {
      setProblem(null);
      void queryClient.invalidateQueries({ queryKey: getListWebhooksQueryKey() });
      if (webhook) {
        void queryClient.invalidateQueries({
          queryKey: getGetWebhookQueryKey(webhook.id),
        });
      }
      onSaved?.();
      onOpenChange(false);
    },
    onError: (error) => setProblem(problemView(error, 'change webhooks')),
  });

  const created = mode === 'create' ? saveCreate.data : undefined;
  const pending = saveCreate.isPending || saveEdit.isPending;

  const close = (next: boolean) => {
    if (!next) {
      // Drop the one-time secret with the plate.
      saveCreate.reset();
      setProblem(null);
    }
    onOpenChange(next);
  };

  const update = (patch: Partial<WebhookForm>) =>
    setForm((current) => ({ ...current, ...patch }));

  const updateWake = (patch: Partial<WebhookWake>) =>
    setForm((current) =>
      current.wake ? { ...current, wake: { ...current.wake, ...patch } } : current,
    );

  const toggleWake = (waking: boolean) => {
    setForm((current) => {
      if (!waking) {
        const { wake: _wake, ...rest } = current;
        return rest;
      }
      return {
        ...current,
        wake: {
          agent_id: agentOptions[0]?.id ?? '',
          grant_scopes: [],
          grant_ttl_s: 3_600,
          grant_max_uses: null,
        },
      };
    });
  };

  const submit = () => {
    const name = form.name.trim();
    if (name === '' || name.length > 200) {
      setProblem({ message: 'Give the webhook a name of 1-200 characters.' });
      return;
    }
    if (!isValidWebhookUrl(form.url)) {
      setProblem({
        message:
          'Enter an https:// receiver URL, or http://localhost for testing.',
      });
      return;
    }
    if (selectorIsEmpty(form.selector)) {
      setProblem({ message: 'Choose every resource or at least one resource.' });
      return;
    }
    if (form.wake && form.wake.agent_id === '') {
      setProblem({ message: 'Choose the agent to wake, or turn the wake off.' });
      return;
    }

    setProblem(null);
    if (mode === 'create') {
      saveCreate.mutate(form);
      return;
    }
    if (!webhook) {
      setProblem({ message: 'This webhook is not loaded. Close this and try again.' });
      return;
    }
    const body = buildUpdateWebhookBody(webhook, form);
    if (!updateHasChanges(body)) {
      setProblem({ message: 'Nothing has changed yet. Change a field, then save.' });
      return;
    }
    saveEdit.mutate({ id: webhook.id, body });
  };

  const title =
    mode === 'edit' ? 'Edit webhook' : created ? 'Webhook added' : 'Add a webhook';
  const description =
    mode === 'edit'
      ? 'Change the receiver, what fires it, and whether it wakes an agent.'
      : created
        ? 'Copy the signing secret, then open the webhook.'
        : 'Send events to a receiver, and optionally wake an agent with a short grant for the incident.';

  return (
    <Dialog open={open} onOpenChange={close}>
      <DialogContent title={title} description={description}>
        {created ? (
          <div className="flex flex-col gap-4">
            <SecretOnce
              label={`Signing secret for ${created.name}`}
              secret={created.secret}
              note={SECRET_NOTE}
            />
            <div className="flex justify-end border-t border-line pt-4">
              <Button
                variant="primary"
                onClick={() => {
                  const id = created.id;
                  saveCreate.reset();
                  onOpenChange(false);
                  void navigate({
                    to: '/settings/webhooks/$webhookId',
                    params: { webhookId: id },
                  });
                }}
              >
                Done
              </Button>
            </div>
          </div>
        ) : (
          <form
            className="flex flex-col gap-4"
            onSubmit={(event) => {
              event.preventDefault();
              submit();
            }}
          >
            {problem ? <ProblemBanner view={problem} /> : null}

            <Field label="Name" htmlFor="webhook-name">
              <Input
                id="webhook-name"
                value={form.name}
                maxLength={200}
                onChange={(event) => update({ name: event.target.value })}
                placeholder="Wake the on-call agent"
              />
            </Field>

            <Field
              label="Receiver URL"
              htmlFor="webhook-url"
              hint="HTTPS only. Plain http://localhost is allowed for testing."
            >
              <Input
                id="webhook-url"
                value={form.url}
                onChange={(event) => update({ url: event.target.value })}
                placeholder="https://agents.example.com/hooks/quoro"
                spellCheck={false}
              />
            </Field>

            <div className="flex flex-col gap-1.5">
              <span className="type-body-medium text-[13px] text-ink-2">Events</span>
              <WebhookEventPicker
                id="webhook-events"
                value={form.event_types}
                onChange={(types) => update({ event_types: types })}
              />
            </div>

            <div className="flex flex-col gap-1.5">
              <span className="type-body-medium text-[13px] text-ink-2">Resources</span>
              <SelectorEditor
                id="webhook-selector"
                value={form.selector}
                onChange={(selector) => update({ selector })}
              />
            </div>

            <label className="inline-flex cursor-pointer items-center gap-2 text-[14px]">
              <input
                type="checkbox"
                className="h-4 w-4 accent-accent-solid"
                checked={form.enabled}
                onChange={(event) => update({ enabled: event.target.checked })}
              />
              Deliver right away
            </label>

            <fieldset className="rounded-[10px] border border-line bg-sunk/40 px-3.5 py-3">
              <legend className="type-body-medium px-1 text-[13px] text-ink-2">
                Wake
              </legend>
              <div className="flex flex-col gap-3">
                {mode === 'edit' && webhook?.wake ? (
                  <>
                    <p className="text-[14px]">Wakes an agent with an incident grant.</p>
                    <p className="text-[13px] leading-snug text-ink-2">
                      To stop waking the agent, delete this webhook and add it again.
                    </p>
                  </>
                ) : (
                  <label className="inline-flex cursor-pointer items-center gap-2 text-[14px]">
                    <input
                      type="checkbox"
                      className="h-4 w-4 accent-accent-solid"
                      checked={form.wake !== undefined}
                      onChange={(event) => toggleWake(event.target.checked)}
                    />
                    Wake an agent
                  </label>
                )}

                {form.wake ? (
                  <>
                    <Field label="Agent" htmlFor="webhook-wake-agent">
                      <select
                        id="webhook-wake-agent"
                        className="field"
                        value={form.wake.agent_id}
                        onChange={(event) => updateWake({ agent_id: event.target.value })}
                      >
                        <option value="">Choose an agent</option>
                        {agentOptions.map((agent) => (
                          <option key={agent.id} value={agent.id}>
                            {agent.disabled ? `${agent.name} (disabled)` : agent.name}
                          </option>
                        ))}
                      </select>
                    </Field>

                    <div className="flex flex-col gap-1.5">
                      <span className="type-body-medium text-[13px] text-ink-2">
                        Grant scopes
                      </span>
                      <ScopePicker
                        id="webhook-wake-scopes"
                        value={form.wake.grant_scopes ?? []}
                        onChange={(scopes: Scope[]) => updateWake({ grant_scopes: scopes })}
                      />
                    </div>

                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                      <Field label="Grant lifetime" htmlFor="webhook-wake-ttl">
                        <select
                          id="webhook-wake-ttl"
                          className="field"
                          value={String(form.wake.grant_ttl_s ?? 3_600)}
                          onChange={(event) =>
                            updateWake({ grant_ttl_s: Number(event.target.value) })
                          }
                        >
                          {TTL_PRESETS.map((preset) => (
                            <option key={preset.seconds} value={preset.seconds}>
                              {preset.label}
                            </option>
                          ))}
                        </select>
                      </Field>

                      <Field
                        label="Maximum uses"
                        htmlFor="webhook-wake-uses"
                        hint="Leave empty for no limit."
                      >
                        <Input
                          id="webhook-wake-uses"
                          type="number"
                          min={1}
                          value={form.wake.grant_max_uses ?? ''}
                          onChange={(event) => {
                            const value = event.target.value;
                            const parsed = Number(value);
                            updateWake({
                              grant_max_uses:
                                value === '' || !Number.isInteger(parsed) || parsed < 1
                                  ? null
                                  : parsed,
                            });
                          }}
                        />
                      </Field>
                    </div>

                    <p className="text-[13px] leading-snug text-ink-2">
                      Incident grants act on your behalf, carry only non-destructive
                      scopes, and end when the incident resolves.
                    </p>
                  </>
                ) : null}
              </div>
            </fieldset>

            {mode === 'edit' ? (
              <p className="text-[13px] leading-snug text-ink-2">
                To change the signing secret, delete this webhook and add it again.
              </p>
            ) : null}

            <div className="flex flex-wrap justify-end gap-2 border-t border-line pt-4">
              <Button variant="default" onClick={() => close(false)}>
                Cancel
              </Button>
              <Button busy={pending} variant="primary" type="submit" disabled={pending}>
                {pending
                  ? 'Saving…'
                  : mode === 'edit'
                    ? 'Save changes'
                    : 'Add webhook'}
              </Button>
            </div>
          </form>
        )}
      </DialogContent>
    </Dialog>
  );
}
