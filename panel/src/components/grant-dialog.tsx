import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useEffect, useRef, useState } from 'react';
import { useListAgents } from '../api/generated/agents/agents';
import { createGrant, getListGrantsQueryKey } from '../api/generated/grants/grants';
import { useListIncidents } from '../api/generated/incidents/incidents';
import type {
  CreateGrantRequest,
  Grant,
  Scope,
  Selector,
} from '../api/generated/model';
import { shortId } from '../lib/agency-format';
import {
  buildCreateGrant,
  grantRequestHeaders,
  grantSummary,
  TTL_PRESETS,
  TTL_UNITS,
  ttlFromCustom,
  ttlFromPreset,
  type TtlChoice,
  type TtlUnit,
} from '../lib/grants';
import { problemView, type ProblemView } from '../lib/problem';
import { ScopePicker } from './scope-picker';
import { SelectorEditor } from './selector-editor';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';
import { Field } from './ui/field';
import { Input } from './ui/input';
import { ProblemBanner } from './ui/problem-banner';
import { Textarea } from './ui/textarea';

const DEFAULT_TTL: TtlChoice = '1h';

export function GrantDialog({
  open,
  onOpenChange,
  preselectedAgentId,
  onIssued,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  preselectedAgentId?: string;
  onIssued: (grant: Grant) => void;
}) {
  const queryClient = useQueryClient();
  const agents = useListAgents({ limit: 200 }, { query: { staleTime: 30_000 } });
  const incidents = useListIncidents(
    { status: 'open' },
    { query: { enabled: open, staleTime: 15_000 } },
  );

  const [agentId, setAgentId] = useState('');
  const [scopes, setScopes] = useState<Scope[]>([]);
  const [selector, setSelector] = useState<Selector>({});
  const [ttlChoice, setTtlChoice] = useState<TtlChoice>(DEFAULT_TTL);
  const [customAmount, setCustomAmount] = useState('30');
  const [customUnit, setCustomUnit] = useState<TtlUnit>('minutes');
  const [maxUses, setMaxUses] = useState('');
  const [reason, setReason] = useState('');
  const [incidentId, setIncidentId] = useState('');
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const enabledAgents = (agents.data?.items ?? []).filter((agent) => !agent.disabled);
  const openIncidents = incidents.data?.items ?? [];
  const selectedAgent = enabledAgents.find((agent) => agent.id === agentId);

  useEffect(() => {
    if (!open) return;
    setAgentId('');
    setScopes([]);
    setSelector({});
    setTtlChoice(DEFAULT_TTL);
    setCustomAmount('30');
    setCustomUnit('minutes');
    setMaxUses('');
    setReason('');
    setIncidentId('');
    setProblem(null);
  }, [open]);

  // The agent filter may have opened this dialog: preselect it once the list
  // is known, and only while it is an agent that can still hold a grant.
  const preselectedApplied = useRef(false);
  useEffect(() => {
    if (!open) {
      preselectedApplied.current = false;
      return;
    }
    if (preselectedApplied.current) return;
    if (preselectedAgentId && enabledAgents.some((agent) => agent.id === preselectedAgentId)) {
      setAgentId(preselectedAgentId);
      preselectedApplied.current = true;
    }
  }, [open, preselectedAgentId, enabledAgents]);

  const ttlSeconds =
    ttlChoice === 'custom' ? ttlFromCustom(customAmount, customUnit) : ttlFromPreset(ttlChoice);

  const summary = grantSummary({
    agentName: selectedAgent?.name ?? '',
    scopes,
    selector,
    ttlSeconds,
    maxUses,
  });

  const issue = useMutation({
    mutationFn: (body: CreateGrantRequest) =>
      createGrant(body, {
        headers: grantRequestHeaders(incidentId === '' ? undefined : incidentId),
      }),
    onSuccess: (grant) => {
      setProblem(null);
      void queryClient.invalidateQueries({ queryKey: getListGrantsQueryKey() });
      onIssued(grant);
      onOpenChange(false);
    },
    onError: (error) => setProblem(problemView(error, 'issue grants')),
  });

  const submit = () => {
    const result = buildCreateGrant({
      agentId,
      scopes,
      selector,
      ttlSeconds,
      maxUses,
      reason,
    });
    if (!result.ok) {
      setProblem({ message: result.message });
      return;
    }
    setProblem(null);
    issue.mutate(result.body);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        title="Issue a grant"
        description="A grant is the only authority an agent has. It expires, and you can revoke it at any time."
      >
        <form
          className="flex flex-col gap-3.5"
          onSubmit={(event) => {
            event.preventDefault();
            submit();
          }}
        >
          {problem ? <ProblemBanner view={problem} /> : null}

          <Field
            label="Agent"
            htmlFor="grant-agent"
            hint="Disabled agents are not offered because they cannot use a grant."
          >
            <select
              id="grant-agent"
              className="field"
              value={agentId}
              onChange={(event) => setAgentId(event.target.value)}
            >
              <option value="">Choose an agent</option>
              {enabledAgents.map((agent) => (
                <option key={agent.id} value={agent.id}>
                  {agent.name}
                </option>
              ))}
            </select>
          </Field>

          <div className="flex flex-col gap-1.5">
            <p className="type-body-medium text-[13px] text-ink-2">Scopes</p>
            <ScopePicker id="grant-scopes" value={scopes} onChange={setScopes} />
            <p className="text-[13px] leading-snug text-ink-2">
              Delete and human-only scopes can't be granted: deleting needs an approval for each
              call.
            </p>
          </div>

          <div className="flex flex-col gap-1.5">
            <p className="type-body-medium text-[13px] text-ink-2">Resources</p>
            <SelectorEditor id="grant-selector" value={selector} onChange={setSelector} />
          </div>

          <div className="grid gap-3 sm:grid-cols-2">
            <Field label="Lifetime" htmlFor="grant-ttl" hint="The clock starts when you issue it.">
              <select
                id="grant-ttl"
                className="field"
                value={ttlChoice}
                onChange={(event) => setTtlChoice(event.target.value as TtlChoice)}
              >
                {TTL_PRESETS.map((preset) => (
                  <option key={preset.key} value={preset.key}>
                    {preset.label}
                  </option>
                ))}
                <option value="custom">Custom</option>
              </select>
            </Field>

            <Field
              label="Max uses"
              htmlFor="grant-max-uses"
              hint="Leave empty for no cap until it expires."
            >
              <Input
                id="grant-max-uses"
                type="number"
                min={1}
                step={1}
                inputMode="numeric"
                value={maxUses}
                onChange={(event) => setMaxUses(event.target.value)}
                placeholder="No cap"
              />
            </Field>
          </div>

          {ttlChoice === 'custom' ? (
            <div className="grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)] gap-2">
              <Input
                aria-label="Custom lifetime amount"
                type="number"
                min={1}
                step={1}
                inputMode="numeric"
                value={customAmount}
                onChange={(event) => setCustomAmount(event.target.value)}
              />
              <select
                aria-label="Custom lifetime unit"
                className="field"
                value={customUnit}
                onChange={(event) => setCustomUnit(event.target.value as TtlUnit)}
              >
                {TTL_UNITS.map((unit) => (
                  <option key={unit.value} value={unit.value}>
                    {unit.label}
                  </option>
                ))}
              </select>
            </div>
          ) : null}

          <Field
            label="Reason"
            htmlFor="grant-reason"
            hint="Recorded with the grant and in the audit log."
          >
            <Textarea
              id="grant-reason"
              value={reason}
              onChange={(event) => setReason(event.target.value)}
              placeholder="Triage the blog outage during the on-call shift"
            />
          </Field>

          <Field
            label="Incident"
            htmlFor="grant-incident"
            hint="The audit log records this grant under the incident. It stays a standard grant."
          >
            <select
              id="grant-incident"
              className="field"
              value={incidentId}
              onChange={(event) => setIncidentId(event.target.value)}
            >
              <option value="">No incident</option>
              {openIncidents.map((incident) => (
                <option key={incident.id} value={incident.id}>
                  {incident.title} ({shortId(incident.id)})
                </option>
              ))}
            </select>
          </Field>

          <p className="rounded-[10px] border border-line bg-sunk px-3 py-2 text-[13px] leading-snug text-ink">
            {summary}
          </p>

          <div className="mt-1 flex flex-wrap justify-end gap-2 border-t border-line pt-4">
            <Button variant="default" onClick={() => onOpenChange(false)}>
              Cancel
            </Button>
            <Button type="submit" variant="primary" disabled={issue.isPending}>
              {issue.isPending ? 'Issuing…' : 'Issue grant'}
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
}
