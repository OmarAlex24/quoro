import { useNavigate } from '@tanstack/react-router';
import { useEffect, useState } from 'react';
import { useListAgents } from '../api/generated/agents/agents';
import { useListIncidents } from '../api/generated/incidents/incidents';
import type { AuditRecordResult } from '../api/generated/model';
import { shortId } from '../lib/agency-format';
import { useSession } from '../session';
import type { AuditSearch } from '../router';
import { Button } from './ui/button';
import { Field } from './ui/field';
import { Input } from './ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select';

const resultOptions: ReadonlyArray<{ value: AuditRecordResult; label: string }> = [
  { value: 'ok', label: 'OK' },
  { value: 'refused', label: 'Refused' },
  { value: 'error', label: 'Error' },
];

/** The filters, bound to the route search so a narrowed view can be shared. */
export function AuditFilters({ search }: { search: AuditSearch }) {
  const navigate = useNavigate();
  const { user } = useSession();
  const agents = useListAgents({ limit: 200 }, { query: { staleTime: 30_000, retry: false } });
  const incidents = useListIncidents({ limit: 200 }, { query: { staleTime: 30_000, retry: false } });

  const [actorDraft, setActorDraft] = useState(search.actor_id ?? '');
  const [targetDraft, setTargetDraft] = useState(search.target_id ?? '');

  useEffect(() => {
    setActorDraft(search.actor_id ?? '');
  }, [search.actor_id]);
  useEffect(() => {
    setTargetDraft(search.target_id ?? '');
  }, [search.target_id]);

  const go = (patch: Partial<AuditSearch>) => {
    void navigate({
      to: '/audit',
      search: { ...search, ...patch, seq: undefined },
    });
  };

  const hasFilters =
    search.actor_id !== undefined ||
    search.target_id !== undefined ||
    search.result !== undefined ||
    search.incident_id !== undefined ||
    search.seq !== undefined;

  const agentItems = agents.data?.items ?? [];
  const incidentItems = incidents.data?.items ?? [];

  const actorValue =
    search.actor_id === undefined
      ? 'any'
      : user && search.actor_id === user.id
        ? 'you'
        : search.actor_id;

  const actorName =
    user && search.actor_id === user.id
      ? 'You'
      : (agentItems.find((agent) => agent.id === search.actor_id)?.name ??
        (search.actor_id ? shortId(search.actor_id) : ''));

  const incidentValue = search.incident_id ?? 'any';
  const knownIncident = incidentItems.some((incident) => incident.id === search.incident_id);

  const applyActorDraft = () => {
    const next = actorDraft.trim();
    if (next === (search.actor_id ?? '')) return;
    go({ actor_id: next === '' ? undefined : next });
  };

  const applyTargetDraft = () => {
    const next = targetDraft.trim();
    if (next === (search.target_id ?? '')) return;
    go({ target_id: next === '' ? undefined : next });
  };

  return (
    <section
      aria-label="Audit filters"
      className="rounded-[6px] border border-rail bg-panel px-4 py-3 bay-shadow"
    >
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <Field label="Actor" htmlFor="audit-actor">
          <Select
            value={actorValue}
            onValueChange={(value) => {
              if (value === 'any') go({ actor_id: undefined });
              else if (value === 'you') go({ actor_id: user?.id });
              else go({ actor_id: value });
            }}
          >
            <SelectTrigger id="audit-actor" aria-label="Actor">
              <SelectValue placeholder="Anyone">{actorName || 'Anyone'}</SelectValue>
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="any">Anyone</SelectItem>
              {user ? <SelectItem value="you">You</SelectItem> : null}
              {agentItems.map((agent) => (
                <SelectItem key={agent.id} value={agent.id}>
                  {agent.name}
                </SelectItem>
              ))}
              {search.actor_id !== undefined &&
              search.actor_id !== user?.id &&
              !agentItems.some((agent) => agent.id === search.actor_id) ? (
                <SelectItem value={search.actor_id}>
                  {shortId(search.actor_id)}
                </SelectItem>
              ) : null}
            </SelectContent>
          </Select>
        </Field>

        <Field
          label="Actor id"
          htmlFor="audit-actor-id"
          hint="Use an id that is not in the list."
        >
          <Input
            id="audit-actor-id"
            value={actorDraft}
            onChange={(event) => setActorDraft(event.target.value)}
            onBlur={applyActorDraft}
            onKeyDown={(event) => {
              if (event.key === 'Enter') {
                event.preventDefault();
                applyActorDraft();
              }
            }}
            placeholder="agt_…"
            spellCheck={false}
            autoComplete="off"
          />
        </Field>

        <Field label="Target id" htmlFor="audit-target-id">
          <Input
            id="audit-target-id"
            value={targetDraft}
            onChange={(event) => setTargetDraft(event.target.value)}
            onBlur={applyTargetDraft}
            onKeyDown={(event) => {
              if (event.key === 'Enter') {
                event.preventDefault();
                applyTargetDraft();
              }
            }}
            placeholder="app_…"
            spellCheck={false}
            autoComplete="off"
          />
        </Field>

        <Field label="Result" htmlFor="audit-result">
          <Select
            value={search.result ?? 'any'}
            onValueChange={(value) =>
              go({ result: value === 'any' ? undefined : (value as AuditRecordResult) })
            }
          >
            <SelectTrigger id="audit-result" aria-label="Result">
              <SelectValue placeholder="Any" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="any">Any</SelectItem>
              {resultOptions.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </Field>

        <Field label="Incident" htmlFor="audit-incident" className="sm:col-span-2">
          <Select
            value={incidentValue}
            onValueChange={(value) => go({ incident_id: value === 'any' ? undefined : value })}
          >
            <SelectTrigger id="audit-incident" aria-label="Incident">
              <SelectValue placeholder="Any incident" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="any">Any incident</SelectItem>
              {incidentItems.map((incident) => (
                <SelectItem key={incident.id} value={incident.id}>
                  {incident.title}
                </SelectItem>
              ))}
              {search.incident_id !== undefined && !knownIncident ? (
                <SelectItem value={search.incident_id}>
                  {shortId(search.incident_id)}
                </SelectItem>
              ) : null}
            </SelectContent>
          </Select>
        </Field>
      </div>

      {hasFilters ? (
        <div className="mt-3 flex justify-end">
          <Button
            variant="quiet"
            onClick={() => {
              setActorDraft('');
              setTargetDraft('');
              void navigate({ to: '/audit', search: {} });
            }}
          >
            Clear filters
          </Button>
        </div>
      ) : null}
    </section>
  );
}
