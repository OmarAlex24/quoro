import { useNavigate } from '@tanstack/react-router';
import { Search } from 'lucide-react';
import { useEffect, useState } from 'react';
import { useListAgents } from '../api/generated/agents/agents';
import { useListIncidents } from '../api/generated/incidents/incidents';
import type { AuditRecordResult } from '../api/generated/model';
import { shortId } from '../lib/agency-format';
import { useSession } from '../session';
import type { AuditSearch } from '../lib/routes';
import { Button } from './ui/button';
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
      to: '/activity',
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
    <section aria-label="Activity filters" className="filters">
      <Select
        value={actorValue}
        onValueChange={(value) => {
          if (value === 'any') go({ actor_id: undefined });
          else if (value === 'you') go({ actor_id: user?.id });
          else go({ actor_id: value });
        }}
      >
        <SelectTrigger aria-label="Actor" className="filter">
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
            <SelectItem value={search.actor_id}>{shortId(search.actor_id)}</SelectItem>
          ) : null}
        </SelectContent>
      </Select>

      <Select
        value={search.result ?? 'any'}
        onValueChange={(value) => go({ result: value === 'any' ? undefined : (value as AuditRecordResult) })}
      >
        <SelectTrigger aria-label="Result" className="filter">
          <SelectValue placeholder="Any result" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="any">Any result</SelectItem>
          {resultOptions.map((option) => (
            <SelectItem key={option.value} value={option.value}>
              {option.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      <Select
        value={incidentValue}
        onValueChange={(value) => go({ incident_id: value === 'any' ? undefined : value })}
      >
        <SelectTrigger aria-label="Incident" className="filter filter-wide">
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
            <SelectItem value={search.incident_id}>{shortId(search.incident_id)}</SelectItem>
          ) : null}
        </SelectContent>
      </Select>

      <label className="filter-find">
        <Search size={15} aria-hidden="true" />
        <span className="sr-only">Target id</span>
        <Input
          value={targetDraft}
          onChange={(event) => setTargetDraft(event.target.value)}
          onBlur={applyTargetDraft}
          onKeyDown={(event) => {
            if (event.key === 'Enter') {
              event.preventDefault();
              applyTargetDraft();
            }
          }}
          placeholder="Find a target by id"
          spellCheck={false}
          autoComplete="off"
        />
      </label>

      <label className="filter-find filter-narrow">
        <span className="sr-only">Actor id</span>
        <Input
          value={actorDraft}
          onChange={(event) => setActorDraft(event.target.value)}
          onBlur={applyActorDraft}
          onKeyDown={(event) => {
            if (event.key === 'Enter') {
              event.preventDefault();
              applyActorDraft();
            }
          }}
          placeholder="Actor id not listed"
          spellCheck={false}
          autoComplete="off"
        />
      </label>

      {hasFilters ? (
        <Button
          variant="ghost"
          onClick={() => {
            setActorDraft('');
            setTargetDraft('');
            void navigate({ to: '/activity', search: {} });
          }}
        >
          Clear filters
        </Button>
      ) : null}
    </section>
  );
}
