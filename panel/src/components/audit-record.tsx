import { getRouteApi, Link } from '@tanstack/react-router';
import type { ReactNode } from 'react';
import { useGetAgent } from '../api/generated/agents/agents';
import { useGetAuditRecord } from '../api/generated/audit/audit';
import type { AuditRecord } from '../api/generated/model';
import { isApiProblem } from '../api/fetcher';
import { shortId } from '../lib/agency-format';
import {
  actorWords,
  AUDIT_DETAIL_FIELDS,
  channelWords,
  localTime,
  resultLamp,
  resultWords,
  type AuditDetailFieldKey,
} from '../lib/audit';
import { problemView } from '../lib/problem';
import { CopyButton } from './copy-button';
import { AgentName, UserName } from './identity-name';
import { ToneLamp } from './tone-lamp';
import { ProblemBanner } from './ui/problem-banner';
import { SkeletonLines } from './skeleton';

/** A record field the format omits when it has no value. */
const hasText = (value: string | null | undefined): value is string =>
  typeof value === 'string' && value.length > 0;

function NotRecorded() {
  return <span className="text-ink-2">Not recorded</span>;
}

function DetailRow({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="set-row leading-snug">
      <dt>{label}</dt>
      <dd>{children}</dd>
    </div>
  );
}

/** The owner of an agent, read from the agent itself (only for agent actors). */
function AgentOwner({ agentId }: { agentId: string }) {
  const agent = useGetAgent(agentId, { query: { retry: false, staleTime: 30_000 } });
  const owner = agent.data?.owner_user_id;
  return hasText(owner) ? <UserName id={owner} /> : <NotRecorded />;
}

function Parameters({ params }: { params: AuditRecord['params'] }) {
  const entries = Object.entries(params ?? {});
  if (entries.length === 0) {
    return <span className="text-ink-2">No parameters recorded.</span>;
  }
  return (
    <dl className="flex flex-col gap-1.5">
      {entries.map(([key, value]) => (
        <div key={key} className="min-w-0">
          <dt className="type-code text-[11px] text-ink-2">{key}</dt>
          <dd className="min-w-0 [overflow-wrap:anywhere]">
            {value !== null && typeof value === 'object' ? (
              <pre className="type-code mt-0.5 max-w-full overflow-x-auto whitespace-pre-wrap break-words text-[11px] leading-snug text-ink">
                {JSON.stringify(value, null, 2)}
              </pre>
            ) : (
              <span className="type-code break-words text-[11px] text-ink">
                {value === null ? 'null' : String(value)}
              </span>
            )}
          </dd>
        </div>
      ))}
    </dl>
  );
}

function HashLine({ value }: { value: string }) {
  return (
    <div className="min-w-0">
      <span className="type-code block break-all text-[11px] leading-snug">{value}</span>
      <CopyButton value={value} label="Copy hash" />
    </div>
  );
}

function TargetValue({ record }: { record: AuditRecord }) {
  if (!hasText(record.target_type) && !hasText(record.target_id)) return <NotRecorded />;
  const type = record.target_type ?? 'target';
  if (!hasText(record.target_id)) return <span className="type-data">{type}</span>;
  const id = record.target_id;
  return (
    <span className="flex flex-wrap items-baseline gap-x-2 gap-y-1">
      <span className="text-ink-2">{type}</span>
      {type === 'app' ? (
        <Link
          to="/apps/$appId"
          params={{ appId: id }}
          title={id}
          className="type-code text-ink underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
        >
          {shortId(id)}
        </Link>
      ) : (
        <span className="type-code break-all" title={id}>
          {shortId(id)}
        </span>
      )}
    </span>
  );
}

/**
 * Every field of one audit record, in the order of audit-log-format.md
 * section 2, read with `getAuditRecord`. The side panel and the record sheet
 * both show it.
 */
export function AuditRecordBody({ seq }: { seq: number }) {
  const search = getRouteApi('/shell/activity').useSearch();
  const recordQuery = useGetAuditRecord(seq);

  const missing = isApiProblem(recordQuery.error) && recordQuery.error.code === 'not_found';
  const record = recordQuery.data;

  const renderValue = (key: AuditDetailFieldKey, value: AuditRecord): ReactNode => {
    switch (key) {
      case 'seq':
        return <span className="type-data text-[14px]">{value.seq}</span>;
      case 'time':
        return (
          <span className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5">
            <span className="type-data text-[13px]">{localTime(value.time)}</span>
            <span className="type-data text-[12px] text-ink-2" title="RFC 3339">
              {value.time}
            </span>
          </span>
        );
      case 'actor':
        return (
          <span className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5">
            <span className="text-ink-2">{actorWords[value.actor_type]}</span>
            {value.actor_type === 'agent' ? (
              <AgentName id={value.actor_id} />
            ) : value.actor_type === 'user' ? (
              <UserName id={value.actor_id} />
            ) : (
              <span className="type-code break-all" title={value.actor_id}>
                {shortId(value.actor_id)}
              </span>
            )}
          </span>
        );
      case 'agent_model':
        return hasText(value.agent_model) ? (
          <span className="type-data">{value.agent_model}</span>
        ) : (
          <NotRecorded />
        );
      case 'agent_owner':
        return value.actor_type === 'agent' ? (
          <AgentOwner agentId={value.actor_id} />
        ) : (
          <NotRecorded />
        );
      case 'on_behalf_of':
        return hasText(value.on_behalf_of) ? <UserName id={value.on_behalf_of} /> : <NotRecorded />;
      case 'channel':
        return <span>{channelWords[value.channel]}</span>;
      case 'operation':
        return <span className="type-code break-all">{value.operation}</span>;
      case 'scope':
        return hasText(value.scope) ? (
          <span className="type-code">{value.scope}</span>
        ) : (
          <NotRecorded />
        );
      case 'target':
        return <TargetValue record={value} />;
      case 'project':
        return hasText(value.project_id) ? (
          <span className="type-code break-all" title={value.project_id}>
            {shortId(value.project_id)}
          </span>
        ) : (
          <NotRecorded />
        );
      case 'params':
        return <Parameters params={value.params} />;
      case 'params_digest':
        return hasText(value.params_sha256) && typeof value.params_bytes === 'number' ? (
          <span className="type-code break-all text-[11px] leading-snug">
            sha256 {value.params_sha256} · {value.params_bytes} bytes
          </span>
        ) : (
          <NotRecorded />
        );
      case 'justification':
        return hasText(value.justification) ? (
          <span>{value.justification}</span>
        ) : (
          <NotRecorded />
        );
      case 'trigger_event':
        return hasText(value.trigger_event_id) ? (
          <span className="type-code break-all" title={value.trigger_event_id}>
            {shortId(value.trigger_event_id)}
          </span>
        ) : (
          <NotRecorded />
        );
      case 'incident':
        return hasText(value.incident_id) ? (
          <Link
            to="/activity"
            search={{ ...search, incident_id: value.incident_id, seq: undefined }}
            title={value.incident_id}
            className="type-code text-ink underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
          >
            {shortId(value.incident_id)}
          </Link>
        ) : (
          <NotRecorded />
        );
      case 'grant':
        return hasText(value.grant_id) ? (
          <span className="type-code break-all" title={value.grant_id}>
            {shortId(value.grant_id)}
          </span>
        ) : (
          <NotRecorded />
        );
      case 'approval':
        return hasText(value.approval_id) ? (
          <span className="type-code break-all" title={value.approval_id}>
            {shortId(value.approval_id)}
          </span>
        ) : (
          <NotRecorded />
        );
      case 'request_id':
        return (
          <span className="flex flex-wrap items-center gap-2">
            <span className="type-code break-all" title={value.request_id}>
              {shortId(value.request_id)}
            </span>
            <CopyButton value={value.request_id} label="Copy request id" />
          </span>
        );
      case 'result':
        return (
          <span className="inline-flex items-center gap-1.5">
            <ToneLamp
              tone={resultLamp(value.result)}
              size={13}
              label={resultWords[value.result]}
            />
            <span>{resultWords[value.result]}</span>
          </span>
        );
      case 'result_detail':
        return hasText(value.result_detail) ? (
          <span className="type-code">{value.result_detail}</span>
        ) : (
          <NotRecorded />
        );
      case 'change_seq':
        return typeof value.change_seq === 'number' ? (
          <span className="type-data">{value.change_seq}</span>
        ) : (
          <NotRecorded />
        );
      case 'prev_hash':
        return <HashLine value={value.prev_hash} />;
      case 'hash':
        return <HashLine value={value.hash} />;
    }
  };

  if (recordQuery.isPending) return <SkeletonLines label={`Loading entry ${seq}`} lines={6} heading={false} />;
  if (missing) return <p className="text-[13px] text-ink">No entry with that sequence number.</p>;
  if (recordQuery.error) return <ProblemBanner view={problemView(recordQuery.error, 'read this entry')} />;
  if (!record) return null;
  return (
    <dl className="box set-rows">
      {AUDIT_DETAIL_FIELDS.map((field) => (
        <DetailRow key={field.key} label={field.label}>
          {renderValue(field.key, record)}
        </DetailRow>
      ))}
    </dl>
  );
}
