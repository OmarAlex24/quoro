import type { AuditRecord } from '../api/generated/model';
import { shortHash, shortId } from '../lib/agency-format';
import { localTime, resultLamp, resultWords } from '../lib/audit';
import { cn } from '../lib/utils';
import { Avatar } from './avatar';
import { AgentName, UserAvatar, UserName } from './identity-name';
import { ToneLamp } from './tone-lamp';

/**
 * The actor as the list row says it: an agent's name, a person's name, or an
 * id, each behind its avatar (square dither, round initials, the mark).
 */
function RowActor({ record }: { record: AuditRecord }) {
  if (record.actor_type === 'agent') {
    return (
      <span className="inline-flex min-w-0 items-center gap-2">
        <Avatar kind="agent" seed={record.actor_id} />
        <span className="min-w-0">
          <AgentName id={record.actor_id} link={false} />
          <span className="block text-[11px] leading-tight text-ink-2">agent</span>
        </span>
      </span>
    );
  }
  if (record.actor_type === 'user') {
    return (
      <span className="inline-flex min-w-0 items-center gap-2">
        <UserAvatar id={record.actor_id} />
        <UserName id={record.actor_id} />
      </span>
    );
  }
  return (
    <span className="inline-flex min-w-0 items-center gap-2">
      <Avatar kind="system" />
      <span className="type-code min-w-0 truncate text-[12px]" title={record.actor_id}>
        {shortId(record.actor_id)}
      </span>
    </span>
  );
}

/**
 * One link on the rail: the connector into the entry (labelled with the link),
 * the node, and the entry itself as a button that opens the record.
 */
export function AuditEntryRow({
  record,
  above,
  selected,
  broken,
  onSelect,
}: {
  record: AuditRecord;
  above: AuditRecord | undefined;
  selected: boolean;
  broken: boolean;
  onSelect: () => void;
}) {
  const connected = record.seq > 1;
  // Only compare against the entry above when it really is the previous
  // sequence; a filtered view hides entries and cannot check the link here.
  const linkMatches =
    connected && above?.seq === record.seq - 1 ? above.hash === record.prev_hash : true;

  return (
    <li
      id={`audit-entry-${record.seq}`}
      className="grid scroll-mt-6 grid-cols-[20px_minmax(0,1fr)] gap-x-2"
    >
      <div className="relative min-h-full" aria-hidden="true">
        {connected ? (
          broken ? (
            <span className="absolute left-1/2 top-0 h-[10px] w-[2px] -translate-x-1/2">
              <span className="absolute left-1/2 top-[4px] h-[2px] w-[9px] -translate-x-1/2 -rotate-[28deg] bg-fault" />
            </span>
          ) : (
            <span className="absolute left-1/2 top-0 h-[10px] w-[2px] -translate-x-1/2 bg-ink-2/35" />
          )
        ) : null}
        <span
          className={cn(
            'absolute left-1/2 top-[9px] h-[9px] w-[9px] -translate-x-1/2 rounded-full border-2',
            broken
              ? 'border-fault bg-fault'
              : selected
                ? 'border-accent bg-accent-solid'
                : 'border-ink-2/60 bg-surface',
          )}
        />
        <span className="absolute bottom-0 left-1/2 top-[18px] w-[2px] -translate-x-1/2 bg-ink-2/35" />
      </div>

      <div className="min-w-0 pb-2.5">
        <p
          className={cn(
            'flex h-[14px] items-center gap-1.5 text-[11px] leading-none',
            broken ? 'text-fault' : 'text-ink-2',
          )}
        >
          {connected ? (
            <>
              <span className="type-data" title={record.prev_hash}>
                {broken ? 'broken link' : `link ${shortHash(record.prev_hash)}`}
              </span>
              {!broken && !linkMatches ? (
                <span className="text-fault">does not match the entry above</span>
              ) : null}
            </>
          ) : (
            <span className="type-data">chain start</span>
          )}
        </p>

        <button
          type="button"
          onClick={onSelect}
          aria-current={selected ? 'true' : undefined}
          aria-label={`Open audit entry ${record.seq}`}
          className={cn(
            'mt-1 w-full rounded-[10px] border bg-surface px-3 py-2 text-left transition-colors card-sheen',
            'hover:border-ink-2 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent',
            broken ? 'border-fault' : selected ? 'border-accent' : 'border-line',
          )}
        >
          <span className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
            <span className="type-data text-[12px] text-ink-2">seq {record.seq}</span>
            <span className="type-data text-[12px] text-ink-2">
              {localTime(record.time)}
            </span>
            <span className="min-w-0 flex-1 text-[13px] text-ink">
              <RowActor record={record} />
            </span>
            <span className="inline-flex items-center gap-1.5 whitespace-nowrap text-[12px] text-ink-2">
              <ToneLamp
                tone={resultLamp(record.result)}
                size={12}
                label={resultWords[record.result]}
              />
              <span>
                {resultWords[record.result]}
                {record.result_detail ? ` · ${record.result_detail}` : ''}
              </span>
            </span>
          </span>
          <span className="mt-1 flex flex-wrap gap-x-3 gap-y-0.5 text-[12px] text-ink-2">
            <span className="type-code">{record.operation}</span>
            {record.target_type || record.target_id ? (
              <span>
                {record.target_type ?? 'target'}
                {record.target_id ? (
                  <span className="type-code ml-1" title={record.target_id}>
                    {shortId(record.target_id)}
                  </span>
                ) : null}
              </span>
            ) : null}
          </span>
        </button>
      </div>
    </li>
  );
}
