import type { AuditRecord } from '../api/generated/model';
import { shortHash, shortId } from '../lib/agency-format';
import { localTime, resultLamp, resultWords } from '../lib/audit';
import { cn } from '../lib/utils';
import { AgentName, UserName } from './identity-name';
import { ToneLamp } from './tone-lamp';

/** The actor as the list row says it: an agent's name, a person's name, or an id. */
function RowActor({ record }: { record: AuditRecord }) {
  if (record.actor_type === 'agent') {
    return (
      <span className="min-w-0">
        <AgentName id={record.actor_id} link={false} />
        <span className="block text-[11px] leading-tight text-ink-muted">agent</span>
      </span>
    );
  }
  if (record.actor_type === 'user') {
    return (
      <span className="min-w-0">
        <UserName id={record.actor_id} />
      </span>
    );
  }
  return (
    <span className="type-data min-w-0 truncate text-[13px]" title={record.actor_id}>
      {shortId(record.actor_id)}
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
              <span className="absolute left-1/2 top-[4px] h-[2px] w-[9px] -translate-x-1/2 -rotate-[28deg] bg-lamp-fault" />
            </span>
          ) : (
            <span className="absolute left-1/2 top-0 h-[10px] w-[2px] -translate-x-1/2 bg-ink-muted/35" />
          )
        ) : null}
        <span
          className={cn(
            'absolute left-1/2 top-[9px] h-[9px] w-[9px] -translate-x-1/2 rounded-full border-2',
            broken
              ? 'border-lamp-fault bg-lamp-fault'
              : selected
                ? 'border-cobalt bg-cobalt'
                : 'border-ink-muted/60 bg-panel',
          )}
        />
        <span className="absolute bottom-0 left-1/2 top-[18px] w-[2px] -translate-x-1/2 bg-ink-muted/35" />
      </div>

      <div className="min-w-0 pb-2.5">
        <p
          className={cn(
            'flex h-[14px] items-center gap-1.5 text-[11px] leading-none',
            broken ? 'text-lamp-fault' : 'text-ink-muted',
          )}
        >
          {connected ? (
            <>
              <span className="type-data" title={record.prev_hash}>
                {broken ? 'broken link' : `link ${shortHash(record.prev_hash)}`}
              </span>
              {!broken && !linkMatches ? (
                <span className="text-lamp-fault">does not match the entry above</span>
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
            'mt-1 w-full rounded-[6px] border bg-panel px-3 py-2 text-left transition-colors bay-shadow',
            'hover:border-ink-muted focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-cobalt',
            broken ? 'border-lamp-fault' : selected ? 'border-cobalt' : 'border-rail',
          )}
        >
          <span className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
            <span className="type-data text-[12px] text-ink-muted">seq {record.seq}</span>
            <span className="type-data text-[12px] text-ink-muted">
              {localTime(record.time)}
            </span>
            <span className="min-w-0 flex-1 text-[13px] text-ink">
              <RowActor record={record} />
            </span>
            <span className="inline-flex items-center gap-1.5 whitespace-nowrap text-[12px] text-ink-muted">
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
          <span className="mt-1 flex flex-wrap gap-x-3 gap-y-0.5 text-[12px] text-ink-muted">
            <span className="type-data">{record.operation}</span>
            {record.target_type || record.target_id ? (
              <span>
                {record.target_type ?? 'target'}
                {record.target_id ? (
                  <span className="type-data ml-1" title={record.target_id}>
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
