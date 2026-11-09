// SPDX-License-Identifier: AGPL-3.0-only
import { ChevronRight } from 'lucide-react';
import type { ReactNode } from 'react';
import type { AuditRecord } from '../api/generated/model';
import {
  clockTime,
  dayLabel,
  readsSentence,
  readsWhen,
  recordSentence,
  resultTag,
  segmentsText,
  type RecordContext,
  type Segment,
} from '../lib/activity';
import { cn } from '../lib/utils';
import { Avatar } from './avatar';
import { UserAvatar } from './identity-name';
import { Tag } from './ui/tag';

/** A sentence from the activity engine: names bold, operations and scopes in code, the channel faint. */
export function Sentence({ segments }: { segments: readonly Segment[] }) {
  return (
    <>
      {segments.map((segment, index) =>
        segment.code ? (
          <code key={index} className="type-code">
            {segment.text}
          </code>
        ) : segment.strong ? (
          <b key={index}>{segment.text}</b>
        ) : segment.faint ? (
          <span key={index} className="text-ink-3">
            {segment.text}
          </span>
        ) : (
          <span key={index}>{segment.text}</span>
        ),
      )}
    </>
  );
}

/** Who acted, at a glance: an agent's square, a person's initials, the Quoro mark. */
export function ActorAvatar({ record }: { record: Pick<AuditRecord, 'actor_type' | 'actor_id'> }) {
  switch (record.actor_type) {
    case 'agent':
      return <Avatar kind="agent" seed={record.actor_id} />;
    case 'user':
      return <UserAvatar id={record.actor_id} />;
    default:
      return <Avatar kind="system" />;
  }
}

/**
 * One record in the feed: its place on the chain, who acted, what they did in
 * a sentence, the result and the clock time. The whole row opens the record.
 */
export function ActivityRow({
  record,
  ctx,
  selected,
  broken,
  onOpen,
  withDay = false,
  nested = false,
}: {
  record: AuditRecord;
  ctx: RecordContext;
  selected: boolean;
  /** Verification found the link into this record broken. */
  broken: boolean;
  onOpen: () => void;
  /** Say the day too, for a list that is not grouped by day. */
  withDay?: boolean;
  /** One of the records of an opened run of reads. */
  nested?: boolean;
}) {
  const day = withDay ? dayLabel(record.time) : null;
  const tag = resultTag(record, ctx);
  const sentence = recordSentence(record, ctx);
  return (
    <li
      id={`audit-entry-${record.seq}`}
      className={cn('fr', nested && 'fr-sub', selected && 'fr-on', broken && 'fr-broken')}
    >
      <span className="fr-chain" aria-hidden="true">
        <i />
      </span>
      <ActorAvatar record={record} />
      <button
        type="button"
        className="fr-s"
        onClick={onOpen}
        aria-current={selected ? 'true' : undefined}
        aria-label={`Entry ${record.seq}: ${segmentsText(sentence)}`}
      >
        <Sentence segments={sentence} />
      </button>
      <Tag tone={tag.tone} className="fr-tag">
        {tag.words}
      </Tag>
      <time dateTime={record.time} title={record.time} className={cn(day && day !== 'Today' && 'fr-when')}>
        {day && day !== 'Today' ? `${day}, ${clockTime(record.time)}` : clockTime(record.time)}
      </time>
    </li>
  );
}

/**
 * A run of the same read by the same actor, said once: "cleanup-bot read the
 * request to delete scratch 40 times via the API, 14:45–14:46". Its clock
 * shows the newest read, as every row shows its record's time. The row opens
 * to the single records, each of which opens as usual.
 */
export function ReadsRow({
  records,
  ctx,
  open,
  onToggle,
  withDay = false,
  children,
}: {
  records: readonly AuditRecord[];
  ctx: RecordContext;
  open: boolean;
  onToggle: () => void;
  /** Say the day too, for a list that is not grouped by day. */
  withDay?: boolean;
  /** The single records, shown while the run is open. */
  children: ReactNode;
}) {
  const first = records[0];
  if (!first) return null;
  const sentence = readsSentence(records, ctx);
  const when = readsWhen(records);
  const day = withDay ? dayLabel(when.to) : null;
  const seqs = records.map((record) => record.seq);
  return (
    <>
      <li className={cn('fr fr-group', open && 'fr-group-open')}>
        <span className="fr-chain" aria-hidden="true">
          <i />
        </span>
        <ActorAvatar record={first} />
        <button
          type="button"
          className="fr-s"
          onClick={onToggle}
          aria-expanded={open}
          aria-label={`Entries ${Math.min(...seqs)} to ${Math.max(...seqs)}: ${segmentsText(sentence)}. ${open ? 'Hide' : 'Show'} each one.`}
        >
          <Sentence segments={sentence} />
          <ChevronRight className="fr-caret" size={14} strokeWidth={1.8} aria-hidden="true" />
        </button>
        <Tag tone="run" className="fr-tag">
          OK
        </Tag>
        <time dateTime={when.to} title={`${when.from} to ${when.to}`} className={cn(day && day !== 'Today' && 'fr-when')}>
          {day && day !== 'Today' ? `${day}, ${clockTime(when.to)}` : clockTime(when.to)}
        </time>
      </li>
      {open ? children : null}
    </>
  );
}
