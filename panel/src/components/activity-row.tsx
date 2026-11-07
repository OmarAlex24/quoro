// SPDX-License-Identifier: AGPL-3.0-only
import type { AuditRecord } from '../api/generated/model';
import { clockTime, dayLabel, recordSentence, resultTag, segmentsText, type RecordContext, type Segment } from '../lib/activity';
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
}: {
  record: AuditRecord;
  ctx: RecordContext;
  selected: boolean;
  /** Verification found the link into this record broken. */
  broken: boolean;
  onOpen: () => void;
  /** Say the day too, for a list that is not grouped by day. */
  withDay?: boolean;
}) {
  const day = withDay ? dayLabel(record.time) : null;
  const tag = resultTag(record, ctx);
  const sentence = recordSentence(record, ctx);
  return (
    <li
      id={`audit-entry-${record.seq}`}
      className={cn('fr', selected && 'fr-on', broken && 'fr-broken')}
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
