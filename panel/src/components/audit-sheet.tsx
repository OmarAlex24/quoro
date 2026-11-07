// SPDX-License-Identifier: AGPL-3.0-only
import * as DialogPrimitive from '@radix-ui/react-dialog';
import { X } from 'lucide-react';
import { useGetAuditRecord } from '../api/generated/audit/audit';
import { recordSentence, resultTag, type RecordContext } from '../lib/activity';
import { shortHash } from '../lib/agency-format';
import { ActorAvatar, Sentence } from './activity-row';
import { AuditRecordBody } from './audit-record';
import { Tag } from './ui/tag';

/**
 * One audit record opened from the feed, in a sheet over it: the sentence on
 * top, then every field of the record as `getAuditRecord` returns it.
 */
export function AuditSheet({ seq, ctx, onClose }: { seq: number; ctx: RecordContext; onClose: () => void }) {
  const record = useGetAuditRecord(seq);
  const data = record.data;
  const tag = data ? resultTag(data, ctx) : null;

  return (
    <DialogPrimitive.Root open onOpenChange={(open) => (open ? undefined : onClose())}>
      <DialogPrimitive.Portal>
        <DialogPrimitive.Overlay className="dialog-overlay fixed inset-0 z-40 bg-scrim" />
        <DialogPrimitive.Content className="sheet" aria-describedby={undefined}>
          <div className="sheet-head">
            <DialogPrimitive.Title className="sheet-title">Entry {seq}</DialogPrimitive.Title>
            {data ? (
              <span className="type-code text-[11px] text-ink-3" title={data.hash}>
                {shortHash(data.hash)}
              </span>
            ) : null}
            <span className="flex-1" />
            <DialogPrimitive.Close className="iconbtn" aria-label="Close">
              <X strokeWidth={1.7} aria-hidden="true" />
            </DialogPrimitive.Close>
          </div>
          <div className="sheet-body">
            <div className="grid gap-4 px-5 pb-6">
              {data ? (
                <p className="rec-lead">
                  <ActorAvatar record={data} />
                  <span className="min-w-0">
                    <Sentence segments={recordSentence(data, ctx)} />
                    {tag ? (
                      <>
                        {' '}
                        <Tag tone={tag.tone}>{tag.words}</Tag>
                      </>
                    ) : null}
                  </span>
                </p>
              ) : null}
              <AuditRecordBody seq={seq} />
            </div>
          </div>
        </DialogPrimitive.Content>
      </DialogPrimitive.Portal>
    </DialogPrimitive.Root>
  );
}
