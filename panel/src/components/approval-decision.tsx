import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useState, type ReactNode } from 'react';
import {
  approveApproval,
  denyApproval,
  getListApprovalsQueryKey,
} from '../api/generated/approvals/approvals';
import { getListAuditQueryKey } from '../api/generated/audit/audit';
import { getListGrantsQueryKey } from '../api/generated/grants/grants';
import type { Approval } from '../api/generated/model';
import { isApiProblem } from '../api/fetcher';
import {
  approvalConfirm,
  approveWords,
  decisionBody,
  isDestructiveScope,
  type Decision,
} from '../lib/approvals';
import { shortId } from '../lib/agency-format';
import { problemView, type ProblemView } from '../lib/problem';
import { cn } from '../lib/utils';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';
import { Field } from './ui/field';
import { ProblemBanner } from './ui/problem-banner';
import { Textarea } from './ui/textarea';

/** A 409 says exactly this: the request moved on without this human. */
const CONFLICT_SENTENCE = 'Someone already decided this request, or it expired.';

export interface DecisionCallbacks {
  onDecided: (kind: Decision, decided: Approval) => void;
  /** The request is about to leave the queue, so the screen says what happened. */
  onConflict: (view: ProblemView) => void;
}

/**
 * The decision itself: approve or deny one request with an optional note.
 * A 409 or 404 means the request moved on without this person; the caller
 * hears it through `onConflict` and the lists refresh.
 */
export function useApprovalDecision(approval: Approval, { onDecided, onConflict }: DecisionCallbacks) {
  const queryClient = useQueryClient();
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const decide = useMutation({
    mutationFn: ({ kind, note }: { kind: Decision; note: string }) =>
      kind === 'approve'
        ? approveApproval(approval.id, decisionBody(note))
        : denyApproval(approval.id, decisionBody(note)),
    onSuccess: (decided, { kind }) => {
      setProblem(null);
      void queryClient.invalidateQueries({ queryKey: getListApprovalsQueryKey() });
      void queryClient.invalidateQueries({ queryKey: getListGrantsQueryKey() });
      void queryClient.invalidateQueries({ queryKey: getListAuditQueryKey() });
      onDecided(kind, decided);
    },
    onError: (error) => {
      const view = problemView(error, 'decide this request');
      const conflict = isApiProblem(error) && error.code === 'conflict';
      const gone = isApiProblem(error) && error.code === 'not_found';
      if (conflict || gone) {
        onConflict(conflict ? { ...view, message: CONFLICT_SENTENCE } : view);
        setProblem(null);
        void queryClient.invalidateQueries({ queryKey: getListApprovalsQueryKey() });
        return;
      }
      setProblem(view);
    },
  });

  return { decide, problem, setProblem };
}

/**
 * The deciding human's controls: an optional note, Approve and Deny, each behind
 * a confirm step that states what happens. Approving a destructive action is
 * the red button and says what it approves; denying is the neutral one.
 * Rendered only for an owner or operator, so a viewer or an agent never gets a
 * decision body computed.
 */
export function ApprovalDecision({
  approval,
  onDecided,
  onConflict,
  withNote = true,
  aside,
  className,
}: DecisionCallbacks & {
  approval: Approval;
  /** The note field; the inline decision in a story leaves it to the confirm step. */
  withNote?: boolean;
  /** Something at the far end of the button row, such as a link to the inbox. */
  aside?: ReactNode;
  className?: string;
}) {
  const [note, setNote] = useState('');
  const [confirm, setConfirm] = useState<Decision | null>(null);
  const { decide, problem, setProblem } = useApprovalDecision(approval, {
    onDecided: (kind, decided) => {
      setConfirm(null);
      setNote('');
      onDecided(kind, decided);
    },
    onConflict: (view) => {
      setConfirm(null);
      onConflict(view);
    },
  });
  const noteId = `decision-note-${approval.id}`;
  const destructive = isDestructiveScope(approval.scope);
  const approveLabel = approveWords(approval.scope);
  const trimmed = note.trim();

  return (
    <div className={cn('grid gap-3', className)}>
      {problem && confirm === null ? <ProblemBanner view={problem} /> : null}

      {withNote ? (
        <Field label="Note (optional)" htmlFor={noteId} hint="Recorded with the decision in the audit log.">
          <Textarea
            id={noteId}
            value={note}
            maxLength={2000}
            placeholder="Why you decided this way"
            onChange={(event) => setNote(event.target.value)}
          />
        </Field>
      ) : null}

      <div className="flex flex-wrap items-center gap-2.5">
        <Button
          variant={destructive ? 'danger-solid' : 'primary'}
          disabled={decide.isPending}
          onClick={() => {
            setProblem(null);
            setConfirm('approve');
          }}
        >
          {approveLabel}
        </Button>
        <Button
          variant="default"
          disabled={decide.isPending}
          onClick={() => {
            setProblem(null);
            setConfirm('deny');
          }}
        >
          Deny
        </Button>
        {aside ? <span className="ml-auto">{aside}</span> : null}
      </div>

      <Dialog
        open={confirm !== null}
        onOpenChange={(open) => {
          if (!open) setConfirm(null);
        }}
      >
        {confirm ? (
          <DialogContent
            title={confirm === 'approve' ? 'Approve this request?' : 'Deny this request?'}
            description={approvalConfirm(confirm, approval)}
          >
            <div className="flex flex-col gap-3">
              {problem ? <ProblemBanner view={problem} /> : null}
              <p className="type-data text-[13px] leading-snug text-ink">
                <span className="type-code">{approval.scope}</span> on {approval.target.type}{' '}
                <span className="type-code" title={approval.target.id}>{shortId(approval.target.id)}</span>
              </p>
              {withNote ? (
                trimmed !== '' ? (
                  <p className="border-l-2 border-line pl-3 text-[13px] leading-snug text-ink">
                    Your note: {trimmed}
                  </p>
                ) : null
              ) : (
                <Field label="Note (optional)" htmlFor={noteId} hint="Recorded with the decision in the audit log.">
                  <Textarea
                    id={noteId}
                    value={note}
                    maxLength={2000}
                    placeholder="Why you decided this way"
                    onChange={(event) => setNote(event.target.value)}
                  />
                </Field>
              )}
              <div className="mt-1 flex flex-wrap justify-end gap-2 border-t border-line pt-4">
                <Button
                  variant="default"
                  disabled={decide.isPending}
                  onClick={() => setConfirm(null)}
                >
                  Cancel
                </Button>
                <Button
                  variant={confirm === 'approve' && destructive ? 'danger-solid' : 'primary'}
                  disabled={decide.isPending}
                  onClick={() => decide.mutate({ kind: confirm, note })}
                >
                  {decide.isPending
                    ? 'Sending…'
                    : confirm === 'approve'
                      ? 'Approve and issue grant'
                      : 'Deny request'}
                </Button>
              </div>
            </div>
          </DialogContent>
        ) : null}
      </Dialog>
    </div>
  );
}
