import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import {
  approveApproval,
  denyApproval,
  getListApprovalsQueryKey,
} from '../api/generated/approvals/approvals';
import { getListGrantsQueryKey } from '../api/generated/grants/grants';
import type { Approval } from '../api/generated/model';
import { isApiProblem } from '../api/fetcher';
import { approvalConfirm, decisionBody, type Decision } from '../lib/approvals';
import { shortId } from '../lib/agency-format';
import { problemView, type ProblemView } from '../lib/problem';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';
import { Field } from './ui/field';
import { ProblemBanner } from './ui/problem-banner';
import { Textarea } from './ui/textarea';

/** A 409 says exactly this: the request moved on without this human. */
const CONFLICT_SENTENCE = 'Someone already decided this request, or it expired.';

/**
 * The deciding human's controls: an optional note, Approve and Deny, each behind
 * a confirm step that states what happens. Rendered only for an owner or
 * operator, so a viewer or an agent never gets a decision body computed.
 */
export function ApprovalDecision({
  approval,
  onDecided,
  onConflict,
}: {
  approval: Approval;
  onDecided: (kind: Decision, decided: Approval) => void;
  /** The row is about to leave the queue, so the screen says what happened. */
  onConflict: (view: ProblemView) => void;
}) {
  const queryClient = useQueryClient();
  const [note, setNote] = useState('');
  const [confirm, setConfirm] = useState<Decision | null>(null);
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const noteId = `decision-note-${approval.id}`;

  const decide = useMutation({
    mutationFn: (kind: Decision) =>
      kind === 'approve'
        ? approveApproval(approval.id, decisionBody(note))
        : denyApproval(approval.id, decisionBody(note)),
    onSuccess: (decided, kind) => {
      setProblem(null);
      setConfirm(null);
      setNote('');
      void queryClient.invalidateQueries({ queryKey: getListApprovalsQueryKey() });
      void queryClient.invalidateQueries({ queryKey: getListGrantsQueryKey() });
      onDecided(kind, decided);
    },
    onError: (error) => {
      const view = problemView(error, 'decide this request');
      const conflict = isApiProblem(error) && error.code === 'conflict';
      const gone = isApiProblem(error) && error.code === 'not_found';
      if (conflict || gone) {
        onConflict(conflict ? { ...view, message: CONFLICT_SENTENCE } : view);
        setConfirm(null);
        setProblem(null);
        void queryClient.invalidateQueries({ queryKey: getListApprovalsQueryKey() });
        return;
      }
      setProblem(view);
    },
  });

  const trimmed = note.trim();

  return (
    <div className="border-t border-line px-4 py-4 sm:px-5">
      {problem ? <ProblemBanner view={problem} className="mb-3" /> : null}

      <Field
        label="Note (optional)"
        htmlFor={noteId}
        hint="Recorded with the decision and in the audit log."
      >
        <Textarea
          id={noteId}
          value={note}
          maxLength={2000}
          placeholder="Why you decided this way"
          onChange={(event) => setNote(event.target.value)}
        />
      </Field>

      <div className="mt-3 flex flex-wrap gap-2">
        <Button
          variant="primary"
          disabled={decide.isPending}
          onClick={() => {
            setProblem(null);
            setConfirm('approve');
          }}
        >
          Approve
        </Button>
        <Button
          variant="danger"
          disabled={decide.isPending}
          onClick={() => {
            setProblem(null);
            setConfirm('deny');
          }}
        >
          Deny
        </Button>
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
              {trimmed !== '' ? (
                <p className="border-l-2 border-line pl-3 text-[13px] leading-snug text-ink">
                  Your note: {trimmed}
                </p>
              ) : null}
              <div className="mt-1 flex flex-wrap justify-end gap-2 border-t border-line pt-4">
                <Button
                  variant="default"
                  disabled={decide.isPending}
                  onClick={() => setConfirm(null)}
                >
                  Cancel
                </Button>
                <Button
                  variant={confirm === 'approve' ? 'primary' : 'danger-solid'}
                  disabled={decide.isPending}
                  onClick={() => decide.mutate(confirm)}
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
