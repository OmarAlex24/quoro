import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useRef, useState, type ReactNode, type RefObject } from 'react';
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
  /**
   * Where keyboard focus goes once a decision is sent or the request moved on,
   * since the button that opened the confirm step leaves with the request. The
   * screen's notice or heading; without it, focus goes back to that button.
   */
  focusAfter?: () => HTMLElement | null | undefined;
}

/**
 * The decision itself: approve or deny one request with an optional note.
 * A 409 or 404 means the request moved on without this person; the caller
 * hears it through `onConflict` and the lists refresh.
 */
export function useApprovalDecision(approval: Approval, { onDecided, onConflict }: DecisionCallbacks) {
  const queryClient = useQueryClient();
  const [problem, setProblem] = useState<ProblemView | null>(null);
  /** Whether the confirm step closes because the decision went out or the request moved on, not on Cancel. */
  const settled = useRef(false);

  const decide = useMutation({
    mutationFn: ({ kind, note }: { kind: Decision; note: string }) =>
      kind === 'approve'
        ? approveApproval(approval.id, decisionBody(note))
        : denyApproval(approval.id, decisionBody(note)),
    onSuccess: (decided, { kind }) => {
      setProblem(null);
      settled.current = true;
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
        settled.current = true;
        onConflict(conflict ? { ...view, message: CONFLICT_SENTENCE } : view);
        setProblem(null);
        void queryClient.invalidateQueries({ queryKey: getListApprovalsQueryKey() });
        return;
      }
      setProblem(view);
    },
  });

  return { decide, problem, setProblem, settled };
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
  focusAfter,
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
  const opener = useRef<HTMLButtonElement | null>(null);
  const { decide, problem, setProblem, settled } = useApprovalDecision(approval, {
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
  const open = (kind: Decision, button: HTMLButtonElement) => {
    opener.current = button;
    setProblem(null);
    setConfirm(kind);
  };
  const noteId = `decision-note-${approval.id}`;
  const destructive = isDestructiveScope(approval.scope);
  const approveLabel = approveWords(approval.scope);

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
          onClick={(event) => open('approve', event.currentTarget)}
        >
          {approveLabel}
        </Button>
        <Button
          variant="default"
          disabled={decide.isPending}
          onClick={(event) => open('deny', event.currentTarget)}
        >
          Deny
        </Button>
        {aside ? <span className="ml-auto">{aside}</span> : null}
      </div>

      <DecisionConfirm
        approval={approval}
        confirm={confirm}
        onCancel={() => setConfirm(null)}
        pending={decide.isPending}
        problem={problem}
        noteId={noteId}
        note={note}
        onNote={setNote}
        noteInDialog={!withNote}
        onSend={(kind) => decide.mutate({ kind, note })}
        settled={settled}
        opener={opener}
        focusAfter={focusAfter}
      />
    </div>
  );
}

/**
 * The confirm step of a decision: what happens, the scope and target, the
 * note (asked here when the caller has no note field of its own), Cancel and
 * the button that sends it.
 */
function DecisionConfirm({
  approval,
  confirm,
  onCancel,
  pending,
  problem,
  noteId,
  note,
  onNote,
  noteInDialog,
  onSend,
  settled,
  opener,
  focusAfter,
}: {
  approval: Approval;
  confirm: Decision | null;
  onCancel: () => void;
  pending: boolean;
  problem: ProblemView | null;
  noteId: string;
  note: string;
  onNote: (note: string) => void;
  noteInDialog: boolean;
  onSend: (kind: Decision) => void;
  settled: RefObject<boolean>;
  /** The button that opened this step; Cancel, Escape and Close return focus to it. */
  opener: RefObject<HTMLButtonElement | null>;
  focusAfter?: () => HTMLElement | null | undefined;
}) {
  const destructive = isDestructiveScope(approval.scope);
  const trimmed = note.trim();
  // The dialog has no trigger of its own, so it says where focus goes when it closes.
  const returnFocus = (event: Event) => {
    event.preventDefault();
    const after = settled.current ? focusAfter?.() : null;
    settled.current = false;
    const target = after ?? opener.current;
    if (target?.isConnected) target.focus();
  };
  return (
    <Dialog
      open={confirm !== null}
      onOpenChange={(open) => {
        if (!open) onCancel();
      }}
    >
      {confirm ? (
        <DialogContent
          title={confirm === 'approve' ? 'Approve this request?' : 'Deny this request?'}
          description={approvalConfirm(confirm, approval)}
          onCloseAutoFocus={returnFocus}
        >
          <div className="flex flex-col gap-3">
            {problem ? <ProblemBanner view={problem} /> : null}
            <p className="type-data text-[13px] leading-snug text-ink">
              <span className="type-code">{approval.scope}</span> on {approval.target.type}{' '}
              <span className="type-code" title={approval.target.id}>{shortId(approval.target.id)}</span>
            </p>
            {!noteInDialog ? (
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
                  onChange={(event) => onNote(event.target.value)}
                />
              </Field>
            )}
            <div className="mt-1 flex flex-wrap justify-end gap-2 border-t border-line pt-4">
              <Button variant="default" disabled={pending} onClick={onCancel}>
                Cancel
              </Button>
              <Button
                busy={pending}
                variant={confirm === 'approve' && destructive ? 'danger-solid' : 'primary'}
                disabled={pending}
                onClick={() => onSend(confirm)}
              >
                {pending ? 'Sending…' : confirm === 'approve' ? 'Approve and issue grant' : 'Deny request'}
              </Button>
            </div>
          </div>
        </DialogContent>
      ) : null}
    </Dialog>
  );
}

/**
 * Deny on its own, for a request shown away from the inbox: the same confirm
 * step as the inbox's Deny, with its note, its 409 and 404 handling and its
 * refreshes. Approving stays in the inbox, where the whole request is read.
 * Render it only for someone who may decide (`canDecideApprovals`).
 */
export function DenyButton({
  approval,
  onDecided,
  onConflict,
  focusAfter,
  className,
}: DecisionCallbacks & { approval: Approval; className?: string }) {
  const [note, setNote] = useState('');
  const [confirm, setConfirm] = useState<Decision | null>(null);
  const opener = useRef<HTMLButtonElement | null>(null);
  const { decide, problem, setProblem, settled } = useApprovalDecision(approval, {
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
  return (
    <>
      <Button
        ref={opener}
        variant="default"
        size="sm"
        className={className}
        disabled={decide.isPending}
        onClick={() => {
          setProblem(null);
          setConfirm('deny');
        }}
      >
        Deny
      </Button>
      <DecisionConfirm
        approval={approval}
        confirm={confirm}
        onCancel={() => setConfirm(null)}
        pending={decide.isPending}
        problem={problem}
        noteId={`deny-note-${approval.id}`}
        note={note}
        onNote={setNote}
        noteInDialog
        onSend={(kind) => decide.mutate({ kind, note })}
        settled={settled}
        opener={opener}
        focusAfter={focusAfter}
      />
    </>
  );
}
