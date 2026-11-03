import { Link } from '@tanstack/react-router';
import { useState } from 'react';
import { useListApprovals } from '../api/generated/approvals/approvals';
import type { Approval } from '../api/generated/model';
import { shortId } from '../lib/agency-format';
import {
  DECIDER_ONLY_SENTENCE,
  decidedNewestFirst,
  pendingByExpiry,
  type Decision,
} from '../lib/approvals';
import { canDecideApprovals } from '../lib/capabilities';
import { problemView, type ProblemView } from '../lib/problem';
import { useSession } from '../session';
import { ApprovalPermit } from '../components/approval-permit';
import { ApprovalRow } from '../components/approval-row';
import { AgentName } from '../components/identity-name';
import { ProblemBanner } from '../components/ui/problem-banner';
import { LoginScreen } from './login-screen';

interface DecisionNotice {
  kind: Decision;
  approval: Approval;
}

/**
 * The approvals queue: requests waiting for a human decision, and what was
 * already decided. Only an owner or operator gets the note and the two buttons;
 * everyone else reads the requests and is told once why.
 */
export function ApprovalsScreen() {
  const { me, loading, unauthenticated } = useSession();
  const canDecide = canDecideApprovals(me);
  const [notice, setNotice] = useState<DecisionNotice | null>(null);
  const [decisionProblem, setDecisionProblem] = useState<ProblemView | null>(null);

  const pending = useListApprovals(
    { status: 'pending' },
    { query: { enabled: me !== undefined, refetchInterval: 5_000, retry: false } },
  );
  const approved = useListApprovals(
    { status: 'approved' },
    { query: { enabled: me !== undefined, refetchInterval: 10_000, retry: false } },
  );
  const denied = useListApprovals(
    { status: 'denied' },
    { query: { enabled: me !== undefined, refetchInterval: 10_000, retry: false } },
  );
  const expired = useListApprovals(
    { status: 'expired' },
    { query: { enabled: me !== undefined, refetchInterval: 10_000, retry: false } },
  );

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-6xl px-4 py-8 sm:px-6">
        <p className="text-[15px] text-ink-muted">Loading…</p>
      </main>
    );
  }

  if (unauthenticated || !me) return <LoginScreen />;

  const listError = pending.error ?? approved.error ?? denied.error ?? expired.error;
  const errorView =
    listError !== null && listError !== undefined
      ? problemView(listError, 'see approvals')
      : null;

  const waiting = pendingByExpiry(pending.data?.items ?? []);
  const decided = decidedNewestFirst([
    ...(approved.data?.items ?? []),
    ...(denied.data?.items ?? []),
    ...(expired.data?.items ?? []),
  ]);

  return (
    <main className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 sm:py-8">
      <h1 className="type-display text-[24px] leading-none">Approvals</h1>
      <p className="mt-2 max-w-2xl text-[14px] leading-snug text-ink-muted">
        Agents ask before any delete. Approving lets that one call run once, within 10
        minutes.
      </p>

      {!canDecide ? (
        <p className="mt-4 rounded-[6px] border border-rail bg-panel px-3.5 py-2.5 text-[13px] leading-snug text-ink-muted">
          {DECIDER_ONLY_SENTENCE}
        </p>
      ) : null}

      {notice ? (
        <div
          role="status"
          aria-live="polite"
          className="mt-4 rounded-[6px] border border-rail bg-panel px-3.5 py-2.5"
        >
          {notice.kind === 'approve' ? (
            <p className="text-[14px] leading-snug text-ink">
              {notice.approval.grant_id ? (
                <>
                  Approved. Grant{' '}
                  <span className="type-data" title={notice.approval.grant_id}>
                    {shortId(notice.approval.grant_id)}
                  </span>{' '}
                  is valid for 10 minutes.{' '}
                  <Link
                    to="/grants"
                    search={{ agent_id: notice.approval.agent_id, kind: 'approval' }}
                    className="text-cobalt underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-cobalt"
                  >
                    See the grant
                  </Link>
                </>
              ) : (
                'Approved.'
              )}
            </p>
          ) : (
            <p className="text-[14px] leading-snug text-ink">
              Denied <AgentName id={notice.approval.agent_id} link={false} />
              &rsquo;s request to{' '}
              <span className="type-data">{notice.approval.scope}</span>.
            </p>
          )}
        </div>
      ) : null}

      {errorView ? <ProblemBanner view={errorView} className="mt-4" /> : null}
      {decisionProblem ? <ProblemBanner view={decisionProblem} className="mt-4" /> : null}

      <section className="mt-6">
        <h2 className="type-display-light text-[15px] leading-none text-ink">
          Waiting for a decision
        </h2>

        <div className="mt-3 flex flex-col gap-3">
          {pending.isPending ? (
            <>
              <div className="h-[220px] rounded-[6px] border border-rail bg-panel/60" />
              <div className="h-[220px] rounded-[6px] border border-rail bg-panel/60" />
            </>
          ) : waiting.length === 0 ? (
            <div className="rounded-[6px] border border-dashed border-rail bg-panel/50 px-4 py-10 text-center">
              <p className="text-[15px] text-ink">
                Nothing is waiting. When an agent asks to delete something, it shows up
                here.
              </p>
            </div>
          ) : (
            waiting.map((approval) => (
              <ApprovalPermit
                key={approval.id}
                approval={approval}
                canDecide={canDecide}
                onConflict={setDecisionProblem}
                onDecided={(kind, decidedApproval) => {
                  setDecisionProblem(null);
                  setNotice({ kind, approval: decidedApproval });
                }}
              />
            ))
          )}
        </div>
      </section>

      <section className="mt-8">
        <h2 className="type-display-light text-[15px] leading-none text-ink">Decided</h2>

        {decided.length === 0 ? (
          approved.isPending || denied.isPending || expired.isPending ? (
            <p className="mt-3 text-[13px] text-ink-muted">Loading…</p>
          ) : (
            <p className="mt-3 text-[13px] text-ink-muted">
              Nothing has been decided yet. Approved, denied and expired requests stay
              here.
            </p>
          )
        ) : (
          <ul className="mt-2 flex flex-col divide-y divide-rail border-y border-rail">
            {decided.map((approval) => (
              <ApprovalRow key={approval.id} approval={approval} />
            ))}
          </ul>
        )}
      </section>
    </main>
  );
}
