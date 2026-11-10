import { getRouteApi, Link } from '@tanstack/react-router';
import { useState } from 'react';
import { useGetApproval, useListApprovals } from '../api/generated/approvals/approvals';
import type { Approval } from '../api/generated/model';
import { expiryWords } from '../lib/agency-format';
import { relativeTime } from '../lib/app-status';
import type { RecordContext } from '../lib/activity';
import {
  approvalTitle,
  approvalWords,
  askState,
  decidedAt,
  decidedNewestFirst,
  pendingByExpiry,
  type Decision,
} from '../lib/approvals';
import { canDecideApprovals } from '../lib/capabilities';
import { problemView, type ProblemView } from '../lib/problem';
import { useNow } from '../lib/use-now';
import { useKeptNames, useRecordContext } from '../lib/use-record-context';
import { cn } from '../lib/utils';
import { useSession } from '../session';
import { ApprovalDetail } from '../components/approval-detail';
import { Avatar } from '../components/avatar';
import { AgentName, UserName } from '../components/identity-name';
import { ProblemBanner } from '../components/ui/problem-banner';
import { LoginScreen } from './login-screen';
import { EmptyState } from '../components/empty-state';
import { PageSkeleton, SkeletonRows } from '../components/skeleton';

const approvalsRoute = getRouteApi('/shell/approvals');

interface DecisionNotice {
  kind: Decision;
  approval: Approval;
}

const linkClass =
  'text-accent underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent';

/** One request in the inbox list: the agent, what it asks in two words, and its clock or its outcome. */
function ApprovalItem({
  approval,
  selected,
  ctx,
  now,
}: {
  approval: Approval;
  selected: boolean;
  ctx: RecordContext;
  now: number;
}) {
  const left = expiryWords(approval.expires_at, now);
  const name = (type: string, id: string) => ctx.name?.(type, id);
  return (
    <li>
      <Link
        to="/approvals"
        search={{ approval: approval.id }}
        className={cn('li', selected && 'li-on')}
        aria-current={selected ? 'true' : undefined}
      >
        <Avatar kind="agent" seed={approval.agent_id} />
        <span className="li-t">
          <b>{approvalTitle(approval, name, askState(approval, ctx))}</b>
          <small>
            <AgentName id={approval.agent_id} link={false} />
            {' · '}
            {approval.status === 'pending' ? (
              left.startsWith('in ') ? `${left.slice(3)} left` : 'expiring'
            ) : approval.status === 'expired' ? (
              `expired ${relativeTime(decidedAt(approval), now)}`
            ) : (
              <>
                {approvalWords[approval.status].toLowerCase()}
                {approval.decided_by ? (
                  <>
                    {' by '}
                    <UserName id={approval.decided_by} />
                  </>
                ) : null}
                {` · ${relativeTime(decidedAt(approval), now)}`}
              </>
            )}
          </small>
        </span>
      </Link>
    </li>
  );
}

/**
 * The approvals inbox: requests that wait for a person first, soonest expiry
 * on top, then what was decided. One request is open on the right with its
 * exact call, its clock and the decision. Only an owner or operator decides.
 */
export function ApprovalsScreen() {
  const { me, loading, unauthenticated } = useSession();
  const search = approvalsRoute.useSearch();
  const canDecide = canDecideApprovals(me);
  const records = useRecordContext();
  const now = useNow(true, 30_000);
  const [notice, setNotice] = useState<DecisionNotice | null>(null);
  const [decisionProblem, setDecisionProblem] = useState<ProblemView | null>(null);

  const enabled = me !== undefined;
  const pending = useListApprovals(
    { status: 'pending' },
    { query: { enabled, refetchInterval: 5_000, retry: false } },
  );
  const approved = useListApprovals(
    { status: 'approved' },
    { query: { enabled, refetchInterval: 10_000, retry: false } },
  );
  const denied = useListApprovals(
    { status: 'denied' },
    { query: { enabled, refetchInterval: 10_000, retry: false } },
  );
  const expired = useListApprovals(
    { status: 'expired' },
    { query: { enabled, refetchInterval: 10_000, retry: false } },
  );

  const waiting = pendingByExpiry(pending.data?.items ?? []);
  const decided = decidedNewestFirst([
    ...(approved.data?.items ?? []),
    ...(denied.data?.items ?? []),
    ...(expired.data?.items ?? []),
  ]);
  const listed = [...waiting, ...decided];
  const chosenId = search.approval ?? waiting[0]?.id ?? decided[0]?.id;
  const listedChoice = listed.find((approval) => approval.id === chosenId);
  // A link to a request the lists do not hold (an old one) still opens it.
  const direct = useGetApproval(search.approval ?? '', {
    query: { enabled: enabled && search.approval !== undefined && listedChoice === undefined && !pending.isPending, retry: false },
  });
  const chosen = listedChoice ?? direct.data;
  // A request about an app deleted since still says its name.
  const ctx = useKeptNames(
    records,
    [...listed, ...(direct.data ? [direct.data] : [])].map((approval) => approval.target),
  );

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-6xl px-4 py-8 sm:px-6">
        <PageSkeleton />
      </main>
    );
  }

  if (unauthenticated || !me) return <LoginScreen />;

  const listError = pending.error ?? approved.error ?? denied.error ?? expired.error;
  const errorView = listError ? problemView(listError, 'see approvals') : null;
  const loadingDecided = approved.isPending || denied.isPending || expired.isPending;

  return (
    <main className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 sm:py-8">
      <div className="page-h">
        <h1>Approvals</h1>
      </div>
      <p className="-mt-3 mb-4 max-w-2xl text-[13px] leading-snug text-ink-2">
        Agents ask before any delete. Approving lets that one call run once, within 10 minutes.
      </p>

      {notice ? (
        <div role="status" aria-live="polite" className="box mb-4 px-3.5 py-2.5 text-[14px] leading-snug text-ink">
          {notice.kind === 'approve' ? (
            <>
              Approved. The one-time grant is valid for 10 minutes.{' '}
              {notice.approval.grant_id ? (
                <Link
                  to="/agents"
                  search={{ agent: notice.approval.agent_id, tab: 'grants' }}
                  className={linkClass}
                >
                  See the grant
                </Link>
              ) : null}
            </>
          ) : (
            <>
              Denied <AgentName id={notice.approval.agent_id} link={false} />
              &rsquo;s request. Its retry will be refused.
            </>
          )}
        </div>
      ) : null}

      {errorView ? <ProblemBanner view={errorView} className="mb-4" /> : null}
      {decisionProblem ? <ProblemBanner view={decisionProblem} className="mb-4" /> : null}

      <div className="split" data-open={search.approval ? 'detail' : 'list'}>
        <nav className="split-list" aria-label="Requests">
          <h2 className="list-h">Waiting</h2>
          {pending.isPending ? (
            <SkeletonRows label="Loading waiting requests" rows={1} className="mr-4" />
          ) : waiting.length === 0 ? (
            <EmptyState title="Nothing waits" className="mr-4">
              When an agent asks to delete something, the request shows up here for a person to approve or deny.
            </EmptyState>
          ) : (
            <ul>
              {waiting.map((approval) => (
                <ApprovalItem key={approval.id} approval={approval} selected={approval.id === chosen?.id} ctx={ctx} now={now} />
              ))}
            </ul>
          )}

          <h2 className="list-h mt-4">Decided</h2>
          {decided.length === 0 && loadingDecided ? (
            <SkeletonRows label="Loading decided requests" rows={2} className="mr-4" />
          ) : decided.length === 0 ? (
            <EmptyState title="Nothing decided yet" className="mr-4">
              Approved, denied and expired requests stay here, so you can see what an agent was allowed to do.
            </EmptyState>
          ) : (
            <ul>
              {decided.map((approval) => (
                <ApprovalItem key={approval.id} approval={approval} selected={approval.id === chosen?.id} ctx={ctx} now={now} />
              ))}
            </ul>
          )}
        </nav>

        <section className="split-detail" aria-label="Request">
          {chosen ? (
            <ApprovalDetail
              key={chosen.id}
              approval={chosen}
              ctx={ctx}
              canDecide={canDecide}
              onConflict={(view) => {
                setNotice(null);
                setDecisionProblem(view);
              }}
              onDecided={(kind, decidedApproval) => {
                setDecisionProblem(null);
                setNotice({ kind, approval: decidedApproval });
              }}
            />
          ) : search.approval && direct.isError ? (
            <ProblemBanner view={problemView(direct.error, 'see this request')} />
          ) : pending.isPending ? null : (
            <p className="text-[14px] text-ink-2">Nothing to open yet.</p>
          )}
        </section>
      </div>
    </main>
  );
}
