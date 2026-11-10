import { useInfiniteQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getRouteApi, useNavigate } from '@tanstack/react-router';
import { useEffect, useMemo, useState } from 'react';
import {
  getListAuditQueryKey,
  listAudit,
  verifyAudit,
} from '../api/generated/audit/audit';
import type {
  AuditRecord,
  AuditVerificationRequest,
  AuditVerificationResult,
  ListAuditParams,
} from '../api/generated/model';
import { Link2 } from 'lucide-react';
import { ActivityRow, ReadsRow } from '../components/activity-row';
import { AuditFilters } from '../components/audit-filters';
import { AuditSheet } from '../components/audit-sheet';
import { ChainStrip } from '../components/chain-strip';
import { Button } from '../components/ui/button';
import { ProblemBanner } from '../components/ui/problem-banner';
import { groupByDay, groupReads, recordTargets } from '../lib/activity';
import { isConnectorBroken } from '../lib/audit';
import { problemView, type ProblemView } from '../lib/problem';
import { useNow } from '../lib/use-now';
import { useKeptNames, useRecordContext } from '../lib/use-record-context';
import { useSession } from '../session';
import { LoginScreen } from './login-screen';
import { EmptyState } from '../components/empty-state';
import { PageSkeleton, SkeletonRows } from '../components/skeleton';

const activityRoute = getRouteApi('/shell/activity');

export function AuditScreen() {
  const { user, loading, unauthenticated } = useSession();
  const search = activityRoute.useSearch();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const records = useRecordContext();
  const now = useNow(true, 60_000);
  const [verifyResult, setVerifyResult] = useState<AuditVerificationResult | null>(null);
  const [checkedAt, setCheckedAt] = useState<number | null>(null);
  const [verifyProblem, setVerifyProblem] = useState<ProblemView | null>(null);
  const [scrollTo, setScrollTo] = useState<number | null>(null);

  // Newest first (ADR 0283): the feed opens at the latest record and each
  // page's cursor continues toward older ones. The order is part of the query
  // key, so a cursor is never reused with another order or other filters.
  const filters = useMemo<ListAuditParams>(
    () => ({
      actor_id: search.actor_id,
      target_id: search.target_id,
      result: search.result,
      incident_id: search.incident_id,
      order: 'desc',
      limit: 50,
    }),
    [search.actor_id, search.target_id, search.result, search.incident_id],
  );

  const auditQuery = useInfiniteQuery({
    queryKey: getListAuditQueryKey(filters),
    queryFn: ({ pageParam }) =>
      pageParam ? listAudit({ ...filters, cursor: pageParam }) : listAudit(filters),
    initialPageParam: null as string | null,
    getNextPageParam: (lastPage) => lastPage.next_cursor,
    enabled: user !== undefined,
  });

  const verify = useMutation({
    mutationFn: (body: AuditVerificationRequest) => verifyAudit(body),
    onSuccess: (result) => {
      setVerifyProblem(null);
      setVerifyResult(result);
      setCheckedAt(Date.now());
      void queryClient.invalidateQueries({ queryKey: getListAuditQueryKey() });
    },
    onError: (error) => {
      setVerifyResult(null);
      setVerifyProblem(problemView(error, 'verify the chain'));
    },
  });

  const entries = useMemo(
    () => auditQuery.data?.pages.flatMap((page) => page.items) ?? [],
    [auditQuery.data],
  );
  // Each day's runs of the same read fold into one row (an agent that polls).
  const days = useMemo(
    () => groupByDay(entries, now).map((day) => ({ ...day, items: groupReads(day.records) })),
    [entries, now],
  );
  const [openRuns, setOpenRuns] = useState<ReadonlySet<string>>(() => new Set());
  const toggleRun = (key: string) =>
    setOpenRuns((current) => {
      const next = new Set(current);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  // A deleted app, VM or project keeps the name its creation recorded.
  const targets = useMemo(() => recordTargets(entries, records), [entries, records]);
  const ctx = useKeptNames(records, targets, entries);

  useEffect(() => {
    if (scrollTo === null) return;
    const element = document.getElementById(`audit-entry-${scrollTo}`);
    if (!element) return;
    element.scrollIntoView({ block: 'center' });
    setScrollTo(null);
  }, [scrollTo, entries]);

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-6xl px-4 py-8 sm:px-6">
        <PageSkeleton />
      </main>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  const listError =
    auditQuery.error !== null && auditQuery.error !== undefined
      ? problemView(auditQuery.error, 'read the audit log')
      : null;

  const hasFilters =
    search.actor_id !== undefined ||
    search.target_id !== undefined ||
    search.result !== undefined ||
    search.incident_id !== undefined;

  const openEntry = (seq: number) => {
    void navigate({ to: '/activity', search: { ...search, seq } });
  };

  const closeEntry = () => {
    void navigate({ to: '/activity', search: { ...search, seq: undefined } });
  };

  /**
   * Open a failed entry and make sure it is on screen: keep loading older
   * pages until the record is present, then scroll to its node on the rail.
   */
  const openLoadedEntry = async (seq: number) => {
    openEntry(seq);
    let pages = auditQuery.data?.pages ?? [];
    let hasNext = auditQuery.hasNextPage;
    let guard = 0;
    while (
      !pages.some((page) => page.items.some((record) => record.seq === seq)) &&
      hasNext &&
      guard < 100
    ) {
      const next = await auditQuery.fetchNextPage();
      pages = next.data?.pages ?? pages;
      hasNext = next.hasNextPage;
      guard += 1;
    }
    setScrollTo(seq);
  };

  const brokenAtSeq =
    verifyResult && !verifyResult.ok ? (verifyResult.broken_at_seq ?? null) : null;

  const runVerify = (body: AuditVerificationRequest) => {
    setVerifyResult(null);
    setVerifyProblem(null);
    verify.mutate(body);
  };

  return (
    <main className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 sm:py-8">
      <div className="page-h">
        <h1>Activity</h1>
        <span className="flex-1" />
        <Button busy={auditQuery.isRefetching && !auditQuery.isFetchingNextPage} variant="ghost" onClick={() => void auditQuery.refetch()} disabled={auditQuery.isFetching}>
          {auditQuery.isFetching ? 'Refreshing…' : 'Refresh'}
        </Button>
        <Button busy={verify.isPending} variant="default" onClick={() => runVerify({})} disabled={verify.isPending}>
          <Link2 size={15} strokeWidth={1.8} aria-hidden="true" />
          {verify.isPending ? 'Verifying…' : 'Verify the chain'}
        </Button>
      </div>

      <ChainStrip
        result={verifyResult}
        checkedAt={checkedAt}
        problem={verifyProblem}
        pending={verify.isPending}
        onRun={runVerify}
        onOpenEntry={(seq) => void openLoadedEntry(seq)}
      />

      <AuditFilters search={search} />

      {listError ? <ProblemBanner view={listError} className="mt-4" /> : null}

      <section aria-label="Activity" className="mt-2 min-w-0">
        {auditQuery.isPending ? (
          <SkeletonRows label="Loading activity" rows={4} className="mt-3" />
        ) : entries.length === 0 ? (
          <EmptyState
            title={hasFilters ? 'Nothing recorded matches these filters' : 'Nothing recorded yet'}
            className="mt-3"
            action={
              hasFilters ? (
                <Button variant="default" onClick={() => void navigate({ to: '/activity', search: {} })}>
                  Clear filters
                </Button>
              ) : undefined
            }
          >
            {hasFilters
              ? 'Clear the filters to see every entry again.'
              : 'Every call by people and agents will appear here, in the order it happened.'}
          </EmptyState>
        ) : (
          days.map((day) => (
            <div key={`${day.label}-${day.records[0]?.seq}`}>
              <h2 className="day">{day.label}</h2>
              <ol className="feed">
                {day.items.map((item) => {
                  const row = (record: AuditRecord, nested = false) => (
                    <ActivityRow
                      key={record.seq}
                      record={record}
                      ctx={ctx}
                      selected={search.seq === record.seq}
                      broken={isConnectorBroken(brokenAtSeq, record.seq)}
                      onOpen={() => openEntry(record.seq)}
                      nested={nested}
                    />
                  );
                  if (item.kind === 'one') return row(item.record);
                  // A run opens by itself when it holds the open record or the broken link.
                  const held = item.records.some(
                    (record) => search.seq === record.seq || isConnectorBroken(brokenAtSeq, record.seq),
                  );
                  return (
                    <ReadsRow
                      key={item.key}
                      records={item.records}
                      ctx={ctx}
                      open={held || openRuns.has(item.key)}
                      onToggle={() => toggleRun(item.key)}
                    >
                      {item.records.map((record) => row(record, true))}
                    </ReadsRow>
                  );
                })}
              </ol>
            </div>
          ))
        )}

        <div className="mt-3 flex flex-wrap items-center gap-3 sm:pl-[34px]">
          {auditQuery.hasNextPage ? (
            <Button
              busy={auditQuery.isFetchingNextPage}
              variant="default"
              onClick={() => void auditQuery.fetchNextPage()}
              disabled={auditQuery.isFetchingNextPage}
            >
              {auditQuery.isFetchingNextPage ? 'Loading older…' : 'Load older'}
            </Button>
          ) : entries.length > 0 ? (
            <p className="text-[13px] text-ink-2">
              {hasFilters ? 'Nothing older matches these filters.' : 'Nothing older. The log starts here.'}
            </p>
          ) : null}
        </div>
      </section>

      {search.seq !== undefined ? <AuditSheet seq={search.seq} ctx={ctx} onClose={closeEntry} /> : null}
    </main>
  );
}
