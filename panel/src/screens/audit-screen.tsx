import { useInfiniteQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getRouteApi, useNavigate } from '@tanstack/react-router';
import { useEffect, useMemo, useState } from 'react';
import {
  getListAuditQueryKey,
  listAudit,
  verifyAudit,
} from '../api/generated/audit/audit';
import type {
  AuditVerificationRequest,
  AuditVerificationResult,
  ListAuditParams,
} from '../api/generated/model';
import { AuditEntryRow } from '../components/audit-chain';
import { AuditCopy } from '../components/audit-copy';
import { AuditFilters } from '../components/audit-filters';
import { AuditRecordPanel } from '../components/audit-record';
import { AuditVerify } from '../components/audit-verify';
import { Button } from '../components/ui/button';
import { ProblemBanner } from '../components/ui/problem-banner';
import { isConnectorBroken } from '../lib/audit';
import { problemView, type ProblemView } from '../lib/problem';
import { useSession } from '../session';
import { LoginScreen } from './login-screen';

const auditRoute = getRouteApi('/audit');

export function AuditScreen() {
  const { user, loading, unauthenticated } = useSession();
  const search = auditRoute.useSearch();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [verifyResult, setVerifyResult] = useState<AuditVerificationResult | null>(null);
  const [verifyProblem, setVerifyProblem] = useState<ProblemView | null>(null);
  const [scrollTo, setScrollTo] = useState<number | null>(null);

  const filters = useMemo<ListAuditParams>(
    () => ({
      actor_id: search.actor_id,
      target_id: search.target_id,
      result: search.result,
      incident_id: search.incident_id,
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
        <p className="text-[15px] text-ink-2">Loading…</p>
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
    void navigate({ to: '/audit', search: { ...search, seq } });
  };

  const closeEntry = () => {
    void navigate({ to: '/audit', search: { ...search, seq: undefined } });
  };

  /**
   * Open a failed entry and make sure it is on screen: keep loading pages
   * until the record is present, then scroll to its node on the rail.
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

  return (
    <main className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 sm:py-8">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <h1 className="type-display text-[24px] leading-none">Audit log</h1>
          <p className="mt-2 max-w-2xl text-[14px] leading-snug text-ink-2">
            Every call, by people and agents, in order. Each entry is chained to the one
            before it, so an edit shows.
          </p>
        </div>
        <Button
          variant="ghost"
          onClick={() => void auditQuery.refetch()}
          disabled={auditQuery.isFetching}
        >
          {auditQuery.isFetching ? 'Refreshing…' : 'Refresh'}
        </Button>
      </div>

      <div className="mt-4 flex flex-col gap-3">
        <AuditVerify
          result={verifyResult}
          problem={verifyProblem}
          pending={verify.isPending}
          onRun={(body) => {
            setVerifyResult(null);
            setVerifyProblem(null);
            verify.mutate(body);
          }}
          onOpenEntry={(seq) => void openLoadedEntry(seq)}
        />
        <AuditFilters search={search} />
        <AuditCopy />
      </div>

      {listError ? <ProblemBanner view={listError} className="mt-4" /> : null}

      <div
        className={
          search.seq !== undefined
            ? 'mt-4 grid items-start gap-4 lg:grid-cols-[minmax(0,1fr)_23rem]'
            : 'mt-4'
        }
      >
        <section aria-label="Audit entries" className="min-w-0">
          {auditQuery.isPending ? (
            <div className="flex flex-col gap-2">
              {[0, 1, 2, 3].map((index) => (
                <div
                  key={index}
                  className="h-[64px] rounded-[10px] border border-line bg-surface/60"
                />
              ))}
            </div>
          ) : entries.length === 0 ? (
            <div className="rounded-[10px] border border-dashed border-line bg-surface/50 px-4 py-10 text-center">
              <p className="text-[15px] text-ink">
                {hasFilters
                  ? 'No audit records match these filters.'
                  : 'Nothing has been recorded yet. Actions will appear here.'}
              </p>
              {hasFilters ? (
                <Button
                  variant="default"
                  className="mt-4"
                  onClick={() => void navigate({ to: '/audit', search: {} })}
                >
                  Clear filters
                </Button>
              ) : null}
            </div>
          ) : (
            <ol className="flex flex-col">
              {entries.map((record, index) => (
                <AuditEntryRow
                  key={record.seq}
                  record={record}
                  above={entries[index - 1]}
                  selected={search.seq === record.seq}
                  broken={isConnectorBroken(brokenAtSeq, record.seq)}
                  onSelect={() => openEntry(record.seq)}
                />
              ))}
            </ol>
          )}

          <div className="mt-3 flex flex-wrap items-center gap-3 pl-[28px]">
            {auditQuery.hasNextPage ? (
              <Button
                variant="default"
                onClick={() => void auditQuery.fetchNextPage()}
                disabled={auditQuery.isFetchingNextPage}
              >
                {auditQuery.isFetchingNextPage ? 'Loading more…' : 'Load more'}
              </Button>
            ) : entries.length > 0 ? (
              <p className="text-[13px] text-ink-2">End of the log.</p>
            ) : null}
          </div>
        </section>

        {search.seq !== undefined ? (
          <div className="min-w-0 lg:sticky lg:top-4">
            <AuditRecordPanel seq={search.seq} onClose={closeEntry} />
          </div>
        ) : null}
      </div>
    </main>
  );
}
