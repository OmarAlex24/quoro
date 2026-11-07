// SPDX-License-Identifier: AGPL-3.0-only
/**
 * An app's vulnerability report (ADR 0138, ADR 0150): what the node knows
 * about the image's packages, how old the data is, the open findings with the
 * filters that map to the query, and the rebuild that picks up a patched base.
 */
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from '@tanstack/react-router';
import { useState } from 'react';
import type {
  App,
  AppVulnerabilities as AppVulnerabilitiesReport,
  VulnerabilityCounts,
  VulnerabilityFinding,
  VulnerabilitySeverity,
} from '../api/generated/model';
import { startBuild } from '../api/generated/builds/builds';
import {
  getAppVulnerabilities,
  getGetAppVulnerabilitiesQueryKey,
  getGetVulnerabilitySummaryQueryKey,
  useGetAppVulnerabilities,
} from '../api/generated/vulnerabilities/vulnerabilities';
import { relativeTime } from '../lib/app-status';
import { problemView, type ProblemView } from '../lib/problem';
import { cn } from '../lib/utils';
import {
  dataAgeSentence,
  inventorySentence,
  isOpenFinding,
  openFindingCount,
  resolvedReasonWords,
  severityLabel,
  severityTone,
} from '../lib/vulnerabilities';
import { ToneLamp } from './tone-lamp';
import { Button } from './ui/button';
import { Field } from './ui/field';
import { ProblemBanner } from './ui/problem-banner';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from './ui/select';

type SeverityFilter = 'all' | 'high' | 'critical';

const severityParams: Record<SeverityFilter, VulnerabilitySeverity | undefined> = {
  all: undefined,
  high: 'high',
  critical: 'critical',
};

const PAGE_SIZE = 50;

/** The critical / high / medium / low / unknown and KEV readout. */
export function VulnerabilityCountsLine({
  counts,
  className,
}: {
  counts: VulnerabilityCounts;
  className?: string;
}) {
  const severities: VulnerabilitySeverity[] = [
    'critical',
    'high',
    'medium',
    'low',
    'unknown',
  ];
  return (
    <ul className={cn('flex flex-wrap items-center gap-x-4 gap-y-1.5', className)}>
      {severities.map((severity) => (
        <li key={severity} className="flex items-center gap-1.5">
          <ToneLamp
            tone={severityTone(severity)}
            size={14}
            label={severityLabel(severity)}
          />
          <span className="type-data text-[13px] text-ink">{counts[severity]}</span>
          <span className="text-[13px] text-ink-2">
            {severityLabel(severity).toLowerCase()}
          </span>
        </li>
      ))}
      <li className="flex items-center gap-1.5">
        <ToneLamp
          tone={counts.kev > 0 ? 'fault' : 'idle'}
          size={14}
          label="Known exploited"
        />
        <span className="type-data text-[13px] text-ink">{counts.kev}</span>
        <span className="text-[13px] text-ink-2">KEV</span>
      </li>
    </ul>
  );
}

/** The OSV record of a finding, opened in a new tab. */
export function VulnIdLink({ vulnId }: { vulnId: string }) {
  return (
    <a
      href={`https://osv.dev/vulnerability/${vulnId}`}
      target="_blank"
      rel="noreferrer"
      className="type-code text-[12px] text-ink underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
    >
      {vulnId}
    </a>
  );
}

const KevMark = () => (
  <span
    className="type-data text-[12px] font-medium text-fault"
    title="In CISA's Known Exploited Vulnerabilities catalog"
  >
    KEV
  </span>
);

/** The installed and fixed versions, said the way the report reads. */
export function VersionPair({
  installed,
  fixed,
}: {
  installed: string;
  fixed: string | null;
}) {
  return (
    <>
      <span className="type-data text-[13px] text-ink-2">
        installed {installed}
      </span>
      <span className="type-data text-[13px] text-ink-2">
        {fixed === null ? 'no fix yet' : `fixed in ${fixed}`}
      </span>
    </>
  );
}

function FindingRow({ finding }: { finding: VulnerabilityFinding }) {
  const resolved = !isOpenFinding(finding);
  return (
    <li className="py-2.5">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1.5">
        <ToneLamp
          tone={severityTone(finding.severity)}
          size={16}
          label={severityLabel(finding.severity)}
        />
        <span className="type-body-medium text-[14px] text-ink">
          {severityLabel(finding.severity)}
        </span>
        {finding.kev ? <KevMark /> : null}
        <VulnIdLink vulnId={finding.vuln_id} />
        <span className="type-data text-[13px] text-ink">
          {finding.package}
          {finding.source_package
            ? ` from ${finding.source_package}`
            : ''}
        </span>
        <VersionPair
          installed={finding.installed_version}
          fixed={finding.fixed_version}
        />
        {resolved && finding.resolved_at !== null ? (
          <span className="w-full text-[13px] leading-snug text-ink-2">
            resolved {relativeTime(finding.resolved_at)}:{' '}
            {resolvedReasonWords(finding.resolved_reason)}
          </span>
        ) : null}
      </div>
      {finding.summary ? (
        <p className="mt-1 text-[13px] leading-snug text-ink-2">
          {finding.summary}
        </p>
      ) : null}
    </li>
  );
}

/**
 * "Rebuild with refreshed base": start a build that resolves every `FROM`
 * again, then open it. Shared with the Builds section so the action stays
 * reachable from both places (ADR 0150).
 */
export function RebuildWithRefreshedBase({
  appId,
  disabled = false,
  disabledReason = null,
  onStarted,
  variant = 'default',
  className,
}: {
  appId: string;
  disabled?: boolean;
  disabledReason?: string | null;
  onStarted?: () => void;
  variant?: 'primary' | 'default';
  className?: string;
}) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const rebuild = useMutation({
    mutationFn: () =>
      startBuild(
        appId,
        { refresh_base: true },
        { headers: { 'Idempotency-Key': crypto.randomUUID() } },
      ),
    onSuccess: (build) => {
      setProblem(null);
      void queryClient.invalidateQueries({
        queryKey: getGetAppVulnerabilitiesQueryKey(appId),
      });
      void queryClient.invalidateQueries({
        queryKey: getGetVulnerabilitySummaryQueryKey(),
      });
      onStarted?.();
      void navigate({
        to: '/apps/$appId/builds/$buildId',
        params: { appId, buildId: build.id },
      });
    },
    onError: (error) => setProblem(problemView(error, 'start builds')),
  });

  return (
    <div className={cn('flex flex-col gap-1.5', className)}>
      <Button
        variant={variant}
        disabled={disabled || rebuild.isPending}
        onClick={() => {
          setProblem(null);
          rebuild.mutate();
        }}
      >
        {rebuild.isPending ? 'Starting…' : 'Rebuild with refreshed base'}
      </Button>
      {disabledReason ? (
        <p className="max-w-[280px] text-[13px] leading-snug text-ink-2">
          {disabledReason}
        </p>
      ) : null}
      {problem ? <ProblemBanner view={problem} className="mt-2" /> : null}
    </div>
  );
}

function SectionBody({
  report,
  severityFilter,
  kevOnly,
  includeResolved,
  filtersActive,
  items,
  nextCursor,
  loadMorePending,
  loadProblem,
  onSeverityChange,
  onKevChange,
  onResolvedChange,
  onLoadMore,
}: {
  report: AppVulnerabilitiesReport;
  severityFilter: SeverityFilter;
  kevOnly: boolean;
  includeResolved: boolean;
  filtersActive: boolean;
  items: VulnerabilityFinding[];
  nextCursor: string | null;
  loadMorePending: boolean;
  loadProblem: ProblemView | null;
  onSeverityChange: (next: SeverityFilter) => void;
  onKevChange: (next: boolean) => void;
  onResolvedChange: (next: boolean) => void;
  onLoadMore: () => void;
}) {
  const { inventory, counts, data } = report;
  const openCount = openFindingCount(counts);
  const checked =
    inventory.status === 'inventoried' || inventory.status === 'pending_data';
  const notInventoried = inventory.not_inventoried ?? [];

  return (
    <>
      <p className="mt-3 text-[13px] leading-snug text-ink">
        {inventorySentence(inventory)}
      </p>
      {notInventoried.length > 0 ? (
        <ul className="mt-1 flex flex-col gap-0.5">
          {notInventoried.map((kind) => (
            <li key={kind} className="text-[13px] leading-snug text-ink-2">
              Could not inventory: {kind}
            </li>
          ))}
        </ul>
      ) : null}

      {data.length > 0 ? (
        <ul className="type-data mt-2 flex flex-col gap-0.5 text-[13px]">
          {data.map((source) => (
            <li key={source.ecosystem} className="leading-snug text-ink-2">
              {dataAgeSentence(source)}
              {source.last_error ? (
                <span className="block text-fault">{source.last_error}</span>
              ) : null}
            </li>
          ))}
        </ul>
      ) : null}

      {checked ? (
        <VulnerabilityCountsLine counts={counts} className="mt-3" />
      ) : null}

      <div className="mt-4 flex flex-wrap items-end gap-x-4 gap-y-3 border-t border-line pt-3">
        <Field label="Severity" htmlFor="vuln-severity" className="w-48">
          <Select
            value={severityFilter}
            onValueChange={(value) => onSeverityChange(value as SeverityFilter)}
          >
            <SelectTrigger id="vuln-severity" aria-label="Severity filter">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All severities</SelectItem>
              <SelectItem value="high">High and above</SelectItem>
              <SelectItem value="critical">Critical only</SelectItem>
            </SelectContent>
          </Select>
        </Field>
        <label className="inline-flex cursor-pointer items-center gap-1.5 pb-1.5 text-[13px]">
          <input
            id="vuln-kev"
            type="checkbox"
            className="h-3.5 w-3.5 accent-accent-solid"
            checked={kevOnly}
            onChange={(event) => onKevChange(event.target.checked)}
          />
          KEV only
        </label>
        <label className="inline-flex cursor-pointer items-center gap-1.5 pb-1.5 text-[13px]">
          <input
            id="vuln-resolved"
            type="checkbox"
            className="h-3.5 w-3.5 accent-accent-solid"
            checked={includeResolved}
            onChange={(event) => onResolvedChange(event.target.checked)}
          />
          Show resolved
        </label>
      </div>

      {items.length === 0 ? (
        filtersActive ? (
          <p className="mt-3 text-[13px] text-ink-2">
            No findings match these filters.
          </p>
        ) : checked && openCount === 0 ? (
          <p className="mt-3 text-[13px] text-ink-2">
            No known vulnerabilities in this image.
          </p>
        ) : null
      ) : (
        <ul className="mt-3 flex flex-col divide-y divide-line border-y border-line">
          {items.map((finding) => (
            <FindingRow key={finding.id} finding={finding} />
          ))}
        </ul>
      )}

      {loadProblem ? <ProblemBanner view={loadProblem} className="mt-3" /> : null}

      {nextCursor !== null ? (
        <Button
          variant="default"
          className="mt-3"
          disabled={loadMorePending}
          onClick={onLoadMore}
        >
          {loadMorePending ? 'Loading…' : 'Load more'}
        </Button>
      ) : null}
    </>
  );
}

function AppVulnerabilitiesSection({
  app,
  canWrite,
  onRebuilt,
}: {
  app: App;
  canWrite: boolean;
  onRebuilt: () => void;
}) {
  const [severityFilter, setSeverityFilter] = useState<SeverityFilter>('all');
  const [kevOnly, setKevOnly] = useState(false);
  const [includeResolved, setIncludeResolved] = useState(false);
  // `undefined` until a further page was loaded; then the cursor that page
  // returned, `null` once the last page is in.
  const [cursor, setCursor] = useState<string | null | undefined>(undefined);
  const [extraItems, setExtraItems] = useState<VulnerabilityFinding[]>([]);
  const [loadProblem, setLoadProblem] = useState<ProblemView | null>(null);

  const filtersActive =
    severityFilter !== 'all' || kevOnly || includeResolved;

  const params = {
    severity: severityParams[severityFilter],
    kev: kevOnly ? true : undefined,
    include_resolved: includeResolved ? true : undefined,
    limit: PAGE_SIZE,
  };

  const reportQuery = useGetAppVulnerabilities(app.id, params, {
    query: { refetchInterval: 30_000 },
  });

  const loadMore = useMutation({
    mutationFn: (nextCursor: string) =>
      getAppVulnerabilities(app.id, { ...params, cursor: nextCursor }),
    onSuccess: (page) => {
      setExtraItems((previous) => [...previous, ...page.items]);
      setCursor(page.next_cursor ?? null);
      setLoadProblem(null);
    },
    onError: (error) => setLoadProblem(problemView(error, 'load more findings')),
  });

  const reset = () => {
    setCursor(undefined);
    setExtraItems([]);
    setLoadProblem(null);
  };

  const changeSeverity = (next: SeverityFilter) => {
    setSeverityFilter(next);
    reset();
  };
  const changeKev = (next: boolean) => {
    setKevOnly(next);
    reset();
  };
  const changeResolved = (next: boolean) => {
    setIncludeResolved(next);
    reset();
  };

  const report = reportQuery.data;
  const items = [...(report?.items ?? []), ...extraItems];
  const nextCursor = cursor === undefined ? (report?.next_cursor ?? null) : cursor;
  const openCount = report ? openFindingCount(report.counts) : 0;
  const error =
    reportQuery.error !== null && reportQuery.error !== undefined
      ? problemView(reportQuery.error, 'read the vulnerability report')
      : null;

  // The rebuild control only makes sense for an app Quoro builds itself.
  const hasSource = app.source !== null && app.source !== undefined;
  const showRebuild = hasSource && openCount > 0;
  const showImageNote = !hasSource && openCount > 0;
  const disabledReason = !canWrite
    ? 'Your role can read vulnerabilities but not start builds.'
    : null;

  return (
    <section className="mt-5 rounded-[10px] border border-line bg-surface px-4 py-4 sm:px-5">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h2 className="type-display-light text-[15px] leading-none text-ink">
          Vulnerabilities
        </h2>
        {report ? (
          <span className="type-data text-[13px] text-ink-2">
            {openCount} open {openCount === 1 ? 'finding' : 'findings'}
          </span>
        ) : null}
      </div>

      {error ? (
        <ProblemBanner view={error} className="mt-3" />
      ) : reportQuery.isPending ? (
        <p className="mt-3 text-[13px] text-ink-2">
          Reading the vulnerability report…
        </p>
      ) : report === undefined ? null : (
        <>
          <SectionBody
            report={report}
            severityFilter={severityFilter}
            kevOnly={kevOnly}
            includeResolved={includeResolved}
            filtersActive={filtersActive}
            items={items}
            nextCursor={nextCursor}
            loadMorePending={loadMore.isPending}
            loadProblem={loadProblem}
            onSeverityChange={changeSeverity}
            onKevChange={changeKev}
            onResolvedChange={changeResolved}
            onLoadMore={() => {
              if (nextCursor !== null) loadMore.mutate(nextCursor);
            }}
          />

          {showRebuild ? (
            <div className="mt-4 border-t border-line pt-3">
              <RebuildWithRefreshedBase
                appId={app.id}
                disabled={!canWrite}
                disabledReason={disabledReason}
                onStarted={onRebuilt}
              />
            </div>
          ) : null}
          {showImageNote ? (
            <p className="mt-4 border-t border-line pt-3 text-[13px] leading-snug text-ink-2">
              This app runs an image Quoro did not build: deploy a patched image
              to fix these.
            </p>
          ) : null}
        </>
      )}
    </section>
  );
}

/** The app's "Vulnerabilities" section, mounted after Deployment. */
export function AppVulnerabilities({
  app,
  canWrite,
  onRebuilt,
}: {
  app: App;
  canWrite: boolean;
  onRebuilt: () => void;
}) {
  return (
    <AppVulnerabilitiesSection
      key={app.id}
      app={app}
      canWrite={canWrite}
      onRebuilt={onRebuilt}
    />
  );
}
