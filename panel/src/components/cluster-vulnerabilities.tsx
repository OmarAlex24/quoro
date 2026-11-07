// SPDX-License-Identifier: AGPL-3.0-only
/**
 * The cluster's vulnerability summary (ADR 0150) on the nodes screen: the
 * running Quoro release's own components and the pinned guest kernel, each
 * node's host kernel, the per-app lines, and the age of the vulnerability data.
 * `quoro` and `host_kernels` are node data and are simply absent for a caller
 * without node:read, so their parts are absent too.
 */
import { useMutation } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';
import { useState } from 'react';
import type {
  AppVulnerabilitySummary,
  NodeKernelVulnerabilities,
  QuoroVulnerabilities,
  VulnerabilityDataSource,
  VulnerabilityFinding,
  VulnerabilitySummary,
} from '../api/generated/model';
import {
  getVulnerabilitySummary,
  useGetVulnerabilitySummary,
} from '../api/generated/vulnerabilities/vulnerabilities';
import pinsRaw from '../../../crates/engine/pins.txt?raw';
import { relativeTime } from '../lib/app-status';
import { problemView, type ProblemView } from '../lib/problem';
import { cn } from '../lib/utils';
import {
  guestKernelFindings,
  guestKernelIsClear,
  guestKernelVersion,
  inventoryStatusWords,
  openFindingCount,
  severityLabel,
  severityTone,
} from '../lib/vulnerabilities';
import {
  VulnerabilityCountsLine,
  VulnIdLink,
} from './app-vulnerabilities';
import { ToneLamp } from './tone-lamp';
import { Button } from './ui/button';
import { ProblemBanner } from './ui/problem-banner';

const PAGE_SIZE = 50;

/** The release's guest kernel, read from the pins compiled into this build. */
export const GUEST_KERNEL_VERSION = guestKernelVersion(pinsRaw);

function FindingLine({ finding }: { finding: VulnerabilityFinding }) {
  return (
    <li className="flex flex-wrap items-center gap-x-3 gap-y-1.5 py-2">
      <ToneLamp
        tone={severityTone(finding.severity)}
        size={16}
        label={severityLabel(finding.severity)}
      />
      <span className="type-body-medium text-[13px] text-ink">
        {severityLabel(finding.severity)}
      </span>
      {finding.kev ? (
        <span
          className="type-data text-[12px] font-medium text-fault"
          title="In CISA's Known Exploited Vulnerabilities catalog"
        >
          KEV
        </span>
      ) : null}
      {finding.component ? (
        <span className="type-data text-[13px] text-ink">{finding.component}</span>
      ) : null}
      <VulnIdLink vulnId={finding.vuln_id} />
      <span className="type-data text-[13px] text-ink-2">
        installed {finding.installed_version}
      </span>
      <span className="type-data text-[13px] text-ink-2">
        {finding.fixed_version === null
          ? 'no fix yet'
          : `fixed in ${finding.fixed_version}`}
      </span>
    </li>
  );
}

function QuoroPart({ quoro }: { quoro: QuoroVulnerabilities }) {
  const kernelFindings = guestKernelFindings(quoro.items);
  const otherFindings = quoro.items.filter(
    (finding) => finding.component !== 'guest-kernel',
  );
  const kernelClear = guestKernelIsClear(quoro.items);

  return (
    <div className="mt-4 border-t border-line pt-3">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h3 className="type-bay-title text-[14px] leading-tight text-ink">
          Quoro {quoro.version}
        </h3>
        <span className="type-data text-[13px] text-ink-2">
          {openFindingCount(quoro.counts)} open{' '}
          {openFindingCount(quoro.counts) === 1 ? 'finding' : 'findings'}
        </span>
      </div>
      <VulnerabilityCountsLine counts={quoro.counts} className="mt-2" />

      <div className="mt-3 flex flex-wrap items-baseline gap-x-3 gap-y-1 border-t border-line pt-2.5">
        <span className="type-body-medium text-[13px] text-ink">
          Guest kernel {GUEST_KERNEL_VERSION ?? 'not pinned'}
        </span>
        {GUEST_KERNEL_VERSION === null ? (
          <span className="text-[13px] text-ink-2">
            This panel build does not name a guest kernel.
          </span>
        ) : kernelClear ? (
          <span className="text-[13px] text-ink-2">
            no known vulnerabilities
          </span>
        ) : null}
      </div>

      {!kernelClear ? (
        <ul className="mt-1 flex flex-col divide-y divide-line border-y border-line">
          {kernelFindings.map((finding) => (
            <FindingLine key={finding.id} finding={finding} />
          ))}
        </ul>
      ) : null}

      {otherFindings.length > 0 ? (
        <ul className="mt-3 flex flex-col divide-y divide-line border-y border-line">
          {otherFindings.map((finding) => (
            <FindingLine key={finding.id} finding={finding} />
          ))}
        </ul>
      ) : (
        <p className="mt-2 text-[13px] leading-snug text-ink-2">
          No other Quoro component has an open finding.
        </p>
      )}
    </div>
  );
}

function HostKernelsPart({
  kernels,
  nodeNames,
}: {
  kernels: NodeKernelVulnerabilities[];
  nodeNames: Record<string, string>;
}) {
  return (
    <div className="mt-4 border-t border-line pt-3">
      <h3 className="type-bay-title text-[14px] leading-tight text-ink">
        Host kernels
      </h3>
      <ul className="mt-2 flex flex-col divide-y divide-line border-y border-line">
        {kernels.map((host) => {
          const upstreamOnly = host.items.some(
            (finding) => finding.upstream_version_only === true,
          );
          return (
            <li key={host.node_id} className="py-2.5">
              <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
                <span className="type-body-medium text-[14px] text-ink">
                  {nodeNames[host.node_id] ?? host.node_id}
                </span>
                <span className="type-data text-[13px] text-ink-2">
                  {host.kernel}
                </span>
                <span className="type-data text-[13px] text-ink-2">
                  {openFindingCount(host.counts)} open{' '}
                  {openFindingCount(host.counts) === 1 ? 'finding' : 'findings'}
                </span>
              </div>
              {host.items.length === 0 ? (
                <p className="mt-1 text-[13px] text-ink-2">
                  No known vulnerabilities in this kernel.
                </p>
              ) : (
                <ul className="mt-1 flex flex-col divide-y divide-line/70">
                  {host.items.map((finding) => (
                    <FindingLine key={finding.id} finding={finding} />
                  ))}
                </ul>
              )}
              {upstreamOnly ? (
                <p className="mt-1 text-[13px] leading-snug text-ink-2">
                  Matched by upstream version: a distribution kernel may already
                  carry the fix.
                </p>
              ) : null}
            </li>
          );
        })}
      </ul>
    </div>
  );
}

function AppRow({ app }: { app: AppVulnerabilitySummary }) {
  return (
    <tr className="border-b border-line/70 last:border-b-0">
      <td className="py-2 pr-3">
        <Link
          to="/apps/$appId"
          params={{ appId: app.app_id }}
          className="text-ink underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
        >
          {app.name}
        </Link>
      </td>
      <td className="py-2 pr-3 text-ink-2">
        {inventoryStatusWords(app.inventory_status)}
      </td>
      <td className="py-2 pr-3 text-ink">{openFindingCount(app.counts)}</td>
      <td className="py-2">
        <VulnerabilityCountsLine counts={app.counts} />
      </td>
    </tr>
  );
}

function AppsPart({
  summary,
  extraApps,
  nextCursor,
  loadMorePending,
  loadProblem,
  onLoadMore,
}: {
  summary: VulnerabilitySummary;
  extraApps: AppVulnerabilitySummary[];
  nextCursor: string | null;
  loadMorePending: boolean;
  loadProblem: ProblemView | null;
  onLoadMore: () => void;
}) {
  const apps = [...summary.apps, ...extraApps];
  return (
    <div className="mt-4 border-t border-line pt-3">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h3 className="type-bay-title text-[14px] leading-tight text-ink">Apps</h3>
        <span className="type-data text-[13px] text-ink-2">
          {apps.length} {apps.length === 1 ? 'app' : 'apps'}
        </span>
      </div>
      {apps.length === 0 ? (
        <p className="mt-2 text-[13px] text-ink-2">
          No app is visible to your role.
        </p>
      ) : (
        <div className="mt-2 overflow-x-auto">
          <table className="type-data w-full min-w-[640px] text-[13px]">
            <thead>
              <tr className="border-b border-line text-left text-ink-2">
                <th className="py-1.5 pr-3 font-normal">App</th>
                <th className="py-1.5 pr-3 font-normal">Image</th>
                <th className="py-1.5 pr-3 font-normal">Open findings</th>
                <th className="py-1.5 font-normal">By severity</th>
              </tr>
            </thead>
            <tbody>
              {apps.map((app) => (
                <AppRow key={app.app_id} app={app} />
              ))}
            </tbody>
          </table>
        </div>
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
    </div>
  );
}

function DataPart({ data }: { data: VulnerabilityDataSource[] }) {
  return (
    <div className="mt-4 border-t border-line pt-3">
      <h3 className="type-bay-title text-[14px] leading-tight text-ink">
        Vulnerability data
      </h3>
      {data.length === 0 ? (
        <p className="mt-2 text-[13px] text-ink-2">
          No vulnerability data has been brought in yet.
        </p>
      ) : (
        <ul className="mt-2 flex flex-col divide-y divide-line border-y border-line">
          {data.map((source) => (
            <li key={source.ecosystem} className="py-2">
              <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
                <span className="type-data text-[14px] text-ink">
                  {source.ecosystem}
                </span>
                <span className="text-[13px] text-ink-2">
                  source {source.source}
                </span>
                <span className="type-data text-[13px] text-ink-2">
                  {source.data_modified_at === null
                    ? 'no data yet'
                    : `data from ${relativeTime(source.data_modified_at)}`}
                </span>
              </div>
              {source.last_error ? (
                <p className="mt-1 text-[13px] leading-snug text-fault">
                  {source.last_error}
                </p>
              ) : null}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function SummaryBody({
  summary,
  nodeNames,
  extraApps,
  nextCursor,
  loadMorePending,
  loadProblem,
  onLoadMore,
}: {
  summary: VulnerabilitySummary;
  nodeNames: Record<string, string>;
  extraApps: AppVulnerabilitySummary[];
  nextCursor: string | null;
  loadMorePending: boolean;
  loadProblem: ProblemView | null;
  onLoadMore: () => void;
}) {
  return (
    <>
      {summary.quoro ? <QuoroPart quoro={summary.quoro} /> : null}
      {summary.host_kernels ? (
        <HostKernelsPart kernels={summary.host_kernels} nodeNames={nodeNames} />
      ) : null}
      <AppsPart
        summary={summary}
        extraApps={extraApps}
        nextCursor={nextCursor}
        loadMorePending={loadMorePending}
        loadProblem={loadProblem}
        onLoadMore={onLoadMore}
      />
      <DataPart data={summary.data} />
    </>
  );
}

/** The cluster-wide vulnerability summary, mounted under the node cards. */
export function ClusterVulnerabilities({
  nodeNames,
  className,
}: {
  nodeNames: Record<string, string>;
  className?: string;
}) {
  // `undefined` until a further page was loaded; then the cursor that page
  // returned, `null` once the last page is in.
  const [cursor, setCursor] = useState<string | null | undefined>(undefined);
  const [extraApps, setExtraApps] = useState<AppVulnerabilitySummary[]>([]);
  const [loadProblem, setLoadProblem] = useState<ProblemView | null>(null);

  const summaryQuery = useGetVulnerabilitySummary(
    { limit: PAGE_SIZE },
    { query: { refetchInterval: 10_000 } },
  );

  const loadMore = useMutation({
    mutationFn: (nextCursor: string) =>
      getVulnerabilitySummary({ limit: PAGE_SIZE, cursor: nextCursor }),
    onSuccess: (page) => {
      setExtraApps((previous) => [...previous, ...page.apps]);
      setCursor(page.next_cursor ?? null);
      setLoadProblem(null);
    },
    onError: (error) => setLoadProblem(problemView(error, 'load more apps')),
  });

  const summary = summaryQuery.data;
  const nextCursor = cursor === undefined ? (summary?.next_cursor ?? null) : cursor;
  const error =
    summaryQuery.error !== null && summaryQuery.error !== undefined
      ? problemView(summaryQuery.error, 'read the vulnerability summary')
      : null;

  return (
    <section
      className={cn(
        'rounded-[10px] border border-line bg-surface px-4 py-4 sm:px-5',
        className,
      )}
    >
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h2 className="type-display-light text-[15px] leading-none text-ink">
          Vulnerabilities
        </h2>
        <span className="type-data text-[13px] text-ink-2">
          Cluster summary
        </span>
      </div>

      {error ? (
        <ProblemBanner view={error} className="mt-3" />
      ) : summaryQuery.isPending ? (
        <p className="mt-3 text-[13px] text-ink-2">
          Reading the vulnerability summary…
        </p>
      ) : summary === undefined ? null : (
        <SummaryBody
          summary={summary}
          nodeNames={nodeNames}
          extraApps={extraApps}
          nextCursor={nextCursor}
          loadMorePending={loadMore.isPending}
          loadProblem={loadProblem}
          onLoadMore={() => {
            if (nextCursor !== null) loadMore.mutate(nextCursor);
          }}
        />
      )}
    </section>
  );
}
