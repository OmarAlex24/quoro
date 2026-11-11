// SPDX-License-Identifier: AGPL-3.0-only
import { useMutation } from '@tanstack/react-query';
import { ChevronDown } from 'lucide-react';
import { useState, type KeyboardEvent } from 'react';
import type { AppDetail, VulnerabilityFinding } from '../api/generated/model';
import { getAppVulnerabilities, useGetAppVulnerabilities } from '../api/generated/vulnerabilities/vulnerabilities';
import { relativeTime } from '../lib/app-status';
import { problemView, type ProblemView } from '../lib/problem';
import {
  checkedAgainstSentence,
  dataAgeSentence,
  groupFindings,
  inventorySentence,
  isOpenFinding,
  openFindingCount,
  resolvedReasonWords,
  securityParams,
  severityTone,
  type SeverityFilter,
} from '../lib/vulnerabilities';
import { RebuildWithRefreshedBase, VulnerabilityCountsLine, VulnIdLink } from './app-vulnerabilities';
import { LampDot } from './lamp';
import { Button } from './ui/button';
import { ProblemBanner } from './ui/problem-banner';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select';
import { Tag } from './ui/tag';
import { EmptyState } from './empty-state';
import { SkeletonRows } from './skeleton';

/**
 * The Security tab: the open findings of the image the app runs, grouped by
 * severity, most severe first. "With resolved" lists the rest; the severity
 * and KEV filters map to the report's query. What the report is based on
 * (the package inventory, the data sources and their age, the six counters)
 * sits under "About this report". A Git app keeps "Rebuild with refreshed
 * base" here and in Deployments.
 */
export function SecurityTab({ app, canWrite, onRebuilt }: { app: AppDetail; canWrite: boolean; onRebuilt: () => void }) {
  const [severity, setSeverity] = useState<SeverityFilter>('all');
  const [kevOnly, setKevOnly] = useState(false);
  const [includeResolved, setIncludeResolved] = useState(false);
  // Pages after the first: what they added, and the cursor the last one returned.
  const [more, setMore] = useState<{ items: VulnerabilityFinding[]; cursor: string | null } | null>(null);
  const [loadProblem, setLoadProblem] = useState<ProblemView | null>(null);

  const params = securityParams(severity, kevOnly, includeResolved);
  const reportQuery = useGetAppVulnerabilities(app.id, params, { query: { refetchInterval: 30_000 } });
  const loadMore = useMutation({
    mutationFn: (cursor: string) => getAppVulnerabilities(app.id, { ...params, cursor }),
    onSuccess: (page) => {
      setMore((previous) => ({ items: [...(previous?.items ?? []), ...page.items], cursor: page.next_cursor ?? null }));
      setLoadProblem(null);
    },
    onError: (error) => setLoadProblem(problemView(error, 'load more findings')),
  });
  const reset = () => {
    setMore(null);
    setLoadProblem(null);
  };

  const report = reportQuery.data;
  if (reportQuery.error) {
    return <ProblemBanner view={problemView(reportQuery.error, 'read the vulnerability report')} />;
  }
  if (!report) return <SkeletonRows label="Loading the vulnerability report" rows={3} />;

  const items = [...report.items, ...(more?.items ?? [])];
  const cursor = more === null ? (report.next_cursor ?? null) : more.cursor;
  const open = openFindingCount(report.counts);
  const checked = report.inventory.status === 'inventoried' || report.inventory.status === 'pending_data';
  const filtered = severity !== 'all' || kevOnly;
  const groups = groupFindings(items);
  const hasSource = app.source !== null && app.source !== undefined;

  const rebuild =
    hasSource && open > 0 ? (
      <RebuildWithRefreshedBase
        appId={app.id}
        disabled={!canWrite}
        disabledReason={canWrite ? null : 'Your role can read vulnerabilities but not start builds.'}
        onStarted={onRebuilt}
      />
    ) : null;
  const inventoried = report.inventory.status === 'inventoried';

  return (
    <>
      {inventoried ? (
        <>
          <dl className="stats stats-3">
            <div>
              <dt>Open findings</dt>
              <dd>{open}</dd>
            </div>
            <div>
              <dt>Packages checked</dt>
              <dd>{report.inventory.package_count ?? 0}</dd>
            </div>
            <div>
              <dt>Base</dt>
              <dd className="stats-word" title={report.inventory.os}>
                {report.inventory.os ?? 'Not reported'}
              </dd>
            </div>
          </dl>
          {rebuild ? <div>{rebuild}</div> : null}
        </>
      ) : (
        <div className="sec-head">
          <div className="box-t">
            <b className="sec-title">
              {checked ? `${open} open ${open === 1 ? 'finding' : 'findings'}` : 'Not checked yet'}
            </b>
            <small>{inventorySentence(report.inventory)}</small>
          </div>
          {rebuild}
        </div>
      )}

      {checked ? (
        <div className="sec-filters">
          <OpenOrAll
            includeResolved={includeResolved}
            onChange={(next) => {
              setIncludeResolved(next);
              reset();
            }}
          />
          <Select
            value={severity}
            onValueChange={(value) => {
              setSeverity(value as SeverityFilter);
              reset();
            }}
          >
            <SelectTrigger aria-label="Severity filter" className="sec-select">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All severities</SelectItem>
              <SelectItem value="high">High and above</SelectItem>
              <SelectItem value="critical">Critical only</SelectItem>
            </SelectContent>
          </Select>
          <label className="sec-check">
            <input
              type="checkbox"
              checked={kevOnly}
              onChange={(event) => {
                setKevOnly(event.target.checked);
                reset();
              }}
            />
            Known exploited only
          </label>
        </div>
      ) : null}

      {checked && items.length === 0 ? (
        <EmptyState
          title={
            filtered
              ? 'No findings match these filters'
              : includeResolved
                ? 'No findings, open or resolved, for this image'
                : 'No known vulnerabilities in this image'
          }
        >
          {filtered ? null : cleanSentence(checkedAgainstSentence(report.inventory, report.data))}
        </EmptyState>
      ) : null}

      {groups.map((group) => (
        <section key={group.key} className="fgroup" aria-labelledby={`fgroup-${group.key}`}>
          <h3 id={`fgroup-${group.key}`} className="fgroup-h">
            {group.key === 'resolved' ? (
              <LampDot ring="idle" core="idle" size={12} />
            ) : (
              <LampDot ring={severityTone(group.key)} core={severityTone(group.key)} size={12} />
            )}
            {group.label}
            <span className="count count-soft">{group.items.length}</span>
          </h3>
          <ul className="box">
            {group.items.map((finding) => (
              <FindingRow key={finding.id} finding={finding} />
            ))}
          </ul>
        </section>
      ))}

      {loadProblem ? <ProblemBanner view={loadProblem} /> : null}
      {cursor !== null ? (
        <div>
          <Button busy={loadMore.isPending} size="sm" disabled={loadMore.isPending} onClick={() => loadMore.mutate(cursor)}>
            {loadMore.isPending ? 'Loading…' : 'Load more'}
          </Button>
        </div>
      ) : null}

      {!hasSource && open > 0 ? (
        <p className="notice">
          <span>This app runs an image Quoro did not build: deploy a patched image to fix these.</span>
        </p>
      ) : null}

      <details className="sec-about">
        <summary className="hist-h">
          <ChevronDown strokeWidth={1.8} aria-hidden="true" />
          About this report
        </summary>
        <div className="box sec-about-b">
          <p>{inventorySentence(report.inventory)}</p>
          {(report.inventory.not_inventoried ?? []).map((kind) => (
            <p key={kind} className="text-ink-2">
              Could not inventory: {kind}
            </p>
          ))}
          {report.data.map((source) => (
            <p key={source.ecosystem} className="text-ink-2">
              {dataAgeSentence(source)}
              {source.last_error ? <span className="block text-fault">{source.last_error}</span> : null}
            </p>
          ))}
          {checked ? <VulnerabilityCountsLine counts={report.counts} className="mt-1" /> : null}
        </div>
      </details>
    </>
  );
}

/** The empty state's words: what the image was checked against, and where a new finding would show. */
const cleanSentence = (checkedAgainst: string | null): string =>
  [checkedAgainst, 'New findings show a count on this tab.'].filter(Boolean).join(' ');

/** Open findings, or every finding with the resolved ones after them. */
function OpenOrAll({ includeResolved, onChange }: { includeResolved: boolean; onChange: (next: boolean) => void }) {
  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (!['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown'].includes(event.key)) return;
    event.preventDefault();
    onChange(!includeResolved);
    event.currentTarget.querySelector<HTMLButtonElement>(`[data-value="${includeResolved ? 'open' : 'all'}"]`)?.focus();
  };
  return (
    <div role="radiogroup" aria-label="Which findings" className="seg seg-fit" onKeyDown={onKeyDown}>
      <button
        type="button"
        role="radio"
        data-value="open"
        aria-checked={!includeResolved}
        tabIndex={includeResolved ? -1 : 0}
        onClick={() => onChange(false)}
      >
        Open
      </button>
      <button
        type="button"
        role="radio"
        data-value="all"
        aria-checked={includeResolved}
        tabIndex={includeResolved ? 0 : -1}
        onClick={() => onChange(true)}
      >
        With resolved
      </button>
    </div>
  );
}

/**
 * One finding: its id (linked to its OSV record), the KEV mark, the package
 * with the installed version and the first one that fixes it, and the
 * record's summary.
 */
function FindingRow({ finding }: { finding: VulnerabilityFinding }) {
  const resolved = !isOpenFinding(finding);
  return (
    <li className="box-row frow">
      <div className="box-t">
        <span className="frow-id">
          <VulnIdLink vulnId={finding.vuln_id} />
          {finding.kev ? (
            <Tag tone="fault" title="In CISA's Known Exploited Vulnerabilities catalog">
              KEV
            </Tag>
          ) : null}
        </span>
        <small>
          <span>
            {finding.package}
            {finding.source_package && finding.source_package !== finding.package
              ? ` from ${finding.source_package}`
              : ''}
          </span>
          <span>
            <span className="sr-only">installed </span>
            <span className="type-code">{finding.installed_version}</span>
          </span>
          <span aria-hidden="true">→</span>
          {finding.fixed_version === null ? (
            <span>no fix yet</span>
          ) : (
            <span>
              fixed in <span className="type-code">{finding.fixed_version}</span>
            </span>
          )}
        </small>
        {finding.summary ? <p className="frow-sum">{finding.summary}</p> : null}
        {resolved && finding.resolved_at !== null ? (
          <p className="frow-sum">
            Resolved {relativeTime(finding.resolved_at)}: {resolvedReasonWords(finding.resolved_reason)}
          </p>
        ) : null}
      </div>
    </li>
  );
}
