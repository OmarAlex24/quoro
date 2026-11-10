// SPDX-License-Identifier: AGPL-3.0-only
/**
 * The pieces of an app's vulnerability report (ADR 0138, ADR 0150) that the
 * service sheet's Security tab, its Deployments tab and the cluster summary
 * share: the counts line, a finding's id and versions, and the rebuild that
 * picks up a patched base.
 */
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from '@tanstack/react-router';
import { useState } from 'react';
import type {
  VulnerabilityCounts,
  VulnerabilitySeverity,
} from '../api/generated/model';
import { startBuild } from '../api/generated/builds/builds';
import {
  getGetAppVulnerabilitiesQueryKey,
  getGetVulnerabilitySummaryQueryKey,
} from '../api/generated/vulnerabilities/vulnerabilities';
import { problemView, type ProblemView } from '../lib/problem';
import { cn } from '../lib/utils';
import {
  severityLabel,
  severityTone,
} from '../lib/vulnerabilities';
import { ToneLamp } from './tone-lamp';
import { Button } from './ui/button';
import { ProblemBanner } from './ui/problem-banner';

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
        busy={rebuild.isPending}
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
