import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import {
  getListAuditExportsQueryKey,
  probeAuditExport,
  useListAuditExports,
} from '../api/generated/audit/audit';
import { shortId } from '../lib/agency-format';
import { relativeTime } from '../lib/app-status';
import { LOCK_VIEW } from '../lib/audit';
import { canManageUsers } from '../lib/capabilities';
import { problemView, type ProblemView } from '../lib/problem';
import { useSession } from '../session';
import { ToneLamp } from './tone-lamp';
import { Button } from './ui/button';
import { ProblemBanner } from './ui/problem-banner';

/**
 * The off-site copy of the audit log and the lock strength its probe observed
 * (ADR 0149, audit-log-format.md section 8). There is no create form in M5a.
 */
export function AuditCopy() {
  const { me } = useSession();
  const queryClient = useQueryClient();
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const exportsQuery = useListAuditExports();
  const auditExport = exportsQuery.data?.items[0];

  const probe = useMutation({
    mutationFn: (exportId: string) => probeAuditExport(exportId),
    onSuccess: () => {
      setProblem(null);
      void queryClient.invalidateQueries({
        queryKey: getListAuditExportsQueryKey(),
      });
    },
    onError: (error) => setProblem(problemView(error, 'probe the off-site copy')),
  });

  const listError =
    exportsQuery.error !== null && exportsQuery.error !== undefined
      ? problemView(exportsQuery.error, 'read the off-site copy')
      : null;

  // `provider:manage` is human and owner-only in M5a; `canManageUsers` is the
  // panel's existing owner-only gate, so the button is hidden for every other role.
  const canProbe = canManageUsers(me);

  return (
    <section
      aria-label="Off-site copy"
      className="rounded-[10px] border border-line bg-surface px-4 py-3 card-sheen"
    >
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          <h2 className="type-bay-title text-[15px] leading-tight">Off-site copy</h2>
          <p className="mt-1 text-[13px] leading-snug text-ink-2">
            A second copy of the audit log, in a bucket outside this server.
          </p>
        </div>
        {auditExport && canProbe ? (
          <Button
            variant="default"
            disabled={probe.isPending}
            onClick={() => {
              setProblem(null);
              probe.mutate(auditExport.id);
            }}
          >
            {probe.isPending ? 'Probing…' : 'Probe again'}
          </Button>
        ) : null}
      </div>

      {listError ? <ProblemBanner view={listError} className="mt-3" /> : null}
      {problem ? <ProblemBanner view={problem} className="mt-3" /> : null}

      {exportsQuery.isPending ? (
        <p className="mt-2 text-[13px] text-ink-2">Reading the off-site copy…</p>
      ) : auditExport === undefined ? (
        <p className="mt-2 text-[13px] leading-snug text-ink-2">
          No off-site copy is configured.{' '}
          <span className="type-code">POST /api/v1/audit-exports</span> creates
          one; the panel has no create form in M5a.
        </p>
      ) : (
        <div className="mt-3">
          <div className="flex items-start gap-2.5">
            <ToneLamp
              tone={LOCK_VIEW[auditExport.lock_strength].tone}
              size={16}
              label={LOCK_VIEW[auditExport.lock_strength].words}
              className="mt-0.5"
            />
            <div className="min-w-0 flex-1">
              <p className="type-body-medium text-[14px] leading-tight text-ink">
                {LOCK_VIEW[auditExport.lock_strength].words}
              </p>
              <p className="mt-1 text-[13px] leading-snug text-ink">
                {LOCK_VIEW[auditExport.lock_strength].meaning}
              </p>
            </div>
          </div>

          <dl className="type-data mt-3 grid grid-cols-1 gap-x-6 gap-y-1 border-t border-line pt-3 text-[13px] sm:grid-cols-2">
            <div className="flex min-w-0 gap-2">
              <dt className="text-ink-2">Provider</dt>
              <dd className="min-w-0 truncate text-ink" title={auditExport.provider_id}>
                {shortId(auditExport.provider_id)}
              </dd>
            </div>
            <div className="flex min-w-0 gap-2">
              <dt className="text-ink-2">Prefix</dt>
              <dd className="min-w-0 truncate text-ink" title={auditExport.prefix}>
                {auditExport.prefix}
              </dd>
            </div>
            <div className="flex gap-2">
              <dt className="text-ink-2">Strength</dt>
              <dd className="text-ink">{auditExport.lock_strength}</dd>
            </div>
            <div className="flex gap-2">
              <dt className="text-ink-2">Last probe</dt>
              <dd className="text-ink" title={auditExport.probed_at ?? undefined}>
                {auditExport.probed_at
                  ? relativeTime(auditExport.probed_at)
                  : 'Never probed'}
              </dd>
            </div>
            <div className="flex gap-2">
              <dt className="text-ink-2">Exported through</dt>
              <dd className="text-ink">
                {auditExport.last_exported_seq === 0
                  ? 'Nothing yet'
                  : `seq ${auditExport.last_exported_seq}`}
              </dd>
            </div>
            {auditExport.probe_detail ? (
              <div className="flex min-w-0 gap-2 sm:col-span-2">
                <dt className="text-ink-2">Probe saw</dt>
                <dd className="min-w-0 text-ink">{auditExport.probe_detail}</dd>
              </div>
            ) : null}
          </dl>

          {auditExport.last_error ? (
            <p className="mt-2 text-[13px] leading-snug text-fault">
              {auditExport.last_error}
            </p>
          ) : null}
        </div>
      )}
    </section>
  );
}
