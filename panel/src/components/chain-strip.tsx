// SPDX-License-Identifier: AGPL-3.0-only
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Check, ChevronDown, Link2, Unlink } from 'lucide-react';
import { useState } from 'react';
import {
  getListAuditExportsQueryKey,
  probeAuditExport,
  useListAuditExports,
} from '../api/generated/audit/audit';
import type { AuditVerificationRequest, AuditVerificationResult } from '../api/generated/model';
import { shortHash, shortId } from '../lib/agency-format';
import { relativeTime } from '../lib/app-status';
import { brokenReasonWords, buildVerifyBody, LOCK_VIEW } from '../lib/audit';
import { canManageUsers } from '../lib/capabilities';
import { problemView, type ProblemView } from '../lib/problem';
import { useNow } from '../lib/use-now';
import { cn } from '../lib/utils';
import { useSession } from '../session';
import { CopyButton } from './copy-button';
import { ToneLamp } from './tone-lamp';
import { Button } from './ui/button';
import { Field } from './ui/field';
import { Input } from './ui/input';
import { ProblemBanner } from './ui/problem-banner';

/** Empty means "leave it to the API"; anything else must be a whole number. */
const parseBound = (text: string): number | undefined => (text.trim() === '' ? undefined : Number(text.trim()));

/**
 * The chain in one line: whether the log was checked and held, from which
 * entry to which, and how far the off-site copy reaches under which lock.
 * Checking is a recorded call, so the strip never runs it by itself; until
 * someone does, it says so. Details open the bounds of a partial check and
 * the off-site copy itself.
 */
export function ChainStrip({
  result,
  checkedAt,
  problem,
  pending,
  onRun,
  onOpenEntry,
}: {
  result: AuditVerificationResult | null;
  checkedAt: number | null;
  problem: ProblemView | null;
  pending: boolean;
  onRun: (body: AuditVerificationRequest) => void;
  onOpenEntry: (seq: number) => void;
}) {
  const { me } = useSession();
  const queryClient = useQueryClient();
  const now = useNow(checkedAt !== null, 30_000);
  const [open, setOpen] = useState(false);
  const [fromText, setFromText] = useState('');
  const [toText, setToText] = useState('');
  const [boundProblem, setBoundProblem] = useState<ProblemView | null>(null);
  const [probeProblem, setProbeProblem] = useState<ProblemView | null>(null);

  const exportsQuery = useListAuditExports();
  const copy = exportsQuery.data?.items[0];
  const lock = copy ? LOCK_VIEW[copy.lock_strength] : null;
  // `provider:manage` is human and owner-only in M5a; `canManageUsers` is the
  // panel's owner-only gate.
  const canProbe = canManageUsers(me);

  const probe = useMutation({
    mutationFn: (exportId: string) => probeAuditExport(exportId),
    onSuccess: () => {
      setProbeProblem(null);
      void queryClient.invalidateQueries({ queryKey: getListAuditExportsQueryKey() });
    },
    onError: (error) => setProbeProblem(problemView(error, 'probe the off-site copy')),
  });

  const brokenAt = result && !result.ok && typeof result.broken_at_seq === 'number' ? result.broken_at_seq : null;
  const tone = pending ? 'shift' : result === null ? 'idle' : result.ok ? 'run' : 'fault';

  const copyWords = exportsQuery.isPending
    ? 'Reading the off-site copy…'
    : exportsQuery.isError
      ? 'Off-site copy could not be read'
      : !copy
        ? 'No off-site copy'
        : `Off-site copy ${copy.last_exported_seq === 0 ? 'holds nothing yet' : `through ${copy.last_exported_seq}`} · ${lock?.words.toLowerCase()}`;

  return (
    <section aria-label="The chain" className="chain-wrap">
      <div className={cn('n1 chain-strip', tone === 'fault' && 'n1-fault', tone !== 'run' && tone !== 'fault' && 'chain-idle')} aria-busy={pending}>
        <span className="n1-gl" aria-hidden="true">
          {tone === 'run' ? <Check strokeWidth={2} /> : tone === 'fault' ? <Unlink strokeWidth={1.8} /> : <Link2 strokeWidth={1.8} />}
        </span>
        <p className="n1-t" role="status" aria-live="polite">
          {pending ? (
            <b>Checking every link…</b>
          ) : result === null ? (
            <>
              <b>Chain not checked</b> on this visit · verifying recomputes every link and is recorded
            </>
          ) : result.ok ? (
            <>
              <b>Chain intact</b> from entry {result.first_seq} to {result.last_seq}
              {checkedAt !== null ? ` · checked ${relativeTime(new Date(checkedAt).toISOString(), now)}` : ''}
            </>
          ) : (
            <>
              <b>Chain broken</b>
              {brokenAt !== null ? ` at entry ${brokenAt}` : ''} · {brokenReasonWords(result.reason)}
            </>
          )}
        </p>
        {brokenAt !== null ? (
          <Button variant="default" size="sm" onClick={() => onOpenEntry(brokenAt)}>
            Open entry {brokenAt}
          </Button>
        ) : null}
        <button
          type="button"
          className="chain-more"
          aria-expanded={open}
          aria-controls="chain-details"
          onClick={() => setOpen((value) => !value)}
        >
          <span>{copyWords}</span>
          <ChevronDown size={14} className={cn('shrink-0 transition-transform', open && 'rotate-180')} aria-hidden="true" />
        </button>
      </div>

      {problem ? <ProblemBanner view={problem} className="mb-3" /> : null}

      {open ? (
        <div id="chain-details" className="box chain-details">
          <form
            className="chain-part"
            onSubmit={(event) => {
              event.preventDefault();
              const built = buildVerifyBody(parseBound(fromText), parseBound(toText));
              if (!built.ok) {
                setBoundProblem({ message: built.error });
                return;
              }
              setBoundProblem(null);
              onRun(built.body);
            }}
          >
            <h3>Check part of the chain</h3>
            <p>Leave both empty to check the whole log.</p>
            <div className="flex flex-wrap items-end gap-2">
              <Field label="From entry" htmlFor="chain-from" className="w-28">
                <Input
                  id="chain-from"
                  type="number"
                  min={1}
                  step={1}
                  inputMode="numeric"
                  placeholder="1"
                  value={fromText}
                  onChange={(event) => setFromText(event.target.value)}
                />
              </Field>
              <Field label="To entry" htmlFor="chain-to" className="w-28">
                <Input
                  id="chain-to"
                  type="number"
                  min={1}
                  step={1}
                  inputMode="numeric"
                  placeholder="Last"
                  value={toText}
                  onChange={(event) => setToText(event.target.value)}
                />
              </Field>
              <Button variant="default" type="submit" disabled={pending}>
                {pending ? 'Checking…' : 'Check'}
              </Button>
            </div>
            {boundProblem ? <ProblemBanner view={boundProblem} /> : null}
            {result ? (
              <p className="flex flex-wrap items-center gap-2 text-[12.5px] text-ink-2">
                Checked {result.checked} entries · head{' '}
                <span className="type-code" title={result.head_hash}>
                  {shortHash(result.head_hash)}
                </span>
                <CopyButton value={result.head_hash} label="Copy head hash" />
              </p>
            ) : null}
          </form>

          <div className="chain-copy">
            <div className="flex flex-wrap items-start gap-2">
              <h3 className="min-w-0 flex-1">Off-site copy</h3>
              {copy && canProbe ? (
                <Button
                  variant="default"
                  size="sm"
                  disabled={probe.isPending}
                  onClick={() => {
                    setProbeProblem(null);
                    probe.mutate(copy.id);
                  }}
                >
                  {probe.isPending ? 'Probing…' : 'Probe again'}
                </Button>
              ) : null}
            </div>
            {exportsQuery.isError ? (
              <ProblemBanner view={problemView(exportsQuery.error, 'read the off-site copy')} />
            ) : null}
            {probeProblem ? <ProblemBanner view={probeProblem} /> : null}
            {exportsQuery.isPending ? (
              <p>Reading the off-site copy…</p>
            ) : !copy ? (
              <p>
                No off-site copy is configured. <span className="type-code">POST /api/v1/audit-exports</span> creates
                one; the panel has no create form in M5a.
              </p>
            ) : (
              <>
                <p className="flex items-start gap-2 text-ink">
                  <ToneLamp tone={lock!.tone} size={14} label={lock!.words} className="mt-0.5" />
                  <span>
                    <b className="font-[600]">{lock!.words}.</b> {lock!.meaning}
                  </span>
                </p>
                <dl className="chain-facts">
                  <div>
                    <dt>Provider</dt>
                    <dd className="type-code" title={copy.provider_id}>
                      {shortId(copy.provider_id)}
                    </dd>
                  </div>
                  <div>
                    <dt>Prefix</dt>
                    <dd title={copy.prefix}>{copy.prefix}</dd>
                  </div>
                  <div>
                    <dt>Strength</dt>
                    <dd className="type-code">{copy.lock_strength}</dd>
                  </div>
                  <div>
                    <dt>Last probe</dt>
                    <dd title={copy.probed_at ?? undefined}>
                      {copy.probed_at ? relativeTime(copy.probed_at) : 'Never probed'}
                    </dd>
                  </div>
                  <div>
                    <dt>Exported through</dt>
                    <dd>{copy.last_exported_seq === 0 ? 'Nothing yet' : `entry ${copy.last_exported_seq}`}</dd>
                  </div>
                  {copy.probe_detail ? (
                    <div className="chain-fact-wide">
                      <dt>Probe saw</dt>
                      <dd>{copy.probe_detail}</dd>
                    </div>
                  ) : null}
                </dl>
                {copy.last_error ? <p className="text-fault">{copy.last_error}</p> : null}
              </>
            )}
          </div>
        </div>
      ) : null}
    </section>
  );
}
