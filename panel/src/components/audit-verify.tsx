import { useState } from 'react';
import type { AuditVerificationRequest, AuditVerificationResult } from '../api/generated/model';
import { shortHash } from '../lib/agency-format';
import { buildVerifyBody } from '../lib/audit';
import type { ProblemView } from '../lib/problem';
import { CopyButton } from './copy-button';
import { ToneLamp } from './tone-lamp';
import { Button } from './ui/button';
import { Field } from './ui/field';
import { Input } from './ui/input';
import { ProblemBanner } from './ui/problem-banner';

/** The verification `reason` codes (audit-log-format.md, section 6), said plainly. */
const brokenReasonWords = (reason: string | null | undefined): string => {
  switch (reason) {
    case 'hash_mismatch':
      return 'The entry was changed after it was written: its hash no longer matches its contents (hash_mismatch).';
    case 'prev_mismatch':
      return 'The entry no longer links to the one before it (prev_mismatch).';
    case 'seq_gap':
      return 'An entry is missing before this one (seq_gap).';
    default:
      return reason ?? 'The link does not match the record before it.';
  }
};

/** Empty means "leave it to the API"; anything else must be a whole number. */
const parseBound = (text: string): number | undefined => {
  const trimmed = text.trim();
  if (trimmed === '') return undefined;
  return Number(trimmed);
};

/**
 * The verify control and its result plate. The result stays until the next
 * run (the screen owns it, so the rail can show the break too).
 */
export function AuditVerify({
  result,
  problem,
  pending,
  onRun,
  onOpenEntry,
}: {
  result: AuditVerificationResult | null;
  problem: ProblemView | null;
  pending: boolean;
  onRun: (body: AuditVerificationRequest) => void;
  onOpenEntry: (seq: number) => void;
}) {
  const [fromText, setFromText] = useState('');
  const [toText, setToText] = useState('');
  const [localProblem, setLocalProblem] = useState<ProblemView | null>(null);

  const submit = () => {
    const built = buildVerifyBody(parseBound(fromText), parseBound(toText));
    if (!built.ok) {
      setLocalProblem({ message: built.error });
      return;
    }
    setLocalProblem(null);
    onRun(built.body);
  };

  const brokenAt =
    result && !result.ok && typeof result.broken_at_seq === 'number'
      ? result.broken_at_seq
      : null;

  return (
    <section
      aria-label="Chain verification"
      className="rounded-[10px] border border-line bg-surface px-4 py-3 card-sheen"
    >
      <form
        onSubmit={(event) => {
          event.preventDefault();
          submit();
        }}
        className="flex flex-wrap items-end gap-x-3 gap-y-2"
      >
        <div className="min-w-0 flex-1 basis-64">
          <h2 className="type-bay-title text-[15px] leading-tight">Chain verification</h2>
          <p className="mt-1 text-[13px] leading-snug text-ink-2">
            Recomputes every link. Leave the bounds empty to check the whole log.
          </p>
        </div>
        <Field label="From seq" htmlFor="audit-verify-from" className="w-24">
          <Input
            id="audit-verify-from"
            type="number"
            min={1}
            step={1}
            inputMode="numeric"
            placeholder="1"
            value={fromText}
            onChange={(event) => setFromText(event.target.value)}
          />
        </Field>
        <Field label="To seq" htmlFor="audit-verify-to" className="w-24">
          <Input
            id="audit-verify-to"
            type="number"
            min={1}
            step={1}
            inputMode="numeric"
            placeholder="Last"
            value={toText}
            onChange={(event) => setToText(event.target.value)}
          />
        </Field>
        <Button variant="primary" type="submit" disabled={pending}>
          {pending ? 'Verifying…' : 'Verify the chain'}
        </Button>
      </form>

      {localProblem ? <ProblemBanner view={localProblem} className="mt-3" /> : null}
      {problem ? <ProblemBanner view={problem} className="mt-3" /> : null}

      {result ? (
        <div className="mt-3 rounded-[10px] border border-line bg-sunk/45 px-3 py-2.5">
          <div className="flex items-start gap-2.5">
            <ToneLamp
              tone={result.ok ? 'run' : 'fault'}
              size={16}
              label={result.ok ? 'Chain intact' : 'Chain broken'}
              className="mt-0.5"
            />
            <div className="min-w-0 flex-1">
              {result.ok ? (
                <p className="text-[13px] leading-snug text-ink">
                  Chain intact. Checked {result.checked} entries, seq {result.first_seq} to{' '}
                  {result.last_seq}.
                </p>
              ) : (
                <p className="text-[13px] leading-snug text-ink">
                  {brokenAt !== null
                    ? `Chain broken at seq ${brokenAt}.`
                    : 'Chain broken.'}
                </p>
              )}

              {!result.ok ? (
                <p className="mt-0.5 text-[13px] leading-snug text-ink-2">
                  {brokenReasonWords(result.reason)}
                </p>
              ) : null}

              {!result.ok ? (
                <p className="type-data mt-1 text-[12px] text-ink-2">
                  Checked {result.checked} entries, seq {result.first_seq} to {result.last_seq}.
                </p>
              ) : null}

              <div className="mt-1.5 flex flex-wrap items-center gap-2">
                <span className="type-code text-[11px] text-ink-2" title={result.head_hash}>
                  Head {shortHash(result.head_hash)}
                </span>
                <CopyButton value={result.head_hash} label="Copy head hash" />
                {brokenAt !== null ? (
                  <Button variant="default" onClick={() => onOpenEntry(brokenAt)}>
                    Open entry {brokenAt}
                  </Button>
                ) : null}
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}
