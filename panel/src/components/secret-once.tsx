import { CopyButton } from './copy-button';

/**
 * A secret the API returns exactly once (an agent key, a webhook signing
 * secret). It is rendered from the mutation result the caller holds and
 * nowhere else: never put it in the query cache, the URL or browser storage,
 * and call the mutation's `reset()` when the plate closes so it is dropped.
 */
export function SecretOnce({
  label,
  secret,
  note,
}: {
  label: string;
  secret: string;
  note: string;
}) {
  return (
    <div className="rounded-[6px] border border-rail border-l-4 border-l-lamp-shift bg-panel px-3.5 py-3">
      <p className="type-body-medium text-[13px] text-ink">{label}</p>
      <div className="mt-2 flex flex-wrap items-center gap-2">
        <code className="min-w-0 flex-1 break-all rounded-[4px] border border-rail bg-enamel px-2.5 py-1.5 type-data text-[14px] text-ink select-all">
          {secret}
        </code>
        <CopyButton value={secret} label="Copy secret" />
      </div>
      <p className="mt-2 text-[13px] leading-snug text-ink-muted">{note}</p>
    </div>
  );
}
