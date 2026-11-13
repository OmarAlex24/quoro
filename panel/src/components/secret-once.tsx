import { secretParts } from '../lib/secret-words';
import { CopyButton } from './copy-button';

/**
 * A secret the API returns exactly once (an agent key, a webhook signing
 * secret). It is rendered from the mutation result the caller holds and
 * nowhere else: never put it in the query cache, the URL or browser storage,
 * and call the mutation's `reset()` when the plate closes so it is dropped.
 *
 * The value has the card's whole width, with the copy button in the corner of
 * its well. It wraps at its spaces and each word stays whole: a word that does
 * not fit beside the button starts on the next line, and only a word wider
 * than the whole line (a long token, a fingerprint) breaks inside itself.
 */
export function SecretOnce({
  label,
  secret,
  note,
  copyLabel = 'Copy secret',
}: {
  label: string;
  secret: string;
  note: string;
  copyLabel?: string;
}) {
  return (
    <div className="rounded-[10px] border border-line border-l-4 border-l-shift bg-surface px-3.5 py-3">
      <p className="type-body-medium text-[13px] text-ink">{label}</p>
      <div className="mt-2 flow-root rounded-[7px] border border-line-2 bg-sunk px-[10px] py-[7px]">
        {/*
         * The 28 px button gives up 3 px above and below, so it takes exactly
         * one 22 px line: centred on the first, clear of the second, 4 px
         * from the edges of the well.
         */}
        <CopyButton
          value={secret}
          label={copyLabel}
          variant="default"
          className="float-right -my-[3px] -mr-[6px] ml-3"
        />
        <code className="block whitespace-pre-wrap wrap-anywhere type-code text-[12.5px] leading-[22px] text-ink select-all">
          {secretParts(secret).map((part, index) =>
            part.space ? (
              part.text
            ) : (
              <span key={index} className="inline-block max-w-full">
                {part.text}
              </span>
            ),
          )}
        </code>
      </div>
      <p className="mt-2 text-[13px] leading-snug text-ink-2">{note}</p>
    </div>
  );
}
