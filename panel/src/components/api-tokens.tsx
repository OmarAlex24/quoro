import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useState, type FormEvent } from 'react';
import type { ApiToken } from '../api/generated/model';
import {
  createToken,
  getListTokensQueryKey,
  revokeToken,
  useListTokens,
} from '../api/generated/tokens/tokens';
import { isApiProblem } from '../api/fetcher';
import { relativeTime } from '../lib/app-status';
import { expiryWords, shortId } from '../lib/agency-format';
import { problemView, type ProblemView } from '../lib/problem';
import {
  DEFAULT_TOKEN_EXPIRY,
  TOKEN_EXPIRY_CHOICES,
  TOKEN_NAME_MAX,
  tokenExpiresAt,
  tokenLamp,
  tokenNameProblem,
  tokenState,
  tokenStateWords,
  type TokenExpiryChoice,
} from '../lib/tokens';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';
import { Field } from './ui/field';
import { Input } from './ui/input';
import { NoticeLine, ProblemBanner } from './ui/problem-banner';
import { CopyButton } from './copy-button';
import { SecretOnce } from './secret-once';
import { ToneLamp } from './tone-lamp';

const revokeProblem = (error: unknown): ProblemView =>
  isApiProblem(error) && error.status === 404
    ? { message: 'That token is gone or belongs to someone else.' }
    : problemView(error, 'revoke tokens');

const expiryLine = (token: ApiToken): string => {
  if (!token.expires_at) return 'never expires';
  const words = expiryWords(token.expires_at);
  return Date.parse(token.expires_at) > Date.now() ? `expires ${words}` : `expired ${words}`;
};

/** One token in the list: the lamp burns while it still signs requests in. */
function TokenRow({ token, onRevoke }: { token: ApiToken; onRevoke: (token: ApiToken) => void }) {
  const state = tokenState(token);
  const words = tokenStateWords[state];

  return (
    <li className="flex flex-wrap items-start gap-x-3 gap-y-2 py-3">
      <ToneLamp tone={tokenLamp(state)} size={20} label={words} className="mt-0.5" />

      <div className="min-w-0 flex-1 basis-[min(100%,260px)]">
        <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
          <span className="min-w-0 break-words type-body-medium text-[14px] text-ink">
            {token.name}
          </span>
          <span className="text-[13px] text-ink-2">{words}</span>
          <span className="inline-flex min-w-0 items-center gap-1">
            <span className="type-code text-[12px] text-ink" title={token.id}>
              {shortId(token.id)}
            </span>
            <CopyButton value={token.id} label="Copy token id" />
          </span>
        </div>

        <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-[13px] text-ink-2">
          <span className="type-data" title={token.created_at}>
            created {relativeTime(token.created_at)}
          </span>
          <span className="type-data" title={token.expires_at ?? undefined}>
            {expiryLine(token)}
          </span>
          <span className="type-data" title={token.last_used_at ?? undefined}>
            {token.last_used_at ? `last used ${relativeTime(token.last_used_at)}` : 'never used'}
          </span>
          {token.revoked_at ? (
            <span className="type-data" title={token.revoked_at}>
              revoked {relativeTime(token.revoked_at)}
            </span>
          ) : null}
        </div>
      </div>

      {state === 'active' ? (
        <Button variant="danger" size="sm" className="ml-auto" onClick={() => onRevoke(token)}>
          Revoke
        </Button>
      ) : null}
    </li>
  );
}

/**
 * The signed-in human's personal API tokens: the list, a form to create one,
 * and the one secret the API returns, rendered straight from the mutation
 * result and never cached, stored or put in the URL.
 */
export function ApiTokens() {
  const queryClient = useQueryClient();
  const [name, setName] = useState('');
  const [expiry, setExpiry] = useState<TokenExpiryChoice>(DEFAULT_TOKEN_EXPIRY);
  const [nameError, setNameError] = useState<string | null>(null);
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [revokeTarget, setRevokeTarget] = useState<ApiToken | null>(null);
  const [revokeError, setRevokeError] = useState<ProblemView | null>(null);

  const tokens = useListTokens({ limit: 200 }, { query: { staleTime: 15_000, retry: false } });

  const refresh = () => {
    void queryClient.invalidateQueries({ queryKey: getListTokensQueryKey() });
  };

  const create = useMutation({
    mutationFn: (draft: { name: string; expiresAt: string | null }) =>
      createToken(
        { name: draft.name, expires_at: draft.expiresAt },
        { headers: { 'Idempotency-Key': crypto.randomUUID() } },
      ),
    onSuccess: () => {
      setName('');
      setExpiry(DEFAULT_TOKEN_EXPIRY);
      setProblem(null);
      refresh();
    },
    onError: (error) => {
      if (isApiProblem(error) && error.code === 'validation_failed') {
        const detail = error.detail ?? 'The form needs a fix before it can be sent.';
        setProblem({ message: `${detail.charAt(0).toUpperCase()}${detail.slice(1)}.` });
        return;
      }
      setProblem(problemView(error, 'create tokens'));
    },
  });

  const revoke = useMutation({
    mutationFn: (token: ApiToken) => revokeToken(token.id),
    onSuccess: (_data, token) => {
      setRevokeError(null);
      setRevokeTarget(null);
      setNotice(`Revoked “${token.name}”. Its next request is refused.`);
      refresh();
    },
    onError: (error) => setRevokeError(revokeProblem(error)),
  });

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setNotice(null);
    setProblem(null);
    const invalid = tokenNameProblem(name);
    if (invalid) {
      setNameError(invalid);
      return;
    }
    setNameError(null);
    // A new token replaces the one on show: its secret is dropped for good.
    create.reset();
    create.mutate({ name: name.trim(), expiresAt: tokenExpiresAt(expiry) });
  };

  const items = tokens.data?.items ?? [];
  const listError =
    tokens.error !== null && tokens.error !== undefined
      ? problemView(tokens.error, 'read tokens')
      : null;
  const created = create.data;

  return (
    <section className="mt-4 rounded-[10px] border border-line bg-surface px-4 py-4 card-sheen sm:px-5">
      <h2 className="type-display text-[18px] leading-tight">API tokens</h2>
      <p className="mt-1 max-w-2xl text-[13px] leading-snug text-ink-2">
        A token signs the quoro CLI or a script in as you, with your role. Give each machine its
        own, so revoking one leaves the others working.
      </p>

      {created ? (
        <div className="mt-4 flex flex-col gap-2">
          <SecretOnce
            label={`New token “${created.name}”`}
            secret={created.token}
            note="Copy it now. You will not see it again: Quoro keeps only a hash of it. Set it as QUORO_TOKEN for the CLI."
          />
          <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
            <span className="text-[13px] text-ink-2">{expiryLine(created)}</span>
            <Button variant="default" onClick={() => create.reset()}>
              Done
            </Button>
          </div>
        </div>
      ) : null}

      {problem ? <ProblemBanner view={problem} className="mt-4" /> : null}
      {notice ? (
        <div className="mt-3">
          <NoticeLine>{notice}</NoticeLine>
        </div>
      ) : null}

      <form
        onSubmit={submit}
        noValidate
        className="mt-4 grid gap-3 sm:grid-cols-[minmax(0,1fr)_11rem_auto] sm:items-start"
      >
        <Field
          label="Name"
          htmlFor="token-name"
          hint={
            nameError ? (
              <span className="text-fault">{nameError}</span>
            ) : (
              'Where the token is used, for example laptop cli.'
            )
          }
        >
          <Input
            id="token-name"
            value={name}
            maxLength={TOKEN_NAME_MAX}
            onChange={(event) => {
              setName(event.target.value);
              setNameError(null);
            }}
            autoComplete="off"
            spellCheck={false}
            aria-invalid={nameError ? true : undefined}
          />
        </Field>

        <Field label="Expires" htmlFor="token-expiry">
          <select
            id="token-expiry"
            className="field"
            value={expiry}
            onChange={(event) => setExpiry(event.target.value as TokenExpiryChoice)}
          >
            {TOKEN_EXPIRY_CHOICES.map((choice) => (
              <option key={choice.key} value={choice.key}>
                {choice.key === 'never' ? choice.label : `In ${choice.label}`}
              </option>
            ))}
          </select>
        </Field>

        {/* A label-height spacer lines the button up with the fields beside it. */}
        <div className="flex flex-col gap-1.5">
          <span aria-hidden="true" className="invisible hidden type-body-medium text-[13px] sm:block">
            Create
          </span>
          <Button
            variant="primary"
            type="submit"
            className="self-start"
            disabled={create.isPending}
          >
            {create.isPending ? 'Creating…' : 'Create token'}
          </Button>
        </div>
      </form>

      {listError ? (
        <ProblemBanner view={listError} className="mt-4" />
      ) : tokens.isPending ? (
        <p className="mt-4 text-[13px] text-ink-2">Loading tokens…</p>
      ) : items.length === 0 ? (
        <p className="mt-4 border-y border-line py-3 text-[13px] text-ink-2">
          No tokens yet. Create one to use the CLI or the API.
        </p>
      ) : (
        <ul className="mt-4 flex flex-col divide-y divide-line border-y border-line">
          {items.map((token) => (
            <TokenRow
              key={token.id}
              token={token}
              onRevoke={(target) => {
                setNotice(null);
                setRevokeError(null);
                revoke.reset();
                setRevokeTarget(target);
              }}
            />
          ))}
        </ul>
      )}

      <Dialog
        open={revokeTarget !== null}
        onOpenChange={(open) => {
          if (!open && !revoke.isPending) {
            setRevokeTarget(null);
            setRevokeError(null);
          }
        }}
      >
        <DialogContent
          title="Revoke token"
          description={revokeTarget ? `“${revokeTarget.name}” stops working now.` : undefined}
        >
          <div className="flex flex-col gap-4">
            {revokeError ? <ProblemBanner view={revokeError} /> : null}
            <p className="text-[14px] leading-snug text-ink">
              The next request made with this token is refused, from the CLI or anywhere else, and
              terminal sessions it opened close at their next check. This cannot be undone.
            </p>
            <div className="flex flex-wrap justify-end gap-2 border-t border-line pt-4">
              <Button
                variant="default"
                disabled={revoke.isPending}
                onClick={() => {
                  setRevokeTarget(null);
                  setRevokeError(null);
                }}
              >
                Keep token
              </Button>
              <Button
                variant="danger-solid"
                disabled={revoke.isPending}
                onClick={() => {
                  if (revokeTarget) revoke.mutate(revokeTarget);
                }}
              >
                {revoke.isPending ? 'Revoking…' : 'Revoke token'}
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </section>
  );
}
