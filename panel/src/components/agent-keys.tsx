import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import type { AgentKey } from '../api/generated/model';
import {
  createAgentKey,
  getListAgentKeysQueryKey,
  revokeAgentKey,
  useListAgentKeys,
} from '../api/generated/agents/agents';
import { isApiProblem } from '../api/fetcher';
import { relativeTime } from '../lib/app-status';
import { shortId } from '../lib/agency-format';
import { problemView, type ProblemView } from '../lib/problem';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';
import { NoticeLine, ProblemBanner } from './ui/problem-banner';
import { CopyButton } from './copy-button';
import { SecretOnce } from './secret-once';
import { ToneLamp } from './tone-lamp';

const revokeProblem = (error: unknown): ProblemView =>
  isApiProblem(error) && error.status === 404
    ? { message: 'No key with that id belongs to this agent.' }
    : problemView(error, 'revoke keys');

/** One key in the list: the lamp burns while it can still mint tokens. */
function KeyRow({
  agentKey,
  canWrite,
  onRevoke,
}: {
  agentKey: AgentKey;
  canWrite: boolean;
  onRevoke: (agentKey: AgentKey) => void;
}) {
  const revoked = agentKey.revoked_at !== null && agentKey.revoked_at !== undefined;
  const statusWords = revoked ? 'Revoked' : 'Active';

  return (
    <li className="flex flex-wrap items-start gap-x-3 gap-y-2 py-3">
      <ToneLamp
        tone={revoked ? 'idle' : 'run'}
        size={20}
        label={statusWords}
        className="mt-0.5"
      />

      <div className="min-w-0 flex-1 basis-[min(100%,260px)]">
        <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
          <span className="type-body-medium text-[14px] text-ink">{statusWords}</span>
          <span className="inline-flex min-w-0 items-center gap-1">
            <span className="type-data text-[13px] text-ink" title={agentKey.id}>
              {shortId(agentKey.id)}
            </span>
            <CopyButton value={agentKey.id} label="Copy key id" />
          </span>
          <span className="type-data text-[13px] text-ink-muted" title={agentKey.created_at}>
            created {relativeTime(agentKey.created_at)}
          </span>
        </div>

        <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-[13px] text-ink-muted">
          <span className="type-data">
            {agentKey.last_used_at
              ? `last used ${relativeTime(agentKey.last_used_at)}`
              : 'last used never'}
          </span>
          {agentKey.revoked_at ? (
            <span className="type-data" title={agentKey.revoked_at}>
              revoked {relativeTime(agentKey.revoked_at)}
            </span>
          ) : null}
        </div>
      </div>

      {canWrite && !revoked ? (
        <Button variant="danger" size="compact" className="ml-auto" onClick={() => onRevoke(agentKey)}>
          Revoke
        </Button>
      ) : null}
    </li>
  );
}

/**
 * The keys block: the agent's keys newest first, with the one secret the API
 * returns straight from the mutation result and never cached, stored or put in
 * the URL. A key only mints 5-minute access tokens; grants decide what it may do.
 */
export function AgentKeys({
  agentId,
  agentName,
  canWrite,
}: {
  agentId: string;
  agentName: string;
  canWrite: boolean;
}) {
  const queryClient = useQueryClient();
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [revokeTarget, setRevokeTarget] = useState<AgentKey | null>(null);
  const [revokeError, setRevokeError] = useState<ProblemView | null>(null);

  const keys = useListAgentKeys(
    agentId,
    { limit: 200 },
    { query: { enabled: agentId !== '', staleTime: 15_000, retry: false } },
  );

  const create = useMutation({
    mutationFn: () =>
      createAgentKey(agentId, {
        headers: { 'Idempotency-Key': crypto.randomUUID() },
      }),
    onSuccess: (created) => {
      setProblem(null);
      setNotice(`Created key ${shortId(created.id)} for ${agentName}.`);
      void queryClient.invalidateQueries({ queryKey: getListAgentKeysQueryKey(agentId) });
    },
    onError: (error) => {
      setNotice(null);
      setProblem(problemView(error, 'create keys'));
    },
  });

  const revoke = useMutation({
    mutationFn: (id: string) => revokeAgentKey(agentId, id),
    onSuccess: (_data, id) => {
      setProblem(null);
      setRevokeError(null);
      setNotice(`Revoked key ${shortId(id)}; tokens it minted stop within 5 minutes.`);
      setRevokeTarget(null);
      void queryClient.invalidateQueries({ queryKey: getListAgentKeysQueryKey(agentId) });
    },
    onError: (error) => {
      setNotice(null);
      setRevokeError(revokeProblem(error));
    },
  });

  const items = keys.data?.items ?? [];
  const listError =
    keys.error !== null && keys.error !== undefined
      ? problemView(keys.error, 'read keys')
      : null;
  const created = create.data;

  return (
    <section>
      <h2 className="type-display-light text-[15px] leading-none text-ink">Keys</h2>
      <p className="mt-2 text-[13px] leading-snug text-ink-muted">
        A key only mints 5-minute access tokens; what the agent may do comes from its grants.
      </p>

      {problem ? <ProblemBanner view={problem} className="mt-3" /> : null}
      {notice ? (
        <div className="mt-3">
          <NoticeLine>{notice}</NoticeLine>
        </div>
      ) : null}

      {created ? (
        <div className="mt-3 flex flex-col gap-2">
          <SecretOnce
            label={`New key for ${agentName}`}
            secret={created.key}
            note="Copy it now. Quoro shows it only once."
          />
          <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
            <span className="type-data text-[13px] text-ink" title={created.id}>
              {shortId(created.id)}
            </span>
            <CopyButton value={created.id} label="Copy key id" />
            <span className="text-[13px] text-ink-muted" title={created.created_at}>
              created {relativeTime(created.created_at)}
            </span>
            <Button
              variant="secondary"
              onClick={() => {
                setNotice(null);
                setProblem(null);
                create.reset();
              }}
            >
              Done
            </Button>
          </div>
        </div>
      ) : null}

      {listError ? (
        <ProblemBanner view={listError} className="mt-2" />
      ) : keys.isPending ? (
        <p className="mt-3 text-[13px] text-ink-muted">Loading keys…</p>
      ) : items.length === 0 ? (
        <p className="mt-3 border-y border-rail py-3 text-[13px] text-ink-muted">
          No keys yet
        </p>
      ) : (
        <ul className="mt-3 flex flex-col divide-y divide-rail border-y border-rail">
          {items.map((agentKey) => (
            <KeyRow
              key={agentKey.id}
              agentKey={agentKey}
              canWrite={canWrite}
              onRevoke={(target) => {
                setNotice(null);
                setProblem(null);
                setRevokeError(null);
                revoke.reset();
                setRevokeTarget(target);
              }}
            />
          ))}
        </ul>
      )}

      {canWrite ? (
        <div className="mt-4 flex flex-wrap items-center gap-x-3 gap-y-2 border-t border-rail pt-3">
          <Button
            variant="primary"
            disabled={create.isPending}
            onClick={() => {
              setNotice(null);
              setProblem(null);
              create.mutate();
            }}
          >
            {create.isPending ? 'Creating…' : 'Create a key'}
          </Button>
          <p className="text-[13px] text-ink-muted">
            Use a new key per agent host, so revoking one leaves the others working.
          </p>
        </div>
      ) : null}

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
          title="Revoke key"
          description={
            revokeTarget
              ? `Key ${shortId(revokeTarget.id)} for ${agentName} ends now.`
              : undefined
          }
        >
          <div className="flex flex-col gap-4">
            {revokeError ? <ProblemBanner view={revokeError} /> : null}
            <p className="text-[14px] leading-snug text-ink">
              Tokens this key already minted stop working within 5 minutes. The agent needs
              another key to mint new ones.
            </p>
            <div className="flex flex-wrap justify-end gap-2 border-t border-rail pt-4">
              <Button
                variant="secondary"
                disabled={revoke.isPending}
                onClick={() => {
                  setRevokeTarget(null);
                  setRevokeError(null);
                }}
              >
                Keep key
              </Button>
              <Button
                variant="danger"
                disabled={revoke.isPending}
                onClick={() => {
                  if (revokeTarget) revoke.mutate(revokeTarget.id);
                }}
              >
                {revoke.isPending ? 'Revoking…' : 'Revoke key'}
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </section>
  );
}
