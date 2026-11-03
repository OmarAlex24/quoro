import { useMutation } from '@tanstack/react-query';
import { useState } from 'react';
import {
  createAgentKey,
  revokeAgentKey,
} from '../api/generated/agents/agents';
import { isApiProblem } from '../api/fetcher';
import { relativeTime } from '../lib/app-status';
import { shortId } from '../lib/agency-format';
import { isAgentKeyId } from '../lib/agents';
import { problemView, type ProblemView } from '../lib/problem';
import { Button } from './ui/button';
import { Input } from './ui/input';
import { NoticeLine, ProblemBanner } from './ui/problem-banner';
import { CopyButton } from './copy-button';
import { SecretOnce } from './secret-once';

const revokeProblem = (error: unknown): ProblemView =>
  isApiProblem(error) && error.status === 404
    ? { message: 'No key with that id belongs to this agent.' }
    : problemView(error, 'revoke keys');

/**
 * The keys block. A key only mints 5-minute access tokens, so it says that
 * once and then shows the one secret the API returns, straight from the
 * mutation result: never cached, stored or put in the URL.
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
  const [keyId, setKeyId] = useState('');
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const create = useMutation({
    mutationFn: () =>
      createAgentKey(agentId, {
        headers: { 'Idempotency-Key': crypto.randomUUID() },
      }),
    onSuccess: (created) => {
      setProblem(null);
      setNotice(`Created key ${shortId(created.id)} for ${agentName}.`);
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
      setNotice(`Revoked key ${shortId(id)}; tokens it minted stop within 5 minutes.`);
      setKeyId('');
      create.reset();
    },
    onError: (error) => {
      setNotice(null);
      setProblem(revokeProblem(error));
    },
  });

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
              variant="danger"
              disabled={revoke.isPending}
              onClick={() => {
                setNotice(null);
                setProblem(null);
                revoke.mutate(created.id);
              }}
            >
              Revoke this key
            </Button>
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

      {canWrite ? (
        <div className="mt-4 flex flex-col gap-3 border-t border-rail pt-3">
          <div>
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
          </div>

          <form
            className="flex flex-col gap-2 sm:flex-row sm:items-start"
            onSubmit={(event) => {
              event.preventDefault();
              const value = keyId.trim();
              if (!isAgentKeyId(value)) {
                setNotice(null);
                setProblem({
                  message:
                    'A key id starts with akey_ and has 26 more characters. Check the id and try again.',
                });
                return;
              }
              setNotice(null);
              setProblem(null);
              revoke.mutate(value);
            }}
          >
            <div className="flex min-w-0 flex-col gap-1.5 sm:flex-1">
              <label
                htmlFor="agent-key-id"
                className="text-[13px] text-ink-muted"
              >
                Key id
              </label>
              <Input
                id="agent-key-id"
                value={keyId}
                onChange={(event) => setKeyId(event.target.value)}
                placeholder="akey_…"
                spellCheck={false}
                autoComplete="off"
              />
              <p className="text-[13px] leading-snug text-ink-muted">
                To revoke an earlier key, enter its id.
              </p>
            </div>
            <Button
              variant="danger"
              type="submit"
              className="sm:mt-[26px]"
              disabled={revoke.isPending}
            >
              {revoke.isPending ? 'Revoking…' : 'Revoke key'}
            </Button>
          </form>
        </div>
      ) : null}
    </section>
  );
}
