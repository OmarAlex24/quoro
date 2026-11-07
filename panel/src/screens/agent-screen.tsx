import { useMutation, useQueryClient } from '@tanstack/react-query';
import { getRouteApi, Link, useNavigate } from '@tanstack/react-router';
import { useState } from 'react';
import {
  deleteAgent,
  getGetAgentQueryKey,
  getListAgentsQueryKey,
  updateAgent,
  useGetAgent,
} from '../api/generated/agents/agents';
import { useListGrants } from '../api/generated/grants/grants';
import { isApiProblem } from '../api/fetcher';
import { agentDisabledPatch, agentLamp } from '../lib/agents';
import { relativeTime } from '../lib/app-status';
import { canManageAgents } from '../lib/capabilities';
import { problemView, type ProblemView } from '../lib/problem';
import { useSession } from '../session';
import { AgentKeys } from '../components/agent-keys';
import { EditAgentDialog } from '../components/agent-dialog';
import { CopyButton } from '../components/copy-button';
import { UserName } from '../components/identity-name';
import { Avatar } from '../components/avatar';
import { ToneLamp } from '../components/tone-lamp';
import { Button } from '../components/ui/button';
import { Dialog, DialogContent } from '../components/ui/dialog';
import { NoticeLine, ProblemBanner } from '../components/ui/problem-banner';
import { LoginScreen } from './login-screen';

const routeApi = getRouteApi('/agents/$agentId');

const backLink = (
  <Link
    to="/agents"
    className="text-[13px] text-ink-2 underline-offset-4 hover:text-ink hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
  >
    All agents
  </Link>
);

export function AgentScreen() {
  const { me, user, loading, unauthenticated } = useSession();
  const { agentId } = routeApi.useParams();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [editOpen, setEditOpen] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState(false);

  const agentQuery = useGetAgent(agentId, {
    query: { enabled: agentId !== '', retry: false },
  });

  const activeGrants = useListGrants(
    { agent_id: agentId, status: 'active' },
    { query: { enabled: agentId !== '', staleTime: 15_000, retry: false } },
  );

  const toggle = useMutation({
    mutationFn: (disabled: boolean) =>
      updateAgent(agentId, agentDisabledPatch(disabled)),
    onSuccess: (updated) => {
      setProblem(null);
      setNotice(updated.disabled ? `Disabled ${updated.name}` : `Enabled ${updated.name}`);
      void queryClient.invalidateQueries({ queryKey: getGetAgentQueryKey(agentId) });
      void queryClient.invalidateQueries({ queryKey: getListAgentsQueryKey() });
    },
    onError: (error, disabled) => {
      setNotice(null);
      setProblem(problemView(error, disabled ? 'disable agents' : 'enable agents'));
    },
  });

  const remove = useMutation({
    mutationFn: () => deleteAgent(agentId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: getListAgentsQueryKey() });
      void navigate({ to: '/agents' });
    },
    onError: (error) => {
      setConfirmDelete(false);
      setProblem(problemView(error, 'delete agents'));
    },
  });

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-4xl px-4 py-8 sm:px-6">
        <p className="text-[15px] text-ink-2">Loading…</p>
      </main>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  if (agentQuery.isPending) {
    return (
      <main className="mx-auto w-full max-w-4xl px-4 py-8 sm:px-6">
        <div className="h-[150px] rounded-[10px] border border-line bg-surface/60" />
      </main>
    );
  }

  if (agentQuery.error !== null && agentQuery.error !== undefined) {
    const missing = isApiProblem(agentQuery.error) && agentQuery.error.status === 404;
    return (
      <main className="mx-auto w-full max-w-4xl px-4 py-6 sm:px-6 sm:py-8">
        <h1 className="type-display text-[24px] leading-none">Agent</h1>
        {missing ? (
          <p className="mt-4 text-[15px] text-ink">That agent does not exist.</p>
        ) : (
          <ProblemBanner view={problemView(agentQuery.error, 'see this agent')} className="mt-4" />
        )}
        <div className="mt-4">{backLink}</div>
      </main>
    );
  }

  const agent = agentQuery.data;
  if (!agent) return null;

  const canManage = canManageAgents(me);
  const lamp = agentLamp(agent);
  const grantCount = activeGrants.data?.items.length;
  const grantError =
    activeGrants.error !== null && activeGrants.error !== undefined
      ? problemView(activeGrants.error, 'count its grants').message
      : null;

  return (
    <main className="mx-auto w-full max-w-4xl px-4 py-6 sm:px-6 sm:py-8">
      <div className="mb-3">{backLink}</div>

      <header className="rounded-[10px] border border-line bg-surface px-4 py-4 card-sheen sm:px-5">
        <div className="flex flex-wrap items-start gap-x-4 gap-y-3">
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2.5">
              <Avatar kind="agent" seed={agent.id} size="lg" />
              <ToneLamp tone={lamp.tone} label={lamp.statusText} />
              <h1 className="type-display min-w-0 truncate text-[26px] leading-none">
                {agent.name}
              </h1>
              <span className="type-data text-[13px] text-ink-2">{lamp.statusText}</span>
            </div>
            {agent.description ? (
              <p className="mt-2 max-w-2xl text-[14px] leading-snug text-ink-2">
                {agent.description}
              </p>
            ) : (
              <p className="mt-2 text-[13px] text-ink-2">
                No description yet. Edit the agent to say what it is for.
              </p>
            )}
          </div>

          {canManage ? (
            <div className="flex flex-wrap items-center gap-2">
              <Button variant="default" onClick={() => setEditOpen(true)}>
                Edit
              </Button>
              <Button
                variant="default"
                disabled={toggle.isPending}
                onClick={() => {
                  setNotice(null);
                  setProblem(null);
                  toggle.mutate(!agent.disabled);
                }}
              >
                {toggle.isPending
                  ? 'Saving…'
                  : agent.disabled
                    ? 'Enable agent'
                    : 'Disable agent'}
              </Button>
              <Button
                variant="danger"
                disabled={remove.isPending}
                onClick={() => setConfirmDelete(true)}
              >
                Delete agent
              </Button>
            </div>
          ) : null}
        </div>

        {canManage ? (
          <p className="mt-2 text-[13px] leading-snug text-ink-2">
            A disabled agent can't get new access tokens, so it is locked out within 5
            minutes.
          </p>
        ) : (
          <p className="mt-2 text-[13px] text-ink-2">
            Your role can read this agent but not change it.
          </p>
        )}

        <dl className="type-data mt-4 grid grid-cols-1 gap-x-6 gap-y-1 border-t border-line pt-3 text-[13px] sm:grid-cols-2">
          <div className="flex gap-2">
            <dt className="text-ink-2">Model</dt>
            <dd className="min-w-0 truncate text-ink">{agent.model}</dd>
          </div>
          <div className="flex gap-2">
            <dt className="text-ink-2">Owner</dt>
            <dd className="min-w-0 truncate text-ink">
              <UserName id={agent.owner_user_id} />
            </dd>
          </div>
          <div className="flex gap-2">
            <dt className="text-ink-2">Created</dt>
            <dd className="text-ink" title={agent.created_at}>
              {relativeTime(agent.created_at)}
            </dd>
          </div>
          <div className="flex gap-2">
            <dt className="text-ink-2">Updated</dt>
            <dd className="text-ink" title={agent.updated_at}>
              {relativeTime(agent.updated_at)}
            </dd>
          </div>
          <div className="flex min-w-0 flex-wrap items-center gap-2 sm:col-span-2">
            <dt className="text-ink-2">Id</dt>
            <dd className="type-code min-w-0 break-all text-ink" title={agent.id}>
              {agent.id}
            </dd>
            <CopyButton value={agent.id} label="Copy id" />
          </div>
        </dl>
      </header>

      {notice ? <div className="mt-2">{<NoticeLine>{notice}</NoticeLine>}</div> : null}
      {problem ? <ProblemBanner view={problem} className="mt-3" /> : null}

      <section className="mt-5 rounded-[10px] border border-line bg-surface px-4 py-4 sm:px-5">
        <h2 className="type-display-light text-[15px] leading-none text-ink">Grants</h2>
        <p className="mt-2 text-[13px] leading-snug text-ink-2">
          {grantError
            ? grantError
            : grantCount === undefined
              ? 'Counting its active grants…'
              : grantCount === 0
                ? 'No active grants. Without one the agent can mint tokens but do nothing.'
                : grantCount === 1
                  ? '1 active grant.'
                  : `${grantCount} active grants.`}
        </p>
        <div className="mt-3 flex flex-wrap gap-x-4 gap-y-2 text-[13px]">
          <Link
            to="/grants"
            search={{ agent_id: agent.id }}
            className="text-accent underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
          >
            See its grants
          </Link>
          <Link
            to="/audit"
            search={{ actor_id: agent.id }}
            className="text-accent underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
          >
            See what it did
          </Link>
        </div>
      </section>

      <section className="mt-5 rounded-[10px] border border-line bg-surface px-4 py-4 sm:px-5">
        <AgentKeys key={agent.id} agentId={agent.id} agentName={agent.name} canWrite={canManage} />
      </section>

      {canManage ? (
        <>
          <EditAgentDialog
            key={`${agent.id}-${agent.updated_at}`}
            open={editOpen}
            onOpenChange={setEditOpen}
            agent={agent}
            onSaved={(updated) => {
              setProblem(null);
              setNotice(`Saved changes to ${updated.name}`);
            }}
          />

          <Dialog open={confirmDelete} onOpenChange={setConfirmDelete}>
            <DialogContent
              title="Delete agent"
              description="Deletes the agent and revokes its keys and tokens. Its grants and audit entries stay readable."
            >
              <div className="flex flex-wrap justify-end gap-2">
                <Button variant="default" onClick={() => setConfirmDelete(false)}>
                  Keep the agent
                </Button>
                <Button
                  variant="danger-solid"
                  disabled={remove.isPending}
                  onClick={() => remove.mutate()}
                >
                  {remove.isPending ? 'Deleting…' : 'Delete agent'}
                </Button>
              </div>
            </DialogContent>
          </Dialog>
        </>
      ) : null}
    </main>
  );
}
