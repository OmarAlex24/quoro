import { useMutation, useQueryClient } from '@tanstack/react-query';
import { getRouteApi, Link, useNavigate } from '@tanstack/react-router';
import { useMemo, useState } from 'react';
import {
  deleteAgent,
  getGetAgentQueryKey,
  getListAgentsQueryKey,
  updateAgent,
  useGetAgent,
  useListAgentKeys,
} from '../api/generated/agents/agents';
import { useListGrants } from '../api/generated/grants/grants';
import type { Agent, AuditRecord, Grant } from '../api/generated/model';
import { isApiProblem } from '../api/fetcher';
import { groupReads, recordTargets, type RecordContext } from '../lib/activity';
import { agentDisabledPatch, agentStatus } from '../lib/agents';
import { relativeTime } from '../lib/app-status';
import {
  canDecideApprovals,
  canManageAgents,
  canManageGrants,
  canResolveIncidents,
} from '../lib/capabilities';
import { grantKindWords, grantMeter } from '../lib/grants';
import { problemView, type ProblemView } from '../lib/problem';
import { agentTab, type AgentTab } from '../lib/routes';
import { useAgentCalls } from '../lib/use-agent-calls';
import { useNow } from '../lib/use-now';
import { useKeptNames } from '../lib/use-record-context';
import { cn } from '../lib/utils';
import { useSession } from '../session';
import { ActivityRow, ReadsRow } from '../components/activity-row';
import { AgentCard, useAgentDirectory, type AgentFacts } from '../components/agent-card';
import { CreateAgentDialog, EditAgentDialog } from '../components/agent-dialog';
import { AgentKeys } from '../components/agent-keys';
import { Avatar } from '../components/avatar';
import { CopyButton } from '../components/copy-button';
import { DitherMeter } from '../components/dither-meter';
import { GrantCards } from '../components/grant-card';
import { GrantDialog } from '../components/grant-dialog';
import { UserName } from '../components/identity-name';
import { DecideBox, IncidentRun } from '../components/incident-run';
import { LampDot } from '../components/lamp';
import { Button } from '../components/ui/button';
import { Dialog, DialogContent } from '../components/ui/dialog';
import { NoticeLine, ProblemBanner } from '../components/ui/problem-banner';
import { Tag } from '../components/ui/tag';
import { LoginScreen } from './login-screen';

const agentsRoute = getRouteApi('/shell/agents');

const linkClass =
  'text-accent underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent';

/** How many of its records the Activity tab shows when no incident is open. */
const RECENT = 8;

/**
 * `/agents?agent=`: the agents down the left, the one open on the right with
 * its tabs. Activity tells the incident it handles, or what it did lately;
 * Grants, Keys and Settings are what a person changes about it.
 */
export function AgentScreen({ agentId }: { agentId: string }) {
  const { me, user, loading, unauthenticated } = useSession();
  const search = agentsRoute.useSearch();
  const navigate = useNavigate();
  const tab = agentTab(search);
  const [createOpen, setCreateOpen] = useState(false);

  const directory = useAgentDirectory();
  const agentQuery = useGetAgent(agentId, { query: { enabled: agentId !== '', retry: false } });

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-6xl px-4 py-8 sm:px-6">
        <p className="text-[15px] text-ink-2">Loading…</p>
      </main>
    );
  }
  if (unauthenticated || !user) return <LoginScreen />;

  const canManage = canManageAgents(me);
  const agents = directory.agents.data?.items ?? [];

  return (
    <main className="agents-split mx-auto w-full max-w-6xl px-4 pb-10 sm:px-6">
      <aside className="agents-list" aria-label="Agents">
        <div className="list-h">
          <span>{agents.length === 1 ? '1 agent' : `${agents.length} agents`}</span>
          {canManage ? (
            <Button variant="default" size="sm" className="ml-auto" onClick={() => setCreateOpen(true)}>
              New agent
            </Button>
          ) : null}
        </div>
        <div className="acards">
          {agents.map((agent) => (
            <AgentCard
              key={agent.id}
              agent={agent}
              facts={directory.facts(agent.id)}
              selected={agent.id === agentId}
            />
          ))}
        </div>
      </aside>

      <section className="agent-pane" aria-label="Agent">
        {agentQuery.isPending ? (
          <div className="h-[160px] rounded-[10px] border border-line bg-surface/60" />
        ) : agentQuery.error ? (
          <div>
            <Link to="/agents" className={cn('text-[13px]', linkClass)}>
              All agents
            </Link>
            {isApiProblem(agentQuery.error) && agentQuery.error.status === 404 ? (
              <p className="mt-3 text-[15px] text-ink">That agent does not exist.</p>
            ) : (
              <ProblemBanner view={problemView(agentQuery.error, 'see this agent')} className="mt-3" />
            )}
          </div>
        ) : agentQuery.data ? (
          <AgentPane
            agent={agentQuery.data}
            tab={tab}
            facts={directory.facts(agentQuery.data.id)}
            ctx={directory.ctx}
          />
        ) : null}
      </section>

      {canManage ? (
        <CreateAgentDialog
          open={createOpen}
          onOpenChange={setCreateOpen}
          ownerUserId={user.id}
          onCreated={(agent) => {
            void navigate({ to: '/agents', search: { agent: agent.id } });
          }}
        />
      ) : null}
    </main>
  );
}

function AgentPane({
  agent,
  tab,
  facts,
  ctx,
}: {
  agent: Agent;
  tab: AgentTab | 'activity';
  facts: AgentFacts;
  ctx: RecordContext;
}) {
  const { me } = useSession();
  const status = agentStatus({
    agent,
    activeGrants: facts.grants.filter((grant) => grant.status === 'active').length,
    pending: facts.pending,
    pendingTargetName: facts.pendingTargetName,
    incident: facts.incident,
  });
  const activeCount = facts.grants.filter((grant) => grant.status === 'active').length;

  const tabLink = (value: AgentTab | undefined, words: string, count?: number) => {
    const on = (value ?? 'activity') === tab;
    return (
      <Link
        to="/agents"
        search={value ? { agent: agent.id, tab: value } : { agent: agent.id }}
        className={cn(on && 'on')}
        aria-current={on ? 'page' : undefined}
      >
        {words}
        {count !== undefined && count > 0 ? (
          <>
            <span className="count count-soft" aria-hidden="true">
              {count}
            </span>
            <span className="sr-only">, {count}</span>
          </>
        ) : null}
      </Link>
    );
  };

  return (
    <>
      <div className="agent-top">
        <Link to="/agents" className={cn('text-[13px]', linkClass)}>
          All agents
        </Link>
        <div className="agent-id">
          <Avatar kind="agent" seed={agent.id} size="lg" />
          <span className="min-w-0">
            <b>{agent.name}</b>
            <small>
              {agent.model} · owned by <UserName id={agent.owner_user_id} />
            </small>
          </span>
        </div>
        <p className={cn('acard-state', status.attention && 'acard-state-ask')}>
          <LampDot ring={status.tone} core={status.tone} pulse={status.pulse} size={14} />
          <span className="min-w-0">{status.phrase}</span>
        </p>
      </div>

      <nav className="tabs agent-tabs" aria-label={`${agent.name}`}>
        {tabLink(undefined, 'Activity')}
        {tabLink('grants', 'Grants', activeCount)}
        {tabLink('keys', 'Keys')}
        {tabLink('settings', 'Settings')}
      </nav>

      <div className="agent-body">
        {tab === 'grants' ? (
          <AgentGrants agent={agent} canManage={canManageGrants(me)} />
        ) : tab === 'keys' ? (
          <div className="box px-4 py-4">
            <AgentKeys agentId={agent.id} agentName={agent.name} canWrite={canManageAgents(me)} />
          </div>
        ) : tab === 'settings' ? (
          <AgentSettings key={agent.id} agent={agent} />
        ) : facts.incident ? (
          <IncidentRun
            key={facts.incident.id}
            agent={agent}
            incident={facts.incident}
            ctx={ctx}
            canDecide={canDecideApprovals(me)}
            canResolve={canResolveIncidents(me)}
          />
        ) : (
          <AgentOverview agent={agent} facts={facts} ctx={ctx} />
        )}
      </div>
    </>
  );
}

/** No incident open: what the agent did lately, the grants it holds and its keys. */
function AgentOverview({ agent, facts, ctx: records }: { agent: Agent; facts: AgentFacts; ctx: RecordContext }) {
  const { me } = useSession();
  const navigate = useNavigate();
  const now = useNow(true, 30_000);
  const [grantOpen, setGrantOpen] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const calls = useAgentCalls(agent.id);
  const keys = useListAgentKeys(agent.id, { limit: 200 }, { query: { staleTime: 15_000, retry: false } });

  // Newest first, as Activity reads: the first page always holds the latest.
  // A run of the same read is one row, so a polling agent's request and the
  // decision on it stay in view: fold first, then keep the newest rows.
  const recentItems = useMemo(
    () => (calls.data ? groupReads(calls.data.records).slice(0, RECENT) : []),
    [calls.data],
  );
  const recent = useMemo(
    () => recentItems.flatMap((item) => (item.kind === 'one' ? [item.record] : item.records)),
    [recentItems],
  );
  const [openRuns, setOpenRuns] = useState<ReadonlySet<string>>(() => new Set());
  const targets = useMemo(() => recordTargets(recent, records), [recent, records]);
  const ctx = useKeptNames(records, targets, recent);
  const active = facts.grants.filter((grant) => grant.status === 'active');
  const liveKeys = (keys.data?.items ?? []).filter((key) => !key.revoked_at);
  const lastUsed = liveKeys
    .map((key) => key.last_used_at)
    .filter((at): at is string => Boolean(at))
    .sort()
    .at(-1);
  const waiting = facts.pending[0];
  const canManage = canManageGrants(me);

  return (
    <div className="grid gap-5">
      {notice ? <NoticeLine>{notice}</NoticeLine> : null}
      {problem ? <ProblemBanner view={problem} /> : null}

      {waiting ? (
        <section aria-label="Waiting on you" className="grid gap-3">
          <h3 className="sec-h">
            Waiting on you
            {facts.pending.length > 1 ? (
              <Link to="/approvals" className={cn('ml-auto text-[12.5px] font-normal', linkClass)}>
                {facts.pending.length - 1} more in Approvals
              </Link>
            ) : null}
          </h3>
          <DecideBox
            approval={waiting}
            canDecide={canDecideApprovals(me)}
            now={now}
            onConflict={(view) => {
              setNotice(null);
              setProblem(view);
            }}
            onDecided={(kind) => {
              setProblem(null);
              setNotice(kind === 'approve' ? 'Approved. The agent has 10 minutes to repeat the exact call.' : 'Denied.');
            }}
          />
        </section>
      ) : null}

      <section aria-label="Recent activity" className="grid gap-2">
        <h3 className="sec-h">
          Recent activity
          <Link to="/activity" search={{ actor_id: agent.id }} className={cn('ml-auto text-[12.5px] font-normal', linkClass)}>
            All of it in Activity
          </Link>
        </h3>
        {calls.isPending ? (
          <div className="h-[120px] rounded-[10px] border border-line bg-surface/60" />
        ) : calls.isError ? (
          <ProblemBanner view={problemView(calls.error, 'read the audit log')} />
        ) : recent.length === 0 ? (
          <p className="box px-4 py-3 text-[13px] text-ink-2">Nothing recorded yet. Its calls will show here.</p>
        ) : (
          <ol className="feed feed-box">
            {recentItems.map((item) => {
              const row = (record: AuditRecord, nested = false) => (
                <ActivityRow
                  key={record.seq}
                  record={record}
                  ctx={ctx}
                  selected={false}
                  broken={false}
                  withDay
                  nested={nested}
                  onOpen={() => void navigate({ to: '/activity', search: { actor_id: agent.id, seq: record.seq } })}
                />
              );
              if (item.kind === 'one') return row(item.record);
              return (
                <ReadsRow
                  key={item.key}
                  records={item.records}
                  ctx={ctx}
                  withDay
                  open={openRuns.has(item.key)}
                  onToggle={() =>
                    setOpenRuns((current) => {
                      const next = new Set(current);
                      if (next.has(item.key)) next.delete(item.key);
                      else next.add(item.key);
                      return next;
                    })
                  }
                >
                  {item.records.map((record) => row(record, true))}
                </ReadsRow>
              );
            })}
          </ol>
        )}
      </section>

      <section aria-label="Active grants" className="grid gap-2">
        <h3 className="sec-h">
          Grants
          <Link to="/agents" search={{ agent: agent.id, tab: 'grants' }} className={cn('ml-auto text-[12.5px] font-normal', linkClass)}>
            Every grant
          </Link>
        </h3>
        {active.length === 0 ? (
          <div className="box flex flex-wrap items-center gap-3 px-4 py-3">
            <p className="min-w-0 flex-1 text-[13px] text-ink-2">
              No active grants. Without one it can get tokens but do nothing.
            </p>
            {canManage && !agent.disabled ? (
              <Button variant="default" size="sm" onClick={() => setGrantOpen(true)}>
                Issue a grant
              </Button>
            ) : null}
          </div>
        ) : (
          <ul className="box box-list">
            {active.map((grant) => (
              <ActiveGrantRow key={grant.id} grant={grant} now={now} />
            ))}
          </ul>
        )}
      </section>

      <section aria-label="Keys" className="grid gap-2">
        <h3 className="sec-h">
          Keys
          <Link to="/agents" search={{ agent: agent.id, tab: 'keys' }} className={cn('ml-auto text-[12.5px] font-normal', linkClass)}>
            Manage keys
          </Link>
        </h3>
        <p className="box px-4 py-3 text-[13px] text-ink-2">
          {keys.isPending
            ? 'Reading its keys…'
            : keys.isError
              ? problemView(keys.error, 'see its keys').message
              : liveKeys.length === 0
                ? 'No active key. It cannot get a token until it has one.'
                : `${liveKeys.length === 1 ? '1 active key' : `${liveKeys.length} active keys`} · ${
                    lastUsed ? `last used ${relativeTime(lastUsed, now)}` : 'never used'
                  }`}
        </p>
      </section>

      {canManage ? (
        <GrantDialog
          open={grantOpen}
          onOpenChange={setGrantOpen}
          preselectedAgentId={agent.id}
          onIssued={() => setNotice(`Issued a grant to ${agent.name}`)}
        />
      ) : null}
    </div>
  );
}

function ActiveGrantRow({ grant, now }: { grant: Grant; now: number }) {
  const meter = grantMeter(grant, now);
  const shown = grant.scopes.slice(0, 3);
  return (
    <li className="grant-line">
      {meter ? (
        <DitherMeter label={grantKindWords[grant.kind]} words={meter.words} share={meter.share} />
      ) : (
        <p className="text-[12.5px] font-[560]">{grantKindWords[grant.kind]}</p>
      )}
      <p className="flex flex-wrap items-center gap-1.5 text-[12px] text-ink-2">
        {shown.map((scope) => (
          <Tag key={scope} tone="code">
            {scope}
          </Tag>
        ))}
        {grant.scopes.length > shown.length ? <span>+{grant.scopes.length - shown.length}</span> : null}
      </p>
    </li>
  );
}

/** Every grant the agent holds or held, newest first among the active ones. */
function AgentGrants({ agent, canManage }: { agent: Agent; canManage: boolean }) {
  const [dialogOpen, setDialogOpen] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const grants = useListGrants(
    { agent_id: agent.id, limit: 200 },
    { query: { refetchInterval: 10_000, retry: false } },
  );
  const items = grants.data?.items ?? [];
  const active = items
    .filter((grant) => grant.status === 'active')
    .sort((left, right) => left.expires_at.localeCompare(right.expires_at));
  const past = items
    .filter((grant) => grant.status !== 'active')
    .sort((left, right) => right.created_at.localeCompare(left.created_at));

  return (
    <div className="grid gap-5">
      <div className="flex flex-wrap items-center gap-3">
        <p className="min-w-0 flex-1 text-[13px] text-ink-2">
          What {agent.name} may do, on which resources, until when. Every grant expires.
        </p>
        {canManage && !agent.disabled ? (
          <Button variant="primary" onClick={() => setDialogOpen(true)}>
            Issue a grant
          </Button>
        ) : null}
      </div>
      {notice ? <NoticeLine>{notice}</NoticeLine> : null}
      {grants.error ? <ProblemBanner view={problemView(grants.error, 'see grants')} /> : null}

      <section aria-label="Active grants" className="grid gap-2">
        <h3 className="sec-h">Active</h3>
        {grants.isPending ? (
          <div className="h-[140px] rounded-[10px] border border-line bg-surface/60" />
        ) : active.length === 0 ? (
          <p className="box px-4 py-3 text-[13px] text-ink-2">
            No active grants. Without one it can get tokens but do nothing.
          </p>
        ) : (
          <GrantCards grants={active} canManage={canManage} onNotice={setNotice} />
        )}
      </section>

      {past.length > 0 ? (
        <section aria-label="Past grants" className="grid gap-2">
          <h3 className="sec-h">Past</h3>
          <GrantCards grants={past} canManage={canManage} onNotice={setNotice} />
        </section>
      ) : null}

      {canManage ? (
        <GrantDialog
          open={dialogOpen}
          onOpenChange={setDialogOpen}
          preselectedAgentId={agent.id}
          onIssued={() => setNotice(`Issued a grant to ${agent.name}`)}
        />
      ) : null}
    </div>
  );
}

/** What the agent is, and the changes only a person makes: edit, disable, delete. */
function AgentSettings({ agent }: { agent: Agent }) {
  const { me } = useSession();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const canManage = canManageAgents(me);
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [editOpen, setEditOpen] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState(false);

  const toggle = useMutation({
    mutationFn: (disabled: boolean) => updateAgent(agent.id, agentDisabledPatch(disabled)),
    onSuccess: (updated) => {
      setProblem(null);
      setNotice(updated.disabled ? `Disabled ${updated.name}` : `Enabled ${updated.name}`);
      void queryClient.invalidateQueries({ queryKey: getGetAgentQueryKey(agent.id) });
      void queryClient.invalidateQueries({ queryKey: getListAgentsQueryKey() });
    },
    onError: (error, disabled) => {
      setNotice(null);
      setProblem(problemView(error, disabled ? 'disable agents' : 'enable agents'));
    },
  });

  const remove = useMutation({
    mutationFn: () => deleteAgent(agent.id),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: getListAgentsQueryKey() });
      void navigate({ to: '/agents' });
    },
    onError: (error) => {
      setConfirmDelete(false);
      setProblem(problemView(error, 'delete agents'));
    },
  });

  return (
    <div className="grid gap-4">
      {notice ? <NoticeLine>{notice}</NoticeLine> : null}
      {problem ? <ProblemBanner view={problem} /> : null}

      <section className="box px-4 py-4 sm:px-5">
        {agent.description ? (
          <p className="max-w-2xl text-[14px] leading-snug text-ink">{agent.description}</p>
        ) : (
          <p className="text-[13px] text-ink-2">No description yet. Edit the agent to say what it is for.</p>
        )}
        <dl className="type-data mt-4 grid grid-cols-1 gap-x-6 gap-y-1.5 border-t border-line pt-3 text-[13px] sm:grid-cols-2">
          <div className="flex min-w-0 gap-2">
            <dt className="text-ink-2">Model</dt>
            <dd className="min-w-0 truncate text-ink">{agent.model}</dd>
          </div>
          <div className="flex min-w-0 gap-2">
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
      </section>

      {canManage ? (
        <section className="box grid gap-3 px-4 py-4 sm:px-5">
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
              {toggle.isPending ? 'Saving…' : agent.disabled ? 'Enable agent' : 'Disable agent'}
            </Button>
            <Button variant="danger" className="ml-auto" disabled={remove.isPending} onClick={() => setConfirmDelete(true)}>
              Delete agent
            </Button>
          </div>
          <p className="text-[13px] leading-snug text-ink-2">
            A disabled agent can't get new access tokens, so it is locked out within 5 minutes.
          </p>
        </section>
      ) : (
        <p className="text-[13px] text-ink-2">Your role can read this agent but not change it.</p>
      )}

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
                <Button variant="danger-solid" disabled={remove.isPending} onClick={() => remove.mutate()}>
                  {remove.isPending ? 'Deleting…' : 'Delete agent'}
                </Button>
              </div>
            </DialogContent>
          </Dialog>
        </>
      ) : null}
    </div>
  );
}
