import { useMutation, useQueryClient } from '@tanstack/react-query';
import { getRouteApi, useNavigate } from '@tanstack/react-router';
import { useState } from 'react';
import { useListAgents } from '../api/generated/agents/agents';
import {
  getListGrantsQueryKey,
  revokeGrant,
  useListGrants,
} from '../api/generated/grants/grants';
import type {
  Grant,
  GrantKind,
  GrantStatus,
  ListGrantsParams,
} from '../api/generated/model';
import { shortId } from '../lib/agency-format';
import { canManageGrants } from '../lib/capabilities';
import { problemView, type ProblemView } from '../lib/problem';
import { useSession } from '../session';
import { GrantDialog } from '../components/grant-dialog';
import { GrantRow } from '../components/grant-row';
import { RevokeGrantDialog } from '../components/revoke-grant-dialog';
import { Button } from '../components/ui/button';
import { NoticeLine, ProblemBanner } from '../components/ui/problem-banner';
import { LoginScreen } from './login-screen';

const grantsRoute = getRouteApi('/grants');

const STATUS_OPTIONS: ReadonlyArray<{ value: GrantStatus; label: string }> = [
  { value: 'active', label: 'Active' },
  { value: 'expired', label: 'Expired' },
  { value: 'revoked', label: 'Revoked' },
  { value: 'exhausted', label: 'Exhausted' },
];

const KIND_OPTIONS: ReadonlyArray<{ value: GrantKind; label: string }> = [
  { value: 'standard', label: 'Standard' },
  { value: 'incident', label: 'Incident' },
  { value: 'approval', label: 'Approval' },
];

export function GrantsScreen() {
  const { me, user, loading, unauthenticated } = useSession();
  const search = grantsRoute.useSearch();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [dialogOpen, setDialogOpen] = useState(false);
  const [revokeTarget, setRevokeTarget] = useState<Grant | null>(null);
  const [revokeProblem, setRevokeProblem] = useState<ProblemView | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const agentsQuery = useListAgents({ limit: 200 }, { query: { staleTime: 30_000 } });

  const params: ListGrantsParams = {};
  if (search.agent_id) params.agent_id = search.agent_id;
  if (search.status) params.status = search.status;
  if (search.kind) params.kind = search.kind;

  const grantsQuery = useListGrants(params, {
    query: { refetchInterval: 10_000 },
  });

  const revoke = useMutation({
    mutationFn: (grant: Grant) => revokeGrant(grant.id),
    onSuccess: (grant) => {
      setRevokeProblem(null);
      setRevokeTarget(null);
      setNotice(`Revoked grant ${shortId(grant.id)}`);
      void queryClient.invalidateQueries({ queryKey: getListGrantsQueryKey() });
    },
    onError: (error) => {
      // A conflict means the grant is no longer active: refresh the list too.
      setRevokeProblem(problemView(error, 'revoke grants'));
      void queryClient.invalidateQueries({ queryKey: getListGrantsQueryKey() });
    },
  });

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-6xl px-4 py-8 sm:px-6">
        <p className="text-[15px] text-ink-2">Loading…</p>
      </main>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  const canManage = canManageGrants(me);
  const agents = agentsQuery.data?.items ?? [];
  const grants = grantsQuery.data?.items ?? [];
  const hasFilters = Boolean(search.agent_id || search.status || search.kind);
  const listError =
    grantsQuery.error !== null && grantsQuery.error !== undefined
      ? problemView(grantsQuery.error, 'see grants')
      : null;
  const filteredAgentMissing =
    search.agent_id !== undefined && !agents.some((agent) => agent.id === search.agent_id);

  const setFilter = (patch: {
    agent_id?: string;
    status?: GrantStatus;
    kind?: GrantKind;
  }) => {
    void navigate({ to: '/grants', search: (prev) => ({ ...prev, ...patch }) });
  };

  const clearFilters = () => {
    void navigate({ to: '/grants', search: {} });
  };

  const agentName = (id: string) =>
    agents.find((agent) => agent.id === id)?.name ?? shortId(id);

  return (
    <main className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 sm:py-8">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="type-display text-[24px] leading-none">Grants</h1>
          <p className="mt-1.5 text-[15px] text-ink-2">
            What each agent may do, on which resources, until when. Every grant expires.
          </p>
        </div>
        {canManage ? (
          <Button variant="primary" onClick={() => setDialogOpen(true)}>
            Issue a grant
          </Button>
        ) : null}
      </div>

      {canManage ? null : (
        <p className="mt-2 text-[13px] text-ink-2">
          Your role can read grants but not change them.
        </p>
      )}

      <div className="mt-5 flex flex-wrap items-end gap-3">
        <label className="flex flex-col gap-1">
          <span className="type-body-medium text-[13px] text-ink-2">Agent</span>
          <select
            className="field min-w-[170px]"
            value={search.agent_id ?? ''}
            onChange={(event) =>
              setFilter({ agent_id: event.target.value === '' ? undefined : event.target.value })
            }
          >
            <option value="">Any agent</option>
            {filteredAgentMissing && search.agent_id ? (
              <option value={search.agent_id}>{shortId(search.agent_id)}</option>
            ) : null}
            {agents.map((agent) => (
              <option key={agent.id} value={agent.id}>
                {agent.disabled ? `${agent.name} (disabled)` : agent.name}
              </option>
            ))}
          </select>
        </label>

        <label className="flex flex-col gap-1">
          <span className="type-body-medium text-[13px] text-ink-2">Status</span>
          <select
            className="field min-w-[140px]"
            value={search.status ?? ''}
            onChange={(event) =>
              setFilter({
                status:
                  event.target.value === '' ? undefined : (event.target.value as GrantStatus),
              })
            }
          >
            <option value="">Any status</option>
            {STATUS_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>

        <label className="flex flex-col gap-1">
          <span className="type-body-medium text-[13px] text-ink-2">Kind</span>
          <select
            className="field min-w-[140px]"
            value={search.kind ?? ''}
            onChange={(event) =>
              setFilter({
                kind: event.target.value === '' ? undefined : (event.target.value as GrantKind),
              })
            }
          >
            <option value="">Any kind</option>
            {KIND_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>

        {hasFilters ? (
          <Button variant="ghost" onClick={clearFilters}>
            Clear filters
          </Button>
        ) : null}
      </div>

      {notice ? (
        <div className="mt-3">
          <NoticeLine>{notice}</NoticeLine>
        </div>
      ) : null}

      {listError ? <ProblemBanner view={listError} className="mt-4" /> : null}

      <div className="mt-5 flex flex-col gap-2">
        {grantsQuery.isPending ? (
          <div className="h-[128px] rounded-[10px] border border-line bg-surface/60" />
        ) : grants.length === 0 ? (
          <div className="rounded-[10px] border border-dashed border-line bg-surface/50 px-4 py-10 text-center">
            <p className="text-[15px] text-ink">
              {hasFilters
                ? 'No grants match these filters.'
                : 'No grants yet. Issue one to let an agent act.'}
            </p>
            {hasFilters ? (
              <Button variant="default" className="mt-4" onClick={clearFilters}>
                Clear filters
              </Button>
            ) : canManage ? (
              <Button variant="primary" className="mt-4" onClick={() => setDialogOpen(true)}>
                Issue a grant
              </Button>
            ) : null}
          </div>
        ) : (
          grants.map((grant) => (
            <GrantRow
              key={grant.id}
              grant={grant}
              canManage={canManage}
              onRevoke={(target) => {
                setRevokeProblem(null);
                setRevokeTarget(target);
              }}
            />
          ))
        )}
      </div>

      <GrantDialog
        open={dialogOpen}
        onOpenChange={setDialogOpen}
        preselectedAgentId={search.agent_id}
        onIssued={(grant) => {
          setNotice(`Issued grant ${shortId(grant.id)} to ${agentName(grant.agent_id)}`);
        }}
      />

      <RevokeGrantDialog
        grant={revokeTarget}
        open={revokeTarget !== null}
        onOpenChange={(open) => {
          if (!open) setRevokeTarget(null);
        }}
        pending={revoke.isPending}
        problem={revokeProblem}
        onConfirm={() => {
          if (revokeTarget) revoke.mutate(revokeTarget);
        }}
      />
    </main>
  );
}
