import { Link, useNavigate } from '@tanstack/react-router';
import { useState } from 'react';
import { useListAgents } from '../api/generated/agents/agents';
import { canManageAgents } from '../lib/capabilities';
import { problemView } from '../lib/problem';
import { useSession } from '../session';
import { AgentBay } from '../components/agent-bay';
import { CreateAgentDialog } from '../components/agent-dialog';
import { Button } from '../components/ui/button';
import { ProblemBanner } from '../components/ui/problem-banner';
import { LoginScreen } from './login-screen';

export function AgentsScreen() {
  const { me, user, loading, unauthenticated } = useSession();
  const navigate = useNavigate();
  const [dialogOpen, setDialogOpen] = useState(false);

  const agentsQuery = useListAgents(undefined);

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-6xl px-4 py-8 sm:px-6">
        <p className="text-[15px] text-ink-2">Loading…</p>
      </main>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  const canManage = canManageAgents(me);
  const agents = agentsQuery.data?.items ?? [];
  const listError =
    agentsQuery.error !== null && agentsQuery.error !== undefined
      ? problemView(agentsQuery.error, 'see agents')
      : null;

  return (
    <main className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 sm:py-8">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="type-display text-[24px] leading-none">Agents</h1>
          <p className="mt-2 max-w-2xl text-[13px] leading-snug text-ink-2">
            AI identities that act on the cluster through grants. Each has a human owner.
          </p>
        </div>
        {canManage ? (
          <Button variant="primary" onClick={() => setDialogOpen(true)}>
            Create an agent
          </Button>
        ) : null}
      </div>

      {!canManage ? (
        <p className="mt-3 text-[13px] text-ink-2">
          Your role can read agents but not change them.
        </p>
      ) : null}

      {listError ? <ProblemBanner view={listError} className="mt-4" /> : null}

      <div className="mt-5 flex flex-col gap-2">
        {agentsQuery.isPending ? (
          <div className="h-[104px] rounded-[10px] border border-line bg-surface/60" />
        ) : agents.length === 0 ? (
          <div className="rounded-[10px] border border-dashed border-line bg-surface/50 px-4 py-10 text-center">
            <p className="text-[15px] text-ink">
              No agents yet. Create one, then give it a grant so it can act.
            </p>
            {canManage ? (
              <Button
                variant="primary"
                className="mt-4"
                onClick={() => setDialogOpen(true)}
              >
                Create an agent
              </Button>
            ) : (
              <Button variant="default" className="mt-4" asChild>
                <Link to="/agents" search={{ tab: 'grants' }}>
                  See the grants
                </Link>
              </Button>
            )}
          </div>
        ) : (
          agents.map((agent) => <AgentBay key={agent.id} agent={agent} />)
        )}
      </div>

      {canManage ? (
        <CreateAgentDialog
          open={dialogOpen}
          onOpenChange={setDialogOpen}
          ownerUserId={user.id}
          onCreated={(agent) => {
            void navigate({ to: '/agents', search: { agent: agent.id } });
          }}
        />
      ) : null}
    </main>
  );
}
