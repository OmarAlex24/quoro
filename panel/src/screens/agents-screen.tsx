import { Link, useNavigate } from '@tanstack/react-router';
import { useState } from 'react';
import { canManageAgents } from '../lib/capabilities';
import { problemView } from '../lib/problem';
import { useSession } from '../session';
import { AgentCard, useAgentDirectory } from '../components/agent-card';
import { CreateAgentDialog } from '../components/agent-dialog';
import { Button } from '../components/ui/button';
import { ProblemBanner } from '../components/ui/problem-banner';
import { LoginScreen } from './login-screen';

/**
 * `/agents`: every agent as a card, with where it stands, the grant it works
 * under and its calls over the last day. A card opens the agent.
 */
export function AgentsScreen() {
  const { me, user, loading, unauthenticated } = useSession();
  const navigate = useNavigate();
  const [dialogOpen, setDialogOpen] = useState(false);
  const directory = useAgentDirectory();

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-6xl px-4 py-8 sm:px-6">
        <p className="text-[15px] text-ink-2">Loading…</p>
      </main>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  const canManage = canManageAgents(me);
  const query = directory.agents;
  const agents = query.data?.items ?? [];
  const listError = query.error ? problemView(query.error, 'see agents') : null;

  return (
    <main className="mx-auto w-full max-w-6xl px-4 pb-10 sm:px-6">
      <div className="flex flex-wrap items-center gap-3">
        <p className="min-w-0 flex-1 text-[13px] leading-snug text-ink-2">
          {query.isPending
            ? 'Reading the agents…'
            : `${agents.length === 1 ? '1 agent' : `${agents.length} agents`}. AI identities that act on the cluster through grants; each has a human owner.`}
        </p>
        {canManage ? (
          <Button variant="primary" onClick={() => setDialogOpen(true)}>
            New agent
          </Button>
        ) : null}
      </div>

      {!canManage ? (
        <p className="mt-2 text-[13px] text-ink-2">Your role can read agents but not change them.</p>
      ) : null}

      {listError ? <ProblemBanner view={listError} className="mt-4" /> : null}

      {query.isPending ? (
        <div className="agents-grid mt-5">
          <div className="h-[220px] rounded-[12px] border border-line bg-surface/60" />
          <div className="h-[220px] rounded-[12px] border border-line bg-surface/60" />
        </div>
      ) : agents.length === 0 ? (
        <div className="mt-5 rounded-[10px] border border-dashed border-line bg-surface/50 px-4 py-10 text-center">
          <p className="text-[15px] text-ink">No agents yet. Create one, then give it a grant so it can act.</p>
          {canManage ? (
            <Button variant="primary" className="mt-4" onClick={() => setDialogOpen(true)}>
              New agent
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
        <div className="agents-grid mt-5">
          {agents.map((agent) => (
            <AgentCard key={agent.id} agent={agent} facts={directory.facts(agent.id)} selected={false} />
          ))}
        </div>
      )}

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
