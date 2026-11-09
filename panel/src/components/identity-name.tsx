import { Link } from '@tanstack/react-router';
import type { Build, Deployment } from '../api/generated/model';
import { useListAgents } from '../api/generated/agents/agents';
import { useListUsers } from '../api/generated/users/users';
import { canListUsers } from '../lib/capabilities';
import { shortId } from '../lib/agency-format';
import { personLabel } from '../lib/users';
import { useSession } from '../session';
import { Avatar } from './avatar';

/** An agent id shown as its name, linked to the agent; the id while loading or unknown. */
export function AgentName({ id, link = true }: { id: string; link?: boolean }) {
  const agents = useListAgents({ limit: 200 }, { query: { staleTime: 30_000 } });
  const agent = agents.data?.items.find((candidate) => candidate.id === id);
  const label = agent?.name ?? shortId(id);
  if (!link) return <span title={id}>{label}</span>;
  return (
    <Link
      to="/agents"
      search={{ agent: id }}
      title={id}
      className="text-ink underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
    >
      {label}
    </Link>
  );
}

/**
 * A user id shown as a name. Every human role can read the user list
 * (`user:read`), so the panel shows names and "(you)" for the signed-in user.
 */
const useUserLabel = (id: string): { name: string | null; self: boolean } => {
  const { me, user } = useSession();
  const users = useListUsers(
    { limit: 200 },
    { query: { enabled: canListUsers(me) && user?.id !== id, staleTime: 60_000, retry: false } },
  );
  if (user && user.id === id) return { name: user.name, self: true };
  const named = users.data?.items.find((candidate) => candidate.id === id);
  return { name: named?.name ?? null, self: false };
};

export function UserName({ id }: { id: string }) {
  const { name, self } = useUserLabel(id);
  return <span title={id}>{personLabel(id, name, self)}</span>;
}

/** The round initials avatar for a user id; a neutral mark until the name is known. */
export function UserAvatar({ id }: { id: string }) {
  const { name } = useUserLabel(id);
  return <Avatar kind="human" name={name ?? '?'} />;
}

/**
 * Who started a build or a deployment: a person, an agent or the system, with
 * their avatar. The deployment card and the build view say it the same way.
 */
export function StartedBy({
  type,
  id,
}: {
  type: Deployment['created_by_type'] | Build['created_by_type'];
  id: string;
}) {
  if (type === 'user') {
    return (
      <>
        <UserAvatar id={id} />
        <UserName id={id} />
      </>
    );
  }
  if (type === 'agent') {
    return (
      <>
        <Avatar kind="agent" seed={id} />
        <AgentName id={id} link={false} />
      </>
    );
  }
  return (
    <>
      <Avatar kind="system" />
      System
    </>
  );
}
