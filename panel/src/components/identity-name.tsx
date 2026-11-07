import { Link } from '@tanstack/react-router';
import { useListAgents } from '../api/generated/agents/agents';
import { useListUsers } from '../api/generated/users/users';
import { canListUsers } from '../lib/capabilities';
import { shortId } from '../lib/agency-format';
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
      to="/agents/$agentId"
      params={{ agentId: id }}
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
  if (self) return <span title={id}>{name} (you)</span>;
  return <span title={id}>{name ?? shortId(id)}</span>;
}

/** The round initials avatar for a user id; a neutral mark until the name is known. */
export function UserAvatar({ id }: { id: string }) {
  const { name } = useUserLabel(id);
  return <Avatar kind="human" name={name ?? '?'} />;
}
