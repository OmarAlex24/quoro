import { Link } from '@tanstack/react-router';
import { useListGrants } from '../api/generated/grants/grants';
import type { Project } from '../api/generated/model';
import { projectAccess, timeLeft } from '../lib/access';
import { Avatar } from './avatar';
import { AgentName } from './identity-name';

/**
 * Who else can act here: the agent whose active grant names this project
 * (or every resource), what its scopes let it do, and how long it has left.
 * When several grants qualify, the one that expires first speaks and the
 * rest are a count that opens the grants. Nothing shows when none does.
 */
export function AccessChip({ project }: { project: Pick<Project, 'slug'> }) {
  const grants = useListGrants(
    { status: 'active', limit: 500 },
    { query: { staleTime: 30_000, refetchInterval: 60_000, retry: false } },
  );
  const access = projectAccess(grants.data?.items ?? [], project);
  const first = access[0];
  if (!first) return null;
  const more = access.length - 1;

  return (
    <div className="access">
      <Avatar kind="agent" seed={first.grant.agent_id} className="access-avatar" />
      <p className="min-w-0 truncate">
        <b>
          <AgentName id={first.grant.agent_id} link={false} />
        </b>{' '}
        can {first.phrase} here · {timeLeft(first.grant.expires_at)}
      </p>
      {more > 0 ? (
        <Link
          to="/agents"
          search={{ tab: 'grants', status: 'active' }}
          className="access-more"
          aria-label={`${more} more ${more === 1 ? 'grant' : 'grants'} here`}
        >
          +{more}
        </Link>
      ) : null}
    </div>
  );
}
