import { Link } from '@tanstack/react-router';
import type { Agent } from '../api/generated/model';
import { relativeTime } from '../lib/app-status';
import { agentLamp } from '../lib/agents';
import { UserName } from './identity-name';
import { Avatar } from './avatar';
import { ToneLamp } from './tone-lamp';

/** One agent as a rack bay: lamp well on the left, equipment plate on the right. */
export function AgentBay({ agent }: { agent: Agent }) {
  const lamp = agentLamp(agent);

  return (
    <article className="grid grid-cols-[56px_minmax(0,1fr)] overflow-hidden rounded-[10px] border border-line bg-surface card-sheen">
      <div className="flex items-center justify-center border-r border-line bg-sunk">
        <ToneLamp tone={lamp.tone} label={lamp.statusText} />
      </div>

      <div className="flex flex-wrap items-start gap-x-4 gap-y-2 px-4 py-3">
        <div className="min-w-0 flex-1 basis-[min(100%,320px)]">
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
            <Avatar kind="agent" seed={agent.id} />
            <Link
              to="/agents/$agentId"
              params={{ agentId: agent.id }}
              className="type-bay-title truncate text-[18px] leading-tight text-ink underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
            >
              {agent.name}
            </Link>
            <span className="type-data text-[13px] text-ink-2">{lamp.statusText}</span>
          </div>

          <p className="type-data mt-2 truncate text-[13px] text-ink">{agent.model}</p>

          <p className="mt-0.5 flex flex-wrap items-baseline gap-x-1.5 text-[13px] text-ink-2">
            <span>Owner</span>
            <UserName id={agent.owner_user_id} />
          </p>

          {agent.description ? (
            <p className="mt-0.5 truncate text-[13px] text-ink-2" title={agent.description}>
              {agent.description}
            </p>
          ) : null}
        </div>

        <p className="type-data text-[13px] text-ink-2" title={agent.created_at}>
          created {relativeTime(agent.created_at)}
        </p>
      </div>
    </article>
  );
}
