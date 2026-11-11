import { Link } from '@tanstack/react-router';
import {
  Activity,
  Bell,
  ChartColumn,
  Check,
  ChevronDown,
  FileText,
  LayoutGrid,
  Network,
  SlidersVertical,
  type LucideIcon,
} from 'lucide-react';
import type { ReactNode } from 'react';
import type { Project } from '../../api/generated/model';
import { EXACT_LINK, type ProjectView } from '../../lib/routes';
import { cn } from '../../lib/utils';
import { QuoroMark } from '../avatar';
import { AvatarMenu } from './avatar-menu';
import { MenuPanel, useMenu } from './menu';
import { usePendingApprovalCount } from './shell-data';
import { PendingCount } from './sidebar';

const RAIL: ReadonlyArray<{ view: ProjectView; label: string; Icon: LucideIcon }> = [
  { view: 'canvas', label: 'Canvas', Icon: Network },
  { view: 'metrics', label: 'Metrics', Icon: ChartColumn },
  { view: 'logs', label: 'Logs', Icon: FileText },
  { view: 'settings', label: 'Project settings', Icon: SlidersVertical },
];

/**
 * The project rail's links. Only the view the URL names is current: no
 * `view` is the canvas.
 */
export function RailLinks({ projectId, view }: { projectId: string; view: ProjectView }) {
  return (
    <>
      {RAIL.map(({ view: item, label, Icon }) => (
        <Link
          key={item}
          to="/projects/$projectId"
          params={{ projectId }}
          search={item === 'canvas' ? {} : { view: item }}
          activeOptions={EXACT_LINK}
          className={cn(view === item && 'on')}
          aria-label={label}
          title={label}
          aria-current={view === item ? 'page' : undefined}
        >
          <Icon strokeWidth={1.7} aria-hidden="true" />
        </Link>
      ))}
    </>
  );
}

/**
 * Inside a project: a top bar with the breadcrumb and the project switcher,
 * and the rail with the project's views. The sidebar steps aside; the mark
 * leads back to all projects.
 */
export function ProjectShell({
  projectId,
  project,
  projects,
  leaf,
  view,
  canvas,
  summary,
  children,
}: {
  projectId: string;
  project?: Project;
  projects: readonly Project[];
  /** The open service, after the project in the breadcrumb. */
  leaf?: string;
  view: ProjectView;
  /** Lay the page on the dotted canvas field. */
  canvas: boolean;
  /** How the project's services stand, in the top bar (the canvas only). */
  summary?: ReactNode;
  children: ReactNode;
}) {
  const pending = usePendingApprovalCount();

  return (
    <div className="project-shell">
      <header className="top">
        <Link to="/projects" className="flex-none rounded-[6px]" aria-label="All projects">
          <QuoroMark className="logo" />
        </Link>
        <nav className="crumb" aria-label="Breadcrumb">
          <span className="slash" aria-hidden="true">
            /
          </span>
          <ProjectSwitcher projectId={projectId} project={project} projects={projects} />
          {leaf ? (
            <>
              <span className="slash" aria-hidden="true">
                /
              </span>
              <span className="crumb-leaf" aria-current="page">
                {leaf}
              </span>
            </>
          ) : null}
        </nav>
        <span className="flex-1" />
        {summary}
        <Link to="/activity" className="iconbtn" aria-label="Activity" title="Activity">
          <Activity strokeWidth={1.7} aria-hidden="true" />
        </Link>
        <Link
          to="/approvals"
          className="iconbtn"
          aria-label={pending > 0 ? `Approvals, ${pending} waiting` : 'Approvals'}
          title="Approvals"
        >
          <Bell strokeWidth={1.7} aria-hidden="true" />
          <PendingCount count={pending} />
        </Link>
        <span className="vbar" aria-hidden="true" />
        <AvatarMenu placement="topbar" />
      </header>

      <div className="rail-col">
        <nav className="rail" aria-label="Project views">
          <RailLinks projectId={projectId} view={view} />
        </nav>
      </div>

      <div className={cn('min-w-0', canvas && 'canvas-field')}>{children}</div>
    </div>
  );
}

function ProjectSwitcher({
  projectId,
  project,
  projects,
}: {
  projectId: string;
  project?: Project;
  projects: readonly Project[];
}) {
  const menu = useMenu();
  return (
    <div ref={menu.rootRef} className="relative min-w-0">
      <button
        ref={menu.triggerRef}
        type="button"
        className="sw-btn max-w-full"
        aria-label={`Project ${project?.name ?? ''}, switch project`}
        {...menu.triggerProps}
      >
        <span>{project?.name ?? '…'}</span>
        <ChevronDown strokeWidth={1.7} aria-hidden="true" />
      </button>
      <MenuPanel menu={menu} label="Projects">
        <p className="menu-label">Projects</p>
        {projects.map((item) => (
          <Link
            key={item.id}
            to="/projects/$projectId"
            params={{ projectId: item.id }}
            role="menuitem"
            className="menu-item"
            aria-current={item.id === projectId ? 'page' : undefined}
          >
            <span className="min-w-0 flex-1 truncate">{item.name}</span>
            {item.id === projectId ? <Check strokeWidth={1.7} aria-hidden="true" /> : null}
          </Link>
        ))}
        <div className="menu-sep" role="separator" />
        <Link to="/projects" role="menuitem" className="menu-item">
          <LayoutGrid strokeWidth={1.7} aria-hidden="true" />
          All projects
        </Link>
      </MenuPanel>
    </div>
  );
}
