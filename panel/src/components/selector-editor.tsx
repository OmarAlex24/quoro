import type { Selector } from '../api/generated/model';
import { useListApps } from '../api/generated/apps/apps';
import { useListNodes } from '../api/generated/nodes/nodes';
import { useListProjects } from '../api/generated/projects/projects';
import { cn } from '../lib/utils';

type Kind = 'projects' | 'apps' | 'nodes';

const toggleIn = (list: readonly string[] | undefined, value: string): string[] => {
  const current = list ?? [];
  return current.includes(value) ? current.filter((item) => item !== value) : [...current, value];
};

/** Drop empty lists so the request carries only what was chosen. */
const tidy = (selector: Selector): Selector => {
  const next: Selector = {};
  if (selector.projects?.length) next.projects = selector.projects;
  if (selector.apps?.length) next.apps = selector.apps;
  if (selector.nodes?.length) next.nodes = selector.nodes;
  return next;
};

/**
 * The resources a grant or webhook applies to: every resource, or chosen
 * projects (by slug), apps and nodes (by id). `{}` matches nothing, so the
 * caller should refuse to submit an empty choice.
 */
export function SelectorEditor({
  id,
  value,
  onChange,
  disabled = false,
}: {
  id: string;
  value: Selector;
  onChange: (selector: Selector) => void;
  disabled?: boolean;
}) {
  const projects = useListProjects(undefined, { query: { staleTime: 30_000 } });
  const apps = useListApps(undefined, { query: { staleTime: 30_000 } });
  const nodes = useListNodes(undefined, { query: { staleTime: 30_000 } });
  const everything = value.all === true;

  const groups: Array<{ kind: Kind; label: string; options: Array<{ value: string; label: string }> }> = [
    {
      kind: 'projects',
      label: 'Projects',
      options: (projects.data?.items ?? []).map((project) => ({ value: project.slug, label: project.name })),
    },
    {
      kind: 'apps',
      label: 'Apps',
      options: (apps.data?.items ?? []).map((app) => ({ value: app.id, label: app.name })),
    },
    {
      kind: 'nodes',
      label: 'Nodes',
      options: (nodes.data?.items ?? []).map((node) => ({ value: node.id, label: node.name })),
    },
  ];

  return (
    <fieldset id={id} disabled={disabled} className="flex flex-col gap-2.5">
      <div className="flex flex-wrap gap-x-5 gap-y-1.5 text-[14px]">
        <label className="inline-flex cursor-pointer items-center gap-1.5">
          <input
            type="radio"
            name={`${id}-mode`}
            className="accent-cobalt"
            checked={!everything}
            onChange={() => onChange({})}
          />
          Chosen resources
        </label>
        <label className="inline-flex cursor-pointer items-center gap-1.5">
          <input
            type="radio"
            name={`${id}-mode`}
            className="accent-cobalt"
            checked={everything}
            onChange={() => onChange({ all: true })}
          />
          Every resource
        </label>
      </div>

      <div className={cn('flex flex-col gap-2', everything && 'opacity-45')} aria-disabled={everything}>
        {groups.map((group) => (
          <div key={group.kind} className="grid grid-cols-[72px_minmax(0,1fr)] items-baseline gap-x-3">
            <span className="type-body-medium w-[72px] shrink-0 text-[13px] text-ink-muted">{group.label}</span>
            <span className="flex flex-wrap gap-x-3 gap-y-1">
            {group.options.length === 0 ? (
              <span className="text-[13px] text-ink-muted">None yet</span>
            ) : (
              group.options.map((option) => (
                <label key={option.value} className="inline-flex cursor-pointer items-center gap-1.5 text-[13px]">
                  <input
                    type="checkbox"
                    className="h-3.5 w-3.5 accent-cobalt"
                    disabled={everything}
                    checked={!everything && (value[group.kind] ?? []).includes(option.value)}
                    onChange={() =>
                      onChange(tidy({ ...value, all: undefined, [group.kind]: toggleIn(value[group.kind], option.value) }))
                    }
                  />
                  <span className="type-data">{option.label}</span>
                </label>
              ))
            )}
            </span>
          </div>
        ))}
      </div>
    </fieldset>
  );
}

/** `{}` matches nothing: a form must not send it. */
export const selectorIsEmpty = (selector: Selector): boolean =>
  !selector.all && !selector.projects?.length && !selector.apps?.length && !selector.nodes?.length;
