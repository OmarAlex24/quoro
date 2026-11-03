import type { Scope } from '../api/generated/model';
import { grantableScopesByResource } from '../lib/scopes';

/**
 * Checkboxes for the scopes a standard or incident grant may carry, grouped
 * by resource. Human-only and destructive scopes are never offered
 * (permission-model.md, sections 4.1 and 4.2).
 */
export function ScopePicker({
  id,
  value,
  onChange,
  disabled = false,
}: {
  id: string;
  value: readonly Scope[];
  onChange: (scopes: Scope[]) => void;
  disabled?: boolean;
}) {
  const selected = new Set(value);
  const toggle = (scope: Scope) => {
    const next = new Set(selected);
    if (next.has(scope)) next.delete(scope);
    else next.add(scope);
    // Keep contract order so the request and the summary read the same way.
    onChange(grantableScopesByResource().flatMap((group) => group.scopes).filter((s) => next.has(s)));
  };

  return (
    <fieldset id={id} disabled={disabled} className="grid gap-x-4 gap-y-2.5 sm:grid-cols-2">
      {grantableScopesByResource().map((group) => (
        <div key={group.resource} className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
          <span className="type-body-medium w-[72px] shrink-0 text-[13px] capitalize text-ink-muted">
            {group.resource}
          </span>
          {group.scopes.map((scope) => (
            <label key={scope} className="inline-flex cursor-pointer items-center gap-1.5 text-[13px]">
              <input
                type="checkbox"
                className="h-3.5 w-3.5 accent-cobalt"
                checked={selected.has(scope)}
                onChange={() => toggle(scope)}
              />
              <span className="type-data">{scope.split(':')[1]}</span>
            </label>
          ))}
        </div>
      ))}
    </fieldset>
  );
}
