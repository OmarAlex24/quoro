import * as DialogPrimitive from '@radix-ui/react-dialog';
import { LayoutGrid, Search } from 'lucide-react';
import { useEffect, useId, useState, type KeyboardEvent } from 'react';
import type { AddKind } from '../lib/routes';
import { cn } from '../lib/utils';
import { GlyphTile, type GlyphKind } from './glyph-tile';

/**
 * What can be added to a project, in the order the menu offers it. A
 * template needs a catalog listing the contract does not have yet, so it is
 * not offered.
 */
export const ADD_ITEMS: ReadonlyArray<{ kind: AddKind; title: string; detail: string }> = [
  {
    kind: 'git',
    title: 'Git repository',
    detail: 'Build from a Dockerfile in a public HTTPS repository',
  },
  { kind: 'image', title: 'Image', detail: 'Run a public OCI image as it is' },
  { kind: 'vm', title: 'Virtual machine', detail: 'A microVM you size and manage yourself' },
];

/** A place the menu can jump to: a project, or an app or VM inside one. */
export interface JumpItem {
  key: string;
  kind: GlyphKind | 'project';
  title: string;
  detail: string;
}

interface Entry {
  key: string;
  kind: GlyphKind | 'project';
  title: string;
  detail: string;
  run: () => void;
}

const matches = (query: string) => (entry: Pick<Entry, 'title' | 'detail'>) => {
  const words = query.trim().toLowerCase();
  return words === '' || `${entry.title} ${entry.detail}`.toLowerCase().includes(words);
};

/**
 * "+ Add" and Cmd+K: a typed filter over what can be added, or, given
 * `jumps`, over the projects and services to open. Arrow keys move, Enter
 * chooses, Escape closes.
 */
export function CommandMenu({
  open,
  onOpenChange,
  onChoose,
  jumps,
  onJump,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onChoose?: (kind: AddKind) => void;
  /** Search these instead of what can be added. */
  jumps?: readonly JumpItem[];
  onJump?: (key: string) => void;
}) {
  const [query, setQuery] = useState('');
  const [active, setActive] = useState(0);
  const listId = useId();
  const searching = jumps !== undefined;
  const entries: Entry[] = searching
    ? jumps.map((item) => ({ ...item, run: () => onJump?.(item.key) }))
    : ADD_ITEMS.map((item) => ({ key: item.kind, ...item, run: () => onChoose?.(item.kind) }));
  const items = entries.filter(matches(query));
  const words = searching
    ? { title: 'Search projects and apps', prompt: 'Search projects and apps', list: 'Projects and apps', none: 'No project or app matches that.' }
    : { title: 'Add to this project', prompt: 'What do you want to add?', list: 'Things to add', none: 'Nothing to add matches that.' };

  useEffect(() => {
    if (open) {
      setQuery('');
      setActive(0);
    }
  }, [open]);

  const choose = (entry: Entry) => {
    onOpenChange(false);
    entry.run();
  };

  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (items.length === 0) return;
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault();
      const step = event.key === 'ArrowDown' ? 1 : -1;
      setActive((index) => (index + step + items.length) % items.length);
    } else if (event.key === 'Enter') {
      event.preventDefault();
      const item = items[Math.min(active, items.length - 1)];
      if (item) choose(item);
    }
  };

  const current = Math.min(active, items.length - 1);

  // A long list keeps the chosen row in view as the arrows move through it.
  useEffect(() => {
    if (!open || current < 0) return;
    document.getElementById(`${listId}-${current}`)?.scrollIntoView({ block: 'nearest' });
  }, [open, current, listId]);

  return (
    <DialogPrimitive.Root open={open} onOpenChange={onOpenChange}>
      <DialogPrimitive.Portal>
        <DialogPrimitive.Overlay className="dialog-overlay fixed inset-0 z-40 bg-scrim" />
        <DialogPrimitive.Content className="cmd" aria-describedby={undefined}>
          <DialogPrimitive.Title className="sr-only">{words.title}</DialogPrimitive.Title>
          <div className="cmd-input">
            <Search strokeWidth={1.7} aria-hidden="true" />
            <input
              autoFocus
              value={query}
              onChange={(event) => {
                setQuery(event.target.value);
                setActive(0);
              }}
              onKeyDown={onKeyDown}
              placeholder={words.prompt}
              aria-label={words.prompt}
              role="combobox"
              aria-expanded="true"
              aria-controls={listId}
              aria-activedescendant={items.length > 0 ? `${listId}-${current}` : undefined}
              spellCheck={false}
              autoComplete="off"
            />
          </div>
          <div id={listId} role="listbox" aria-label={words.list} className="cmd-list">
            {items.map((item, index) => (
              <div
                key={item.key}
                id={`${listId}-${index}`}
                role="option"
                aria-selected={index === current}
                className={cn('mi', index === current && 'on')}
                onPointerMove={() => setActive(index)}
                onClick={() => choose(item)}
              >
                {item.kind === 'project' ? (
                  <span className="glyph glyph-neutral" aria-hidden="true">
                    <LayoutGrid strokeWidth={1.7} />
                  </span>
                ) : (
                  <GlyphTile kind={item.kind} />
                )}
                <span className="mi-t">
                  <b>{item.title}</b>
                  <small>{item.detail}</small>
                </span>
              </div>
            ))}
            {items.length === 0 ? <p className="cmd-none">{words.none}</p> : null}
          </div>
        </DialogPrimitive.Content>
      </DialogPrimitive.Portal>
    </DialogPrimitive.Root>
  );
}
