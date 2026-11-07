import * as DialogPrimitive from '@radix-ui/react-dialog';
import { Search } from 'lucide-react';
import { useEffect, useId, useState, type KeyboardEvent } from 'react';
import type { AddKind } from '../lib/routes';
import { cn } from '../lib/utils';
import { GlyphTile } from './glyph-tile';

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

const matches = (query: string) => (item: (typeof ADD_ITEMS)[number]) => {
  const words = query.trim().toLowerCase();
  return words === '' || `${item.title} ${item.detail}`.toLowerCase().includes(words);
};

/**
 * "+ Add" and Cmd+K: a typed filter over what can be added. Arrow keys move,
 * Enter chooses, Escape closes.
 */
export function CommandMenu({
  open,
  onOpenChange,
  onChoose,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onChoose: (kind: AddKind) => void;
}) {
  const [query, setQuery] = useState('');
  const [active, setActive] = useState(0);
  const listId = useId();
  const items = ADD_ITEMS.filter(matches(query));

  useEffect(() => {
    if (open) {
      setQuery('');
      setActive(0);
    }
  }, [open]);

  const choose = (kind: AddKind) => {
    onOpenChange(false);
    onChoose(kind);
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
      if (item) choose(item.kind);
    }
  };

  const current = Math.min(active, items.length - 1);

  return (
    <DialogPrimitive.Root open={open} onOpenChange={onOpenChange}>
      <DialogPrimitive.Portal>
        <DialogPrimitive.Overlay className="dialog-overlay fixed inset-0 z-40 bg-scrim" />
        <DialogPrimitive.Content className="cmd" aria-describedby={undefined}>
          <DialogPrimitive.Title className="sr-only">Add to this project</DialogPrimitive.Title>
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
              placeholder="What do you want to add?"
              aria-label="What do you want to add?"
              role="combobox"
              aria-expanded="true"
              aria-controls={listId}
              aria-activedescendant={items.length > 0 ? `${listId}-${current}` : undefined}
              spellCheck={false}
              autoComplete="off"
            />
          </div>
          <div id={listId} role="listbox" aria-label="Things to add" className="cmd-list">
            {items.map((item, index) => (
              <div
                key={item.kind}
                id={`${listId}-${index}`}
                role="option"
                aria-selected={index === current}
                className={cn('mi', index === current && 'on')}
                onPointerMove={() => setActive(index)}
                onClick={() => choose(item.kind)}
              >
                <GlyphTile kind={item.kind} />
                <span className="mi-t">
                  <b>{item.title}</b>
                  <small>{item.detail}</small>
                </span>
              </div>
            ))}
            {items.length === 0 ? <p className="cmd-none">Nothing to add matches that.</p> : null}
          </div>
        </DialogPrimitive.Content>
      </DialogPrimitive.Portal>
    </DialogPrimitive.Root>
  );
}
