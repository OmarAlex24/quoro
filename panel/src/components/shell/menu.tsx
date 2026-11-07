import {
  useCallback,
  useEffect,
  useId,
  useRef,
  useState,
  type KeyboardEvent,
  type ReactNode,
  type RefObject,
} from 'react';
import { cn } from '../../lib/utils';

const ITEMS = '[role="menuitem"],[role="menuitemradio"]';

export interface MenuState {
  open: boolean;
  setOpen: (open: boolean) => void;
  menuId: string;
  rootRef: RefObject<HTMLDivElement | null>;
  triggerRef: RefObject<HTMLButtonElement | null>;
  /** Spread on the button that opens the menu. */
  triggerProps: {
    'aria-haspopup': 'menu';
    'aria-expanded': boolean;
    'aria-controls': string | undefined;
    onClick: () => void;
    onKeyDown: (event: KeyboardEvent<HTMLButtonElement>) => void;
  };
}

/**
 * A button that opens a small menu: it closes on Escape, on a click outside,
 * on Tab, and after an item is chosen; arrow keys, Home and End move between
 * items. Focus goes to the first item on open and back to the button on close.
 */
export function useMenu(): MenuState {
  const [open, setOpenState] = useState(false);
  const focusFirst = useRef(false);
  const menuId = useId();
  const rootRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);

  const setOpen = useCallback((next: boolean) => setOpenState(next), []);

  useEffect(() => {
    if (!open) return;
    const onPointerDown = (event: PointerEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) setOpenState(false);
    };
    document.addEventListener('pointerdown', onPointerDown);
    return () => document.removeEventListener('pointerdown', onPointerDown);
  }, [open]);

  useEffect(() => {
    if (!open || !focusFirst.current) return;
    focusFirst.current = false;
    const menu = document.getElementById(menuId);
    const chosen = menu?.querySelector<HTMLElement>('[aria-checked="true"]');
    (chosen ?? menu?.querySelector<HTMLElement>(ITEMS))?.focus();
  }, [open, menuId]);

  return {
    open,
    setOpen,
    menuId,
    rootRef,
    triggerRef,
    triggerProps: {
      'aria-haspopup': 'menu',
      'aria-expanded': open,
      'aria-controls': open ? menuId : undefined,
      onClick: () => {
        focusFirst.current = true;
        setOpenState((value) => !value);
      },
      onKeyDown: (event) => {
        if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
          event.preventDefault();
          focusFirst.current = true;
          setOpenState(true);
        }
      },
    },
  };
}

/** The menu panel. Items are links or buttons with a `menuitem` role. */
export function MenuPanel({
  menu,
  label,
  align = 'start',
  className,
  children,
}: {
  menu: MenuState;
  label: string;
  align?: 'start' | 'end';
  className?: string;
  children: ReactNode;
}) {
  if (!menu.open) return null;

  const close = (refocus: boolean) => {
    menu.setOpen(false);
    if (refocus) menu.triggerRef.current?.focus();
  };

  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    const items = Array.from(event.currentTarget.querySelectorAll<HTMLElement>(ITEMS));
    const at = items.indexOf(document.activeElement as HTMLElement);
    const move = (index: number) => {
      event.preventDefault();
      items[(index + items.length) % items.length]?.focus();
    };
    switch (event.key) {
      case 'Escape':
        event.preventDefault();
        close(true);
        return;
      case 'Tab':
        close(false);
        return;
      case 'ArrowDown':
        move(at + 1);
        return;
      case 'ArrowUp':
        move(at - 1);
        return;
      case 'Home':
        move(0);
        return;
      case 'End':
        move(items.length - 1);
        return;
    }
  };

  return (
    <div
      id={menu.menuId}
      role="menu"
      aria-label={label}
      className={cn('menu', align === 'end' ? 'right-0' : 'left-0', className)}
      onKeyDown={onKeyDown}
      onClick={(event) => {
        const item = (event.target as HTMLElement).closest<HTMLElement>(ITEMS);
        if (item && item.dataset.keepOpen === undefined) close(false);
      }}
    >
      {children}
    </div>
  );
}
