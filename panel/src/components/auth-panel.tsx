import type { ReactNode } from 'react';
import { DitherField } from './dither-field';
import { Wordmark } from './wordmark';

/**
 * Sign-in and setup: the only screens with a dithered field behind them, where
 * the brand gets to speak. One card in the middle: the wordmark, a title and
 * the line under it, then the form; `footer` closes the card with the way to
 * the other screen.
 */
export function AuthPanel({
  title,
  lede,
  children,
  footer,
}: {
  title: string;
  lede?: ReactNode;
  children: ReactNode;
  footer?: ReactNode;
}) {
  return (
    <main className="auth">
      <DitherField className="auth-field" />
      <div className="auth-card">
        <Wordmark className="auth-mark" />
        <div>
          <h1 className="auth-title">{title}</h1>
          {lede ? <p className="auth-lede">{lede}</p> : null}
        </div>
        {children}
        {footer ? <div className="auth-foot">{footer}</div> : null}
      </div>
    </main>
  );
}
