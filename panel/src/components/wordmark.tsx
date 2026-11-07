import { useCallback, useEffect, useState } from 'react';
import { drawWordmark, WORDMARK_FONT, type DitherSurface, type TokenReader } from '../lib/dither-draw';
import { DitherCanvas } from './dither-canvas';

/** Is the wordmark's face ready to draw with? */
const faceLoaded = (): boolean =>
  typeof document !== 'undefined' && document.fonts?.check(`800 40px ${WORDMARK_FONT}`) === true;

/**
 * "quoro" in the two dither tones, the brand on the sign-in and setup cards.
 * It waits for Archivo before it draws, so it never dithers a fallback face.
 */
export function Wordmark({ className }: { className?: string }) {
  const [ready, setReady] = useState(faceLoaded);

  useEffect(() => {
    if (ready || !document.fonts) return;
    let alive = true;
    void document.fonts
      .load(`800 40px ${WORDMARK_FONT}`)
      .catch(() => [])
      .then(() => {
        if (alive) setReady(true);
      });
    return () => {
      alive = false;
    };
  }, [ready]);

  const paint = useCallback(
    (surface: DitherSurface, read: TokenReader) => {
      if (ready) drawWordmark(surface, read);
    },
    [ready],
  );

  return <DitherCanvas paint={paint} label="Quoro" className={className} />;
}
