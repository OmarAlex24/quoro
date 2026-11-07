// SPDX-License-Identifier: AGPL-3.0-only
import { useEffect, useState } from 'react';

/**
 * The time, once a second while `running`, so a live clock and the tape's
 * running segment move without another read. Still otherwise.
 */
export function useNow(running: boolean, every = 1_000): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    setNow(Date.now());
    if (!running) return;
    const timer = window.setInterval(() => setNow(Date.now()), every);
    return () => window.clearInterval(timer);
  }, [running, every]);
  return now;
}
