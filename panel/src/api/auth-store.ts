/**
 * A tiny external store for one fact the whole app cares about: did the
 * session end while the operator was using the panel? A 401 from any request
 * flips it, and the login screen then explains what happened.
 */
let ended = false;
let hadSession = false;
const listeners = new Set<() => void>();

const emit = () => {
  for (const listener of listeners) listener();
};

export const authEvents = {
  subscribe(listener: () => void) {
    listeners.add(listener);
    return () => {
      listeners.delete(listener);
    };
  },
  isEnded(): boolean {
    return ended;
  },
  hasSession(): boolean {
    return hadSession;
  },
  /** A session now exists: clear any earlier "it ended" notice. */
  startSession() {
    hadSession = true;
    if (ended) {
      ended = false;
      emit();
    }
  },
  /** Refuse to claim the session ended before one ever existed. */
  markUnauthenticated() {
    if (!hadSession || ended) return;
    ended = true;
    emit();
  },
  /** After a deliberate log-out: back to a clean, never-signed-in state. */
  endSession() {
    hadSession = false;
    ended = false;
    emit();
  },
};
