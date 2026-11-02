import {
  createContext,
  useContext,
  useEffect,
  useSyncExternalStore,
  type ReactNode,
} from 'react';
import { useGetMe } from './api/generated/auth/auth';
import type { MeResponse, User } from './api/generated/model';
import { authEvents } from './api/auth-store';

export interface SessionValue {
  me?: MeResponse;
  user?: User;
  loading: boolean;
  /** A session existed and the API then answered 401. */
  ended: boolean;
  /** No usable session: first visit, logged out, or ended. */
  unauthenticated: boolean;
  /** Viewers hold read scopes only, so write controls are hidden for them. */
  canWrite: boolean;
}

const SessionContext = createContext<SessionValue | null>(null);

export function SessionProvider({ children }: { children: ReactNode }) {
  const ended = useSyncExternalStore(
    authEvents.subscribe,
    authEvents.isEnded,
    authEvents.isEnded,
  );
  const meQuery = useGetMe({ query: { retry: false } });
  const me = meQuery.data;
  // Once the API has refused mid-session, drop the cached identity so the
  // header and write controls disappear with the session.
  const user = ended ? undefined : me?.type === 'user' ? me.user : undefined;

  useEffect(() => {
    if (user) authEvents.startSession();
  }, [user]);

  const loading = meQuery.isPending;
  const unauthenticated = !loading && (me === undefined || ended);

  const value: SessionValue = {
    me,
    user,
    loading,
    ended,
    unauthenticated,
    canWrite: user !== undefined && user.role !== 'viewer',
  };

  return <SessionContext.Provider value={value}>{children}</SessionContext.Provider>;
}

export const useSession = (): SessionValue => {
  const value = useContext(SessionContext);
  if (!value) throw new Error('useSession must be used inside SessionProvider');
  return value;
};
