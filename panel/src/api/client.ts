import { QueryCache, QueryClient } from '@tanstack/react-query';
import { isApiProblem } from './fetcher';
import { authEvents } from './auth-store';

/** Do not hammer the engine: retry network and 5xx failures, never a refusal. */
const shouldRetry = (failureCount: number, error: unknown): boolean => {
  if (isApiProblem(error) && error.status < 500 && error.status !== 429) return false;
  return failureCount < 2;
};

export const queryClient = new QueryClient({
  queryCache: new QueryCache({
    onError: (error) => {
      if (isApiProblem(error) && error.status === 401) authEvents.markUnauthenticated();
    },
  }),
  defaultOptions: {
    queries: {
      retry: shouldRetry,
      refetchOnWindowFocus: false,
      staleTime: 5_000,
    },
  },
});
