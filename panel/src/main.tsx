import '@fontsource-variable/archivo';
import './styles.css';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { QueryClientProvider } from '@tanstack/react-query';
import { RouterProvider } from '@tanstack/react-router';
import { queryClient } from './api/client';
import { router } from './router';

/**
 * In mock mode the MSW worker is started before the first render, so the very
 * first `GET /auth/me` already sees the stateful mock. The production bundle
 * replaces the mode with "production" and drops this branch (and the worker)
 * entirely.
 */
const bootstrap = async () => {
  if (import.meta.env.MODE === 'mock') {
    const { startMock } = await import('./mocks/browser');
    await startMock();
  }

  const container = document.getElementById('root');
  if (!container) throw new Error('Missing #root');

  createRoot(container).render(
    <StrictMode>
      <QueryClientProvider client={queryClient}>
        <RouterProvider router={router} />
      </QueryClientProvider>
    </StrictMode>,
  );
};

void bootstrap();
