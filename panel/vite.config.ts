import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';

/**
 * Two modes:
 * - `vite` (real): proxies `/api` to the local engine on 8443 with a self-signed cert.
 * - `vite --mode mock`: serves `mock-public/` (the MSW worker) and the app boots
 *   the stateful in-browser mock. The production build never includes either.
 */
export default defineConfig(({ mode }) => {
  const mock = mode === 'mock';

  return {
    plugins: [react(), tailwindcss()],
    publicDir: mock ? 'mock-public' : 'public',
    build: {
      outDir: 'dist',
      emptyOutDir: true,
    },
    server: {
      port: 5173,
      strictPort: true,
      proxy: mock
        ? undefined
        : {
            '/api': {
              target: 'https://127.0.0.1:8443',
              changeOrigin: true,
              secure: false,
            },
          },
    },
    test: {
      environment: 'node',
      include: ['src/**/*.test.ts'],
    },
  };
});
