import { setupWorker } from 'msw/browser';
import { handlers } from './handlers';

export const worker = setupWorker(...handlers);

/** Start the service worker before the first render; never called in a build. */
export const startMock = async (): Promise<void> => {
  await worker.start({
    serviceWorker: { url: '/mockServiceWorker.js' },
    onUnhandledFrame: 'bypass',
  });
};
