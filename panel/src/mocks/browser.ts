import { setupWorker } from 'msw/browser';
import { setRole, tamperAudit } from './agency';
import { fileMockApproval } from './handlers-approvals';
import { handlers } from './handlers';

export const worker = setupWorker(...handlers);

declare global {
  interface Window {
    /** Mock-mode console helpers; absent from every build. */
    quoroMock?: {
      tamperAudit: typeof tamperAudit;
      setRole: typeof setRole;
      fileMockApproval: typeof fileMockApproval;
    };
  }
}

/** Start the service worker before the first render; never called in a build. */
export const startMock = async (): Promise<void> => {
  window.quoroMock = { tamperAudit, setRole, fileMockApproval };
  await worker.start({
    serviceWorker: { url: '/mockServiceWorker.js' },
    onUnhandledFrame: 'bypass',
  });
};
