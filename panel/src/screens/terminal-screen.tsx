import { Link, useParams } from '@tanstack/react-router';
import { useGetApp } from '../api/generated/apps/apps';
import { createTerminalSession } from '../api/generated/interactive/interactive';
import { canOpenTerminal } from '../lib/capabilities';
import { problemView } from '../lib/problem';
import { useSession } from '../session';
import { SessionRoom } from '../components/session-room';
import { Button } from '../components/ui/button';
import { ProblemBanner } from '../components/ui/problem-banner';
import { LoginScreen } from './login-screen';

const backToApp = (appId: string) => (
  <Link
    to="/apps/$appId"
    params={{ appId }}
    className="text-[13px] text-ink-muted underline-offset-4 hover:text-ink hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-cobalt"
  >
    App
  </Link>
);

/**
 * `/apps/$appId/terminal`: a web terminal in the app's guest. The open call is
 * audited by the server; the panel only keeps the one-shot ticket long enough
 * to attach the WebSocket.
 */
export function TerminalScreen() {
  const { me, user, loading, unauthenticated } = useSession();
  const params = useParams({ strict: false });
  const appId = params.appId ?? '';

  const appQuery = useGetApp(appId, { query: { enabled: appId !== '' } });

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-5xl px-4 py-8 sm:px-6">
        <p className="text-[15px] text-ink-muted">Loading…</p>
      </main>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  if (appQuery.isPending) {
    return (
      <main className="mx-auto w-full max-w-5xl px-4 py-8 sm:px-6">
        <div className="h-[180px] rounded-[6px] border border-rail bg-panel/60" />
      </main>
    );
  }

  if (appQuery.error !== null && appQuery.error !== undefined) {
    return (
      <main className="mx-auto w-full max-w-5xl px-4 py-6 sm:px-6 sm:py-8">
        <ProblemBanner view={problemView(appQuery.error, 'see this app')} />
        <Button variant="secondary" className="mt-4" asChild>
          <Link to="/">Back to apps</Link>
        </Button>
      </main>
    );
  }

  const app = appQuery.data;
  if (!app) return null;

  return (
    <SessionRoom
      kind="terminal"
      eyebrow="Web terminal"
      title={app.name}
      description="A shell inside the app's guest, in its root filesystem and environment. The server audits the open and records a timed transcript."
      back={backToApp(app.id)}
      connect={() => createTerminalSession(app.id, { rows: 24, cols: 80 })}
      canConnect={canOpenTerminal(me)}
      deniedMessage="Only an owner or operator can open a web terminal: app:terminal is human-only in this release."
    />
  );
}
