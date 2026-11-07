import { Link, useParams } from '@tanstack/react-router';
import { createConsoleSession } from '../api/generated/interactive/interactive';
import type { Vm } from '../api/generated/model';
import { useGetVm } from '../api/generated/vms/vms';
import { canViewConsole } from '../lib/capabilities';
import { problemView } from '../lib/problem';
import { useSession } from '../session';
import { SessionRoom } from '../components/session-room';
import { Button } from '../components/ui/button';
import { ProblemBanner } from '../components/ui/problem-banner';
import { LoginScreen } from './login-screen';

const BACK_CLASS =
  'text-[13px] text-ink-2 underline-offset-4 hover:text-ink hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent';

/** Back to the VM's sheet on its project's canvas, where the console was opened from. */
const backToVm = (vm: Pick<Vm, 'id' | 'name' | 'project_id'>) => (
  <Link to="/projects/$projectId" params={{ projectId: vm.project_id }} search={{ vm: vm.id }} className={BACK_CLASS}>
    Back to {vm.name}
  </Link>
);

/**
 * `/vms/$vmId/console`: the VM's serial console, read-only. It replays the
 * node's recent boot output and then follows it live; input never opens.
 */
export function ConsoleScreen() {
  const { me, user, loading, unauthenticated } = useSession();
  const params = useParams({ strict: false });
  const vmId = params.vmId ?? '';

  const vmQuery = useGetVm(vmId, { query: { enabled: vmId !== '' } });

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-5xl px-4 py-8 sm:px-6">
        <p className="text-[15px] text-ink-2">Loading…</p>
      </main>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  if (vmQuery.isPending) {
    return (
      <main className="mx-auto w-full max-w-5xl px-4 py-8 sm:px-6">
        <div className="h-[180px] rounded-[10px] border border-line bg-surface/60" />
      </main>
    );
  }

  if (vmQuery.error !== null && vmQuery.error !== undefined) {
    return (
      <main className="mx-auto w-full max-w-5xl px-4 py-6 sm:px-6 sm:py-8">
        <ProblemBanner view={problemView(vmQuery.error, 'see this VM')} />
        <Button variant="default" className="mt-4" asChild>
          <Link to="/nodes" search={{ tab: 'vms' }}>
            Back to virtual machines
          </Link>
        </Button>
      </main>
    );
  }

  const vm = vmQuery.data;
  if (!vm) return null;

  return (
    <SessionRoom
      kind="console"
      eyebrow="Serial console"
      title={vm.name}
      description="The VM's serial console, read-only: recent boot output and then live kernel and init lines. Nothing in the guest reads the serial port, so input is refused."
      back={backToVm(vm)}
      // The id from the route, the one the operator chose; never a field
      // of the fetched record.
      connect={() => createConsoleSession(vmId)}
      canConnect={canViewConsole(me)}
      deniedMessage="This session can't view a serial console. A viewer needs a grant that covers vm:console."
      vmId={vmId}
      appId={vm.app_id ?? undefined}
    />
  );
}
