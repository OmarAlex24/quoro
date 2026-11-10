// SPDX-License-Identifier: AGPL-3.0-only
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Link, useNavigate } from '@tanstack/react-router';
import { SquareTerminal } from 'lucide-react';
import { useState, type ReactNode } from 'react';
import { deleteApp, getListAppsQueryKey } from '../api/generated/apps/apps';
import type { AppDetail, Build, Node } from '../api/generated/model';
import { shortDeploymentId } from '../lib/app-status';
import { builtImageLabel, lastBuiltCommit, shortCommit } from '../lib/build-status';
import { problemView, type ProblemView } from '../lib/problem';
import { resourcesLine } from '../lib/services';
import { AppDomains } from './app-domains';
import { AppEdgeBackends } from './app-edge';
import { AppInstances } from './app-instances';
import { ImageRef } from './image-ref';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';
import { ProblemBanner } from './ui/problem-banner';

/** A titled group of the Settings tab; the VM sheet uses it too. */
export function Group({ title, lede, children }: { title: string; lede?: string; children: ReactNode }) {
  return (
    <section className="set-group" aria-label={title}>
      <h3 className="set-h">{title}</h3>
      {lede ? <p className="set-lede">{lede}</p> : null}
      {children}
    </section>
  );
}

/** Label and value rows in a box. */
export function Rows({ rows }: { rows: ReadonlyArray<readonly [string, ReactNode] | null> }) {
  return (
    <dl className="box set-rows">
      {rows.filter((row) => row !== null).map(([label, value]) => (
        <div key={label} className="set-row">
          <dt>{label}</dt>
          <dd>{value}</dd>
        </div>
      ))}
    </dl>
  );
}

const code = (value: string) => <span className="type-code text-[11.5px]">{value}</span>;

/**
 * The Settings tab: what the app is set to, grouped. General first, then how
 * it is reached (domains and the edge), how it scales (replicas, placement
 * and the instances running now), the terminal, and the danger zone. Every
 * action keeps the confirmation it had on the app page.
 */
export function SettingsTab({
  app,
  builds,
  canWrite,
  terminal,
  nodeNames,
  nodeStatuses,
  onChanged,
}: {
  app: AppDetail;
  builds: readonly Build[];
  canWrite: boolean;
  /** The session may open a shell in the app's VM. */
  terminal: boolean;
  nodeNames: Record<string, string>;
  nodeStatuses: Record<string, Node['status']>;
  onChanged: () => void;
}) {
  const source = app.source ?? null;
  const image = builtImageLabel(app.image);
  const lastCommit = lastBuiltCommit([...builds]);
  const placement =
    app.placement.node_ids.length === 0
      ? 'Any eligible node, chosen by the scheduler'
      : app.placement.node_ids.map((id) => nodeNames[id] ?? id).join(', ');
  const health = app.health_check;

  return (
    <>
      <Group title="General">
        <Rows
          rows={[
            ['Name', app.name],
            ['Source', source ? 'Built from a Git repository' : 'Runs a public image'],
            [
              'Image',
              source !== null && image === null ? 'Not built yet' : <ImageRef image={app.image} />,
            ],
            source ? ['Repository', code(source.repo_url)] : null,
            source ? ['Ref', code(source.ref)] : null,
            source ? ['Dockerfile', code(source.dockerfile_path ?? 'Dockerfile')] : null,
            source ? ['Context', code(source.context_dir ?? '.')] : null,
            source ? ['Target', source.target ?? 'last stage'] : null,
            source
              ? ['Last built commit', lastCommit === null ? 'Not built yet' : code(shortCommit(lastCommit))]
              : null,
            ['Port', String(app.port)],
            [
              'Health check',
              <>
                {code(health.path)} every {health.interval_s} s, {health.failures}{' '}
                {health.failures === 1 ? 'failure' : 'failures'} to fail
              </>,
            ],
            [
              'Deployment',
              app.current_deployment_id ? (
                <span title={app.current_deployment_id}>{code(shortDeploymentId(app.current_deployment_id))}</span>
              ) : (
                'None yet'
              ),
            ],
            ['Created', new Date(app.created_at).toLocaleString()],
          ]}
        />
      </Group>

      <Group title="Networking">
        <div className="box px-4 py-4">
          <AppDomains
            appId={app.id}
            domains={app.domains}
            canWrite={canWrite}
            onChanged={onChanged}
          />
        </div>
        <div className="set-host [&>:first-child]:mt-0">
          <AppEdgeBackends backends={app.edge_backends ?? []} nodeNames={nodeNames} />
        </div>
      </Group>

      <Group title="Scaling">
        <Rows
          rows={[
            ['Replicas', `${app.replicas} ${app.replicas === 1 ? 'instance' : 'instances, one per node'}`],
            [
              'Resources',
              resourcesLine(app.resources),
            ],
            ['Placement', placement],
          ]}
        />
        <div className="set-host [&>:first-child]:mt-0">
          <AppInstances
            instances={app.instances}
            nodeNames={nodeNames}
            nodeStatuses={nodeStatuses}
            hasSource={source !== null}
          />
        </div>
      </Group>

      {terminal ? (
        <Group title="Terminal" lede="A shell inside the app's VM. Every session is recorded in the audit log.">
          <div>
            <Button asChild>
              <Link to="/apps/$appId/terminal" params={{ appId: app.id }}>
                <SquareTerminal className="size-4" strokeWidth={1.7} aria-hidden="true" />
                Open the terminal
              </Link>
            </Button>
          </div>
        </Group>
      ) : null}

      {canWrite ? <DangerZone app={app} /> : null}
    </>
  );
}

function DangerZone({ app }: { app: AppDetail }) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [confirm, setConfirm] = useState(false);
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const remove = useMutation({
    mutationFn: () => deleteApp(app.id),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: getListAppsQueryKey() });
      void navigate({ to: '/projects/$projectId', params: { projectId: app.project_id }, search: {} });
    },
    onError: (error) => {
      setConfirm(false);
      setProblem(problemView(error, 'delete apps'));
    },
  });

  return (
    <Group title="Danger zone">
      <div className="box box-danger flex flex-wrap items-center gap-x-4 gap-y-3 px-4 py-4">
        <div className="min-w-0 flex-1">
          <p className="text-[13.5px] font-medium text-ink">Delete this app</p>
          <p className="mt-0.5 text-[12.5px] leading-snug text-ink-2">
            Its VM, domains and environment are removed. This cannot be undone.
          </p>
        </div>
        <Button variant="danger" disabled={remove.isPending} onClick={() => setConfirm(true)}>
          Delete
        </Button>
      </div>
      {problem ? <ProblemBanner view={problem} /> : null}

      <Dialog open={confirm} onOpenChange={setConfirm}>
        <DialogContent
          title="Delete app"
          description={`Delete ${app.name}? Its VM, domains and environment are removed.`}
        >
          <div className="flex flex-wrap justify-end gap-2">
            <Button variant="default" onClick={() => setConfirm(false)}>
              Keep the app
            </Button>
            <Button busy={remove.isPending} variant="danger-solid" disabled={remove.isPending} onClick={() => remove.mutate()}>
              {remove.isPending ? 'Deleting…' : 'Delete'}
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </Group>
  );
}
