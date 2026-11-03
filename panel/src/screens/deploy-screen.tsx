import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Link, useNavigate } from '@tanstack/react-router';
import { useEffect, useRef, useState, type FormEvent } from 'react';
import {
  createApp,
  getListAppsQueryKey,
  replaceAppEnv,
} from '../api/generated/apps/apps';
import { createDeployment } from '../api/generated/deployments/deployments';
import { createAppDomain } from '../api/generated/domains/domains';
import type { CreateDomainRequestTlsMode } from '../api/generated/model';
import {
  createProject,
  getListProjectsQueryKey,
  useListProjects,
} from '../api/generated/projects/projects';
import { useListNodes } from '../api/generated/nodes/nodes';
import { problemView, type ProblemView } from '../lib/problem';
import { pickDefaultNode } from '../lib/app-status';
import {
  deploySteps,
  deriveAppName,
  type DeployStepKey,
  type DeployStepState,
} from '../lib/deploy-flow';
import { deriveSlug, validateSlug } from '../lib/slug';
import { useSession } from '../session';
import { ToneLamp } from '../components/tone-lamp';
import { Button } from '../components/ui/button';
import { Field } from '../components/ui/field';
import { Input } from '../components/ui/input';
import { ProblemBanner } from '../components/ui/problem-banner';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../components/ui/select';
import { LoginScreen } from './login-screen';

const APP_NAME_PATTERN = /^[a-z0-9-]{1,40}$/;
const ENV_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
const HOSTNAME_PATTERN =
  /^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$/;

const DEFAULT_IMAGE = 'docker.io/library/nginx:alpine';

interface EnvRow {
  key: number;
  name: string;
  value: string;
  show: boolean;
}

const stateTone = (state: DeployStepState) => {
  if (state === 'done') return 'run' as const;
  if (state === 'active') return 'shift' as const;
  if (state === 'error') return 'fault' as const;
  return 'idle' as const;
};

const stateWords: Record<DeployStepState, string> = {
  waiting: 'Waiting',
  active: 'Working',
  done: 'Done',
  error: 'Failed',
};

export function DeployScreen() {
  const { user, loading, unauthenticated, canWrite } = useSession();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const projectsQuery = useListProjects();
  const nodesQuery = useListNodes(undefined, { query: { staleTime: 10_000 } });

  const [image, setImage] = useState(DEFAULT_IMAGE);
  const [name, setName] = useState(() => deriveAppName(DEFAULT_IMAGE));
  const [nameEdited, setNameEdited] = useState(false);
  const [port, setPort] = useState('80');
  const [projectId, setProjectId] = useState('');
  const [nodeId, setNodeId] = useState('');
  const [hostname, setHostname] = useState('');
  const [tlsMode, setTlsMode] = useState<CreateDomainRequestTlsMode>('tunnel');

  const [projectView, setProjectView] = useState<'pick' | 'create'>('pick');
  const [projectName, setProjectName] = useState('');
  const [projectSlug, setProjectSlug] = useState('');
  const [slugEdited, setSlugEdited] = useState(false);

  const nextRowKey = useRef(1);
  const [envRows, setEnvRows] = useState<EnvRow[]>([
    { key: 0, name: '', value: '', show: false },
  ]);

  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [activeSteps, setActiveSteps] = useState<
    ReturnType<typeof deploySteps> | null
  >(null);
  const [stepStates, setStepStates] = useState<
    Partial<Record<DeployStepKey, DeployStepState>>
  >({});
  const [failedStep, setFailedStep] = useState<DeployStepKey | null>(null);
  const [createdAppId, setCreatedAppId] = useState<string | null>(null);

  const projects = projectsQuery.data?.items ?? [];
  const nodes = nodesQuery.data?.items ?? [];

  useEffect(() => {
    if (nodeId === '' && nodes.length > 0) setNodeId(pickDefaultNode(nodes));
  }, [nodeId, nodes]);

  const set = (key: 'name' | 'port' | 'hostname', value: string) => {
    if (key === 'name') setNameEdited(true);
    if (key === 'name') setName(value);
    if (key === 'port') setPort(value);
    if (key === 'hostname') setHostname(value);
    setFieldErrors((current) => {
      const next = { ...current };
      delete next[key];
      return next;
    });
  };

  const updateRow = (key: number, patch: Partial<EnvRow>) =>
    setEnvRows((current) =>
      current.map((row) => (row.key === key ? { ...row, ...patch } : row)),
    );

  const addRow = () => {
    const key = nextRowKey.current;
    nextRowKey.current += 1;
    setEnvRows((current) => [...current, { key, name: '', value: '', show: false }]);
  };

  const removeRow = (key: number) =>
    setEnvRows((current) => current.filter((row) => row.key !== key));

  const filledRows = envRows.filter(
    (row) => row.name.trim() !== '' || row.value !== '',
  );

  const createProjectMutation = useMutation({
    mutationFn: () => createProject({ name: projectName.trim(), slug: projectSlug }),
    onSuccess: (project) => {
      void queryClient.invalidateQueries({ queryKey: getListProjectsQueryKey() });
      setProjectId(project.id);
      setProjectName('');
      setProjectSlug('');
      setSlugEdited(false);
      setProjectView('pick');
    },
    onError: (error) => setProblem(problemView(error, 'create projects')),
  });

  const validate = (): Record<string, string> => {
    const errors: Record<string, string> = {};
    if (!APP_NAME_PATTERN.test(name.trim())) {
      errors.name = 'Use 1–40 lowercase letters, digits or dashes.';
    }
    if (image.trim() === '') errors.image = 'Enter the image reference.';
    const parsedPort = Number(port);
    if (
      port.trim() === '' ||
      !Number.isInteger(parsedPort) ||
      parsedPort < 1 ||
      parsedPort > 65_535
    ) {
      errors.port = 'Use a whole port between 1 and 65535.';
    }
    if (projectId === '') errors.project = 'Choose a project, or create one here.';
    if (nodeId === '') errors.node = 'No node is connected. Connect a node first.';
    if (hostname.trim() !== '' && !HOSTNAME_PATTERN.test(hostname.trim())) {
      errors.hostname = 'Use a hostname like blog.example.com.';
    }

    const seen = new Set<string>();
    for (const row of filledRows) {
      const rowName = row.name.trim();
      if (!ENV_NAME_PATTERN.test(rowName)) {
        errors.env = 'Variable names use letters, digits and underscores, and cannot start with a digit.';
        break;
      }
      if (seen.has(rowName)) {
        errors.env = `The variable ${rowName} is listed twice.`;
        break;
      }
      if (row.value === '') {
        errors.env = `Enter a value for ${rowName}.`;
        break;
      }
      seen.add(rowName);
    }
    return errors;
  };

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setProblem(null);
    setFailedStep(null);

    const errors = validate();
    setFieldErrors(errors);
    if (Object.keys(errors).length > 0) return;

    const steps = deploySteps({
      envCount: filledRows.length,
      hostname: hostname.trim(),
    });
    setActiveSteps(steps);
    setStepStates(Object.fromEntries(steps.map((step) => [step.key, 'waiting' as const])));
    setSubmitting(true);
    setCreatedAppId(null);

    const mark = (key: DeployStepKey, state: DeployStepState) =>
      setStepStates((current) => ({ ...current, [key]: state }));

    let current: DeployStepKey = 'create';
    try {
      mark('create', 'active');
      const app = await createApp(
        {
          project_id: projectId,
          name: name.trim(),
          image: image.trim(),
          port: Number(port),
          placement: { node_ids: [nodeId] },
        },
        { headers: { 'Idempotency-Key': crypto.randomUUID() } },
      );
      setCreatedAppId(app.id);
      mark('create', 'done');

      if (filledRows.length > 0) {
        current = 'env';
        mark('env', 'active');
        await replaceAppEnv(app.id, {
          items: filledRows.map((row) => ({
            name: row.name.trim(),
            value: row.value,
          })),
        });
        mark('env', 'done');
      }

      if (hostname.trim() !== '') {
        current = 'domain';
        mark('domain', 'active');
        await createAppDomain(
          app.id,
          { hostname: hostname.trim(), tls_mode: tlsMode },
          { headers: { 'Idempotency-Key': crypto.randomUUID() } },
        );
        mark('domain', 'done');
      }

      current = 'deploy';
      mark('deploy', 'active');
      await createDeployment(
        app.id,
        { image: image.trim() },
        { headers: { 'Idempotency-Key': crypto.randomUUID() } },
      );
      mark('deploy', 'done');

      void queryClient.invalidateQueries({ queryKey: getListAppsQueryKey() });
      void navigate({ to: '/apps/$appId', params: { appId: app.id } });
    } catch (error) {
      mark(current, 'error');
      setFailedStep(current);
      setProblem(problemView(error, 'deploy apps'));
      void queryClient.invalidateQueries({ queryKey: getListAppsQueryKey() });
    } finally {
      setSubmitting(false);
    }
  };

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-3xl px-4 py-8 sm:px-6">
        <p className="text-[15px] text-ink-muted">Loading…</p>
      </main>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  if (!canWrite) {
    return (
      <main className="mx-auto w-full max-w-3xl px-4 py-6 sm:px-6 sm:py-8">
        <h1 className="type-display text-[24px] leading-none">Deploy an app</h1>
        <div className="mt-5 rounded-[6px] border border-rail bg-panel px-4 py-8">
          <p className="text-[15px] text-ink">
            Your role can read apps but not deploy them.
          </p>
          <Button variant="secondary" className="mt-4" asChild>
            <Link to="/">Back to apps</Link>
          </Button>
        </div>
      </main>
    );
  }

  const projectSlugError =
    projectView === 'create' && projectSlug !== '' ? validateSlug(projectSlug) : undefined;
  const canOpenApp = createdAppId !== null && failedStep !== null;
  const showChecklist = activeSteps !== null;

  const liveNameError =
    name.trim() !== '' && !APP_NAME_PATTERN.test(name.trim())
      ? 'Use 1–40 lowercase letters, digits or dashes.'
      : undefined;
  const livePortError =
    port.trim() !== '' &&
    (!Number.isInteger(Number(port)) || Number(port) < 1 || Number(port) > 65_535)
      ? 'Use a whole port between 1 and 65535.'
      : undefined;
  const liveHostnameError =
    hostname.trim() !== '' && !HOSTNAME_PATTERN.test(hostname.trim())
      ? 'Use a hostname like blog.example.com.'
      : undefined;
  const nameError = fieldErrors.name ?? liveNameError;
  const portError = fieldErrors.port ?? livePortError;
  const hostnameError = fieldErrors.hostname ?? liveHostnameError;

  return (
    <main className="mx-auto w-full max-w-3xl px-4 py-6 sm:px-6 sm:py-8">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="type-display text-[24px] leading-none">Deploy an app</h1>
        <Button variant="quiet" asChild>
          <Link to="/">Back to apps</Link>
        </Button>
      </div>
      <p className="mt-2 text-[15px] text-ink-muted">
        Name an image, pick where it runs and deploy it. The app gets one microVM.
      </p>

      <form
        onSubmit={submit}
        noValidate
        className="mt-5 rounded-[6px] border border-rail bg-panel bay-shadow"
      >
        <section className="px-4 py-5 sm:px-5">
          <h2 className="type-display-light text-[15px] leading-none text-ink">Image</h2>
          <div className="mt-4 grid grid-cols-1 gap-4 sm:grid-cols-[minmax(0,1fr)_140px]">
            <Field label="OCI reference" htmlFor="app-image">
              <Input
                id="app-image"
                value={image}
                onChange={(event) => {
                  setImage(event.target.value);
                  if (!nameEdited) setName(deriveAppName(event.target.value));
                  setFieldErrors((current) => {
                    const next = { ...current };
                    delete next.image;
                    return next;
                  });
                }}
                placeholder="docker.io/library/nginx:alpine"
                spellCheck={false}
                autoFocus
                aria-invalid={fieldErrors.image !== undefined}
              />
              {fieldErrors.image ? (
                <p className="text-[13px] text-lamp-fault">{fieldErrors.image}</p>
              ) : null}
            </Field>
            <Field label="Container port" htmlFor="app-port">
              <Input
                id="app-port"
                type="number"
                min={1}
                max={65_535}
                step={1}
                inputMode="numeric"
                value={port}
                onChange={(event) => set('port', event.target.value)}
                aria-invalid={portError !== undefined}
              />
              {portError ? (
                <p className="text-[13px] text-lamp-fault">{portError}</p>
              ) : null}
            </Field>
          </div>
        </section>

        <section className="border-t border-rail px-4 py-5 sm:px-5">
          <h2 className="type-display-light text-[15px] leading-none text-ink">
            Where it runs
          </h2>
          <div className="mt-4 flex flex-col gap-4">
            <Field
              label="App name"
              htmlFor="app-name"
              hint="Lowercase letters, digits and dashes; unique in the project."
            >
              <Input
                id="app-name"
                value={name}
                onChange={(event) => set('name', event.target.value)}
                placeholder="blog"
                spellCheck={false}
                aria-invalid={nameError !== undefined}
              />
              {nameError ? (
                <p className="text-[13px] text-lamp-fault">{nameError}</p>
              ) : null}
            </Field>

            <div className="flex flex-col gap-1.5">
              <div className="flex items-baseline justify-between gap-3">
                <span className="type-body-medium text-[13px] text-ink-muted">Project</span>
                {projectView === 'pick' ? (
                  <button
                    type="button"
                    className="text-[13px] text-cobalt underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-cobalt"
                    onClick={() => {
                      setProblem(null);
                      setProjectView('create');
                    }}
                  >
                    New project
                  </button>
                ) : null}
              </div>

              {projectView === 'pick' ? (
                <>
                  <Select
                    value={projectId}
                    onValueChange={(value) => {
                      setProjectId(value);
                      setFieldErrors((current) => {
                        const next = { ...current };
                        delete next.project;
                        return next;
                      });
                    }}
                  >
                    <SelectTrigger id="app-project" aria-label="Project">
                      <SelectValue placeholder="Choose a project" />
                    </SelectTrigger>
                    <SelectContent>
                      {projects.map((project) => (
                        <SelectItem key={project.id} value={project.id}>
                          {project.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  {fieldErrors.project ? (
                    <p className="text-[13px] text-lamp-fault">{fieldErrors.project}</p>
                  ) : null}
                </>
              ) : (
                <div className="rounded-[6px] border border-rail bg-recess/40 p-3">
                  <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                    <Field label="Project name" htmlFor="deploy-project-name">
                      <Input
                        id="deploy-project-name"
                        value={projectName}
                        placeholder="Blog"
                        onChange={(event) => {
                          const value = event.target.value;
                          setProjectName(value);
                          if (!slugEdited) setProjectSlug(deriveSlug(value));
                        }}
                      />
                    </Field>
                    <Field
                      label="Slug"
                      htmlFor="deploy-project-slug"
                      hint="Lowercase letters, digits and dashes."
                    >
                      <Input
                        id="deploy-project-slug"
                        value={projectSlug}
                        onChange={(event) => {
                          setSlugEdited(true);
                          setProjectSlug(event.target.value);
                        }}
                        spellCheck={false}
                        aria-invalid={projectSlugError !== undefined}
                      />
                      {projectSlugError ? (
                        <p className="text-[13px] text-lamp-fault">{projectSlugError}</p>
                      ) : null}
                    </Field>
                  </div>
                  <div className="mt-3 flex flex-wrap gap-2">
                    <Button
                      variant="secondary"
                      disabled={
                        createProjectMutation.isPending ||
                        projectName.trim() === '' ||
                        validateSlug(projectSlug) !== undefined
                      }
                      onClick={() => {
                        setProblem(null);
                        createProjectMutation.mutate();
                      }}
                    >
                      {createProjectMutation.isPending ? 'Creating…' : 'Create project'}
                    </Button>
                    <Button
                      variant="quiet"
                      onClick={() => {
                        setProjectView('pick');
                        setProjectName('');
                        setProjectSlug('');
                        setSlugEdited(false);
                      }}
                    >
                      Cancel
                    </Button>
                  </div>
                </div>
              )}
            </div>

            <Field label="Target node" htmlFor="app-node">
              <Select
                value={nodeId}
                onValueChange={(value) => {
                  setNodeId(value);
                  setFieldErrors((current) => {
                    const next = { ...current };
                    delete next.node;
                    return next;
                  });
                }}
              >
                <SelectTrigger id="app-node" aria-label="Target node">
                  <SelectValue placeholder="No node available" />
                </SelectTrigger>
                <SelectContent>
                  {nodes.map((node) => (
                    <SelectItem key={node.id} value={node.id}>
                      {node.name} · {node.status}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {fieldErrors.node ? (
                <p className="text-[13px] text-lamp-fault">{fieldErrors.node}</p>
              ) : null}
            </Field>
          </div>
        </section>

        <section className="border-t border-rail px-4 py-5 sm:px-5">
          <h2 className="type-display-light text-[15px] leading-none text-ink">Domain</h2>
          <div className="mt-4 flex flex-col gap-4">
            <Field
              label="Hostname"
              htmlFor="app-hostname"
              hint="Optional. Leave it empty to deploy without a domain."
            >
              <Input
                id="app-hostname"
                value={hostname}
                onChange={(event) => set('hostname', event.target.value)}
                placeholder="blog.example.com"
                spellCheck={false}
                aria-invalid={hostnameError !== undefined}
              />
              {hostnameError ? (
                <p className="text-[13px] text-lamp-fault">{hostnameError}</p>
              ) : null}
            </Field>

            <fieldset className="flex flex-col gap-2">
              <legend className="type-body-medium mb-1 text-[13px] text-ink-muted">
                TLS mode
              </legend>
              <label className="flex items-center gap-2 text-[15px] text-ink">
                <input
                  type="radio"
                  name="tls-mode"
                  value="tunnel"
                  checked={tlsMode === 'tunnel'}
                  onChange={() => setTlsMode('tunnel')}
                  className="h-4 w-4 accent-cobalt"
                />
                Cloudflare Tunnel
              </label>
              <label className="flex items-center gap-2 text-[15px] text-ink">
                <input
                  type="radio"
                  name="tls-mode"
                  value="acme"
                  checked={tlsMode === 'acme'}
                  onChange={() => setTlsMode('acme')}
                  className="h-4 w-4 accent-cobalt"
                />
                ACME certificate
              </label>
            </fieldset>
          </div>
        </section>

        <section className="border-t border-rail px-4 py-5 sm:px-5">
          <div className="flex flex-wrap items-baseline justify-between gap-2">
            <h2 className="type-display-light text-[15px] leading-none text-ink">
              Environment
            </h2>
            <button
              type="button"
              className="text-[13px] text-cobalt underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-cobalt"
              onClick={addRow}
            >
              Add variable
            </button>
          </div>
          <p className="mt-2 text-[13px] leading-snug text-ink-muted">
            Values are write-only: Quoro stores them encrypted and never shows them again.
          </p>

          <div className="mt-3 flex flex-col gap-3">
            {envRows.map((row) => (
              <div
                key={row.key}
                className="grid grid-cols-1 gap-2 sm:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto]"
              >
                <Input
                  aria-label="Variable name"
                  value={row.name}
                  onChange={(event) => updateRow(row.key, { name: event.target.value })}
                  placeholder="DATABASE_URL"
                  spellCheck={false}
                />
                <div className="flex gap-2">
                  <Input
                    aria-label="Variable value"
                    type={row.show ? 'text' : 'password'}
                    value={row.value}
                    onChange={(event) => updateRow(row.key, { value: event.target.value })}
                    placeholder="Value"
                    spellCheck={false}
                  />
                  <Button
                    variant="quiet"
                    className="px-2.5"
                    aria-pressed={row.show}
                    onClick={() => updateRow(row.key, { show: !row.show })}
                  >
                    {row.show ? 'Hide' : 'Show'}
                  </Button>
                </div>
                <Button
                  variant="quiet"
                  className="justify-self-start px-2.5"
                  onClick={() => removeRow(row.key)}
                  disabled={envRows.length === 1}
                >
                  Remove
                </Button>
              </div>
            ))}
          </div>
          {fieldErrors.env ? (
            <p className="mt-2 text-[13px] text-lamp-fault">{fieldErrors.env}</p>
          ) : null}
        </section>

        {showChecklist ? (
          <section className="border-t border-rail px-4 py-5 sm:px-5">
            <h2 className="type-display-light text-[15px] leading-none text-ink">
              Progress
            </h2>
            <ol className="mt-3 flex flex-col gap-2">
              {activeSteps.map((step) => {
                const state = stepStates[step.key] ?? 'waiting';
                return (
                  <li key={step.key} className="flex flex-col gap-2">
                    <div className="flex items-center gap-2.5">
                      <ToneLamp
                        tone={stateTone(state)}
                        pulse={state === 'active'}
                        size={18}
                        label={stateWords[state]}
                      />
                      <span className="text-[15px] text-ink">{step.label}</span>
                      <span className="type-data text-[13px] text-ink-muted">
                        {stateWords[state]}
                      </span>
                    </div>
                    {failedStep === step.key && problem ? (
                      <ProblemBanner view={problem} className="ml-7" />
                    ) : null}
                  </li>
                );
              })}
            </ol>
          </section>
        ) : null}

        <div className="flex flex-wrap items-center justify-end gap-2 border-t border-rail px-4 py-4 sm:px-5">
          {problem && failedStep === null ? <ProblemBanner view={problem} className="mr-auto" /> : null}
          {canOpenApp ? (
            <Button variant="primary" asChild>
              <Link to="/apps/$appId" params={{ appId: createdAppId ?? '' }}>
                Open the app
              </Link>
            </Button>
          ) : (
            <Button variant="primary" type="submit" disabled={submitting}>
              {submitting ? 'Deploying…' : 'Deploy'}
            </Button>
          )}
        </div>
      </form>
    </main>
  );
}
