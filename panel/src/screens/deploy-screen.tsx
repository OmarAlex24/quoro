import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Link, useNavigate } from '@tanstack/react-router';
import { useEffect, useRef, useState, type FormEvent } from 'react';
import {
  createApp,
  getListAppsQueryKey,
  replaceAppEnv,
} from '../api/generated/apps/apps';
import { createDeployment } from '../api/generated/deployments/deployments';
import { startBuild } from '../api/generated/builds/builds';
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
import { cn } from '../lib/utils';
import {
  DEFAULT_CONTEXT_DIR,
  DEFAULT_DOCKERFILE_PATH,
  DEFAULT_SOURCE_REF,
  deriveAppNameFromRepo,
  repoUrlError,
  validateSourceForm,
  type SourceErrors,
} from '../lib/source-form';
import { useSession } from '../session';
import { ToneLamp } from '../components/tone-lamp';
import { GlyphTile } from '../components/glyph-tile';
import { Button } from '../components/ui/button';
import { Field } from '../components/ui/field';
import { Input } from '../components/ui/input';
import { ProblemBanner } from '../components/ui/problem-banner';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../components/ui/select';
import { LoginScreen } from './login-screen';
import { PageSkeleton } from '../components/skeleton';

const APP_NAME_PATTERN = /^[a-z0-9-]{1,40}$/;
const ENV_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
const HOSTNAME_PATTERN =
  /^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$/;

const DEFAULT_IMAGE = 'docker.io/library/nginx:alpine';

type SourceKind = 'image' | 'git';

interface EnvRow {
  key: number;
  name: string;
  value: string;
  show: boolean;
}

interface BuildArgRow {
  key: number;
  name: string;
  value: string;
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

/**
 * Deploy an app: from an image or a public Git repository, into a project,
 * on a node, with a domain and variables. Full page at `/projects?add=app`;
 * inside a project's create sheet (`sheet`), the source is the one the sheet
 * was opened for and the project starts chosen. The steps and requests are
 * the same either way.
 */
export function DeployScreen({
  sheet,
}: {
  sheet?: { source: SourceKind; projectId: string };
} = {}) {
  const { user, loading, unauthenticated, canWrite } = useSession();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const projectsQuery = useListProjects();
  const nodesQuery = useListNodes(undefined, { query: { staleTime: 10_000 } });

  const [image, setImage] = useState(DEFAULT_IMAGE);
  const [name, setName] = useState(() =>
    sheet?.source === 'git' ? deriveAppNameFromRepo('') : deriveAppName(DEFAULT_IMAGE),
  );
  const [nameEdited, setNameEdited] = useState(false);
  const [port, setPort] = useState('80');
  const [projectId, setProjectId] = useState(sheet?.projectId ?? '');
  const [nodeId, setNodeId] = useState('');
  const [hostname, setHostname] = useState('');
  const [tlsMode, setTlsMode] = useState<CreateDomainRequestTlsMode>('tunnel');

  const [sourceKind, setSourceKind] = useState<SourceKind>(sheet?.source ?? 'image');
  const [repoUrl, setRepoUrl] = useState('');
  const [sourceRef, setSourceRef] = useState(DEFAULT_SOURCE_REF);
  const [dockerfilePath, setDockerfilePath] = useState(DEFAULT_DOCKERFILE_PATH);
  const [contextDir, setContextDir] = useState(DEFAULT_CONTEXT_DIR);
  const [target, setTarget] = useState('');
  const [secretNames, setSecretNames] = useState<string[]>([]);

  const [projectView, setProjectView] = useState<'pick' | 'create'>('pick');
  const [projectName, setProjectName] = useState('');
  const [projectSlug, setProjectSlug] = useState('');
  const [slugEdited, setSlugEdited] = useState(false);

  const nextRowKey = useRef(1);
  const [envRows, setEnvRows] = useState<EnvRow[]>([
    { key: 0, name: '', value: '', show: false },
  ]);

  const nextArgKey = useRef(1);
  const [buildArgRows, setBuildArgRows] = useState<BuildArgRow[]>([
    { key: 0, name: '', value: '' },
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

  const updateArgRow = (key: number, patch: Partial<BuildArgRow>) =>
    setBuildArgRows((current) =>
      current.map((row) => (row.key === key ? { ...row, ...patch } : row)),
    );

  const addArgRow = () => {
    const key = nextArgKey.current;
    nextArgKey.current += 1;
    setBuildArgRows((current) => [...current, { key, name: '', value: '' }]);
  };

  const removeArgRow = (key: number) =>
    setBuildArgRows((current) => current.filter((row) => row.key !== key));

  const filledRows = envRows.filter(
    (row) => row.name.trim() !== '' || row.value !== '',
  );

  const filledArgRows = buildArgRows.filter(
    (row) => row.name.trim() !== '' || row.value !== '',
  );

  const availableSecretNames = filledRows
    .map((row) => row.name.trim())
    .filter((rowName) => ENV_NAME_PATTERN.test(rowName));
  const secretKey = availableSecretNames.join('\n');

  useEffect(() => {
    setSecretNames((current) =>
      current.filter((name) => availableSecretNames.includes(name)),
    );
    // `secretKey` is the content of the list; a new array with the same names
    // must not clear the selection.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [secretKey]);

  const chooseSource = (kind: SourceKind) => {
    setSourceKind(kind);
    setFieldErrors({});
    if (!nameEdited) {
      setName(kind === 'git' ? deriveAppNameFromRepo(repoUrl) : deriveAppName(image));
    }
  };

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
    if (sourceKind === 'image' && image.trim() === '') {
      errors.image = 'Enter the image reference.';
    }
    if (sourceKind === 'git') {
      Object.assign(
        errors,
        validateSourceForm({
          repoUrl,
          ref: sourceRef,
          dockerfilePath,
          contextDir,
          target,
          buildArgs: filledArgRows.map((row) => ({
            name: row.name.trim(),
            value: row.value,
          })),
          secrets: secretNames,
          envNames: availableSecretNames,
        }),
      );
    }
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
      build: sourceKind === 'git',
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
          port: Number(port),
          placement: { node_ids: [nodeId] },
          ...(sourceKind === 'git'
            ? {
                source: {
                  kind: 'git' as const,
                  repo_url: repoUrl.trim(),
                  ref: sourceRef.trim() === '' ? DEFAULT_SOURCE_REF : sourceRef.trim(),
                  dockerfile_path:
                    dockerfilePath.trim() === ''
                      ? DEFAULT_DOCKERFILE_PATH
                      : dockerfilePath.trim(),
                  context_dir:
                    contextDir.trim() === '' ? DEFAULT_CONTEXT_DIR : contextDir.trim(),
                  target: target.trim() === '' ? null : target.trim(),
                  build_args: filledArgRows.map((row) => ({
                    name: row.name.trim(),
                    value: row.value,
                  })),
                  secrets: secretNames,
                },
              }
            : { image: image.trim() }),
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

      void queryClient.invalidateQueries({ queryKey: getListAppsQueryKey() });

      if (sourceKind === 'git') {
        current = 'build';
        mark('build', 'active');
        const build = await startBuild(app.id, undefined, {
          headers: { 'Idempotency-Key': crypto.randomUUID() },
        });
        mark('build', 'done');
        void navigate({
          to: '/apps/$appId/builds/$buildId',
          params: { appId: app.id, buildId: build.id },
        });
        return;
      }

      current = 'deploy';
      mark('deploy', 'active');
      await createDeployment(
        app.id,
        { image: image.trim() },
        { headers: { 'Idempotency-Key': crypto.randomUUID() } },
      );
      mark('deploy', 'done');

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
        <PageSkeleton />
      </main>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  if (!canWrite) {
    return (
      <main className="mx-auto w-full max-w-3xl px-4 py-6 sm:px-6 sm:py-8">
        <h1 className="type-title">Deploy an app</h1>
        <div className="mt-5 rounded-[10px] border border-line bg-surface px-4 py-8">
          <p className="text-[15px] text-ink">
            Your role can read apps but not deploy them.
          </p>
          <Button variant="default" className="mt-4" asChild>
            <Link to="/projects">Back to projects</Link>
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
  const liveRepoError =
    sourceKind === 'git' && repoUrl.trim() !== ''
      ? repoUrlError(repoUrl) ?? undefined
      : undefined;
  const nameError = fieldErrors.name ?? liveNameError;
  const portError = fieldErrors.port ?? livePortError;
  const hostnameError = fieldErrors.hostname ?? liveHostnameError;
  const repoError = fieldErrors.repo_url ?? liveRepoError;

  const toggleSecret = (name: string, checked: boolean) =>
    setSecretNames((current) =>
      checked ? [...current, name] : current.filter((item) => item !== name),
    );

  // In a sheet the sheet carries the title and the source; the form sits on it flat.
  const Frame = sheet ? 'div' : 'main';

  return (
    <Frame className={sheet ? undefined : 'mx-auto w-full max-w-3xl px-4 py-6 sm:px-6 sm:py-8'}>
      {sheet ? null : (
        <>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <h1 className="type-title">Deploy an app</h1>
            <Button variant="ghost" asChild>
              <Link to="/projects">Back to projects</Link>
            </Button>
          </div>
          <p className="mt-2 text-[15px] text-ink-2">
            Name an image or a public Git repository, pick where it runs and deploy it. The
            app gets one microVM.
          </p>
        </>
      )}

      <form
        onSubmit={submit}
        noValidate
        className={
          sheet ? undefined : 'mt-5 rounded-[10px] border border-line bg-surface card-sheen'
        }
      >
        <section className={sheet ? 'px-4 pb-5 sm:px-5' : 'px-4 py-5 sm:px-5'}>
          {sheet ? null : (
          <>
          <h2 className="type-display-light text-[15px] leading-none text-ink">Source</h2>
          <div
            role="radiogroup"
            aria-label="How the app image is made"
            className="mt-4 grid grid-cols-1 gap-2.5 sm:grid-cols-2"
          >
            <label
              className={cn(
                'flex cursor-pointer items-start gap-3 rounded-[10px] border px-3 py-3 transition-colors',
                sourceKind === 'image'
                  ? 'border-accent bg-raised shadow-[0_0_0_3px_color-mix(in_oklab,var(--accent)_22%,transparent)]'
                  : 'border-line-2 hover:border-ink-3',
              )}
            >
              <input
                type="radio"
                name="source-kind"
                value="image"
                checked={sourceKind === 'image'}
                onChange={() => chooseSource('image')}
                className="mt-1 h-4 w-4 shrink-0 accent-accent-solid"
              />
              <GlyphTile kind="image" />
              <span className="min-w-0">
                <span className="type-body-medium block text-[15px] leading-tight text-ink">
                  Image
                </span>
                <span className="mt-1 block text-[13px] leading-snug text-ink-2">
                  Deploy a public OCI reference as it is.
                </span>
              </span>
            </label>
            <label
              className={cn(
                'flex cursor-pointer items-start gap-3 rounded-[10px] border px-3 py-3 transition-colors',
                sourceKind === 'git'
                  ? 'border-accent bg-raised shadow-[0_0_0_3px_color-mix(in_oklab,var(--accent)_22%,transparent)]'
                  : 'border-line-2 hover:border-ink-3',
              )}
            >
              <input
                type="radio"
                name="source-kind"
                value="git"
                checked={sourceKind === 'git'}
                onChange={() => chooseSource('git')}
                className="mt-1 h-4 w-4 shrink-0 accent-accent-solid"
              />
              <GlyphTile kind="git" />
              <span className="min-w-0">
                <span className="type-body-medium block text-[15px] leading-tight text-ink">
                  Git repository
                </span>
                <span className="mt-1 block text-[13px] leading-snug text-ink-2">
                  Build the image from a public repository and a Dockerfile.
                </span>
              </span>
            </label>
          </div>
          </>
          )}

          {sourceKind === 'image' ? (
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
                  <p className="text-[13px] text-fault">{fieldErrors.image}</p>
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
                  <p className="text-[13px] text-fault">{portError}</p>
                ) : null}
              </Field>
            </div>
          ) : (
            <>
              <div className="mt-4 grid grid-cols-1 gap-4 sm:grid-cols-[minmax(0,1fr)_140px]">
                <Field
                  label="Repository URL"
                  htmlFor="app-repo"
                  hint="Public repositories only, over https://."
                >
                  <Input
                    id="app-repo"
                    value={repoUrl}
                    onChange={(event) => {
                      setRepoUrl(event.target.value);
                      if (!nameEdited) setName(deriveAppNameFromRepo(event.target.value));
                      setFieldErrors((current) => {
                        const next = { ...current };
                        delete next.repo_url;
                        return next;
                      });
                    }}
                    placeholder="https://github.com/example/docs-site.git"
                    spellCheck={false}
                    autoFocus
                    aria-invalid={repoError !== undefined}
                  />
                  {repoError ? (
                    <p className="text-[13px] text-fault">{repoError}</p>
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
                    <p className="text-[13px] text-fault">{portError}</p>
                  ) : null}
                </Field>
              </div>

              <div className="mt-4 grid grid-cols-1 gap-4 sm:grid-cols-2">
                <Field label="Ref" htmlFor="app-ref" hint="Branch, tag or full commit SHA.">
                  <Input
                    id="app-ref"
                    value={sourceRef}
                    onChange={(event) => setSourceRef(event.target.value)}
                    placeholder={DEFAULT_SOURCE_REF}
                    spellCheck={false}
                    aria-invalid={fieldErrors.ref !== undefined}
                  />
                  {fieldErrors.ref ? (
                    <p className="text-[13px] text-fault">{fieldErrors.ref}</p>
                  ) : null}
                </Field>
                <Field
                  label="Target stage"
                  htmlFor="app-target"
                  hint="Optional. Leave it empty to build the last stage."
                >
                  <Input
                    id="app-target"
                    value={target}
                    onChange={(event) => setTarget(event.target.value)}
                    placeholder="runtime"
                    spellCheck={false}
                    aria-invalid={fieldErrors.target !== undefined}
                  />
                  {fieldErrors.target ? (
                    <p className="text-[13px] text-fault">{fieldErrors.target}</p>
                  ) : null}
                </Field>
                <Field
                  label="Dockerfile path"
                  htmlFor="app-dockerfile"
                  hint="Relative to the build context."
                >
                  <Input
                    id="app-dockerfile"
                    value={dockerfilePath}
                    onChange={(event) => setDockerfilePath(event.target.value)}
                    placeholder={DEFAULT_DOCKERFILE_PATH}
                    spellCheck={false}
                    aria-invalid={fieldErrors.dockerfile !== undefined}
                  />
                  {fieldErrors.dockerfile ? (
                    <p className="text-[13px] text-fault">{fieldErrors.dockerfile}</p>
                  ) : null}
                </Field>
                <Field
                  label="Context directory"
                  htmlFor="app-context"
                  hint="Relative to the repository root; . is the root."
                >
                  <Input
                    id="app-context"
                    value={contextDir}
                    onChange={(event) => setContextDir(event.target.value)}
                    placeholder={DEFAULT_CONTEXT_DIR}
                    spellCheck={false}
                    aria-invalid={fieldErrors.context !== undefined}
                  />
                  {fieldErrors.context ? (
                    <p className="text-[13px] text-fault">{fieldErrors.context}</p>
                  ) : null}
                </Field>
              </div>

              <div className="mt-4">
                <div className="flex flex-wrap items-baseline justify-between gap-2">
                  <span className="type-body-medium text-[13px] text-ink-2">
                    Build arguments
                  </span>
                  <button
                    type="button"
                    className="text-[13px] text-accent underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
                    onClick={addArgRow}
                  >
                    Add argument
                  </button>
                </div>
                <p className="mt-1 text-[13px] leading-snug text-ink-2">
                  Passed as <span className="type-code">--build-arg</span>. They are not
                  secret: they end up in the image history.
                </p>
                <div className="mt-2 flex flex-col gap-2">
                  {buildArgRows.map((row) => (
                    <div
                      key={row.key}
                      className="grid grid-cols-1 gap-2 sm:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto]"
                    >
                      <Input
                        aria-label="Argument name"
                        value={row.name}
                        onChange={(event) => updateArgRow(row.key, { name: event.target.value })}
                        placeholder="NODE_ENV"
                        spellCheck={false}
                      />
                      <Input
                        aria-label="Argument value"
                        value={row.value}
                        onChange={(event) => updateArgRow(row.key, { value: event.target.value })}
                        placeholder="production"
                        spellCheck={false}
                      />
                      <Button
                        variant="ghost"
                        className="justify-self-start px-2.5"
                        onClick={() => removeArgRow(row.key)}
                        disabled={buildArgRows.length === 1}
                      >
                        Remove
                      </Button>
                    </div>
                  ))}
                </div>
                {fieldErrors.build_args ? (
                  <p className="mt-2 text-[13px] text-fault">{fieldErrors.build_args}</p>
                ) : null}
              </div>
            </>
          )}
        </section>

        <section className="border-t border-line px-4 py-5 sm:px-5">
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
                <p className="text-[13px] text-fault">{nameError}</p>
              ) : null}
            </Field>

            <div className="flex flex-col gap-1.5">
              <div className="flex items-baseline justify-between gap-3">
                <span className="type-body-medium text-[13px] text-ink-2">Project</span>
                {projectView === 'pick' ? (
                  <button
                    type="button"
                    className="text-[13px] text-accent underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
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
                    <p className="text-[13px] text-fault">{fieldErrors.project}</p>
                  ) : null}
                </>
              ) : (
                <div className="rounded-[10px] border border-line bg-sunk/40 p-3">
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
                        <p className="text-[13px] text-fault">{projectSlugError}</p>
                      ) : null}
                    </Field>
                  </div>
                  <div className="mt-3 flex flex-wrap gap-2">
                    <Button
                      busy={createProjectMutation.isPending}
                      variant="default"
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
                      variant="ghost"
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
                <p className="text-[13px] text-fault">{fieldErrors.node}</p>
              ) : null}
            </Field>
          </div>
        </section>

        <section className="border-t border-line px-4 py-5 sm:px-5">
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
                <p className="text-[13px] text-fault">{hostnameError}</p>
              ) : null}
            </Field>

            <fieldset className="flex flex-col gap-2">
              <legend className="type-body-medium mb-1 text-[13px] text-ink-2">
                TLS mode
              </legend>
              <label className="flex items-center gap-2 text-[15px] text-ink">
                <input
                  type="radio"
                  name="tls-mode"
                  value="tunnel"
                  checked={tlsMode === 'tunnel'}
                  onChange={() => setTlsMode('tunnel')}
                  className="h-4 w-4 accent-accent-solid"
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
                  className="h-4 w-4 accent-accent-solid"
                />
                ACME certificate
              </label>
            </fieldset>
          </div>
        </section>

        <section className="border-t border-line px-4 py-5 sm:px-5">
          <div className="flex flex-wrap items-baseline justify-between gap-2">
            <h2 className="type-display-light text-[15px] leading-none text-ink">
              Environment
            </h2>
            <button
              type="button"
              className="text-[13px] text-accent underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
              onClick={addRow}
            >
              Add variable
            </button>
          </div>
          <p className="mt-2 text-[13px] leading-snug text-ink-2">
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
                    variant="ghost"
                    className="px-2.5"
                    aria-pressed={row.show}
                    onClick={() => updateRow(row.key, { show: !row.show })}
                  >
                    {row.show ? 'Hide' : 'Show'}
                  </Button>
                </div>
                <Button
                  variant="ghost"
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
            <p className="mt-2 text-[13px] text-fault">{fieldErrors.env}</p>
          ) : null}

          {sourceKind === 'git' ? (
            <fieldset className="mt-5 border-t border-line pt-4">
              <legend className="type-body-medium mb-1 text-[13px] text-ink-2">
                Build secrets
              </legend>
              <p className="text-[13px] leading-snug text-ink-2">
                Variables above to mount into the build. Their values still reach the
                app and are never written to the build log.
              </p>
              {availableSecretNames.length === 0 ? (
                <p className="mt-2 text-[13px] text-ink-2">
                  Name a variable above to use it as a build secret.
                </p>
              ) : (
                <div className="mt-2 flex flex-wrap gap-x-5 gap-y-2">
                  {availableSecretNames.map((secretName) => (
                    <label
                      key={secretName}
                      className="flex items-center gap-2 text-[15px] text-ink"
                    >
                      <input
                        type="checkbox"
                        checked={secretNames.includes(secretName)}
                        onChange={(event) =>
                          toggleSecret(secretName, event.target.checked)
                        }
                        className="h-4 w-4 accent-accent-solid"
                      />
                      <span className="type-code">{secretName}</span>
                    </label>
                  ))}
                </div>
              )}
              {fieldErrors.secrets ? (
                <p className="mt-2 text-[13px] text-fault">{fieldErrors.secrets}</p>
              ) : null}
            </fieldset>
          ) : null}
        </section>

        {showChecklist ? (
          <section className="border-t border-line px-4 py-5 sm:px-5">
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
                      <span className="type-data text-[13px] text-ink-2">
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

        <div className="flex flex-wrap items-center justify-end gap-2 border-t border-line px-4 py-4 sm:px-5">
          {problem && failedStep === null ? <ProblemBanner view={problem} className="mr-auto" /> : null}
          {canOpenApp ? (
            <Button variant="primary" asChild>
              <Link to="/apps/$appId" params={{ appId: createdAppId ?? '' }}>
                Open the app
              </Link>
            </Button>
          ) : (
            <Button busy={submitting} variant="primary" type="submit" disabled={submitting}>
              {submitting
                ? sourceKind === 'git'
                  ? 'Starting the build…'
                  : 'Deploying…'
                : sourceKind === 'git'
                  ? 'Build and deploy'
                  : 'Deploy'}
            </Button>
          )}
        </div>
      </form>
    </Frame>
  );
}
