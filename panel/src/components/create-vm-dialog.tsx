import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useState, type FormEvent } from 'react';
import { createProject, getListProjectsQueryKey } from '../api/generated/projects/projects';
import type { Node, Project } from '../api/generated/model';
import { createVm, getListVmsQueryKey } from '../api/generated/vms/vms';
import { problemView, type ProblemView } from '../lib/problem';
import { validateSlug, deriveSlug } from '../lib/slug';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';
import { Field } from './ui/field';
import { Input } from './ui/input';
import { ProblemBanner } from './ui/problem-banner';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select';

// A VM runs its image's default command, and M1 has no field to override it:
// an image whose command exits at once (alpine's shell) stops right after it
// starts. whoami is a small static HTTP server that keeps running.
const DEFAULT_IMAGE = 'docker.io/traefik/whoami:v1.11';

interface FormState {
  name: string;
  vcpus: string;
  memoryMb: string;
  diskGb: string;
  image: string;
  projectId: string;
  nodeId: string;
}

const initialForm = (nodes: Node[], projectId = ''): FormState => ({
  name: '',
  vcpus: '1',
  memoryMb: '512',
  diskGb: '2',
  image: DEFAULT_IMAGE,
  projectId,
  nodeId: nodes[0]?.id ?? '',
});

const fieldMessage = (value: string, kind: 'int' | 'name'): string | undefined => {
  if (kind === 'name') {
    if (value.trim().length === 0) return 'Give the VM a name.';
    if (value.trim().length > 200) return 'Keep the name under 200 characters.';
    return undefined;
  }
  const parsed = Number(value);
  if (value.trim() === '' || !Number.isFinite(parsed)) return 'Enter a number.';
  if (!Number.isInteger(parsed)) return 'Enter a whole number.';
  if (parsed < 1) return 'Use at least 1.';
  return undefined;
};

/**
 * Create a VM: a name, its size, an image, a project and a node. A dialog
 * over the VM list; with `sheet`, the same form without the dialog, for a
 * project's create sheet, with that project chosen. Closing (Cancel, or
 * success) calls `onOpenChange(false)` either way.
 */
export function CreateVmDialog({
  open,
  onOpenChange,
  projects,
  nodes,
  onCreated,
  sheet,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  projects: Project[];
  nodes: Node[];
  onCreated: (name: string) => void;
  sheet?: { projectId: string };
}) {
  const queryClient = useQueryClient();
  const [form, setForm] = useState<FormState>(() => initialForm(nodes, sheet?.projectId));
  const [projectView, setProjectView] = useState<'pick' | 'create'>('pick');
  const [projectName, setProjectName] = useState('');
  const [projectSlug, setProjectSlug] = useState('');
  const [slugEdited, setSlugEdited] = useState(false);
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});

  const set = (key: keyof FormState, value: string) =>
    setForm((current) => ({ ...current, [key]: value }));

  const createVmMutation = useMutation({
    mutationFn: () =>
      createVm(
        {
          project_id: form.projectId,
          node_id: form.nodeId || nodes[0]?.id || '',
          name: form.name.trim(),
          vcpus: Number(form.vcpus),
          memory_mb: Number(form.memoryMb),
          disk_gb: Number(form.diskGb),
          image: form.image.trim(),
        },
        { headers: { 'Idempotency-Key': crypto.randomUUID() } },
      ),
    onSuccess: (vm) => {
      void queryClient.invalidateQueries({ queryKey: getListVmsQueryKey() });
      setForm(initialForm(nodes, sheet?.projectId));
      setProjectView('pick');
      onOpenChange(false);
      onCreated(vm.name);
    },
    onError: (error) => setProblem(problemView(error, 'create VMs')),
  });

  const createProjectMutation = useMutation({
    mutationFn: () => createProject({ name: projectName.trim(), slug: projectSlug }),
    onSuccess: (project) => {
      void queryClient.invalidateQueries({ queryKey: getListProjectsQueryKey() });
      setForm((current) => ({ ...current, projectId: project.id }));
      setProjectName('');
      setProjectSlug('');
      setSlugEdited(false);
      setProjectView('pick');
    },
    onError: (error) => setProblem(problemView(error, 'create projects')),
  });

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setProblem(null);

    const errors: Record<string, string> = {};
    const nameError = fieldMessage(form.name, 'name');
    if (nameError) errors.name = nameError;
    const vcpuError = fieldMessage(form.vcpus, 'int');
    if (vcpuError) errors.vcpus = vcpuError;
    const memoryError = fieldMessage(form.memoryMb, 'int');
    if (memoryError) errors.memory = memoryError;
    const diskError = fieldMessage(form.diskGb, 'int');
    if (diskError) errors.disk = diskError;
    if (form.projectId === '') errors.project = 'Choose a project, or create one here.';
    if ((form.nodeId || nodes[0]?.id) === '') errors.node = 'No node is available.';
    setFieldErrors(errors);
    if (Object.keys(errors).length > 0) return;

    createVmMutation.mutate();
  };

  const projectSlugError =
    projectView === 'create' && projectSlug !== '' ? validateSlug(projectSlug) : undefined;

  const body = (
        <form onSubmit={submit} className="flex flex-col gap-4" noValidate>
          {problem ? <ProblemBanner view={problem} /> : null}

          <Field label="Name" htmlFor="vm-name" className="[&>label]:mb-0">
            <Input
              id="vm-name"
              value={form.name}
              onChange={(event) => set('name', event.target.value)}
              placeholder="web-1"
              autoFocus
              aria-invalid={fieldErrors.name !== undefined}
            />
            {fieldErrors.name ? (
              <p className="text-[13px] text-fault">{fieldErrors.name}</p>
            ) : null}
          </Field>

          <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
            <Field label="vCPUs" htmlFor="vm-vcpus">
              <Input
                id="vm-vcpus"
                type="number"
                min={1}
                step={1}
                inputMode="numeric"
                value={form.vcpus}
                onChange={(event) => set('vcpus', event.target.value)}
                aria-invalid={fieldErrors.vcpus !== undefined}
              />
              {fieldErrors.vcpus ? (
                <p className="text-[13px] text-fault">{fieldErrors.vcpus}</p>
              ) : null}
            </Field>
            <Field label="Memory (MiB)" htmlFor="vm-memory">
              <Input
                id="vm-memory"
                type="number"
                min={1}
                step={1}
                inputMode="numeric"
                value={form.memoryMb}
                onChange={(event) => set('memoryMb', event.target.value)}
                aria-invalid={fieldErrors.memory !== undefined}
              />
              {fieldErrors.memory ? (
                <p className="text-[13px] text-fault">{fieldErrors.memory}</p>
              ) : null}
            </Field>
            <Field label="Disk (GiB)" htmlFor="vm-disk">
              <Input
                id="vm-disk"
                type="number"
                min={1}
                step={1}
                inputMode="numeric"
                value={form.diskGb}
                onChange={(event) => set('diskGb', event.target.value)}
                aria-invalid={fieldErrors.disk !== undefined}
              />
              {fieldErrors.disk ? (
                <p className="text-[13px] text-fault">{fieldErrors.disk}</p>
              ) : null}
            </Field>
          </div>

          <Field label="Image" htmlFor="vm-image">
            <Input
              id="vm-image"
              value={form.image}
              onChange={(event) => set('image', event.target.value)}
              spellCheck={false}
            />
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
                  value={form.projectId}
                  onValueChange={(value) => {
                    set('projectId', value);
                    setFieldErrors((current) => {
                      const next = { ...current };
                      delete next.project;
                      return next;
                    });
                  }}
                >
                  <SelectTrigger id="vm-project" aria-label="Project">
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
                ) : (
                  <p className="text-[13px] text-ink-2">
                    VMs belong to a project; apps will group under it too.
                  </p>
                )}
              </>
            ) : (
              <div className="rounded-[10px] border border-line bg-sunk/40 p-3">
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                  <Field label="Project name" htmlFor="project-name">
                    <Input
                      id="project-name"
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
                    htmlFor="project-slug"
                    hint="Lowercase letters, digits and dashes."
                  >
                    <Input
                      id="project-slug"
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

          <Field label="Node" htmlFor="vm-node">
            <Select
              value={form.nodeId || nodes[0]?.id}
              onValueChange={(value) => set('nodeId', value)}
            >
              <SelectTrigger id="vm-node" aria-label="Node">
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

          <div className="mt-1 flex flex-wrap justify-end gap-2 border-t border-line pt-4">
            <Button variant="default" onClick={() => onOpenChange(false)}>
              Cancel
            </Button>
            <Button busy={createVmMutation.isPending} variant="primary" type="submit" disabled={createVmMutation.isPending}>
              {createVmMutation.isPending ? 'Creating…' : 'Create VM'}
            </Button>
          </div>
        </form>
  );

  if (sheet) return body;

  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!next) {
          setProblem(null);
          setFieldErrors({});
        }
        onOpenChange(next);
      }}
    >
      <DialogContent
        title="New VM"
        description="Choose a project and a node. The microVM boots from the image you name."
      >
        {body}
      </DialogContent>
    </Dialog>
  );
}
