import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from '@tanstack/react-router';
import { useState, type FormEvent } from 'react';
import { createProject, getListProjectsQueryKey } from '../api/generated/projects/projects';
import { problemView, type ProblemView } from '../lib/problem';
import { deriveSlug, validateSlug } from '../lib/slug';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';
import { Field } from './ui/field';
import { Input } from './ui/input';
import { ProblemBanner } from './ui/problem-banner';

/**
 * `createProject`: a name and a slug (derived from the name until edited).
 * The new, empty project opens on its canvas.
 */
export function NewProjectDialog({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const [name, setName] = useState('');
  const [slug, setSlug] = useState('');
  const [slugEdited, setSlugEdited] = useState(false);
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const reset = () => {
    setName('');
    setSlug('');
    setSlugEdited(false);
    setProblem(null);
  };

  const create = useMutation({
    mutationFn: () => createProject({ name: name.trim(), slug }),
    onSuccess: (project) => {
      void queryClient.invalidateQueries({ queryKey: getListProjectsQueryKey() });
      reset();
      onOpenChange(false);
      void navigate({ to: '/projects/$projectId', params: { projectId: project.id } });
    },
    onError: (error) => setProblem(problemView(error, 'create projects')),
  });

  const slugError = slug !== '' ? validateSlug(slug) : undefined;
  const ready = name.trim() !== '' && validateSlug(slug) === undefined;

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setProblem(null);
    if (ready) create.mutate();
  };

  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!next) reset();
        onOpenChange(next);
      }}
    >
      <DialogContent
        title="New project"
        description="A project groups apps and VMs. Its slug names it in grants and the CLI."
      >
        <form onSubmit={submit} className="flex flex-col gap-4" noValidate>
          {problem ? <ProblemBanner view={problem} /> : null}
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <Field label="Name" htmlFor="new-project-name">
              <Input
                id="new-project-name"
                value={name}
                placeholder="Atlas"
                autoFocus
                onChange={(event) => {
                  setName(event.target.value);
                  if (!slugEdited) setSlug(deriveSlug(event.target.value));
                }}
              />
            </Field>
            <Field label="Slug" htmlFor="new-project-slug" hint="Lowercase letters, digits and dashes.">
              <Input
                id="new-project-slug"
                value={slug}
                spellCheck={false}
                aria-invalid={slugError !== undefined}
                onChange={(event) => {
                  setSlugEdited(true);
                  setSlug(event.target.value);
                }}
              />
              {slugError ? <p className="text-[13px] text-fault">{slugError}</p> : null}
            </Field>
          </div>
          <div className="mt-1 flex flex-wrap justify-end gap-2 border-t border-line pt-4">
            <Button variant="default" onClick={() => onOpenChange(false)}>
              Cancel
            </Button>
            <Button variant="primary" type="submit" disabled={!ready || create.isPending}>
              {create.isPending ? 'Creating…' : 'Create project'}
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
}
