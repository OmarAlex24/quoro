import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useState, type FormEvent } from 'react';
import {
  createAgent,
  getGetAgentQueryKey,
  getListAgentsQueryKey,
  updateAgent,
} from '../api/generated/agents/agents';
import type { Agent } from '../api/generated/model';
import {
  AGENT_DESCRIPTION_MAX,
  AGENT_NAME_MAX,
  agentDraftProblem,
  agentPatchIsEmpty,
  buildAgentPatch,
  type AgentDraft,
} from '../lib/agents';
import { problemView, type ProblemView } from '../lib/problem';
import { UserName } from './identity-name';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';
import { Field } from './ui/field';
import { Input } from './ui/input';
import { ProblemBanner } from './ui/problem-banner';
import { Textarea } from './ui/textarea';

const emptyDraft = (): AgentDraft => ({ name: '', model: '', description: '' });

const draftOf = (agent: Agent): AgentDraft => ({
  name: agent.name,
  model: agent.model,
  description: agent.description ?? '',
});

function AgentFields({
  idPrefix,
  draft,
  onChange,
  autoFocus,
}: {
  idPrefix: string;
  draft: AgentDraft;
  onChange: (draft: AgentDraft) => void;
  autoFocus: boolean;
}) {
  const set = (patch: Partial<AgentDraft>) => onChange({ ...draft, ...patch });

  return (
    <>
      <Field label="Name" htmlFor={`${idPrefix}-name`}>
        <Input
          id={`${idPrefix}-name`}
          value={draft.name}
          onChange={(event) => set({ name: event.target.value })}
          placeholder="on-call-restarter"
          maxLength={AGENT_NAME_MAX}
          autoFocus={autoFocus}
          spellCheck={false}
        />
      </Field>

      <Field
        label="Model"
        htmlFor={`${idPrefix}-model`}
        hint="The model identifier the agent runs, recorded in every audit entry"
      >
        <Input
          id={`${idPrefix}-model`}
          value={draft.model}
          onChange={(event) => set({ model: event.target.value })}
          placeholder="claude-sonnet-5-5"
          spellCheck={false}
        />
      </Field>

      <Field label="Description" htmlFor={`${idPrefix}-description`}>
        <Textarea
          id={`${idPrefix}-description`}
          value={draft.description}
          onChange={(event) => set({ description: event.target.value })}
          placeholder="What this agent is for"
          maxLength={AGENT_DESCRIPTION_MAX}
        />
      </Field>
    </>
  );
}

const ownerLine = (
  <p className="text-[13px] text-ink-muted">Owner: you</p>
);

/** Create an agent owned by the signed-in human. */
export function CreateAgentDialog({
  open,
  onOpenChange,
  ownerUserId,
  onCreated,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  ownerUserId: string;
  onCreated: (agent: Agent) => void;
}) {
  const queryClient = useQueryClient();
  const [draft, setDraft] = useState<AgentDraft>(emptyDraft);
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const create = useMutation({
    mutationFn: (values: AgentDraft) =>
      createAgent(
        {
          name: values.name.trim(),
          model: values.model.trim(),
          owner_user_id: ownerUserId,
          ...(values.description.trim() === ''
            ? {}
            : { description: values.description.trim() }),
        },
        { headers: { 'Idempotency-Key': crypto.randomUUID() } },
      ),
    onSuccess: (agent) => {
      setProblem(null);
      setDraft(emptyDraft());
      void queryClient.invalidateQueries({ queryKey: getListAgentsQueryKey() });
      onOpenChange(false);
      onCreated(agent);
    },
    onError: (error) => setProblem(problemView(error, 'create agents')),
  });

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const invalid = agentDraftProblem(draft);
    if (invalid) {
      setProblem({ message: invalid });
      return;
    }
    setProblem(null);
    create.mutate(draft);
  };

  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!next) {
          setProblem(null);
          setDraft(emptyDraft());
        }
        onOpenChange(next);
      }}
    >
      <DialogContent
        title="Create an agent"
        description="It starts with no grants and no keys, so it can do nothing until you give it a grant."
      >
        <form onSubmit={submit} className="flex flex-col gap-4" noValidate>
          {problem ? <ProblemBanner view={problem} /> : null}

          <AgentFields idPrefix="agent-new" draft={draft} onChange={setDraft} autoFocus />
          {ownerLine}

          <div className="mt-1 flex flex-wrap justify-end gap-2 border-t border-rail pt-4">
            <Button variant="secondary" onClick={() => onOpenChange(false)}>
              Cancel
            </Button>
            <Button variant="primary" type="submit" disabled={create.isPending}>
              {create.isPending ? 'Creating…' : 'Create agent'}
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
}

/** Change name, model and description, sending only what changed. */
export function EditAgentDialog({
  open,
  onOpenChange,
  agent,
  onSaved,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  agent: Agent;
  onSaved: (agent: Agent) => void;
}) {
  const queryClient = useQueryClient();
  const [draft, setDraft] = useState<AgentDraft>(() => draftOf(agent));
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const patch = buildAgentPatch(agent, draft);

  const save = useMutation({
    mutationFn: (body: ReturnType<typeof buildAgentPatch>) => updateAgent(agent.id, body),
    onSuccess: (updated) => {
      setProblem(null);
      void queryClient.invalidateQueries({ queryKey: getGetAgentQueryKey(agent.id) });
      void queryClient.invalidateQueries({ queryKey: getListAgentsQueryKey() });
      onOpenChange(false);
      onSaved(updated);
    },
    onError: (error) => setProblem(problemView(error, 'change agents')),
  });

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const invalid = agentDraftProblem(draft);
    if (invalid) {
      setProblem({ message: invalid });
      return;
    }
    if (agentPatchIsEmpty(patch)) {
      setProblem({ message: 'Nothing has changed yet.' });
      return;
    }
    setProblem(null);
    save.mutate(patch);
  };

  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!next) {
          setProblem(null);
          setDraft(draftOf(agent));
        }
        onOpenChange(next);
      }}
    >
      <DialogContent
        title="Edit agent"
        description="Only the fields you change are sent."
      >
        <form onSubmit={submit} className="flex flex-col gap-4" noValidate>
          {problem ? <ProblemBanner view={problem} /> : null}

          <AgentFields
            idPrefix="agent-edit"
            draft={draft}
            onChange={setDraft}
            autoFocus={false}
          />
          <p className="flex flex-wrap items-baseline gap-1.5 text-[13px] text-ink-muted">
            <span>Owner</span>
            <UserName id={agent.owner_user_id} />
          </p>

          <div className="mt-1 flex flex-wrap justify-end gap-2 border-t border-rail pt-4">
            <Button variant="secondary" onClick={() => onOpenChange(false)}>
              Cancel
            </Button>
            <Button
              variant="primary"
              type="submit"
              disabled={save.isPending || agentPatchIsEmpty(patch)}
            >
              {save.isPending ? 'Saving…' : 'Save changes'}
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
}
