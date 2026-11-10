import { useMutation } from '@tanstack/react-query';
import { useEffect, useRef, useState } from 'react';
import { replaceAppEnv } from '../api/generated/apps/apps';
import type { AppEnvVar } from '../api/generated/model';
import { problemView, type ProblemView } from '../lib/problem';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';
import { Input } from './ui/input';
import { ProblemBanner } from './ui/problem-banner';

const ENV_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

interface Row {
  key: number;
  name: string;
  value: string;
  show: boolean;
}

export function AppEnvDialog({
  open,
  onOpenChange,
  appId,
  env,
  onSaved,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  appId: string;
  env: readonly AppEnvVar[];
  onSaved: () => void;
}) {
  const nextKey = useRef(1);
  const [rows, setRows] = useState<Row[]>([]);
  const [problem, setProblem] = useState<ProblemView | null>(null);

  useEffect(() => {
    if (!open) return;
    nextKey.current = env.length + 1;
    setRows(
      env.map((item, index) => ({
        key: index,
        name: item.name,
        value: '',
        show: false,
      })),
    );
    setProblem(null);
  }, [open, env]);

  const save = useMutation({
    mutationFn: () =>
      replaceAppEnv(appId, {
        items: rows.map((row) => ({ name: row.name.trim(), value: row.value })),
      }),
    onSuccess: () => {
      setProblem(null);
      onSaved();
      onOpenChange(false);
    },
    onError: (error) => setProblem(problemView(error, 'save variables')),
  });

  const update = (key: number, patch: Partial<Row>) =>
    setRows((current) =>
      current.map((row) => (row.key === key ? { ...row, ...patch } : row)),
    );

  const submit = () => {
    const seen = new Set<string>();
    for (const row of rows) {
      const name = row.name.trim();
      if (!ENV_NAME_PATTERN.test(name)) {
        setProblem({
          message:
            'Variable names use letters, digits and underscores, and cannot start with a digit.',
        });
        return;
      }
      if (seen.has(name)) {
        setProblem({ message: `The variable ${name} is listed twice.` });
        return;
      }
      if (row.value === '') {
        setProblem({ message: `Enter the value for ${name} again.` });
        return;
      }
      seen.add(name);
    }
    setProblem(null);
    save.mutate();
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        title="Edit variables"
        description="Saving replaces the whole environment. Values are never shown, so re-enter every value you want to keep."
      >
        <div className="flex flex-col gap-3">
          {problem ? <ProblemBanner view={problem} /> : null}

          <p className="text-[13px] leading-snug text-ink-2">
            Variables you remove here are deleted. Every listed variable needs its value
            entered again.
          </p>

          {rows.length === 0 ? (
            <p className="text-[13px] text-ink-2">
              No variables yet. Add one below.
            </p>
          ) : null}

          {rows.map((row) => (
            <div
              key={row.key}
              className="grid grid-cols-1 gap-2 sm:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto]"
            >
              <Input
                aria-label="Variable name"
                value={row.name}
                onChange={(event) => update(row.key, { name: event.target.value })}
                placeholder="DATABASE_URL"
                spellCheck={false}
              />
              <div className="flex gap-2">
                <Input
                  aria-label="Variable value"
                  type={row.show ? 'text' : 'password'}
                  value={row.value}
                  onChange={(event) => update(row.key, { value: event.target.value })}
                  placeholder="Value"
                  spellCheck={false}
                />
                <Button
                  variant="ghost"
                  className="px-2.5"
                  aria-pressed={row.show}
                  onClick={() => update(row.key, { show: !row.show })}
                >
                  {row.show ? 'Hide' : 'Show'}
                </Button>
              </div>
              <Button
                variant="ghost"
                className="justify-self-start px-2.5"
                onClick={() =>
                  setRows((current) => current.filter((item) => item.key !== row.key))
                }
              >
                Remove
              </Button>
            </div>
          ))}

          <div>
            <button
              type="button"
              className="text-[13px] text-accent underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
              onClick={() => {
                const key = nextKey.current;
                nextKey.current += 1;
                setRows((current) => [...current, { key, name: '', value: '', show: false }]);
              }}
            >
              Add variable
            </button>
          </div>

          <div className="mt-1 flex flex-wrap justify-end gap-2 border-t border-line pt-4">
            <Button variant="default" onClick={() => onOpenChange(false)}>
              Cancel
            </Button>
            <Button
              busy={save.isPending}
              variant="primary"
              disabled={save.isPending}
              onClick={submit}
            >
              {save.isPending ? 'Saving…' : 'Save variables'}
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
