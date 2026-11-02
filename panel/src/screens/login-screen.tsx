import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Link, useNavigate } from '@tanstack/react-router';
import { useEffect, useState, type FormEvent } from 'react';
import { getGetMeQueryKey, login } from '../api/generated/auth/auth';
import { authEvents } from '../api/auth-store';
import { isApiProblem } from '../api/fetcher';
import { problemView, type ProblemView } from '../lib/problem';
import { takeSetupNotice } from '../lib/setup-notice';
import { useSession } from '../session';
import { AuthPanel } from '../components/auth-panel';
import { Button } from '../components/ui/button';
import { Field } from '../components/ui/field';
import { Input } from '../components/ui/input';
import { ProblemBanner } from '../components/ui/problem-banner';

export function LoginScreen() {
  const { user, ended } = useSession();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [created] = useState(() => takeSetupNotice());
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [problem, setProblem] = useState<ProblemView | null>(null);

  useEffect(() => {
    if (user) void navigate({ to: '/' });
  }, [user, navigate]);

  const loginMutation = useMutation({
    mutationFn: () => login({ email: email.trim(), password }),
    onSuccess: (me) => {
      authEvents.startSession();
      queryClient.setQueryData(getGetMeQueryKey(), me);
      setProblem(null);
      void navigate({ to: '/' });
    },
    onError: (error) => {
      if (isApiProblem(error) && error.status === 401) {
        setProblem({
          message: 'Email or password is incorrect.',
          code: 'unauthenticated',
        });
        return;
      }
      setProblem(problemView(error, 'log in'));
    },
  });

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setProblem(null);
    loginMutation.mutate();
  };

  return (
    <AuthPanel
      footer={
        <p className="text-[13px] text-ink-muted">
          First time here?{' '}
          <Link
            to="/setup"
            className="text-cobalt underline-offset-4 hover:underline"
          >
            Set up the owner account
          </Link>
        </p>
      }
    >
      {ended ? (
        <div className="mb-4 rounded-[6px] border border-rail border-l-4 border-l-lamp-fault bg-panel px-3.5 py-2.5">
          <p className="text-[15px] leading-snug text-ink">
            Your session ended. Log in again.
          </p>
        </div>
      ) : created ? (
        <p className="mb-4 text-[13px] text-ink-muted">{created}</p>
      ) : null}

      <h2 className="type-display text-[18px] leading-tight">Log in</h2>

      <form onSubmit={submit} className="mt-4 flex flex-col gap-4" noValidate>
        {problem ? <ProblemBanner view={problem} /> : null}

        <Field label="Email" htmlFor="login-email">
          <Input
            id="login-email"
            type="email"
            name="email"
            autoComplete="username"
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            autoFocus
            required
          />
        </Field>

        <Field label="Password" htmlFor="login-password">
          <Input
            id="login-password"
            type="password"
            name="password"
            autoComplete="current-password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            required
          />
        </Field>

        <Button
          type="submit"
          variant="primary"
          className="mt-1 w-full"
          disabled={loginMutation.isPending}
        >
          {loginMutation.isPending ? 'Logging in…' : 'Log in'}
        </Button>
      </form>
    </AuthPanel>
  );
}
