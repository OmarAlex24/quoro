import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Link, useNavigate } from '@tanstack/react-router';
import { useEffect, useState, type FormEvent } from 'react';
import { getGetMeQueryKey, login } from '../api/generated/auth/auth';
import { useGetSetup } from '../api/generated/health/health';
import { authEvents } from '../api/auth-store';
import { isApiProblem } from '../api/fetcher';
import { problemView, type ProblemView } from '../lib/problem';
import { offerSetup, takeSetupNotice } from '../lib/setup-notice';
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
  // ADR 0293: the link to setup shows only while the cluster says it has no owner.
  const setupState = useGetSetup({ query: { retry: false } });

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
      setProblem(problemView(error, 'sign in'));
    },
  });

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setProblem(null);
    loginMutation.mutate();
  };

  return (
    <AuthPanel
      title="Sign in"
      lede="One microVM per app, on your own metal."
      footer={
        offerSetup(setupState.data) ? (
          <p>
            A new cluster with no owner yet?{' '}
            <Link to="/setup" className="auth-link">
              Set up the owner account
            </Link>
          </p>
        ) : undefined
      }
    >
      {ended ? (
        <p role="status" className="auth-note auth-note-fault">
          Your session ended. Sign in again.
        </p>
      ) : created ? (
        <p role="status" className="auth-note">
          {created}
        </p>
      ) : null}

      <form onSubmit={submit} className="grid gap-4" noValidate>
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

          busy={loginMutation.isPending}
          type="submit"
          variant="primary"
          className="h-9 w-full"
          disabled={loginMutation.isPending}
        >
          {loginMutation.isPending ? 'Signing in…' : 'Sign in'}
        </Button>
      </form>
    </AuthPanel>
  );
}
