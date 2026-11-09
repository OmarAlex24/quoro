import { isApiProblem } from '../api/fetcher';

/**
 * The one line the login screen says after a successful setup. Setup does not
 * start a session (the contract returns the user, not a cookie), so the panel
 * carries the news across the navigation in memory.
 */
let notice: string | undefined;

export const setSetupNotice = (value: string) => {
  notice = value;
};

export const takeSetupNotice = (): string | undefined => {
  const value = notice;
  notice = undefined;
  return value;
};

/*
 * Setup runs once, while the cluster has no user. The contract has no public
 * read that says whether setup is done (`GET /api/v1/health` answers only
 * `status`), so the panel cannot know before it tries: the form stays, and the
 * cluster's answer decides. Setup answers 409 `conflict` as soon as a user
 * exists, before it reads the token, so that answer means the cluster is
 * already set up whatever was typed.
 */

/** Whether a failed setup means the cluster already has its owner. */
export const setupAlreadyDone = (error: unknown): boolean => isApiProblem(error) && error.status === 409;

/** What the setup page says once the cluster answered that it is already set up. */
export const SETUP_DONE = {
  title: 'This cluster is already set up',
  lede: 'Its owner account exists, so setup cannot run again.',
  next: 'Sign in with your account. If you have none yet, ask an owner of this cluster to invite you.',
} as const;
