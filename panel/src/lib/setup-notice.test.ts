// SPDX-License-Identifier: AGPL-3.0-only
import { describe, expect, it } from 'vitest';
import { ApiProblem } from '../api/fetcher';
import { SETUP_DONE, setSetupNotice, setupAlreadyDone, takeSetupNotice } from './setup-notice';

describe('setupAlreadyDone', () => {
  // Issue 640: once the owner exists, setup answers 409 `conflict` before it reads the token.
  it('reads a conflict as a cluster that is already set up', () => {
    expect(setupAlreadyDone(new ApiProblem({ status: 409, code: 'conflict', detail: 'Setup is already done.' }))).toBe(
      true,
    );
  });

  it('leaves every other failure to the problem banner', () => {
    expect(setupAlreadyDone(new ApiProblem({ status: 401, code: 'unauthenticated' }))).toBe(false);
    expect(setupAlreadyDone(new ApiProblem({ status: 422, code: 'validation_failed' }))).toBe(false);
    expect(setupAlreadyDone(new Error('network down'))).toBe(false);
    expect(setupAlreadyDone(undefined)).toBe(false);
  });

  it('says the cluster is set up in words, with no raw problem code', () => {
    const words = Object.values(SETUP_DONE).join(' ');
    expect(SETUP_DONE.title).toBe('This cluster is already set up');
    expect(words).not.toMatch(/conflict|409/);
  });
});

describe('the setup notice', () => {
  it('is said once, on the next sign-in screen', () => {
    setSetupNotice('Owner created.');
    expect(takeSetupNotice()).toBe('Owner created.');
    expect(takeSetupNotice()).toBeUndefined();
  });
});
