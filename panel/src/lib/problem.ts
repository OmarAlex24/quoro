import { isApiProblem } from '../api/fetcher';
import type { Problem } from '../api/generated/model';

/** A problem said in plain words, plus the machine facts in muted text. */
export interface ProblemView {
  message: string;
  code?: Problem['code'];
  reason?: Problem['reason'];
  scope?: string;
}

/**
 * Turn an API refusal into something an operator can act on. `action` is the
 * verb phrase of what they tried, for example "create VMs".
 */
export const problemView = (error: unknown, action: string): ProblemView => {
  if (!isApiProblem(error)) {
    return {
      message: 'Could not reach the server. Check the connection and try again.',
    };
  }

  const codes: ProblemView = { message: '', code: error.code };
  if (error.reason) codes.reason = error.reason;
  if (error.scope) codes.scope = error.scope;

  switch (error.code) {
    case 'unauthenticated':
      return { ...codes, message: 'Your session ended. Sign in again.' };
    case 'forbidden':
      return {
        ...codes,
        message:
          error.reason && error.scope
            ? `Your role can't ${action} (${error.reason}: ${error.scope})`
            : error.reason
              ? `Your role can't ${action} (${error.reason})`
              : `Your role can't ${action}.`,
      };
    case 'validation_failed':
      return {
        ...codes,
        message: error.detail ?? 'The form needs a fix before it can be sent.',
      };
    case 'payload_too_large':
      return {
        ...codes,
        message: error.detail ?? 'That request is too large to send. Send less at once.',
      };
    case 'conflict':
      return {
        ...codes,
        message: error.detail ?? 'That conflicts with the current state. Reload and try again.',
      };
    case 'not_found':
      return {
        ...codes,
        message: error.detail ?? 'That is already gone. The list has been refreshed.',
      };
    case 'node_unreachable':
      return {
        ...codes,
        message: error.detail ?? 'The node is not reachable right now.',
      };
    case 'not_primary':
      return {
        ...codes,
        message: error.detail ?? 'This node is not the primary. Send the request to the primary.',
      };
    case 'rate_limited':
      return {
        ...codes,
        message: error.detail ?? 'Too many requests. Wait a moment and try again.',
      };
    case 'approval_required':
      return {
        ...codes,
        message: error.detail ?? 'That needs an approval grant before it can run.',
      };
    case 'internal':
      return {
        ...codes,
        message: error.detail ?? 'The server could not answer. Try again.',
      };
  }
};

/** The muted second line of a banner: the code, and the reason when there is one. */
export const problemFacts = (view: ProblemView): string => {
  return [view.code, view.reason]
    .filter((part) => typeof part === 'string' && part.length > 0)
    .join(' · ');
};
