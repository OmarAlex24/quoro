import type { Approval, App, Vm } from '../api/generated/model';

/*
 * What needs a person radiates. On the canvas an aura marks a card that is
 * waiting on an approval or deploying (amber) or that failed (red). Rules:
 * - per card, waiting on a person beats deploying, and deploying beats failed;
 * - red never breathes: a failure stays put until someone acts;
 * - at most one aura breathes on a screen: the first card waiting on a
 *   person, otherwise the first card deploying, in the order given (the
 *   canvas passes its reading order);
 * - under `prefers-reduced-motion` the CSS stops the breathing.
 */

export type AuraReason = 'waiting' | 'deploying' | 'failed';
export type AuraTone = 'shift' | 'fault';

export interface AuraSignals {
  key: string;
  /** A pending approval targets this service. */
  waiting: boolean;
  /** The app is deploying. */
  deploying: boolean;
  /** The app is down, the VM crashed, or the app's newest build failed. */
  failed: boolean;
}

export interface Aura {
  readonly reason: AuraReason;
  readonly tone: AuraTone;
  readonly breathe: boolean;
}

const reasonOf = (signals: AuraSignals): AuraReason | null => {
  if (signals.waiting) return 'waiting';
  if (signals.deploying) return 'deploying';
  if (signals.failed) return 'failed';
  return null;
};

/** The aura of each card that has one, keyed by service key. */
export const chooseAuras = (items: readonly AuraSignals[]): Record<string, Aura> => {
  const reasons = items
    .map((item) => ({ key: item.key, reason: reasonOf(item) }))
    .filter((entry): entry is { key: string; reason: AuraReason } => entry.reason !== null);

  const breather =
    reasons.find((entry) => entry.reason === 'waiting') ??
    reasons.find((entry) => entry.reason === 'deploying');

  const out: Record<string, Aura> = {};
  for (const entry of reasons) {
    out[entry.key] = {
      reason: entry.reason,
      tone: entry.reason === 'failed' ? 'fault' : 'shift',
      breathe: entry.key === breather?.key,
    };
  }
  return out;
};

/** Is a pending approval waiting on a decision about this app or VM? */
export const waitsOnApproval = (
  approvals: ReadonlyArray<Pick<Approval, 'status' | 'target'>>,
  target: { type: 'app' | 'vm'; id: string },
): boolean =>
  approvals.some(
    (approval) =>
      approval.status === 'pending' &&
      approval.target.type === target.type &&
      approval.target.id === target.id,
  );

export const appFailed = (app: Pick<App, 'status'>, newestBuildFailed: boolean): boolean =>
  app.status === 'down' || newestBuildFailed;

export const vmFailed = (vm: Pick<Vm, 'observed_status'>): boolean =>
  vm.observed_status === 'crashed';
