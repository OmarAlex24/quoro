// SPDX-License-Identifier: AGPL-3.0-only
import type { UseMutationOptions } from '@tanstack/react-query';
import { createNodeJoinToken } from '../api/generated/nodes/nodes';
import type { NodeJoinTokenCreated } from '../api/generated/model';

/** One call of `createNodeJoinToken`; the generated client unless a test passes its own. */
export type JoinTokenCreator = typeof createNodeJoinToken;

/**
 * A join token for one new machine, as Add a node and the HA view's witness
 * offer ask for it: an empty body, so the server's one-hour expiry applies,
 * and a fresh `Idempotency-Key` for every call: a repeated key with the same
 * body returns the original result instead of a new token.
 */
export const createJoinToken = ({
  create = createNodeJoinToken,
  idempotencyKey = crypto.randomUUID(),
}: { create?: JoinTokenCreator; idempotencyKey?: string } = {}): Promise<NodeJoinTokenCreated> =>
  create({}, { headers: { 'Idempotency-Key': idempotencyKey } });

/**
 * The join-token mutation of Add a node and of the HA view's witness offer,
 * passed to `useMutation` as is. Each `mutate()` is a new token, with its own
 * key. The secret lives in the mutation's result only; `reset()` drops it.
 */
export const joinTokenMutation = (): UseMutationOptions<NodeJoinTokenCreated, Error, void> => ({
  mutationFn: () => createJoinToken(),
});

/** Where the installer lives, as installer/README.md names it. */
export const INSTALLER_URL = 'https://raw.githubusercontent.com/OmarAlex24/quoro/main/installer/install.sh';

/** Where the releases are published (.github/workflows/release.yml), one tag `v<version>` each. */
export const RELEASES_URL = 'https://github.com/OmarAlex24/quoro/releases';

/**
 * The release page of the cluster's version, whose archive holds the `quoro`
 * binary and `quoro-node.service` (the installer checks for both), or the
 * list of releases while the version is unknown. A witness takes them from
 * there: the installer has no witness role.
 */
export const releasePage = (version: string | undefined): string =>
  version ? `${RELEASES_URL}/tag/v${encodeURIComponent(version)}` : RELEASES_URL;

/**
 * The installer's command for a new node (installer/README.md), as one line.
 * The URL, token and fingerprint stay placeholders: the panel shows the token
 * only inside the one-time join command, and does not take that command apart.
 * `QUORO_VERSION` is the cluster's, or a placeholder while it is unknown:
 * without it the installer takes the latest release, and a cluster runs one
 * version.
 */
export const nodeInstallerCommand = (version: string | undefined): string =>
  `curl -fsSL ${INSTALLER_URL} | sudo QUORO_ROLE=node QUORO_VERSION=${version || '<version>'} ` +
  'QUORO_JOIN_URL=<url> QUORO_JOIN_TOKEN=<token> QUORO_JOIN_FINGERPRINT=<fingerprint> sh';
