// SPDX-License-Identifier: AGPL-3.0-only
/**
 * Simulated Git builds: a build walks `queued` to `succeeded` over about four
 * seconds, writing BuildKit-style lines with `seq` and `vertex`. A repository
 * URL containing `fail` stops in `building` at `[3/4] RUN ls /does-not-exist`,
 * with the Dockerfile excerpt BuildKit would print. A successful build starts
 * a `trigger: build` deployment and sets the app's image to the built image.
 * Mock mode only.
 */
import type {
  App,
  Build,
  BuildFailedStep,
  BuildLogLine,
  LogStreamName,
} from '../api/generated/model';
import { db, findApp, mockId, newestFirst, now, runDeployment, schedule } from './db';

const TERMINAL = new Set<Build['status']>([
  'succeeded',
  'failed',
  'cancelled',
]);

export const isMockBuildTerminal = (status: Build['status']): boolean =>
  TERMINAL.has(status);

export const findBuild = (buildId: string): Build | undefined =>
  db.builds.find((candidate) => candidate.id === buildId);

/** Newest first, as the contract serves a build list. */
export const appBuilds = (appId: string): Build[] =>
  newestFirst(db.builds.filter((build) => build.app_id === appId));

export const hasLiveBuild = (appId: string): boolean =>
  db.builds.some(
    (build) => build.app_id === appId && !isMockBuildTerminal(build.status),
  );

const appendLine = (
  buildId: string,
  vertex: number | null,
  text: string,
  stream: LogStreamName = 'stdout',
): void => {
  const lines = db.buildLogs[buildId] ?? (db.buildLogs[buildId] = []);
  lines.push({
    seq: lines.length + 1,
    time: now(),
    stream,
    vertex,
    line: text,
  });
};

const hex = (length: number): string => {
  let out = '';
  for (let index = 0; index < length; index += 1) {
    out += '0123456789abcdef'[Math.floor(Math.random() * 16)];
  }
  return out;
};

const failedStep = (): BuildFailedStep => {
  const error =
    'process "/bin/sh -c ls /does-not-exist" did not complete successfully: exit code: 2';
  return {
    vertex: 5,
    name: '[3/4] RUN ls /does-not-exist',
    error,
    dockerfile_line: 3,
    excerpt: [
      'Dockerfile:3',
      '--------------------',
      '   1 | FROM node:22-alpine',
      '   2 |     RUN echo "step one ok"',
      '   3 | >>> RUN ls /does-not-exist',
      '--------------------',
      `ERROR: failed to solve: ${error}`,
    ],
  };
};

const builtImage = (appId: string, buildId: string): string =>
  `build.quoro.invalid/${appId}:${buildId}`;

/**
 * Queue a build of the app's Git source. The caller has already refused the
 * app without a source and an app with a live build.
 */
export const startMockBuild = (app: App, refOverride?: string): Build => {
  const source = app.source;
  if (!source) throw new Error('A build needs a Git source');

  const ref =
    refOverride !== undefined && refOverride.trim() !== ''
      ? refOverride.trim()
      : source.ref;
  const buildSource = { ...source, ref };
  const shouldFail = /fail/i.test(buildSource.repo_url);
  const createdAt = now();

  const build: Build = {
    id: mockId('bld'),
    app_id: app.id,
    node_id: app.placement.node_ids[0] ?? db.node.id,
    source: buildSource,
    status: 'queued',
    commit: null,
    image: null,
    image_digest: null,
    failed_phase: null,
    error: null,
    failed_step: null,
    deployment_id: null,
    trigger: 'api',
    refresh_base: false,
    created_by_type: 'user',
    created_by_id: db.sessionUserId ?? 'system',
    created_at: createdAt,
    started_at: null,
    finished_at: null,
  };
  db.builds.push(build);
  db.buildLogs[build.id] = [];

  appendLine(
    build.id,
    null,
    `Queued ${buildSource.repo_url} at ${buildSource.ref} on ${db.node.name}`,
  );

  const commit = hex(40);
  const cue = (delay: number, run: (current: Build) => void) => {
    schedule(() => {
      const current = findBuild(build.id);
      if (!current || isMockBuildTerminal(current.status)) return;
      run(current);
    }, delay);
  };

  cue(250, (current) => {
    current.status = 'preparing';
    current.started_at = now();
    appendLine(current.id, 1, '#1 [internal] load build definition from Dockerfile');
  });
  cue(500, (current) => {
    appendLine(current.id, 1, '#1 transferring dockerfile: 412B done');
  });
  cue(750, (current) => {
    appendLine(
      current.id,
      2,
      '#2 [internal] load metadata for docker.io/library/node:22-alpine',
    );
  });
  // When each step began, so its DONE line says how long it really took.
  const began = new Map<number, number>();
  const begin = (current: Build, vertex: number, text: string) => {
    began.set(vertex, Date.now());
    appendLine(current.id, vertex, `#${vertex} ${text}`);
  };
  const done = (current: Build, vertex: number) => {
    const seconds = (Date.now() - (began.get(vertex) ?? Date.now())) / 1_000;
    appendLine(current.id, vertex, `#${vertex} DONE ${seconds.toFixed(1)}s`);
  };

  cue(1_000, (current) => {
    current.status = 'building';
    current.commit = commit;
    begin(current, 3, '[1/4] FROM docker.io/library/node:22-alpine');
    appendLine(current.id, 3, '#3 CACHED');
  });
  cue(1_020, (current) => {
    begin(current, 4, '[2/4] COPY package.json package-lock.json ./');
  });
  cue(1_050, (current) => {
    done(current, 4);
  });

  if (shouldFail) {
    cue(2_000, (current) => {
      appendLine(current.id, 5, '#5 [3/4] RUN ls /does-not-exist');
    });
    cue(2_200, (current) => {
      appendLine(
        current.id,
        5,
        '#5 0.221 ls: /does-not-exist: No such file or directory',
        'stderr',
      );
      appendLine(
        current.id,
        5,
        '#5 ERROR: process "/bin/sh -c ls /does-not-exist" did not complete successfully: exit code: 2',
        'stderr',
      );
    });
    cue(2_400, (current) => {
      const step = failedStep();
      current.status = 'failed';
      current.failed_phase = 'building';
      current.error = step.error;
      current.failed_step = step;
      current.finished_at = now();
      appendLine(current.id, null, 'Build failed; nothing was deployed', 'stderr');
    });
    return build;
  }

  cue(1_080, (current) => {
    begin(current, 5, '[3/4] RUN npm ci');
  });
  cue(2_200, (current) => {
    appendLine(current.id, 5, '#5 11.92 added 312 packages, and audited 313 packages in 11s');
  });
  cue(2_750, (current) => {
    done(current, 5);
    begin(current, 6, '[4/4] RUN npm run build');
  });
  cue(3_350, (current) => {
    done(current, 6);
    current.status = 'exporting';
    appendLine(current.id, null, '#7 exporting to oci image format');
  });
  cue(3_550, (current) => {
    current.status = 'importing';
    appendLine(current.id, null, '#7 writing image to the local store');
  });
  cue(4_000, (current) => {
    const image = builtImage(current.app_id, current.id);
    current.status = 'succeeded';
    current.image = image;
    current.image_digest = `sha256:${hex(64)}`;
    current.finished_at = now();
    appendLine(current.id, null, '#7 DONE 0.4s');
    appendLine(current.id, null, 'Build succeeded; deploying the built image');

    const appNow = findApp(current.app_id);
    if (appNow) {
      appNow.image = image;
      appNow.updated_at = now();
      const deployment = runDeployment(appNow.id, image, 'build');
      current.deployment_id = deployment.id;
    }
  });

  return build;
};

/** Stop a live build; the node confirms with `cancelled`. */
export const cancelMockBuild = (build: Build): void => {
  build.status = 'cancelled';
  build.finished_at = now();
  appendLine(build.id, null, 'Build cancelled by the operator', 'stderr');
};

/** The log lines read by the panel, oldest first. */
export const buildLogLines = (buildId: string): BuildLogLine[] =>
  db.buildLogs[buildId] ?? [];
