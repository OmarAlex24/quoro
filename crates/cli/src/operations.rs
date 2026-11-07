//! The API operations the client calls by name.
//!
//! A command that calls a fixed operation (anything but `quoro api`, whose
//! method and path come from the user) takes its method and path template
//! from a constant here and lists it in [`OPERATIONS`]. The contract test
//! (`tests/contract.rs`) checks every entry against
//! `docs/contracts/openapi.yaml`, so a call the contract does not define, or
//! one whose method or path drifted, fails CI instead of failing on a primary.
//!
//! The destructive operations at the end are the ones whose approval params
//! the client can reproduce (`approval.rs`); registering them checks their
//! templates against the contract. `deleteApp`, `deleteVolume` and
//! `restoreBackup` are also called by name (`quoro app delete`, `quoro volume
//! delete`, `quoro backup restore`); `deleteVm` is reached with `quoro api`.

/// One operation of `openapi.yaml`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Operation {
    /// Its `operationId`.
    pub id: &'static str,
    /// The HTTP method, upper case.
    pub method: &'static str,
    /// The path template, with `{name}` parameters as the contract writes them.
    pub path: &'static str,
}

/// `getHealth`: whether the node that answers is up; needs no credential.
pub const GET_HEALTH: Operation = Operation {
    id: "getHealth",
    method: "GET",
    path: "/api/v1/health",
};

/// `getMe`: the identity behind the credential.
pub const GET_ME: Operation = Operation {
    id: "getMe",
    method: "GET",
    path: "/api/v1/auth/me",
};

/// `createAgentToken`: an access token from an agent key.
pub const CREATE_AGENT_TOKEN: Operation = Operation {
    id: "createAgentToken",
    method: "POST",
    path: "/api/v1/auth/agent-token",
};

/// `createApproval`: file a just-in-time approval request.
pub const CREATE_APPROVAL: Operation = Operation {
    id: "createApproval",
    method: "POST",
    path: "/api/v1/approvals",
};

/// `getApproval`: one approval request and its status.
pub const GET_APPROVAL: Operation = Operation {
    id: "getApproval",
    method: "GET",
    path: "/api/v1/approvals/{approval_id}",
};

/// `streamEvents`: server-sent events, here the decision on an approval.
pub const STREAM_EVENTS: Operation = Operation {
    id: "streamEvents",
    method: "GET",
    path: "/api/v1/events/stream",
};

/// `listProjects` (`project:read`).
pub const LIST_PROJECTS: Operation = Operation {
    id: "listProjects",
    method: "GET",
    path: "/api/v1/projects",
};

/// `createProject` (`project:write`).
pub const CREATE_PROJECT: Operation = Operation {
    id: "createProject",
    method: "POST",
    path: "/api/v1/projects",
};

/// `getProject` (`project:read`).
pub const GET_PROJECT: Operation = Operation {
    id: "getProject",
    method: "GET",
    path: "/api/v1/projects/{project_id}",
};

/// `deleteProject` (`project:delete`). In the contract, not served by the
/// primary yet: `quoro project delete` says so on its 405.
pub const DELETE_PROJECT: Operation = Operation {
    id: "deleteProject",
    method: "DELETE",
    path: "/api/v1/projects/{project_id}",
};

/// `listApps` (`app:read`).
pub const LIST_APPS: Operation = Operation {
    id: "listApps",
    method: "GET",
    path: "/api/v1/apps",
};

/// `createApp` (`app:create`).
pub const CREATE_APP: Operation = Operation {
    id: "createApp",
    method: "POST",
    path: "/api/v1/apps",
};

/// `getApp` (`app:read`): the app with its instances and domains.
pub const GET_APP: Operation = Operation {
    id: "getApp",
    method: "GET",
    path: "/api/v1/apps/{app_id}",
};

/// `updateApp` (`app:update`).
pub const UPDATE_APP: Operation = Operation {
    id: "updateApp",
    method: "PATCH",
    path: "/api/v1/apps/{app_id}",
};

/// `startApp` (`app:start`).
pub const START_APP: Operation = Operation {
    id: "startApp",
    method: "POST",
    path: "/api/v1/apps/{app_id}/start",
};

/// `stopApp` (`app:stop`).
pub const STOP_APP: Operation = Operation {
    id: "stopApp",
    method: "POST",
    path: "/api/v1/apps/{app_id}/stop",
};

/// `listAppDeployments` (`app:read`), newest first.
pub const LIST_APP_DEPLOYMENTS: Operation = Operation {
    id: "listAppDeployments",
    method: "GET",
    path: "/api/v1/apps/{app_id}/deployments",
};

/// `restartApp` (`app:restart`).
pub const RESTART_APP: Operation = Operation {
    id: "restartApp",
    method: "POST",
    path: "/api/v1/apps/{app_id}/restart",
};

/// `redeployApp` (`app:redeploy`): the current image again.
pub const REDEPLOY_APP: Operation = Operation {
    id: "redeployApp",
    method: "POST",
    path: "/api/v1/apps/{app_id}/redeploy",
};

/// `createDeployment` (`app:deploy`): a new image; `conflict` for a Git app.
pub const CREATE_DEPLOYMENT: Operation = Operation {
    id: "createDeployment",
    method: "POST",
    path: "/api/v1/apps/{app_id}/deployments",
};

/// `getDeployment` (`app:read`).
pub const GET_DEPLOYMENT: Operation = Operation {
    id: "getDeployment",
    method: "GET",
    path: "/api/v1/deployments/{deployment_id}",
};

/// `getDeploymentLogs` (`app:logs`): progress lines, read from the node.
pub const GET_DEPLOYMENT_LOGS: Operation = Operation {
    id: "getDeploymentLogs",
    method: "GET",
    path: "/api/v1/deployments/{deployment_id}/logs",
};

/// `getAppLogs` (`app:logs`), read from the nodes.
pub const GET_APP_LOGS: Operation = Operation {
    id: "getAppLogs",
    method: "GET",
    path: "/api/v1/apps/{app_id}/logs",
};

/// `streamAppLogs` (`app:logs`): server-sent events of live output.
pub const STREAM_APP_LOGS: Operation = Operation {
    id: "streamAppLogs",
    method: "GET",
    path: "/api/v1/apps/{app_id}/logs/stream",
};

/// `listAppBuilds` (`app:read`), newest first.
pub const LIST_APP_BUILDS: Operation = Operation {
    id: "listAppBuilds",
    method: "GET",
    path: "/api/v1/apps/{app_id}/builds",
};

/// `startBuild` (`app:deploy`): a build of the app's Git source.
pub const START_BUILD: Operation = Operation {
    id: "startBuild",
    method: "POST",
    path: "/api/v1/apps/{app_id}/builds",
};

/// `getBuild` (`app:read`).
pub const GET_BUILD: Operation = Operation {
    id: "getBuild",
    method: "GET",
    path: "/api/v1/builds/{build_id}",
};

/// `cancelBuild` (`app:deploy`).
pub const CANCEL_BUILD: Operation = Operation {
    id: "cancelBuild",
    method: "POST",
    path: "/api/v1/builds/{build_id}/cancel",
};

/// `getBuildLogs` (`app:logs`): one page of the log, oldest first.
pub const GET_BUILD_LOGS: Operation = Operation {
    id: "getBuildLogs",
    method: "GET",
    path: "/api/v1/builds/{build_id}/logs",
};

/// `streamBuildLogs` (`app:logs`): server-sent events of the log and status.
pub const STREAM_BUILD_LOGS: Operation = Operation {
    id: "streamBuildLogs",
    method: "GET",
    path: "/api/v1/builds/{build_id}/logs/stream",
};

/// `getAppMetrics` (`app:read`), read from the nodes.
pub const GET_APP_METRICS: Operation = Operation {
    id: "getAppMetrics",
    method: "GET",
    path: "/api/v1/apps/{app_id}/metrics",
};

/// `getAppVulnerabilities` (`app:read`).
pub const GET_APP_VULNERABILITIES: Operation = Operation {
    id: "getAppVulnerabilities",
    method: "GET",
    path: "/api/v1/apps/{app_id}/vulnerabilities",
};

/// `getCluster` (`cluster:read`).
pub const GET_CLUSTER: Operation = Operation {
    id: "getCluster",
    method: "GET",
    path: "/api/v1/cluster",
};

/// `listNodes` (`node:read`).
pub const LIST_NODES: Operation = Operation {
    id: "listNodes",
    method: "GET",
    path: "/api/v1/nodes",
};

/// `getAppEnv` (`app:read`): names and change times, never a value.
pub const GET_APP_ENV: Operation = Operation {
    id: "getAppEnv",
    method: "GET",
    path: "/api/v1/apps/{app_id}/env",
};

/// `replaceAppEnv` (`app:update`): the whole environment at once.
pub const REPLACE_APP_ENV: Operation = Operation {
    id: "replaceAppEnv",
    method: "PUT",
    path: "/api/v1/apps/{app_id}/env",
};

/// `listAppDomains` (`app:read`).
pub const LIST_APP_DOMAINS: Operation = Operation {
    id: "listAppDomains",
    method: "GET",
    path: "/api/v1/apps/{app_id}/domains",
};

/// `createAppDomain` (`app:update`).
pub const CREATE_APP_DOMAIN: Operation = Operation {
    id: "createAppDomain",
    method: "POST",
    path: "/api/v1/apps/{app_id}/domains",
};

/// `deleteDomain` (`app:update`), by the domain's id.
pub const DELETE_DOMAIN: Operation = Operation {
    id: "deleteDomain",
    method: "DELETE",
    path: "/api/v1/domains/{domain_id}",
};

/// `listProviders` (`provider:manage`, human-only): a provider named by name.
pub const LIST_PROVIDERS: Operation = Operation {
    id: "listProviders",
    method: "GET",
    path: "/api/v1/providers",
};

/// `listAppVolumes` (`app:read`): every volume of the app, not paged.
pub const LIST_APP_VOLUMES: Operation = Operation {
    id: "listAppVolumes",
    method: "GET",
    path: "/api/v1/apps/{app_id}/volumes",
};

/// `createVolume` (`app:update`).
pub const CREATE_VOLUME: Operation = Operation {
    id: "createVolume",
    method: "POST",
    path: "/api/v1/apps/{app_id}/volumes",
};

/// `getVolume` (`app:read`).
pub const GET_VOLUME: Operation = Operation {
    id: "getVolume",
    method: "GET",
    path: "/api/v1/volumes/{volume_id}",
};

/// `listAppBackups` (`backup:read`), newest first.
pub const LIST_APP_BACKUPS: Operation = Operation {
    id: "listAppBackups",
    method: "GET",
    path: "/api/v1/apps/{app_id}/backups",
};

/// `createBackup` (`backup:write`): a backup of the app's volume now.
pub const CREATE_BACKUP: Operation = Operation {
    id: "createBackup",
    method: "POST",
    path: "/api/v1/apps/{app_id}/backups",
};

/// `getBackup` (`backup:read`).
pub const GET_BACKUP: Operation = Operation {
    id: "getBackup",
    method: "GET",
    path: "/api/v1/backups/{backup_id}",
};

/// `deleteApp` (`app:delete`).
pub const DELETE_APP: Operation = Operation {
    id: "deleteApp",
    method: "DELETE",
    path: "/api/v1/apps/{app_id}",
};

/// `deleteVm` (`vm:delete`).
pub const DELETE_VM: Operation = Operation {
    id: "deleteVm",
    method: "DELETE",
    path: "/api/v1/vms/{vm_id}",
};

/// `deleteVolume` (`app:delete`).
pub const DELETE_VOLUME: Operation = Operation {
    id: "deleteVolume",
    method: "DELETE",
    path: "/api/v1/volumes/{volume_id}",
};

/// `restoreBackup` (`backup:restore`).
pub const RESTORE_BACKUP: Operation = Operation {
    id: "restoreBackup",
    method: "POST",
    path: "/api/v1/backups/{backup_id}/restore",
};

/// Every operation the client calls by name.
pub const OPERATIONS: &[Operation] = &[
    GET_HEALTH,
    GET_ME,
    CREATE_AGENT_TOKEN,
    CREATE_APPROVAL,
    GET_APPROVAL,
    STREAM_EVENTS,
    LIST_PROJECTS,
    CREATE_PROJECT,
    GET_PROJECT,
    DELETE_PROJECT,
    LIST_APPS,
    CREATE_APP,
    GET_APP,
    UPDATE_APP,
    START_APP,
    STOP_APP,
    LIST_APP_DEPLOYMENTS,
    RESTART_APP,
    REDEPLOY_APP,
    CREATE_DEPLOYMENT,
    GET_DEPLOYMENT,
    GET_DEPLOYMENT_LOGS,
    GET_APP_LOGS,
    STREAM_APP_LOGS,
    LIST_APP_BUILDS,
    START_BUILD,
    GET_BUILD,
    CANCEL_BUILD,
    GET_BUILD_LOGS,
    STREAM_BUILD_LOGS,
    GET_APP_METRICS,
    GET_APP_VULNERABILITIES,
    GET_CLUSTER,
    LIST_NODES,
    GET_APP_ENV,
    REPLACE_APP_ENV,
    LIST_APP_DOMAINS,
    CREATE_APP_DOMAIN,
    DELETE_DOMAIN,
    LIST_PROVIDERS,
    LIST_APP_VOLUMES,
    CREATE_VOLUME,
    GET_VOLUME,
    LIST_APP_BACKUPS,
    CREATE_BACKUP,
    GET_BACKUP,
    DELETE_APP,
    DELETE_VM,
    DELETE_VOLUME,
    RESTORE_BACKUP,
];
