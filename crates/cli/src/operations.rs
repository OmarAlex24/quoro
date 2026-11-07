//! The API operations the client calls by name.
//!
//! A command that calls a fixed operation (anything but `quoro api`, whose
//! method and path come from the user) takes its method and path template
//! from a constant here and lists it in [`OPERATIONS`]. The contract test
//! (`tests/contract.rs`) checks every entry against
//! `docs/contracts/openapi.yaml`, so a call the contract does not define, or
//! one whose method or path drifted, fails CI instead of failing on a primary.
//!
//! The destructive operations at the end are not called by name: they are
//! the ones whose approval params the client can reproduce (`approval.rs`),
//! and registering them checks their templates against the contract.

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
    DELETE_APP,
    DELETE_VM,
    DELETE_VOLUME,
    RESTORE_BACKUP,
];
