//! The API operations the client calls by name.
//!
//! A command that calls a fixed operation (anything but `quoro api`, whose
//! method and path come from the user) takes its method and path template
//! from a constant here and lists it in [`OPERATIONS`]. The contract test
//! (`tests/contract.rs`) checks every entry against
//! `docs/contracts/openapi.yaml`, so a call the contract does not define, or
//! one whose method or path drifted, fails CI instead of failing on a primary.
//!
//! The first entries are the two operations the end-to-end tests call through
//! `quoro api`; the commands that call fixed operations add theirs.

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

/// Every operation the client calls by name.
pub const OPERATIONS: &[Operation] = &[GET_HEALTH, GET_ME];
