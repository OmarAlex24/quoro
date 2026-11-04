//! The service layer: one function per operation, called by every channel
//! (D-0031). The HTTP handlers in [`crate::api`] are thin adapters that build
//! a [`crate::request::CallCtx`] from the request and call these functions;
//! the MCP adapter builds the same context with `Channel::Mcp`, the tool name
//! as its operation and the tool's `justification` and `incident_id`
//! arguments, and calls the very same functions.
//!
//! Every function here authorizes through the context (`authorize` or
//! `authorize_read`), writes its state change and its audit record in one
//! transaction, and audits failures and agent reads. An adapter never touches
//! the database and never builds an audit record.
//!
//! Each function builds the audit `params` from its own arguments, so a call
//! has the same `params` (and the same `params_hash` for approvals) whichever
//! channel it came through.

pub mod agents;
pub mod approvals;
pub mod apps;
pub mod audit;
pub mod builds;
pub mod cluster;
pub mod deployments;
pub mod events;
pub mod grants;
pub mod incidents;
pub mod interactive;
pub mod logs;
pub mod nodes;
pub mod projects;
pub mod users;
pub mod volumes;
pub mod webhooks;
