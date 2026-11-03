//! The agent half of the decision procedure (permission-model.md section 7.2,
//! step 3). [`crate::request::CallCtx`] is the only caller: it handles the
//! human branch, audits every refusal and records the authority an allowed
//! call ran under.

use crate::auth::AgentIdentity;
use crate::error::{ApiError, Code, Reason, Target};
use quoro_types::Scope;

/// The `approval_required` problem: it carries the destructive scope and the
/// target so the agent can file an approval request without guessing
/// (permission-model.md section 7.4).
#[must_use]
pub fn approval_required(scope: Scope, target: Option<&Target>) -> ApiError {
    let mut error = ApiError::new(
        Code::ApprovalRequired,
        format!(
            "{} is destructive: file an approval request and repeat the call once a human approves it",
            scope.as_str()
        ),
    );
    error.scope = Some(scope.as_str().to_string());
    error.target = target.cloned();
    error
}

/// The refusal for an agent that holds no grant for `scope`, in the order of
/// section 7.4: `human_only`, `agent_disabled`, then `approval_required` for
/// a destructive scope and `scope_missing` otherwise (step d).
#[must_use]
pub fn refuse_agent(agent: &AgentIdentity, scope: Scope, target: Option<&Target>) -> ApiError {
    if scope.is_human_only() {
        return ApiError::forbidden(Reason::HumanOnly, Some(scope.as_str()));
    }
    if agent.disabled {
        return ApiError::forbidden(Reason::AgentDisabled, Some(scope.as_str()));
    }
    if scope.is_destructive() {
        return approval_required(scope, target);
    }
    ApiError::forbidden(Reason::ScopeMissing, Some(scope.as_str()))
}
