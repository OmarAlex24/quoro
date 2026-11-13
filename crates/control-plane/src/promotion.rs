// SPDX-License-Identifier: AGPL-3.0-only
//! The write that moves the primary status in `quoro cluster promote`
//! (`node-agent-protocol.md` section 7.3 step 5; ADR 0182, ADR 0236,
//! ADR 0261), and the token-cap check of `--ack-fence` (section 7.3, section
//! 8.6; ADR 0325; issue #931).
//!
//! - [`check_token_cap`] reads the rows the fold of the followed log gives
//!   ([`crate::rebuild::fold`]) and names every lease that the `--ack-fence`
//!   write's declaration would raise past the cap. The command runs it before
//!   any real vote request, and so before the freeze, so a refusal takes no
//!   term and freezes nothing (section 7.3, "`--ack-fence`"). It folds
//!   nothing itself.
//! - [`write`] runs on the new primary's database once it is built at term
//!   `T` (step 4). In one change-log write, so every entry carries `T`, it
//!   clears `cp_status` on the old primary's node, sets `cp_status =
//!   'primary'` on the promoted node and records `cluster.promoted`
//!   (`audit-log-format.md` section 3.9). With `--ack-fence` the same write
//!   declares the old primary's node lost, as `declareNodeLost` does
//!   (section 8.3, [`crate::api::nodes::declare_lost_write`]); with
//!   `--accept-loss` it marks every `expiring` lease row unsettled (section
//!   8.4, "The mark", [`crate::leases::mark_expiring_rows`]). One statement
//!   serves both flags.
//!
//! Nothing calls either yet but the tests: the command itself, its votes and
//! freeze, the build of the database file and the start of the new primary
//! are later layers of issue #19.

use crate::audit::{Channel, Outcome, Record};
use crate::changelog::{self, Actor, Op};
use crate::error::ApiError;
use crate::leases::MAX_TOKEN;
use crate::rebuild::{Cell, Folded, FoldedRow};
use crate::store::{Store, TermError, read_term};
use quoro_types::{Prefix, new_id};
use serde_json::{Value, json};
use sqlx::SqliteConnection;

/// The `operation` of the audit record (`audit-log-format.md` section 3.9).
pub const OPERATION: &str = "cluster.promoted";

/// The system actor of the record and of every entry of the write
/// (`audit-log-format.md` section 3.9: `actor_id` `cluster`).
const ACTOR: &str = "cluster";

/// The `token_exhausted` refusal of `--ack-fence` (section 7.3, section 8.6,
/// ADR 0325): a lease that declaring the old primary's node lost would raise
/// is already at the token cap, 2^53 − 1. Its text is the one every path
/// that raises a token gives at the cap ([`crate::leases::token_exhausted`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenExhausted {
    /// Each lease at the cap, by lease id, with its token, in id order.
    pub leases: Vec<ExhaustedLease>,
}

/// A lease at the token cap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExhaustedLease {
    pub lease_id: String,
    pub fencing_token: i64,
}

impl TokenExhausted {
    /// The refusal's reason, as the API's `409 conflict` names it.
    pub const REASON: &'static str = "token_exhausted";
}

impl std::fmt::Display for TokenExhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let named: Vec<(&str, i64)> = self
            .leases
            .iter()
            .map(|lease| (lease.lease_id.as_str(), lease.fencing_token))
            .collect();
        write!(
            f,
            "{}: {}",
            Self::REASON,
            crate::leases::token_exhausted_detail(&named)
        )
    }
}

impl std::error::Error for TokenExhausted {}

/// One lease row as the cap check reads it from the fold.
struct CapRow<'a> {
    id: &'a str,
    holder_node_id: Option<&'a str>,
    /// `None` for a cell that holds no integer.
    fencing_token: Option<i64>,
    revoked: bool,
}

/// The cap check itself, over every lease row: when the node is not
/// declared lost yet, the declaration raises every row that names it as
/// holder, a revoked one included, but a row already revoked at the cap,
/// which it leaves as it is, as `removeNode` does (`removal.rs`)
/// ([`crate::api::nodes::declare_lost_write`], then
/// [`crate::leases::bump_tokens`]). Only an unrevoked row among them can
/// refuse: a revoked row it raises is below the cap and reaches at most the
/// cap. A node already declared lost has nothing raised: the declaration
/// changes nothing then (ADR 0202).
fn exhausted<'a>(
    node_id: &str,
    node_lost: bool,
    rows: impl IntoIterator<Item = CapRow<'a>>,
) -> Result<(), TokenExhausted> {
    if node_lost {
        return Ok(());
    }
    let mut leases: Vec<ExhaustedLease> = rows
        .into_iter()
        .filter(|row| row.holder_node_id == Some(node_id) && !row.revoked)
        .filter_map(|row| {
            row.fencing_token
                .filter(|token| *token >= MAX_TOKEN)
                .map(|token| ExhaustedLease {
                    lease_id: row.id.to_string(),
                    fencing_token: token,
                })
        })
        .collect();
    if leases.is_empty() {
        return Ok(());
    }
    leases.sort_by(|a, b| a.lease_id.cmp(&b.lease_id));
    Err(TokenExhausted { leases })
}

/// The cell `column` of `row` when it holds a value, `None` for a column the
/// row lacks or that takes its default.
fn value<'a>(row: &'a FoldedRow, column: &str) -> Option<&'a Value> {
    match row.cells.get(column) {
        Some(Cell::Value(value)) => Some(value),
        _ => None,
    }
}

/// Whether the nullable column `column` of `row` is set. A column that takes
/// its default is not: `leases.revoked_at` and `nodes.lost_at` default to
/// NULL.
fn is_set(row: &FoldedRow, column: &str) -> bool {
    value(row, column).is_some_and(|value| !value.is_null())
}

/// The integer in `column` of `row`. A value above `i64` reads as
/// `i64::MAX` and a float as its integer part, so a corrupted token above the
/// cap is still named; anything else reads as `None`.
fn integer(row: &FoldedRow, column: &str) -> Option<i64> {
    let Some(Value::Number(number)) = value(row, column) else {
        return None;
    };
    number
        .as_i64()
        .or_else(|| number.as_u64().map(|_| i64::MAX))
        .or_else(|| {
            number
                .as_f64()
                .filter(|float| float.is_finite())
                // A float's integer part, saturating at the bounds of `i64`.
                .map(|float| float as i64)
        })
}

/// The node that holds the primary status in the fold, other than
/// `to_node_id`: the first such node by id, the one [`write`] clears and,
/// with `--ack-fence`, declares lost. `None` when no other node holds it.
#[must_use]
pub fn old_primary<'a>(folded: &'a Folded, to_node_id: &str) -> Option<&'a str> {
    folded
        .tables
        .get("nodes")?
        .iter()
        .find(|(id, row)| {
            id.as_str() != to_node_id
                && value(row, "cp_status").and_then(Value::as_str) == Some("primary")
        })
        .map(|(id, _)| id.as_str())
}

/// The token-cap check of `quoro cluster promote --ack-fence` (section 7.3,
/// "`--ack-fence`"; section 8.6, "The cap"; ADR 0325; issue #931), on the
/// rows the fold of the followed log gives: every lease that the write's
/// declaration of `old_primary_node_id` as lost would raise, and whose token
/// is already at 2^53 − 1 or above, is named in a [`TokenExhausted`].
///
/// [`write`] raises every row of the `leases` table that names the node as
/// holder, a revoked one included, and none when the `nodes` row of the node
/// already carries `lost_at` ([`crate::api::nodes::declare_lost_write`]).
/// Only an unrevoked one among them can be at the cap: a revoked row below
/// the cap reaches at most the cap, and a row revoked at the cap is left as
/// it is and refuses nothing.
///
/// The command calls it before any real vote request, and so before the
/// freeze: a refusal then takes no term, freezes nothing and writes nothing.
///
/// # Errors
///
/// [`TokenExhausted`] naming each such lease and its token.
pub fn check_token_cap(folded: &Folded, old_primary_node_id: &str) -> Result<(), TokenExhausted> {
    let node_lost = folded
        .tables
        .get("nodes")
        .and_then(|nodes| nodes.get(old_primary_node_id))
        .is_some_and(|row| is_set(row, "lost_at"));
    let rows = folded
        .tables
        .get("leases")
        .into_iter()
        .flatten()
        .map(|(id, row)| CapRow {
            id: id.as_str(),
            holder_node_id: value(row, "holder_node_id").and_then(Value::as_str),
            fencing_token: integer(row, "fencing_token"),
            revoked: is_set(row, "revoked_at"),
        });
    exhausted(old_primary_node_id, node_lost, rows)
}

/// What the command passes to [`write`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Promotion {
    /// The term `T` the command took (step 2). The new database's
    /// `meta.term` already holds it (step 4).
    pub term: i64,
    /// This machine's node, the one promoted.
    pub to_node_id: String,
    /// The last followed entry the database was rebuilt from
    /// (`params.log_seq`).
    pub log_seq: i64,
    /// `--ack-fence`: the write declares the old primary's node lost.
    pub ack_fence: bool,
    /// `--accept-loss`: the write marks every `expiring` lease row.
    pub accept_loss: bool,
    /// `--statement`, which each flag requires; one serves both. The record
    /// keeps it as its `justification` when a flag is given, and only then
    /// (`audit-log-format.md` section 3.9).
    pub statement: Option<String>,
}

/// What [`write`] wrote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Written {
    /// The first and the last `seq` of the write's change-log entries: one
    /// contiguous range, every entry at term `T`, the record's own entry
    /// included.
    pub first_seq: i64,
    pub last_seq: i64,
    /// The `seq` of the `cluster.promoted` record in the audit log.
    pub audit_seq: i64,
    /// The node whose primary status was cleared (`params.from_node_id`),
    /// `None` when no other node held it.
    pub from_node_id: Option<String>,
}

/// Why [`write`] wrote nothing.
#[derive(Debug)]
pub enum PromotionError {
    /// `meta.term` does not hold the term the command passed: `found` is the
    /// term it holds, or its stored text when that is no term.
    Term { expected: i64, found: String },
    /// `--ack-fence` or `--accept-loss` without a statement (section 7.3).
    NoStatement,
    /// The promoted node has no row in the database, or a `removed` one.
    Node { node_id: String, why: &'static str },
    /// A lease the old primary's node holds is at the token cap
    /// ([`check_token_cap`]).
    TokenExhausted(TokenExhausted),
    /// A database error, or another failure of the write.
    Failed(String),
}

impl std::fmt::Display for PromotionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Term { expected, found } => write!(
                f,
                "the database is not at the promotion's term: meta.term is {found}, not {expected}"
            ),
            Self::NoStatement => f.write_str("--ack-fence and --accept-loss require --statement"),
            Self::Node { node_id, why } => write!(f, "cannot promote node {node_id}: {why}"),
            Self::TokenExhausted(refusal) => write!(f, "{refusal}"),
            Self::Failed(why) => write!(f, "the promotion write failed: {why}"),
        }
    }
}

impl std::error::Error for PromotionError {}

impl From<sqlx::Error> for PromotionError {
    fn from(error: sqlx::Error) -> Self {
        Self::Failed(error.to_string())
    }
}

impl From<changelog::AppendError> for PromotionError {
    fn from(error: changelog::AppendError) -> Self {
        Self::Failed(error.to_string())
    }
}

impl From<ApiError> for PromotionError {
    fn from(error: ApiError) -> Self {
        let code = error.audit_detail();
        Self::Failed(format!("{code}: {}", error.detail.unwrap_or_default()))
    }
}

/// Step 5 of section 7.3, on the new primary's database `store`, whose
/// `meta.term` already holds `promotion.term` (`T`). In one transaction, so
/// in one change-log write whose entries all carry `T`:
///
/// 1. clears `cp_status` on the old primary's node, with its node entry, as
///    [`crate::primary_status`] moves the status. The old primary is the node
///    that holds the status other than the promoted one ([`old_primary`]);
///    should more than one hold it, each is cleared and the first by id is
///    the one `params.from_node_id` names and `--ack-fence` declares lost;
/// 2. sets `cp_status = 'primary'` on the promoted node, with its node entry;
/// 3. with `--ack-fence`, declares the old primary's node lost, as
///    `declareNodeLost` does (section 8.3,
///    [`crate::api::nodes::declare_lost_write`]), after the cap check of
///    [`check_token_cap`] on the same rows. That call's own refusals, of a
///    connected or `removed` node, are not the promotion's: the contract
///    gives this write none. Nor does it write a `declareNodeLost` record:
///    `cluster.promoted` with `ack_fence: true` and `from_node_id` is its
///    record (`audit-log-format.md` sections 3.7 and 3.9, ADR 0182);
/// 4. with `--accept-loss`, marks every `expiring` lease row unsettled
///    ([`crate::leases::mark_expiring_rows`]);
/// 5. records `cluster.promoted` (`audit-log-format.md` section 3.9): actor
///    `system` / `cluster`, channel `cli`, target the promoted node, `params`
///    `{from_node_id, to_node_id, term, log_seq, ack_fence, accept_loss}`,
///    the statement as `justification` when a flag is given, and as
///    `change_seq` the entry of step 2. It is the first record of the term.
///
/// A refusal or an error drops the transaction, so nothing is written.
///
/// # Errors
///
/// [`PromotionError::NoStatement`] before anything is read;
/// [`PromotionError::Term`] when `meta.term` is not `T`;
/// [`PromotionError::Node`] for a promoted node with no row or a `removed`
/// one; [`PromotionError::TokenExhausted`] with `--ack-fence` at the cap; or
/// [`PromotionError::Failed`].
pub async fn write(store: &Store, promotion: &Promotion) -> Result<Written, PromotionError> {
    let flagged = promotion.ack_fence || promotion.accept_loss;
    let statement = promotion
        .statement
        .as_deref()
        .filter(|statement| !statement.trim().is_empty());
    if flagged && statement.is_none() {
        return Err(PromotionError::NoStatement);
    }
    let mut w = store.begin_write().await?;
    match read_term(&mut *w.tx).await {
        Ok(term) if term == promotion.term => {}
        Ok(term) => {
            return Err(PromotionError::Term {
                expected: promotion.term,
                found: term.to_string(),
            });
        }
        Err(TermError::Bad(value)) => {
            return Err(PromotionError::Term {
                expected: promotion.term,
                found: format!("{value:?}"),
            });
        }
        Err(TermError::Database(error)) => return Err(error.into()),
    }
    let to = promotion.to_node_id.as_str();
    let status: Option<String> = sqlx::query_scalar("SELECT status FROM nodes WHERE id = ?")
        .bind(to)
        .fetch_optional(&mut *w.tx)
        .await?;
    match status.as_deref() {
        None => {
            return Err(PromotionError::Node {
                node_id: to.to_string(),
                why: "the database holds no such node",
            });
        }
        Some("removed") => {
            return Err(PromotionError::Node {
                node_id: to.to_string(),
                why: "the node is removed",
            });
        }
        Some(_) => {}
    }
    let holders: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM nodes WHERE cp_status = 'primary' AND id != ? ORDER BY id",
    )
    .bind(to)
    .fetch_all(&mut *w.tx)
    .await?;
    let from = holders.first().cloned();
    if promotion.ack_fence
        && let Some(from) = &from
    {
        check_cap_in(&mut w.tx, from)
            .await?
            .map_err(PromotionError::TokenExhausted)?;
    }

    let head: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(seq), 0) FROM change_log")
        .fetch_one(&mut *w.tx)
        .await?;
    let actor = Actor::system(ACTOR);
    for holder in &holders {
        set_cp_status(&mut w.tx, &actor, holder, None).await?;
    }
    let moved = set_cp_status(&mut w.tx, &actor, to, Some("primary")).await?;
    if promotion.ack_fence
        && let Some(from) = &from
    {
        crate::api::nodes::declare_lost_write(&mut w.tx, from, &actor).await?;
    }
    if promotion.accept_loss {
        crate::leases::mark_expiring_rows(&mut w.tx, &actor).await?;
    }
    let record = Record {
        actor,
        agent_model: None,
        on_behalf_of: None,
        channel: Channel::Cli,
        operation: OPERATION.to_string(),
        scope: None,
        target_type: Some("node".to_string()),
        target_id: Some(to.to_string()),
        project_id: None,
        params: json!({
            "from_node_id": from.as_deref().map_or(Value::Null, Value::from),
            "to_node_id": to,
            "term": promotion.term,
            "log_seq": promotion.log_seq,
            "ack_fence": promotion.ack_fence,
            "accept_loss": promotion.accept_loss,
        }),
        justification: statement.filter(|_| flagged).map(str::to_string),
        trigger_event_id: None,
        incident_id: None,
        grant_id: None,
        approval_id: None,
        request_id: new_id(Prefix::Request),
        result: Outcome::Ok,
        result_detail: None,
        change_seq: Some(moved),
    };
    let audit_seq = crate::audit::append(&mut w.tx, &record)
        .await
        .map_err(|error| PromotionError::Failed(format!("cannot record {OPERATION}: {error}")))?;
    let last_seq: i64 = sqlx::query_scalar("SELECT MAX(seq) FROM change_log")
        .fetch_one(&mut *w.tx)
        .await?;
    w.commit().await?;
    Ok(Written {
        first_seq: head + 1,
        last_seq,
        audit_seq,
        from_node_id: from,
    })
}

/// [`check_token_cap`]'s rule on the rows of the database, inside the
/// write: nothing when the node already carries `lost_at`, otherwise the
/// unrevoked leases the node holds ([`crate::leases::held_by_node`]) at the
/// cap or above. The declaration raises the node's revoked rows too
/// ([`crate::api::nodes::declare_lost_write`]), but none of them refuses:
/// one below the cap reaches at most the cap, and one at the cap is left as
/// it is.
async fn check_cap_in(
    conn: &mut SqliteConnection,
    node_id: &str,
) -> Result<Result<(), TokenExhausted>, PromotionError> {
    let lost_at: Option<Option<String>> =
        sqlx::query_scalar("SELECT lost_at FROM nodes WHERE id = ?")
            .bind(node_id)
            .fetch_optional(&mut *conn)
            .await?;
    if lost_at.flatten().is_some() {
        return Ok(Ok(()));
    }
    // `held_by_node` gives the rows in id order.
    let leases: Vec<ExhaustedLease> = crate::leases::held_by_node(conn, node_id)
        .await?
        .into_iter()
        .filter(|lease| lease.fencing_token >= MAX_TOKEN)
        .map(|lease| ExhaustedLease {
            lease_id: lease.id,
            fencing_token: lease.fencing_token,
        })
        .collect();
    if leases.is_empty() {
        return Ok(Ok(()));
    }
    Ok(Err(TokenExhausted { leases }))
}

/// Writes `cp_status` of `node_id` and its node entry, the whole row, as
/// every node write does; returns the entry's `seq`.
async fn set_cp_status(
    conn: &mut SqliteConnection,
    actor: &Actor,
    node_id: &str,
    cp_status: Option<&str>,
) -> Result<i64, PromotionError> {
    sqlx::query("UPDATE nodes SET cp_status = ? WHERE id = ?")
        .bind(cp_status)
        .bind(node_id)
        .execute(&mut *conn)
        .await?;
    let payload = crate::driver::node_payload(conn, node_id).await?;
    Ok(changelog::append(conn, actor, "node", node_id, Op::Update, payload).await?)
}

#[cfg(test)]
mod tests {
    use super::{CapRow, ExhaustedLease, TokenExhausted, check_token_cap, exhausted, old_primary};
    use crate::leases::MAX_TOKEN;
    use crate::rebuild::{Cell, Folded, FoldedRow, Row};
    use serde_json::{Value, json};

    const OLD: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0A";
    const NEW: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0B";
    const OTHER: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0C";

    fn row(cells: &[(&str, Cell)]) -> FoldedRow {
        FoldedRow {
            created_seq: 1,
            cells: cells
                .iter()
                .map(|(name, cell)| ((*name).to_string(), cell.clone()))
                .collect::<Row>(),
        }
    }

    fn lease(holder: &str, token: Value, revoked_at: Cell) -> FoldedRow {
        row(&[
            ("holder_node_id", Cell::Value(json!(holder))),
            ("fencing_token", Cell::Value(token)),
            ("revoked_at", revoked_at),
        ])
    }

    fn node(cp_status: Option<&str>, lost_at: Cell) -> FoldedRow {
        row(&[
            ("cp_status", Cell::Value(json!(cp_status))),
            ("lost_at", lost_at),
        ])
    }

    fn folded(nodes: Vec<(&str, FoldedRow)>, leases: Vec<(&str, FoldedRow)>) -> Folded {
        let mut folded = Folded::default();
        folded.tables.insert(
            "nodes",
            nodes
                .into_iter()
                .map(|(id, row)| (id.to_string(), row))
                .collect(),
        );
        folded.tables.insert(
            "leases",
            leases
                .into_iter()
                .map(|(id, row)| (id.to_string(), row))
                .collect(),
        );
        folded
    }

    fn named(leases: &[(&str, i64)]) -> TokenExhausted {
        TokenExhausted {
            leases: leases
                .iter()
                .map(|(lease_id, fencing_token)| ExhaustedLease {
                    lease_id: (*lease_id).to_string(),
                    fencing_token: *fencing_token,
                })
                .collect(),
        }
    }

    /// Section 7.3 and section 8.6 (ADR 0325): only a lease the declaration
    /// would raise past the cap is named: an unrevoked row of the old
    /// primary's node at or above the cap. A revoked row below the cap is
    /// raised to at most the cap, and one revoked at the cap is left as it
    /// is (as `removeNode` leaves it), so neither refuses; nor does a row at
    /// the cap that another node holds, or the node's unrevoked rows below
    /// the cap. Rows above the cap are named too, each with its token, in id
    /// order.
    #[test]
    fn the_cap_check_names_exactly_the_held_leases_at_the_cap() {
        let not_revoked = || Cell::Value(Value::Null);
        let folded = folded(
            vec![
                (OLD, node(Some("primary"), Cell::Default)),
                (NEW, node(None, Cell::Default)),
            ],
            vec![
                ("lse_b_at_cap", lease(OLD, json!(MAX_TOKEN), not_revoked())),
                ("lse_a_above", lease(OLD, json!(u64::MAX), Cell::Default)),
                ("lse_below", lease(OLD, json!(MAX_TOKEN - 1), not_revoked())),
                ("lse_other", lease(OTHER, json!(MAX_TOKEN), not_revoked())),
                (
                    "lse_revoked",
                    lease(
                        OLD,
                        json!(MAX_TOKEN),
                        Cell::Value(json!("2026-11-13T10:00:00Z")),
                    ),
                ),
            ],
        );
        assert_eq!(
            check_token_cap(&folded, OLD),
            Err(named(&[
                ("lse_a_above", i64::MAX),
                ("lse_b_at_cap", MAX_TOKEN)
            ]))
        );
        assert_eq!(
            check_token_cap(&folded, OTHER),
            Err(named(&[("lse_other", MAX_TOKEN)]))
        );
        assert_eq!(check_token_cap(&folded, NEW), Ok(()));
    }

    /// A node already declared lost has nothing raised (ADR 0202: the
    /// declaration changes nothing), so its leases at the cap refuse
    /// nothing. A fold with no `nodes` row for it, or no `leases` table,
    /// still checks what it has.
    #[test]
    fn a_node_already_lost_or_absent_is_checked_as_the_write_would() {
        let at_cap = || lease(OLD, json!(MAX_TOKEN), Cell::Default);
        let lost = folded(
            vec![(
                OLD,
                node(Some("primary"), Cell::Value(json!("2026-11-13T10:00:00Z"))),
            )],
            vec![("lse_x", at_cap())],
        );
        assert_eq!(check_token_cap(&lost, OLD), Ok(()));
        let unlisted = folded(vec![], vec![("lse_x", at_cap())]);
        assert_eq!(
            check_token_cap(&unlisted, OLD),
            Err(named(&[("lse_x", MAX_TOKEN)]))
        );
        assert_eq!(check_token_cap(&Folded::default(), OLD), Ok(()));
    }

    /// The refusal reads as the API's at the cap
    /// ([`crate::leases::token_exhausted`]), after its reason.
    #[test]
    fn the_refusal_is_worded_as_every_refusal_at_the_cap() {
        let refusal = exhausted(
            OLD,
            false,
            [CapRow {
                id: "lse_x",
                holder_node_id: Some(OLD),
                fencing_token: Some(MAX_TOKEN),
                revoked: false,
            }],
        )
        .unwrap_err();
        let lease = crate::leases::Lease {
            id: "lse_x".to_string(),
            resource_type: "app".to_string(),
            resource_id: "app_x".to_string(),
            holder_node_id: OLD.to_string(),
            mode: "local".to_string(),
            fencing_token: MAX_TOKEN,
            duration_ms: None,
            granted_at: String::new(),
            renewed_at: None,
            expires_at: None,
            revoked_at: None,
            unsettled: false,
        };
        let api = crate::leases::token_exhausted(&[&lease]);
        assert_eq!(
            refusal.to_string(),
            format!("token_exhausted: {}", api.detail.unwrap())
        );
        assert!(
            refusal
                .to_string()
                .contains(&format!("lease lse_x is at fencing token {MAX_TOKEN}")),
            "{refusal}"
        );
    }

    /// The old primary is the node that holds the status other than the
    /// promoted one, first by id; none when only the promoted node holds it.
    #[test]
    fn the_old_primary_is_the_other_holder_of_the_status() {
        let two = folded(
            vec![
                (NEW, node(None, Cell::Default)),
                (OTHER, node(Some("primary"), Cell::Default)),
                (OLD, node(Some("primary"), Cell::Default)),
            ],
            vec![],
        );
        assert_eq!(old_primary(&two, NEW), Some(OLD));
        assert_eq!(old_primary(&two, OLD), Some(OTHER));
        let only_new = folded(vec![(NEW, node(Some("primary"), Cell::Default))], vec![]);
        assert_eq!(old_primary(&only_new, NEW), None);
        assert_eq!(old_primary(&Folded::default(), NEW), None);
    }
}
